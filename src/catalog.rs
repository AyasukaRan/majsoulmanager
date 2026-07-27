use std::{
    fmt,
    future::Future,
    str::FromStr,
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use sqlx::{Row, postgres::PgPoolOptions};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    clickhouse::{ClickHouse, ClickHouseError},
    config::Config,
    pack::PackLocation,
};

/// Both schemas are embedded rather than read from disk: the runtime image
/// only carries the binary, and `migrations/*` is mounted at
/// `/docker-entrypoint-initdb.d`, which both containers run only when their
/// data volume is created. The live volumes already exist, so the application
/// is the only thing that can still apply them.
const POSTGRES_SCHEMA: &str = include_str!("../migrations/postgres.sql");
const CLICKHOUSE_SCHEMA: &str = include_str!("../migrations/clickhouse/001_records.sql");

const RECORDS_TABLE: &str = "mjai.records";
const RECORD_COLUMNS: &str = "record_id, source, sha256, received_at, played_at, players, rule, \
                              event_count, pack_key, pack_offset, compressed_size, raw_size";

/// A flush turns the whole buffer into one MergeTree part, so the batch size
/// trades insert amplification against how many packed-but-unindexed records a
/// crash leaves behind. Both ends are safe: the pack file is written first, and
/// the startup scan re-indexes anything the index is missing.
const INSERT_BATCH_ROWS: usize = 1_000;

/// docs/architecture.md line 76 — a query either carries a time range or is
/// bounded by a server-side maximum window.
const MAX_QUERY_WINDOW_DAYS: i64 = 90;

/// docs/architecture.md line 63 — the idempotency table keeps only the window
/// in which a collector may retry, never hundreds of millions of rows.
const IDEMPOTENCY_RETENTION_DAYS: i32 = 30;

/// A finished export is a file in `export_dir`, so the job row is only useful
/// for as long as that file is worth keeping.
const DOWNLOAD_JOB_RETENTION_DAYS: i32 = 7;

/// Arbitrary but stable: "mjai" as ASCII.
const MIGRATION_LOCK: i64 = 0x6D6A_6169;

#[derive(Clone, Debug, Serialize)]
pub struct Record {
    pub id: Uuid,
    pub source: String,
    pub sha256: String,
    pub received_at: DateTime<Utc>,
    pub played_at: Option<DateTime<Utc>>,
    pub players: Vec<String>,
    pub rule: Option<String>,
    pub event_count: u32,
    pub storage: PackLocation,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RecordFilter {
    pub source: Option<String>,
    pub player: Option<String>,
    pub received_from: Option<DateTime<Utc>>,
    pub received_to: Option<DateTime<Utc>>,
    pub played_from: Option<DateTime<Utc>>,
    pub played_to: Option<DateTime<Utc>>,
}

impl RecordFilter {
    /// Fills in the missing end of the `received_at` range instead of letting a
    /// bare `GET /api/v1/records` walk every partition, and refuses a range
    /// wider than the server window rather than silently truncating it.
    fn bounded(&self) -> Result<Self, CatalogError> {
        let mut bounded = self.clone();
        let window = TimeDelta::days(MAX_QUERY_WINDOW_DAYS);
        let to = bounded.received_to.unwrap_or_else(Utc::now);
        let from = *bounded.received_from.get_or_insert(to - window);
        if to - from > window {
            return Err(CatalogError::WindowTooWide(MAX_QUERY_WINDOW_DAYS));
        }
        Ok(bounded)
    }
}

/// Keyset position in `(received_at DESC, record_id DESC)`. Serialised as one
/// opaque string so the console keeps treating it as a token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub received_at: DateTime<Utc>,
    pub record_id: Uuid,
}

impl fmt::Display for Cursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}_{}",
            self.received_at.timestamp_millis(),
            self.record_id
        )
    }
}

impl FromStr for Cursor {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (millis, record_id) = value.split_once('_').ok_or("malformed cursor")?;
        Ok(Self {
            received_at: millis
                .parse()
                .ok()
                .and_then(DateTime::from_timestamp_millis)
                .ok_or("malformed cursor timestamp")?,
            record_id: record_id.parse().map_err(|_| "malformed cursor id")?,
        })
    }
}

impl Serialize for Cursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("idempotency key was already used with different content")]
    Conflict,
    #[error("the first request with this idempotency key is still being processed")]
    Pending,
    #[error("received_at range must not exceed {0} days")]
    WindowTooWide(i64),
    #[error("record index is unavailable: {0}")]
    Index(#[from] ClickHouseError),
    #[error("idempotency store is unavailable: {0}")]
    Store(#[from] sqlx::Error),
}

pub enum IdempotencyClaim {
    New,
    Existing(Record),
}

pub struct Catalog {
    index: ClickHouse,
    postgres: sqlx::PgPool,
    /// Records packed and claimed but not yet in a ClickHouse part. Point reads
    /// and idempotency replays answer from here so that a caller still reads
    /// its own write inside the batching window.
    pending: Mutex<Vec<Record>>,
}

impl Catalog {
    pub async fn connect(config: &Config) -> anyhow::Result<Self> {
        let deadline = Instant::now() + Duration::from_secs(config.database_wait_secs);
        let index = ClickHouse::new(
            &config.clickhouse_url,
            &config.clickhouse_user,
            &config.clickhouse_password,
        )?;
        let dsn = config.postgres_dsn.as_str();
        let postgres = wait_ready("PostgreSQL", deadline, || {
            // PostgreSQL only sees one small statement per ingest and the pack
            // writer serialises ingest anyway, so a wide pool would buy
            // nothing and the test suite opens one pool per case.
            PgPoolOptions::new()
                .max_connections(4)
                .acquire_timeout(Duration::from_secs(5))
                .connect(dsn)
        })
        .await?;
        wait_ready("ClickHouse", deadline, || {
            index.execute("SELECT 1", &[], String::new())
        })
        .await?;

        // Concurrent `CREATE TABLE IF NOT EXISTS` is not actually safe in
        // PostgreSQL — it races on pg_type — and every API replica applies both
        // schemas on boot. One advisory lock, held for both stores, makes that
        // a queue instead. It is released when the connection drops.
        let mut migrator = postgres.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(MIGRATION_LOCK)
            .execute(&mut *migrator)
            .await?;
        sqlx::raw_sql(POSTGRES_SCHEMA)
            .execute(&mut *migrator)
            .await?;
        // The ClickHouse HTTP interface takes one statement per request, so the
        // schema is split on `;`. Line comments come out first: a semicolon
        // inside one would cut a statement in half, and a chunk that is only a
        // comment is rejected as an empty query.
        let clickhouse_schema = CLICKHOUSE_SCHEMA
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for statement in clickhouse_schema
            .split(';')
            .filter(|statement| !statement.trim().is_empty())
        {
            index.execute(statement, &[], String::new()).await?;
        }
        drop(migrator);

        let catalog = Self {
            index,
            postgres,
            pending: Mutex::new(Vec::new()),
        };
        catalog.prune().await?;
        Ok(catalog)
    }

    /// Keeps PostgreSQL to the two time-windowed tables it is meant to hold.
    /// Runs at boot rather than on a timer: the tables only grow while the
    /// process is up, and one deploy per day is plenty for a 30 day window.
    async fn prune(&self) -> Result<(), sqlx::Error> {
        let idempotency = sqlx::query(
            "DELETE FROM ingest_idempotency WHERE created_at < now() - make_interval(days => $1)",
        )
        .bind(IDEMPOTENCY_RETENTION_DAYS)
        .execute(&self.postgres)
        .await?;
        let jobs = sqlx::query("DELETE FROM download_jobs WHERE expires_at < now()")
            .execute(&self.postgres)
            .await?;
        tracing::info!(
            idempotency = idempotency.rows_affected(),
            jobs = jobs.rows_affected(),
            "pruned expired PostgreSQL rows"
        );
        Ok(())
    }

    /// The `INSERT ... ON CONFLICT DO NOTHING` is the whole check-and-set: two
    /// concurrent requests with the same key reach PostgreSQL, exactly one gets
    /// a row back and packs the record, the other falls through to the lookup.
    pub async fn claim(
        &self,
        key: &str,
        id: Uuid,
        sha256: &str,
    ) -> Result<IdempotencyClaim, CatalogError> {
        let claimed = sqlx::query(
            "INSERT INTO ingest_idempotency (key_hash, record_id, content_sha256, state)
             VALUES (sha256($1), $2, decode($3, 'hex'), 'accepted')
             ON CONFLICT (key_hash) DO NOTHING
             RETURNING record_id",
        )
        .bind(key.as_bytes())
        .bind(id)
        .bind(sha256)
        .fetch_optional(&self.postgres)
        .await?;
        if claimed.is_some() {
            return Ok(IdempotencyClaim::New);
        }

        let row = sqlx::query(
            "SELECT record_id, encode(content_sha256, 'hex') AS content_sha256
             FROM ingest_idempotency WHERE key_hash = sha256($1)",
        )
        .bind(key.as_bytes())
        .fetch_optional(&self.postgres)
        .await?
        // The holder abandoned the claim between the two statements. Reporting
        // it as pending costs the caller one retry; taking the claim here would
        // need the insert repeated in a loop for no practical gain.
        .ok_or(CatalogError::Pending)?;
        if row.try_get::<String, _>("content_sha256")? != sha256 {
            return Err(CatalogError::Conflict);
        }
        self.get(row.try_get("record_id")?)
            .await?
            .map(IdempotencyClaim::Existing)
            .ok_or(CatalogError::Pending)
    }

    pub async fn abandon_claim(&self, key: &str, id: Uuid) -> Result<(), CatalogError> {
        sqlx::query(
            "DELETE FROM ingest_idempotency WHERE key_hash = sha256($1) AND record_id = $2",
        )
        .bind(key.as_bytes())
        .bind(id)
        .execute(&self.postgres)
        .await?;
        Ok(())
    }

    pub async fn insert(&self, record: Record) -> Result<(), CatalogError> {
        let mut pending = self.pending.lock().await;
        pending.push(record);
        if pending.len() >= INSERT_BATCH_ROWS {
            flush_pending(&self.index, &mut pending).await?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), CatalogError> {
        let mut pending = self.pending.lock().await;
        Ok(flush_pending(&self.index, &mut pending).await?)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Record>, CatalogError> {
        if let Some(record) = self
            .pending
            .lock()
            .await
            .iter()
            .rev()
            .find(|record| record.id == id)
        {
            return Ok(Some(record.clone()));
        }
        // `record_id` is last in the sorting key, so this leans on the bloom
        // filter added for it; `ORDER BY indexed_at` picks the newest of any
        // replayed rows without paying for FINAL.
        let rows: Vec<RecordRow> = self
            .index
            .query(
                &format!(
                    "SELECT {RECORD_COLUMNS} FROM {RECORDS_TABLE} \
                     WHERE record_id = {{id:UUID}} ORDER BY indexed_at DESC LIMIT 1"
                ),
                &[("id", id.to_string())],
            )
            .await?;
        Ok(rows.into_iter().next().map(Record::from))
    }

    pub async fn search(
        &self,
        filter: &RecordFilter,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Cursor>), CatalogError> {
        self.page(&filter.bounded()?, cursor, limit).await
    }

    /// Export paging. Unlike `search` it carries no window: docs/architecture.md
    /// line 88 wants an export streamed by keyset instead of materialising the
    /// hit set, and a keyset walk stays bounded however many rows it visits.
    pub async fn scan(
        &self,
        filter: &RecordFilter,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Cursor>), CatalogError> {
        self.page(filter, cursor, limit).await
    }

    async fn page(
        &self,
        filter: &RecordFilter,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<Cursor>), CatalogError> {
        self.flush().await?;
        // FINAL collapses rows a replayed insert batch duplicated;
        // ReplacingMergeTree otherwise only dedups within a merged part.
        let mut sql = format!("SELECT {RECORD_COLUMNS} FROM {RECORDS_TABLE} FINAL WHERE 1");
        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(source) = &filter.source {
            sql.push_str(" AND source = {source:String}");
            params.push(("source", source.clone()));
        }
        if let Some(player) = &filter.player {
            sql.push_str(" AND has(players, {player:String})");
            params.push(("player", player.clone()));
        }
        // Bounds go over the wire as epoch milliseconds: a bare
        // `{x:DateTime64(3)}` would be read in the server timezone, and the
        // `played_at` comparisons are NULL for unset values either way, which
        // is the same rows the in-memory filter used to drop.
        for (column, comparison, name, value) in [
            ("received_at", ">=", "received_from", filter.received_from),
            ("played_at", ">=", "played_from", filter.played_from),
            ("received_at", "<", "received_to", filter.received_to),
            ("played_at", "<", "played_to", filter.played_to),
        ] {
            if let Some(value) = value {
                sql.push_str(&format!(
                    " AND {column} {comparison} fromUnixTimestamp64Milli({{{name}:Int64}})"
                ));
                params.push((name, value.timestamp_millis().to_string()));
            }
        }
        if let Some(cursor) = cursor {
            sql.push_str(
                " AND (received_at, record_id) < \
                 (fromUnixTimestamp64Milli({cursor_at:Int64}), {cursor_id:UUID})",
            );
            params.push((
                "cursor_at",
                cursor.received_at.timestamp_millis().to_string(),
            ));
            params.push(("cursor_id", cursor.record_id.to_string()));
        }
        sql.push_str(" ORDER BY received_at DESC, record_id DESC LIMIT {limit:UInt32}");
        params.push(("limit", (limit + 1).to_string()));

        let rows: Vec<RecordRow> = self.index.query(&sql, &params).await?;
        let mut records: Vec<Record> = rows.into_iter().map(Record::from).collect();
        let next_cursor = (records.len() > limit).then(|| {
            records.truncate(limit);
            let last = &records[limit - 1];
            Cursor {
                received_at: last.received_at,
                record_id: last.id,
            }
        });
        Ok((records, next_cursor))
    }

    /// Per-pack row counts, used by the startup scan to decide which packs are
    /// worth reading. `uniqExact` rather than `count()` so a replayed insert
    /// does not make a pack look complete.
    pub async fn indexed_counts(&self) -> Result<Vec<(String, u64)>, CatalogError> {
        #[derive(Deserialize)]
        struct PackCount {
            pack_key: String,
            records: u64,
        }
        let rows: Vec<PackCount> = self
            .index
            .query(
                &format!(
                    "SELECT pack_key, uniqExact(record_id) AS records \
                     FROM {RECORDS_TABLE} GROUP BY pack_key"
                ),
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.pack_key, row.records))
            .collect())
    }

    pub async fn indexed_ids(&self, pack_key: &str) -> Result<Vec<Uuid>, CatalogError> {
        #[derive(Deserialize)]
        struct IndexedId {
            record_id: Uuid,
        }
        let rows: Vec<IndexedId> = self
            .index
            .query(
                &format!(
                    "SELECT DISTINCT record_id FROM {RECORDS_TABLE} WHERE pack_key = {{pack:String}}"
                ),
                &[("pack", pack_key.to_owned())],
            )
            .await?;
        Ok(rows.into_iter().map(|row| row.record_id).collect())
    }

    pub async fn create_job(&self, request: &DownloadRequest) -> Result<DownloadJob, CatalogError> {
        let id = Uuid::new_v4();
        let created_at = Utc::now();
        sqlx::query(
            "INSERT INTO download_jobs (id, state, filter, format, expires_at)
             VALUES ($1, 'queued', $2, $3, now() + make_interval(days => $4))",
        )
        .bind(id)
        .bind(sqlx::types::Json(&request.filter))
        .bind(request.format.as_str())
        .bind(DOWNLOAD_JOB_RETENTION_DAYS)
        .execute(&self.postgres)
        .await?;
        Ok(DownloadJob {
            id,
            state: JobState::Queued,
            created_at,
            record_count: 0,
            download_url: None,
            error: None,
        })
    }

    pub async fn start_job(&self, id: Uuid) -> Result<(), CatalogError> {
        sqlx::query("UPDATE download_jobs SET state = 'running', started_at = now() WHERE id = $1")
            .bind(id)
            .execute(&self.postgres)
            .await?;
        Ok(())
    }

    pub async fn finish_job(
        &self,
        id: Uuid,
        outcome: Result<(usize, String), String>,
    ) -> Result<(), CatalogError> {
        let (state, count, object_key, error) = match outcome {
            Ok((count, object_key)) => ("completed", count as i64, Some(object_key), None),
            Err(error) => ("failed", 0, None, Some(error)),
        };
        sqlx::query(
            "UPDATE download_jobs
             SET state = $2, record_count = $3, result_object_key = $4, error = $5,
                 completed_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(state)
        .bind(count)
        .bind(object_key)
        .bind(error)
        .execute(&self.postgres)
        .await?;
        Ok(())
    }

    pub async fn get_job(&self, id: Uuid) -> Result<Option<DownloadJob>, CatalogError> {
        let row = sqlx::query(
            "SELECT id, state, record_count, result_object_key, error, created_at
             FROM download_jobs WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.postgres)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state: String = row.try_get("state")?;
        Ok(Some(DownloadJob {
            id,
            state: JobState::from_column(&state),
            created_at: row.try_get("created_at")?,
            record_count: row.try_get::<i64, _>("record_count")?.max(0) as usize,
            download_url: row
                .try_get::<Option<String>, _>("result_object_key")?
                .map(|_| format!("/api/v1/downloads/{id}/file")),
            error: row.try_get("error")?,
        }))
    }
}

/// Rows stay in the buffer when the insert fails so the next flush retries
/// them; a retry that lands twice converges through ReplacingMergeTree.
async fn flush_pending(
    index: &ClickHouse,
    pending: &mut Vec<Record>,
) -> Result<(), ClickHouseError> {
    if pending.is_empty() {
        return Ok(());
    }
    let rows = pending
        .iter()
        .map(record_json)
        .collect::<Vec<_>>()
        .join("\n");
    index.insert(RECORDS_TABLE, rows).await?;
    pending.clear();
    Ok(())
}

fn record_json(record: &Record) -> String {
    serde_json::json!({
        "record_id": record.id,
        "source": record.source,
        "sha256": record.sha256,
        "received_at": clickhouse_timestamp(record.received_at),
        "played_at": record.played_at.map(clickhouse_timestamp),
        "players": record.players,
        // LowCardinality(String) is not nullable; no mjai `start_game.rule` is
        // an empty string, so "" round-trips back to None.
        "rule": record.rule.clone().unwrap_or_default(),
        // The column is UInt16. `max_record_bytes` (16KiB) caps a record at
        // roughly 2k events, so this only guards a future raise of that limit,
        // where a wrong count still beats failing the whole insert batch.
        "event_count": record.event_count.min(u32::from(u16::MAX)),
        "pack_key": record.storage.pack_key,
        "pack_offset": record.storage.offset,
        "compressed_size": record.storage.compressed_size,
        "raw_size": record.storage.raw_size,
        "codec": record.storage.codec,
    })
    .to_string()
}

async fn wait_ready<T, E, F, Fut>(
    label: &str,
    deadline: Instant,
    mut attempt: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    let mut backoff = Duration::from_millis(250);
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) if Instant::now() + backoff < deadline => {
                tracing::warn!(database = label, %error, "database is not ready yet, retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
            Err(error) => {
                anyhow::bail!(
                    "{label} was still unreachable when the startup wait ran out: {error}"
                )
            }
        }
    }
}

fn clickhouse_timestamp(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn parse_clickhouse_timestamp<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<DateTime<Utc>, D::Error> {
    let text = String::deserialize(deserializer)?;
    NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f")
        .map(|naive| naive.and_utc())
        .map_err(D::Error::custom)
}

fn parse_optional_clickhouse_timestamp<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    let Some(text) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f")
        .map(|naive| Some(naive.and_utc()))
        .map_err(D::Error::custom)
}

#[derive(Deserialize)]
struct RecordRow {
    record_id: Uuid,
    source: String,
    sha256: String,
    #[serde(deserialize_with = "parse_clickhouse_timestamp")]
    received_at: DateTime<Utc>,
    #[serde(deserialize_with = "parse_optional_clickhouse_timestamp")]
    played_at: Option<DateTime<Utc>>,
    players: Vec<String>,
    rule: String,
    event_count: u32,
    pack_key: String,
    pack_offset: u64,
    compressed_size: u32,
    raw_size: u32,
}

impl From<RecordRow> for Record {
    fn from(row: RecordRow) -> Self {
        Self {
            id: row.record_id,
            source: row.source,
            sha256: row.sha256,
            received_at: row.received_at,
            played_at: row.played_at,
            players: row.players,
            rule: (!row.rule.is_empty()).then_some(row.rule),
            event_count: row.event_count,
            storage: PackLocation {
                pack_key: row.pack_key,
                offset: row.pack_offset,
                compressed_size: row.compressed_size,
                raw_size: row.raw_size,
                codec: "zstd",
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DownloadRequest {
    #[serde(default)]
    pub filter: RecordFilter,
    #[serde(default)]
    pub format: DownloadFormat,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub enum DownloadFormat {
    #[default]
    #[serde(rename = "tar.gz")]
    TarGz,
    #[serde(rename = "manifest.jsonl")]
    ManifestJsonl,
}

impl DownloadFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TarGz => "tar.gz",
            Self::ManifestJsonl => "manifest.jsonl",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Completed,
    Failed,
}

impl JobState {
    fn from_column(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => Self::Queued,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DownloadJob {
    pub id: Uuid,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub record_count: usize,
    pub download_url: Option<String>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_through_its_token() {
        let cursor = Cursor {
            received_at: DateTime::from_timestamp_millis(1_753_600_000_123).unwrap(),
            record_id: Uuid::new_v4(),
        };
        assert_eq!(cursor.to_string().parse::<Cursor>().unwrap(), cursor);
    }

    #[test]
    fn bounded_fills_the_window_and_rejects_a_wider_range() {
        let now = Utc::now();
        let bounded = RecordFilter::default().bounded().unwrap();
        assert!(bounded.received_from.unwrap() <= now - TimeDelta::days(MAX_QUERY_WINDOW_DAYS - 1));
        let wide = RecordFilter {
            received_from: Some(now - TimeDelta::days(MAX_QUERY_WINDOW_DAYS + 1)),
            received_to: Some(now),
            ..RecordFilter::default()
        };
        assert!(matches!(
            wide.bounded(),
            Err(CatalogError::WindowTooWide(MAX_QUERY_WINDOW_DAYS))
        ));
    }
}
