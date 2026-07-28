use std::{
    fmt,
    future::Future,
    str::FromStr,
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDateTime, SubsecRound, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use sqlx::{Row, postgres::PgPoolOptions};
use thiserror::Error;
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

/// The overview names the busiest collectors, not every value that has ever
/// appeared in an `X-Mjai-Source` header.
const MAX_REPORTED_SOURCES: usize = 100;

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
    pub rule: Option<String>,
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
    /// The record id the first request with this key was given. Only the id,
    /// never the indexed row: under the Kafka pipeline a duplicate routinely
    /// arrives before the worker has indexed the first copy, and reading the
    /// row back to prove the claim would answer `Pending` — a `409` — for every
    /// one of them, turning an ordinary re-import into a wall of conflicts.
    Existing(Uuid),
}

pub struct Catalog {
    index: ClickHouse,
    postgres: sqlx::PgPool,
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
            // PostgreSQL sees one small statement per ingested record and one
            // more per sealed pack, so a wide pool would buy nothing and the
            // test suite opens one pool per case.
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
        // a queue instead. It has to be the transaction-scoped variant:
        // `pg_advisory_lock` is session-scoped, and dropping a pooled
        // connection only returns it to the pool, so a session lock would
        // outlive this function and block the next replica for the life of the
        // process. A transaction releases it on commit and on rollback alike.
        let mut migration = postgres.begin().await?;
        tracing::info!("acquiring the schema migration lock");
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(MIGRATION_LOCK)
            .execute(&mut *migration)
            .await?;
        sqlx::raw_sql(POSTGRES_SCHEMA)
            .execute(&mut *migration)
            .await?;
        // Whether the skip index reaches the rows it was added for is decided
        // before the DDL runs: `ADD INDEX` only affects parts written after it,
        // so an installation that already has the table needs the existing
        // parts materialised, and one that does not gets the index from the
        // `CREATE` and nothing to rewrite.
        let backfill_record_id_bloom = !index
            .has_skip_index("mjai", "records", "record_id_bloom")
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
        if backfill_record_id_bloom {
            // Asynchronous by default: the mutation rewrites the index files of
            // every existing part, and boot must not wait for it. Running it
            // only on the boot that introduced the index keeps a table that is
            // sized for 数亿 rows from being rewritten on every restart.
            index
                .execute(
                    "ALTER TABLE mjai.records MATERIALIZE INDEX record_id_bloom",
                    &[],
                    String::new(),
                )
                .await?;
        }
        migration.commit().await?;

        let catalog = Self { index, postgres };
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
    /// a row back and produces the record, the other falls through to the
    /// lookup.
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
        Ok(IdempotencyClaim::Existing(row.try_get("record_id")?))
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

    /// One sealed pack's rows, in one statement and therefore one MergeTree
    /// part. The batch is whatever the pack worker sealed, which at the 256MB
    /// pack target and the measured record sizes is 2,500 to 23,000 rows and a
    /// few megabytes of JSON — well inside what one HTTP insert carries, and
    /// the ceiling only moves if `MJAI_PACK_TARGET_BYTES` is raised by orders
    /// of magnitude, at which point the worker would chunk this call.
    ///
    /// Called only after the pack is durably in the bucket, and the Kafka
    /// offset is committed only after this returns, which is the ordering the
    /// whole pipeline's failure story rests on: a crash here replays the batch
    /// and the replay converges through ReplacingMergeTree.
    pub async fn insert_batch(&self, records: &[Record]) -> Result<(), CatalogError> {
        if records.is_empty() {
            return Ok(());
        }
        // Milliseconds are the resolution the index and the cursor both have:
        // the columns are DateTime64(3) and a cursor token is epoch millis. A
        // replay must land on the byte-identical sorting key, so truncating
        // here rather than at the call site keeps every producer of a `Record`
        // — the worker and the recovery scan alike — on one resolution.
        let rows: Vec<String> = records
            .iter()
            .map(|record| {
                record_json(
                    record,
                    record.received_at.trunc_subsecs(3),
                    record.played_at.map(|at| at.trunc_subsecs(3)),
                )
            })
            .collect();
        self.index.insert(RECORDS_TABLE, rows.join("\n")).await?;
        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Record>, CatalogError> {
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

    /// Export paging. Unlike `search` it carries no window: the window in
    /// docs/architecture.md line 76 is a rule about 索引与筛选, and the export
    /// section that follows asks for the opposite — a keyset walk streamed into
    /// the archive — because exporting the whole corpus is the feature. What
    /// that section also asks for and this does not yet do is write the result
    /// to RustFS and hand back a presigned URL; until the RustFS adapter lands,
    /// a filterless export is a second copy of the corpus on the API's own
    /// disk. That is the listed pre-launch gap, not a missing time filter.
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
        // Every row this answers with comes from ClickHouse and nowhere else,
        // and nothing may ever merge a second source into it again. A page is
        // ordered by `(received_at DESC, record_id DESC)`, so a timestamp tie
        // rests the whole order on the collation of `record_id` — and
        // ClickHouse compares a UUID as (low 64 bits, high 64 bits) while
        // `Uuid: Ord` compares the sixteen bytes big-endian. Merging an
        // in-memory buffer in Rust therefore needed Rust to reproduce
        // ClickHouse's collation for every column the cursor touches and its
        // filter to reproduce every `WHERE` clause built below. Two rounds of
        // that lost records. The buffer is gone with the inline ingest path
        // that needed it — rows now reach the index in one batch per sealed
        // pack — which leaves one sort, one filter and one collation, all of
        // them ClickHouse's, with nothing to reconcile.

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
        // The rule is one of the twelve `{players}p-{room}-{game_length}` tokens
        // the parser derives, so it is matched whole like the source rather than
        // by prefix: a substring match would make "3p-jade-east" a hit for
        // "3p-jade-east-something" the day a thirteenth value appears.
        if let Some(rule) = &filter.rule {
            sql.push_str(" AND rule = {rule:String}");
            params.push(("rule", rule.clone()));
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
                // Records are stored truncated to the millisecond, so a bound
                // carrying finer precision has to round outwards or it excludes
                // the very millisecond it falls inside: an inclusive `from`
                // floors onto that millisecond, an exclusive `to` has to reach
                // past it. Flooring both would drop every record that arrived
                // earlier in the same millisecond as an exclusive upper bound.
                let millis = match comparison {
                    "<" => ceil_millis(value),
                    _ => value.timestamp_millis(),
                };
                params.push((name, millis.to_string()));
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
        // `(received_at DESC, record_id DESC)` is not a prefix of the table's
        // sorting key — `toDate(received_at)` and `source` come first — so
        // ClickHouse cannot read the page already ordered and sorts the matched
        // set instead. Kept anyway: the cursor is the pair docs/architecture.md
        // line 76 mandates, and ordering by the full key would have to carry
        // `source` in the cursor and break the token the console already holds.
        // Measured on 641k rows shaped like the live corpus (41 packs, 180 days,
        // 90 day window, limit 100): 8ms against 7ms for the sorting-key order.
        // FINAL forces a sort either way, so the ordering is not what to fix
        // first if that ever becomes the bottleneck.
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
    ///
    /// It is a full aggregate over two columns and it runs before the listener
    /// binds. Measured on 641k rows shaped like the live corpus: 9ms, against
    /// 12ms for the same query restricted to the pack keys found on disk, which
    /// is slower because every indexed pack is still on local disk and the
    /// filter only adds work to the same scan. Reconciliation is what costs at
    /// scale, not this query — the header walk in `recovery` reads 24 bytes per
    /// record — and the fix for both is to stop reconciling the whole corpus on
    /// the boot path, not to narrow the `WHERE`.
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

    /// The Kafka offset table lives in the same database, and the pack worker
    /// commits into it on the same connection pool this already sizes and waits
    /// for at boot. Handing out the pool keeps `src/kafka.rs` owning its own
    /// statements rather than growing a second set of query methods here.
    pub fn postgres(&self) -> &sqlx::PgPool {
        &self.postgres
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

    /// Publishes how far a running export has got. Without it `record_count`
    /// stays at its default until the job finishes, so the console shows a job
    /// that has been writing for hours as `导出中 / 0` — indistinguishable from
    /// one that is wedged. One statement per page of a thousand records is
    /// cheap enough that the alternative is only worth it if exports ever get
    /// small enough for the count not to matter, at which point nobody is
    /// watching the number anyway.
    pub async fn record_job_progress(&self, id: Uuid, written: usize) -> Result<(), CatalogError> {
        sqlx::query("UPDATE download_jobs SET record_count = $2 WHERE id = $1")
            .bind(id)
            .bind(written as i64)
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
            Ok((count, object_key)) => ("completed", Some(count as i64), Some(object_key), None),
            // The count is left as `record_job_progress` last published it
            // rather than zeroed. Overwriting it here would make a job that
            // died on its last page read exactly like one that died on its
            // first, which is the ambiguity that publishing progress at all was
            // meant to remove.
            Err(error) => ("failed", None, None, Some(error)),
        };
        sqlx::query(
            "UPDATE download_jobs
             SET state = $2, record_count = coalesce($3, record_count),
                 result_object_key = $4, error = $5, completed_at = now()
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
        let row = sqlx::query(&format!("{JOB_COLUMNS} WHERE id = $1"))
            .bind(id)
            .fetch_optional(&self.postgres)
            .await?;
        Ok(row.as_ref().map(job_from_row).transpose()?)
    }

    /// The newest jobs first. A sort with no index behind it, deliberately:
    /// `prune` deletes everything past `DOWNLOAD_JOB_RETENTION_DAYS`, so this
    /// orders a few days of exports rather than a history, and the partial
    /// index the schema does carry covers only the unfinished ones. It wants a
    /// `created_at` index the day exports become frequent enough that a week of
    /// them stops being a small table.
    pub async fn recent_jobs(&self, limit: usize) -> Result<Vec<DownloadJob>, CatalogError> {
        let rows = sqlx::query(&format!("{JOB_COLUMNS} ORDER BY created_at DESC LIMIT $1"))
            .bind(limit as i64)
            .fetch_all(&self.postgres)
            .await?;
        Ok(rows.iter().map(job_from_row).collect::<Result<_, _>>()?)
    }

    /// Grouped rather than counted four times, over the same few days of rows
    /// `recent_jobs` orders; a state with no jobs is simply absent from the
    /// answer and keeps its zero.
    pub async fn download_counts(&self) -> Result<DownloadCounts, CatalogError> {
        let rows = sqlx::query("SELECT state, count(*) AS jobs FROM download_jobs GROUP BY state")
            .fetch_all(&self.postgres)
            .await?;
        let mut counts = DownloadCounts::default();
        for row in rows {
            let jobs = row.try_get::<i64, _>("jobs")?.max(0) as u64;
            match JobState::from_column(&row.try_get::<String, _>("state")?) {
                JobState::Queued => counts.queued = jobs,
                JobState::Running => counts.running = jobs,
                JobState::Completed => counts.completed = jobs,
                JobState::Failed => counts.failed = jobs,
            }
        }
        Ok(counts)
    }

    /// The console's overview, polled. Three statements rather than one because
    /// they cost wildly different amounts and folding them together would drag
    /// the cheap ones up to the price of the dear one; issued together because
    /// they are independent and the poll waits for all three anyway.
    ///
    /// None of them uses FINAL, and none of them counts distinct record ids. A
    /// replayed insert batch leaves a second row for a record until the parts
    /// merge, so every count here can read a few rows high inside that window.
    /// That is the trade: `uniqExact(record_id)` would be exact and would build
    /// a hash set of every id in a table sized for hundreds of millions of
    /// them, on every poll, which is not a price an overview may charge.
    pub async fn stats(&self) -> Result<IndexStats, CatalogError> {
        #[derive(Default, Deserialize)]
        struct Totals {
            total: u64,
            packs: u64,
            raw_bytes: u64,
            compressed_bytes: u64,
        }
        #[derive(Default, Deserialize)]
        struct Recent {
            last_24h: u64,
        }
        // The dear one. `count()` on its own would be answered out of part
        // metadata without reading a column at all, but the two sums read eight
        // bytes per row whatever else the statement does, so the count rides
        // along free; `uniqExact(pack_key)` accumulates one entry per 256MB
        // pack rather than one per record, which is what makes it affordable
        // here where `uniqExact(record_id)` is not. This is the statement to
        // replace with an AggregatingMergeTree rollup maintained on insert if
        // the overview ever becomes the slowest thing the console does.
        let totals_sql = format!(
            "SELECT count() AS total, uniqExact(pack_key) AS packs, \
             sum(raw_size) AS raw_bytes, sum(compressed_size) AS compressed_bytes \
             FROM {RECORDS_TABLE}"
        );
        // Cheap: `toYYYYMM(received_at)` is the partition key and
        // `toDate(received_at)` leads the sorting key, so a lower bound on
        // `received_at` reaches both and the scan is one day of granules.
        let recent_sql = format!(
            "SELECT count() AS last_24h FROM {RECORDS_TABLE} \
             WHERE received_at >= now() - toIntervalDay(1)"
        );
        // `source` is LowCardinality, so grouping by it reads a dictionary-coded
        // column that compresses to a rounding error beside the table. Capped
        // because the value arrives in a collector's header: nothing stops an
        // authenticated collector from inventing a source per request, and an
        // overview answering with an unbounded array is the wrong place to find
        // that out.
        let sources_sql = format!(
            "SELECT source, count() AS records FROM {RECORDS_TABLE} \
             GROUP BY source ORDER BY records DESC LIMIT {MAX_REPORTED_SOURCES}"
        );
        let (totals, recent, sources) = tokio::try_join!(
            self.index.query::<Totals>(&totals_sql, &[]),
            self.index.query::<Recent>(&recent_sql, &[]),
            self.index.query::<SourceCount>(&sources_sql, &[]),
        )?;
        let totals = totals.into_iter().next().unwrap_or_default();
        Ok(IndexStats {
            records: RecordStats {
                total: totals.total,
                last_24h: recent.into_iter().next().unwrap_or_default().last_24h,
                sources,
            },
            storage: StorageStats {
                packs: totals.packs,
                raw_bytes: totals.raw_bytes,
                compressed_bytes: totals.compressed_bytes,
            },
        })
    }
}

/// Every column a `DownloadJob` is built from, in the one place both readers of
/// the table select them.
const JOB_COLUMNS: &str = "SELECT id, state, record_count, result_object_key, error, created_at \
                           FROM download_jobs";

fn job_from_row(row: &sqlx::postgres::PgRow) -> Result<DownloadJob, sqlx::Error> {
    let id: Uuid = row.try_get("id")?;
    Ok(DownloadJob {
        id,
        state: JobState::from_column(&row.try_get::<String, _>("state")?),
        created_at: row.try_get("created_at")?,
        record_count: row.try_get::<i64, _>("record_count")?.max(0) as usize,
        download_url: row
            .try_get::<Option<String>, _>("result_object_key")?
            .map(|_| format!("/api/v1/downloads/{id}/file")),
        error: row.try_get("error")?,
    })
}

fn record_json(
    record: &Record,
    received_at: DateTime<Utc>,
    played_at: Option<DateTime<Utc>>,
) -> String {
    serde_json::json!({
        "record_id": record.id,
        "source": record.source,
        "sha256": record.sha256,
        "received_at": clickhouse_timestamp(received_at),
        "played_at": played_at.map(clickhouse_timestamp),
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
        // Each attempt is bounded by the wait itself. A database that accepts
        // the connection and then never answers fails no client-side check, so
        // without this the deadline is only consulted between attempts and the
        // process hangs on the first one instead of exiting to be restarted.
        let remaining = deadline.saturating_duration_since(Instant::now());
        let error = match tokio::time::timeout(remaining, attempt()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!("no answer within {remaining:?}"),
        };
        if Instant::now() + backoff >= deadline {
            anyhow::bail!("{label} was still unreachable when the startup wait ran out: {error}");
        }
        tracing::warn!(database = label, %error, "database is not ready yet, retrying");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// Smallest whole millisecond at or after `value`, for an exclusive upper bound
/// compared against millisecond-truncated rows.
fn ceil_millis(value: DateTime<Utc>) -> i64 {
    let millis = value.timestamp_millis();
    if value.timestamp_subsec_nanos() % 1_000_000 == 0 {
        millis
    } else {
        millis + 1
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

/// What the console's overview asks the index for. `records.total` is a row
/// count, not a distinct record count, and `storage` is what the index says the
/// packs hold rather than what the bucket bills for; both are documented on
/// `Catalog::stats`, which is where the reasons are.
#[derive(Clone, Debug, Serialize)]
pub struct IndexStats {
    pub records: RecordStats,
    pub storage: StorageStats,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecordStats {
    pub total: u64,
    pub last_24h: u64,
    /// The busiest `MAX_REPORTED_SOURCES`, so this does not have to sum to
    /// `total`.
    pub sources: Vec<SourceCount>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceCount {
    pub source: String,
    pub records: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageStats {
    pub packs: u64,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct DownloadCounts {
    pub queued: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
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
