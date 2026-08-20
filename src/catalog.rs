use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    str::FromStr,
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, NaiveDateTime, SubsecRound, TimeDelta, Timelike, Utc};
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
/// Applied in order at every boot, each statement separately. Splitting the
/// schema across files rather than growing one keeps a table's definition, its
/// comments and its later `ALTER`s in one place.
const CLICKHOUSE_SCHEMA: [&str; 4] = [
    include_str!("../migrations/clickhouse/001_records.sql"),
    include_str!("../migrations/clickhouse/002_player_games.sql"),
    include_str!("../migrations/clickhouse/003_paipuya_games.sql"),
    include_str!("../migrations/clickhouse/004_game_uuids.sql"),
];

const RECORDS_TABLE: &str = "mjai.records";
const RECORD_COLUMNS: &str = "record_id, source, sha256, received_at, played_at, players, rule, \
                              event_count, pack_key, pack_offset, compressed_size, raw_size, \
                              pb_offset, pb_compressed_size, pb_size";

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

/// The server-side ceiling docs/architecture.md 索引与筛选 asks for, applied to
/// the daily trend buckets. Cited by section rather than by line: the
/// neighbouring constants point at line numbers that the file has since moved
/// out from under them. Wider than `MAX_QUERY_WINDOW_DAYS` on purpose: that one
/// bounds a query that returns a row per record, this one returns a row per
/// bucket and reads three columns to build them, so a year costs the scan and
/// not the transfer.
const MAX_TREND_DAYS: u32 = 365;

/// A week of hourly buckets. The ceiling is the chart rather than the query:
/// past about this many bars a bucket is thinner than the gap beside it, and a
/// window that wide is asking a question about days anyway.
const MAX_TREND_HOURS: u32 = 168;

/// What a caller gets for asking without saying how much.
pub const DEFAULT_TREND_SPAN: u32 = 30;

/// The parser derives twelve `{players}p-{room}-{length}` tokens, but `rule` is
/// whatever a collector's own converter wrote into the header, so the panel that
/// reports it is bounded like the source breakdown beside it.
const MAX_REPORTED_RULES: usize = 24;

/// The search box shows one screen of names, not a directory of the corpus.
const MAX_PLAYER_HITS: usize = 50;

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
    /// Where the Mahjong Soul protobuf this record was converted from sits in
    /// the same pack, when one was kept. Absent for every record indexed before
    /// the converter stopped discarding it, and for every record that never had
    /// one: an imported mjai log was never a protobuf.
    pub majsoul_pb: Option<PbLocation>,
}

/// The protobuf half of a record, in the record's own pack. Deliberately not a
/// `PackLocation`: it has no `pack_key` of its own — see the column comments in
/// `migrations/clickhouse/001_records.sql` for why it cannot need one — and
/// giving it one would invite a caller to set the two keys apart.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PbLocation {
    pub offset: u64,
    pub compressed_size: u32,
    pub raw_size: u32,
}

impl PbLocation {
    /// Reads the three stored columns back, treating the all-zero row the
    /// `ALTER` left behind on existing installations as "no protobuf". A stored
    /// entry can never look like that: `offset` counts from the start of a pack
    /// whose first bytes are its magic header, so a real one is never zero.
    fn from_columns(offset: u64, compressed_size: u32, raw_size: u32) -> Option<Self> {
        (raw_size > 0 && offset > 0).then_some(Self {
            offset,
            compressed_size,
            raw_size,
        })
    }

    /// The location a pack read wants, borrowing the record's own pack key.
    pub fn in_pack(&self, pack_key: &str) -> PackLocation {
        PackLocation {
            pack_key: pack_key.to_owned(),
            offset: self.offset,
            compressed_size: self.compressed_size,
            raw_size: self.raw_size,
            codec: "zstd",
        }
    }
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
    /// Only records that carry no Mahjong Soul protobuf. Defaulted rather than
    /// optional so every existing caller and every export job snapshot written
    /// before it existed keeps meaning "the whole corpus".
    ///
    /// It exists for the re-fetch walk, which is looking for exactly those and
    /// would otherwise page through the million rows that already have one to
    /// skip them a thousand at a time.
    #[serde(default)]
    pub missing_pb: bool,
    /// The other half: only records that *do* carry one, which is what can be
    /// re-converted without asking Mahjong Soul for anything. Same reasoning as
    /// above — the walk would otherwise page through the 1.6M rows that have no
    /// protobuf to find the 245k that do.
    #[serde(default)]
    pub stored_pb: bool,
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
    #[error("the trend range ends before it starts")]
    RangeInverted,
    #[error("record index is unavailable: {0}")]
    Index(#[from] ClickHouseError),
    #[error("idempotency store is unavailable: {0}")]
    Store(#[from] sqlx::Error),
}

/// Where an idempotency key came from, which decides both what a repeat of it
/// means and how long the claim outlives the request that made it. The two
/// always move together — they are the same question asked twice — so they
/// travel as one value rather than as two booleans a call site could disagree
/// with itself about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyScope {
    /// Derived from the record's own `majsoul.uuid`. It names the game and
    /// promises nothing about bytes, so two renderings of one game are the one
    /// record we already have rather than a conflict.
    ///
    /// The claim never expires, because it is the only thing anywhere that
    /// records that this game has been stored: the uuid is not a column of the
    /// index, so nothing else can be asked. Pruning it does not lose a retry
    /// guard, it loses the answer — and the next import of an archive holding
    /// that game stores it again under a fresh `record_id`, which lands beside
    /// the original rather than on it.
    Game,
    /// Supplied by the caller, scoped by its source. It is a promise that this
    /// key names this content, so a repeat over different bytes is the caller
    /// contradicting itself and earns a `409`. It expires with the window in
    /// which a collector may retry, which is all a key nobody can re-derive
    /// from a record is good for.
    Caller,
}

impl KeyScope {
    fn content_must_match(self) -> bool {
        matches!(self, Self::Caller)
    }

    fn expires(self) -> bool {
        matches!(self, Self::Caller)
    }
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
            .join("\n")
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
        // `expires` is what keeps this from being the thing that silently ends
        // deduplication: a game-scoped claim is the only record anywhere that a
        // game has been stored, so deleting it does not free a retry guard, it
        // discards the answer. See `KeyScope`.
        let idempotency = sqlx::query(
            "DELETE FROM ingest_idempotency
             WHERE expires AND created_at < now() - make_interval(days => $1)",
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
    ///
    /// `scope` says where the key came from, which decides both what a repeat of
    /// it means and whether the claim ever expires. See `KeyScope`.
    ///
    /// A `Game`-scoped claim is not a retry guard. It is the record that this
    /// game has been stored, and the only one there is, because the uuid is not
    /// a column of the index and so nothing else can be asked. That is why
    /// `prune` leaves it alone, and it is the whole of what makes "one game is
    /// one record" outlive the retry window.
    pub async fn claim(
        &self,
        key: &str,
        id: Uuid,
        sha256: &str,
        scope: KeyScope,
    ) -> Result<IdempotencyClaim, CatalogError> {
        let claimed = sqlx::query(
            "INSERT INTO ingest_idempotency (key_hash, record_id, content_sha256, state, expires)
             VALUES (sha256($1), $2, decode($3, 'hex'), 'accepted', $4)
             ON CONFLICT (key_hash) DO NOTHING
             RETURNING record_id",
        )
        .bind(key.as_bytes())
        .bind(id)
        .bind(sha256)
        .bind(scope.expires())
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
        if scope.content_must_match() && row.try_get::<String, _>("content_sha256")? != sha256 {
            return Err(CatalogError::Conflict);
        }
        Ok(IdempotencyClaim::Existing(row.try_get("record_id")?))
    }

    /// Writes permanent game-scoped claims for records that are already in the
    /// index, which is what the `game_scoped_claims` backfill is made of.
    ///
    /// `DO UPDATE` rather than `DO NOTHING`, and that is the point of the whole
    /// pass: the live collector's claims are already stored under exactly these
    /// keys, but as expiring ones, so ignoring the conflict would leave the
    /// entire collected corpus scheduled for deletion thirty days after it was
    /// gathered. The existing `record_id` is deliberately left alone — whichever
    /// request stored the game first is the one that owns it, and a second row
    /// for one game means the index already holds a duplicate this pass is not
    /// there to resolve.
    ///
    /// Keys are deduplicated by the caller, because PostgreSQL refuses a
    /// statement whose `ON CONFLICT DO UPDATE` would touch one row twice, and
    /// two rows in the index carrying one game is exactly the state that
    /// happens in.
    pub async fn adopt_game_claims(
        &self,
        claims: &[(String, Uuid, String)],
    ) -> Result<u64, CatalogError> {
        if claims.is_empty() {
            return Ok(0);
        }
        let keys: Vec<&[u8]> = claims.iter().map(|(key, _, _)| key.as_bytes()).collect();
        let ids: Vec<Uuid> = claims.iter().map(|(_, id, _)| *id).collect();
        let hashes: Vec<&str> = claims.iter().map(|(_, _, sha)| sha.as_str()).collect();
        let written = sqlx::query(
            "INSERT INTO ingest_idempotency (key_hash, record_id, content_sha256, state, expires)
             SELECT sha256(k), r, decode(c, 'hex'), 'indexed', false
             FROM unnest($1::bytea[], $2::uuid[], $3::text[]) AS t(k, r, c)
             ON CONFLICT (key_hash) DO UPDATE SET expires = false, updated_at = now()",
        )
        .bind(&keys)
        .bind(&ids)
        .bind(&hashes)
        .execute(&self.postgres)
        .await?;
        Ok(written.rows_affected())
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
        // Reads the same "no protobuf" as `PbLocation::from_columns`: a stored
        // entry always has a non-zero size, because a record that was converted
        // from one has bytes. Correct only under the FINAL above — a record that
        // has since been re-fetched still has its old zero-sized row in an
        // unmerged part, and without FINAL the walk would keep finding it.
        if filter.missing_pb {
            sql.push_str(" AND pb_size = 0");
        }
        if filter.stored_pb {
            sql.push_str(" AND pb_size > 0");
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

    /// How many indexed records carry no Mahjong Soul protobuf: the size of the
    /// re-fetch backlog.
    ///
    /// FINAL, unlike every count in [`Self::stats`]. A record the re-fetch pass
    /// has already replaced keeps its old zero-sized row until the parts merge,
    /// and a backlog figure that counts finished work as outstanding would never
    /// reach zero. Affordable because a re-fetch run asks once at the start, not
    /// on every console poll.
    pub async fn count_missing_pb(&self) -> Result<u64, CatalogError> {
        #[derive(Default, Deserialize)]
        struct Missing {
            missing: u64,
        }
        let sql = format!("SELECT count() AS missing FROM {RECORDS_TABLE} FINAL WHERE pb_size = 0");
        Ok(self
            .index
            .query::<Missing>(&sql, &[])
            .await?
            .into_iter()
            .next()
            .unwrap_or_default()
            .missing)
    }

    /// One row per day for the console's trend charts, gap-filled so a caller
    /// never has to do date arithmetic to tell "collected nothing" from "no
    /// bucket". Always exactly `days` points, oldest first, ending today.
    ///
    /// Two groupings rather than one, because the two questions have different
    /// answers: `records` and the byte sums bucket by `received_at` — when the
    /// index learned of a record — while `games` buckets by `played_at`, when
    /// the hand was actually dealt. The historical import is why both are here
    /// and neither can stand in for the other: it landed six days of play in a
    /// single afternoon, so a chart drawn on `received_at` alone is one spike
    /// with nothing either side of it, and one drawn on `played_at` alone never
    /// shows that an import happened at all.
    pub async fn series(
        &self,
        window: SeriesWindow,
        filter: &RuleFilter,
    ) -> Result<Series, CatalogError> {
        let SeriesWindow { unit, first, span } = window;
        #[derive(Deserialize)]
        struct ReceivedBucket {
            bucket: String,
            records: u64,
            raw_bytes: u64,
            compressed_bytes: u64,
        }
        #[derive(Deserialize)]
        struct PlayedBucket {
            bucket: String,
            games: u64,
        }
        // Both bounds are decided once, by `SeriesWindow`, and sent to all
        // three statements rather than letting each call `now()`: two
        // evaluations either side of a bucket boundary would cover two
        // different ranges, and the merge below would silently drop the bucket
        // they disagreed about.
        let params = [("start", unit.key(first)), ("end", unit.key(window.end()))];
        let bucket = unit.clickhouse_fn();
        // One clause, three statements. The mode is a property of the record,
        // not of the axis, so the filter has to reach the arrival series and the
        // byte sums as well — a page filtered to 三麻 that still charts every
        // record's bytes is showing two different populations side by side.
        let modes = filter.predicate();
        // Bounded by the timestamp rather than by the date, because an hourly
        // window is not a whole number of days. `toDate(received_at)` leads the
        // sorting key and is monotonic in it, so a bound on `received_at` still
        // prunes to the days it can touch.
        let received_sql = format!(
            "SELECT toString({bucket}(received_at)) AS bucket, count() AS records, \
             sum(raw_size) AS raw_bytes, sum(compressed_size) AS compressed_bytes \
             FROM {RECORDS_TABLE} WHERE received_at >= toDateTime({{start:String}}, 'UTC') \
             AND received_at < toDateTime({{end:String}}, 'UTC'){modes} \
             GROUP BY bucket"
        );
        // `played_at` is in no key, so these two read one column of every part —
        // a game played inside the window may have been received at any time,
        // which is exactly what makes the chart worth drawing and also what
        // stops the partition key from helping. Eight bytes a row against a
        // corpus measured in tens of gigabytes; if it ever matters, the answer
        // is a rollup maintained on insert, not a narrower window. NULL compares
        // as NULL and drops out, which is what a record with no start time
        // contributes to a chart of play times: nothing.
        let played_sql = format!(
            "SELECT toString({bucket}(played_at)) AS bucket, count() AS games \
             FROM {RECORDS_TABLE} WHERE played_at >= toDateTime({{start:String}}, 'UTC') \
             AND played_at < toDateTime({{end:String}}, 'UTC'){modes} \
             GROUP BY bucket"
        );
        // Grouped on the same rows as `games`, so the breakdown sums to the
        // series beside it. `rule` is LowCardinality, so this groups over a
        // dictionary rather than over the strings; the empty rule is kept rather
        // than filtered, because a panel that silently drops the records whose
        // mode the converter could not name is a panel whose total is wrong.
        let rules_sql = format!(
            "SELECT rule, count() AS games FROM {RECORDS_TABLE} \
             WHERE played_at >= toDateTime({{start:String}}, 'UTC') \
             AND played_at < toDateTime({{end:String}}, 'UTC'){modes} \
             GROUP BY rule ORDER BY games DESC LIMIT {MAX_REPORTED_RULES}"
        );
        // Counted without FINAL, like `stats` and for the same reason: a
        // replayed insert batch is double-counted only until the parts merge,
        // and a chart is the wrong place to pay for a collapse of the whole
        // table on every poll.
        let (received, played, rules) = tokio::try_join!(
            self.index.query::<ReceivedBucket>(&received_sql, &params),
            self.index.query::<PlayedBucket>(&played_sql, &params),
            self.index.query::<RuleCount>(&rules_sql, &params),
        )?;
        let received: HashMap<String, ReceivedBucket> = received
            .into_iter()
            .map(|bucket| (bucket.bucket.clone(), bucket))
            .collect();
        let played: HashMap<String, u64> = played
            .into_iter()
            .map(|bucket| (bucket.bucket, bucket.games))
            .collect();
        // Keyed by the bucket string on both sides, not zipped: the statements
        // return only the buckets they have rows for, so position means nothing
        // and a zip would shift one series against the other.
        let points = (0..span)
            .map(|offset| {
                let at = first + unit.step() * offset as i32;
                let key = unit.key(at);
                let bucket = received.get(&key);
                SeriesPoint {
                    at: unit.label(at),
                    records: bucket.map_or(0, |bucket| bucket.records),
                    raw_bytes: bucket.map_or(0, |bucket| bucket.raw_bytes),
                    compressed_bytes: bucket.map_or(0, |bucket| bucket.compressed_bytes),
                    games: played.get(&key).copied().unwrap_or(0),
                }
            })
            .collect();
        Ok(Series {
            unit,
            points,
            rules,
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
        // Zero for a record that has no protobuf, which is what the column
        // default already says; written explicitly because a partial column
        // list would make every insert here disagree with `RECORD_COLUMNS`.
        "pb_offset": record.majsoul_pb.as_ref().map_or(0, |pb| pb.offset),
        "pb_compressed_size": record
            .majsoul_pb
            .as_ref()
            .map_or(0, |pb| pb.compressed_size),
        "pb_size": record.majsoul_pb.as_ref().map_or(0, |pb| pb.raw_size),
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
    pb_offset: u64,
    pb_compressed_size: u32,
    pb_size: u32,
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
            majsoul_pb: PbLocation::from_columns(
                row.pb_offset,
                row.pb_compressed_size,
                row.pb_size,
            ),
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

/// How wide one point of a trend chart is. Everything that differs between the
/// two granularities lives on this type rather than in `if` arms scattered
/// through the query: the ceiling, the ClickHouse bucketing function, the string
/// the two sides join on, and the string the console is handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SeriesUnit {
    Hour,
    Day,
}

impl SeriesUnit {
    fn max_span(self) -> u32 {
        match self {
            Self::Hour => MAX_TREND_HOURS,
            Self::Day => MAX_TREND_DAYS,
        }
    }

    fn step(self) -> TimeDelta {
        match self {
            Self::Hour => TimeDelta::hours(1),
            Self::Day => TimeDelta::days(1),
        }
    }

    fn clickhouse_fn(self) -> &'static str {
        match self {
            Self::Hour => "toStartOfHour",
            Self::Day => "toDate",
        }
    }

    /// The start of the bucket `at` falls in. Every window ends on a whole
    /// bucket, so the last bar is the one still being filled rather than a
    /// sliver of one.
    fn truncate(self, at: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Self::Hour => at
                .with_minute(0)
                .and_then(|at| at.with_second(0))
                .unwrap_or(at),
            Self::Day => at
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .map_or(at, |at| at.and_utc()),
        }
        .trunc_subsecs(0)
    }

    /// What ClickHouse prints for `toString(bucket(column))`, and therefore the
    /// only string the merge may join on. Rust has to reproduce it exactly: a
    /// format that differs by so much as the `T` would leave every bucket
    /// looking empty, with no error anywhere.
    fn key(self, at: DateTime<Utc>) -> String {
        match self {
            Self::Hour => at.format("%Y-%m-%d %H:%M:%S").to_string(),
            Self::Day => at.format("%Y-%m-%d").to_string(),
        }
    }

    /// What the console gets. RFC 3339 for an hour so the browser can render it
    /// in the reader's own timezone — a bar labelled 05:00 to someone whose
    /// clock says 13:00 is worse than no label. A day stays a bare date,
    /// because parsing that as an instant is what shifts it across midnight.
    fn label(self, at: DateTime<Utc>) -> String {
        match self {
            Self::Hour => at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            Self::Day => at.format("%Y-%m-%d").to_string(),
        }
    }
}

/// Which buckets a request covers: a granularity, the first bucket, and how
/// many of them. Built either from a span ending with the bucket in progress —
/// what the preset buttons ask for — or from an explicit range, and clamped to
/// the unit's ceiling either way. Kept as one value so that `series` is handed
/// a decided window rather than deciding one from three optional parameters.
#[derive(Clone, Copy, Debug)]
pub struct SeriesWindow {
    unit: SeriesUnit,
    first: DateTime<Utc>,
    span: u32,
}

impl SeriesWindow {
    /// The `span` buckets ending with the one in progress.
    pub fn recent(unit: SeriesUnit, span: u32) -> Self {
        let span = span.clamp(1, unit.max_span());
        let last = unit.truncate(Utc::now());
        Self {
            unit,
            first: last - unit.step() * (span as i32 - 1),
            span,
        }
    }

    /// Every bucket from the one `from` falls in through the one `to` falls in,
    /// both ends included — a range picked as two calendar days covers both of
    /// those days rather than the gap between them.
    ///
    /// A range wider than the unit allows keeps its end and loses its start.
    /// Clamped rather than refused, like the span: somebody who asked for two
    /// years of days is going to read the recent end first, and the timestamps
    /// on the points say what they actually got.
    pub fn between(
        unit: SeriesUnit,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Self, CatalogError> {
        if to < from {
            return Err(CatalogError::RangeInverted);
        }
        let last = unit.truncate(to);
        let wanted = (last - unit.truncate(from)).num_seconds() / unit.step().num_seconds() + 1;
        let span = wanted.clamp(1, i64::from(unit.max_span())) as u32;
        Ok(Self {
            unit,
            first: last - unit.step() * (span as i32 - 1),
            span,
        })
    }

    /// One past the last bucket, which is the exclusive upper bound the
    /// statements need. Without it a window that ends in the past would still
    /// scan every part up to today and throw the rows away in the merge.
    fn end(&self) -> DateTime<Utc> {
        self.first + self.unit.step() * self.span as i32
    }

    /// The half-open `[start, end)` this window covers, as the strings
    /// ClickHouse is given. One place builds them, so a query that bounds by
    /// this window cannot spell the bucket key differently from the one that
    /// groups by it.
    fn bounds(&self) -> [(&'static str, String); 2] {
        [
            ("start", self.unit.key(self.first)),
            ("end", self.unit.key(self.end())),
        ]
    }
}

/// One bucket of the console's trend charts. `records` and the byte sums are
/// what arrived in it, `games` is what was played in it; see `Catalog::series`
/// for why those are not the same series.
#[derive(Clone, Debug, Serialize)]
pub struct SeriesPoint {
    /// `YYYY-MM-DD` for a day, RFC 3339 for an hour. UTC either way.
    pub at: String,
    pub records: u64,
    pub games: u64,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

/// The three independent parts of a `{players}p-{room}-{length}` rule token.
/// They are enums rather than strings on purpose: `RuleFilter` builds the token
/// list that reaches the `IN` clause by formatting these variants, so the set of
/// strings that can ever be spliced into that statement is fixed at compile
/// time and no caller-supplied byte reaches it. The API layer parses into them
/// and rejects anything else.
/// Lets `RuleFilter::predicate` write one clause builder for all three facets.
trait TokenOf {
    fn token(self) -> &'static str;
}

macro_rules! rule_facet {
    ($name:ident { $($variant:ident => $token:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
        }

        impl TokenOf for $name {
            fn token(self) -> &'static str {
                match self {
                    $(Self::$variant => $token),+
                }
            }
        }

        impl FromStr for $name {
            type Err = ();

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($token => Ok(Self::$variant),)+
                    _ => Err(()),
                }
            }
        }
    };
}

rule_facet!(RulePlayers { Three => "3p", Four => "4p" });
rule_facet!(RuleRoom { Gold => "gold", Jade => "jade", Throne => "throne" });
rule_facet!(RuleLength { East => "east", South => "south" });

/// Which modes a trend window covers. Each facet is a set, and an empty one
/// means "all of it".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuleFilter {
    pub players: Vec<RulePlayers>,
    pub rooms: Vec<RuleRoom>,
    pub lengths: Vec<RuleLength>,
}

impl RuleFilter {
    pub fn is_empty(&self) -> bool {
        self.players.is_empty() && self.rooms.is_empty() && self.lengths.is_empty()
    }

    /// The `AND` clause this filter adds, empty when it admits everything.
    ///
    /// "Everything" has to mean *no predicate*, not "every token I know about":
    /// `rule` holds whatever a collector's converter wrote, so a record with a
    /// thirteenth value or none at all belongs in an unfiltered chart. It
    /// necessarily leaves a filtered one — constraining any facet is a
    /// statement about a token this one does not have.
    ///
    /// One clause per constrained facet, matched against that facet's position
    /// in the token, rather than one `IN` list of the whole cross product. Two
    /// reasons, and the first is correctness: a cross product can only name
    /// tokens this binary already knows, so `players=4p` over a record whose
    /// converter wrote `4p-throne-hanchan` would drop it — and that record *is*
    /// four player, which is the only thing the caller asked about. Matching the
    /// prefix keeps it. The second is size: the clause is now the sum of the
    /// chosen values rather than their product, so it cannot be inflated by a
    /// caller repeating one.
    fn predicate(&self) -> String {
        /// Canonical, deduplicated, and empty when the facet is unconstrained —
        /// which is what makes an unconstrained facet add no clause rather than
        /// a clause listing everything. Filtered through `ALL` rather than
        /// deduplicated in place so that `?players=3p,3p,3p…` costs one term,
        /// not one per repeat.
        fn selected<T: Copy + PartialEq>(chosen: &[T], all: &'static [T]) -> Vec<T> {
            if chosen.is_empty() {
                return Vec::new();
            }
            all.iter()
                .copied()
                .filter(|value| chosen.contains(value))
                .collect()
        }
        // Every byte of every clause comes from the `token()` arms, so the
        // quoting cannot be escaped by anything a caller sends: a caller picks
        // *which* tokens are named, never what they say.
        fn any_of<T: Copy + TokenOf>(
            values: &[T],
            test: impl Fn(&'static str) -> String,
        ) -> Option<String> {
            let terms: Vec<String> = values.iter().map(|value| test(value.token())).collect();
            match terms.len() {
                0 => None,
                1 => terms.into_iter().next(),
                _ => Some(format!("({})", terms.join(" OR "))),
            }
        }
        // `{players}p-{room}-{length}`: the player count is the prefix, the
        // length is the suffix, and the room is the part between two dashes.
        let clauses: Vec<String> = [
            any_of(&selected(&self.players, RulePlayers::ALL), |token| {
                format!("startsWith(rule, '{token}-')")
            }),
            any_of(&selected(&self.rooms, RuleRoom::ALL), |token| {
                format!("position(rule, '-{token}-') > 0")
            }),
            any_of(&selected(&self.lengths, RuleLength::ALL), |token| {
                format!("endsWith(rule, '-{token}')")
            }),
        ]
        .into_iter()
        .flatten()
        .collect();
        if clauses.is_empty() {
            String::new()
        } else {
            format!(" AND {}", clauses.join(" AND "))
        }
    }
}

/// Games in the window by the mode they were played in, busiest first.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuleCount {
    /// One of the twelve `{players}p-{room}-{length}` tokens, or empty where the
    /// converter left no mode on the record.
    pub rule: String,
    pub games: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Series {
    pub unit: SeriesUnit,
    /// Gap-filled: always exactly `span` entries, oldest first.
    pub points: Vec<SeriesPoint>,
    /// Counted over the same rows as `points[].games`, so the two agree.
    pub rules: Vec<RuleCount>,
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

    /// Three rules the catalogue page depends on, none of which announces
    /// itself when it is broken.
    ///
    /// The alias is the sharp one and it was reproduced rather than reasoned
    /// about: ClickHouse binds a bare identifier in `WHERE` to a `SELECT` alias
    /// before it binds it to the column of the same name, so aliasing the
    /// millisecond conversion onto `started_at` compares milliseconds with
    /// seconds — true for every row — and the walk returns page one for ever
    /// while reporting the catalogue swept.
    #[test]
    fn the_catalogue_page_keeps_its_index_and_does_not_shadow_its_own_column() {
        let paged = game_uuid_listings_sql(true);
        assert!(
            paged.contains("AS started_ms") && !paged.contains("AS started_at"),
            "the conversion must not be aliased onto the column it reads: {paged}"
        );
        // Redundant to the meaning, required for the cost: ClickHouse has no
        // primary-key condition for `greater(tuple, const)`, so without the
        // scalar every page reads the table from the beginning.
        assert!(
            paged.contains("started_at >= fromUnixTimestamp64Milli({after_ms:Int64})"),
            "the keyset tuple alone does not prune the primary key: {paged}"
        );
        assert!(
            paged.contains("(started_at, uuid) >"),
            "the scalar alone would skip or repeat games sharing one second: {paged}"
        );
        // FINAL does not stop early for a LIMIT — it reads the rest of the key
        // range — which at half a billion rows is the whole table, once a page.
        for sql in [paged.as_str(), &game_uuid_listings_sql(false)] {
            assert!(!sql.contains("FINAL"), "{sql}");
            assert!(sql.contains("ORDER BY started_at, uuid"), "{sql}");
        }
        // The first page has no cursor and therefore no predicate at all.
        assert!(!game_uuid_listings_sql(false).contains("WHERE"));
    }

    #[test]
    fn an_unconstrained_mode_filter_adds_no_clause() {
        // Not "every token I know about": a record whose converter wrote a mode
        // this binary has never heard of belongs in an unfiltered chart, and
        // only a filter with no predicate at all keeps it.
        assert_eq!(RuleFilter::default().predicate(), "");
    }

    #[test]
    fn a_mode_filter_constrains_only_the_facets_it_was_given() {
        let players_only = RuleFilter {
            players: vec![RulePlayers::Four],
            ..RuleFilter::default()
        };
        assert_eq!(
            players_only.predicate(),
            " AND startsWith(rule, '4p-')",
            "an unconstrained facet must not contribute a clause"
        );

        let every_facet = RuleFilter {
            players: vec![RulePlayers::Three],
            rooms: vec![RuleRoom::Jade],
            lengths: vec![RuleLength::East],
        };
        assert_eq!(
            every_facet.predicate(),
            " AND startsWith(rule, '3p-') AND position(rule, '-jade-') > 0 \
             AND endsWith(rule, '-east')"
                .replace("             ", "")
        );
    }

    #[test]
    fn a_mode_filter_is_the_sum_of_its_values_and_never_the_product() {
        // The clause is built from the facets separately, so a caller repeating
        // one value cannot inflate it. Two rooms is two terms whatever else is
        // chosen — the earlier cross-product form turned 2,547 repeats of `3p`
        // into a quarter of a megabyte of SQL and a 500 from ClickHouse.
        let noisy = RuleFilter {
            players: vec![RulePlayers::Three; 2_000],
            rooms: vec![RuleRoom::Throne, RuleRoom::Gold, RuleRoom::Throne],
            ..RuleFilter::default()
        };
        let predicate = noisy.predicate();
        assert_eq!(
            predicate,
            " AND startsWith(rule, '3p-') \
             AND (position(rule, '-gold-') > 0 OR position(rule, '-throne-') > 0)"
                .replace("             ", ""),
            "repeats must collapse and the order must be the declared one"
        );
        assert!(predicate.len() < 200, "{} bytes", predicate.len());
    }

    #[test]
    fn a_mode_filter_quotes_nothing_a_caller_supplied() {
        // The whole safety argument for formatting this into SQL: every literal
        // comes from a `token()` arm. If a facet ever gains a value carrying a
        // quote or a space, this fails rather than shipping an injection.
        for token in RulePlayers::ALL
            .iter()
            .map(|value| value.token())
            .chain(RuleRoom::ALL.iter().map(|value| value.token()))
            .chain(RuleLength::ALL.iter().map(|value| value.token()))
        {
            assert!(
                token
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric()),
                "{token} is not a bare alphanumeric token"
            );
        }
    }

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

// ---------------------------------------------------------------------------
// Per-player statistics
// ---------------------------------------------------------------------------

const PLAYER_GAMES_TABLE: &str = "mjai.player_games";

/// How many months' worth of seat rows may travel in one insert.
///
/// ClickHouse's `max_partitions_per_insert_block` defaults to a hundred and
/// `player_games` is partitioned by month, so this is that limit with room to
/// spare rather than a tuning choice.
const MAX_MONTHS_PER_INSERT: usize = 64;

/// One seat of one game as it is stored. Flat because it is one row: the shape
/// `replay` returns is grouped by game, and this is that shape crossed with the
/// record's own identity and filters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlayerGame {
    pub record_id: Uuid,
    pub played_at: DateTime<Utc>,
    pub rule: Option<String>,
    pub seats: u8,
    pub detailed: bool,
    pub stats: crate::replay::SeatStats,
}

impl PlayerGame {
    /// Every seat of one replayed game, stamped with what the index knows about
    /// the record it came from.
    ///
    /// `played_at` falls back to the ingest time: it is the partition key of
    /// `player_games`, and a record with no `majsoul.start_time` — any mjai log
    /// that did not come from the converter — would otherwise have nowhere to
    /// go. Seats with no name are dropped rather than stored under the empty
    /// string, which would collect every anonymous seat in the corpus into one
    /// player.
    pub fn of(record: &Record, game: crate::replay::GameStats) -> Vec<Self> {
        let played_at = record.played_at.unwrap_or(record.received_at);
        game.players
            .into_iter()
            .filter(|stats| !stats.player.is_empty())
            .map(|stats| Self {
                record_id: record.id,
                played_at: played_at.trunc_subsecs(3),
                rule: record.rule.clone(),
                seats: game.seats,
                detailed: game.detailed,
                stats,
            })
            .collect()
    }
}

/// What a filtered set of `player_games` rows sums to. Ratios are left to the
/// caller so that the denominators stay visible: `hands` for most of them,
/// `detailed_games` for the four counters an older record cannot fill in.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PlayerSummary {
    pub games: u64,
    pub detailed_games: u64,
    pub hands: u64,
    pub hands_as_dealer: u64,
    pub max_dealer_streak: u64,
    pub net_points: i64,
    pub placements: Vec<u64>,
    pub busted: u64,
    pub final_score: i64,
    pub settled_point: i64,
    pub grading_score: i64,
    pub wins: u64,
    pub wins_tsumo: u64,
    pub win_points: i64,
    pub win_turns: u64,
    pub deal_ins: u64,
    pub deal_in_points: i64,
    pub riichi: u64,
    pub riichi_wins: u64,
    pub riichi_deal_ins: u64,
    pub riichi_turns: u64,
    pub riichi_first: u64,
    pub riichi_chasing: u64,
    pub riichi_chased: u64,
    pub riichi_net: i64,
    pub called: u64,
    pub called_wins: u64,
    pub draws: u64,
    pub draws_tenpai: u64,
    pub riichi_ippatsu: u64,
    pub riichi_ura_hits: u64,
    pub yakuman: u64,
    pub max_han: u64,
}

/// Seat rows split so that no insert block spans more months than ClickHouse
/// will accept.
///
/// `player_games` is partitioned by `toYYYYMM(played_at)` and ClickHouse refuses
/// an insert block touching more than `max_partitions_per_insert_block`
/// partitions — a hundred by default. Nothing written so far has come close,
/// because everything was collected as it was played and one pack spans an hour
/// at most. A pass that fetches games *by uuid* has no such property: a page of
/// a thousand can be spread over every month Mahjong Soul has existed, which is
/// already about eighty and grows by twelve a year. The whole block would be
/// rejected, and the records would land with no seat rows behind them.
///
/// Grouped by month rather than chunked at a fixed row count, so each insert
/// carries whole partitions.
fn month_blocks(games: &[PlayerGame]) -> Vec<String> {
    let mut by_month: BTreeMap<(i32, u32), Vec<String>> = BTreeMap::new();
    for game in games {
        by_month
            .entry((game.played_at.year(), game.played_at.month()))
            .or_default()
            .push(player_game_json(game));
    }
    let mut blocks = Vec::new();
    let mut batch: Vec<String> = Vec::with_capacity(games.len());
    let mut months = 0usize;
    for rows in by_month.into_values() {
        batch.extend(rows);
        months += 1;
        if months == MAX_MONTHS_PER_INSERT {
            blocks.push(std::mem::take(&mut batch).join("\n"));
            months = 0;
        }
    }
    if !batch.is_empty() {
        blocks.push(batch.join("\n"));
    }
    blocks
}

#[cfg(test)]
mod player_game_batching_tests {
    use super::*;

    fn seat_row(played_at: &str) -> PlayerGame {
        PlayerGame {
            record_id: Uuid::new_v4(),
            played_at: played_at.parse().unwrap(),
            rule: Some("4p-jade-south".into()),
            seats: 4,
            detailed: true,
            stats: crate::replay::SeatStats::default(),
        }
    }

    #[test]
    fn a_page_spanning_years_is_split_and_loses_no_row() {
        let months: Vec<PlayerGame> = (0..8)
            .flat_map(|year| {
                (1..=12u32).map(move |month| {
                    seat_row(&format!("20{:02}-{month:02}-01T00:00:00Z", 19 + year))
                })
            })
            .collect();
        assert_eq!(months.len(), 96, "eight years of monthly partitions");

        let blocks = month_blocks(&months);
        assert!(blocks.len() > 1, "96 months cannot travel in one block");
        let lines: usize = blocks.iter().map(|block| block.lines().count()).sum();
        assert_eq!(lines, 96, "no seat row is dropped by the split");

        // The ordinary case — a pack of games collected as they were played —
        // still goes in one statement.
        let live: Vec<PlayerGame> = (1..=4)
            .map(|seat| seat_row(&format!("2026-08-04T0{seat}:00:00Z")))
            .collect();
        assert_eq!(month_blocks(&live).len(), 1);
        assert!(
            month_blocks(&[]).is_empty(),
            "nothing to write, nothing sent"
        );
    }
}

/// A name and how many games it appears in, for the search box.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlayerHit {
    pub player: String,
    pub games: u64,
}

impl Catalog {
    /// Writes seat rows, split so that no single insert block spans more months
    /// than ClickHouse will accept.
    ///
    /// `player_games` is partitioned by `toYYYYMM(played_at)` and ClickHouse
    /// refuses an insert block touching more than `max_partitions_per_insert_block`
    /// partitions — a hundred by default. Nothing has ever come close, because
    /// everything written so far was collected as it was played and a pack
    /// spans an hour at most. A pass that fetches games *by uuid* has no such
    /// property: a page of a thousand of them can be spread over every month
    /// Mahjong Soul has existed, which is already about eighty and grows by
    /// twelve a year. The whole page would be rejected, and the records
    /// themselves would land with no seat rows behind them.
    ///
    /// Grouped rather than chunked at a fixed size, so each insert carries whole
    /// months and a page that does span a hundred of them becomes a handful of
    /// statements instead of one failure.
    pub async fn insert_player_games(&self, games: &[PlayerGame]) -> Result<(), CatalogError> {
        for block in month_blocks(games) {
            self.index.insert(PLAYER_GAMES_TABLE, block).await?;
        }
        Ok(())
    }

    /// Players whose name contains `query`, busiest first.
    ///
    /// A substring match rather than a prefix, because a Mahjong Soul nickname
    /// is as likely to be recognised by its middle as by its start, and because
    /// neither can use the sorting key: `player` leads it, but a `LIKE '%x%'`
    /// prunes nothing. This is a scan of one column, which is what the bloom
    /// filter cannot help with and what the column store makes affordable.
    pub async fn search_players(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<PlayerHit>, CatalogError> {
        #[derive(Deserialize)]
        struct Row {
            player: String,
            games: u64,
        }
        let rows: Vec<Row> = self
            .index
            .query(
                &format!(
                    "SELECT player, uniqExact(record_id) AS games FROM {PLAYER_GAMES_TABLE} \
                     WHERE positionCaseInsensitiveUTF8(player, {{query:String}}) > 0 \
                     GROUP BY player ORDER BY games DESC, player ASC LIMIT {}",
                    limit.clamp(1, MAX_PLAYER_HITS)
                ),
                &[("query", query.to_owned())],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| PlayerHit {
                player: row.player,
                games: row.games,
            })
            .collect())
    }

    /// Sums one player's rows over a window and a mode filter.
    ///
    /// No FINAL, the same as every other count here: a replayed insert converges
    /// through ReplacingMergeTree, and within the merge window a counter can
    /// read high. The alternative costs a full merge on every page load.
    pub async fn player_summary(
        &self,
        player: &str,
        window: SeriesWindow,
        filter: &RuleFilter,
    ) -> Result<PlayerSummary, CatalogError> {
        let modes = filter.predicate();
        let sql = format!(
            "SELECT count() AS games, sum(detailed) AS detailed_games, \
             sum(hands) AS hands, sum(hands_as_dealer) AS hands_as_dealer, \
             max(max_dealer_streak) AS max_dealer_streak, sum(net_points) AS net_points, \
             countIf(placement = 1) AS first, countIf(placement = 2) AS second, \
             countIf(placement = 3) AS third, countIf(placement = 4) AS fourth, \
             sum(busted) AS busted, sum(final_score) AS final_score, \
             sum(settled_point) AS settled_point, sum(grading_score) AS grading_score, \
             sum(wins) AS wins, sum(wins_tsumo) AS wins_tsumo, sum(win_points) AS win_points, \
             sum(win_turns) AS win_turns, sum(deal_ins) AS deal_ins, \
             sum(deal_in_points) AS deal_in_points, sum(riichi) AS riichi, \
             sum(riichi_wins) AS riichi_wins, sum(riichi_deal_ins) AS riichi_deal_ins, \
             sum(riichi_turns) AS riichi_turns, sum(riichi_first) AS riichi_first, \
             sum(riichi_chasing) AS riichi_chasing, sum(riichi_chased) AS riichi_chased, \
             sum(riichi_net) AS riichi_net, sum(called) AS called, \
             sum(called_wins) AS called_wins, sum(draws) AS draws, \
             sum(draws_tenpai) AS draws_tenpai, sum(riichi_ippatsu) AS riichi_ippatsu, \
             sum(riichi_ura_hits) AS riichi_ura_hits, sum(yakuman) AS yakuman, \
             max(max_han) AS max_han \
             FROM {PLAYER_GAMES_TABLE} \
             WHERE player = {{player:String}} \
             AND played_at >= toDateTime({{start:String}}, 'UTC') \
             AND played_at < toDateTime({{end:String}}, 'UTC'){modes}"
        );
        #[derive(Default, Deserialize)]
        struct Row {
            games: u64,
            detailed_games: u64,
            hands: u64,
            hands_as_dealer: u64,
            max_dealer_streak: u64,
            net_points: i64,
            first: u64,
            second: u64,
            third: u64,
            fourth: u64,
            busted: u64,
            final_score: i64,
            settled_point: i64,
            grading_score: i64,
            wins: u64,
            wins_tsumo: u64,
            win_points: i64,
            win_turns: u64,
            deal_ins: u64,
            deal_in_points: i64,
            riichi: u64,
            riichi_wins: u64,
            riichi_deal_ins: u64,
            riichi_turns: u64,
            riichi_first: u64,
            riichi_chasing: u64,
            riichi_chased: u64,
            riichi_net: i64,
            called: u64,
            called_wins: u64,
            draws: u64,
            draws_tenpai: u64,
            riichi_ippatsu: u64,
            riichi_ura_hits: u64,
            yakuman: u64,
            max_han: u64,
        }
        let rows: Vec<Row> = self
            .index
            .query(&sql, &{
                let mut params = vec![("player", player.to_owned())];
                params.extend(window.bounds());
                params
            })
            .await?;
        let row = rows.into_iter().next().unwrap_or_default();
        Ok(PlayerSummary {
            games: row.games,
            detailed_games: row.detailed_games,
            hands: row.hands,
            hands_as_dealer: row.hands_as_dealer,
            max_dealer_streak: row.max_dealer_streak,
            net_points: row.net_points,
            placements: vec![row.first, row.second, row.third, row.fourth],
            busted: row.busted,
            final_score: row.final_score,
            settled_point: row.settled_point,
            grading_score: row.grading_score,
            wins: row.wins,
            wins_tsumo: row.wins_tsumo,
            win_points: row.win_points,
            win_turns: row.win_turns,
            deal_ins: row.deal_ins,
            deal_in_points: row.deal_in_points,
            riichi: row.riichi,
            riichi_wins: row.riichi_wins,
            riichi_deal_ins: row.riichi_deal_ins,
            riichi_turns: row.riichi_turns,
            riichi_first: row.riichi_first,
            riichi_chasing: row.riichi_chasing,
            riichi_chased: row.riichi_chased,
            riichi_net: row.riichi_net,
            called: row.called,
            called_wins: row.called_wins,
            draws: row.draws,
            draws_tenpai: row.draws_tenpai,
            riichi_ippatsu: row.riichi_ippatsu,
            riichi_ura_hits: row.riichi_ura_hits,
            yakuman: row.yakuman,
            max_han: row.max_han,
        })
    }

    /// Records what 牌谱屋 says exists. Idempotent: the table collapses on
    /// `uuid` within a `started_at`, so re-syncing a window costs storage until
    /// the next merge and nothing else.
    pub async fn insert_paipuya_games(&self, games: &[PaipuyaGame]) -> Result<(), CatalogError> {
        if games.is_empty() {
            return Ok(());
        }
        let rows: Vec<String> = games
            .iter()
            .map(|game| {
                serde_json::json!({
                    "uuid": game.uuid,
                    "mode_id": game.mode_id,
                    "started_at": game.started_at.timestamp_millis(),
                    "ended_at": game.ended_at.timestamp_millis(),
                    "players": game.players,
                    "account_ids": game.account_ids,
                    "scores": game.scores,
                })
                .to_string()
            })
            .collect();
        self.index.insert(PAIPUYA_TABLE, rows.join("\n")).await?;
        Ok(())
    }

    /// Adds full game uuids to the re-fetch walk's work list.
    ///
    /// Re-importing an overlapping range is expected — the enumerator resumes by
    /// date range — and a repeat collapses at the next merge, the same way the
    /// catalogue's own page-boundary duplicate does.
    pub async fn insert_game_uuids(&self, games: &[GameUuid]) -> Result<(), CatalogError> {
        if games.is_empty() {
            return Ok(());
        }
        let rows: Vec<String> = games
            .iter()
            .map(|game| {
                serde_json::json!({
                    "uuid": game.uuid,
                    "mode_id": game.mode_id,
                    "started_at": game.started_at.timestamp_millis(),
                })
                .to_string()
            })
            .collect();
        self.index.insert(GAME_UUIDS_TABLE, rows.join("\n")).await?;
        Ok(())
    }

    /// How many games 牌谱屋 lists in a window, and how many of them this corpus
    /// is missing.
    ///
    /// The comparison is the whole point of the window: matching is on the start
    /// time and the set of player names, so both sides can be restricted to the
    /// same range and the corpus side stays a few thousand rows however large
    /// the catalogue is. Fetching a uuid from Mahjong Soul costs a request from
    /// a rate-limited account; answering "do we already have this game" costs
    /// part of one scan, so it is worth doing first for every game rather than
    /// last for the ones that turn out to be duplicates.
    ///
    /// Names are compared sorted, not seat by seat. Both sides claim to be seat
    /// ordered, and a mismatch in that claim would otherwise report the entire
    /// catalogue as missing — a failure that looks exactly like success at
    /// finding work to do.
    pub async fn paipuya_gap(&self, window: SeriesWindow) -> Result<PaipuyaGap, CatalogError> {
        #[derive(Default, Deserialize)]
        struct Row {
            listed: u64,
            missing: u64,
        }
        // `NOT IN` over a tuple, with both sides bounded by the same window.
        // FINAL on the corpus side because a record re-indexed by the re-fetch
        // pool has an older row beside it until the parts merge, and the older
        // row carries the same players and start time — harmless here, but
        // FINAL keeps the count of what is present honest.
        //
        // Bound with `toDateTime({start}, 'UTC')` like every other windowed
        // query in this file, because that is what `SeriesWindow::bounds`
        // actually supplies: it yields `start`/`end` as `YYYY-MM-DD` strings.
        // This asked for `{from:Int64}` millisecond substitutions that nothing
        // ever sent, so ClickHouse answered `UNKNOWN_QUERY_PARAMETER` to every
        // click on the console's comparison card — the one tool the "compare
        // before you fetch" rule is supposed to be read off.
        let sql = format!(
            "SELECT count() AS listed, \
             countIf((toUnixTimestamp(started_at), arraySort(players)) NOT IN ( \
                 SELECT toUnixTimestamp(played_at), arraySort(players) \
                 FROM {RECORDS_TABLE} FINAL \
                 WHERE played_at >= toDateTime({{start:String}}, 'UTC') \
                   AND played_at < toDateTime({{end:String}}, 'UTC') \
             )) AS missing \
             FROM {PAIPUYA_TABLE} FINAL \
             WHERE started_at >= toDateTime({{start:String}}, 'UTC') \
               AND started_at < toDateTime({{end:String}}, 'UTC')"
        );
        let rows: Vec<Row> = self.index.query(&sql, &window.bounds()).await?;
        let row = rows.into_iter().next().unwrap_or_default();
        Ok(PaipuyaGap {
            listed: row.listed,
            missing: row.missing,
        })
    }

    /// How far the 牌谱屋 sync has got for one mode.
    pub async fn paipuya_cursor(
        &self,
        mode_id: i32,
    ) -> Result<Option<DateTime<Utc>>, CatalogError> {
        let row: Option<(DateTime<Utc>,)> =
            sqlx::query_as("SELECT next_from FROM paipuya_cursor WHERE mode_id = $1")
                .bind(mode_id)
                .fetch_optional(&self.postgres)
                .await?;
        Ok(row.map(|row| row.0))
    }

    /// Every mode's bookmark, for the console to show where the sweep is.
    ///
    /// Read from here rather than from the supervisor's own counters because
    /// those are per run: they start empty and are cleared on stop, so a
    /// console asking a stopped deployment "how far did this get" would be told
    /// nothing. This row survives both.
    pub async fn paipuya_cursors(&self) -> Result<Vec<PaipuyaModeCursor>, CatalogError> {
        let rows: Vec<(i32, DateTime<Utc>, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT mode_id, next_from, synced_games, updated_at FROM paipuya_cursor \
             ORDER BY mode_id",
        )
        .fetch_all(&self.postgres)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(mode_id, next_from, synced_games, updated_at)| PaipuyaModeCursor {
                    mode_id,
                    next_from,
                    synced_games: synced_games.max(0) as u64,
                    updated_at,
                },
            )
            .collect())
    }

    /// Moves every mode's bookmark back to `next_from`, so the next sweep walks
    /// the window again. The catalogue itself is untouched — what moves is the
    /// position, not the games.
    ///
    /// An `UPDATE` rather than a `DELETE`, and that is the whole point of the
    /// distinction: `synced_games` counts what this deployment has ever pulled
    /// for that mode, which rewinding does not undo. Dropping the row reset it
    /// to zero, and the console's 目录累计 — which is the sum of these — fell off
    /// a cliff the moment somebody rewound.
    pub async fn rewind_paipuya_cursors(
        &self,
        next_from: DateTime<Utc>,
    ) -> Result<u64, CatalogError> {
        let moved = sqlx::query("UPDATE paipuya_cursor SET next_from = $1, updated_at = now()")
            .bind(next_from)
            .execute(&self.postgres)
            .await?
            .rows_affected();
        Ok(moved)
    }

    /// Moves that bookmark, after the page it describes has landed. `added` is
    /// accumulated rather than replaced, so the row also says how much of the
    /// catalogue this deployment has ever pulled.
    pub async fn set_paipuya_cursor(
        &self,
        mode_id: i32,
        next_from: DateTime<Utc>,
        added: u64,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO paipuya_cursor (mode_id, next_from, synced_games) VALUES ($1, $2, $3) \
             ON CONFLICT (mode_id) DO UPDATE SET next_from = EXCLUDED.next_from, \
             synced_games = paipuya_cursor.synced_games + EXCLUDED.synced_games, \
             updated_at = now()",
        )
        .bind(mode_id)
        .bind(next_from)
        .bind(added as i64)
        .execute(&self.postgres)
        .await?;
        Ok(())
    }

    /// The whole catalogue's size and span, for the console to show without
    /// asking for a window first.
    pub async fn paipuya_totals(&self) -> Result<PaipuyaTotals, CatalogError> {
        self.table_totals(PAIPUYA_TABLE).await
    }

    /// How many game uuids the walk has to work through, and the range they
    /// span. Deliberately not `paipuya_totals`: that counts what 牌谱屋 listed,
    /// and none of those short ids is something Mahjong Soul will serve.
    pub async fn game_uuid_totals(&self) -> Result<PaipuyaTotals, CatalogError> {
        self.table_totals(GAME_UUIDS_TABLE).await
    }

    /// How many uuids are still ahead of a walk resuming from `after` — the
    /// denominator of the sweep's progress bar.
    ///
    /// The predicate is [`GAME_UUIDS_AFTER`], the same string the page reads
    /// with, because a bar drawn against a differently-worded bound would drift
    /// from the walk by exactly the games sharing the cursor's second and no
    /// test would ever notice.
    ///
    /// One count over a key range rather than the table's size: partition
    /// pruning by year and then the primary index leave it reading marks, not
    /// rows, and it is asked once per run.
    pub async fn game_uuids_ahead(
        &self,
        after: Option<&SweepPosition>,
    ) -> Result<u64, CatalogError> {
        #[derive(Default, Deserialize)]
        struct Row {
            games: u64,
        }
        let mut sql = format!("SELECT count() AS games FROM {GAME_UUIDS_TABLE}");
        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(after) = after {
            sql.push_str(GAME_UUIDS_AFTER);
            params.push(("after_ms", after.started_at.timestamp_millis().to_string()));
            params.push(("after_uuid", after.uuid.clone()));
        }
        let rows: Vec<Row> = self.index.query(&sql, &params).await?;
        Ok(rows.into_iter().next().unwrap_or_default().games)
    }

    /// Row count and `started_at` range of one of the two game tables.
    ///
    /// No FINAL: these are headline counts, and the same reasoning `stats`
    /// documents applies — a re-synced or re-imported window reads a few rows
    /// high until the parts merge.
    async fn table_totals(&self, table: &str) -> Result<PaipuyaTotals, CatalogError> {
        #[derive(Default, Deserialize)]
        struct Row {
            games: u64,
            earliest: i64,
            latest: i64,
        }
        let sql = format!(
            "SELECT count() AS games, \
             toUnixTimestamp(min(started_at)) AS earliest, \
             toUnixTimestamp(max(started_at)) AS latest \
             FROM {table}"
        );
        let rows: Vec<Row> = self.index.query(&sql, &[]).await?;
        let row = rows.into_iter().next().unwrap_or_default();
        Ok(PaipuyaTotals {
            games: row.games,
            earliest: (row.games > 0)
                .then(|| DateTime::from_timestamp(row.earliest, 0))
                .flatten(),
            latest: (row.games > 0)
                .then(|| DateTime::from_timestamp(row.latest, 0))
                .flatten(),
        })
    }

    /// One page of known game uuids in the table's own sorting-key order, for
    /// the walk that hands them to the re-fetch pool.
    ///
    /// Reads `mjai.game_uuids`, not `mjai.paipuya_games`. 牌谱屋 masks every
    /// listing this deployment's key can see, so its `uuid` column holds an
    /// 11-character short id — which `fetchGameRecord` refuses. The catalogue
    /// answers "what is missing"; this answers "what can be asked for".
    ///
    /// Three things here are load-bearing and each of them is a silent failure
    /// rather than an error when it is got wrong.
    ///
    /// The alias is `started_ms`, not `started_at`. ClickHouse resolves a bare
    /// identifier in `WHERE` to a `SELECT` alias in preference to the column of
    /// the same name — which is what `prefer_column_name_to_alias` exists to
    /// invert — so aliasing the millisecond conversion back onto the column name
    /// would make the keyset comparison read epoch milliseconds against a
    /// `DateTime64` whose numeric value is seconds. That is true for every row
    /// after 1970: the walk would return the same first page for ever, never see
    /// a short page, and report the whole catalogue swept.
    ///
    /// The scalar `started_at >=` beside the tuple is redundant to the meaning
    /// and required for the cost. ClickHouse expands tuple comparisons only for
    /// equality, and its primary-key condition has no atom for `greater(tuple,
    /// const)`, so the tuple alone leaves the index unused and every page reads
    /// the whole table from the beginning. The scalar is what turns that into a
    /// binary search; the tuple is what keeps games sharing one second from
    /// being skipped or repeated.
    ///
    /// No `FINAL`. It does not stop early for a `LIMIT` — it reads the rest of
    /// the key range to decide what to collapse — which at half a billion rows
    /// is the whole table per page. What it would have hidden is the duplicate
    /// the sync writes at each of its own page boundaries by design, and that is
    /// one adjacent pair in an ordered page, so the caller drops it.
    pub async fn game_uuid_listings(
        &self,
        after: Option<&SweepPosition>,
        limit: usize,
    ) -> Result<Vec<SweepPosition>, CatalogError> {
        #[derive(Deserialize)]
        struct Row {
            uuid: String,
            started_ms: i64,
        }
        let sql = game_uuid_listings_sql(after.is_some());
        let mut params: Vec<(&str, String)> = vec![("limit", limit.to_string())];
        if let Some(after) = after {
            params.push(("after_ms", after.started_at.timestamp_millis().to_string()));
            params.push(("after_uuid", after.uuid.clone()));
        }
        let rows: Vec<Row> = self.index.query(&sql, &params).await?;
        let mut page: Vec<SweepPosition> = rows
            .into_iter()
            .filter_map(|row| {
                Some(SweepPosition {
                    started_at: DateTime::from_timestamp_millis(row.started_ms)?,
                    uuid: row.uuid,
                })
            })
            .collect();
        // Adjacent, because the page is ordered by the pair a ReplacingMergeTree
        // row is identified by.
        page.dedup();
        Ok(page)
    }

    /// Which of these games have ever been stored, asked of the one place that
    /// records it.
    ///
    /// A `Game`-scoped claim is written for every record carrying a Mahjong Soul
    /// uuid and never expires, so this is exactly the question `claim` would
    /// answer a page later and one request each more expensively — except that
    /// by then the request has been spent. Digests rather than keys because the
    /// key carries a NUL byte and PostgreSQL rejects that in `text`; `= ANY` on
    /// a `bytea[]` of them probes the primary key directly.
    pub async fn claimed_games(
        &self,
        hashes: &[Vec<u8>],
    ) -> Result<std::collections::HashSet<Vec<u8>>, CatalogError> {
        if hashes.is_empty() {
            return Ok(std::collections::HashSet::new());
        }
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT key_hash FROM ingest_idempotency WHERE key_hash = ANY($1)")
                .bind(hashes)
                .fetch_all(&self.postgres)
                .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    /// Where a named walk left off, or `None` if it has never run or has
    /// finished a full pass over its source.
    pub async fn refetch_cursor(&self, walk: &str) -> Result<Option<SweepPosition>, CatalogError> {
        let row: Option<(DateTime<Utc>, String)> =
            sqlx::query_as("SELECT started_at, uuid FROM refetch_cursor WHERE walk = $1")
                .bind(walk)
                .fetch_optional(&self.postgres)
                .await?;
        Ok(row.map(|(started_at, uuid)| SweepPosition { started_at, uuid }))
    }

    /// Moves that bookmark, after the page it describes has been fetched.
    pub async fn set_refetch_cursor(
        &self,
        walk: &str,
        position: &SweepPosition,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO refetch_cursor (walk, started_at, uuid) VALUES ($1, $2, $3) \
             ON CONFLICT (walk) DO UPDATE SET started_at = EXCLUDED.started_at, \
             uuid = EXCLUDED.uuid, updated_at = now()",
        )
        .bind(walk)
        .bind(position.started_at)
        .bind(&position.uuid)
        .execute(&self.postgres)
        .await?;
        Ok(())
    }

    /// Sends a walk back to the start of its source, which is what reaching the
    /// end of the catalogue means: the games it could not fetch this time are
    /// only ever retried by another pass.
    pub async fn clear_refetch_cursor(&self, walk: &str) -> Result<(), CatalogError> {
        sqlx::query("DELETE FROM refetch_cursor WHERE walk = $1")
            .bind(walk)
            .execute(&self.postgres)
            .await?;
        Ok(())
    }

    /// Whether a startup backfill has run to completion. Read by anything whose
    /// correctness depends on one having finished — the marker is written only
    /// after the pass covered every record, so this is the difference between
    /// "the answer is complete" and "the answer is whatever has been reached".
    pub async fn backfill_completed(&self, name: &str) -> Result<bool, CatalogError> {
        Ok(
            sqlx::query("SELECT 1 FROM completed_backfills WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.postgres)
                .await?
                .is_some(),
        )
    }
}

const PAIPUYA_TABLE: &str = "mjai.paipuya_games";
/// Full uuids the re-fetch walk can hand to Mahjong Soul. Not `PAIPUYA_TABLE`:
/// see `migrations/clickhouse/004_game_uuids.sql` for why the two are apart.
const GAME_UUIDS_TABLE: &str = "mjai.game_uuids";

/// One game as 牌谱屋 lists it. Not a record: this deployment may or may not
/// have the game itself.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PaipuyaGame {
    pub uuid: String,
    pub mode_id: i32,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    /// Seat-ordered nicknames as they were at the time the game was played.
    pub players: Vec<String>,
    #[serde(default)]
    pub account_ids: Vec<u64>,
    #[serde(default)]
    pub scores: Vec<i32>,
}

/// The statement [`Catalog::game_uuid_listings`] sends, apart from its bindings.
///
/// Split out so the three rules its doc comment states can be asserted, because
/// every one of them fails silently: the alias shadow returns a correct-looking
/// page for ever, the missing scalar bound is only slow, and `FINAL` is only
/// slower. None of them raises an error, and none of them is visible in a result.
fn game_uuid_listings_sql(with_cursor: bool) -> String {
    let mut sql = format!(
        "SELECT uuid, toUnixTimestamp64Milli(started_at) AS started_ms FROM {GAME_UUIDS_TABLE}"
    );
    if with_cursor {
        sql.push_str(GAME_UUIDS_AFTER);
    }
    sql.push_str(" ORDER BY started_at, uuid LIMIT {limit:UInt32}");
    sql
}

/// Everything in `mjai.game_uuids` that sorts after a keyset position.
///
/// Shared by the page and by [`Catalog::game_uuids_ahead`] so the bar and the
/// walk cannot disagree about where the cursor is.
const GAME_UUIDS_AFTER: &str = " WHERE started_at >= fromUnixTimestamp64Milli({after_ms:Int64}) \
     AND (started_at, uuid) > \
     (fromUnixTimestamp64Milli({after_ms:Int64}), {after_uuid:String})";

/// One row of the re-fetch walk's work list: a game uuid Mahjong Soul will
/// accept. Nothing about the game itself — the protobuf that comes back
/// describes it, and a second copy here would be the one to go stale.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GameUuid {
    pub uuid: String,
    pub mode_id: i32,
    pub started_at: DateTime<Utc>,
}

/// A keyset position in `mjai.game_uuids`, which is its whole sorting key —
/// and everything the walk needs from a row, because the protobuf that comes
/// back describes itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SweepPosition {
    pub started_at: DateTime<Utc>,
    pub uuid: String,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct PaipuyaGap {
    /// Games 牌谱屋 lists in the window.
    pub listed: u64,
    /// Of those, the ones with no matching record here.
    pub missing: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct PaipuyaTotals {
    pub games: u64,
    pub earliest: Option<DateTime<Utc>>,
    pub latest: Option<DateTime<Utc>>,
}

/// Where one mode's sweep has got to, as the `paipuya_cursor` row holds it.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct PaipuyaModeCursor {
    pub mode_id: i32,
    /// The moment the next page will be asked for.
    pub next_from: DateTime<Utc>,
    /// Games this mode has contributed to the catalogue across every run.
    pub synced_games: u64,
    pub updated_at: DateTime<Utc>,
}

fn player_game_json(game: &PlayerGame) -> String {
    let stats = &game.stats;
    serde_json::json!({
        "record_id": game.record_id,
        "seat": stats.seat,
        "player": stats.player,
        "played_at": clickhouse_timestamp(game.played_at),
        // LowCardinality(String) is not nullable, the same as `records.rule`.
        "rule": game.rule.clone().unwrap_or_default(),
        "seats": game.seats,
        "detailed": u8::from(game.detailed),
        "placement": stats.placement,
        "final_score": stats.final_score,
        "settled_point": stats.settled_point,
        "grading_score": stats.grading_score,
        "level_id": stats.level_id,
        "busted": u8::from(stats.busted),
        "hands": stats.hands,
        "hands_as_dealer": stats.hands_as_dealer,
        "max_dealer_streak": stats.max_dealer_streak,
        "net_points": stats.net_points,
        "wins": stats.wins,
        "wins_tsumo": stats.wins_tsumo,
        "win_points": stats.win_points,
        "win_turns": stats.win_turns,
        "deal_ins": stats.deal_ins,
        "deal_in_points": stats.deal_in_points,
        "riichi": stats.riichi,
        "riichi_wins": stats.riichi_wins,
        "riichi_deal_ins": stats.riichi_deal_ins,
        "riichi_turns": stats.riichi_turns,
        "riichi_first": stats.riichi_first,
        "riichi_chasing": stats.riichi_chasing,
        "riichi_chased": stats.riichi_chased,
        "riichi_net": stats.riichi_net,
        "called": stats.called,
        "called_wins": stats.called_wins,
        "draws": stats.draws,
        "draws_tenpai": stats.draws_tenpai,
        "riichi_ippatsu": stats.riichi_ippatsu,
        "riichi_ura_hits": stats.riichi_ura_hits,
        "yakuman": stats.yakuman,
        "max_han": stats.max_han,
    })
    .to_string()
}
