use std::path::PathBuf;

use clap::Parser;

/// The "about 10KB per mjson" figure describes the gzip file on disk, not the payload this limit
/// applies to. Measured over 300 real 4p throne records, decompressed: min 11,352 / p50 53,668 /
/// p95 80,374 / max 106,157 bytes. 16 KiB rejected every one of them; 256 KiB keeps ~2.5x
/// headroom over the observed max.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 256 * 1024;

/// Below the largest record ever measured, so a limit under this rejects whole imports rather
/// than a long tail.
const MIN_USABLE_RECORD_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, env = "MJAI_LISTEN", default_value = "0.0.0.0:8000")]
    pub listen: String,

    #[arg(long, env = "MJAI_API_KEY", default_value = "change-me")]
    pub api_key: String,

    #[arg(long, env = "MJAI_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,

    #[arg(long, env = "MJAI_MAX_RECORD_BYTES", default_value_t = DEFAULT_MAX_RECORD_BYTES)]
    pub max_record_bytes: usize,

    #[arg(
        long,
        env = "MJAI_MAX_BATCH_BYTES",
        default_value_t = 512 * 1024 * 1024
    )]
    pub max_batch_bytes: usize,

    #[arg(long, env = "MJAI_MAX_BATCH_RECORDS", default_value_t = 50_000)]
    pub max_batch_records: usize,

    #[arg(
        long,
        env = "MJAI_PACK_TARGET_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    pub pack_target_bytes: u64,

    /// Defaults address the Compose service names; nothing publishes a host
    /// port, so the databases are only reachable from inside the network.
    #[arg(
        long,
        env = "MJAI_POSTGRES_DSN",
        default_value = "postgres://mjai:mjai@postgres:5432/mjai"
    )]
    pub postgres_dsn: String,

    /// How many PostgreSQL connections the process may hold.
    ///
    /// It was four, on the reasoning that "PostgreSQL sees one small statement
    /// per ingested record and one more per sealed pack, so a wide pool would
    /// buy nothing". True when the walks were a couple of hundred wide and the
    /// repair walk claimed nothing at all; not true of the 牌谱屋 sweep, which
    /// claims once per game with a thousand games in flight.
    ///
    /// What four did on the live deployment was worse than slow. Every user of
    /// this pool shares it, so when the four were held past `ACQUIRE_TIMEOUT`
    /// the walk cursor, the 牌谱屋 sync and the Kafka backlog sampler all failed
    /// inside three seconds of each other, and the sweep — which has months to
    /// run — aborted. A pool this narrow does not degrade, it takes everything
    /// down together.
    ///
    /// Thirty-two against PostgreSQL's own default of a hundred, for one client
    /// process. At the measured 0.6 ms per claim that is fifty thousand a
    /// second, which is not a number this will ever need; the point is headroom
    /// for a stall, not throughput.
    #[arg(long, env = "MJAI_POSTGRES_MAX_CONNECTIONS", default_value_t = 32)]
    pub postgres_max_connections: u32,

    #[arg(
        long,
        env = "MJAI_CLICKHOUSE_URL",
        default_value = "http://clickhouse:8123"
    )]
    pub clickhouse_url: String,

    #[arg(long, env = "MJAI_CLICKHOUSE_USER", default_value = "mjai")]
    pub clickhouse_user: String,

    #[arg(long, env = "MJAI_CLICKHOUSE_PASSWORD", default_value = "mjai")]
    pub clickhouse_password: String,

    /// The API container starts alongside its databases, so a connection
    /// refused during this window is expected rather than fatal. Past it the
    /// process exits: an API serving an empty index looks exactly like data
    /// loss, and a crash loop is the only failure mode that is visible.
    #[arg(long, env = "MJAI_DATABASE_WAIT_SECS", default_value_t = 120)]
    pub database_wait_secs: u64,

    #[arg(
        long,
        env = "MJAI_MIHOMO_CONTROLLER_URL",
        default_value = "http://mihomo:9090"
    )]
    pub mihomo_controller_url: String,

    #[arg(long, env = "MJAI_MIHOMO_SECRET", default_value = "change-mihomo-me")]
    pub mihomo_secret: String,

    #[arg(
        long,
        env = "MJAI_MIHOMO_PROXY_URL",
        default_value = "http://mihomo:7890"
    )]
    pub mihomo_proxy_url: String,

    #[arg(long, env = "MJAI_PUBLIC_URL", default_value = "http://localhost:3000")]
    pub public_url: String,

    #[arg(long, env = "MJAI_ADMIN_EMAIL", default_value = "admin@example.com")]
    pub admin_email: String,

    #[arg(
        long,
        env = "MJAI_ADMIN_PASSWORD",
        default_value = "change-this-password"
    )]
    pub admin_password: String,

    #[arg(long, env = "MJAI_EMAIL_API_URL")]
    pub email_api_url: Option<String>,

    #[arg(long, env = "MJAI_EMAIL_API_TOKEN")]
    pub email_api_token: Option<String>,

    #[arg(long, env = "MJAI_EMAIL_FROM", default_value = "noreply@example.com")]
    pub email_from: String,

    #[arg(
        long,
        env = "MJAI_S3_ENDPOINT_URL",
        default_value = "http://rustfs:9000"
    )]
    pub s3_endpoint_url: String,

    #[arg(long, env = "MJAI_S3_ACCESS_KEY", default_value = "rustfsadmin")]
    pub s3_access_key: String,

    #[arg(long, env = "MJAI_S3_SECRET_KEY", default_value = "rustfsadmin")]
    pub s3_secret_key: String,

    #[arg(long, env = "MJAI_S3_BUCKET", default_value = "mjai-raw")]
    pub s3_bucket: String,

    /// RustFS never validates the region, it only has to be the same string the
    /// bucket was created with, which is what the Compose sidecar passes.
    #[arg(long, env = "MJAI_S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(
        long,
        env = "MJAI_KAFKA_BOOTSTRAP_SERVERS",
        default_value = "redpanda:9092"
    )]
    pub kafka_bootstrap_servers: String,

    #[arg(long, env = "MJAI_KAFKA_TOPIC", default_value = "mjai.records.raw")]
    pub kafka_topic: String,

    /// One pack worker per partition, and the pack worker is the pipeline's
    /// narrowest point — so this is how wide the pipeline is.
    ///
    /// It was one, on the reasoning that "a single broker pinned to one core
    /// gains nothing from more". True of the broker and beside the point: what
    /// a partition buys is not broker parallelism but a second consumer. Each
    /// record costs its worker a gunzip, two mjai parses, a full replay for the
    /// player statistics, a sha256 and two zstd frames — measured at 3.4 ms,
    /// all of it on one thread, and while that thread is sealing a 256MB pack it
    /// is not consuming at all.
    ///
    /// What one partition did on the live deployment: 290 records a second with
    /// 56 cores idle, the topic pinned at the `MJAI_KAFKA_MAX_LAG` gate, and 400
    /// logged-in accounts sleeping in it. Adding accounts moved nothing, because
    /// the accounts were never the limit.
    ///
    /// Eight, which is eight cores at full tilt and leaves the rest for the
    /// conversions. Raising it on a deployment that already has a topic is two
    /// steps, because rskafka cannot widen one: `rpk topic add-partitions` on
    /// the broker, then restart. `Kafka::connect` reads the live count and says
    /// so when the two disagree.
    #[arg(long, env = "MJAI_KAFKA_PARTITIONS", default_value_t = 8)]
    pub kafka_partitions: i32,

    /// The durable replacement for the old in-memory pending-row cap. Ingest is
    /// refused past this backlog so the topic cannot outgrow its retention and
    /// silently drop records that were already acknowledged as accepted.
    ///
    /// **Per partition**, like `retention_bytes` and for the same reason: a
    /// pack worker commits its offset when it seals, so each partition sits one
    /// pack cycle behind as a matter of course, and eight of them sit eight
    /// times as far behind while every one of them is caught up. Read as a
    /// topic-wide total it turned into a throttle on a healthy pipeline —
    /// 26,000 of normal backlog against a 25,000 ceiling.
    #[arg(long, env = "MJAI_KAFKA_MAX_LAG", default_value_t = 50_000)]
    pub kafka_max_lag: i64,

    /// Packs seal on size or on age, whichever comes first. Size alone would
    /// leave a quiet period's records unindexed and invisible to every query
    /// until 256MB had accumulated, which at the live collection rate is weeks.
    #[arg(long, env = "MJAI_PACK_MAX_AGE_SECS", default_value_t = 300)]
    pub pack_max_age_secs: u64,

    /// The age at which a pack seals once the worker has caught up with the
    /// topic. Waiting out the full age limit with nothing left to consume only
    /// delays every record in the pack becoming readable and prolongs the window
    /// in which the broker's single volume holds the only copy of bytes the API
    /// has already answered `202` for.
    ///
    /// It buys that with one object and one ClickHouse part per interval in
    /// which anything arrived at all — the age runs from the pack's first
    /// append, so a continuous trickle seals every interval whether the pack
    /// holds one record or twenty. At 30 seconds that is at most 2,880 a day
    /// against 288 at the age limit, and both the GC's bucket listing and
    /// `indexed_pack_keys` scale with the number of packs. Raise it if
    /// that starts to cost more than the visibility is worth; past that point
    /// the honest fix is compacting small packs, not waiting longer.
    ///
    /// Which is where thirty seconds got to. "Under load the worker is never
    /// caught up" stopped being true when the topic went to eight partitions:
    /// each worker sees an eighth of the flow and reaches the end of its own
    /// partition constantly, so the idle rule decided almost every seal.
    /// Measured at 670 records a second — 56 packs in five minutes, 48MB each
    /// against a 256MB target, sixteen thousand objects a day where the size
    /// rule would have made three thousand. Two minutes is still a bounded wait
    /// for a record to become readable, and lets a pack reach about 135MB at
    /// that rate.
    #[arg(long, env = "MJAI_PACK_IDLE_SECS", default_value_t = 120)]
    pub pack_idle_secs: u64,

    /// An object younger than this is never collected, however orphaned it
    /// looks: an upload that has landed but whose index rows are still in
    /// flight is indistinguishable from one whose writer died.
    #[arg(long, env = "MJAI_GC_GRACE_SECS", default_value_t = 86_400)]
    pub gc_grace_secs: u64,

    #[arg(long, env = "MJAI_GC_INTERVAL_SECS", default_value_t = 3_600)]
    pub gc_interval_secs: u64,
}

impl Config {
    /// Raising the compiled-in default does not reach a deployment whose own `.env` still pins
    /// `MJAI_MAX_RECORD_BYTES=16384` from the old `.env.example`; upgrading the image leaves that
    /// file alone. Nothing in the repository can rewrite it, so say so at every boot.
    pub fn record_limit_warning(&self) -> Option<String> {
        (self.max_record_bytes < MIN_USABLE_RECORD_BYTES).then(|| {
            format!(
                "MJAI_MAX_RECORD_BYTES={} 低于真实对局记录的大小（解压后实测 11KB–104KB），批量导入会整批被拒绝；\
                 请把 .env 里的这一项改成 {DEFAULT_MAX_RECORD_BYTES} 或删掉该行改用默认值",
                self.max_record_bytes
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_record_limit(bytes: &str) -> Config {
        // Explicit args, so an exported MJAI_MAX_RECORD_BYTES cannot change what is under test.
        Config::parse_from(["mjai", "--max-record-bytes", bytes])
    }

    #[test]
    fn warns_only_when_the_record_limit_cannot_hold_a_real_record() {
        let warning = config_with_record_limit("16384").record_limit_warning();
        assert!(warning.unwrap().contains("16384"));
        assert_eq!(
            config_with_record_limit(&DEFAULT_MAX_RECORD_BYTES.to_string()).record_limit_warning(),
            None
        );
    }
}
