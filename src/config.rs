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
