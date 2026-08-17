//! The client's own logging, which this deployment did not do at all.
//!
//! Every real login uploads four lines to Mahjong Soul's Aliyun log store, and
//! every one of them carries the `account_id` that just logged in. That makes
//! "accounts that logged in and never reported" a single join away — no
//! fingerprinting, no traffic analysis, one query against two tables the
//! operator already has. Sending nothing is not quiet; it is a flag.
//!
//! What is sent adds nothing the server did not already have. The login it
//! follows carried the same account, from the same address, with the same
//! declared machine; the only new fact here is that the account behaves like a
//! client. Which is the point.
//!
//! Every field is copied from `captures/register_20260805_174145.json`, a real
//! browser session. Failures are silent by design: telemetry that could fail a
//! login would be a worse bargain than not sending it.

use std::time::Duration;

use serde_json::json;
use tracing::debug;

use super::rpc::requests::Persona;

const TRACK_URL: &str =
    "https://majsoul-hk-client.cn-hongkong.log.aliyuncs.com/logstores/client/track";

/// One login, as the log store will hear about it.
pub struct LoginReport<'a> {
    pub persona: Persona,
    /// The same value the login carried in `random_key`. If these two disagree,
    /// reporting is worse than staying quiet: it says the machine that logged in
    /// is not the machine reporting.
    pub device_id: &'a str,
    pub account_id: u64,
    pub res_version: &'a str,
    pub package_version: &'a str,
    pub gateway_host: &'a str,
    /// How long the login itself took, which the client reports negated.
    pub login_seconds: f64,
    /// How long the client says it spent loading, in milliseconds.
    pub load_millis: u64,
}

/// The four lines one login produces, in order.
///
/// The first two go out before the lobby is drawn and carry neither
/// `session_id` nor `connect_lobby`; the last two carry both. That split is in
/// the capture and is reproduced rather than tidied — a uniform set of
/// parameters would be a shape of its own.
pub async fn report_login(http: &reqwest::Client, report: LoginReport<'_>) {
    let LoginReport {
        persona,
        device_id,
        account_id,
        res_version,
        package_version,
        gateway_host,
        login_seconds,
        load_millis,
    } = report;
    // One run of the application, one id, shared by all four lines — that is
    // what the capture shows. `session_id` is separate and appears only on the
    // pair that follows the lobby.
    let app_runtime_id = uuid::Uuid::new_v4().to_string();
    let session_id = uuid::Uuid::new_v4().to_string();
    let device_model = format!("Chrome {}.0.0.0", persona.chrome);

    let base = |lobby: Option<&str>, session: Option<&str>| {
        let mut fields = vec![
            ("APIVersion", "0.6.0".to_owned()),
            ("server", "1".to_owned()),
            ("level", "info".to_owned()),
            ("app_runtime_id", app_runtime_id.clone()),
            ("res_version", res_version.to_owned()),
            ("client_version", package_version.to_owned()),
            ("client_type", "web".to_owned()),
            ("device_type", "pc".to_owned()),
            ("device_model", device_model.clone()),
            // Frozen at 10.15.7 by Chrome itself, the same as the user agent.
            ("device_os", "MacOS 10.15.7".to_owned()),
            ("device_gpu_name", persona.gpu.to_owned()),
            // The same value the login carries in `random_key`. If these two
            // ever disagree, reporting is worse than staying quiet: it says the
            // machine that logged in is not the machine that is reporting.
            ("device_id", device_id.to_owned()),
            ("account_id", account_id.to_string()),
        ];
        if let Some(session) = session {
            fields.push(("session_id", session.to_owned()));
        }
        if let Some(lobby) = lobby {
            fields.push(("connect_lobby", lobby.to_owned()));
        }
        fields
    };

    /// One log line: its common fields, its category and its `content`.
    type Line<'a> = (Vec<(&'a str, String)>, &'a str, String);

    let lines: [Line<'_>; 4] = [
        (
            base(None, None),
            "login_stats",
            json!({ "success": true, "use_time": -login_seconds }).to_string(),
        ),
        (
            base(None, None),
            "game_status",
            json!({ "type": "login_loading_start" }).to_string(),
        ),
        (
            base(Some(gateway_host), Some(&session_id)),
            "certificate_info",
            "[{\"ip\":{}}]".to_owned(),
        ),
        (
            base(Some(gateway_host), Some(&session_id)),
            "game_status",
            json!({
                "type": "login_loading_end",
                "load_time": load_millis,
                "error_code": 0,
            })
            .to_string(),
        ),
    ];

    for (fields, category, content) in lines {
        let mut query = fields;
        query.push(("log_category", category.to_owned()));
        query.push(("content", content));
        let sent = http
            .get(TRACK_URL)
            .query(&query)
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        if let Err(error) = sent {
            // Not worth a warning: the login already succeeded and nothing here
            // is load bearing.
            debug!("client telemetry {category} did not go out: {error}");
            return;
        }
    }
}
