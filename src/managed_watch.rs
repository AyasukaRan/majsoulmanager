use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{TimeZone, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    catalog::Catalog,
    indexer,
    kafka::Kafka,
    majsoul::{
        BROWSER_USER_AGENT,
        convert::{GameMetadata, convert_record_bytes},
        gateway::{discover_gateway, discover_package_version},
        modes::{mode_metadata, room_modes, uuid_year},
        proto::{FieldIterator, extract_string, extract_varint},
        rpc::{
            FETCH_GAME_LIVE_LIST_METHOD, FETCH_GAME_RECORD_METHOD, MajsoulRpc, ServerError,
            build_fetch_game_live_list_request, build_fetch_game_record_request,
            ensure_success_response,
        },
    },
    mihomo::MihomoManager,
    watch::{WatchEvent, WatchEventKind, WatchRegistry},
    watch_log::{WatchLogBuffer, WatchLogLevel},
    watch_service::{PluginWorker, WatchInstance, WatchProxyMode, WatchServiceConfig},
};

const FILTER_ID_OFFSET: i32 = 200;
const SETTLE_SECS: u64 = 120;
// How long a finished game may keep failing to fetch before it is dropped from
// the pending queue.
const GIVE_UP_SECS: u64 = 3 * 3600;
const RECONNECT_DELAY_SECS: u64 = 5;
// Login client version numbers, both captured from a real web client.
//
// Measured against the live CN server: only `client_version_string`
// (`WebGL_2022-<code>`) is validated, and only as a *lower bound* — the server
// accepts anything at or above roughly (current - 3 patches) and accepts
// arbitrarily future values, while `client_version { resource, package }` is
// not checked at all. So the pinned code version only has to be recent enough,
// and on rejection the floor can be found by search (discover_version_floor).
const CN_CODE_VERSION: &str = "0.16.257";
const CN_PACKAGE_VERSION: &str = "4.0.45";
// Ceiling for the version search: four minor releases above the rejected
// value. Reaching it means the login is failing for some reason other than a
// raised version floor.
const VERSION_SEARCH_SPAN: u32 = 4096;
const VERSION_PROBE_DELAY_SECS: u64 = 5;

pub struct ManagedWatchDependencies {
    pub data_dir: PathBuf,
    /// Where the re-fetch backfill leaves work. An instance serves it only in
    /// the part of its poll interval it would otherwise sleep through.
    pub refetch: Arc<crate::refetch::RefetchBroker>,
    pub catalog: Arc<Catalog>,
    pub kafka: Arc<Kafka>,
    pub registry: Arc<WatchRegistry>,
    pub accounts: Arc<crate::accounts::AccountPool>,
    pub mihomo: Arc<MihomoManager>,
    pub logs: Arc<WatchLogBuffer>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LiveGame {
    uuid: String,
    mode_id: i32,
    start_time: u32,
    #[serde(default)]
    queued_at: Option<u64>,
}

pub(crate) enum LoginTransport {
    Builtin(MajsoulRpc),
    External {
        worker: Arc<PluginWorker>,
        session_id: String,
    },
}

impl LoginTransport {
    async fn call(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::Builtin(rpc) => rpc.call(method, payload).await,
            Self::External { worker, session_id } => {
                let result = worker
                    .request(
                        "rpc",
                        serde_json::json!({
                            "session_id": session_id,
                            "method": method,
                            "payload_base64": STANDARD.encode(payload),
                        }),
                    )
                    .await
                    .map_err(|error| anyhow::Error::msg(error.to_string()))?;
                decode_base64_field(&result, "payload_base64")
            }
        }
    }

    pub(crate) async fn close(&self) {
        match self {
            Self::Builtin(rpc) => {
                let _ = rpc.close().await;
            }
            Self::External { worker, session_id } => {
                let _ = worker
                    .request(
                        "close_session",
                        serde_json::json!({"session_id": session_id}),
                    )
                    .await;
            }
        }
    }
}

#[derive(Deserialize)]
struct RpcRequest {
    method: String,
    payload_base64: String,
}

pub(crate) async fn run(
    config: WatchServiceConfig,
    mut instance: WatchInstance,
    dependencies: Arc<ManagedWatchDependencies>,
    login_worker: Option<Arc<PluginWorker>>,
    pb_worker: Option<Arc<PluginWorker>>,
) -> Result<()> {
    // Every log line this collector emits carries its instance id, which is
    // how the console tells concurrent collectors apart.
    let source = format!("collector:{}", instance.id);
    let modes = room_modes(&instance.room, &instance.modes, instance.players)?;
    let (username, password) =
        load_first_account(&instance.account_secret_ref, &dependencies.accounts)?;
    let proxy = match config.proxy_mode {
        WatchProxyMode::Direct => None,
        WatchProxyMode::Mihomo => Some(
            dependencies
                .mihomo
                .proxy_url_for(crate::mihomo::MihomoLane::Watch),
        ),
        WatchProxyMode::Custom => config.custom_proxy_url.clone(),
    };
    // The password and the proxy credentials are registered by `connect`, which
    // is the one place every login goes through.
    let state_path = state_path(&dependencies.data_dir, &instance);
    let discovery_dir = discovery_dir(&dependencies.data_dir, &instance);
    // Shared: the gateway, package version and version floor are properties of
    // the Majsoul deployment, not of the account, so instances benefit from
    // each other's lookups.
    let cache_dir = dependencies.data_dir.join("watch/cache");
    // A previously discovered floor, so a restart does not pay for the search
    // again. Ignored when it is below the pinned default, which a code update
    // may have moved past it.
    if instance.client_version.is_none()
        && let Some(stored) = load_client_version(&cache_dir)
        && parse_version(&stored) > parse_version(CN_CODE_VERSION)
    {
        instance.client_version = Some(stored);
    }
    let (tracked_state, pending_state) = load_state(&state_path)?;
    let mut tracked = tracked_state
        .into_iter()
        .map(|game| (game.uuid.clone(), game))
        .collect::<HashMap<_, _>>();
    let mut pending = pending_state
        .into_iter()
        .map(|game| (game.uuid.clone(), game))
        .collect::<HashMap<_, _>>();

    let proxy_label = proxy
        .as_deref()
        .map(proxy_display)
        .unwrap_or_else(|| "直连".into());

    loop {
        dependencies.logs.append(
            WatchLogLevel::Info,
            &source,
            format!(
                "开始连接雀魂服务器 (账号 {}, 代理 {proxy_label})",
                masked_account(&username)
            ),
        );
        match connect(
            &config.server,
            instance.client_version.as_deref(),
            &username,
            &password,
            proxy.as_deref(),
            login_worker.clone(),
            &dependencies.logs,
            &source,
            &cache_dir,
        )
        .await
        {
            Ok((mut transport, client_version)) => {
                info!(
                    account = %username,
                    ?modes,
                    proxy = ?proxy,
                    "managed Mahjong Soul watch connected"
                );
                if let Err(error) = watch_session(
                    &config,
                    &dependencies,
                    &source,
                    &modes,
                    &discovery_dir,
                    &client_version,
                    &state_path,
                    &mut tracked,
                    &mut pending,
                    &mut transport,
                    pb_worker.as_ref(),
                )
                .await
                {
                    warn!(error = %format!("{error:#}"), "watch session disconnected");
                    dependencies.logs.append(
                        WatchLogLevel::Warn,
                        &source,
                        format!("会话断开: {error:#}"),
                    );
                }
                transport.close().await;
            }
            Err(error) => {
                let detail = format!("{error:#}");
                warn!(error = %detail, "watch login failed");
                dependencies.logs.append(
                    WatchLogLevel::Error,
                    &source,
                    format!("登录失败: {detail}"),
                );
                if let Some(refreshed) = refreshed_client_version(
                    &config.server,
                    &username,
                    &password,
                    proxy.as_deref(),
                    login_worker.clone(),
                    &dependencies.logs,
                    &source,
                    &cache_dir,
                    instance.client_version.as_deref(),
                    &detail,
                )
                .await
                {
                    instance.client_version = Some(refreshed);
                }
            }
        }
        dependencies.logs.append(
            WatchLogLevel::Info,
            &source,
            format!("等待 {RECONNECT_DELAY_SECS} 秒后重连"),
        );
        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}

/// This collector's working queue, named by the instance's immutable key and
/// never by its id.
///
/// The id is a label an operator may rename at any moment, and a renamed state
/// file is not reported missing: [`load_state`] answers `Ok` with an empty
/// queue for a file that is not there, so the collector would come back up
/// looking healthy while every game it had seen live but not yet fetched — up
/// to [`GIVE_UP_SECS`] of them, still being played or still answering 1203 —
/// was left in a file nothing will ever open again.
fn state_path(data_dir: &Path, instance: &WatchInstance) -> PathBuf {
    data_dir.join(format!("watch/state-{}.json", instance.key))
}

/// Where [`append_discovered`] writes, keyed the same way and for a weaker
/// version of the same reason: a rename would not lose the audit trail, but it
/// would silently start a second one beside it with nothing joining the two.
fn discovery_dir(data_dir: &Path, instance: &WatchInstance) -> PathBuf {
    data_dir.join(format!("watch/discovered/{}", instance.key))
}

/// Whether a failed Mahjong Soul request means this session is finished.
///
/// The one rule every fetch loop in this codebase has to get right, so it is
/// stated once. A business error means the socket and the session answered and
/// the failure is about the one record asked for: reconnecting neither fixes it
/// nor lets anything else through, and the record would fail again after every
/// reconnect, forever. Anything else — a stale login, a dead socket — means
/// nothing else will succeed either.
pub(crate) fn ends_the_session(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<ServerError>()
        .is_none_or(|server| server.is_session_stale())
}

/// Renders a proxy URL as `scheme://host[:port]`, never exposing credentials.
pub(crate) fn proxy_display(proxy: &str) -> String {
    match reqwest::Url::parse(proxy) {
        Ok(url) => {
            let scheme = url.scheme();
            match (url.host_str(), url.port()) {
                (Some(host), Some(port)) => format!("{scheme}://{host}:{port}"),
                (Some(host), None) => format!("{scheme}://{host}"),
                (None, _) => scheme.to_owned(),
            }
        }
        Err(_) => "无法解析".into(),
    }
}

/// Extracts the host of a gateway endpoint URL for log output.
fn endpoint_host(endpoint: &str) -> String {
    reqwest::Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "未知主机".into())
}

async fn fetch_live_list(
    transport: &LoginTransport,
    pb_worker: Option<&Arc<PluginWorker>>,
    filter_id: u32,
    fallback_mode_id: i32,
) -> Result<Vec<LiveGame>> {
    if let Some(worker) = pb_worker {
        let request = plugin_rpc_request(
            worker,
            "build_live_list_request",
            serde_json::json!({"filter_id": filter_id}),
        )
        .await?;
        let payload = STANDARD.decode(request.payload_base64)?;
        let response = transport.call(&request.method, &payload).await?;
        let result = worker
            .request(
                "parse_live_list_response",
                serde_json::json!({
                    "filter_mode_id": fallback_mode_id,
                    "payload_base64": STANDARD.encode(response),
                }),
            )
            .await
            .map_err(|error| anyhow::Error::msg(error.to_string()))?;
        let games = result
            .get("games")
            .cloned()
            .context("PB module response is missing games")?;
        let games: Vec<LiveGame> = serde_json::from_value(games)?;
        return Ok(games
            .into_iter()
            .filter(|game| {
                !game.uuid.is_empty()
                    && mode_metadata(game.mode_id).is_ok()
                    && uuid_year(&game.uuid).is_ok()
            })
            .collect());
    }
    let request = build_fetch_game_live_list_request(filter_id);
    let response = transport
        .call(FETCH_GAME_LIVE_LIST_METHOD, &request)
        .await?;
    ensure_success_response(&response, "fetchGameLiveList")?;
    parse_live_list(&response, fallback_mode_id)
}

pub(crate) async fn fetch_game_record(
    transport: &LoginTransport,
    pb_worker: Option<&Arc<PluginWorker>>,
    uuid: &str,
    client_version: &str,
) -> Result<Vec<u8>> {
    if let Some(worker) = pb_worker {
        let request = plugin_rpc_request(
            worker,
            "build_record_request",
            serde_json::json!({
                "uuid": uuid,
                "client_version": client_version,
            }),
        )
        .await?;
        let payload = STANDARD.decode(request.payload_base64)?;
        let response = transport.call(&request.method, &payload).await?;
        let result = worker
            .request(
                "parse_record_response",
                serde_json::json!({
                    "uuid": uuid,
                    "payload_base64": STANDARD.encode(response),
                }),
            )
            .await
            .map_err(|error| anyhow::Error::msg(error.to_string()))?;
        return decode_base64_field(&result, "pb_base64");
    }
    let request = build_fetch_game_record_request(uuid, client_version);
    let response = transport.call(FETCH_GAME_RECORD_METHOD, &request).await?;
    ensure_success_response(&response, "fetchGameRecord")?;
    Ok(response)
}

async fn plugin_rpc_request(
    worker: &PluginWorker,
    method: &str,
    params: serde_json::Value,
) -> Result<RpcRequest> {
    let result = worker
        .request(method, params)
        .await
        .map_err(|error| anyhow::Error::msg(error.to_string()))?;
    Ok(serde_json::from_value(result)?)
}

fn required_string(value: &serde_json::Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("module response is missing {field}"))
}

fn decode_base64_field(value: &serde_json::Value, field: &str) -> Result<Vec<u8>> {
    let encoded = required_string(value, field)?;
    Ok(STANDARD.decode(encoded)?)
}

fn client_version_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("client_version")
}

/// Last discovered version floor, if one was ever written and still parses.
fn load_client_version(cache_dir: &Path) -> Option<String> {
    let stored = std::fs::read_to_string(client_version_path(cache_dir)).ok()?;
    let stored = stored.trim().to_string();
    parse_version(&stored).map(|_| stored)
}

fn store_client_version(cache_dir: &Path, version: &str) {
    // Written via a temporary file because instances share this path: a reader
    // must never see a half-written version.
    let temporary = cache_dir.join(format!("client_version.{}", Uuid::new_v4().simple()));
    if std::fs::create_dir_all(cache_dir).is_ok()
        && let Err(error) = std::fs::write(&temporary, version)
            .and_then(|()| std::fs::rename(&temporary, client_version_path(cache_dir)))
    {
        let _ = std::fs::remove_file(&temporary);
        warn!(error = %error, "failed to persist discovered client version");
    }
}

/// Parse `0.<minor>.<patch>` into one number so the two moving components can
/// be searched as a single ordered value.
fn parse_version(version: &str) -> Option<u32> {
    let mut parts = version.split('.');
    if parts.next()? != "0" {
        return None;
    }
    let minor: u32 = parts.next()?.parse().ok()?;
    let patch: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || patch >= 1000 {
        return None;
    }
    Some(minor * 1000 + patch)
}

/// Inverse of [`parse_version`]; patch overflow carries into minor, which is
/// what Majsoul itself does when a patch series rolls over.
fn format_version(value: u32) -> String {
    format!("0.{}.{}", value / 1000, value % 1000)
}

/// What to log in with next after a rejected login, or `None` when the
/// rejection was not about the client version.
///
/// Majsoul raises its accepted floor server-wide, so every login in the process
/// meets it within the same minute — the collectors and the re-fetch pool alike.
/// Whichever one finishes the search first publishes the floor to the shared
/// cache, and the rest read it instead of paying for a search of their own. That
/// sharing is the reason this is one function rather than a copy per caller: two
/// copies would be two searches, which is a burst of failed logins from several
/// accounts at once.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn refreshed_client_version(
    server: &str,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
    logs: &WatchLogBuffer,
    source: &str,
    cache_dir: &Path,
    current: Option<&str>,
    failure: &str,
) -> Option<String> {
    if !failure.contains("151") {
        return None;
    }
    let rejected = current.unwrap_or(CN_CODE_VERSION).to_owned();
    if let Some(shared) = load_client_version(cache_dir)
        .filter(|shared| parse_version(shared) > parse_version(&rejected))
    {
        logs.append(
            WatchLogLevel::Info,
            source,
            format!("error 151 = 客户端版本 {rejected} 已过期,采用其他会话探测到的 {shared}"),
        );
        return Some(shared);
    }
    logs.append(
        WatchLogLevel::Warn,
        source,
        format!("error 151 = 客户端版本 {rejected} 已过期,开始探测服务端接受的最低版本"),
    );
    match discover_version_floor(
        server, username, password, proxy, worker, logs, source, cache_dir, &rejected,
    )
    .await
    {
        Ok(version) => {
            store_client_version(cache_dir, &version);
            logs.append(
                WatchLogLevel::Info,
                source,
                format!("客户端版本已自动更新为 {version}"),
            );
            Some(version)
        }
        Err(error) => {
            logs.append(
                WatchLogLevel::Error,
                source,
                format!("版本探测失败: {error:#}"),
            );
            None
        }
    }
}

/// Try one candidate version. `Ok(false)` means the server rejected it as too
/// old (151); any other failure is a real error and must abort the search
/// rather than be mistaken for a rejection.
#[allow(clippy::too_many_arguments)]
async fn probe_version(
    server: &str,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
    logs: &WatchLogBuffer,
    source: &str,
    cache_dir: &Path,
    version: &str,
) -> Result<bool> {
    match connect(
        server,
        Some(version),
        username,
        password,
        proxy,
        worker,
        logs,
        source,
        cache_dir,
    )
    .await
    {
        Ok((transport, _)) => {
            transport.close().await;
            Ok(true)
        }
        Err(error) if format!("{error:#}").contains("151") => Ok(false),
        Err(error) => Err(error),
    }
}

/// Find the lowest client version the server still accepts.
///
/// The server validates `client_version_string` as a lower bound, so the set of
/// accepted versions is upward-closed: escalate until one is accepted, then
/// binary-search back down to the boundary. Costs about six logins, and only
/// when Majsoul raises the floor. The floor is returned rather than something
/// comfortably above it because the floor is what a browser tab a few patches
/// behind reports, whereas an arbitrarily high version is a giveaway.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn discover_version_floor(
    server: &str,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
    logs: &WatchLogBuffer,
    source: &str,
    cache_dir: &Path,
    rejected: &str,
) -> Result<String> {
    let rejected_value =
        parse_version(rejected).with_context(|| format!("unparsable client version {rejected}"))?;

    let floor = search_version_floor(rejected_value, |value: u32| {
        let worker = worker.clone();
        async move {
            let version = format_version(value);
            let accepted = probe_version(
                server, username, password, proxy, worker, logs, source, cache_dir, &version,
            )
            .await?;
            logs.append(
                WatchLogLevel::Info,
                source,
                format!(
                    "版本探测 {version} -> {}",
                    if accepted { "接受" } else { "拒绝" }
                ),
            );
            // A burst of back-to-back failed logins is itself worth hiding, and
            // the search runs rarely enough that pacing it costs nothing.
            tokio::time::sleep(Duration::from_secs(VERSION_PROBE_DELAY_SECS)).await;
            Ok(accepted)
        }
    })
    .await?;
    Ok(format_version(floor))
}

/// Lowest value for which `probe` returns true, given that the accepted set is
/// upward-closed and `rejected` is known to be below it.
async fn search_version_floor<F, Fut>(rejected: u32, mut probe: F) -> Result<u32>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    // Escalate until something is accepted, remembering the highest rejection
    // so the binary search starts from a known-bad bound.
    let (mut too_low, mut accepted) = (rejected, None);
    let mut step = 4;
    while step <= VERSION_SEARCH_SPAN {
        let candidate = rejected + step;
        if probe(candidate).await? {
            accepted = Some(candidate);
            break;
        }
        too_low = candidate;
        step *= 4;
    }
    let mut accepted = accepted.context("no accepted client version within the search span")?;

    while too_low + 1 < accepted {
        let middle = too_low + (accepted - too_low) / 2;
        if probe(middle).await? {
            accepted = middle;
        } else {
            too_low = middle;
        }
    }
    Ok(accepted)
}

/// Opens one authenticated Mahjong Soul session.
///
/// It takes the server and the pinned client version rather than the documents
/// they usually come out of, because the re-fetch pool logs in the same way from
/// a configuration that has no collectors in it at all. Those two values are
/// everything the login depends on; the rest of a collector's configuration
/// describes what to do afterwards.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn connect(
    server: &str,
    client_version: Option<&str>,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
    logs: &WatchLogBuffer,
    source: &str,
    cache_dir: &Path,
) -> Result<(LoginTransport, String)> {
    // Here rather than at each caller, because an external login module is
    // handed the password as plain JSON and its stderr is appended to a buffer
    // every console member can read. A caller that forgot this would leak every
    // account it logs in with, and the only sign would be the password sitting
    // in the log.
    logs.register_secret(password);
    if let Some(url) = proxy {
        logs.register_secret(url);
        if let Ok(parsed) = reqwest::Url::parse(url) {
            if let Some(credential) = parsed.password() {
                logs.register_secret(credential);
            }
            if !parsed.username().is_empty() {
                logs.register_secret(parsed.username());
            }
        }
    }
    if let Some(worker) = worker {
        let result = worker
            .request(
                "open_session",
                serde_json::json!({
                    "server": server,
                    "username": username,
                    "password": password,
                    "proxy_url": proxy,
                    "client_version": client_version,
                }),
            )
            .await
            .map_err(|error| anyhow::Error::msg(error.to_string()))?;
        let session_id = required_string(&result, "session_id")?;
        let client_version = required_string(&result, "client_version")?;
        logs.append(
            WatchLogLevel::Info,
            source,
            format!("登录成功 (外置模块, 客户端版本 {client_version})"),
        );
        return Ok((
            LoginTransport::External { worker, session_id },
            client_version,
        ));
    }
    let mut builder = reqwest::Client::builder().user_agent(BROWSER_USER_AGENT);
    if let Some(proxy) = proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy)?);
    }
    let http = builder.build()?;
    let (endpoint, _resource_version, route_id) =
        discover_gateway(&http, server, cache_dir).await?;
    logs.append(
        WatchLogLevel::Info,
        source,
        format!("网关发现完成 ({})", endpoint_host(&endpoint)),
    );
    // Login sends client_version_string = WebGL_2022-<code_version>; the code
    // version differs from the resource version and is pinned, overridable per
    // instance — which is also how a discovered floor is applied. The package
    // (Unity build) is discovered from index.html, falling back to the pinned
    // default.
    let code_version = client_version.unwrap_or(CN_CODE_VERSION).to_owned();
    let package_version = discover_package_version(&http, server, cache_dir)
        .await
        .unwrap_or_else(|_| CN_PACKAGE_VERSION.to_string());
    logs.append(
        WatchLogLevel::Info,
        source,
        format!("客户端版本 WebGL_2022-{code_version} (package {package_version})"),
    );
    let rpc = MajsoulRpc::connect_with_proxy(&endpoint, proxy).await?;
    logs.append(WatchLogLevel::Info, source, "WebSocket 已连接");
    rpc.login_native_exact(
        username,
        password,
        &code_version,
        &package_version,
        server,
        &route_id,
    )
    .await?;
    logs.append(WatchLogLevel::Info, source, "登录成功");
    Ok((
        LoginTransport::Builtin(rpc),
        format!("WebGL_2022-{code_version}"),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn watch_session(
    config: &WatchServiceConfig,
    dependencies: &ManagedWatchDependencies,
    source: &str,
    modes: &[i32],
    discovery_dir: &Path,
    client_version: &str,
    state_path: &Path,
    tracked: &mut HashMap<String, LiveGame>,
    pending: &mut HashMap<String, LiveGame>,
    transport: &mut LoginTransport,
    pb_worker: Option<&Arc<PluginWorker>>,
) -> Result<()> {
    loop {
        let round_started = tokio::time::Instant::now();
        let mut live_now = HashMap::new();
        for mode_id in modes {
            let games = fetch_live_list(
                transport,
                pb_worker,
                (*mode_id + FILTER_ID_OFFSET) as u32,
                *mode_id,
            )
            .await?;
            for game in games {
                dependencies
                    .registry
                    .apply(event(&game, WatchEventKind::Live, None, None))?;
                // A uuid in the live list is a game that exists; the paipu just
                // is not written yet. Record it the moment it is seen, before
                // any fetching, so the fact survives whatever happens to the
                // working queue afterwards — including this process dying mid
                // round, or the entry being dropped as unfetchable.
                if !tracked.contains_key(&game.uuid) {
                    append_discovered(discovery_dir, &game);
                }
                live_now.insert(game.uuid.clone(), game);
            }
            tokio::time::sleep(Duration::from_millis(config.request_delay_ms)).await;
        }

        let now = now_unix();
        for (uuid, game) in tracked.iter() {
            if !live_now.contains_key(uuid) {
                let mut queued = game.clone();
                queued.queued_at = Some(now);
                dependencies.registry.apply(event(
                    &queued,
                    WatchEventKind::Pending,
                    Some("牌局已结束，等待牌谱生成".into()),
                    None,
                ))?;
                pending.entry(uuid.clone()).or_insert(queued);
            }
        }
        *tracked = live_now;
        persist_state(state_path, tracked.values(), pending.values())?;

        let batch: Vec<_> = pending.values().cloned().collect();
        for game in batch {
            if game
                .queued_at
                .is_some_and(|queued| now.saturating_sub(queued) < SETTLE_SECS)
            {
                continue;
            }
            dependencies
                .registry
                .apply(event(&game, WatchEventKind::Fetching, None, None))?;
            let raw =
                match fetch_game_record(transport, pb_worker, &game.uuid, client_version).await {
                    Ok(raw) => raw,
                    Err(error) => {
                        dependencies.registry.apply(event(
                            &game,
                            WatchEventKind::FetchFailed,
                            Some(error.to_string()),
                            None,
                        ))?;
                        if ends_the_session(&error) {
                            return Err(error);
                        }
                        // Only a business code reaches here, and it is what the
                        // give-up message below names as the reason.
                        let code = error
                            .downcast_ref::<ServerError>()
                            .map_or(0, |server| server.code);
                        // The paipu for a game can legitimately lag behind its
                        // disappearance from the live list, but not for hours;
                        // past that it is never coming and the entry would
                        // otherwise be retried every poll for the life of the
                        // process. Dropping it loses no information: the uuid
                        // was written to the discovery log when it was first
                        // seen live.
                        if game
                            .queued_at
                            .is_some_and(|queued| now.saturating_sub(queued) > GIVE_UP_SECS)
                        {
                            pending.remove(&game.uuid);
                            persist_state(state_path, tracked.values(), pending.values())?;
                            dependencies.logs.append(
                                WatchLogLevel::Warn,
                                source,
                                format!(
                                    "放弃对局 {} (服务端错误 {} 持续超过 {} 分钟)",
                                    game.uuid,
                                    code,
                                    GIVE_UP_SECS / 60
                                ),
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(config.request_delay_ms)).await;
                        continue;
                    }
                };
            dependencies
                .registry
                .apply(event(&game, WatchEventKind::Converting, None, None))?;
            let metadata = metadata_for(&game)?;
            match async {
                let (_, compressed) = convert_record_bytes(&raw, Some(&metadata))?;
                // `raw` is the protobuf the conversion read, and this is the
                // only moment it exists — nothing fetches it again. It goes to
                // ingest alongside the mjai so that whatever the converter does
                // not understand today is still recoverable tomorrow.
                ingest(&game.uuid, &compressed, &raw, dependencies).await
            }
            .await
            {
                Ok(record_id) => {
                    dependencies.registry.apply(event(
                        &game,
                        WatchEventKind::Completed,
                        // 打包和写索引已经移到 worker 里，这一步只保证记录已经
                        // 被 broker 确认，不再是“已经落到 pack 里”。
                        Some("已转换并提交到打包队列".into()),
                        Some(record_id),
                    ))?;
                    pending.remove(&game.uuid);
                }
                Err(error) => {
                    dependencies.registry.apply(event(
                        &game,
                        WatchEventKind::ConversionFailed,
                        Some(format!("{error:#}")),
                        None,
                    ))?;
                    // The same give-up the fetch path has, and for a sharper
                    // reason: this entry has already been fetched, so every
                    // retry costs a fetch *and* a conversion, and the pending
                    // loop is serial with a pacing delay after each. An ingest
                    // that keeps failing — a topic past `MJAI_KAFKA_MAX_LAG` is
                    // the reachable one — would otherwise grow this queue
                    // without bound and stretch the poll round with it, and a
                    // poll round that has stretched is games that were played
                    // and never seen live at all.
                    if game
                        .queued_at
                        .is_some_and(|queued| now.saturating_sub(queued) > GIVE_UP_SECS)
                    {
                        pending.remove(&game.uuid);
                        dependencies.logs.append(
                            WatchLogLevel::Warn,
                            source,
                            format!(
                                "放弃对局 {} (转换或入库连续失败超过 {} 分钟)",
                                game.uuid,
                                GIVE_UP_SECS / 60
                            ),
                        );
                    }
                }
            }
            persist_state(state_path, tracked.values(), pending.values())?;
            tokio::time::sleep(Duration::from_millis(config.request_delay_ms)).await;
        }

        // Whatever is left of the poll interval goes to the re-fetch backlog
        // instead of being slept through. An instance that ran long has no
        // spare time and serves nothing, which is what keeps this from ever
        // delaying live collection.
        let interval = Duration::from_secs(config.poll_interval_secs);
        let deadline = round_started + interval;
        serve_refetches(
            config,
            dependencies,
            source,
            client_version,
            transport,
            pb_worker,
            deadline,
        )
        .await?;
        let elapsed = round_started.elapsed();
        tokio::time::sleep(interval.saturating_sub(elapsed).max(Duration::from_secs(1))).await;
    }
}

/// Answers re-fetch requests until `deadline`, then returns.
///
/// The one thing this has to get right is which failures are the session's and
/// which are the record's, and it is the same rule the pending-game loop above
/// follows: a business error means Mahjong Soul answered and this uuid is not
/// coming, so it travels back to the waiter and the loop goes on; a stale
/// session or a transport failure means nothing else will succeed either, so it
/// ends the session and the caller reconnects.
async fn serve_refetches(
    config: &WatchServiceConfig,
    dependencies: &ManagedWatchDependencies,
    source: &str,
    client_version: &str,
    transport: &mut LoginTransport,
    pb_worker: Option<&Arc<PluginWorker>>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut served = 0usize;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let Some(request) = dependencies.refetch.claim() else {
            // Parked on the counter rather than on a timer, so a request that
            // arrives one second into a sixty second window is served then and
            // not a minute later.
            dependencies.refetch.wait_for_work(deadline - now).await;
            continue;
        };
        match fetch_game_record(transport, pb_worker, request.uuid(), client_version).await {
            Ok(raw) => {
                served += 1;
                request.answer(Ok(raw));
            }
            Err(error) => {
                // Answered before the session is torn down, so a waiter whose
                // request died with the session learns why instead of timing
                // out three minutes later.
                request.answer(Err(crate::refetch::RefetchError::Refused(format!(
                    "{error:#}"
                ))));
                if ends_the_session(&error) {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(config.request_delay_ms)).await;
    }
    if served > 0 {
        dependencies.logs.append(
            WatchLogLevel::Info,
            source,
            format!("空闲时段补抓了 {served} 局历史牌谱"),
        );
    }
    Ok(())
}

/// The live collector's ingest, which is the path that actually feeds the
/// corpus. It goes through the same claim-and-produce as the HTTP API rather
/// than keeping a second copy of it: the two drifted apart once already, and a
/// change that only fixed `src/api.rs` would leave production on the old one.
///
/// The key it passes no longer decides anything: `indexer::claim` reads the game
/// uuid out of the record and claims `majsoul-watch\0{uuid}` whatever it is
/// given, which is byte for byte what this path has always produced. The uuid is
/// still passed because it is the right fallback if a record ever reaches here
/// without a Majsoul header, and `majsoul-watch` is still the source because
/// that is the provenance every row it has ever written carries.
///
/// No `played_at` override travels with the record. This path used to derive one
/// from the uuid's `yymmdd` prefix, at midnight UTC, and that beat the record's
/// own header; but every converted record carries `majsoul.start_time`, a unix
/// second of the same day with the clock still on it, so the override could only
/// ever throw information away. The worker reads the header instead.
async fn ingest(
    game_uuid: &str,
    compressed: &[u8],
    majsoul_pb: &[u8],
    dependencies: &ManagedWatchDependencies,
) -> Result<Uuid> {
    let mut decoder = GzDecoder::new(compressed);
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw)?;
    let accepted = indexer::ingest_one(
        &dependencies.catalog,
        &dependencies.kafka,
        "majsoul-watch",
        game_uuid,
        None,
        &raw,
        Some(majsoul_pb),
    )
    .await?;
    Ok(accepted.id)
}

fn parse_live_list(data: &[u8], fallback_mode_id: i32) -> Result<Vec<LiveGame>> {
    let mut games = Vec::new();
    for field in FieldIterator::new(data) {
        let field = field?;
        if field.number == 1 && field.wire_type == 2 {
            for error in FieldIterator::new(field.data) {
                let error = error?;
                if error.number == 1 && error.wire_type == 0 && extract_varint(error.data)? != 0 {
                    anyhow::bail!("fetchGameLiveList returned an error");
                }
            }
        } else if field.number == 2
            && field.wire_type == 2
            && let Some(game) = parse_live_head(field.data, fallback_mode_id)?
        {
            games.push(game);
        }
    }
    Ok(games)
}

fn parse_live_head(data: &[u8], fallback_mode_id: i32) -> Result<Option<LiveGame>> {
    let mut uuid = String::new();
    let mut start_time = 0u32;
    let mut mode_id = 0i32;
    for field in FieldIterator::new(data) {
        let field = field?;
        match (field.number, field.wire_type) {
            (1, 2) => uuid = extract_string(field.data),
            (2, 0) => start_time = extract_varint(field.data)? as u32,
            (3, 2) => {
                for config in FieldIterator::new(field.data) {
                    let config = config?;
                    if config.number == 3 && config.wire_type == 2 {
                        for meta in FieldIterator::new(config.data) {
                            let meta = meta?;
                            if meta.number == 2 && meta.wire_type == 0 {
                                mode_id = extract_varint(meta.data)? as i32;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if mode_id == 0 {
        mode_id = fallback_mode_id;
    }
    if uuid.is_empty() || mode_metadata(mode_id).is_err() || uuid_year(&uuid).is_err() {
        return Ok(None);
    }
    Ok(Some(LiveGame {
        uuid,
        mode_id,
        start_time,
        queued_at: None,
    }))
}

fn metadata_for(game: &LiveGame) -> Result<GameMetadata> {
    let (room, game_length, players) = mode_metadata(game.mode_id)?;
    Ok(GameMetadata {
        mode_id: game.mode_id,
        room: room.into(),
        game_length: game_length.into(),
        players,
        year: uuid_year(&game.uuid)?,
    })
}

fn event(
    game: &LiveGame,
    kind: WatchEventKind,
    message: Option<String>,
    record_id: Option<Uuid>,
) -> WatchEvent {
    WatchEvent {
        uuid: game.uuid.clone(),
        event: kind,
        mode_id: Some(game.mode_id),
        started_at: Utc.timestamp_opt(game.start_time as i64, 0).single(),
        message,
        record_id,
    }
}

/// Every account in a secret, in file order.
///
/// A collector takes the first and ignores the rest; the re-fetch pool takes all
/// of them, which is the only reason this returns more than one. Reading the
/// same format from one place is what lets an operator point both at a single
/// file and have the collector claim line one while the pool works through what
/// is left — the pool drops any account a collector already holds.
pub fn load_accounts(
    secret_ref: &str,
    pool: &crate::accounts::AccountPool,
) -> Result<Vec<(String, String)>> {
    let (scheme, target) = secret_ref
        .split_once(':')
        .context("account secret reference has no scheme")?;
    let content = match scheme {
        "file" => std::fs::read_to_string(target)
            .with_context(|| format!("failed to read account secret file {target}"))?,
        "env" => std::env::var(target)
            .with_context(|| format!("account secret environment variable {target} is missing"))?,
        // Answered from the store rather than rendered into the text format and
        // parsed back. There is one representation of a console-managed account
        // and it is the document; a second one written to disk beside it is a
        // thing that can disagree, and what it would disagree about is which
        // account a session logs in with.
        "pool" => {
            let reference = crate::accounts::PoolRef::parse(target)
                .with_context(|| format!("账号池引用 pool:{target} 写法不对"))?;
            let accounts = pool.resolve(&reference);
            if accounts.is_empty() {
                anyhow::bail!("账号池里没有 pool:{target} 指向的可用账号（可能被停用或已删除）");
            }
            return Ok(accounts);
        }
        _ => anyhow::bail!("unsupported account secret scheme"),
    };
    let mut accounts = Vec::new();
    for (number, line) in content.lines().map(str::trim).enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let delimiter = if line.contains('\t') { '\t' } else { ',' };
        // Skipped rather than fatal, and this is load-bearing. A malformed line
        // used to be unreachable because the collector stopped at the first
        // usable one; now that the pool reads the whole file, one line missing
        // its comma would take down every collector reading that file at its
        // next start — for a typo in an account the collector does not even use.
        let Some((username, password)) = line.split_once(delimiter) else {
            warn!(
                line = number + 1,
                "跳过账号文件里没有分隔符的一行（需要 用户名,密码）"
            );
            continue;
        };
        if !username.trim().is_empty() && !password.trim().is_empty() {
            accounts.push((username.trim().to_owned(), password.trim().to_owned()));
        }
    }
    if accounts.is_empty() {
        anyhow::bail!("account secret contains no usable account");
    }
    Ok(accounts)
}

/// An account as it may appear in a log the whole console can read.
///
/// A Mahjong Soul username is half a credential, and `/api/v1/watch/logs` is
/// open to every member. Enough is kept to tell two accounts apart, which is all
/// a log line needs it for.
pub fn masked_account(username: &str) -> String {
    let (local, domain) = match username.split_once('@') {
        Some((local, domain)) => (local, format!("@{domain}")),
        None => (username, String::new()),
    };
    let kept: String = local.chars().take(2).collect();
    format!("{kept}***{domain}")
}

fn load_first_account(
    secret_ref: &str,
    pool: &crate::accounts::AccountPool,
) -> Result<(String, String)> {
    Ok(load_accounts(secret_ref, pool)?.swap_remove(0))
}

/// Append-only record of every game uuid this collector has ever seen live.
///
/// The state file is a working queue — entries leave it once fetched or given
/// up on — so it is not a record of what existed. This is: one JSON object per
/// line, appended the moment a uuid appears in the live list, rotated by UTC
/// day so a long-running collector does not accumulate one enormous file.
/// Failures are logged and swallowed: losing the audit trail must never stop
/// collection.
fn append_discovered(discovery_dir: &Path, game: &LiveGame) {
    let entry = serde_json::json!({
        "uuid": game.uuid,
        "mode_id": game.mode_id,
        "start_time": game.start_time,
        "discovered_at": Utc::now().to_rfc3339(),
    });
    let path = discovery_dir.join(format!("{}.jsonl", Utc::now().format("%Y-%m-%d")));
    let appended = std::fs::create_dir_all(discovery_dir).and_then(|()| {
        use std::io::Write as _;
        // O_APPEND keeps concurrent writers from interleaving a short line.
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        writeln!(file, "{entry}")
    });
    if let Err(error) = appended {
        warn!(error = %error, uuid = %game.uuid, "failed to record discovered game uuid");
    }
}

fn load_state(path: &Path) -> Result<(Vec<LiveGame>, Vec<LiveGame>)> {
    #[derive(Deserialize)]
    struct State {
        #[serde(default)]
        tracked: Vec<LiveGame>,
        #[serde(default)]
        pending: Vec<LiveGame>,
    }
    match std::fs::read(path) {
        Ok(bytes) => {
            let state: State = serde_json::from_slice(&bytes)?;
            Ok((state.tracked, state.pending))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok((Vec::new(), Vec::new())),
        Err(error) => Err(error.into()),
    }
}

fn persist_state<'a>(
    path: &Path,
    tracked: impl Iterator<Item = &'a LiveGame>,
    pending: impl Iterator<Item = &'a LiveGame>,
) -> Result<()> {
    #[derive(Serialize)]
    struct State<'a> {
        tracked: Vec<&'a LiveGame>,
        pending: Vec<&'a LiveGame>,
    }
    let state = State {
        tracked: tracked.collect(),
        pending: pending.collect(),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    std::fs::write(&temporary, serde_json::to_vec_pretty(&state)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store with nothing in it, for the `file:`/`env:` cases below. Those
    /// never consult it — which is the property being relied on, so the tests
    /// that read a file are also the tests that prove the store is not
    /// consulted when the reference does not name it.
    fn empty_pool() -> crate::accounts::AccountPool {
        let directory =
            std::env::temp_dir().join(format!("mjai-empty-pool-{}", uuid::Uuid::new_v4()));
        crate::accounts::AccountPool::open(&directory).expect("an empty store")
    }

    /// The rule every fetch loop shares: the live collector's pending queue, the
    /// collector's spare-time re-fetch serving, and the re-fetch pool's own
    /// workers all decide whether to reconnect with this one function.
    #[test]
    fn only_a_stale_session_or_transport_failure_reconnects() {
        // 1203 (record not available) is about one game. Reconnecting neither
        // fixes it nor lets the rest of the queue through, so the session must
        // survive it.
        let business = ensure_success_response(&[0x08, 0xb3, 0x09], "fetchGameRecord").unwrap_err();
        assert!(
            !ends_the_session(&business),
            "1203 must not kill the session"
        );

        // 1201 is ERR_TOKEN_NOT_EXIST: a fresh login is exactly the cure.
        let stale = ensure_success_response(&[0x08, 0xb1, 0x09], "fetchGameRecord").unwrap_err();
        assert!(ends_the_session(&stale), "a stale session must reconnect");

        // Anything that is not a server answer at all is a transport failure.
        assert!(ends_the_session(&anyhow::Error::msg("connection reset")));

        // The context wrapper `ensure_success_response` adds must not hide the
        // code from `downcast_ref`, or every business error would be read as a
        // dead session and the queue would reconnect in a loop forever.
        assert!(!ends_the_session(&business.context("fetching 260716-abc")));
    }

    /// Issue #37: renaming a collector used to move its state file, and a state
    /// file that has moved reads back as an empty queue rather than as an
    /// error, discarding every game already seen live and not yet fetched.
    #[test]
    fn renaming_a_collector_leaves_its_queue_and_its_discovery_log_where_they_are() {
        let data_dir = Path::new("/var/lib/mjai");
        let mut instance = WatchInstance {
            id: "three-player".into(),
            key: "three-player".into(),
            ..WatchInstance::default()
        };
        let queue = state_path(data_dir, &instance);
        let discovered = discovery_dir(data_dir, &instance);
        assert!(queue.ends_with("watch/state-three-player.json"));
        assert!(discovered.ends_with("watch/discovered/three-player"));

        instance.id = "sanma".into();
        assert_eq!(state_path(data_dir, &instance), queue);
        assert_eq!(discovery_dir(data_dir, &instance), discovered);
    }

    #[test]
    fn discovery_log_appends_every_uuid_and_survives_the_working_queue() {
        let dir = std::env::temp_dir().join(format!("mjai-disc-{}", Uuid::new_v4().simple()));
        let game = |uuid: &str| LiveGame {
            uuid: uuid.into(),
            mode_id: 12,
            start_time: 1_700_000_000,
            queued_at: None,
        };
        append_discovered(&dir, &game("260727-aaaa"));
        append_discovered(&dir, &game("260727-bbbb"));

        let file = std::fs::read_dir(&dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let lines: Vec<serde_json::Value> = std::fs::read_to_string(&file)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2, "each discovery is its own line");
        assert_eq!(lines[0]["uuid"], "260727-aaaa");
        assert_eq!(lines[1]["uuid"], "260727-bbbb");
        assert_eq!(lines[0]["mode_id"], 12);
        assert!(lines[0]["discovered_at"].is_string());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_round_trips_and_carries_patch_overflow() {
        assert_eq!(parse_version("0.16.257"), Some(16257));
        assert_eq!(format_version(16257), "0.16.257");
        assert_eq!(format_version(16999 + 3), "0.17.2");
        assert_eq!(parse_version("0.16.257.w"), None);
        assert_eq!(parse_version("1.0.0"), None);
        assert_eq!(parse_version("WebGL_2022-0.16.257"), None);
    }

    #[tokio::test]
    async fn finds_the_exact_version_floor() {
        // The observed live behaviour: 0.16.254 accepted, 0.16.253 rejected.
        for floor in [16254u32, 16255, 17000, 16240 + 1] {
            let probed = std::cell::Cell::new(0u32);
            let found = search_version_floor(16240, |value| {
                probed.set(probed.get() + 1);
                async move { Ok(value >= floor) }
            })
            .await
            .unwrap();
            assert_eq!(found, floor, "floor {floor}");
            // 6 escalation probes plus a binary search over the last bracket.
            // A floor a few patches away — the normal case — costs about 6.
            assert!(
                probed.get() <= 18,
                "floor {floor} took {} probes",
                probed.get()
            );
        }
    }

    #[tokio::test]
    async fn version_search_gives_up_rather_than_climbing_forever() {
        let result = search_version_floor(16240, |_| async { Ok(false) }).await;
        assert!(result.is_err());
    }

    #[test]
    fn loads_account_without_exposing_it_to_config_json() {
        let variable = format!("MJAI_TEST_ACCOUNT_{}", Uuid::new_v4());
        // SAFETY: this uniquely named variable is only read in this test.
        unsafe { std::env::set_var(&variable, "bot@example.com,password") };
        let account = load_first_account(&format!("env:{variable}"), &empty_pool()).unwrap();
        assert_eq!(account.0, "bot@example.com");
        unsafe { std::env::remove_var(variable) };
    }

    /// One malformed line used to be unreachable — the collector stopped at the
    /// first usable account and never looked further. Now that the pool reads
    /// the whole file, a missing comma on line seven would, if it were fatal,
    /// stop every collector reading that file at its next start: an account the
    /// collector does not even use taking down live collection.
    #[test]
    fn a_malformed_line_costs_that_account_and_nothing_else() {
        let variable = format!("MJAI_TEST_BROKEN_{}", Uuid::new_v4());
        // SAFETY: this uniquely named variable is only read in this test.
        unsafe {
            std::env::set_var(
                &variable,
                "live@example.com,one\nthis-line-has-no-separator\npool@example.com,two\n",
            )
        };
        let reference = format!("env:{variable}");
        assert_eq!(
            load_accounts(&reference, &empty_pool()).unwrap(),
            vec![
                ("live@example.com".to_owned(), "one".to_owned()),
                ("pool@example.com".to_owned(), "two".to_owned()),
            ]
        );
        assert_eq!(
            load_first_account(&reference, &empty_pool()).unwrap().0,
            "live@example.com"
        );
        unsafe { std::env::remove_var(variable) };
    }

    /// The log buffer behind `/api/v1/watch/logs` is readable by every console
    /// member, and a Mahjong Soul username is half a credential.
    #[test]
    fn a_log_line_names_an_account_without_giving_it_away() {
        let masked = masked_account("collector-bot@example.com");
        assert_eq!(masked, "co***@example.com");
        assert_ne!(masked_account("aa-one@example.com"), masked);
        // A phone number has no domain to keep.
        assert_eq!(masked_account("13800000000"), "13***");
        assert_eq!(masked_account(""), "***");
    }

    /// The re-fetch pool's whole capacity is the number of accounts it can read,
    /// and a collector pointed at the same secret has to keep taking exactly the
    /// first one — otherwise pointing both at one file would silently move which
    /// account the live collector logs in with.
    #[test]
    fn reads_every_account_in_a_secret_while_a_collector_still_takes_the_first() {
        let variable = format!("MJAI_TEST_POOL_{}", Uuid::new_v4());
        // SAFETY: this uniquely named variable is only read in this test.
        unsafe {
            std::env::set_var(
                &variable,
                "# collector\nlive@example.com,one\n\n  pool-a@example.com , two \n\
                 pool-b@example.com\tthree\n",
            )
        };
        let reference = format!("env:{variable}");
        let accounts = load_accounts(&reference, &empty_pool()).unwrap();
        assert_eq!(
            accounts,
            vec![
                ("live@example.com".to_owned(), "one".to_owned()),
                ("pool-a@example.com".to_owned(), "two".to_owned()),
                ("pool-b@example.com".to_owned(), "three".to_owned()),
            ],
            "comments and blank lines are skipped, tabs and commas both split"
        );
        assert_eq!(
            load_first_account(&reference, &empty_pool()).unwrap().0,
            "live@example.com"
        );
        unsafe { std::env::remove_var(variable) };
    }
}
