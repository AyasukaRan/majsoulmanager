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
use sha2::{Digest, Sha256};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    catalog::{Catalog, IdempotencyClaim, Record},
    majsoul::{
        BROWSER_USER_AGENT,
        convert::{GameMetadata, convert_record_bytes},
        gateway::{discover_gateway, discover_package_version},
        modes::{mode_metadata, room_modes, uuid_year},
        proto::{FieldIterator, extract_string, extract_varint},
        rpc::{
            FETCH_GAME_LIVE_LIST_METHOD, FETCH_GAME_RECORD_METHOD, MajsoulRpc,
            build_fetch_game_live_list_request, build_fetch_game_record_request,
            ensure_success_response,
        },
    },
    mihomo::MihomoManager,
    mjai,
    pack::PackStore,
    watch::{WatchEvent, WatchEventKind, WatchRegistry},
    watch_log::{WatchLogBuffer, WatchLogLevel},
    watch_service::{PluginWorker, WatchInstance, WatchProxyMode, WatchServiceConfig},
};

const FILTER_ID_OFFSET: i32 = 200;
const SETTLE_SECS: u64 = 120;
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
    pub catalog: Arc<Catalog>,
    pub packs: Arc<PackStore>,
    pub registry: Arc<WatchRegistry>,
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

enum LoginTransport {
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

    async fn close(&self) {
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
    let (username, password) = load_first_account(&instance.account_secret_ref)?;
    let proxy = match config.proxy_mode {
        WatchProxyMode::Direct => None,
        WatchProxyMode::Mihomo => Some(dependencies.mihomo.proxy_url().to_owned()),
        WatchProxyMode::Custom => config.custom_proxy_url.clone(),
    };
    // 外置模块的 stderr 与错误链会进入 Web 可见的日志缓冲,可能回显
    // open_session 参数;先注册机密让 append 统一脱敏。
    dependencies.logs.register_secret(password.clone());
    if let Some(proxy_url) = proxy.as_deref() {
        dependencies.logs.register_secret(proxy_url);
        if let Ok(parsed) = reqwest::Url::parse(proxy_url) {
            if let Some(proxy_password) = parsed.password() {
                dependencies.logs.register_secret(proxy_password);
            }
            if !parsed.username().is_empty() {
                dependencies.logs.register_secret(parsed.username());
            }
        }
    }
    let state_path = dependencies
        .data_dir
        .join(format!("watch/state-{}.json", instance.id));
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
            format!("开始连接雀魂服务器 (账号 {username}, 代理 {proxy_label})"),
        );
        match connect(
            &config,
            &instance,
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
                    &modes,
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
                if detail.contains("151") {
                    let rejected = instance
                        .client_version
                        .clone()
                        .unwrap_or_else(|| CN_CODE_VERSION.to_string());
                    // A sibling instance hits the same server-wide bump at the
                    // same time. Whoever finishes first publishes the floor, so
                    // check for it before paying for a second search.
                    let shared = load_client_version(&cache_dir)
                        .filter(|shared| parse_version(shared) > parse_version(&rejected));
                    if let Some(shared) = shared {
                        dependencies.logs.append(
                            WatchLogLevel::Info,
                            &source,
                            format!(
                                "error 151 = 客户端版本 {rejected} 已过期,采用其他实例探测到的 {shared}"
                            ),
                        );
                        instance.client_version = Some(shared);
                    } else {
                        dependencies.logs.append(
                            WatchLogLevel::Warn,
                            &source,
                            format!(
                                "error 151 = 客户端版本 {rejected} 已过期,开始探测服务端接受的最低版本"
                            ),
                        );
                        match discover_version_floor(
                            &config,
                            &instance,
                            &username,
                            &password,
                            proxy.as_deref(),
                            login_worker.clone(),
                            &dependencies.logs,
                            &source,
                            &cache_dir,
                            &rejected,
                        )
                        .await
                        {
                            Ok(version) => {
                                store_client_version(&cache_dir, &version);
                                instance.client_version = Some(version.clone());
                                dependencies.logs.append(
                                    WatchLogLevel::Info,
                                    &source,
                                    format!("客户端版本已自动更新为 {version}"),
                                );
                            }
                            Err(error) => dependencies.logs.append(
                                WatchLogLevel::Error,
                                &source,
                                format!("版本探测失败: {error:#}"),
                            ),
                        }
                    }
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

/// Renders a proxy URL as `scheme://host[:port]`, never exposing credentials.
fn proxy_display(proxy: &str) -> String {
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

async fn fetch_game_record(
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

/// Try one candidate version. `Ok(false)` means the server rejected it as too
/// old (151); any other failure is a real error and must abort the search
/// rather than be mistaken for a rejection.
#[allow(clippy::too_many_arguments)]
async fn probe_version(
    config: &WatchServiceConfig,
    instance: &WatchInstance,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
    logs: &WatchLogBuffer,
    source: &str,
    cache_dir: &Path,
    version: &str,
) -> Result<bool> {
    let mut candidate = instance.clone();
    candidate.client_version = Some(version.to_string());
    match connect(
        config, &candidate, username, password, proxy, worker, logs, source, cache_dir,
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
async fn discover_version_floor(
    config: &WatchServiceConfig,
    instance: &WatchInstance,
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
                config, instance, username, password, proxy, worker, logs, source, cache_dir,
                &version,
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

#[allow(clippy::too_many_arguments)]
async fn connect(
    config: &WatchServiceConfig,
    instance: &WatchInstance,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
    logs: &WatchLogBuffer,
    source: &str,
    cache_dir: &Path,
) -> Result<(LoginTransport, String)> {
    if let Some(worker) = worker {
        let result = worker
            .request(
                "open_session",
                serde_json::json!({
                    "server": config.server,
                    "username": username,
                    "password": password,
                    "proxy_url": proxy,
                    "client_version": instance.client_version,
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
        discover_gateway(&http, &config.server, cache_dir).await?;
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
    let code_version = instance
        .client_version
        .clone()
        .unwrap_or_else(|| CN_CODE_VERSION.to_string());
    let package_version = discover_package_version(&http, &config.server, cache_dir)
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
        &config.server,
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
    modes: &[i32],
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
                        return Err(error);
                    }
                };
            dependencies
                .registry
                .apply(event(&game, WatchEventKind::Converting, None, None))?;
            let metadata = metadata_for(&game)?;
            match convert_record_bytes(&raw, Some(&metadata))
                .and_then(|(_, compressed)| ingest(&game.uuid, &compressed, dependencies))
            {
                Ok(record_id) => {
                    dependencies.registry.apply(event(
                        &game,
                        WatchEventKind::Completed,
                        Some("已转换并写入 mjai pack".into()),
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
                }
            }
            persist_state(state_path, tracked.values(), pending.values())?;
            tokio::time::sleep(Duration::from_millis(config.request_delay_ms)).await;
        }

        let elapsed = round_started.elapsed();
        let interval = Duration::from_secs(config.poll_interval_secs);
        tokio::time::sleep(interval.saturating_sub(elapsed).max(Duration::from_secs(1))).await;
    }
}

fn ingest(
    game_uuid: &str,
    compressed: &[u8],
    dependencies: &ManagedWatchDependencies,
) -> Result<Uuid> {
    let mut decoder = GzDecoder::new(compressed);
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw)?;
    let metadata = mjai::parse_metadata(&raw)?;
    let sha256 = hex::encode(Sha256::digest(&raw));
    let id = Uuid::new_v4();
    let idempotency_key = format!("majsoul-watch\0{game_uuid}");
    match dependencies.catalog.claim(&idempotency_key, id, &sha256)? {
        IdempotencyClaim::Existing(record) => Ok(record.id),
        IdempotencyClaim::New => {
            let location = match dependencies.packs.append(id, &raw) {
                Ok(location) => location,
                Err(error) => {
                    dependencies.catalog.abandon_claim(&idempotency_key, id);
                    return Err(error.into());
                }
            };
            dependencies.catalog.insert(Record {
                id,
                source: "majsoul-watch".into(),
                sha256,
                received_at: Utc::now(),
                played_at: game_uuid
                    .get(0..6)
                    .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%y%m%d").ok())
                    .and_then(|date| date.and_hms_opt(0, 0, 0))
                    .map(|date| Utc.from_utc_datetime(&date)),
                players: metadata.players,
                rule: metadata.rule,
                event_count: metadata.event_count,
                storage: location,
            });
            Ok(id)
        }
    }
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

fn load_first_account(secret_ref: &str) -> Result<(String, String)> {
    let (scheme, target) = secret_ref
        .split_once(':')
        .context("account secret reference has no scheme")?;
    let content = match scheme {
        "file" => std::fs::read_to_string(target)
            .with_context(|| format!("failed to read account secret file {target}"))?,
        "env" => std::env::var(target)
            .with_context(|| format!("account secret environment variable {target} is missing"))?,
        _ => anyhow::bail!("unsupported account secret scheme"),
    };
    for line in content.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let delimiter = if line.contains('\t') { '\t' } else { ',' };
        let (username, password) = line
            .split_once(delimiter)
            .context("account secret must contain username,password")?;
        if !username.trim().is_empty() && !password.trim().is_empty() {
            return Ok((username.trim().into(), password.trim().into()));
        }
    }
    anyhow::bail!("account secret contains no usable account")
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
        let account = load_first_account(&format!("env:{variable}")).unwrap();
        assert_eq!(account.0, "bot@example.com");
        unsafe { std::env::remove_var(variable) };
    }
}
