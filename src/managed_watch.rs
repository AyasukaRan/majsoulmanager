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
        gateway::{discover_client_version, discover_gateway},
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
    watch_service::{PluginWorker, WatchProxyMode, WatchServiceConfig},
};

const FILTER_ID_OFFSET: i32 = 200;
const SETTLE_SECS: u64 = 120;

pub struct ManagedWatchDependencies {
    pub data_dir: PathBuf,
    pub catalog: Arc<Catalog>,
    pub packs: Arc<PackStore>,
    pub registry: Arc<WatchRegistry>,
    pub mihomo: Arc<MihomoManager>,
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
    dependencies: Arc<ManagedWatchDependencies>,
    login_worker: Option<Arc<PluginWorker>>,
    pb_worker: Option<Arc<PluginWorker>>,
) -> Result<()> {
    let modes = room_modes(&config.room, &config.modes, config.players)?;
    let (username, password) = load_first_account(&config.account_secret_ref)?;
    let proxy = match config.proxy_mode {
        WatchProxyMode::Direct => None,
        WatchProxyMode::Mihomo => Some(dependencies.mihomo.proxy_url().to_owned()),
        WatchProxyMode::Custom => config.custom_proxy_url.clone(),
    };
    let state_path = dependencies.data_dir.join("watch/state.json");
    let (tracked_state, pending_state) = load_state(&state_path)?;
    let mut tracked = tracked_state
        .into_iter()
        .map(|game| (game.uuid.clone(), game))
        .collect::<HashMap<_, _>>();
    let mut pending = pending_state
        .into_iter()
        .map(|game| (game.uuid.clone(), game))
        .collect::<HashMap<_, _>>();

    loop {
        match connect(
            &config,
            &username,
            &password,
            proxy.as_deref(),
            login_worker.clone(),
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
                }
                transport.close().await;
            }
            Err(error) => {
                warn!(error = %format!("{error:#}"), "watch login failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
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

async fn connect(
    config: &WatchServiceConfig,
    username: &str,
    password: &str,
    proxy: Option<&str>,
    worker: Option<Arc<PluginWorker>>,
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
                    "client_version": config.client_version,
                }),
            )
            .await
            .map_err(|error| anyhow::Error::msg(error.to_string()))?;
        let session_id = required_string(&result, "session_id")?;
        let client_version = required_string(&result, "client_version")?;
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
    let (endpoint, resource_version, route_id) = discover_gateway(&http, &config.server).await?;
    let client_version = match &config.client_version {
        Some(version) => version.clone(),
        None => discover_client_version(&http, &config.server, &resource_version).await?,
    };
    let rpc = MajsoulRpc::connect_with_proxy(&endpoint, proxy).await?;
    rpc.login_native_exact(username, password, &client_version, &route_id)
        .await?;
    Ok((LoginTransport::Builtin(rpc), client_version))
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
    fn loads_account_without_exposing_it_to_config_json() {
        let variable = format!("MJAI_TEST_ACCOUNT_{}", Uuid::new_v4());
        // SAFETY: this uniquely named variable is only read in this test.
        unsafe { std::env::set_var(&variable, "bot@example.com,password") };
        let account = load_first_account(&format!("env:{variable}")).unwrap();
        assert_eq!(account.0, "bot@example.com");
        unsafe { std::env::remove_var(variable) };
    }
}
