use std::{
    io::{Read, Seek, Write},
    path::Path,
    str::FromStr,
};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path as AxumPath, Query, State},
    http::{
        HeaderMap, HeaderValue, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::{DateTime, Utc};
use flate2::{Compression, write::GzEncoder};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tar::{Builder as TarBuilder, Header as TarHeader};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    AppState,
    accounts::{AccountDocument, AccountPurpose},
    auth::{
        AuthError, AuthSettings, CreateUserRequest, LoginRequest, LoginResponse, RegisterRequest,
        RegistrationStatus, UpdateUserRequest, UserView, VerifyEmailRequest,
    },
    catalog::{
        CatalogError, Cursor, DEFAULT_TREND_SPAN, DownloadCounts, DownloadFormat, DownloadJob,
        DownloadRequest, JobState, PaipuyaGame, PaipuyaGap, PaipuyaTotals, PlayerHit,
        PlayerSummary, Record, RecordFilter, RecordStats, RuleFilter, Series, SeriesUnit,
        SeriesWindow, StorageStats,
    },
    indexer::{self, Claimed, IngestError},
    kafka::MAX_PRODUCE_BATCH_BYTES,
    mihomo::{MihomoAction, MihomoError, MihomoStatus, ProxySelection, SubscriptionUpdate},
    paipuya::{PaipuyaConfig, PaipuyaStatus},
    refetch_service::{RefetchRuntimeStatus, RefetchServiceConfig},
    watch_log::WatchLogEntry,
    watch_service::{
        InstallModuleRequest, InstalledModule, ServicePhase, WatchAction, WatchDashboard,
        WatchRuntimeStatus, WatchServiceConfig, WatchServiceError, module_protocol_contract,
    },
};

pub fn router(state: AppState) -> Router {
    let max_batch = state.config.max_batch_bytes;
    // Everything that changes what the collectors do, kept in one table so that
    // the rule is stated once instead of being remembered six times. See
    // `require_admin_session` for why the machine key is not enough here, and
    // why the reads below it deliberately stay open to every member.
    let admin = Router::new()
        .route("/api/v1/watch/config", put(put_watch_config))
        .route("/api/v1/watch/actions", post(post_watch_action))
        .route("/api/v1/watch/modules", post(install_watch_module))
        .route(
            "/api/v1/watch/proxy/subscription",
            put(put_watch_proxy_subscription),
        )
        .route(
            "/api/v1/watch/proxy/selection",
            put(put_watch_proxy_selection),
        )
        .route("/api/v1/watch/proxy/actions", post(post_watch_proxy_action))
        // The re-fetch pool logs in with its own accounts and hammers Mahjong
        // Soul for as long as the backlog lasts, so starting it and choosing its
        // pacing belong to the same people who may start a collector.
        // The account pool. Writing it is an administrator's job for the same
        // reason starting a collector is: what it decides is which Mahjong Soul
        // accounts this deployment logs in with, and one of the two halves it
        // feeds cannot be redone if it is disconnected.
        // Read as well as written by administrators only. The passwords are
        // redacted, but a Mahjong Soul login is half a credential and the rest
        // of this codebase treats it that way — `masked_account` exists so the
        // console's log panel, which every member can read, never shows one in
        // full. Serving the whole list to the same members would undo that.
        .route("/api/v1/accounts", get(get_accounts).put(put_accounts))
        .route("/api/v1/refetch/config", put(put_refetch_config))
        .route("/api/v1/refetch/actions", post(post_refetch_action))
        // Loading the catalogue changes what the pool will go and fetch, so it
        // sits with the rest of what an administrator decides.
        .route("/api/v1/paipuya/games", post(post_paipuya_games))
        .route("/api/v1/paipuya/config", put(put_paipuya_config))
        .route("/api/v1/paipuya/actions", post(post_paipuya_action))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_session,
        ));
    let protected = Router::new()
        .route("/api/v1/records", post(ingest).get(search))
        .route("/api/v1/records/batch", post(ingest_batch))
        .route("/api/v1/records/{id}", get(get_record))
        .route("/api/v1/records/{id}/raw", get(get_raw))
        .route("/api/v1/records/{id}/majsoul-pb", get(get_majsoul_pb))
        .route("/api/v1/stats", get(get_stats))
        .route("/api/v1/stats/series", get(get_series_stats))
        .route("/api/v1/players", get(search_players))
        .route("/api/v1/players/stats", get(get_player_stats))
        .route(
            "/api/v1/downloads",
            post(create_download).get(list_downloads),
        )
        .route("/api/v1/downloads/{id}", get(get_download))
        .route("/api/v1/downloads/{id}/file", get(download_file))
        .route("/api/v1/watch/status", get(get_watch_status))
        .route("/api/v1/watch/logs", get(get_watch_logs))
        .route("/api/v1/watch/config", get(get_watch_config))
        .route("/api/v1/watch/modules", get(get_watch_modules))
        .route("/api/v1/watch/modules/protocol", get(get_module_protocol))
        .route("/api/v1/watch/proxy", get(get_watch_proxy))
        .route("/api/v1/refetch/config", get(get_refetch_config))
        .route("/api/v1/refetch/status", get(get_refetch_status))
        .route("/api/v1/paipuya/gap", get(get_paipuya_gap))
        .route("/api/v1/paipuya/config", get(get_paipuya_config))
        .route("/api/v1/paipuya/status", get(get_paipuya_status))
        .merge(admin)
        .route("/api/v1/users", get(get_users).post(create_user))
        .route("/api/v1/users/{id}", put(update_user))
        .route(
            "/api/v1/auth/settings",
            get(get_auth_settings).put(put_auth_settings),
        )
        .layer(DefaultBodyLimit::max(max_batch))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));
    Router::new()
        .route("/healthz", get(health))
        .route("/api/v1/auth/status", get(get_registration_status))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/register", post(register))
        .route("/api/v1/auth/verify-email", post(verify_email))
        .route("/api/v1/auth/me", get(get_current_user))
        .route("/api/v1/auth/logout", post(logout))
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

const USER_SESSION_HEADER: &str = "x-mjai-user-session";

async fn get_registration_status(State(state): State<AppState>) -> Json<RegistrationStatus> {
    Json(state.auth.registration_status())
}

async fn login(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    Ok(Json(state.auth.login(request)?))
}

async fn register(
    State(state): State<AppState>,
    Json(request): Json<RegisterRequest>,
) -> Result<StatusCode, ApiError> {
    state.auth.register(request).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn verify_email(
    State(state): State<AppState>,
    Json(request): Json<VerifyEmailRequest>,
) -> Result<StatusCode, ApiError> {
    state.auth.verify_email(request)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_current_user(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<UserView>, ApiError> {
    Ok(Json(state.auth.user_for_session(user_session(&headers)?)?))
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiError> {
    state.auth.logout(user_session(&headers)?);
    Ok(StatusCode::NO_CONTENT)
}

async fn get_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<UserView>>, ApiError> {
    Ok(Json(state.auth.users(user_session(&headers)?)?))
}

async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserView>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(state.auth.create_user(user_session(&headers)?, request)?),
    ))
}

async fn update_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<UpdateUserRequest>,
) -> Result<Json<UserView>, ApiError> {
    Ok(Json(state.auth.update_user(
        user_session(&headers)?,
        id,
        request,
    )?))
}

async fn get_auth_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<RegistrationStatus>, ApiError> {
    Ok(Json(state.auth.settings(user_session(&headers)?)?))
}

async fn put_auth_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(settings): Json<AuthSettings>,
) -> Result<Json<RegistrationStatus>, ApiError> {
    Ok(Json(
        state
            .auth
            .update_settings(user_session(&headers)?, settings)?,
    ))
}

fn user_session(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(USER_SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(ApiError::Unauthorized)
}

#[derive(Deserialize)]
struct WatchQuery {
    state: Option<String>,
    #[serde(default = "default_watch_limit")]
    limit: usize,
}

fn default_watch_limit() -> usize {
    50
}

async fn get_watch_status(
    State(state): State<AppState>,
    Query(query): Query<WatchQuery>,
) -> Result<Json<WatchDashboard>, ApiError> {
    if !(1..=1000).contains(&query.limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 1000".into(),
        ));
    }
    Ok(Json(
        state
            .watch_service
            .dashboard(query.state.as_deref(), query.limit),
    ))
}

#[derive(Deserialize)]
struct WatchLogQuery {
    after: Option<u64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct WatchLogPage {
    boot_id: Uuid,
    items: Vec<WatchLogEntry>,
    next_cursor: Option<u64>,
}

async fn get_watch_logs(
    State(state): State<AppState>,
    Query(query): Query<WatchLogQuery>,
) -> Result<Json<WatchLogPage>, ApiError> {
    let limit = query.limit.unwrap_or(200);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 1000".into(),
        ));
    }
    let items = state
        .watch_service
        .logs_after(query.after.unwrap_or(0), limit);
    let next_cursor = items.last().map(|entry| entry.seq);
    Ok(Json(WatchLogPage {
        boot_id: state.watch_service.log_buffer().boot_id(),
        items,
        next_cursor,
    }))
}

/// Open to every member, like the other reads below `require_admin_session`. The
/// document names its secrets rather than carrying them — except the proxy URL,
/// which may carry credentials inline, so `published_config` takes those out.
async fn get_watch_config(State(state): State<AppState>) -> Json<WatchServiceConfig> {
    Json(state.watch_service.published_config())
}

async fn put_watch_config(
    State(state): State<AppState>,
    Json(config): Json<WatchServiceConfig>,
) -> Result<Json<WatchServiceConfig>, ApiError> {
    Ok(Json(state.watch_service.update_config(config).await?))
}

#[derive(Deserialize)]
struct WatchActionRequest {
    action: WatchAction,
}

async fn post_watch_action(
    State(state): State<AppState>,
    Json(request): Json<WatchActionRequest>,
) -> Result<Json<WatchRuntimeStatus>, ApiError> {
    Ok(Json(
        state.watch_service.apply_action(request.action).await?,
    ))
}

async fn get_accounts(State(state): State<AppState>) -> Json<AccountDocument> {
    Json(state.accounts.published())
}

async fn put_accounts(
    State(state): State<AppState>,
    Json(document): Json<AccountDocument>,
) -> Result<Json<AccountDocument>, ApiError> {
    // An account a collector is pointed at cannot be taken away from it here.
    // Live collection reads its credentials once, when the instance starts, and
    // keeps using them for the life of the process — so switching one off,
    // deleting it, or moving it to the re-fetch pool does not stop the session,
    // it only stops anything from knowing the session exists. The pool would
    // then be handed an account that is still logged in, the two would kick
    // each other off, and the games being played meanwhile are the ones nothing
    // lists twice. Refusing is the only answer that is true to the button.
    let in_use = state.watch_service.referenced_accounts(&state.accounts);
    for account in &document.accounts {
        let still_collecting = in_use.contains(&account.username.to_lowercase());
        if still_collecting && (!account.enabled || account.purpose != AccountPurpose::Watch) {
            return Err(WatchServiceError::InvalidConfig(format!(
                "账号 {} 正被采集实例使用，不能在这里停用或改用途；\
                 先去 Watch 设置把那个实例指向别的账号或停掉它",
                account.username
            ))
            .into());
        }
    }
    let removed: Vec<&String> = in_use
        .iter()
        .filter(|username| {
            !document
                .accounts
                .iter()
                .any(|account| account.username.to_lowercase() == ***username)
        })
        .collect();
    if let Some(username) = removed.first() {
        return Err(WatchServiceError::InvalidConfig(format!(
            "有采集实例正指向账号 {username}，不能删除；先改那个实例的账号引用"
        ))
        .into());
    }

    let saved = state.accounts.update(document)?;
    // Registered after the write, not at boot: a password added from the
    // console has to reach the redactor before anything can echo it, and the
    // one-shot registration in `AppState::local` ran before it existed.
    for password in state.accounts.secrets() {
        state.watch_service.log_buffer().register_secret(password);
    }
    // The nodes the pool is now spread over, handed to mihomo so it grows or
    // drops the listeners behind them. Deliberately not fatal to the save: the
    // accounts are already written, and an outbound that did not come up costs
    // the accounts on it their node — they fall back to the re-fetch lane —
    // rather than costing the operator the edit they just made. The proxy card
    // reads the outbounds back from mihomo and says which of the two happened.
    if let Err(error) = state
        .mihomo
        .set_outbound_nodes(&state.accounts.refetch_nodes())
    {
        tracing::warn!(%error, "账号池的节点绑定没能写进 mihomo 配置");
    } else {
        let mihomo = std::sync::Arc::clone(&state.mihomo);
        tokio::spawn(async move { mihomo.apply_runtime_config().await });
    }
    Ok(Json(saved))
}

async fn get_refetch_config(State(state): State<AppState>) -> Json<RefetchServiceConfig> {
    Json(state.refetch_service.published_config())
}

async fn put_refetch_config(
    State(state): State<AppState>,
    Json(config): Json<RefetchServiceConfig>,
) -> Result<Json<RefetchServiceConfig>, ApiError> {
    Ok(Json(state.refetch_service.update_config(config).await?))
}

async fn get_refetch_status(State(state): State<AppState>) -> Json<RefetchRuntimeStatus> {
    Json(state.refetch_service.status())
}

async fn post_refetch_action(
    State(state): State<AppState>,
    Json(request): Json<WatchActionRequest>,
) -> Result<Json<RefetchRuntimeStatus>, ApiError> {
    Ok(Json(
        state.refetch_service.apply_action(request.action).await?,
    ))
}

async fn get_watch_modules(
    State(state): State<AppState>,
) -> Result<Json<Vec<InstalledModule>>, ApiError> {
    Ok(Json(state.watch_service.modules()?))
}

async fn install_watch_module(
    State(state): State<AppState>,
    Json(request): Json<InstallModuleRequest>,
) -> Result<(StatusCode, Json<InstalledModule>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(state.watch_service.install_module(request).await?),
    ))
}

async fn get_module_protocol() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "protocol_version": crate::watch_service::MODULE_PROTOCOL_VERSION,
        "contract": module_protocol_contract(),
    }))
}

async fn get_watch_proxy(State(state): State<AppState>) -> Json<MihomoStatus> {
    Json(state.mihomo.status().await)
}

async fn put_watch_proxy_subscription(
    State(state): State<AppState>,
    Json(update): Json<SubscriptionUpdate>,
) -> Result<Json<MihomoStatus>, ApiError> {
    Ok(Json(state.mihomo.update_subscription(update).await?))
}

async fn put_watch_proxy_selection(
    State(state): State<AppState>,
    Json(selection): Json<ProxySelection>,
) -> Result<Json<MihomoStatus>, ApiError> {
    Ok(Json(state.mihomo.select(selection).await?))
}

#[derive(Deserialize)]
struct MihomoActionRequest {
    action: MihomoAction,
}

async fn post_watch_proxy_action(
    State(state): State<AppState>,
    Json(request): Json<MihomoActionRequest>,
) -> Result<Json<MihomoStatus>, ApiError> {
    Ok(Json(state.mihomo.action(request.action).await?))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// The guard on everything that changes what the collectors do.
///
/// `require_auth` proves only that a request came from something holding the
/// machine key, and the console holds that key on behalf of every user it
/// serves: it attaches the key itself and forwards whatever the browser asked
/// for. The key therefore cannot say who is behind a request, and a route that
/// asks nothing further is open to every member with a console login, whatever
/// the console chooses to render for them. Only the session carries an identity,
/// which is why this refuses a request that presents none even though the key
/// alone would otherwise be enough — an operator working outside the console
/// takes one from `POST /api/v1/auth/login`.
///
/// Reads are deliberately not behind this. The status, the logs, the
/// configuration document, the installed modules and the proxy status are what
/// the monitoring page shows every member, and none of them carries a secret:
/// `account_secret_ref` names an environment variable rather than holding one.
///
/// It is a layer rather than a line in each handler so that a route added to the
/// table above is covered by where it was written down, and so that a member is
/// refused before a body is parsed rather than after.
async fn require_admin_session(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let session = request
        .headers()
        .get(USER_SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    match session.map(|session| state.auth.require_admin(session)) {
        Some(Ok(_)) => next.run(request).await,
        Some(Err(error)) => ApiError::from(error).into_response(),
        None => ApiError::Unauthorized.into_response(),
    }
}

async fn require_auth(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let expected = format!("Bearer {}", state.config.api_key);
    let authorized = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|provided| {
            provided.len() == expected.len()
                && provided.as_bytes().ct_eq(expected.as_bytes()).into()
        });
    if authorized {
        next.run(request).await
    } else {
        ApiError::Unauthorized.into_response()
    }
}

#[derive(Serialize)]
struct IngestResponse {
    id: Uuid,
    /// `accepted`, not `indexed`: the record is durably in the topic and the
    /// pack worker has not seen it yet, so a read of it can still answer 404
    /// for as long as the worker takes to seal the pack it lands in.
    status: &'static str,
    duplicate: bool,
    sha256: String,
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if body.is_empty() || body.len() > state.config.max_record_bytes {
        return Err(ApiError::PayloadTooLarge(state.config.max_record_bytes));
    }
    let idempotency_key = required_header(&headers, "idempotency-key")?;
    let source = required_header(&headers, "x-mjai-source")?;
    if idempotency_key.len() > 256 || source.len() > 128 {
        return Err(ApiError::BadRequest(
            "collector headers are too long".into(),
        ));
    }
    let played_at = optional_header(&headers, "x-mjai-played-at")
        .map(|value| {
            value
                .parse::<DateTime<Utc>>()
                .map_err(|_| ApiError::BadRequest("X-Mjai-Played-At must be RFC 3339".into()))
        })
        .transpose()?;
    let response = ingest_one(&state, &idempotency_key, &source, played_at, body.as_ref()).await?;
    let status = if response.duplicate {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    Ok((status, Json(response)))
}

/// Validate, hash, claim, produce, `202`. Nothing here packs or indexes any
/// more: the record is handed to the topic and the pack worker does both.
///
/// A batch carries one `X-Mjai-Played-At` header for tens of thousands of
/// games, so the record's own `majsoul.start_time` is the only usable
/// `played_at` and the header stays an override. The worker resolves that,
/// because only it parses the record for the index row.
async fn ingest_one(
    state: &AppState,
    idempotency_key: &str,
    source: &str,
    played_at: Option<DateTime<Utc>>,
    body: &[u8],
) -> Result<IngestResponse, ApiError> {
    check_record_size(state, body)?;
    // No protobuf: an upload is already mjai, and whatever it was converted
    // from happened somewhere this process cannot see.
    let accepted = indexer::ingest_one(
        &state.catalog,
        &state.kafka,
        source,
        idempotency_key,
        played_at,
        body,
        None,
    )
    .await?;
    Ok(IngestResponse {
        id: accepted.id,
        status: "accepted",
        duplicate: accepted.duplicate,
        sha256: accepted.sha256,
    })
}

/// Reported separately from the size limit: a batch that stores nothing answers
/// 422, so this error string becomes the operator's whole diagnosis, and
/// calling an empty record oversized points them at the wrong knob.
fn check_record_size(state: &AppState, body: &[u8]) -> Result<(), ApiError> {
    if body.is_empty() {
        return Err(ApiError::BadRequest("record is empty".into()));
    }
    if body.len() > state.config.max_record_bytes {
        return Err(ApiError::PayloadTooLarge(state.config.max_record_bytes));
    }
    Ok(())
}

#[derive(Serialize)]
struct BatchIngestResponse {
    accepted: usize,
    duplicates: usize,
    rejected: usize,
    errors: Vec<String>,
}

async fn ingest_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut body: Body,
) -> Result<(StatusCode, Json<BatchIngestResponse>), ApiError> {
    let batch_key = required_header(&headers, "idempotency-key")?;
    let source = required_header(&headers, "x-mjai-source")?;
    if batch_key.len() > 256 || source.len() > 128 {
        return Err(ApiError::BadRequest(
            "collector headers are too long".into(),
        ));
    }
    let played_at = optional_header(&headers, "x-mjai-played-at")
        .map(|value| {
            value
                .parse::<DateTime<Utc>>()
                .map_err(|_| ApiError::BadRequest("X-Mjai-Played-At must be RFC 3339".into()))
        })
        .transpose()?;
    let staging_dir = state.config.data_dir.join("staging");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    let staging_path = staging_dir.join(format!("{}.tar", Uuid::new_v4()));
    let mut output = tokio::fs::File::create(&staging_path)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    // Every exit from the upload has to take the half-written archive with it.
    // A 400MB import whose connection resets at 300MB is the ordinary failure
    // here — a collector retries it, and the operator may retry it several
    // times — and nothing sweeps this directory, so a leak on that path fills
    // the API's disk one abandoned attempt at a time until someone notices.
    let uploaded = async {
        let mut received = 0usize;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| ApiError::BadRequest(error.to_string()))?;
            if let Ok(data) = frame.into_data() {
                received = received
                    .checked_add(data.len())
                    .ok_or(ApiError::PayloadTooLarge(state.config.max_batch_bytes))?;
                if received > state.config.max_batch_bytes {
                    return Err(ApiError::PayloadTooLarge(state.config.max_batch_bytes));
                }
                output
                    .write_all(&data)
                    .await
                    .map_err(|error| ApiError::Internal(error.to_string()))?;
            }
        }
        output
            .flush()
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))
    }
    .await;
    drop(output);
    if let Err(error) = uploaded {
        let _ = tokio::fs::remove_file(&staging_path).await;
        return Err(error);
    }

    let worker_state = state.clone();
    // The tar walk stays on a blocking thread; only the catalogue calls inside
    // it are async, and driving them from here keeps the reactor free.
    let handle = tokio::runtime::Handle::current();
    let result = tokio::task::spawn_blocking(move || {
        handle.block_on(process_batch_archive(
            &worker_state,
            &staging_path,
            &batch_key,
            &source,
            played_at,
        ))
    })
    .await
    .map_err(|error| ApiError::Internal(error.to_string()))?;
    result.map(|response| {
        // A batch that landed nothing is not an accepted batch. Answering 202 to it is how a
        // systematically broken import — wrong format, every record over the limit — runs for
        // hours while every response says the collector is fine.
        let status = if response.accepted == 0 && response.duplicates == 0 && response.rejected > 0
        {
            StatusCode::UNPROCESSABLE_ENTITY
        } else {
            StatusCode::ACCEPTED
        };
        (status, Json(response))
    })
}

async fn process_batch_archive(
    state: &AppState,
    path: &Path,
    batch_key: &str,
    source: &str,
    played_at: Option<DateTime<Utc>>,
) -> Result<BatchIngestResponse, ApiError> {
    let result = async {
        let mut file =
            std::fs::File::open(path).map_err(|error| ApiError::BadRequest(error.to_string()))?;
        let mut magic = [0u8; 2];
        file.read_exact(&mut magic)
            .map_err(|_| ApiError::BadRequest("batch archive is empty".into()))?;
        file.rewind()
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        let reader: Box<dyn Read> = if magic == [0x1f, 0x8b] {
            Box::new(flate2::read::MultiGzDecoder::new(file))
        } else {
            Box::new(file)
        };
        let mut archive = tar::Archive::new(reader);
        let entries = archive
            .entries()
            .map_err(|error| ApiError::BadRequest(format!("invalid tar archive: {error}")))?;
        let mut response = BatchIngestResponse {
            accepted: 0,
            duplicates: 0,
            rejected: 0,
            errors: Vec::new(),
        };
        let mut pending: Vec<Claimed> = Vec::new();
        let mut pending_bytes = 0usize;
        // Wrapped so that every exit from the walk — the record ceiling, a corrupt tar entry, an
        // unreadable member path, a record this server could not keep, a failed flush — passes
        // through one release of the claims still in hand. Returning straight out of the loop
        // drops them, and a dropped claim is the one failure an import can neither report nor
        // retry past: the record reached no topic and no index, and the retry that would resend it
        // is told it is a duplicate.
        let walked = async {
            for (index, entry) in entries.enumerate() {
                if index >= state.config.max_batch_records {
                    return Err(ApiError::BadRequest(format!(
                        "batch exceeds {} records",
                        state.config.max_batch_records
                    )));
                }
                let mut entry = entry
                    .map_err(|error| ApiError::BadRequest(format!("invalid tar entry: {error}")))?;
                if !entry.header().entry_type().is_file() {
                    continue;
                }
                let member_path = entry
                    .path()
                    .map_err(|error| ApiError::BadRequest(error.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                let size = entry.size() as usize;
                // Stored size, which for a gzip member is the compressed one, so this only bounds the
                // read; the record limit is enforced against the decompressed payload below.
                if size == 0 || size > state.config.max_record_bytes {
                    response.rejected += 1;
                    push_batch_error(
                        &mut response.errors,
                        format!("{member_path}: invalid size {size}"),
                    );
                    continue;
                }
                let mut raw = Vec::with_capacity(size);
                if let Err(error) = entry.read_to_end(&mut raw) {
                    response.rejected += 1;
                    push_batch_error(&mut response.errors, format!("{member_path}: {error}"));
                    continue;
                }
                // Detected by content, not by name: the collector's files are all named `.mjson` but
                // are gzip on disk, and decompressing 3.2GB into ~34GB to build an archive is wasted
                // work.
                if raw.starts_with(&[0x1f, 0x8b]) {
                    match inflate_member(&raw, state.config.max_record_bytes) {
                        Ok(inflated) => raw = inflated,
                        Err(error) => {
                            response.rejected += 1;
                            push_batch_error(
                                &mut response.errors,
                                format!("{member_path}: {error}"),
                            );
                            continue;
                        }
                    }
                }
                let item_key = format!("{batch_key}/{member_path}");
                let member = match check_record_size(state, &raw) {
                    Ok(()) => indexer::claim(
                        &state.catalog,
                        &state.kafka,
                        source,
                        &item_key,
                        played_at,
                        &raw,
                        None,
                    )
                    .await
                    .map_err(ApiError::from),
                    Err(error) => Err(error),
                };
                let claimed = match member {
                    Ok(claimed) => claimed,
                    // Losing a record the caller handed over is not the member being bad. Counting it
                    // as a rejection would bury a full disk — or a broker that will not take the
                    // record — in the error list of a 202 and let a losing import look healthy.
                    Err(ApiError::Internal(error)) => {
                        return Err(ApiError::Internal(format!("{member_path}: {error}")));
                    }
                    Err(error) => {
                        response.rejected += 1;
                        push_batch_error(&mut response.errors, format!("{member_path}: {error}"));
                        continue;
                    }
                };
                if claimed.is_duplicate() {
                    response.duplicates += 1;
                    continue;
                }
                response.accepted += 1;
                // Produced in size-bounded chunks rather than one request at the end: a 50,000
                // member import is gigabytes, and holding all of it to produce at once would trade
                // the pack worker's memory bound for the API's.
                //
                // The bound is tested before the record joins the chunk, so a chunk stays under it
                // and `produce_batch` sends it as a single request per partition. Pushing first
                // and testing after would overshoot by up to one record, which splits every flush
                // in two, and a split whose first request lands and whose second fails releases
                // the claims of records that are already durably in the topic: the retry produces
                // them again under fresh record ids, and `record_id` is in the sorting key, so
                // ReplacingMergeTree cannot converge those two rows. The record in hand is not in
                // `pending` yet and so is not covered by the walk's release, which is why the
                // failing branch releases it explicitly rather than dropping it.
                if pending_bytes + claimed.raw_len() > MAX_PRODUCE_BATCH_BYTES
                    && let Err(error) =
                        produce_batch_chunk(state, &mut pending, &mut pending_bytes).await
                {
                    indexer::abandon_claimed(&state.catalog, vec![claimed]).await;
                    return Err(error);
                }
                pending_bytes += claimed.raw_len();
                pending.push(claimed);
            }
            produce_batch_chunk(state, &mut pending, &mut pending_bytes).await
        }
        .await;
        if let Err(error) = walked {
            indexer::abandon_claimed(&state.catalog, std::mem::take(&mut pending)).await;
            return Err(error);
        }
        Ok(response)
    }
    .await;
    let _ = std::fs::remove_file(path);
    result
}

/// Hands one chunk of claimed records to the topic. A failure fails the whole
/// import: the earlier chunks are durably produced and their claims stand, so
/// the collector's retry finds them as duplicates and only re-sends what is
/// actually missing.
async fn produce_batch_chunk(
    state: &AppState,
    pending: &mut Vec<Claimed>,
    pending_bytes: &mut usize,
) -> Result<(), ApiError> {
    *pending_bytes = 0;
    indexer::produce_claimed(&state.catalog, &state.kafka, std::mem::take(pending)).await?;
    Ok(())
}

/// MultiGzDecoder, not GzDecoder: concatenated gzip streams are one valid file (that is what
/// `gzip -c a b` and every block-gzip writer produce) and GzDecoder stops after the first one,
/// which would store a truncated record and index it as complete.
///
/// The reader stops one byte past the limit, which is what keeps a small member from inflating
/// without bound — deflate reaches 1032:1, so an entry just under the limit could otherwise
/// expand to hundreds of megabytes before anything looked at its size.
fn inflate_member(raw: &[u8], limit: usize) -> Result<Vec<u8>, String> {
    let mut inflated = Vec::new();
    flate2::read::MultiGzDecoder::new(raw)
        .take(limit as u64 + 1)
        .read_to_end(&mut inflated)
        .map_err(|error| error.to_string())?;
    if inflated.len() > limit {
        return Err(format!("decompresses past the {limit} byte record limit"));
    }
    Ok(inflated)
}

fn push_batch_error(errors: &mut Vec<String>, error: String) {
    const MAX_REPORTED_ERRORS: usize = 100;
    if errors.len() < MAX_REPORTED_ERRORS {
        errors.push(error);
    }
}

#[derive(Deserialize)]
struct SearchQuery {
    source: Option<String>,
    player: Option<String>,
    rule: Option<String>,
    received_from: Option<DateTime<Utc>>,
    received_to: Option<DateTime<Utc>>,
    played_from: Option<DateTime<Utc>>,
    played_to: Option<DateTime<Utc>>,
    cursor: Option<Cursor>,
    #[serde(default = "default_page_size")]
    limit: usize,
}

fn default_page_size() -> usize {
    100
}

#[derive(Serialize)]
struct RecordPage {
    items: Vec<Record>,
    next_cursor: Option<Cursor>,
}

async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<RecordPage>, ApiError> {
    if !(1..=1000).contains(&query.limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 1000".into(),
        ));
    }
    let filter = RecordFilter {
        source: query.source,
        player: query.player,
        rule: query.rule,
        received_from: query.received_from,
        received_to: query.received_to,
        played_from: query.played_from,
        played_to: query.played_to,
        // Built field by field rather than with struct update syntax, so a new
        // filter has to be added to `SearchQuery` before it can be asked for
        // here. This one is for the re-fetch walk, not for the console.
        missing_pb: false,
    };
    let (items, next_cursor) = state
        .catalog
        .search(&filter, query.cursor, query.limit)
        .await?;
    Ok(Json(RecordPage { items, next_cursor }))
}

async fn get_record(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<Record>, ApiError> {
    state
        .catalog
        .get(id)
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn get_raw(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Response, ApiError> {
    let record = state.catalog.get(id).await?.ok_or(ApiError::NotFound)?;
    let raw = state
        .packs
        .read(&record.storage)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    if hex::encode(Sha256::digest(&raw)) != record.sha256 {
        return Err(ApiError::Internal("record checksum mismatch".into()));
    }
    let mut response = raw.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response.headers_mut().insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{id}.mjson\""))
            .map_err(|error| ApiError::Internal(error.to_string()))?,
    );
    Ok(response)
}

/// The Mahjong Soul protobuf the record was converted from, byte for byte.
///
/// `404` when the record has none, which is every record indexed before the
/// converter stopped discarding it and every record that never was a protobuf.
/// No checksum to compare against, unlike `/raw`: `sha256` is the mjai's. The
/// stored length is checked instead, which is what a truncated pack read would
/// break.
async fn get_majsoul_pb(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Response, ApiError> {
    let record = state.catalog.get(id).await?.ok_or(ApiError::NotFound)?;
    let location = record
        .majsoul_pb
        .as_ref()
        .ok_or(ApiError::NotFound)?
        .in_pack(&record.storage.pack_key);
    let raw = state
        .packs
        .read(&location)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    if raw.len() != location.raw_size as usize {
        return Err(ApiError::Internal("protobuf length mismatch".into()));
    }
    let mut response = raw.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{id}.pb\""))
            .map_err(|error| ApiError::Internal(error.to_string()))?,
    );
    Ok(response)
}

/// One keyset page of hits held in memory at a time. docs/architecture.md line
/// 88 forbids materialising the whole hit set, which an export of the 数亿 scale
/// this is sized for would otherwise do.
const EXPORT_PAGE_SIZE: usize = 1_000;

async fn create_download(
    State(state): State<AppState>,
    Json(request): Json<DownloadRequest>,
) -> Result<(StatusCode, Json<DownloadJob>), ApiError> {
    let job = state.catalog.create_job(&request).await?;
    let id = job.id;
    // Compression and pack reads are blocking; the catalogue pages they are
    // interleaved with are not.
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || handle.block_on(run_export(state, id, request)));
    Ok((StatusCode::ACCEPTED, Json(job)))
}

async fn run_export(state: AppState, id: Uuid, request: DownloadRequest) {
    if let Err(error) = state.catalog.start_job(id).await {
        tracing::error!(job = %id, %error, "could not mark the export as running");
        return;
    }
    let outcome = match write_export(&state, id, &request).await {
        Ok(count) => Ok((count, format!("exports/{id}.{}", request.format.as_str()))),
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = state.catalog.finish_job(id, outcome).await {
        tracing::error!(job = %id, %error, "could not record the export outcome");
    }
}

async fn write_export(
    state: &AppState,
    id: Uuid,
    request: &DownloadRequest,
) -> anyhow::Result<usize> {
    let path = state
        .export_dir
        .join(format!("{id}.{}", request.format.as_str()));
    let mut writer = ExportWriter::create(request.format, &path)?;
    let mut cursor = None;
    let mut written = 0usize;
    loop {
        let (page, next) = state
            .catalog
            .scan(&request.filter, cursor, EXPORT_PAGE_SIZE)
            .await?;
        for record in &page {
            writer.append(record, state).await?;
            written += 1;
        }
        // Per page rather than per record, and reported rather than propagated:
        // a progress number the console could not read is not worth failing an
        // export that is otherwise writing correctly.
        if let Err(error) = state.catalog.record_job_progress(id, written).await {
            tracing::warn!(job = %id, %error, "could not publish export progress");
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    writer.finish()?;
    Ok(written)
}

enum ExportWriter {
    TarGz(TarBuilder<GzEncoder<std::fs::File>>),
    Manifest(std::fs::File),
}

impl ExportWriter {
    fn create(format: DownloadFormat, path: &Path) -> std::io::Result<Self> {
        let output = std::fs::File::create(path)?;
        Ok(match format {
            DownloadFormat::TarGz => Self::TarGz(TarBuilder::new(GzEncoder::new(
                output,
                Compression::default(),
            ))),
            DownloadFormat::ManifestJsonl => Self::Manifest(output),
        })
    }

    async fn append(&mut self, record: &Record, state: &AppState) -> anyhow::Result<()> {
        match self {
            Self::TarGz(archive) => {
                let raw = state.packs.read(&record.storage).await?;
                let mut header = TarHeader::new_gnu();
                header.set_size(raw.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                archive.append_data(&mut header, format!("{}.mjson", record.id), raw.as_slice())?;
            }
            Self::Manifest(output) => {
                serde_json::to_writer(&mut *output, record)?;
                output.write_all(b"\n")?;
            }
        }
        Ok(())
    }

    fn finish(self) -> anyhow::Result<()> {
        if let Self::TarGz(mut archive) = self {
            archive.finish()?;
            archive.into_inner()?.finish()?;
        }
        Ok(())
    }
}

async fn get_download(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<DownloadJob>, ApiError> {
    state
        .catalog
        .get_job(id)
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn download_file(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Response, ApiError> {
    let job = state.catalog.get_job(id).await?.ok_or(ApiError::NotFound)?;
    if !matches!(job.state, JobState::Completed) {
        return Err(ApiError::Conflict("download is not ready".into()));
    }
    let url = job
        .download_url
        .as_ref()
        .ok_or_else(|| ApiError::Internal("completed job has no file".into()))?;
    let extension = if url.contains("manifest") {
        "manifest.jsonl"
    } else {
        let tar_path = state.export_dir.join(format!("{id}.tar.gz"));
        if tar_path.exists() {
            "tar.gz"
        } else {
            "manifest.jsonl"
        }
    };
    // Streamed, not read: an export of the whole corpus is as large as the pack
    // corpus itself, and buffering it would put that in the API's heap on top
    // of the copy already on disk.
    let file = tokio::fs::File::open(state.export_dir.join(format!("{id}.{extension}")))
        .await
        .map_err(|_| ApiError::NotFound)?;
    // Both headers are the streaming body's replacement for what `Vec<u8>` used
    // to give for free: axum's `IntoResponse` for bytes sets the content type
    // and the length, its `Body` impl sets neither, and a download without a
    // total size is a progress bar that never fills.
    let length = file.metadata().await.ok().map(|metadata| metadata.len());
    let mut response = Body::from_stream(ReaderStream::new(file)).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Some(length) = length {
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from(length));
    }
    response.headers_mut().insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{id}.{extension}\""))
            .map_err(|error| ApiError::Internal(error.to_string()))?,
    );
    Ok(response)
}

#[derive(Deserialize)]
struct DownloadListQuery {
    #[serde(default = "default_page_size")]
    limit: usize,
}

#[derive(Serialize)]
struct DownloadPage {
    items: Vec<DownloadJob>,
}

async fn list_downloads(
    State(state): State<AppState>,
    Query(query): Query<DownloadListQuery>,
) -> Result<Json<DownloadPage>, ApiError> {
    if !(1..=1000).contains(&query.limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 1000".into(),
        ));
    }
    Ok(Json(DownloadPage {
        items: state.catalog.recent_jobs(query.limit).await?,
    }))
}

#[derive(Serialize)]
struct StatsResponse {
    records: RecordStats,
    storage: StorageStats,
    downloads: DownloadCounts,
    watch: WatchStats,
}

/// The supervisor's counters without its item list, which is what
/// `/api/v1/watch/status` is for.
#[derive(Serialize)]
struct WatchStats {
    phase: ServicePhase,
    live: u64,
    pending: u64,
    completed: u64,
    failed: u64,
}

/// The console polls this. The two stores are asked at once because neither
/// answer depends on the other, and the supervisor is asked afterwards because
/// its dashboard is a lock and a clone rather than a query. What each aggregate
/// costs, and which of them is deliberately approximate, is on `Catalog::stats`.
async fn get_stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, ApiError> {
    let (index, downloads) =
        tokio::try_join!(state.catalog.stats(), state.catalog.download_counts())?;
    // A limit of zero: the counters are summed over every tracked game and only
    // the item list is truncated, so this asks for none of it. Building that
    // empty list still clones and sorts the registry, which is bounded by the
    // 10,000 games it keeps; if this poll ever shows up in a profile the answer
    // is a count-only path in `WatchRegistry`, not a change here.
    let watch = state.watch_service.dashboard(None, 0);
    Ok(Json(StatsResponse {
        records: index.records,
        storage: index.storage,
        downloads,
        watch: WatchStats {
            phase: watch.service.phase,
            live: watch.records.live as u64,
            pending: watch.records.pending as u64,
            completed: watch.records.completed as u64,
            failed: watch.records.failed as u64,
        },
    }))
}

#[derive(Deserialize)]
struct TrendQuery {
    unit: Option<SeriesUnit>,
    span: Option<u32>,
    /// An explicit range, given together or not at all. RFC 3339, like every
    /// other timestamp this API takes.
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    /// Comma separated, because `serde_urlencoded` collects a repeated key into
    /// the last value rather than into a list, and a facet with two of its
    /// values chosen is the ordinary case here.
    players: Option<String>,
    room: Option<String>,
    length: Option<String>,
}

/// Splits one facet and refuses a value it does not know. Not silently ignored:
/// a typo that answers with every mode looks exactly like a filter that found
/// nothing to exclude, and the reader has no way to tell which they got.
fn rule_facet<T: FromStr>(field: &'static str, value: Option<&str>) -> Result<Vec<T>, ApiError> {
    value
        .into_iter()
        .flat_map(|value| value.split(','))
        .filter(|token| !token.is_empty())
        .map(|token| {
            token.parse().map_err(|_| {
                ApiError::BadRequest(format!("{field} does not have a value named {token:?}"))
            })
        })
        .collect()
}

/// The buckets behind the trends page. Either `span` buckets ending now, or the
/// explicit `from`..`to` range — a width is clamped rather than rejected in
/// both shapes, and the timestamps on the points are how a caller sees which
/// window it actually got. What *is* refused: an unknown `unit` or mode, a
/// range that ends before it starts, and half a range. None of those has a
/// nearest legal value to fall back to, and a filter silently ignored looks
/// exactly like a filter that found nothing to exclude.
async fn get_series_stats(
    State(state): State<AppState>,
    Query(query): Query<TrendQuery>,
) -> Result<Json<Series>, ApiError> {
    let filter = RuleFilter {
        players: rule_facet("players", query.players.as_deref())?,
        rooms: rule_facet("room", query.room.as_deref())?,
        lengths: rule_facet("length", query.length.as_deref())?,
    };
    let unit = query.unit.unwrap_or(SeriesUnit::Day);
    let window = match (query.from, query.to) {
        (Some(from), Some(to)) => SeriesWindow::between(unit, from, to)?,
        (None, None) => SeriesWindow::recent(unit, query.span.unwrap_or(DEFAULT_TREND_SPAN)),
        _ => {
            return Err(ApiError::BadRequest(
                "from and to have to be given together".into(),
            ));
        }
    };
    Ok(Json(state.catalog.series(window, &filter).await?))
}

/// How many games one `POST /api/v1/paipuya/games` may carry.
///
/// The catalogue being loaded is hundreds of millions of games, so this is a
/// chunk size for the caller rather than a ceiling on the job: it bounds what
/// one request holds in memory and what one failed request has to be retried.
const MAX_PAIPUYA_BATCH: usize = 100_000;

#[derive(Deserialize)]
struct PaipuyaBatch {
    games: Vec<PaipuyaGame>,
}

#[derive(Serialize)]
struct PaipuyaAccepted {
    accepted: usize,
}

/// Records what 牌谱屋 lists. This stores the catalogue, not the games — a row
/// here is a claim that a game exists, and nothing about it is fetched until
/// `paipuya_gap` says this corpus does not already have it.
async fn post_paipuya_games(
    State(state): State<AppState>,
    Json(batch): Json<PaipuyaBatch>,
) -> Result<Json<PaipuyaAccepted>, ApiError> {
    if batch.games.len() > MAX_PAIPUYA_BATCH {
        return Err(ApiError::BadRequest(format!(
            "a batch must not exceed {MAX_PAIPUYA_BATCH} games"
        )));
    }
    // Refused rather than stored, because a game with no players cannot be
    // matched against the corpus by anything and would be reported as missing
    // for ever, sending the pool to fetch a uuid it already has.
    if let Some(empty) = batch.games.iter().find(|game| game.players.is_empty()) {
        return Err(ApiError::BadRequest(format!(
            "game {} lists no players, so nothing could ever match it",
            empty.uuid
        )));
    }
    state.catalog.insert_paipuya_games(&batch.games).await?;
    Ok(Json(PaipuyaAccepted {
        accepted: batch.games.len(),
    }))
}

#[derive(Deserialize)]
struct PaipuyaGapQuery {
    unit: Option<SeriesUnit>,
    span: Option<u32>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct PaipuyaReport {
    #[serde(flatten)]
    totals: PaipuyaTotals,
    #[serde(flatten)]
    gap: PaipuyaGap,
}

/// How much of what 牌谱屋 lists in a window this corpus is missing.
///
/// The window is the same shape the trend charts use, and it is not optional in
/// spirit: matching is on start time and player names, so both sides are read
/// over the same range and the corpus side stays small however large the
/// catalogue grows.
async fn get_paipuya_gap(
    State(state): State<AppState>,
    Query(query): Query<PaipuyaGapQuery>,
) -> Result<Json<PaipuyaReport>, ApiError> {
    let unit = query.unit.unwrap_or(SeriesUnit::Day);
    let window = match (query.from, query.to) {
        (Some(from), Some(to)) => SeriesWindow::between(unit, from, to)?,
        (None, None) => SeriesWindow::recent(unit, query.span.unwrap_or(DEFAULT_TREND_SPAN)),
        _ => {
            return Err(ApiError::BadRequest(
                "from and to have to be given together".into(),
            ));
        }
    };
    let (totals, gap) = tokio::try_join!(
        state.catalog.paipuya_totals(),
        state.catalog.paipuya_gap(window),
    )?;
    Ok(Json(PaipuyaReport { totals, gap }))
}

/// Open to every member like the other configuration reads. The API key is the
/// one field that could not be, so `published_config` replaces it with three
/// asterisks — handing those back on save keeps the stored one.
async fn get_paipuya_config(State(state): State<AppState>) -> Json<PaipuyaConfig> {
    Json(state.paipuya.published_config())
}

async fn put_paipuya_config(
    State(state): State<AppState>,
    Json(config): Json<PaipuyaConfig>,
) -> Result<Json<PaipuyaConfig>, ApiError> {
    Ok(Json(state.paipuya.update_config(config).await?))
}

async fn get_paipuya_status(State(state): State<AppState>) -> Json<PaipuyaStatus> {
    Json(state.paipuya.status())
}

async fn post_paipuya_action(
    State(state): State<AppState>,
    Json(request): Json<WatchActionRequest>,
) -> Result<Json<PaipuyaStatus>, ApiError> {
    Ok(Json(state.paipuya.apply_action(request.action).await?))
}

#[derive(Deserialize)]
struct PlayerSearchQuery {
    q: Option<String>,
    limit: Option<usize>,
}

/// Names in the corpus containing `q`, busiest first.
///
/// An empty or missing `q` matches everything, which is the useful default for
/// a search box that has not been typed in yet: it answers with the players
/// who appear in the most games.
async fn search_players(
    State(state): State<AppState>,
    Query(query): Query<PlayerSearchQuery>,
) -> Result<Json<PlayerPage>, ApiError> {
    let items = state
        .catalog
        .search_players(
            query.q.as_deref().unwrap_or_default(),
            query.limit.unwrap_or(20),
        )
        .await?;
    Ok(Json(PlayerPage { items }))
}

#[derive(Serialize)]
struct PlayerPage {
    items: Vec<PlayerHit>,
}

/// The same window and mode grammar the trends page uses, plus the player.
#[derive(Deserialize)]
struct PlayerStatsQuery {
    player: String,
    unit: Option<SeriesUnit>,
    span: Option<u32>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    players: Option<String>,
    room: Option<String>,
    length: Option<String>,
}

/// One player's counters over a window.
///
/// The player is a query parameter rather than a path segment because a Mahjong
/// Soul nickname is arbitrary text: slashes, question marks and percent signs
/// are all legal in one, and every one of them means something else in a path.
///
/// Ratios are not computed here. The counters and their denominators travel
/// together so that a reader can see which denominator each rate is over —
/// `hands` for most, `detailed_games` for the four an older record cannot
/// answer at all.
async fn get_player_stats(
    State(state): State<AppState>,
    Query(query): Query<PlayerStatsQuery>,
) -> Result<Json<PlayerSummary>, ApiError> {
    if query.player.trim().is_empty() {
        return Err(ApiError::BadRequest("player must not be empty".into()));
    }
    let filter = RuleFilter {
        players: rule_facet("players", query.players.as_deref())?,
        rooms: rule_facet("room", query.room.as_deref())?,
        lengths: rule_facet("length", query.length.as_deref())?,
    };
    let unit = query.unit.unwrap_or(SeriesUnit::Day);
    let window = match (query.from, query.to) {
        (Some(from), Some(to)) => SeriesWindow::between(unit, from, to)?,
        (None, None) => SeriesWindow::recent(unit, query.span.unwrap_or(DEFAULT_TREND_SPAN)),
        _ => {
            return Err(ApiError::BadRequest(
                "from and to have to be given together".into(),
            ));
        }
    };
    Ok(Json(
        state
            .catalog
            .player_summary(&query.player, window, &filter)
            .await?,
    ))
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    optional_header(headers, name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::BadRequest(format!("missing {name} header")))
}

fn optional_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("{0}")]
    BadRequest(String),
    #[error("payload exceeds {0} bytes")]
    PayloadTooLarge(usize),
    #[error("not found")]
    NotFound,
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Conflict(String),
    /// The caller edited a version of something that has since moved on, and
    /// the edit has to be rebuilt rather than retried.
    #[error("{0}")]
    PreconditionFailed(String),
    #[error("{0}")]
    Internal(String),
}

impl From<CatalogError> for ApiError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::Conflict | CatalogError::Pending => ApiError::Conflict(error.to_string()),
            CatalogError::WindowTooWide(_) | CatalogError::RangeInverted => {
                ApiError::BadRequest(error.to_string())
            }
            CatalogError::Index(_) | CatalogError::Store(_) => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl From<IngestError> for ApiError {
    fn from(error: IngestError) -> Self {
        match error {
            // The caller's record, not this server's problem: in a batch this
            // is one rejected member inside an otherwise fine import.
            IngestError::Malformed(_) => ApiError::BadRequest(error.to_string()),
            IngestError::Catalog(error) => error.into(),
            // A `500` is what makes a collector back off, which is the point in
            // both cases. A produce that failed is a record this server was
            // handed and did not keep, and it must never be reported as
            // anything a `2xx` covers. A backlog past the ceiling is the same
            // answer for the opposite reason: nothing was lost, and ingest has
            // to stop long enough for the pack worker to drain the topic, or
            // the topic outgrows its retention and drops records the API has
            // already acknowledged.
            IngestError::Produce(_) | IngestError::Backlogged(_) => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl From<WatchServiceError> for ApiError {
    fn from(error: WatchServiceError) -> Self {
        match error {
            WatchServiceError::InvalidConfig(_)
            | WatchServiceError::InvalidModule(_)
            | WatchServiceError::ModuleNotInstalled(_, _) => {
                ApiError::BadRequest(error.to_string())
            }
            WatchServiceError::ModuleHealth(_) => ApiError::Conflict(error.to_string()),
            // 412 rather than 409, which module health already uses: the two
            // need different answers from a client. A failed health check means
            // fix the module and try again with the same edit; a revision
            // conflict means the edit itself is against a document that no
            // longer exists and has to be rebuilt on the current one.
            WatchServiceError::RevisionConflict { .. } => {
                ApiError::PreconditionFailed(error.to_string())
            }
            WatchServiceError::Io(_) | WatchServiceError::Json(_) => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl From<MihomoError> for ApiError {
    fn from(error: MihomoError) -> Self {
        match error {
            MihomoError::InvalidConfig(_) => ApiError::BadRequest(error.to_string()),
            MihomoError::Controller(_) => ApiError::Conflict(error.to_string()),
            MihomoError::Io(_) | MihomoError::Json(_) | MihomoError::Http(_) => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl From<AuthError> for ApiError {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::InvalidCredentials => ApiError::Unauthorized,
            AuthError::Forbidden
            | AuthError::EmailNotVerified
            | AuthError::Disabled
            | AuthError::RegistrationDisabled => ApiError::Forbidden(error.to_string()),
            AuthError::NotFound => ApiError::NotFound,
            AuthError::EmailExists => ApiError::Conflict(error.to_string()),
            AuthError::EmailUnavailable
            | AuthError::InvalidVerification
            | AuthError::InvalidInput(_) => ApiError::BadRequest(error.to_string()),
            AuthError::Io(_) | AuthError::Json(_) | AuthError::Email(_) => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::PreconditionFailed(_) => StatusCode::PRECONDITION_FAILED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(serde_json::json!({"error": self.to_string()}))).into_response()
    }
}
