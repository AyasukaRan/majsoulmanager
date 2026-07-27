use std::{
    io::{Read, Seek, Write},
    path::Path,
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
    auth::{
        AuthError, AuthSettings, CreateUserRequest, LoginRequest, LoginResponse, RegisterRequest,
        RegistrationStatus, UpdateUserRequest, UserView, VerifyEmailRequest,
    },
    catalog::{
        CatalogError, Cursor, DownloadFormat, DownloadJob, DownloadRequest, IdempotencyClaim,
        JobState, Record, RecordFilter,
    },
    mihomo::{MihomoAction, MihomoError, MihomoStatus, ProxySelection, SubscriptionUpdate},
    mjai,
    watch_log::WatchLogEntry,
    watch_service::{
        InstallModuleRequest, InstalledModule, WatchAction, WatchDashboard, WatchRuntimeStatus,
        WatchServiceConfig, WatchServiceError, module_protocol_contract,
    },
};

pub fn router(state: AppState) -> Router {
    let max_batch = state.config.max_batch_bytes;
    let protected = Router::new()
        .route("/api/v1/records", post(ingest).get(search))
        .route("/api/v1/records/batch", post(ingest_batch))
        .route("/api/v1/records/{id}", get(get_record))
        .route("/api/v1/records/{id}/raw", get(get_raw))
        .route("/api/v1/downloads", post(create_download))
        .route("/api/v1/downloads/{id}", get(get_download))
        .route("/api/v1/downloads/{id}/file", get(download_file))
        .route("/api/v1/watch/status", get(get_watch_status))
        .route("/api/v1/watch/logs", get(get_watch_logs))
        .route("/api/v1/watch/config", get(get_watch_config))
        .route("/api/v1/watch/config", put(put_watch_config))
        .route("/api/v1/watch/actions", post(post_watch_action))
        .route(
            "/api/v1/watch/modules",
            get(get_watch_modules).post(install_watch_module),
        )
        .route("/api/v1/watch/modules/protocol", get(get_module_protocol))
        .route("/api/v1/watch/proxy", get(get_watch_proxy))
        .route(
            "/api/v1/watch/proxy/subscription",
            put(put_watch_proxy_subscription),
        )
        .route(
            "/api/v1/watch/proxy/selection",
            put(put_watch_proxy_selection),
        )
        .route("/api/v1/watch/proxy/actions", post(post_watch_proxy_action))
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

async fn get_watch_config(State(state): State<AppState>) -> Json<WatchServiceConfig> {
    Json(state.watch_service.config())
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

async fn ingest_one(
    state: &AppState,
    idempotency_key: &str,
    source: &str,
    played_at: Option<DateTime<Utc>>,
    body: &[u8],
) -> Result<IngestResponse, ApiError> {
    // Reported separately from the size limit: a batch that stores nothing now
    // answers 422, so this error string becomes the operator's whole diagnosis,
    // and calling an empty record oversized points them at the wrong knob.
    if body.is_empty() {
        return Err(ApiError::BadRequest("record is empty".into()));
    }
    if body.len() > state.config.max_record_bytes {
        return Err(ApiError::PayloadTooLarge(state.config.max_record_bytes));
    }
    let metadata =
        mjai::parse_metadata(body).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let sha256 = hex::encode(Sha256::digest(body));
    let id = Uuid::new_v4();

    let scoped_idempotency_key = format!("{source}\0{idempotency_key}");
    match state
        .catalog
        .claim(&scoped_idempotency_key, id, &sha256)
        .await?
    {
        IdempotencyClaim::Existing(record) => Ok(IngestResponse {
            id: record.id,
            status: "indexed",
            duplicate: true,
            sha256,
        }),
        IdempotencyClaim::New => {
            let location = match state.packs.append(id, body) {
                Ok(location) => location,
                Err(error) => {
                    // A claim left behind only costs the collector a `409` on
                    // its next retry of this key, and reporting it would hide
                    // the pack failure that actually stopped the ingest.
                    if let Err(claim) = state
                        .catalog
                        .abandon_claim(&scoped_idempotency_key, id)
                        .await
                    {
                        tracing::warn!(%claim, "could not release the idempotency claim");
                    }
                    return Err(ApiError::Internal(error.to_string()));
                }
            };
            state
                .catalog
                .insert(Record {
                    id,
                    source: source.to_owned(),
                    sha256: sha256.clone(),
                    received_at: Utc::now(),
                    // A batch carries one header for tens of thousands of games, so the record's
                    // own majsoul.start_time is the only usable played_at; the header stays an
                    // override.
                    played_at: played_at.or(metadata.played_at),
                    players: metadata.players,
                    rule: metadata.rule,
                    event_count: metadata.event_count,
                    storage: location,
                })
                .await?;
            Ok(IngestResponse {
                id,
                status: "indexed",
                duplicate: false,
                sha256,
            })
        }
    }
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
    let mut received = 0usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| ApiError::BadRequest(error.to_string()))?;
        if let Ok(data) = frame.into_data() {
            received = received
                .checked_add(data.len())
                .ok_or(ApiError::PayloadTooLarge(state.config.max_batch_bytes))?;
            if received > state.config.max_batch_bytes {
                drop(output);
                let _ = tokio::fs::remove_file(&staging_path).await;
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
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    drop(output);

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
                        push_batch_error(&mut response.errors, format!("{member_path}: {error}"));
                        continue;
                    }
                }
            }
            let item_key = format!("{batch_key}/{member_path}");
            match ingest_one(state, &item_key, source, played_at, &raw).await {
                Ok(result) if result.duplicate => response.duplicates += 1,
                Ok(_) => response.accepted += 1,
                // Storage failing is this server losing a record the caller handed over, not the
                // member being bad. Counting it as a rejection would bury a full disk in the
                // error list of a 202 and let a losing import look healthy.
                Err(ApiError::Internal(error)) => {
                    return Err(ApiError::Internal(format!("{member_path}: {error}")));
                }
                Err(error) => {
                    response.rejected += 1;
                    push_batch_error(&mut response.errors, format!("{member_path}: {error}"));
                }
            }
        }
        Ok(response)
    }
    .await;
    let _ = std::fs::remove_file(path);
    result
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
        received_from: query.received_from,
        received_to: query.received_to,
        played_from: query.played_from,
        played_to: query.played_to,
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
            writer.append(record, state)?;
            written += 1;
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

    fn append(&mut self, record: &Record, state: &AppState) -> anyhow::Result<()> {
        match self {
            Self::TarGz(archive) => {
                let raw = state.packs.read(&record.storage)?;
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
    #[error("{0}")]
    Internal(String),
}

impl From<CatalogError> for ApiError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::Conflict | CatalogError::Pending => ApiError::Conflict(error.to_string()),
            CatalogError::WindowTooWide(_) => ApiError::BadRequest(error.to_string()),
            // A `500` is what makes the collector back off, which is the point:
            // the record is already packed, so the backlog needs ingest to stop
            // long enough for the index to catch up, not a retry loop.
            CatalogError::Backlogged(_) | CatalogError::Index(_) | CatalogError::Store(_) => {
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
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(serde_json::json!({"error": self.to_string()}))).into_response()
    }
}
