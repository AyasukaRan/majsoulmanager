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
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE},
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
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    AppState,
    auth::{
        AuthError, AuthSettings, CreateUserRequest, LoginRequest, LoginResponse, RegisterRequest,
        RegistrationStatus, UpdateUserRequest, UserView, VerifyEmailRequest,
    },
    catalog::{
        DownloadFormat, DownloadJob, DownloadRequest, IdempotencyClaim, IdempotencyError, JobState,
        Record, RecordFilter,
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
    let response = ingest_one(&state, &idempotency_key, &source, played_at, body.as_ref())?;
    let status = if response.duplicate {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    Ok((status, Json(response)))
}

fn ingest_one(
    state: &AppState,
    idempotency_key: &str,
    source: &str,
    played_at: Option<DateTime<Utc>>,
    body: &[u8],
) -> Result<IngestResponse, ApiError> {
    if body.is_empty() || body.len() > state.config.max_record_bytes {
        return Err(ApiError::PayloadTooLarge(state.config.max_record_bytes));
    }
    let metadata =
        mjai::parse_metadata(body).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let sha256 = hex::encode(Sha256::digest(body));
    let id = Uuid::new_v4();

    let scoped_idempotency_key = format!("{source}\0{idempotency_key}");
    match state.catalog.claim(&scoped_idempotency_key, id, &sha256)? {
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
                    state.catalog.abandon_claim(&scoped_idempotency_key, id);
                    return Err(ApiError::Internal(error.to_string()));
                }
            };
            state.catalog.insert(Record {
                id,
                source: source.to_owned(),
                sha256: sha256.clone(),
                received_at: Utc::now(),
                played_at,
                players: metadata.players,
                rule: metadata.rule,
                event_count: metadata.event_count,
                storage: location,
            });
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
    let result = tokio::task::spawn_blocking(move || {
        process_batch_archive(&worker_state, &staging_path, &batch_key, &source, played_at)
    })
    .await
    .map_err(|error| ApiError::Internal(error.to_string()))?;
    result.map(|response| (StatusCode::ACCEPTED, Json(response)))
}

fn process_batch_archive(
    state: &AppState,
    path: &Path,
    batch_key: &str,
    source: &str,
    played_at: Option<DateTime<Utc>>,
) -> Result<BatchIngestResponse, ApiError> {
    let result = (|| {
        let mut file =
            std::fs::File::open(path).map_err(|error| ApiError::BadRequest(error.to_string()))?;
        let mut magic = [0u8; 2];
        file.read_exact(&mut magic)
            .map_err(|_| ApiError::BadRequest("batch archive is empty".into()))?;
        file.rewind()
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        let reader: Box<dyn Read> = if magic == [0x1f, 0x8b] {
            Box::new(flate2::read::GzDecoder::new(file))
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
            let item_key = format!("{batch_key}/{member_path}");
            match ingest_one(state, &item_key, source, played_at, &raw) {
                Ok(result) if result.duplicate => response.duplicates += 1,
                Ok(_) => response.accepted += 1,
                Err(error) => {
                    response.rejected += 1;
                    push_batch_error(&mut response.errors, format!("{member_path}: {error}"));
                }
            }
        }
        Ok(response)
    })();
    let _ = std::fs::remove_file(path);
    result
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
    cursor: Option<Uuid>,
    #[serde(default = "default_page_size")]
    limit: usize,
}

fn default_page_size() -> usize {
    100
}

#[derive(Serialize)]
struct RecordPage {
    items: Vec<Record>,
    next_cursor: Option<Uuid>,
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
    let (items, next_cursor) = state.catalog.search(&filter, query.cursor, query.limit);
    Ok(Json(RecordPage { items, next_cursor }))
}

async fn get_record(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<Record>, ApiError> {
    state.catalog.get(id).map(Json).ok_or(ApiError::NotFound)
}

async fn get_raw(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Response, ApiError> {
    let record = state.catalog.get(id).ok_or(ApiError::NotFound)?;
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

async fn create_download(
    State(state): State<AppState>,
    Json(request): Json<DownloadRequest>,
) -> Result<(StatusCode, Json<DownloadJob>), ApiError> {
    let id = Uuid::new_v4();
    let job = DownloadJob {
        id,
        state: JobState::Queued,
        created_at: Utc::now(),
        record_count: 0,
        download_url: None,
        error: None,
    };
    state.catalog.insert_job(job.clone());
    tokio::task::spawn_blocking(move || run_export(state, id, request));
    Ok((StatusCode::ACCEPTED, Json(job)))
}

fn run_export(state: AppState, id: Uuid, request: DownloadRequest) {
    state
        .catalog
        .update_job(id, |job| job.state = JobState::Running);
    let records = state.catalog.all_matching(&request.filter);
    let extension = match request.format {
        DownloadFormat::TarGz => "tar.gz",
        DownloadFormat::ManifestJsonl => "manifest.jsonl",
    };
    let path = state.export_dir.join(format!("{id}.{extension}"));
    let result = match request.format {
        DownloadFormat::TarGz => write_tar_gz(&path, &records, &state),
        DownloadFormat::ManifestJsonl => write_manifest(&path, &records),
    };
    state.catalog.update_job(id, |job| match result {
        Ok(()) => {
            job.state = JobState::Completed;
            job.record_count = records.len();
            job.download_url = Some(format!("/api/v1/downloads/{id}/file"));
        }
        Err(error) => {
            job.state = JobState::Failed;
            job.error = Some(error.to_string());
        }
    });
}

fn write_tar_gz(path: &Path, records: &[Record], state: &AppState) -> anyhow::Result<()> {
    let output = std::fs::File::create(path)?;
    let encoder = GzEncoder::new(output, Compression::default());
    let mut archive = TarBuilder::new(encoder);
    for record in records {
        let raw = state.packs.read(&record.storage)?;
        let mut header = TarHeader::new_gnu();
        header.set_size(raw.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.append_data(&mut header, format!("{}.mjson", record.id), raw.as_slice())?;
    }
    archive.finish()?;
    let encoder = archive.into_inner()?;
    encoder.finish()?;
    Ok(())
}

fn write_manifest(path: &Path, records: &[Record]) -> anyhow::Result<()> {
    let mut output = std::fs::File::create(path)?;
    for record in records {
        serde_json::to_writer(&mut output, record)?;
        output.write_all(b"\n")?;
    }
    Ok(())
}

async fn get_download(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<DownloadJob>, ApiError> {
    state
        .catalog
        .get_job(id)
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn download_file(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Response, ApiError> {
    let job = state.catalog.get_job(id).ok_or(ApiError::NotFound)?;
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
    let data = tokio::fs::read(state.export_dir.join(format!("{id}.{extension}")))
        .await
        .map_err(|_| ApiError::NotFound)?;
    let mut response = data.into_response();
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

impl From<IdempotencyError> for ApiError {
    fn from(error: IdempotencyError) -> Self {
        ApiError::Conflict(error.to_string())
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
