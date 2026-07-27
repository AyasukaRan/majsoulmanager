use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode, header},
    routing::post,
};
use chrono::{SubsecRound, TimeDelta, Utc};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use mjai_management::auth::{
    AuthError, AuthSettings, LoginRequest, RegisterRequest, UserRole, VerifyEmailRequest,
};
use mjai_management::catalog::{Catalog, Cursor, RecordFilter};
use mjai_management::pack::PackStore;
use mjai_management::watch::{WatchEvent, WatchEventKind};
use mjai_management::{AppState, api, config::Config, recovery};
use serde_json::Value;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use uuid::Uuid;

/// The suite talks to the real PostgreSQL and ClickHouse; there is no in-memory
/// mode left to fall back to, and skipping when they are absent would leave the
/// SQL untested. `docker compose -f docker-compose.yml -f docker-compose.dev.yml
/// up -d postgres clickhouse` provides them locally, CI uses service containers.
fn test_config(data_dir: &std::path::Path, email_api_url: Option<String>) -> Config {
    Config {
        listen: "127.0.0.1:0".into(),
        api_key: "test-secret".into(),
        data_dir: data_dir.to_path_buf(),
        // The fixtures are real records; 16 KiB would reject every one of them.
        max_record_bytes: 256 * 1024,
        max_batch_bytes: 1024 * 1024,
        max_batch_records: 100,
        pack_target_bytes: 1024 * 1024,
        postgres_dsn: env_or(
            "MJAI_POSTGRES_DSN",
            "postgres://mjai:mjai@127.0.0.1:5432/mjai",
        ),
        clickhouse_url: env_or("MJAI_CLICKHOUSE_URL", "http://127.0.0.1:8123"),
        clickhouse_user: env_or("MJAI_CLICKHOUSE_USER", "mjai"),
        clickhouse_password: env_or("MJAI_CLICKHOUSE_PASSWORD", "mjai"),
        database_wait_secs: 30,
        mihomo_controller_url: "http://127.0.0.1:1".into(),
        mihomo_secret: "test-mihomo-secret".into(),
        mihomo_proxy_url: "http://127.0.0.1:7890".into(),
        public_url: "http://localhost:3000".into(),
        admin_email: "admin@example.com".into(),
        admin_password: "test-password-123".into(),
        email_api_url,
        email_api_token: None,
        email_from: "noreply@example.com".into(),
        // Nothing in this suite reaches object storage or the broker yet; these
        // carry the compiled-in defaults so the literal stays exhaustive.
        s3_endpoint_url: env_or("MJAI_S3_ENDPOINT_URL", "http://127.0.0.1:9000"),
        s3_access_key: env_or("MJAI_S3_ACCESS_KEY", "rustfsadmin"),
        s3_secret_key: env_or("MJAI_S3_SECRET_KEY", "rustfsadmin"),
        s3_bucket: env_or("MJAI_S3_BUCKET", "mjai-raw"),
        s3_region: env_or("MJAI_S3_REGION", "us-east-1"),
        kafka_bootstrap_servers: env_or("MJAI_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        kafka_topic: env_or("MJAI_KAFKA_TOPIC", "mjai.records.raw"),
        kafka_partitions: 1,
        kafka_max_lag: 50_000,
        pack_max_age_secs: 300,
        gc_grace_secs: 86_400,
        gc_interval_secs: 3_600,
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

/// Only a unique name, not a directory: `AppState::local` is what creates it,
/// so a test that builds a bare `Catalog` — which touches no filesystem — has
/// nothing to remove at the end and an unconditional `remove_dir_all` there
/// would fail on a path that was never created.
fn test_data_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("mjai-api-test-{}", Uuid::new_v4()))
}

/// Every test shares one durable index, so each one tags its records with a
/// source nobody else uses instead of relying on an empty table.
fn test_source(data_dir: &std::path::Path) -> String {
    data_dir.file_name().unwrap().to_string_lossy().into_owned()
}

async fn test_state() -> (AppState, std::path::PathBuf) {
    test_state_with_email(None).await
}

async fn test_state_with_email(email_api_url: Option<String>) -> (AppState, std::path::PathBuf) {
    let data_dir = test_data_dir();
    let state = AppState::local(test_config(&data_dir, email_api_url))
        .await
        .unwrap();
    (state, data_dir)
}

type EmailSender = Arc<Mutex<Option<tokio::sync::oneshot::Sender<Value>>>>;

async fn capture_email(
    State(sender): State<EmailSender>,
    Json(payload): Json<Value>,
) -> StatusCode {
    if let Some(sender) = sender.lock().unwrap().take() {
        let _ = sender.send(payload);
    }
    StatusCode::OK
}

fn ingest_request(source: &str, key: &str, body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/records")
        .header(header::AUTHORIZATION, "Bearer test-secret")
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header("idempotency-key", key)
        .header("x-mjai-source", source)
        .body(Body::from(body))
        .unwrap()
}

fn tar_of(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut archive = tar::Builder::new(Vec::new());
    for (name, raw) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(raw.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, name, raw.as_slice())
            .unwrap();
    }
    archive.into_inner().unwrap()
}

fn gzip(raw: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(raw).unwrap();
    encoder.finish().unwrap()
}

fn batch_request(source: &str, key: &str, body: Vec<u8>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/records/batch")
        .header(header::AUTHORIZATION, "Bearer test-secret")
        .header(header::CONTENT_TYPE, "application/x-tar")
        .header("idempotency-key", key)
        .header("x-mjai-source", source)
        .body(Body::from(body))
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn ingests_and_reads_one_record_without_reading_a_whole_pack() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let raw = r#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;

    let response = app
        .clone()
        .oneshot(ingest_request(&source, "game-1", raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let id = json["id"].as_str().unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/records/{id}/raw"))
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        raw
    );

    let duplicate = app
        .oneshot(ingest_request(&source, "game-1", raw))
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::OK);
    let duplicate_json: Value =
        serde_json::from_slice(&to_bytes(duplicate.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(duplicate_json["duplicate"], true);

    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn rejects_unauthenticated_collection() {
    let (state, data_dir) = test_state().await;
    let response = api::router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/records")
                .body(Body::from(r#"{"type":"start_game"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn logs_in_the_bootstrap_admin_and_protects_user_management() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"email":"admin@example.com","password":"test-password-123"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let login: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let session = login["session_token"].as_str().unwrap();
    assert_eq!(login["user"]["role"], "admin");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/auth/me")
                .header("x-mjai-user-session", session)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/users")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header("x-mjai-user-session", session)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"New user","email":"new@example.com","password":"new-password-123"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn requires_email_verification_before_a_registered_user_can_log_in() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let email_url = format!("http://{}/emails", listener.local_addr().unwrap());
    let (email_sender, email_receiver) = tokio::sync::oneshot::channel();
    let capture = Arc::new(Mutex::new(Some(email_sender)));
    let email_server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/emails", post(capture_email))
                .with_state(capture),
        )
        .await
    });

    let (state, data_dir) = test_state_with_email(Some(email_url)).await;
    let admin = state
        .auth
        .login(LoginRequest {
            email: "admin@example.com".into(),
            password: "test-password-123".into(),
        })
        .unwrap();
    state
        .auth
        .update_settings(
            &admin.session_token,
            AuthSettings {
                registration_enabled: true,
            },
        )
        .unwrap();
    state
        .auth
        .register(RegisterRequest {
            name: "Verified member".into(),
            email: "member@example.com".into(),
            password: "member-password-123".into(),
        })
        .await
        .unwrap();

    let error = state
        .auth
        .login(LoginRequest {
            email: "member@example.com".into(),
            password: "member-password-123".into(),
        })
        .unwrap_err();
    assert!(matches!(error, AuthError::EmailNotVerified));

    let email = email_receiver.await.unwrap();
    let text = email["text"].as_str().unwrap();
    let token = text.split("?token=").nth(1).unwrap().trim().to_owned();
    state
        .auth
        .verify_email(VerifyEmailRequest { token })
        .unwrap();
    let member = state
        .auth
        .login(LoginRequest {
            email: "member@example.com".into(),
            password: "member-password-123".into(),
        })
        .unwrap();
    assert_eq!(member.user.role, UserRole::Member);

    email_server.abort();
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn ingests_a_tar_batch_as_independent_records() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let body = tar_of(&two_records());

    let response = app
        .oneshot(batch_request(&source, "batch-1", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json = json_body(response).await;
    assert_eq!(json["accepted"], 2);
    assert_eq!(json["rejected"], 0);
    std::fs::remove_dir_all(data_dir).unwrap();
}

fn two_records() -> [(&'static str, Vec<u8>); 2] {
    [
        (
            "one.mjson",
            br#"{"type":"start_game","names":["a","b","c","d"]}"#.to_vec(),
        ),
        (
            "two.mjson",
            br#"{"type":"start_game","names":["e","f","g","h"]}"#.to_vec(),
        ),
    ]
}

/// Two real majsoul2mjai records, gzip exactly as the collector left them on disk (48,590 and
/// 25,575 bytes decompressed, both over the old 16 KiB record limit), plus a member that inflates
/// past the limit so a partially bad batch still answers 202.
#[tokio::test]
async fn ingests_gzip_tar_members_with_their_own_played_at() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let throne_4p =
        include_bytes!("fixtures/260716-00000000-0000-4000-8000-000000000004.mjson").to_vec();
    let members = [
        (
            "260716-00000000-0000-4000-8000-000000000004.mjson",
            throne_4p.clone(),
        ),
        (
            "260716-00000000-0000-4000-8000-000000000003.mjson",
            include_bytes!("fixtures/260716-00000000-0000-4000-8000-000000000003.mjson").to_vec(),
        ),
        ("oversize.mjson", gzip(&vec![b'{'; 1024 * 1024])),
    ];

    let response = app
        .clone()
        .oneshot(batch_request(&source, "batch-real", tar_of(&members)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json = json_body(response).await;
    assert_eq!(json["accepted"], 2);
    assert_eq!(json["rejected"], 1);
    assert!(json["errors"][0].as_str().unwrap().starts_with("oversize"));

    // A batch-wide played_at would return both records or neither; only the 4p throne game
    // started inside this window (majsoul.start_time 1784207242 = 2026-07-16T13:07:22Z).
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/records?played_from=2026-07-16T13:00:00Z&played_to=2026-07-16T14:00:00Z")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page = json_body(response).await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["played_at"], "2026-07-16T13:07:22Z");
    assert_eq!(page["items"][0]["players"].as_array().unwrap().len(), 4);
    let id = page["items"][0]["id"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/records/{id}/raw"))
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut expected = Vec::new();
    GzDecoder::new(throne_4p.as_slice())
        .read_to_end(&mut expected)
        .unwrap();
    // The point of the fixture: a real record is far past the old 16 KiB limit,
    // so this round trip could not have happened before it was raised.
    assert!(expected.len() > 16 * 1024, "{} bytes", expected.len());
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        expected
    );

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The decompression bound is the only thing that can reject this member: the gzip trailer is cut
/// off, so a reader that does not stop at the limit runs into the mutilated stream and reports a
/// gzip error instead. A batch that landed nothing must also not answer 2xx.
#[tokio::test]
async fn stops_reading_a_member_that_inflates_past_the_record_limit() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let mut bomb = gzip(&vec![b'{'; 1024 * 1024]);
    bomb.truncate(bomb.len() - 8);

    let response = app
        .oneshot(batch_request(
            &source,
            "batch-bomb",
            tar_of(&[("bomb.mjson", bomb)]),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = json_body(response).await;
    assert_eq!(json["accepted"], 0);
    assert_eq!(json["rejected"], 1);
    assert_eq!(
        json["errors"][0],
        "bomb.mjson: decompresses past the 262144 byte record limit"
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// A wholesale batch failure answers 422, so its error list is the only diagnosis the operator
/// gets. Reporting an empty record as an oversized one sends them to the wrong knob.
#[tokio::test]
async fn names_an_empty_member_as_empty_rather_than_oversized() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);

    let response = app
        .oneshot(batch_request(
            &source,
            "batch-empty",
            tar_of(&[("empty.mjson", gzip(b""))]),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = json_body(response).await;
    assert_eq!(json["rejected"], 1);
    assert_eq!(json["errors"][0], "empty.mjson: record is empty");
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// `gzip -c a b > c`, and every block-gzip writer, produce a file of concatenated gzip streams.
/// Decoding only the first stores a truncated record and indexes it as complete.
#[tokio::test]
async fn reads_every_gzip_stream_of_a_member() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let record = br#"{"type":"start_game","names":["a","b","c","d"]}
{"type":"end_game"}"#;
    let (head, tail) = record.split_at(record.len() / 2);
    let member = [gzip(head), gzip(tail)].concat();

    let response = app
        .clone()
        .oneshot(batch_request(
            &source,
            "batch-split-member",
            tar_of(&[("split.mjson", member)]),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json = json_body(response).await;
    assert_eq!(json["accepted"], 1, "{json}");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/records")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Both events survived, so the second stream was not dropped.
    assert_eq!(json_body(response).await["items"][0]["event_count"], 2);
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The archive itself has the same shape of failure, with thousands of records behind it instead
/// of one.
#[tokio::test]
async fn reads_every_gzip_stream_of_the_archive() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let archive = tar_of(&two_records());
    let (head, tail) = archive.split_at(archive.len() / 2);

    let response = app
        .oneshot(batch_request(
            &source,
            "batch-split-archive",
            [gzip(head), gzip(tail)].concat(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(json_body(response).await["accepted"], 2);
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// Failing to write the pack is this server losing records, not the caller sending bad ones, so
/// the batch must fail loudly instead of listing the loss as rejected members inside a 202.
#[tokio::test]
async fn fails_the_batch_when_records_cannot_be_stored() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    // No pack directory, so appending cannot open a pack file: an I/O failure without needing a
    // full disk.
    std::fs::remove_dir_all(data_dir.join("packs")).unwrap();

    let response = app
        .oneshot(batch_request(
            &source,
            "batch-broken-disk",
            tar_of(&two_records()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        json_body(response).await["error"]
            .as_str()
            .unwrap()
            .starts_with("one.mjson:")
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn creates_and_downloads_a_filtered_archive() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    let response = app
        .clone()
        .oneshot(ingest_request(&source, "export-game", raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"filter":{{"source":"{source}"}},"format":"tar.gz"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let job: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let job_id = job["id"].as_str().unwrap();

    for _ in 0..100 {
        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/downloads/{job_id}"))
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status_json: Value =
            serde_json::from_slice(&to_bytes(status.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        if status_json["state"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/downloads/{job_id}/file"))
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // A streamed body carries no content type of its own, so switching away
    // from `Vec<u8>` silently dropped this one.
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&header::HeaderValue::from_static(
            "application/octet-stream"
        ))
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let decoder = GzDecoder::new(bytes.as_ref());
    let mut archive = tar::Archive::new(decoder);
    let mut entries = archive.entries().unwrap();
    let mut entry = entries.next().unwrap().unwrap();
    let mut extracted = String::new();
    entry.read_to_string(&mut extracted).unwrap();
    assert_eq!(extracted, raw);
    drop(entry);
    assert!(entries.next().is_none());
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn re_indexes_packs_the_index_never_saw() {
    // The bug this reproduces: pack bytes on disk with nothing pointing at
    // them, which is what every restart used to produce.
    let data_dir = test_data_dir();
    let packs = PackStore::new(data_dir.join("packs"), 1024 * 1024).unwrap();
    let raw = br#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}"#;
    let orphans: Vec<Uuid> = (0..3)
        .map(|_| {
            let id = Uuid::new_v4();
            packs.append(id, raw).unwrap();
            id
        })
        .collect();

    let catalog = Catalog::connect(&test_config(&data_dir, None))
        .await
        .unwrap();
    assert_eq!(recovery::recover(&catalog, &packs).await.unwrap(), 3);
    // Every boot runs this, so a second pass must find nothing left to do.
    assert_eq!(recovery::recover(&catalog, &packs).await.unwrap(), 0);

    let record = catalog.get(orphans[0]).await.unwrap().unwrap();
    assert_eq!(record.source, "recovered");
    assert_eq!(record.players, ["a", "b", "c", "d"]);
    assert_eq!(record.rule.as_deref(), Some("tonpu"));
    assert_eq!(packs.read(&record.storage).unwrap(), raw);
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The migration lock used to be session scoped, and dropping the sqlx
/// connection that held it only returned it to the pool. The lock therefore
/// outlived `connect`, so the second replica to boot blocked until the first
/// process exited.
#[tokio::test]
async fn a_second_boot_is_not_blocked_by_the_first_migration_lock() {
    let data_dir = test_data_dir();
    let first = Catalog::connect(&test_config(&data_dir, None))
        .await
        .unwrap();
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        Catalog::connect(&test_config(&data_dir, None)),
    )
    .await
    .expect("the second boot blocked on the migration advisory lock");
    // Both are live at once, which is the replica case: neither may be holding
    // anything that stops the other from working.
    second.unwrap().flush().await.unwrap();
    first.flush().await.unwrap();
}

/// A ClickHouse that accepts the connection and never answers fails no
/// client-side check, so startup used to hang on the first probe rather than
/// giving up at MJAI_DATABASE_WAIT_SECS and letting the restart policy make the
/// outage visible.
#[tokio::test]
async fn a_clickhouse_that_never_answers_gives_up_at_the_startup_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let sink = tokio::spawn(async move {
        let mut accepted = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            accepted.push(stream);
        }
    });

    let data_dir = test_data_dir();
    let mut config = test_config(&data_dir, None);
    config.clickhouse_url = format!("http://{address}");
    config.database_wait_secs = 5;
    let started = std::time::Instant::now();
    // Bounded here as well: without the fix this never returns, and a hung test
    // costs the whole CI job rather than one red line.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(45),
        Catalog::connect(&config),
    )
    .await
    .expect("startup never gave up on a ClickHouse that accepts and never answers");
    assert!(
        outcome.is_err(),
        "a black hole was accepted as a ready index"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(30),
        "startup overran its own wait by {:?}",
        started.elapsed()
    );
    sink.abort();
}

/// Writes batch until something reads, and a read then sees its own write. A
/// read gets there by flushing the buffer rather than merging it into the page:
/// the merge had to reproduce ClickHouse's ordering and filtering in Rust, and
/// twice did not. What batching is worth is asserted first — an insert on its
/// own must not reach the index.
#[tokio::test]
async fn a_read_flushes_the_buffer_and_sees_its_own_write() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let config = test_config(&data_dir, None);
    let app = api::router(state.clone());
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    let response = app
        .clone()
        .oneshot(ingest_request(&source, "buffered-1", raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        indexed_rows(&config, &source).await,
        0,
        "an insert reached the index before the batch was full"
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/records?source={source}"))
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        page["items"].as_array().unwrap().len(),
        1,
        "a read stopped seeing its own write"
    );

    assert_eq!(
        indexed_rows(&config, &source).await,
        1,
        "the read answered from somewhere other than the index it flushed into"
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// A flush now sends a fixed batch at a time and, so that reads are not stuck
/// behind it, releases the buffer lock across the insert. That is exactly the
/// window in which another writer appends, so the flush may only retire the
/// rows it actually sent. Concurrent writers are the point of the test: a
/// single-threaded run cannot tell a correct flush from one that clears the
/// whole buffer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_only_retires_the_rows_it_sent() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Arc::new(Catalog::connect(&config).await.unwrap());
    let writers = 8u32;
    let each = 500u32;
    let mut handles = Vec::new();
    for writer in 0..writers {
        let catalog = Arc::clone(&catalog);
        let source = source.clone();
        handles.push(tokio::spawn(async move {
            for index in 0..each {
                catalog
                    .insert(sample_record(&source, writer * each + index))
                    .await
                    .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    catalog.flush().await.unwrap();
    assert_eq!(
        indexed_rows(&config, &source).await,
        u64::from(writers * each),
        "a flush discarded rows that were appended while it was in flight"
    );
}

fn sample_record(source: &str, index: u32) -> mjai_management::catalog::Record {
    mjai_management::catalog::Record {
        id: Uuid::new_v4(),
        source: source.to_owned(),
        sha256: "0".repeat(64),
        received_at: chrono::Utc::now(),
        played_at: None,
        players: vec!["a".into(), "b".into(), "c".into(), "d".into()],
        rule: None,
        event_count: index,
        storage: mjai_management::pack::PackLocation {
            pack_key: "packs/batch.mjpack".into(),
            offset: u64::from(index),
            compressed_size: 1,
            raw_size: 1,
            codec: "zstd",
        },
    }
}

/// Distinct rows in the index for one source, straight from ClickHouse: what a
/// test cannot ask the catalogue, because the catalogue answers from its
/// buffer as well.
async fn indexed_rows(config: &Config, source: &str) -> u64 {
    #[derive(serde::Deserialize)]
    struct Rows {
        rows: u64,
    }
    let index = mjai_management::clickhouse::ClickHouse::new(
        &config.clickhouse_url,
        &config.clickhouse_user,
        &config.clickhouse_password,
    )
    .unwrap();
    let rows: Vec<Rows> = index
        .query(
            "SELECT uniqExact(record_id) AS rows FROM mjai.records \
             WHERE source = {source:String}",
            &[("source", source.to_owned())],
        )
        .await
        .unwrap();
    rows.first().map(|row| row.rows).unwrap_or_default()
}

#[tokio::test]
async fn pages_records_by_keyset_without_repeating_or_dropping_one() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    for key in ["page-1", "page-2", "page-3"] {
        let response = app
            .clone()
            .oneshot(ingest_request(&source, key, raw))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..3 {
        let query = match &cursor {
            Some(cursor) => format!("source={source}&limit=2&cursor={cursor}"),
            None => format!("source={source}&limit=2"),
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/records?{query}"))
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let page: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        for item in page["items"].as_array().unwrap() {
            seen.push(item["id"].as_str().unwrap().to_owned());
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(seen.len(), 3, "keyset paging lost or repeated a record");
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 3);
    assert!(cursor.is_none(), "the last page still offered a cursor");
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// Two records 400us and 900us into the same millisecond, paged one at a time.
/// The buffer used to hold `Utc::now()` at nanosecond precision while the index
/// stores DateTime64(3) and a cursor token carries epoch millis, so a page
/// ending mid-millisecond handed back a cursor that truncated below its own
/// last row and the next page skipped everything in between. A tar batch import
/// puts several records in one millisecond as a matter of course.
#[tokio::test]
async fn pages_two_records_that_share_one_millisecond() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Catalog::connect(&config).await.unwrap();
    // A second back so the records stay inside the window `search` bounds to
    // `now`, which is read after they are written.
    let millisecond = Utc::now().trunc_subsecs(3) - TimeDelta::seconds(1);
    for micros in [400i64, 900] {
        let mut record = sample_record(&source, micros as u32);
        record.received_at = millisecond + TimeDelta::microseconds(micros);
        catalog.insert(record).await.unwrap();
    }

    let filter = RecordFilter {
        source: Some(source),
        ..RecordFilter::default()
    };
    let mut seen = Vec::new();
    let mut cursor = None;
    for _ in 0..3 {
        let (page, next) = catalog.search(&filter, cursor, 1).await.unwrap();
        seen.extend(page.into_iter().map(|record| record.id));
        // Through the token, because the token is all a client ever holds and
        // the token is where the resolution used to be lost.
        cursor = next.map(|next| next.to_string().parse::<Cursor>().unwrap());
        if cursor.is_none() {
            break;
        }
    }

    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        2,
        "keyset paging dropped a record sharing a millisecond with another"
    );
}

/// A retried flush sends rows the index already holds, so the same record can
/// reach ClickHouse twice. Collapsing that pair is `FINAL`'s job and nothing
/// else's now that reads flush rather than merge — which only works while the
/// two copies land on the same sorting key, and `received_at` is part of it.
#[tokio::test]
async fn a_record_in_both_the_index_and_the_buffer_is_returned_once() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Catalog::connect(&config).await.unwrap();
    let millisecond = Utc::now().trunc_subsecs(3) - TimeDelta::seconds(1);
    let mut duplicated = sample_record(&source, 900);
    duplicated.received_at = millisecond + TimeDelta::microseconds(900);
    catalog.insert(duplicated.clone()).await.unwrap();
    catalog.flush().await.unwrap();
    // The state that race leaves behind: the index holds the record, the buffer
    // holds it again, and a third record is dated between the two copies.
    catalog.insert(duplicated).await.unwrap();
    let mut between = sample_record(&source, 400);
    between.received_at = millisecond + TimeDelta::microseconds(400);
    catalog.insert(between).await.unwrap();

    let filter = RecordFilter {
        source: Some(source),
        ..RecordFilter::default()
    };
    let (page, _) = catalog.search(&filter, None, 10).await.unwrap();
    let mut ids: Vec<_> = page.iter().map(|record| record.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 2, "a page lost a record");
    assert_eq!(page.len(), ids.len(), "a page returned one record twice");
}

/// Three ids whose ClickHouse order is the exact reverse of their `Uuid: Ord`
/// order: ClickHouse compares a UUID as (low 64 bits, high 64 bits) while Rust
/// compares the sixteen bytes big-endian. A random 32 bit prefix, shared by both
/// halves so it cancels out of either comparison, keeps each run's ids distinct
/// in the durable index.
fn ids_that_sort_opposite_ways() -> [Uuid; 3] {
    let tag = Uuid::new_v4().as_u64_pair().0 & 0xffff_ffff_0000_0000;
    [0u64, 1, 2].map(|nth| Uuid::from_u64_pair(tag | nth, tag | (2 - nth)))
}

/// The page order is `(received_at DESC, record_id DESC)`, so a timestamp tie
/// leaves the whole page order resting on the collation of `record_id` — and
/// Rust and ClickHouse do not agree on it. Reads used to sort ClickHouse's
/// answer again in Rust and take the cursor from that second order, so the next
/// page's SQL comparison, made in ClickHouse's order, excluded rows the client
/// had never been handed. Millisecond truncation made ties ordinary rather than
/// exotic: a tar batch import puts several records in one millisecond.
#[tokio::test]
async fn pages_a_timestamp_tie_that_lives_in_the_index() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Catalog::connect(&config).await.unwrap();
    let ids = ids_that_sort_opposite_ways();
    assert!(
        ids[0] < ids[1] && ids[1] < ids[2],
        "the ids no longer straddle the two collations, so this test proves nothing"
    );
    // A second back so the records stay inside the window `search` bounds to
    // `now`, which is read after they are written.
    let millisecond = Utc::now().trunc_subsecs(3) - TimeDelta::seconds(1);
    for (index, id) in ids.iter().enumerate() {
        let mut record = sample_record(&source, index as u32);
        record.id = *id;
        record.received_at = millisecond;
        catalog.insert(record).await.unwrap();
    }
    // The rows have to be in ClickHouse, not the buffer: the buffer never sees
    // the SQL `ORDER BY` or the SQL cursor comparison that lost them.
    catalog.flush().await.unwrap();
    assert_eq!(indexed_rows(&config, &source).await, 3);

    let filter = RecordFilter {
        source: Some(source),
        ..RecordFilter::default()
    };
    let mut seen = Vec::new();
    let mut cursor = None;
    for _ in 0..4 {
        let (page, next) = catalog.search(&filter, cursor, 1).await.unwrap();
        seen.extend(page.into_iter().map(|record| record.id));
        // Through the token, because a token is all a client ever holds.
        cursor = next.map(|next| next.to_string().parse::<Cursor>().unwrap());
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(
        seen.len(),
        3,
        "keyset paging over a timestamp tie in the index returned {seen:?}"
    );
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 3, "a page repeated a record");
}

/// `page` sends every bound over the wire floored to milliseconds, because that
/// is the resolution the column has. The buffer used to be compared against the
/// same bound at full precision, so a `received_from` inside the millisecond a
/// record was truncated to answered nothing before a flush and one record after
/// it. `RecordQuery` parses RFC 3339, which admits sub-millisecond bounds.
#[tokio::test]
async fn a_sub_millisecond_bound_answers_the_same_before_and_after_a_flush() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Catalog::connect(&config).await.unwrap();
    let millisecond = Utc::now().trunc_subsecs(3) - TimeDelta::seconds(1);
    let bound = millisecond + TimeDelta::microseconds(400);
    let mut record = sample_record(&source, 400);
    record.received_at = bound;
    catalog.insert(record).await.unwrap();

    let filter = RecordFilter {
        source: Some(source),
        received_from: Some(bound),
        ..RecordFilter::default()
    };
    let (before, _) = catalog.search(&filter, None, 10).await.unwrap();
    catalog.flush().await.unwrap();
    let (after, _) = catalog.search(&filter, None, 10).await.unwrap();
    // One, not zero: the record is stored at the bottom of its millisecond, so
    // flooring an inclusive `from` bound to the same millisecond is the only
    // answer that does not drop it.
    assert_eq!(after.len(), 1, "a floored bound dropped its own record");
    assert_eq!(
        before.len(),
        after.len(),
        "a sub-millisecond bound answered differently before and after a flush"
    );
}

/// The mirror of the `from` case, and the one that loses data: a record stored at the bottom of its
/// millisecond is genuinely earlier than an exclusive `to` later in the same millisecond, so
/// flooring that bound too would exclude it from a range it belongs to.
#[tokio::test]
async fn an_exclusive_sub_millisecond_upper_bound_keeps_the_record_it_covers() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Catalog::connect(&config).await.unwrap();
    let millisecond = Utc::now().trunc_subsecs(3) - TimeDelta::seconds(1);
    let mut record = sample_record(&source, 401);
    record.received_at = millisecond + TimeDelta::microseconds(200);
    catalog.insert(record).await.unwrap();

    let filter = RecordFilter {
        source: Some(source.clone()),
        received_to: Some(millisecond + TimeDelta::microseconds(400)),
        ..RecordFilter::default()
    };
    let (before, _) = catalog.search(&filter, None, 10).await.unwrap();
    catalog.flush().await.unwrap();
    let (after, _) = catalog.search(&filter, None, 10).await.unwrap();
    assert_eq!(
        after.len(),
        1,
        "an exclusive upper bound dropped a record that arrived before it"
    );
    assert_eq!(before.len(), after.len());

    // The bound still excludes: a record in the NEXT millisecond is out of range.
    let mut later = sample_record(&source, 402);
    later.received_at = millisecond + TimeDelta::milliseconds(1);
    catalog.insert(later).await.unwrap();
    catalog.flush().await.unwrap();
    let (page, _) = catalog.search(&filter, None, 10).await.unwrap();
    assert_eq!(
        page.len(),
        1,
        "rounding the bound outwards pulled in a later millisecond"
    );
}

#[tokio::test]
async fn rejects_a_reused_idempotency_key_with_different_content() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);
    let response = app
        .clone()
        .oneshot(ingest_request(
            &source,
            "same-key",
            r#"{"type":"start_game","names":["a","b","c","d"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(ingest_request(
            &source,
            "same-key",
            r#"{"type":"start_game","names":["e","f","g","h"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn reports_watch_uuid_and_conversion_transitions() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state.clone());
    for event in [
        WatchEventKind::Live,
        WatchEventKind::Pending,
        WatchEventKind::Fetching,
        WatchEventKind::Converting,
        WatchEventKind::Completed,
    ] {
        state
            .watch
            .apply(WatchEvent {
                uuid: "260723-abcdef01-2345-6789-abcd-ef0123456789".into(),
                event,
                mode_id: Some(16),
                started_at: None,
                message: None,
                record_id: None,
            })
            .unwrap();
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/watch/status?limit=10")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(json["total"], 1);
    assert_eq!(json["completed"], 1);
    assert_eq!(json["items"][0]["uuid_state"], "fetched");
    assert_eq!(json["items"][0]["conversion_state"], "completed");
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn updates_and_persists_online_watch_configuration() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/watch/config")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{
                        "revision":1,
                        "enabled":false,
                        "server":"cn",
                        "proxy_mode":"mihomo",
                        "custom_proxy_url":null,
                        "poll_interval_secs":15,
                        "request_delay_ms":800,
                        "login_module":{"name":"builtin","version":"majsoul2mjai-da985809"},
                        "pb_fetch_module":{"name":"builtin","version":"majsoul2mjai-da985809"},
                        "instances":[
                            {
                                "id":"four-player",
                                "enabled":true,
                                "room":"throne",
                                "players":4,
                                "modes":["south"],
                                "account_secret_ref":"env:MAJSOUL_TEST_ACCOUNT",
                                "client_version":null
                            },
                            {
                                "id":"three-player",
                                "enabled":true,
                                "room":"jade",
                                "players":3,
                                "modes":["east","south"],
                                "account_secret_ref":"env:MAJSOUL_TEST_ACCOUNT_3P",
                                "client_version":null
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(json["revision"], 2);
    assert_eq!(json["instances"][0]["room"], "throne");
    assert_eq!(json["instances"][1]["players"], 3);
    assert_eq!(state.watch_service.config().request_delay_ms, 800);
    assert_eq!(state.watch_service.config().instances.len(), 2);

    let persisted: Value =
        serde_json::from_slice(&std::fs::read(data_dir.join("watch/config.json")).unwrap())
            .unwrap();
    assert_eq!(persisted["revision"], 2);
    assert_eq!(
        persisted["instances"][0]["account_secret_ref"],
        "env:MAJSOUL_TEST_ACCOUNT"
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/watch/modules")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let modules: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(modules.as_array().unwrap().len(), 2);
    std::fs::remove_dir_all(data_dir).unwrap();
}
