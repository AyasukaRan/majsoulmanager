use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode, header},
    routing::post,
};
use flate2::read::GzDecoder;
use mjai_management::auth::{
    AuthError, AuthSettings, LoginRequest, RegisterRequest, UserRole, VerifyEmailRequest,
};
use mjai_management::catalog::Catalog;
use mjai_management::pack::PackStore;
use mjai_management::watch::{WatchEvent, WatchEventKind};
use mjai_management::{AppState, api, config::Config, recovery};
use serde_json::Value;
use std::io::Read;
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
        max_record_bytes: 16 * 1024,
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
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

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
    let records = [
        (
            "one.mjson",
            r#"{"type":"start_game","names":["a","b","c","d"]}"#,
        ),
        (
            "two.mjson",
            r#"{"type":"start_game","names":["e","f","g","h"]}"#,
        ),
    ];
    let mut archive = tar::Builder::new(Vec::new());
    for (name, raw) in records {
        let mut header = tar::Header::new_gnu();
        header.set_size(raw.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, name, raw.as_bytes())
            .unwrap();
    }
    let body = archive.into_inner().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/records/batch")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/x-tar")
                .header("idempotency-key", "batch-1")
                .header("x-mjai-source", source.as_str())
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(json["accepted"], 2);
    assert_eq!(json["rejected"], 0);
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

/// Reads used to flush the insert buffer so that they could see recent writes,
/// which meant any read traffic cut the batch down to whatever had arrived
/// since the last read — a MergeTree part per record under a polling console.
#[tokio::test]
async fn a_read_sees_a_buffered_record_without_flushing_the_batch() {
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
        0,
        "the read flushed the batch instead of merging the buffer"
    );
    state.catalog.flush().await.unwrap();
    assert_eq!(indexed_rows(&config, &source).await, 1);
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
