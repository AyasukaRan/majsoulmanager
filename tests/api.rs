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
use mjai_management::watch::{WatchEvent, WatchEventKind};
use mjai_management::{AppState, api, config::Config};
use serde_json::Value;
use std::io::Read;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use uuid::Uuid;

fn test_state() -> (AppState, std::path::PathBuf) {
    test_state_with_email(None)
}

fn test_state_with_email(email_api_url: Option<String>) -> (AppState, std::path::PathBuf) {
    let data_dir = std::env::temp_dir().join(format!("mjai-api-test-{}", Uuid::new_v4()));
    let state = AppState::local(Config {
        listen: "127.0.0.1:0".into(),
        api_key: "test-secret".into(),
        data_dir: data_dir.clone(),
        max_record_bytes: 16 * 1024,
        max_batch_bytes: 1024 * 1024,
        max_batch_records: 100,
        pack_target_bytes: 1024 * 1024,
        mihomo_controller_url: "http://127.0.0.1:1".into(),
        mihomo_secret: "test-mihomo-secret".into(),
        mihomo_proxy_url: "http://127.0.0.1:7890".into(),
        public_url: "http://localhost:3000".into(),
        admin_email: "admin@example.com".into(),
        admin_password: "test-password-123".into(),
        email_api_url,
        email_api_token: None,
        email_from: "noreply@example.com".into(),
    })
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

fn ingest_request(key: &str, body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/records")
        .header(header::AUTHORIZATION, "Bearer test-secret")
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header("idempotency-key", key)
        .header("x-mjai-source", "test-collector")
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn ingests_and_reads_one_record_without_reading_a_whole_pack() {
    let (state, data_dir) = test_state();
    let app = api::router(state);
    let raw = r#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;

    let response = app
        .clone()
        .oneshot(ingest_request("game-1", raw))
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

    let duplicate = app.oneshot(ingest_request("game-1", raw)).await.unwrap();
    assert_eq!(duplicate.status(), StatusCode::OK);
    let duplicate_json: Value =
        serde_json::from_slice(&to_bytes(duplicate.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(duplicate_json["duplicate"], true);

    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn rejects_unauthenticated_collection() {
    let (state, data_dir) = test_state();
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
    let (state, data_dir) = test_state();
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

    let (state, data_dir) = test_state_with_email(Some(email_url));
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
    let (state, data_dir) = test_state();
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
                .header("x-mjai-source", "test-collector")
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
    let (state, data_dir) = test_state();
    let app = api::router(state);
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    let response = app
        .clone()
        .oneshot(ingest_request("export-game", raw))
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
                .body(Body::from(
                    r#"{"filter":{"source":"test-collector"},"format":"tar.gz"}"#,
                ))
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
async fn reports_watch_uuid_and_conversion_transitions() {
    let (state, data_dir) = test_state();
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
    let (state, data_dir) = test_state();
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
                        "room":"throne",
                        "players":4,
                        "modes":["south"],
                        "server":"cn",
                        "account_secret_ref":"env:MAJSOUL_TEST_ACCOUNT",
                        "proxy_mode":"mihomo",
                        "custom_proxy_url":null,
                        "client_version":null,
                        "poll_interval_secs":15,
                        "request_delay_ms":800,
                        "login_module":{"name":"builtin","version":"majsoul2mjai-da985809"},
                        "pb_fetch_module":{"name":"builtin","version":"majsoul2mjai-da985809"}
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
    assert_eq!(json["room"], "throne");
    assert_eq!(state.watch_service.config().request_delay_ms, 800);

    let persisted: Value =
        serde_json::from_slice(&std::fs::read(data_dir.join("watch/config.json")).unwrap())
            .unwrap();
    assert_eq!(persisted["revision"], 2);
    assert_eq!(persisted["account_secret_ref"], "env:MAJSOUL_TEST_ACCOUNT");

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
