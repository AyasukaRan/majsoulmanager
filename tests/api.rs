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
use mjai_management::objects::Objects;
use mjai_management::pack::PackStore;
use mjai_management::watch::{WatchEvent, WatchEventKind};
use mjai_management::{AppState, api, config::Config, recovery};
use serde_json::Value;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower::ServiceExt;
use uuid::Uuid;

/// The suite talks to the real PostgreSQL, ClickHouse, Redpanda and RustFS;
/// there is no in-memory mode left to fall back to, and skipping when they are
/// absent would leave the SQL, the produce path and the pack upload untested.
/// `docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
/// postgres clickhouse redpanda rustfs create-bucket` provides them locally, CI
/// starts the same four.
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
        pack_idle_secs: 30,
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

/// Runs the pack/index worker until the topic is empty, then seals, uploads and
/// indexes what it read. Ingest only promises the record is in the topic, so
/// every test that reads a record back has to put this between the two.
///
/// Partition 0 is the whole topic at the default partition count, and the
/// offset is shared: a worker started by one test will index records another
/// test produced, into a pack under its own data directory. That is harmless
/// and worth knowing — the object lands in the same bucket either way, both
/// tests read it back through the same index, and a record indexed twice
/// converges because `received_at` and `record_id` both travel with the
/// message rather than being re-derived here.
async fn index_pending(state: &AppState) {
    mjai_management::indexer::PackWorker::start(state.clone(), 0)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
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

fn ingest_request(source: &str, key: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/records")
        .header(header::AUTHORIZATION, "Bearer test-secret")
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header("idempotency-key", key)
        .header("x-mjai-source", source)
        .body(Body::from(body.to_owned()))
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
    let app = api::router(state.clone());
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
    // The record is in the topic, not in a pack: the read below is of the pack
    // the worker builds, and of the object it uploads it to.
    index_pending(&state).await;

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
        .clone()
        .oneshot(ingest_request(&source, "game-1", raw))
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::OK);
    let duplicate_json: Value =
        serde_json::from_slice(&to_bytes(duplicate.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(duplicate_json["duplicate"], true);

    // A record with no game of its own is still deduplicated by the key the
    // caller chose, and there the key is a promise that it names this content.
    // Reusing it for different bytes is the caller contradicting itself, and
    // has to stay a refusal rather than quietly resolving to the first record —
    // which is what a game-scoped claim deliberately does, and what this proves
    // was not turned on for everything.
    let contradicted = app
        .oneshot(ingest_request(
            &source,
            "game-1",
            r#"{"type":"start_game","names":["w","x","y","z"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(contradicted.status(), StatusCode::CONFLICT);

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// A game has one identity and two ingests of it are one record, whatever
/// source presented them. Deduplication used to be scoped by the caller's
/// source, so a game the collector had already stored came back out of an
/// archive as a second record — two ids, two rows, counted twice in every
/// aggregate and returned twice by every filter that matched it — and
/// re-importing one archive under a different batch key re-ingested all of it.
///
/// The first request here is byte for byte what the live collector sends, which
/// is the other half of what this has to get right: the claims already in
/// PostgreSQL were written by that path, and the scope has to keep reproducing
/// them or the whole collected corpus is re-ingested under fresh record ids.
#[tokio::test]
async fn collapses_one_game_ingested_from_two_sources() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state.clone());
    // Unique per run: the scope no longer contains the source, and the suite's
    // PostgreSQL outlives a run, so a fixed uuid would be claimed once and
    // answered as a duplicate for every run after it.
    let game = format!("260716-{}", Uuid::new_v4());
    // 2026-07-16T05:00:00Z, kept well clear of the 13:00–14:00 window
    // `ingests_gzip_tar_members_with_their_own_played_at` asserts holds exactly
    // one record. The suite shares one index, so a `played_at` is as much a
    // shared namespace as a source is, and copying one out of a fixture is
    // enough to break a test that never mentions this one.
    let collected = format!(
        r#"{{"type":"start_game","names":["a","b","c","d"],"majsoul":{{"uuid":"{game}","start_time":1784178000,"room":"throne","game_length":"south","players":4}}}}
{{"type":"start_kyoku","bakaze":"E","kyoku":1}}"#
    );

    let response = app
        .clone()
        // The collector's own source and its own key, which is the game uuid.
        .oneshot(ingest_request("majsoul-watch", &game, &collected))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let first = json_body(response).await;

    // The same game out of an archive: a different source, a key built from a
    // batch and a member path, and bytes that do not match — a second
    // conversion of one game renders it slightly differently, and that is still
    // the game we already have rather than a caller reusing a key for unrelated
    // content. The copy in hand is dropped; first writer wins.
    let response = app
        .oneshot(ingest_request(
            &test_source(&data_dir),
            "archive-2026-07/260716.mjson",
            &format!("{collected}\n{{\"type\":\"end_game\"}}"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let second = json_body(response).await;
    assert_eq!(second["duplicate"], true, "{second}");
    assert_eq!(second["id"], first["id"], "one game must be one record");

    // The stored shape, spelled out, because the two assertions above hold for
    // any scope both requests agree on and this is the one thing about it that
    // cannot be re-derived: the claims of every game collected so far are
    // already in this table under `sha256("majsoul-watch\0{uuid}")`. A scope
    // that stopped reproducing that string would still pass everything above,
    // and would re-ingest the entire live corpus under fresh record ids the
    // moment it shipped.
    let claimed: Uuid =
        sqlx::query_scalar("SELECT record_id FROM ingest_idempotency WHERE key_hash = sha256($1)")
            .bind(format!("majsoul-watch\0{game}").into_bytes())
            .fetch_one(state.catalog.postgres())
            .await
            .unwrap();
    assert_eq!(claimed.to_string(), first["id"].as_str().unwrap());

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// A broker mutes a connection while a request is in flight on it and does not
/// read the next one until it has answered. The pack worker keeps a fetch parked
/// on its connection for up to a second waiting for a record, so a producer
/// sharing that connection has its produce sit unread in the socket for the
/// whole poll — including when the produce carries exactly the record that would
/// have satisfied the fetch. Measured on the historical import, that paced
/// ingest at 30 records a second with the broker answering each produce in
/// 0.3ms, the API on 0.6 of 16 cores and the pack worker caught up.
///
/// So the producer and the consumer must not share a `PartitionClient`: one of
/// those is one connection. Collapsing the two back into one — which reads like
/// an obvious simplification — costs an order of magnitude and nothing fails.
///
/// Its own topic, because the suite's shared one has other tests producing to
/// it, and any one of their records would end the parked fetch early and let
/// this pass whatever the connections are doing.
#[tokio::test]
async fn a_produce_does_not_queue_behind_the_pack_workers_long_poll() {
    let data_dir = test_data_dir();
    let mut config = test_config(&data_dir, None);
    config.kafka_topic = format!("mjai.produce-behind-poll.{}", Uuid::new_v4());
    let kafka = mjai_management::kafka::Kafka::connect(&config)
        .await
        .unwrap();

    // Offset 0 of an empty log is the end of the log, so nothing satisfies this
    // and it stays in flight for the full `FETCH_MAX_WAIT_MS`.
    let consumer = kafka.consumer(0).unwrap();
    let parked = tokio::spawn(async move { consumer.fetch(0).await });
    // Long enough for the fetch to be on the wire, short against the poll it is
    // parked in.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    kafka
        .produce(mjai_management::kafka::IngestMessage::new(
            Uuid::new_v4(),
            "produce-behind-poll",
            None,
            br#"{"type":"start_game","names":["a","b","c","d"]}"#.to_vec(),
        ))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    // The record makes the fetch satisfiable, so this returns rather than
    // running out the poll — and a failure to await it would leave the task
    // holding a connection past the end of the test.
    parked.await.unwrap().unwrap();

    // Half the poll: a produce on its own connection takes single-digit
    // milliseconds, and one queued behind the poll cannot come back before the
    // remaining ~900ms of it have elapsed. Nothing lands between those.
    assert!(
        elapsed < Duration::from_millis(500),
        "the produce waited {elapsed:?}, which is the length of the pack worker's \
         long poll rather than the length of a produce: the two are sharing a connection"
    );
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
    let app = api::router(state.clone());
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
    index_pending(&state).await;

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
    let app = api::router(state.clone());
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
    index_pending(&state).await;

    // By source, because the whole suite shares one index and an unfiltered
    // page is whatever another test ingested most recently.
    let page = search_by_source(&app, &source).await;
    // Both events survived, so the second stream was not dropped.
    assert_eq!(page["items"][0]["event_count"], 2, "{page}");
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

/// Failing to keep a record the caller handed over is this server losing it, not the caller
/// sending bad ones, so the batch must fail loudly instead of listing the loss as rejected members
/// inside a 202.
#[tokio::test]
async fn fails_the_batch_when_records_cannot_be_stored() {
    let data_dir = test_data_dir();
    let source = test_source(&data_dir);
    // A backlog ceiling of zero refuses every record, which is the one way to make ingest refuse
    // to keep a record without breaking a store the rest of the suite shares. What it stands in
    // for is a full disk or a broker that will not take a produce: all three reach the batch as
    // `ApiError::Internal`, which is the distinction under test.
    let mut config = test_config(&data_dir, None);
    config.kafka_max_lag = 0;
    let app = api::router(AppState::local(config).await.unwrap());

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

/// A batch that fails partway has already claimed idempotency keys for the
/// members it got through, and those records never reached the topic. Unless
/// the claims are released, the retry the operator is supposed to make is told
/// they are duplicates and skips them: the records exist nowhere, and every
/// response says the import succeeded.
///
/// The failure is forced with a record ceiling of one, because it is the only
/// way to fail the walk *after* a member has been claimed — the backlog ceiling
/// used above is checked before the claim, so it leaves nothing to release and
/// would let this whole path be deleted with the suite still green.
#[tokio::test]
async fn releases_the_claims_of_a_batch_that_failed_partway() {
    let data_dir = test_data_dir();
    let source = test_source(&data_dir);
    let archive = tar_of(&two_records());

    let mut ceilinged = test_config(&data_dir, None);
    ceilinged.max_batch_records = 1;
    let response = api::router(AppState::local(ceilinged).await.unwrap())
        .oneshot(batch_request(&source, "batch-partial", archive.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // The same archive under the same key. The first member was claimed and
    // dropped, so it has to come back as accepted; counted as a duplicate, it
    // would be a record this server lost while reporting success.
    let state = AppState::local(test_config(&data_dir, None)).await.unwrap();
    let response = api::router(state)
        .oneshot(batch_request(&source, "batch-partial", archive))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await;
    assert_eq!(body["accepted"], 2, "a released claim must re-ingest");
    assert_eq!(body["duplicates"], 0);
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test]
async fn creates_and_downloads_a_filtered_archive() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    let response = app
        .clone()
        .oneshot(ingest_request(&source, "export-game", raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    index_pending(&state).await;

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
    let config = test_config(&data_dir, None);
    // Nothing here leaves the local pack directory, so the endpoint is only a
    // constructor argument; a read that fell through to it would be the bug.
    let objects = Arc::new(
        Objects::new(
            &config.s3_endpoint_url,
            &config.s3_bucket,
            &config.s3_region,
            &config.s3_access_key,
            &config.s3_secret_key,
        )
        .unwrap(),
    );
    let packs = PackStore::new(&data_dir, objects).unwrap();
    let raw = br#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}"#;
    // In the legacy directory, under a flat key, which is the only shape this scan will ever meet:
    // it is the corpus collected before the pack worker existed, and the staging packs the worker
    // fills are discarded rather than scanned.
    let mut writer = packs.legacy_writer().unwrap();
    let orphans: Vec<Uuid> = (0..3)
        .map(|_| {
            let id = Uuid::new_v4();
            writer.append(id, raw).unwrap();
            id
        })
        .collect();

    let catalog = Catalog::connect(&config).await.unwrap();
    assert_eq!(recovery::recover(&catalog, &packs).await.unwrap(), 3);
    // Every boot runs this, so a second pass must find nothing left to do.
    assert_eq!(recovery::recover(&catalog, &packs).await.unwrap(), 0);

    let record = catalog.get(orphans[0]).await.unwrap().unwrap();
    assert_eq!(record.source, "recovered");
    assert_eq!(record.players, ["a", "b", "c", "d"]);
    assert_eq!(record.rule.as_deref(), Some("tonpu"));
    assert_eq!(packs.read(&record.storage).await.unwrap(), raw);
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
    second.unwrap().indexed_counts().await.unwrap();
    first.indexed_counts().await.unwrap();
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

/// The asynchrony the pipeline is built on, stated as a test. `202` means the
/// record is durably in the topic and nothing more: it is not in the index, and
/// no read can conjure it there. The worker is what puts it there, and once it
/// has, the read answers out of ClickHouse and only ClickHouse — there is no
/// buffer left to merge, which is what two rounds of lost records paid for.
#[tokio::test]
async fn a_record_reaches_the_index_when_the_worker_packs_it_and_not_before() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let config = test_config(&data_dir, None);
    let app = api::router(state.clone());
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    let response = app
        .clone()
        .oneshot(ingest_request(&source, "queued-1", raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let accepted = json_body(response).await;
    assert_eq!(accepted["status"], "accepted");

    let page = search_by_source(&app, &source).await;
    assert!(
        page["items"].as_array().unwrap().is_empty(),
        "an unpacked record was answered as indexed"
    );
    assert_eq!(indexed_rows(&config, &source).await, 0);

    index_pending(&state).await;

    let page = search_by_source(&app, &source).await;
    assert_eq!(
        page["items"].as_array().unwrap().len(),
        1,
        "a packed record never reached the index"
    );
    assert_eq!(page["items"][0]["id"], accepted["id"]);
    assert_eq!(
        indexed_rows(&config, &source).await,
        1,
        "the read answered from somewhere other than the index"
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

async fn search_by_source(app: &Router, source: &str) -> Value {
    let response = app
        .clone()
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
    json_body(response).await
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

/// Distinct rows in the index for one source, straight from ClickHouse, so that
/// a test can tell an indexed record from one the API merely accepted.
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
    let app = api::router(state.clone());
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    for key in ["page-1", "page-2", "page-3"] {
        let response = app
            .clone()
            .oneshot(ingest_request(&source, key, raw))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    index_pending(&state).await;

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
        catalog.insert_batch(&[record]).await.unwrap();
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
    catalog.insert_batch(&[duplicated.clone()]).await.unwrap();
    // The state a replay leaves behind: the index holds the record, the batch
    // arrives again unchanged, and a third record is dated between the two
    // copies.
    let mut between = sample_record(&source, 400);
    between.received_at = millisecond + TimeDelta::microseconds(400);
    catalog.insert_batch(&[duplicated, between]).await.unwrap();

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
        catalog.insert_batch(&[record]).await.unwrap();
    }
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
    catalog.insert_batch(&[record]).await.unwrap();

    let filter = RecordFilter {
        source: Some(source),
        received_from: Some(bound),
        ..RecordFilter::default()
    };
    let (page, _) = catalog.search(&filter, None, 10).await.unwrap();
    // One, not zero: the record is stored at the bottom of its millisecond, so
    // flooring an inclusive `from` bound to the same millisecond is the only
    // answer that does not drop it.
    assert_eq!(page.len(), 1, "a floored bound dropped its own record");
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
    catalog.insert_batch(&[record]).await.unwrap();

    let filter = RecordFilter {
        source: Some(source.clone()),
        received_to: Some(millisecond + TimeDelta::microseconds(400)),
        ..RecordFilter::default()
    };
    let (covered, _) = catalog.search(&filter, None, 10).await.unwrap();
    assert_eq!(
        covered.len(),
        1,
        "an exclusive upper bound dropped a record that arrived before it"
    );

    // The bound still excludes: a record in the NEXT millisecond is out of range.
    let mut later = sample_record(&source, 402);
    later.received_at = millisecond + TimeDelta::milliseconds(1);
    catalog.insert_batch(&[later]).await.unwrap();
    let (page, _) = catalog.search(&filter, None, 10).await.unwrap();
    assert_eq!(
        page.len(),
        1,
        "rounding the bound outwards pulled in a later millisecond"
    );
}

/// docs/architecture.md lists `rule` among the filter fields, and choosing a
/// composite token for it is only worth anything if a page can be narrowed to
/// one. Three records under this test's own source, of which exactly one is the
/// mode being asked for; the source is part of every filter here because the
/// index is shared and durable, so a page filtered on the rule alone would also
/// answer with whatever every other run has left behind.
#[tokio::test]
async fn filters_a_page_down_to_one_rule() {
    let data_dir = test_data_dir();
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);
    let catalog = Catalog::connect(&config).await.unwrap();
    for (index, rule) in [Some("3p-jade-south"), Some("4p-throne-east"), None]
        .into_iter()
        .enumerate()
    {
        let mut record = sample_record(&source, index as u32);
        record.rule = rule.map(str::to_owned);
        catalog.insert_batch(&[record]).await.unwrap();
    }

    let filter = RecordFilter {
        source: Some(source.clone()),
        rule: Some("3p-jade-south".into()),
        ..RecordFilter::default()
    };
    let (page, _) = catalog.search(&filter, None, 10).await.unwrap();
    assert_eq!(page.len(), 1, "the rule filter did not narrow the page");
    assert_eq!(page[0].rule.as_deref(), Some("3p-jade-south"));

    // The console picks the value from a fixed list, but the API takes it from a
    // query string, so the statement has to survive a quote. An interpolated
    // rule would end the string literal here and either fail the query or match
    // every row this source has.
    let injected = RecordFilter {
        rule: Some("' OR 1 = 1 --".into()),
        ..filter
    };
    let (page, _) = catalog.search(&injected, None, 10).await.unwrap();
    assert!(page.is_empty(), "a quoted rule changed the statement");
}

/// Rows left after ReplacingMergeTree has collapsed the duplicates a re-insert
/// created. `indexed_rows` counts distinct ids and would answer 1 whether the
/// backfill replaced a row or added a second one beside it under a different
/// sorting key; this tells those two apart, and they are the difference between
/// a rewritten index and one that has silently doubled.
async fn collapsed_rows(config: &Config, source: &str) -> u64 {
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
            "SELECT count() AS rows FROM mjai.records FINAL WHERE source = {source:String}",
            &[("source", source.to_owned())],
        )
        .await
        .unwrap();
    rows.first().map(|row| row.rows).unwrap_or_default()
}

/// The 18,633 records collected before the parser learned to read the Majsoul
/// header carry no rule and a `played_at` at the midnight of the day in the
/// game's uuid, and fixing the parser only reaches what comes after it. This is
/// one such row, rewritten from its own bytes.
///
/// It walks the whole index rather than this test's source, because that is what
/// the pass does, so it is one of the slower cases in the suite; every row it
/// meets from another test points at a pack outside this data directory and is
/// skipped, which is the same path an unreadable pack takes in production — and
/// which is why the assertion at the end is that the pass did *not* mark itself
/// done.
#[tokio::test]
async fn rewrites_the_metadata_of_a_row_indexed_before_the_parser_fix() {
    let (state, data_dir) = test_state().await;
    let config = test_config(&data_dir, None);
    let source = test_source(&data_dir);

    let mut raw = Vec::new();
    GzDecoder::new(
        include_bytes!("fixtures/260716-00000000-0000-4000-8000-000000000004.mjson").as_slice(),
    )
    .read_to_end(&mut raw)
    .unwrap();
    let id = Uuid::new_v4();
    let mut writer = state.packs.legacy_writer().unwrap();
    let storage = writer.append(id, &raw).unwrap();

    // Milliseconds, because that is the resolution the index stores and the
    // assertion below compares a rewritten row against the value read here.
    let received_at = Utc::now().trunc_subsecs(3);
    let stale = mjai_management::catalog::Record {
        id,
        source: source.clone(),
        sha256: "0".repeat(64),
        received_at,
        played_at: Some("2026-07-16T00:00:00Z".parse().unwrap()),
        players: vec!["p0".into(), "p1".into(), "p2".into(), "p3".into()],
        rule: None,
        event_count: 300,
        storage,
    };
    state.catalog.insert_batch(&[stale]).await.unwrap();

    // The marker lives in the shared test database, so without this a second run
    // of the suite would find the backfill already done and assert nothing.
    sqlx::query("DELETE FROM completed_backfills WHERE name = $1")
        .bind(mjai_management::backfill::NAME)
        .execute(state.catalog.postgres())
        .await
        .unwrap();
    mjai_management::backfill::rewrite_record_metadata(state.clone()).await;

    let fixed = state.catalog.get(id).await.unwrap().unwrap();
    // The fixture's own header: 4 players in the throne room over a south game,
    // started at majsoul.start_time 1784207242.
    assert_eq!(fixed.rule.as_deref(), Some("4p-throne-south"));
    assert_eq!(
        fixed.played_at,
        Some("2026-07-16T13:07:22Z".parse().unwrap())
    );
    // Every column of the sorting key, back unchanged. A rewrite that moved one
    // of them would still answer the two assertions above, because `get` reads
    // the newest row for an id whatever its key.
    assert_eq!(fixed.received_at, received_at, "the rewrite moved a row");
    assert_eq!(fixed.source, source);
    assert_eq!(fixed.id, id);
    assert_eq!(
        collapsed_rows(&config, &source).await,
        1,
        "the rewritten row landed beside the old one instead of over it"
    );

    // And the marker is deliberately withheld. The shared index carries rows
    // from every other case in the suite, whose packs sit in data directories
    // this process cannot reach, so this pass necessarily meets records it
    // cannot read — the same shape as an object store that will not serve, and
    // the case the one-shot marker must not be spent on. The rewrite above still
    // happened, which is the point: the pass does what it can and simply has not
    // earned the right to declare the corpus done. When the marker is written is
    // pinned by `backfill`'s own unit tests, where nothing else is in the index
    // to make the answer depend on what the rest of the suite left behind.
    let marked = sqlx::query("SELECT 1 FROM completed_backfills WHERE name = $1")
        .bind(mjai_management::backfill::NAME)
        .fetch_optional(state.catalog.postgres())
        .await
        .unwrap();
    assert!(
        marked.is_none(),
        "a pass that could not read every row must not mark itself done"
    );
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

/// The console's overview. Everything but the watch block is an aggregate over
/// the index and the job table the whole suite shares, so the exact assertions
/// are the ones a shared store can carry — this run's own source in the
/// breakdown, and floors on the totals it contributed to. The watch counters
/// are a per-process registry, so those are exact.
#[tokio::test]
async fn reports_index_storage_download_and_watch_totals() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let raw = r#"{"type":"start_game","names":["a","b","c","d"]}"#;
    for key in ["stats-1", "stats-2"] {
        let response = app
            .clone()
            .oneshot(ingest_request(&source, key, raw))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    // The aggregates read the index, so an accepted record counts for nothing
    // until the worker has packed it.
    index_pending(&state).await;
    state
        .watch
        .apply(WatchEvent {
            uuid: "260727-abcdef01-2345-6789-abcd-ef0123456789".into(),
            event: WatchEventKind::Live,
            mode_id: Some(16),
            started_at: None,
            message: None,
            record_id: None,
        })
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/stats")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let stats = json_body(response).await;

    let mine = stats["records"]["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["source"] == source.as_str())
        .unwrap_or_else(|| panic!("the source breakdown never mentioned {source}: {stats}"));
    assert_eq!(mine["records"], 2);
    assert!(stats["records"]["total"].as_u64().unwrap() >= 2, "{stats}");
    assert!(
        stats["records"]["last_24h"].as_u64().unwrap() >= 2,
        "{stats}"
    );
    assert!(stats["storage"]["packs"].as_u64().unwrap() >= 1, "{stats}");
    assert!(
        stats["storage"]["raw_bytes"].as_u64().unwrap() >= 2 * raw.len() as u64,
        "{stats}"
    );
    assert!(
        stats["storage"]["compressed_bytes"].as_u64().unwrap() >= 1,
        "{stats}"
    );
    assert!(stats["downloads"]["queued"].is_u64(), "{stats}");
    assert_eq!(stats["watch"]["phase"], "stopped");
    assert_eq!(stats["watch"]["live"], 1);
    assert_eq!(stats["watch"]["completed"], 0);
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The export page's list. The job table is shared too, so this asserts its own
/// job is in the page and that the page is ordered newest first, not how long
/// the page is.
#[tokio::test]
async fn lists_the_newest_download_jobs() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"filter":{{"source":"{source}"}},"format":"manifest.jsonl"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let created = json_body(response).await;
    let job_id = created["id"].as_str().unwrap().to_owned();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/downloads?limit=1000")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page = json_body(response).await;
    let items = page["items"].as_array().unwrap();
    let mine = items
        .iter()
        .find(|item| item["id"] == job_id.as_str())
        .unwrap_or_else(|| panic!("the job list never mentioned {job_id}"));
    // The same shape `GET /api/v1/downloads/{id}` answers with, because it is
    // the same type.
    assert!(mine["state"].is_string(), "{mine}");
    assert!(mine["record_count"].is_u64(), "{mine}");
    // Parsed rather than compared as strings: chrono drops a whole second's
    // empty fraction, so two RFC 3339 timestamps do not always sort in their
    // own order lexicographically and the check would fail once in a million
    // runs for the wrong reason.
    let created_at: Vec<chrono::DateTime<Utc>> = items
        .iter()
        .map(|item| item["created_at"].as_str().unwrap().parse().unwrap())
        .collect();
    let mut newest_first = created_at.clone();
    newest_first.sort_unstable_by(|left, right| right.cmp(left));
    assert_eq!(created_at, newest_first, "the page was not newest first");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/downloads?limit=0")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// A configuration is saved whole, so an edit built on a revision that has moved
/// on does not merge — it replaces, deleting whatever was added since. A second
/// console tab, or one left open across a deploy, would take a collector and its
/// state key with it, and the queue named by that key would be orphaned while
/// both tabs reported success.
#[tokio::test]
async fn refuses_a_configuration_edited_against_a_revision_that_moved_on() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state.clone());
    let stale = state.watch_service.config();
    let body = serde_json::to_string(&stale).unwrap();

    let accepted = app
        .clone()
        .oneshot(watch_config_request(&body))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(json_body(accepted).await["revision"], stale.revision + 1);

    // Byte for byte what the first save sent, which is what a tab that has not
    // re-read holds. 412 rather than 409: the edit has to be rebuilt on the
    // current document, not retried as it stands.
    let refused = app.oneshot(watch_config_request(&body)).await.unwrap();
    assert_eq!(refused.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        state.watch_service.config().revision,
        stale.revision + 1,
        "a refused save must not have moved the revision"
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

fn watch_config_request(body: &str) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri("/api/v1/watch/config")
        .header(header::AUTHORIZATION, "Bearer test-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
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
