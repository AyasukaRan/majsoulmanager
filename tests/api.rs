use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode, header},
    routing::post,
};
use chrono::{DateTime, NaiveDate, SecondsFormat, SubsecRound, TimeDelta, Utc};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use mjai_management::auth::{
    AuthError, AuthSettings, CreateUserRequest, LoginRequest, RegisterRequest, UserRole,
    VerifyEmailRequest,
};
use mjai_management::catalog::{
    Catalog, Cursor, GameUuid, PaipuyaGame, RecordFilter, SeriesUnit, SeriesWindow, SweepPosition,
};
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
        // A topic per test, because the drain is the one thing they cannot
        // share. `index_pending` runs the pack worker until the topic reaches
        // the end of its log, and with forty-four tests producing into one topic
        // that end keeps moving: a drain chases records another test is still
        // writing, while every other drain does the same. Nothing errors and
        // nothing finishes — which is exactly what the job that timed out after
        // eighteen minutes with not one of the forty-four results printed looks
        // like (#115). They also shared the committed offset, keyed by topic and
        // partition, so they could push each other backwards through it.
        //
        // Left as topics on the broker afterwards, which is free on the CI
        // container and untidy against a long-lived local one. A `MJAI_KAFKA_TOPIC`
        // in the environment still wins, for pinning one deliberately.
        kafka_topic: env_or("MJAI_KAFKA_TOPIC", &format!("mjai.test.{}", Uuid::new_v4())),
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
/// Partition 0 is the whole topic at the default partition count, and the topic
/// belongs to this test alone — see `test_config`. It used to be shared, so a
/// worker started by one test indexed records another test produced and the two
/// read each other's committed offset. The index and the bucket are still
/// shared, which is what `test_source` is for.
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

/// An authenticated GET that asserts the status before the caller looks at the
/// body, so a route that answered `404` or `500` fails on the status line
/// rather than several frames later on whatever the error body was not.
async fn ok(app: &Router, uri: &str) -> axum::response::Response {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
    response
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

/// The live collector's path end to end: the protobuf it converted from
/// survives the broker header, the pack and the three index columns, and comes
/// back byte for byte.
///
/// Driven through `indexer::ingest_one` rather than the HTTP route because that
/// is the collector's own entry point and the only one that ever has a
/// protobuf — an upload is already mjai and whatever it was converted from
/// happened somewhere this process cannot see.
///
/// The bytes deliberately cover the whole `u8` range, including NUL and
/// sequences that are not valid UTF-8: a protobuf is not text, and every hop
/// here has at some point been a place where something helpfully treated it as
/// if it were.
#[tokio::test]
async fn keeps_the_protobuf_the_collector_converted_from() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let raw = br#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;
    let pb: Vec<u8> = (0..4096).map(|byte| (byte % 256) as u8).collect();

    let with = mjai_management::indexer::ingest_one(
        &state.catalog,
        &state.kafka,
        &source,
        "pb-game",
        None,
        raw,
        Some(&pb),
    )
    .await
    .unwrap();
    let without = mjai_management::indexer::ingest_one(
        &state.catalog,
        &state.kafka,
        &source,
        "no-pb-game",
        None,
        br#"{"type":"start_game","names":["w","x","y","z"],"rule":"tonpu"}"#,
        None,
    )
    .await
    .unwrap();
    index_pending(&state).await;

    let stored = ok(&app, &format!("/api/v1/records/{}/majsoul-pb", with.id)).await;
    assert_eq!(
        to_bytes(stored.into_body(), usize::MAX).await.unwrap(),
        pb,
        "the protobuf did not come back byte for byte"
    );

    // The record's own frame is untouched by the one sitting next to it: two
    // offsets into one pack, and a read of either resolves to the right half.
    let mjai = ok(&app, &format!("/api/v1/records/{}/raw", with.id)).await;
    assert_eq!(
        to_bytes(mjai.into_body(), usize::MAX).await.unwrap(),
        raw.as_slice()
    );

    // A record that never had one answers `404` rather than an empty body, so a
    // caller can tell "no protobuf was kept" from "here is a zero-byte one".
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/records/{}/majsoul-pb", without.id))
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The statistics pipeline end to end: a game is scored while it is packed, the
/// seat rows reach ClickHouse, and both the search box and the summary read
/// them back.
///
/// The players are named after this test's own source so that the shared index
/// every test writes into cannot make one run's counters depend on another's.
#[tokio::test]
async fn scores_a_game_into_per_player_counters() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let names: Vec<String> = (0..4).map(|seat| format!("{source}-p{seat}")).collect();
    let raw = format!(
        r#"{{"type":"start_game","names":{names},"rule":"tonpu"}}
{{"type":"start_kyoku","bakaze":"E","kyoku":1,"oya":0,"honba":0,"kyotaku":0,"scores":[25000,25000,25000,25000],"tehais":[[],[],[],[]]}}
{{"type":"tsumo","actor":1,"pai":"1m"}}
{{"type":"reach","actor":1}}
{{"type":"dahai","actor":1,"pai":"1m","tsumogiri":true}}
{{"type":"reach_accepted","actor":1}}
{{"type":"tsumo","actor":3,"pai":"2m"}}
{{"type":"dahai","actor":3,"pai":"2m","tsumogiri":true}}
{{"type":"hora","actor":1,"target":3,"pai":"2m","fan":4,"yakuman":false,"yaku_ids":[1,30],"deltas":[0,9000,0,-8000],"scores":[25000,33000,25000,17000]}}
{{"type":"end_kyoku"}}
{{"type":"end_game","scores":[25000,33000,25000,17000],"majsoul_result":[{{"seat":0,"total_point":-5000,"grading_score":-10}},{{"seat":1,"total_point":25000,"grading_score":75}},{{"seat":2,"total_point":-5000,"grading_score":-10}},{{"seat":3,"total_point":-15000,"grading_score":-55}}]}}"#,
        names = serde_json::to_string(&names).unwrap()
    );

    let response = app
        .clone()
        .oneshot(ingest_request(&source, "stats-game", &raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    index_pending(&state).await;

    let hits = json_body(ok(&app, &format!("/api/v1/players?q={source}")).await).await;
    let found: Vec<&str> = hits["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["player"].as_str().unwrap())
        .collect();
    assert_eq!(found.len(), 4, "every named seat is searchable: {found:?}");
    assert!(found.contains(&names[1].as_str()));

    // `player_summary` counts without FINAL, deliberately — the doc comment on
    // it says a replayed insert converges through ReplacingMergeTree and a
    // counter can read high inside the merge window, because the alternative is
    // a full merge on every page load. `index_pending` settles the topic, which
    // is a different question from whether ClickHouse has merged the parts, so
    // asserting an exact 1 against a live table was betting on the very thing
    // the query says it does not promise. Merged here instead of loosened
    // there: `>= 1` would stop catching a game scored as none.
    merge_player_games(&state.config).await;

    let winner = json_body(
        ok(
            &app,
            &format!("/api/v1/players/stats?player={}&span=365", names[1]),
        )
        .await,
    )
    .await;
    assert_eq!(winner["games"], 1);
    assert_eq!(
        winner["detailed_games"], 1,
        "the record carries a yaku list"
    );
    assert_eq!(winner["hands"], 1);
    assert_eq!(winner["wins"], 1);
    assert_eq!(winner["wins_tsumo"], 0);
    assert_eq!(winner["riichi"], 1);
    assert_eq!(winner["riichi_wins"], 1);
    assert_eq!(winner["riichi_first"], 1);
    assert_eq!(winner["riichi_ippatsu"], 1, "yaku id 30 is 一発");
    // The delta credits the winner with the stick it swept back up; the hand
    // was worth 8000, not 9000.
    assert_eq!(winner["win_points"], 8_000);
    assert_eq!(winner["win_turns"], 1);
    assert_eq!(winner["placements"][0], 1, "first place, once");
    assert_eq!(winner["settled_point"], 25_000);
    assert_eq!(winner["grading_score"], 75);

    let dealt_in = json_body(
        ok(
            &app,
            &format!("/api/v1/players/stats?player={}&span=365", names[3]),
        )
        .await,
    )
    .await;
    assert_eq!(dealt_in["deal_ins"], 1);
    assert_eq!(dealt_in["deal_in_points"], 8_000);
    assert_eq!(dealt_in["wins"], 0);
    assert_eq!(dealt_in["placements"][3], 1, "last place, once");

    // A mode filter reaches this table the same way it reaches the trends, and
    // this record's rule is not one of the twelve, so every facet excludes it.
    let filtered = json_body(
        ok(
            &app,
            &format!(
                "/api/v1/players/stats?player={}&span=365&room=jade",
                names[1]
            ),
        )
        .await,
    )
    .await;
    assert_eq!(filtered["games"], 0);

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The re-conversion pass walks exactly the records it can convert.
///
/// It rebuilds a record's mjai from the protobuf that was stored beside it,
/// which only 245k of 1.9M records have — the rest arrived as mjai or predate
/// the protobuf being kept, and their only repair is a re-fetch. A filter that
/// let those through would hand the pass 1.6M records with nothing to convert,
/// and its whole output would be a failure count.
#[tokio::test]
async fn the_reconversion_walk_sees_only_records_that_kept_their_protobuf() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let mjai = r#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;

    let mut with_pb = None;
    for uuid in ["kept-its-pb", "never-had-one"] {
        let accepted = mjai_management::indexer::ingest_one(
            &state.catalog,
            &state.kafka,
            &source,
            uuid,
            None,
            mjai.as_bytes(),
            None,
        )
        .await
        .unwrap();
        index_pending(&state).await;
        if uuid == "kept-its-pb" {
            let stored = state.catalog.get(accepted.id).await.unwrap().unwrap();
            mjai_management::indexer::reindex_one(
                &state.kafka,
                &stored,
                mjai.as_bytes().to_vec(),
                Some(b"a protobuf".to_vec()),
            )
            .await
            .unwrap();
            index_pending(&state).await;
            with_pb = Some(accepted.id);
        }
    }

    let walk = |stored_pb: bool| mjai_management::catalog::RecordFilter {
        source: Some(source.clone()),
        stored_pb,
        ..Default::default()
    };
    let (page, _) = state.catalog.scan(&walk(true), None, 100).await.unwrap();
    assert_eq!(
        page.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![with_pb.expect("the record that kept its protobuf")],
        "the walk picked up a record it has nothing to convert"
    );
    assert!(page[0].majsoul_pb.is_some());

    // And the two halves partition the corpus: what this pass cannot repair is
    // exactly what the re-fetch pool goes and fetches.
    let (missing, _) = state
        .catalog
        .scan(
            &mjai_management::catalog::RecordFilter {
                source: Some(source.clone()),
                missing_pb: true,
                ..Default::default()
            },
            None,
            100,
        )
        .await
        .unwrap();
    assert_eq!(missing.len(), 1);
    assert_ne!(missing[0].id, page[0].id);
}

/// Replacing a record's bytes keeps it one record.
///
/// This is the property the whole re-fetch backfill rests on: a re-fetched game
/// is converted again and written back under the identity it already has, and
/// `received_at` is the partition key and the head of the sorting key — so a
/// message carrying a fresh timestamp, or a different source, would leave two
/// rows for one game rather than one better row. Both would then be returned by
/// every filter that matched, and counted twice in every aggregate.
#[tokio::test]
async fn re_indexing_a_record_replaces_it_rather_than_adding_a_second() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let before = r#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}"#;
    // What a re-conversion produces: the same game with the fields the fixed
    // converter adds, and a protobuf beside it that the original never had.
    let after = r#"{"type":"start_game","names":["a","b","c","d"],"rule":"tonpu"}
{"type":"start_kyoku","bakaze":"E","kyoku":1}
{"type":"end_game","scores":[25000,25000,25000,25000]}"#;
    let pb = b"the original this was converted from".to_vec();

    let accepted = mjai_management::indexer::ingest_one(
        &state.catalog,
        &state.kafka,
        &source,
        "reindex-game",
        None,
        before.as_bytes(),
        None,
    )
    .await
    .unwrap();
    index_pending(&state).await;

    let stored = state.catalog.get(accepted.id).await.unwrap().unwrap();
    assert!(stored.majsoul_pb.is_none());

    mjai_management::indexer::reindex_one(
        &state.kafka,
        &stored,
        after.as_bytes().to_vec(),
        Some(pb.clone()),
    )
    .await
    .unwrap();
    index_pending(&state).await;

    // One row, not two, and it is the new bytes.
    assert_eq!(
        indexed_rows(&state.config, &source).await,
        1,
        "the re-index wrote a second row instead of replacing the first"
    );
    let replaced = state.catalog.get(accepted.id).await.unwrap().unwrap();
    assert_eq!(replaced.id, stored.id);
    assert_eq!(
        replaced.received_at, stored.received_at,
        "received_at is the sorting key; a re-index must not move it"
    );
    assert_eq!(replaced.source, stored.source);
    assert_eq!(replaced.event_count, 3);
    assert_ne!(
        replaced.storage.pack_key, stored.storage.pack_key,
        "the replacement is in a new pack, which is why it needs no second key"
    );
    assert!(replaced.majsoul_pb.is_some(), "the protobuf came with it");

    let raw = ok(&app, &format!("/api/v1/records/{}/raw", accepted.id)).await;
    assert_eq!(
        to_bytes(raw.into_body(), usize::MAX).await.unwrap(),
        after.as_bytes()
    );
    let original = ok(&app, &format!("/api/v1/records/{}/majsoul-pb", accepted.id)).await;
    assert_eq!(
        to_bytes(original.into_body(), usize::MAX).await.unwrap(),
        pb
    );

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
            None,
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

/// The whole point of splitting the scopes. A caller-supplied key is a retry
/// guard and expires with the retry window; a key derived from the game is the
/// only record anywhere that the game has been stored, because the uuid is not a
/// column of the index. Pruning that one does not free a guard, it discards the
/// answer, and the next import of an archive holding the game stores it again
/// under a record id nothing can collapse onto the original.
#[tokio::test]
async fn the_prune_keeps_game_claims_and_drops_caller_claims() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let game = format!("260716-{}", Uuid::new_v4());
    let caller_key = format!("prune-probe-{}", Uuid::new_v4());
    let with_a_game = format!(
        r#"{{"type":"start_game","names":["a","b","c","d"],"majsoul":{{"uuid":"{game}","start_time":1784178000}}}}"#
    );

    for request in [
        ingest_request(&source, &game, &with_a_game),
        ingest_request(
            &source,
            &caller_key,
            r#"{"type":"start_game","names":["e","f","g","h"]}"#,
        ),
    ] {
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    // Both claims older than the retention window, which is the state a boot a
    // month from now would meet. Only these two rows: the suite shares this
    // table and backdating all of it would prune other cases out from under
    // themselves.
    let game_scope = format!("majsoul-watch\0{game}");
    let caller_scope = format!("{source}\0{caller_key}");
    sqlx::query(
        "UPDATE ingest_idempotency SET created_at = now() - interval '31 days'
         WHERE key_hash IN (sha256($1), sha256($2))",
    )
    .bind(game_scope.as_bytes())
    .bind(caller_scope.as_bytes())
    .execute(state.catalog.postgres())
    .await
    .unwrap();

    // Connecting prunes, which is the only way this runs.
    Catalog::connect(&test_config(&data_dir, None))
        .await
        .unwrap();

    let survived = |key: String| {
        let pool = state.catalog.postgres().clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM ingest_idempotency WHERE key_hash = sha256($1)",
            )
            .bind(key.into_bytes())
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert_eq!(
        survived(game_scope).await,
        1,
        "the claim naming a game must outlive the retry window; it is the only \
         thing that knows the game was ever stored"
    );
    assert_eq!(
        survived(caller_scope).await,
        0,
        "a caller-supplied key is a retry guard and must still be pruned, or the \
         table grows without bound for keys nothing can re-derive"
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// Ingest cannot reach backwards. Records indexed before the game scope existed
/// carry no claim under it, and the ones the collector did leave under that key
/// were written as expiring — so without this pass the collected corpus loses
/// its identity thirty days after it was gathered and quietly begins accepting
/// itself as new.
#[tokio::test]
async fn the_backfill_makes_an_already_indexed_game_permanent() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    let game = format!("260716-{}", Uuid::new_v4());
    let raw = format!(
        r#"{{"type":"start_game","names":["a","b","c","d"],"majsoul":{{"uuid":"{game}","start_time":1784178000}}}}"#
    );
    let response = app
        .oneshot(ingest_request(&source, &game, &raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    index_pending(&state).await;

    let key = format!("majsoul-watch\0{game}");
    // The state every claim written before this change is in.
    sqlx::query("UPDATE ingest_idempotency SET expires = true WHERE key_hash = sha256($1)")
        .bind(key.as_bytes())
        .execute(state.catalog.postgres())
        .await
        .unwrap();
    // The marker lives in the shared database, so without this a second run of
    // the suite would find the pass done and assert nothing.
    sqlx::query("DELETE FROM completed_backfills WHERE name = $1")
        .bind(mjai_management::backfill::GAME_CLAIMS_NAME)
        .execute(state.catalog.postgres())
        .await
        .unwrap();

    mjai_management::backfill::write_game_scoped_claims(state.clone()).await;

    let expires: bool =
        sqlx::query_scalar("SELECT expires FROM ingest_idempotency WHERE key_hash = sha256($1)")
            .bind(key.as_bytes())
            .fetch_one(state.catalog.postgres())
            .await
            .unwrap();
    assert!(
        !expires,
        "the pass has to upgrade the claim it finds, not skip it: ON CONFLICT DO \
         NOTHING here would leave the whole collected corpus due for deletion"
    );
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
    second.unwrap().indexed_pack_keys().await.unwrap();
    first.indexed_pack_keys().await.unwrap();
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
        majsoul_pb: None,
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
/// Collapses `player_games` so a per-player counter can be asserted exactly.
///
/// The product counts these without FINAL on purpose, so a row that landed in
/// two unmerged parts reads twice until the background merge catches up. A test
/// that wants an exact number has to ask for the merge; the alternative is an
/// assertion that passes on a value the query never promised.
async fn merge_player_games(config: &Config) {
    mjai_management::clickhouse::ClickHouse::new(
        &config.clickhouse_url,
        &config.clickhouse_user,
        &config.clickhouse_password,
    )
    .unwrap()
    .execute("OPTIMIZE TABLE mjai.player_games FINAL", &[], String::new())
    .await
    .unwrap();
}

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
        majsoul_pb: None,
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

/// The trends page's buckets. The index is shared with the rest of the suite
/// *and* with every earlier run of it, so nothing here may assert an absolute
/// count: another test can move any bucket, and because `series` counts without
/// FINAL, a ReplacingMergeTree merge landing between two reads can move one
/// *downwards* — a record another test wrote twice over one sorting key
/// collapses to one row with no warning. The current bucket therefore carries
/// no delta at all; it is the one every other test writes into.
///
/// The claim that matters is that the two series read different columns, and it
/// is made on a day nothing else can reach: a record ingested now, played 300
/// days ago. Nothing in the suite is received that long ago and no fixture is
/// played then, so that bucket holds this test's rows and no others — which is
/// what lets `games` be asserted to move and `records` to stay exactly put.
/// `received_at` is stamped by the server at ingest and cannot land 300 days
/// back, so a `games` reading it could not move that day at all.
#[tokio::test]
async fn buckets_ingest_by_arrival_and_games_by_when_they_were_played() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    // One clock read for the whole test. The server derives its window from its
    // own `Utc::now()`, so a second read here could disagree by a bucket about
    // where the window ends, which is a boundary crossing rather than a defect.
    let now = Utc::now();
    let played_at = now - TimeDelta::days(300);
    let played_day = played_at.date_naive().to_string();

    let fetch = |query: &str| {
        let app = app.clone();
        let uri = format!("/api/v1/stats/series{query}");
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .header(header::AUTHORIZATION, "Bearer test-secret")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            json_body(response).await
        }
    };
    let points = |series: &Value| series["points"].as_array().unwrap().clone();
    let on = |series: &Value, at: &str, field: &str| {
        series["points"]
            .as_array()
            .unwrap()
            .iter()
            .find(|point| point["at"] == at)
            .unwrap_or_else(|| panic!("{at} is not in the window: {series}"))[field]
            .as_u64()
            .unwrap()
    };

    // 365 rather than 7, because the probe below is played 300 days back and
    // has to be inside the window that carries it.
    let before = fetch("?unit=day&span=365").await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/records")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .header("idempotency-key", "series-1")
                .header("x-mjai-source", &source)
                .header("x-mjai-played-at", played_at.to_rfc3339())
                .body(Body::from(
                    r#"{"type":"start_game","names":["a","b","c","d"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    // The buckets read the index, so an accepted record counts for nothing
    // until the worker has packed it.
    index_pending(&state).await;
    let after = fetch("?unit=day&span=365").await;

    assert!(
        on(&after, &played_day, "games") > on(&before, &played_day, "games"),
        "the game did not reach {played_day}: {before} -> {after}"
    );
    // Exactly equal, not merely "did not grow much": this is the assertion that
    // `records` is not quietly the same query as `games`. It survives the shared
    // index only because the bucket is 300 days old — nothing was received then,
    // so there is nothing there to insert, duplicate or collapse.
    assert_eq!(
        on(&after, &played_day, "records"),
        on(&before, &played_day, "records"),
        "arrival was counted under the day the game was played: {before} -> {after}"
    );

    let daily = fetch("?unit=day&span=7").await;
    assert_eq!(daily["unit"], "day", "{daily}");
    let shape = points(&daily);
    assert_eq!(shape.len(), 7, "{daily}");
    // Read back out of the response rather than recomputed from the clock: a run
    // that straddles midnight UTC would otherwise fail on a one-day disagreement
    // that is not a defect. It still has to be a day this test recognises.
    let last = shape[6]["at"].as_str().unwrap();
    let last_day = NaiveDate::parse_from_str(last, "%Y-%m-%d").unwrap();
    assert!(
        last_day == now.date_naive() || last_day == now.date_naive() + TimeDelta::days(1),
        "the window ends on neither today nor the day after: {daily}"
    );
    // Gap-filled and consecutive, oldest first: the console draws these straight
    // onto an axis and cannot tell a missing entry from a quiet bucket.
    for (offset, point) in shape.iter().enumerate() {
        assert_eq!(
            point["at"],
            (last_day - TimeDelta::days(6 - offset as i64)).to_string(),
            "{daily}"
        );
        for field in ["records", "games", "raw_bytes", "compressed_bytes"] {
            assert!(point[field].is_u64(), "{field} missing from {point}");
        }
    }

    // The hourly half. A day of daily buckets is one bar, which is the whole
    // reason this granularity exists, so what is pinned here is that an hour is
    // an hour: RFC 3339 with a zero minute, 24 of them, consecutive.
    let hourly = fetch("?unit=hour&span=24").await;
    assert_eq!(hourly["unit"], "hour", "{hourly}");
    let hours = points(&hourly);
    assert_eq!(hours.len(), 24, "{hourly}");
    let first_hour = DateTime::parse_from_rfc3339(hours[0]["at"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    for (offset, point) in hours.iter().enumerate() {
        assert_eq!(
            point["at"],
            (first_hour + TimeDelta::hours(offset as i64))
                .format("%Y-%m-%dT%H:00:00Z")
                .to_string(),
            "{hourly}"
        );
    }
    // The record went in during this hour or the one before it, depending on
    // where the clock was when the worker packed it; either way it is in the
    // window and the last two buckets are the only ones it can be in.
    let tail: u64 = hours[22..]
        .iter()
        .map(|point| point["records"].as_u64().unwrap())
        .sum();
    assert!(tail >= 1, "the ingest reached no recent hour: {hourly}");

    // Which modes the window's games were played in. Bounded, ordered, and
    // never absent — the console draws bars from it without checking.
    let rules = after["rules"].as_array().unwrap();
    assert!(rules.len() <= 24, "{after}");
    assert!(
        rules
            .iter()
            .map(|rule| rule["games"].as_u64().unwrap())
            .sum::<u64>()
            >= 1,
        "the 365 day window reported no games by mode: {after}"
    );
    for pair in rules.windows(2) {
        assert!(
            pair[0]["games"].as_u64().unwrap() >= pair[1]["games"].as_u64().unwrap(),
            "the mode breakdown is not busiest-first: {after}"
        );
    }
    for rule in rules {
        assert!(rule["rule"].is_string(), "{rule}");
    }

    // Clamped rather than refused, and the length of the array is how a caller
    // sees that it was. The two granularities have their own ceilings.
    let clamped = fetch("?unit=day&span=100000").await;
    assert_eq!(points(&clamped).len(), 365, "{clamped}");
    let clamped_hours = fetch("?unit=hour&span=100000").await;
    assert_eq!(points(&clamped_hours).len(), 168, "{clamped_hours}");
    // Nothing at all is a month of days, not an error.
    let default = fetch("").await;
    assert_eq!(default["unit"], "day", "{default}");
    assert_eq!(points(&default).len(), 30, "{default}");
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The custom range. Every preset window ends with the bucket in progress; this
/// one does not, which is both the point of it and the one thing about it that
/// can be asserted without counting rows — the window is the buckets that were
/// asked for, both ends included, wherever they sit.
#[tokio::test]
async fn covers_the_range_it_was_given_rather_than_one_ending_now() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state);
    // One clock read: every bound below is derived from it, so the server
    // truncates values this test chose rather than a moving `now()`.
    let now = Utc::now();

    let fetch = |query: String| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .uri(format!("/api/v1/stats/series?{query}"))
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };
    // Says which status came back rather than letting `json_body` fail to parse
    // an error page — "expected value, line 1 column 1" names nothing.
    let ok = |query: String| async move {
        let response = fetch(query.clone()).await;
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        json_body(response).await
    };
    let points = |series: &Value| series["points"].as_array().unwrap().clone();
    // `Z`, not `+00:00`: an unencoded `+` in a query string is a space by the
    // time the extractor sees it, and the timestamp then fails to parse. The
    // console sends `toISOString()`, which is this form.
    let stamp = |at: DateTime<Utc>| at.to_rfc3339_opts(SecondsFormat::Secs, true);

    // Five calendar days, ending five days before today — nowhere near now.
    let from = now - TimeDelta::days(9);
    let to = now - TimeDelta::days(5);
    let window = ok(format!("unit=day&from={}&to={}", stamp(from), stamp(to))).await;
    let days = points(&window);
    assert_eq!(days.len(), 5, "{window}");
    assert_eq!(days[0]["at"], from.date_naive().to_string(), "{window}");
    assert_eq!(days[4]["at"], to.date_naive().to_string(), "{window}");

    // Both ends included on the hourly side too: two hours apart is three
    // buckets, not two.
    let hourly = ok(format!(
        "unit=hour&from={}&to={}",
        stamp(now - TimeDelta::hours(2)),
        stamp(now)
    ))
    .await;
    assert_eq!(points(&hourly).len(), 3, "{hourly}");

    // Too wide keeps the end and loses the start: the recent side is the side
    // somebody who asked for two years is going to read first.
    let clamped = ok(format!(
        "unit=day&from={}&to={}",
        stamp(now - TimeDelta::days(800)),
        stamp(to)
    ))
    .await;
    let wide = points(&clamped);
    assert_eq!(wide.len(), 365, "{clamped}");
    assert_eq!(
        wide[364]["at"],
        to.date_naive().to_string(),
        "the clamp dropped the end instead of the start: {clamped}"
    );

    // Refused, not quietly reinterpreted. A backwards range is a caller bug,
    // and half a range would otherwise silently become a window ending now.
    for bad in [
        format!("unit=day&from={}&to={}", stamp(to), stamp(from)),
        format!("unit=day&from={}", stamp(from)),
        format!("unit=day&to={}", stamp(to)),
    ] {
        assert_eq!(
            fetch(bad.clone()).await.status(),
            StatusCode::BAD_REQUEST,
            "{bad} was accepted"
        );
    }
    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The mode filter. Whether it works is a property of the SQL rather than of
/// any count, so what is pinned here is the shape a filter forces on the answer
/// — every token in the breakdown matching every constrained facet, an
/// unconstrained facet still spanning all of its values, and a value nobody
/// defined refused instead of quietly ignored. None of that can be moved by
/// another test's rows.
#[tokio::test]
async fn filters_the_trends_by_the_three_parts_of_a_mode() {
    let (state, data_dir) = test_state().await;
    let source = test_source(&data_dir);
    let app = api::router(state.clone());
    // A mode of this test's own, so the filtered breakdown has something in it
    // whatever else the shared index holds. `rule` is read off the start_game
    // event, which is where a converter that knows the mode puts it.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/records")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .header("idempotency-key", "modes-1")
                .header("x-mjai-source", &source)
                .header(
                    "x-mjai-played-at",
                    (Utc::now() - TimeDelta::days(2)).to_rfc3339(),
                )
                .body(Body::from(
                    r#"{"type":"start_game","names":["a","b","c"],"rule":"3p-jade-east"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    index_pending(&state).await;

    let fetch = |query: &str| {
        let app = app.clone();
        let uri = format!("/api/v1/stats/series?unit=day&span=7{query}");
        async move {
            app.oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };
    let modes = |series: &Value| {
        series["rules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|rule| rule["rule"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };

    let three = json_body(fetch("&players=3p").await).await;
    let found = modes(&three);
    assert!(
        found.iter().any(|rule| rule == "3p-jade-east"),
        "the mode this test ingested is missing: {three}"
    );
    for rule in &found {
        assert!(
            rule.starts_with("3p-"),
            "a four player mode survived players=3p: {three}"
        );
    }
    // The other side of the same claim: constraining one facet must not be
    // satisfiable by simply returning everything.
    let four = json_body(fetch("&players=4p").await).await;
    for rule in modes(&four) {
        assert!(
            rule.starts_with("4p-"),
            "a three player mode survived players=4p: {four}"
        );
    }

    // An unconstrained facet spans all of its values, so this is every room and
    // both lengths — but only three player games.
    for rule in modes(&three) {
        assert!(
            ["gold", "jade", "throne"]
                .iter()
                .any(|room| rule.contains(&format!("-{room}-"))),
            "players=3p answered with a token no room explains: {rule}"
        );
    }

    // All three at once names exactly one of the twelve.
    let one = json_body(fetch("&players=3p&room=jade&length=east").await).await;
    assert_eq!(
        modes(&one),
        vec!["3p-jade-east".to_owned()],
        "the fully constrained filter did not name one mode: {one}"
    );

    // Unfiltered, the breakdown keeps the records whose converter named no mode
    // at all; filtered, it cannot — they are neither three player nor four.
    for rule in modes(&one) {
        assert!(!rule.is_empty(), "the empty mode survived a filter: {one}");
    }

    // Refused, not ignored: a typo that answers with every mode looks exactly
    // like a filter that found nothing to exclude.
    for bad in ["&players=5p", "&room=silver", "&length=west"] {
        assert_eq!(
            fetch(bad).await.status(),
            StatusCode::BAD_REQUEST,
            "{bad} was accepted"
        );
    }
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
        .clone()
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

    // Wait for the export this test started, the way the archive test does.
    //
    // Not politeness — leaving it running hangs the whole suite. `create_download`
    // hands the export to `spawn_blocking` and drives it with `Handle::block_on`,
    // which on this test's current-thread runtime drives no IO and no timers of
    // its own. The moment this function stops awaiting, that thread loses every
    // way of making progress *and* every timeout that would have rescued it: the
    // Postgres socket, reqwest's 30s ceiling, sqlx's 5s pool acquire. Dropping the
    // runtime then waits — `BlockingPool::shutdown(None)`, no deadline — for a
    // thread that can no longer finish.
    //
    // Which is why this cost two twenty-minute CI runs before anyone saw it:
    // libtest prints "ok" only after the runtime is dropped, so every assertion
    // above passing still shows up as this test never reporting at all.
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
        if json_body(status).await["state"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

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
    let session = admin_session(&state);
    let stale = state.watch_service.config();
    let body = serde_json::to_string(&stale).unwrap();

    let accepted = app
        .clone()
        .oneshot(watch_config_request(&body, &session))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(json_body(accepted).await["revision"], stale.revision + 1);

    // Byte for byte what the first save sent, which is what a tab that has not
    // re-read holds. 412 rather than 409: the edit has to be rebuilt on the
    // current document, not retried as it stands.
    let refused = app
        .oneshot(watch_config_request(&body, &session))
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        state.watch_service.config().revision,
        stale.revision + 1,
        "a refused save must not have moved the revision"
    );
    std::fs::remove_dir_all(data_dir).unwrap();
}

fn watch_config_request(body: &str, session: &str) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri("/api/v1/watch/config")
        .header(header::AUTHORIZATION, "Bearer test-secret")
        .header("x-mjai-user-session", session)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

/// What a fresh deployment gets when it presses 开始注册.
///
/// Registration has no builtin — rustls cannot produce Chrome's ClientHello and
/// a brand new account has nothing else to be judged on — so on a deployment
/// with no registrar installed the button must say so. The failure this guards
/// against is the run starting anyway and reporting a first account that failed
/// somewhere deep in a module that was never there.
///
/// The empty batch is checked first and separately, because an operator who
/// pasted nothing needs to hear about the empty box rather than about modules.
#[tokio::test]
async fn registration_says_what_is_missing_before_it_starts_anything() {
    let (state, data_dir) = test_state().await;
    let session = admin_session(&state);
    let post = |body: &'static str| {
        api::router(state.clone()).oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/accounts/register")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-mjai-user-session", session.clone())
                .body(Body::from(body))
                .unwrap(),
        )
    };

    let empty = post(r##"{"mailboxes":["   ","# 注释"]}"##).await.unwrap();
    assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(empty).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("邮箱凭据"),
        "an empty batch should name the empty box, said: {reported}"
    );

    // No token and only half of the administrator credentials: neither way in
    // is complete, and finding that out per account would burn the run.
    let half_filled = post(
        r#"{"cloud_mail":{"base_url":"https://mail.example.com","admin_email":"admin@example.com"},"count":3}"#,
    )
    .await
    .unwrap();
    assert_eq!(half_filled.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(half_filled).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("Cloud Mail") && reported.contains("令牌"),
        "a Cloud Mail run with no way in should say what is missing, said: {reported}"
    );

    let no_count = post(r#"{"cloud_mail":{"base_url":"https://mail.example.com","token":"t"}}"#)
        .await
        .unwrap();
    assert_eq!(no_count.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(no_count).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("数量"),
        "a Cloud Mail run has no list to take its length from, so a missing count \
         has to be refused by name, said: {reported}"
    );

    let unregistered = post(r#"{"mailboxes":["someone@example.com----key"]}"#)
        .await
        .unwrap();
    assert_eq!(unregistered.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(unregistered).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("register") && reported.contains("模块"),
        "a deployment with no registrar should be told which module to install, \
         said: {reported}"
    );

    // A temp-mail key is a complete source on its own — no instance, no domain,
    // no list — so it too has to reach the module check rather than be turned
    // away for a missing mailbox.
    let temp = post(r#"{"temp_mail":{"api_key":"sk-probe"},"count":2}"#)
        .await
        .unwrap();
    assert_eq!(temp.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(temp).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("register") && reported.contains("模块"),
        "a temp-mail run should stop at the missing registrar, said: {reported}"
    );

    let no_key = post(r#"{"temp_mail":{"api_key":"  "},"count":2}"#)
        .await
        .unwrap();
    assert_eq!(no_key.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(no_key).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("API key"),
        "a temp-mail run with no key should name it, said: {reported}"
    );

    // An address and administrator credentials get as far as the module check
    // with no `mailboxes` and no domain — the domain is read off the instance,
    // and the failure mode if the two paths were ever wired the other way round
    // is a run that demands a list it was meant to replace.
    let cloud = post(
        r#"{"cloud_mail":{"base_url":"https://mail.example.com","admin_email":"admin@example.com","admin_password":"hunter2"},"count":2}"#,
    )
    .await
    .unwrap();
    assert_eq!(cloud.status(), StatusCode::BAD_REQUEST);
    let reported = json_body(cloud).await["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(
        reported.contains("register") && reported.contains("模块"),
        "a Cloud Mail run should stop at the missing registrar, not at the missing \
         mailbox list, said: {reported}"
    );

    // Nothing started, so nothing is running and no mailbox was spent. Behind
    // the session too: the status names the addresses being registered.
    let status = api::router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/accounts/register/status")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header("x-mjai-user-session", session.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["running"], false);
    assert_eq!(status["total"], 0);

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The bootstrap administrator's session. Changing what the collectors do needs
/// one: the machine key says the request came from a deployment that holds it,
/// and cannot say who is behind it.
fn admin_session(state: &AppState) -> String {
    state
        .auth
        .login(LoginRequest {
            email: "admin@example.com".into(),
            password: "test-password-123".into(),
        })
        .unwrap()
        .session_token
}

/// Every route that changes what the collectors do, so that one added to the
/// table without the guard is caught here rather than in production.
const COLLECTOR_CONTROL_ROUTES: [(&str, &str); 8] = [
    ("PUT", "/api/v1/watch/config"),
    ("POST", "/api/v1/watch/actions"),
    ("POST", "/api/v1/watch/modules"),
    ("PUT", "/api/v1/watch/proxy/subscription"),
    ("PUT", "/api/v1/watch/proxy/selection"),
    ("POST", "/api/v1/watch/proxy/actions"),
    // Registration creates credentials and spends the operator's mailboxes.
    // The machine key that every collector holds must not be able to start
    // one, and neither must an ordinary member.
    ("POST", "/api/v1/accounts/register"),
    ("POST", "/api/v1/accounts/register/stop"),
];

/// The console holds the machine key and attaches it to whatever a browser asks
/// of it, so the key proves only that a request came through the console — which
/// is true of every member's request. The settings page is rendered for
/// administrators alone, but nothing stopped a member from calling the proxy
/// paths behind it directly: rewrite which accounts collect and which rooms they
/// watch, stop collection, install a login module, repoint outbound traffic.
///
/// The body is deliberately `{}`, which no handler here would accept. A refusal
/// has to come before anything reads it, or a member learns whether their
/// payload parsed and the guard is one shape of request away from being a
/// formality.
#[tokio::test]
async fn refuses_collector_control_without_an_administrator_session() {
    let (state, data_dir) = test_state().await;
    let app = api::router(state.clone());
    let admin = admin_session(&state);
    state
        .auth
        .create_user(
            &admin,
            CreateUserRequest {
                name: "Member".into(),
                email: "member@example.com".into(),
                password: "member-password-123".into(),
                role: UserRole::Member,
            },
        )
        .unwrap();
    let member = state
        .auth
        .login(LoginRequest {
            email: "member@example.com".into(),
            password: "member-password-123".into(),
        })
        .unwrap()
        .session_token;

    for (method, uri) in COLLECTOR_CONTROL_ROUTES {
        let control = |session: Option<&str>| {
            let mut request = Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(session) = session {
                request = request.header("x-mjai-user-session", session);
            }
            request.body(Body::from("{}")).unwrap()
        };

        let anonymous = app.clone().oneshot(control(None)).await.unwrap();
        assert_eq!(
            anonymous.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} accepted the machine key alone"
        );

        let refused = app.clone().oneshot(control(Some(&member))).await.unwrap();
        assert_eq!(
            refused.status(),
            StatusCode::FORBIDDEN,
            "{method} {uri} accepted an ordinary member"
        );
    }

    // And the reads the monitoring page makes stay open to that same member,
    // which is the other half of the rule: this is not "the collectors are
    // administrator-only", it is "changing them is".
    for uri in [
        "/api/v1/watch/status",
        "/api/v1/watch/config",
        "/api/v1/watch/modules",
        "/api/v1/watch/proxy",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .header("x-mjai-user-session", &member)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri} refused a member");
    }

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
                .header("x-mjai-user-session", admin_session(&state))
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

/// The 牌谱屋 walk's ClickHouse half, against a real ClickHouse.
///
/// Everything asserted here fails silently rather than loudly, which is why it
/// is worth a container. The keyset page returns a plausible page whichever way
/// it is wrong: alias the millisecond conversion back onto `started_at` and
/// ClickHouse binds the `WHERE` to the alias, comparing milliseconds against
/// seconds, so every row passes and page two is page one — for ever, with the
/// walk reporting the catalogue swept. Drop the tuple and games sharing a second
/// are skipped. Drop the scalar and it is only slow. Nothing raises an error.
///
/// The comparison card is here for the other half of that: it referenced
/// `{from:Int64}` while binding `start`/`end`, so every click answered
/// `UNKNOWN_QUERY_PARAMETER` and no test noticed.
#[tokio::test]
async fn walks_the_paipuya_catalogue_by_keyset_and_skips_the_games_already_claimed() {
    let (state, data_dir) = test_state().await;
    let catalog = &state.catalog;
    // Its own second range, because the suite shares one ClickHouse and this
    // table has no filter but the cursor. Well past anything the other tests
    // insert, and unique per run.
    let base = DateTime::from_timestamp(
        4_000_000_000 + i64::from(Uuid::new_v4().as_fields().0 % 1_000_000) * 100,
        0,
    )
    .unwrap();
    // Numbered inside the uuid, so uuid order is index order. That is what
    // makes the duplicated game the *first* of the pair sharing a second, and
    // therefore what puts both of its copies inside one page where `dedup_by`
    // has to collapse them. With random uuids it was a coin toss, and on the
    // toss where the copies land either side of a page boundary the cursor's
    // strict tuple comparison hides the second one — so the dedup this fixture
    // exists to prove would go untested on half the runs that passed.
    let run = Uuid::new_v4();
    let uuid = |n: usize| format!("260716-{run}-{n:02}");

    // The first two share a second, which is the case a cursor of seconds alone
    // gets wrong in one direction or the other.
    let games: Vec<GameUuid> = (0..6)
        .map(|n| GameUuid {
            uuid: uuid(n),
            mode_id: 16,
            started_at: base + TimeDelta::seconds(if n == 0 { 0 } else { n as i64 - 1 }),
        })
        .collect();
    catalog.insert_game_uuids(&games).await.unwrap();
    // Written twice, exactly as re-importing an overlapping date range does.
    // The page must not hand the walk the same game twice.
    catalog.insert_game_uuids(&games[..1]).await.unwrap();

    let mut ordered: Vec<(DateTime<Utc>, String)> = games
        .iter()
        .map(|game| (game.started_at, game.uuid.clone()))
        .collect();
    ordered.sort();

    // Start one game short of the range so the first page is this test's rows.
    let mut cursor = Some(SweepPosition {
        started_at: base - TimeDelta::seconds(1),
        uuid: String::new(),
    });
    let mut seen: Vec<(DateTime<Utc>, String)> = Vec::new();
    // Paged until this run's rows are all in hand rather than a fixed number of
    // times: a page that `dedup_by` shortens is one row nearer the end than it
    // looks, and the suite shares this table, so a page may also carry another
    // run's rows. Only this run's are collected — a repeat of one of them still
    // shows up, which is what the assertion is for.
    for page_number in 1..=12 {
        assert!(
            page_number < 12,
            "the walk did not reach the end of its rows"
        );
        let page = catalog
            .game_uuid_listings(cursor.as_ref(), 2)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        cursor = page.last().cloned();
        seen.extend(
            page.into_iter()
                .map(|position| (position.started_at, position.uuid))
                .filter(|(_, uuid)| uuid.contains(&run.to_string())),
        );
        if seen.len() >= ordered.len() {
            break;
        }
    }
    // Every game once, in the table's own order. This is the assertion the alias
    // shadow fails — with it, page two is page one again — and the one a missing
    // keyset tuple fails, because the two games sharing a second would be
    // skipped or repeated.
    assert_eq!(seen, ordered);

    // The cursor the walk actually keeps, round-tripped. It is the difference
    // between a restart costing one page and a restart costing the catalogue.
    let resume = SweepPosition {
        started_at: ordered[2].0,
        uuid: ordered[2].1.clone(),
    };
    let walk = format!("test-{run}");
    assert!(catalog.refetch_cursor(&walk).await.unwrap().is_none());
    catalog.set_refetch_cursor(&walk, &resume).await.unwrap();
    assert_eq!(catalog.refetch_cursor(&walk).await.unwrap(), Some(resume));
    catalog.clear_refetch_cursor(&walk).await.unwrap();
    assert!(catalog.refetch_cursor(&walk).await.unwrap().is_none());

    // What decides whether a request is spent. A game with a claim is one this
    // corpus has stored; the hash has to be the one PostgreSQL wrote, not one
    // this process agrees with itself about.
    let stored = &ordered[1].1;
    let raw = format!(
        r#"{{"type":"start_game","names":["a","b","c","d"],"majsoul":{{"uuid":"{stored}","start_time":1784178000}}}}"#
    );
    let response = api::router(state.clone())
        .oneshot(ingest_request(&test_source(&data_dir), stored, &raw))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let hashes: Vec<Vec<u8>> = ordered
        .iter()
        .map(|(_, uuid)| mjai_management::indexer::game_claim_hash(uuid))
        .collect();
    let held = catalog.claimed_games(&hashes).await.unwrap();
    assert_eq!(
        held,
        [mjai_management::indexer::game_claim_hash(stored)]
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        "only the ingested game is claimed"
    );
    assert!(catalog.claimed_games(&[]).await.unwrap().is_empty());

    // And the console's comparison card runs at all.
    let window = SeriesWindow::recent(SeriesUnit::Day, 7);
    let gap = catalog.paipuya_gap(window).await.unwrap();
    assert!(gap.missing <= gap.listed);

    std::fs::remove_dir_all(data_dir).unwrap();
}

/// The one thing each of the two game tables must not be allowed to hold,
/// refused at the door.
///
/// Both were held at once, and neither failure raised anything. A catalogue row
/// with no players matches nothing, so the comparison card counted 191 million
/// of them as missing and read 100%. A 牌谱屋 short id is not a uuid Mahjong
/// Soul serves, so the walk spent one rate-limited request per row to be told
/// the game does not exist — which logs identically to a game that has aged out.
#[tokio::test]
async fn refuses_catalogue_rows_and_work_list_rows_that_could_never_work() {
    let (state, data_dir) = test_state().await;
    // Two endpoints, two ways in, and that is the point rather than an accident:
    // the catalogue is loaded by an administrator in the console, the work list
    // by a collector holding the API key. `majsoul2mjai push-uuids` has a key
    // and no browser session, so putting the work list behind the admin group
    // would answer 401 to the only caller it has.
    let session = admin_session(&state);
    let post = |uri: &'static str, body: String, admin: bool| {
        let mut request = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer test-secret");
        if admin {
            request = request.header("x-mjai-user-session", session.clone());
        }
        api::router(state.clone()).oneshot(request.body(Body::from(body)).unwrap())
    };
    let started_at = DateTime::from_timestamp(4_100_000_000, 0).unwrap();

    let unmatchable = PaipuyaGame {
        uuid: "260716-0000-0000-0000-000000000001".to_owned(),
        mode_id: 16,
        started_at,
        ended_at: started_at + TimeDelta::seconds(600),
        players: Vec::new(),
        account_ids: Vec::new(),
        scores: Vec::new(),
    };
    let response = post(
        "/api/v1/paipuya/games",
        serde_json::json!({ "games": [unmatchable] }).to_string(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // What 牌谱屋 actually returns for every game: `_masked`, with an
    // 11-character base58 id sitting in the `uuid` field.
    let unfetchable = GameUuid {
        uuid: "98yKIfZs7vZ".to_owned(),
        mode_id: 16,
        started_at,
    };
    let response = post(
        "/api/v1/games/uuids",
        serde_json::json!({ "games": [unfetchable] }).to_string(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // The same row with a uuid Mahjong Soul would answer to is accepted, so the
    // guard is the short id and not the endpoint being unreachable.
    let fetchable = GameUuid {
        uuid: format!("260716-{}", Uuid::new_v4()),
        mode_id: 16,
        started_at,
    };
    let response = post(
        "/api/v1/games/uuids",
        serde_json::json!({ "games": [fetchable] }).to_string(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    std::fs::remove_dir_all(data_dir).unwrap();
}
