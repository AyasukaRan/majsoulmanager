use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use flate2::read::GzDecoder;
use mjai_management::{AppState, api, config::Config};
use serde_json::Value;
use std::io::Read;
use tower::ServiceExt;
use uuid::Uuid;

fn test_state() -> (AppState, std::path::PathBuf) {
    let data_dir = std::env::temp_dir().join(format!("mjai-api-test-{}", Uuid::new_v4()));
    let state = AppState::local(Config {
        listen: "127.0.0.1:0".into(),
        api_key: "test-secret".into(),
        data_dir: data_dir.clone(),
        max_record_bytes: 16 * 1024,
        max_batch_bytes: 1024 * 1024,
        max_batch_records: 100,
        pack_target_bytes: 1024 * 1024,
    })
    .unwrap();
    (state, data_dir)
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
