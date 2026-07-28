use std::{sync::Arc, time::Duration};

use clap::Parser;
use mjai_management::{AppState, api, config::Config, gc, indexer};
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// How often the ingest path's view of the topic backlog is refreshed. Two
/// broker round trips and a query per partition, so it is periodic rather than
/// per request; between samples the backlog is only ever underestimated, which
/// makes `MJAI_KAFKA_MAX_LAG` a ceiling overshot by at most one interval's
/// ingest rather than one that refuses work a healthy worker is keeping up
/// with.
const LAG_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mjai_management=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::parse();
    if let Some(warning) = config.record_limit_warning() {
        tracing::warn!("{warning}");
    }
    let listen = config.listen.clone();
    let state = AppState::local(config).await?;
    state.watch_service.start_if_enabled().await?;
    // Both of these run behind the listener rather than in front of it: the
    // legacy upload moves hundreds of megabytes and the collector waits on
    // ClickHouse, and an API that will not answer until either finishes looks
    // like an outage. Neither can therefore report by failing to start, so both
    // log at error level — the only other symptom of a broken upload is a
    // bucket that quietly stays empty.
    tokio::spawn(upload_legacy_packs(state.clone()));
    tokio::spawn(collect_orphans(state.clone()));
    // What the ingest path's backlog ceiling reads. Nothing samples it in the
    // test suite, which is why an unsampled reading of zero has to mean "no
    // backlog" rather than "unknown".
    tokio::spawn(
        Arc::clone(&state.kafka)
            .sample_lag_forever(state.catalog.postgres().clone(), LAG_SAMPLE_INTERVAL),
    );

    // One shutdown signal for the whole process: axum's graceful shutdown waits
    // on it, and when `serve` returns the workers are told the same way rather
    // than through a second handler that could fire at a different time.
    let (stopping, shutdown) = watch::channel(false);
    let workers = indexer::spawn_workers(&state, &shutdown);

    let listener = TcpListener::bind(&listen).await?;
    tracing::info!(%listen, "mjai management API listening");
    let served = axum::serve(listener, api::router(state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await;

    // Sent even when the server failed, so a listener that died does not leave
    // the workers running against a process nobody will stop cleanly.
    let _ = stopping.send(true);
    for worker in workers {
        // A pack worth of records, already durable in the topic, is waiting on
        // the seal each of these is finishing. Exiting without it costs no
        // data — the offset was never committed, so they replay — but it makes
        // every redeploy re-pack and re-upload up to one pack per partition.
        if let Err(error) = worker.await {
            tracing::error!(%error, "a pack/index worker did not stop cleanly");
        }
    }
    Ok(served?)
}

/// Runs once per boot. Uploading a pack that is already there is a `HEAD` and
/// nothing else, so a deployment whose corpus is long since copied pays one
/// request per pack and says so.
async fn upload_legacy_packs(state: AppState) {
    match state.packs.upload_legacy_packs().await {
        Ok(0) => tracing::info!("历史 pack 已全部在对象存储中，无需上传"),
        Ok(count) => tracing::info!("已把 {count} 个历史 pack 上传到对象存储，本地副本保留"),
        Err(error) => tracing::error!(%error, "历史 pack 上传失败，本地副本仍是唯一的一份"),
    }
}

async fn collect_orphans(state: AppState) {
    // `interval` panics on a zero period, and an operator who sets the interval
    // to zero means "as often as possible", not "crash on boot".
    let mut ticks =
        tokio::time::interval(Duration::from_secs(state.config.gc_interval_secs.max(1)));
    let grace = Duration::from_secs(state.config.gc_grace_secs);
    loop {
        ticks.tick().await;
        match gc::collect(&state.catalog, &state.objects, grace).await {
            Ok(0) => {}
            Ok(count) => tracing::info!("对象回收删除了 {count} 个没有索引引用的 pack"),
            Err(error) => tracing::error!(%error, "对象回收失败，本轮没有删除任何对象"),
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
