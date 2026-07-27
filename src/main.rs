use std::time::Duration;

use clap::Parser;
use mjai_management::{AppState, api, config::Config, gc};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    let listener = TcpListener::bind(&listen).await?;
    tracing::info!(%listen, "mjai management API listening");
    axum::serve(listener, api::router(state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    // The startup pack scan would re-index whatever is still buffered, but only
    // under `source = recovered`, because the pack bytes do not carry the
    // original source. Flushing on a planned shutdown keeps it.
    if let Err(error) = state.catalog.flush().await {
        tracing::error!(%error, "could not flush the record index on shutdown");
    }
    Ok(())
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
