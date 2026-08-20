use std::{sync::Arc, time::Duration};

use clap::Parser;
use mjai_management::{AppState, api, backfill, config::Config, gc, indexer};
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// How often the ingest path's view of the topic backlog is refreshed. Two
/// broker round trips and a query per partition, so it is periodic rather than
/// per request; between samples the backlog is only ever underestimated, which
/// makes `MJAI_KAFKA_MAX_LAG` a ceiling overshot by at most one interval's
/// ingest rather than one that refuses work a healthy worker is keeping up
/// with.
const LAG_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// Take the open files the OS is already willing to give.
///
/// Docker starts a container at soft 1024 / hard 524288 and leaves raising the
/// soft limit to the process. Go runtimes do it themselves at startup — which
/// is why mihomo in this same stack runs at 524287 while this process sat on
/// 1024 — and Rust's does not.
///
/// A thousand descriptors is not a lot here. The re-fetch pool holds a
/// websocket per session and builds an HTTP client per login on top of that,
/// and a deployment runs a couple of hundred sessions; add the Postgres and
/// ClickHouse pools, the broker, the object store and the listener and the
/// ceiling is reached in normal operation. What that looks like is the worst
/// part: `EMFILE` surfaces wherever the next descriptor happened to be asked
/// for, so one exhausted limit is reported as a proxy that will not connect,
/// a DNS resolver that will not resolve, and an account file that has gone
/// missing — three unrelated-looking faults with one cause.
///
/// Best effort by design. A limit that cannot be raised is worth a line in the
/// log, not a process that refuses to start.
#[cfg(unix)]
fn raise_open_file_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: the pointer is to a live local of exactly the type the call
    // expects, and the call does not retain it.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        tracing::warn!("读不到本进程的文件描述符上限，跳过抬升");
        return;
    }
    let previous = limit.rlim_cur;
    // Clamped as well as raised: macOS reports an infinite hard limit but
    // refuses anything above `kern.maxfilesperproc`, and nothing here wants a
    // million descriptors — it wants more than a thousand.
    limit.rlim_cur = limit.rlim_max.min(1_048_576);
    if previous >= limit.rlim_cur {
        return;
    }
    // SAFETY: as above.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        tracing::warn!(
            "文件描述符上限还是 {previous}，抬不上去；补抓会话多了会开始报 \
             Too many open files"
        );
        return;
    }
    tracing::info!("文件描述符上限 {previous} -> {}", limit.rlim_cur);
}

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
    #[cfg(unix)]
    raise_open_file_limit();

    let config = Config::parse();
    if let Some(warning) = config.record_limit_warning() {
        tracing::warn!("{warning}");
    }
    let listen = config.listen.clone();
    let state = AppState::local(config).await?;
    state.watch_service.start_if_enabled().await?;
    // The re-fetch pool. Off unless a configuration turned it on, because it
    // logs in with real accounts and asks Mahjong Soul for every record the
    // corpus is missing an original for; an upgrade must not start that by
    // itself. Its switch, its pacing and its progress live on the console's
    // 牌谱补抓 page.
    if let Err(error) = state.paipuya.start_if_enabled().await {
        tracing::error!(%error, "牌谱屋同步没能启动");
    }
    if let Err(error) = state.refetch_service.start_if_enabled().await {
        // Never fatal: a pool that cannot read its account file is a reason to
        // say so on the console, not a reason to take the whole API down.
        tracing::error!(%error, "补抓服务没能启动");
    }
    // All of these run behind the listener rather than in front of it: the
    // legacy upload moves hundreds of megabytes, both backfills read the bytes
    // of every record in the index, and the collector waits on ClickHouse, and
    // an API that will not answer until one of them finishes looks like an
    // outage. None of them can therefore report by failing to start, so they all
    // log at error level — the only other symptom of a broken upload is a bucket
    // that quietly stays empty.
    //
    // The backfills are spawned together rather than chained because each is a
    // no-op once its marker is written, and on the one boot where several have
    // work they are reading the same pages of the same index a moment apart,
    // which the object store serves from the same place either way.
    // Behind the listener like the backfills, and for the same reason: mihomo
    // may not be up yet, and an API that waits on it looks like an outage.
    {
        let mihomo = Arc::clone(&state.mihomo);
        // Re-derived from the pool rather than trusted from the file mihomo's
        // slots were last written to. The two agree whenever the console did
        // the editing, which is every normal case; they disagree when the
        // document was edited some other way, and then the file is the stale
        // one — it would leave an account bound to a node that has no listener.
        let nodes = state.accounts.refetch_nodes();
        tokio::spawn(async move {
            if let Err(error) = mihomo.set_outbound_nodes(&nodes) {
                tracing::error!(%error, "写不出账号池的独立出站配置，补抓池会共用一条出站");
            }
            mihomo.apply_runtime_config().await;
        });
    }
    tokio::spawn(upload_legacy_packs(state.clone()));
    tokio::spawn(backfill::rewrite_record_metadata(state.clone()));
    tokio::spawn(backfill::write_game_scoped_claims(state.clone()));
    tokio::spawn(backfill::score_indexed_records(state.clone()));
    // After the scoring pass rather than before it, and both are spawned rather
    // than chained: the scoring pass repairs a statistic on all 1.9M records
    // from the mjai already stored, and this one rebuilds the mjai itself for
    // the 245k that kept their protobuf. A record this pass rewrites is scored
    // again by the pack worker on the way in, so the order between them costs
    // nothing but a little repeated work on the overlap.
    tokio::spawn(backfill::reconvert_stored_records(state.clone()));
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
