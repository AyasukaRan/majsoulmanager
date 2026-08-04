//! The re-fetch pool: its own accounts, its own proxy, its own pacing.
//!
//! Every record indexed before the converter was fixed carries the errors it was
//! fixed for and no original to re-derive from. They all carry a game uuid, so
//! they can be fetched again — and there are several hundred thousand of them.
//!
//! The first version of this borrowed the live collector's session, because a
//! second login with the same account disconnects the collector. That made it
//! safe by construction and slow by construction: an instance answers only in
//! the part of its poll interval it would otherwise sleep through, one request
//! at a time. This is the other half of that trade. The pool logs in with
//! accounts of its own, so it can open as many sessions as it has accounts and
//! run them all at once, and the only thing it borrows from the collectors is
//! the rule that it must never take an account one of them is using.
//!
//! It still leaves its requests on the same counter (`crate::refetch`), so a
//! collector with spare time keeps answering them for free.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    catalog::{Catalog, Record, RecordFilter},
    indexer,
    kafka::Kafka,
    majsoul::convert::GameMetadata,
    managed_watch::{
        LoginTransport, connect, ends_the_session, fetch_game_record, load_accounts,
        masked_account, proxy_display, refreshed_client_version,
    },
    mihomo::MihomoManager,
    mjai,
    pack::PackStore,
    refetch::{CLAIM_TIMEOUT, RefetchBroker, RefetchError},
    watch_log::{WatchLogBuffer, WatchLogLevel},
    watch_service::{
        PluginWorker, ServicePhase, WatchAction, WatchProxyMode, WatchServiceError,
        WatchSupervisor, persist_json, redacted_proxy, restored_proxy, validate_secret_ref,
    },
};

/// One page of the corpus walk, the size every other walk in this codebase uses.
const PAGE_SIZE: usize = 1_000;

/// How many records pass between progress lines. The console's log buffer holds
/// 500 entries and the collectors write into the same one, so a line per page
/// would push their lines out of it.
const PROGRESS_EVERY: u64 = 500;

/// How long a worker waits before logging in again after a session ended.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// How long the walk waits before looking at the packing backlog again. Long
/// enough that a queue draining at the worker's own pace makes visible progress
/// between checks, short enough that it does not idle once it has.
const BACKLOG_BACKOFF: Duration = Duration::from_secs(15);

/// How long an idle worker parks on the request counter before looking again.
/// It is woken the moment a request arrives, so this only bounds how long a
/// worker can stay asleep after one is missed, never how quickly work starts.
const IDLE_POLL: Duration = Duration::from_secs(5);

/// How long the walk waits between passes.
///
/// A replaced record does not lose its `pb_size = 0` row the instant it is
/// re-fetched: it travels through the topic, waits for its pack to seal
/// (`MJAI_PACK_IDLE_SECS`, 30 by default) and only then reaches the index. A
/// second pass starting immediately would find every record the first one just
/// fixed and fetch them all again.
const SETTLE_BETWEEN_PASSES: Duration = Duration::from_secs(120);

/// How many passes one run makes before stopping on its own.
///
/// The walk repeats because a record skipped for a transient reason — the
/// session that was fetching it dropped — is only retried by another pass, and
/// each pass is cheap once `missing_pb` prunes the finished ones. The cap is
/// what stops a deployment whose pack worker is permanently further behind than
/// `SETTLE_BETWEEN_PASSES` from re-fetching the same records forever.
const MAX_PASSES: u32 = 12;

/// What the console's log panel attributes this service's lines to. Workers tag
/// themselves `refetch:0`, `refetch:1` and so on, so the page can show its own
/// lines by matching this prefix.
const LOG_SOURCE: &str = "refetch";

/// Ceilings on the two knobs. The concurrency ceiling is not a Mahjong Soul
/// limit — the real limit is how many accounts a deployment has, since a second
/// login on one account ends the first session — it only stops a typo from
/// spawning a hundred logins.
const MAX_CONCURRENCY: usize = 16;
const MAX_REQUEST_DELAY_MS: u64 = 60_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RefetchServiceConfig {
    pub revision: u64,
    /// Whether the pool starts with the API. Off by default: it logs in with
    /// real accounts and asks Mahjong Soul for hundreds of thousands of
    /// records, which is not something a deployment should begin doing because
    /// it was upgraded.
    pub enabled: bool,
    pub server: String,
    #[serde(default)]
    pub proxy_mode: WatchProxyMode,
    pub custom_proxy_url: Option<String>,
    /// `file:` or `env:`, one `username,password` per line — the same format a
    /// collector's secret uses, so one file can serve both. A collector reads
    /// only the first line; the pool reads every line and drops the ones a
    /// collector holds.
    pub account_secret_ref: String,
    /// How many sessions to run at once. Capped by the number of usable
    /// accounts, because one account is one session.
    pub concurrency: usize,
    /// How long each session waits between two requests.
    pub request_delay_ms: u64,
    pub client_version: Option<String>,
}

impl Default for RefetchServiceConfig {
    fn default() -> Self {
        Self {
            revision: 1,
            enabled: false,
            server: "cn".into(),
            proxy_mode: WatchProxyMode::Mihomo,
            custom_proxy_url: None,
            account_secret_ref: "file:/run/secrets/majsoul_refetch_accounts".into(),
            concurrency: 2,
            // Three times the collector's, and deliberately. A collector sends
            // a handful of requests per poll and then sleeps; this sends one
            // after another for as long as the backlog lasts, from an account
            // that never plays a game.
            request_delay_ms: 1_500,
            client_version: None,
        }
    }
}

impl RefetchServiceConfig {
    pub fn validate(&self) -> Result<(), WatchServiceError> {
        if !matches!(self.server.as_str(), "cn" | "en" | "jp") {
            return Err(WatchServiceError::InvalidConfig(
                "server must be cn, en or jp".into(),
            ));
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.concurrency) {
            return Err(WatchServiceError::InvalidConfig(format!(
                "concurrency must be between 1 and {MAX_CONCURRENCY}"
            )));
        }
        if self.request_delay_ms > MAX_REQUEST_DELAY_MS {
            return Err(WatchServiceError::InvalidConfig(format!(
                "request_delay_ms must not exceed {MAX_REQUEST_DELAY_MS}"
            )));
        }
        validate_secret_ref(&self.account_secret_ref)?;
        match self.proxy_mode {
            WatchProxyMode::Direct | WatchProxyMode::Mihomo => Ok(()),
            WatchProxyMode::Custom => {
                let value = self.custom_proxy_url.as_deref().ok_or_else(|| {
                    WatchServiceError::InvalidConfig(
                        "custom_proxy_url is required in custom proxy mode".into(),
                    )
                })?;
                let url = reqwest::Url::parse(value).map_err(|error| {
                    WatchServiceError::InvalidConfig(format!("invalid custom proxy URL: {error}"))
                })?;
                if !matches!(url.scheme(), "http" | "https" | "socks5") {
                    return Err(WatchServiceError::InvalidConfig(
                        "custom proxy URL must use http, https or socks5".into(),
                    ));
                }
                Ok(())
            }
        }
    }
}

/// What one run has done so far. Cumulative across the passes of that run, and
/// reset when a run starts, so the console never shows two runs added together.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RefetchProgress {
    /// Which pass of this run is in flight, counting from 1.
    pub pass: u32,
    pub scanned: u64,
    pub replaced: u64,
    /// Mahjong Soul answered and would not give the record: it has aged out, or
    /// the game never had a paipu. Also counts a fetch whose session died — the
    /// record stays untouched and the next pass asks again.
    pub refused: u64,
    /// The record's own bytes could not be read out of its pack.
    pub unreadable: u64,
    /// Nothing was wrong with the fetch; the result was not safe to store. A
    /// record with no Majsoul header to name the game, or a re-conversion that
    /// came out shorter than what is already in the corpus.
    pub unconvertible: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RefetchRuntimeStatus {
    pub phase: ServicePhase,
    pub active_revision: Option<u64>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
    /// Accounts left after the ones the collectors hold were dropped.
    pub accounts: usize,
    /// Sessions asked for, which is `min(concurrency, accounts)`.
    pub workers: usize,
    /// Sessions logged in right now. Below `workers` while one is reconnecting.
    pub sessions: usize,
    /// Requests waiting on the counter for any server to take.
    pub waiting: usize,
    /// Records with no protobuf when this run started. `None` before the first
    /// run, or when the count could not be read.
    pub backlog: Option<u64>,
    pub progress: RefetchProgress,
}

impl Default for RefetchRuntimeStatus {
    fn default() -> Self {
        Self {
            phase: ServicePhase::Stopped,
            active_revision: None,
            started_at: None,
            updated_at: Utc::now(),
            last_error: None,
            accounts: 0,
            workers: 0,
            sessions: 0,
            waiting: 0,
            backlog: None,
            progress: RefetchProgress::default(),
        }
    }
}

/// Everything the pool reads or writes, gathered so the supervisor never holds
/// an `AppState` — that would be an `Arc` cycle, since the state holds this.
pub struct RefetchDependencies {
    pub data_dir: PathBuf,
    pub catalog: Arc<Catalog>,
    pub packs: Arc<PackStore>,
    pub kafka: Arc<Kafka>,
    pub broker: Arc<RefetchBroker>,
    pub mihomo: Arc<MihomoManager>,
    pub logs: Arc<WatchLogBuffer>,
    /// Asked for the login and PB modules a deployment has selected, and for
    /// the accounts its collectors hold.
    pub watch: Arc<WatchSupervisor>,
}

pub struct RefetchSupervisor {
    config_path: PathBuf,
    config: RwLock<RefetchServiceConfig>,
    runtime: RwLock<RefetchRuntimeStatus>,
    progress: RwLock<RefetchProgress>,
    sessions: AtomicUsize,
    dependencies: RefetchDependencies,
    generation: AtomicU64,
    /// The sessions, held apart from the walk so that the walk can end them when
    /// it runs out of work. A logged-in Mahjong Soul session with nothing left
    /// to fetch is an account sitting online for no reason.
    sessions_tasks: Mutex<Vec<JoinHandle<()>>>,
    walk_task: Mutex<Option<JoinHandle<()>>>,
}

impl RefetchSupervisor {
    pub fn new(dependencies: RefetchDependencies) -> Result<Self, WatchServiceError> {
        let directory = dependencies.data_dir.join("refetch");
        std::fs::create_dir_all(&directory)?;
        let config_path = directory.join("config.json");
        let config = match std::fs::read(&config_path) {
            Ok(bytes) => {
                let config: RefetchServiceConfig = serde_json::from_slice(&bytes)?;
                config.validate()?;
                config
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let config = RefetchServiceConfig::default();
                persist_json(&config_path, &config)?;
                config
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            config_path,
            config: RwLock::new(config),
            runtime: RwLock::new(RefetchRuntimeStatus::default()),
            progress: RwLock::new(RefetchProgress::default()),
            sessions: AtomicUsize::new(0),
            dependencies,
            generation: AtomicU64::new(0),
            sessions_tasks: Mutex::new(Vec::new()),
            walk_task: Mutex::new(None),
        })
    }

    pub fn config(&self) -> RefetchServiceConfig {
        self.config.read().clone()
    }

    /// The configuration as the API serves it: the same document with the proxy
    /// credentials taken out. Never used to dial anything.
    pub fn published_config(&self) -> RefetchServiceConfig {
        let mut config = self.config();
        config.custom_proxy_url = redacted_proxy(config.custom_proxy_url.as_deref());
        config
    }

    /// The stored status with the three live readings folded in, so a console
    /// poll sees one consistent picture rather than three that were taken at
    /// different moments.
    pub fn status(&self) -> RefetchRuntimeStatus {
        let mut status = self.runtime.read().clone();
        status.sessions = self.sessions.load(Ordering::Relaxed);
        status.waiting = self.dependencies.broker.waiting();
        status.progress = *self.progress.read();
        status
    }

    pub async fn update_config(
        self: &Arc<Self>,
        next: RefetchServiceConfig,
    ) -> Result<RefetchServiceConfig, WatchServiceError> {
        let mut next = next;
        next.validate()?;
        let submitted = next.revision;
        {
            // Checked and swapped under one lock, the write included, for the
            // same reason the watch configuration is: the state this guards
            // against is two writers interleaving, and a file that disagrees
            // with memory is that same failure one step later.
            let mut current = self.config.write();
            if submitted != current.revision {
                return Err(WatchServiceError::RevisionConflict {
                    submitted,
                    current: current.revision,
                });
            }
            next.custom_proxy_url =
                restored_proxy(next.custom_proxy_url, current.custom_proxy_url.as_deref());
            next.revision = current.revision.saturating_add(1);
            persist_json(&self.config_path, &next)?;
            *current = next.clone();
        }
        // Answered redacted, like every other read of this document.
        next.custom_proxy_url = redacted_proxy(next.custom_proxy_url.as_deref());
        if next.enabled {
            self.start().await?;
        } else {
            self.stop().await;
        }
        Ok(next)
    }

    pub async fn apply_action(
        self: &Arc<Self>,
        action: WatchAction,
    ) -> Result<RefetchRuntimeStatus, WatchServiceError> {
        match action {
            WatchAction::Start | WatchAction::Reload => self.start().await?,
            WatchAction::Stop => self.stop().await,
        }
        Ok(self.status())
    }

    pub async fn start_if_enabled(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        if self.config.read().enabled {
            return self.start().await;
        }
        // A pool that is switched off says how much is owed anyway, once, on the
        // one query it would have run at the start of a run. Nothing else in the
        // deployment tracks this: the backlog is not a marker in PostgreSQL like
        // the boot passes, it is the `pb_size = 0` rows themselves, and the
        // overview's counts deliberately leave them out. Without this line an
        // upgraded deployment goes quiet about a debt nothing else will mention.
        //
        // Whether that debt has a deadline is not known. Nobody here has
        // measured how long Mahjong Soul serves a replay by uuid, and the
        // comments that used to assert it does not serve them forever were
        // repeating a sentence written to justify something else. See #81.
        match self.dependencies.catalog.count_missing_pb().await {
            Ok(0) => {}
            Ok(backlog) => {
                self.runtime.write().backlog = Some(backlog);
                self.report(
                    WatchLogLevel::Warn,
                    format!(
                        "索引里还有 {backlog} 条记录没有雀魂原始牌谱，补抓服务是关着的（控制台「牌谱补抓」页可以启动）"
                    ),
                );
            }
            Err(error) => tracing::warn!(%error, "读不到待补抓的记录条数"),
        }
        Ok(())
    }

    /// The accounts this pool may log in with.
    ///
    /// Anything a collector could log in with is dropped, whether that collector
    /// is running or not: Mahjong Soul allows one session per account, so the
    /// pool taking one would disconnect a collector, and the two would then
    /// spend the rest of the day kicking each other off. A disabled collector
    /// counts, because an operator may enable it at any moment while this runs.
    ///
    /// Live collection is the half that cannot be redone, and not because a
    /// replay expires — nobody has measured that (#81). It is that a uuid only
    /// ever reaches this deployment through `fetchGameLiveList`, which lists a
    /// game while it is being played and never again: a collector that was
    /// locked out for an hour did not fall behind, it never learned those games
    /// existed. So this errs towards giving the pool nothing rather than
    /// towards giving it one account too many.
    fn usable_accounts(
        &self,
        config: &RefetchServiceConfig,
    ) -> Result<Vec<(String, String)>, WatchServiceError> {
        let accounts = load_accounts(&config.account_secret_ref)
            .map_err(|error| WatchServiceError::InvalidConfig(format!("{error:#}")))?;
        Ok(pool_accounts(accounts, &self.collector_accounts()))
    }

    /// The usernames the collectors could log in with, read from the watch
    /// configuration every time rather than remembered.
    ///
    /// Both sides move: a collector can be added, re-pointed at another secret
    /// or have its file's first line changed at any moment, and the watch
    /// service knows nothing about this pool to warn it. A snapshot taken when
    /// the pool started would keep looking correct right up until the two logged
    /// in with one account and started kicking each other off — and what that
    /// costs is games that were being played while the collector was locked out,
    /// which Mahjong Soul does not serve twice.
    ///
    /// A collector whose secret cannot be read contributes nothing, which is the
    /// right answer — it cannot log in either — but it is said out loud, because
    /// the alternative reading is that the pool is about to take its account.
    fn collector_accounts(&self) -> HashSet<String> {
        self.dependencies
            .watch
            .config()
            .instances
            .iter()
            .filter_map(
                |instance| match load_accounts(&instance.account_secret_ref) {
                    // The first line, byte for byte what `load_first_account` gives
                    // the collector itself.
                    Ok(mut accounts) => Some(accounts.swap_remove(0).0),
                    Err(error) => {
                        tracing::warn!(
                            instance = %instance.id,
                            %error,
                            "读不到采集实例的账号，无法确认补抓池是否会和它抢账号"
                        );
                        None
                    }
                },
            )
            .collect()
    }

    async fn start(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        let config = self.config();
        self.report(WatchLogLevel::Info, "补抓服务启动中".to_owned());
        let accounts = match self.usable_accounts(&config) {
            Ok(accounts) if accounts.is_empty() => {
                let message =
                    "账号池里没有可用账号：文件为空，或者里面的账号都已经被采集实例占用".to_owned();
                self.fail(&message);
                return Err(WatchServiceError::InvalidConfig(message));
            }
            Ok(accounts) => accounts,
            Err(error) => {
                self.fail(&format!("读不到账号池：{error}"));
                return Err(error);
            }
        };

        self.stop_tasks().await;
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let workers = config.concurrency.min(accounts.len());
        *self.progress.write() = RefetchProgress::default();
        self.sessions.store(0, Ordering::Relaxed);
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Starting;
            runtime.active_revision = Some(config.revision);
            runtime.started_at = Some(Utc::now());
            runtime.updated_at = Utc::now();
            runtime.last_error = None;
            runtime.accounts = accounts.len();
            runtime.workers = workers;
            runtime.backlog = None;
        }
        if workers < config.concurrency {
            self.report(
                WatchLogLevel::Warn,
                format!(
                    "并发被账号池限制为 {workers}（配置要 {}，可用账号 {}）；一个账号只能开一个会话，要更高并发得加账号",
                    config.concurrency,
                    accounts.len()
                ),
            );
        }

        let proxy = self.proxy_url(&config);
        for (_, password) in &accounts {
            self.dependencies.logs.register_secret(password.clone());
        }
        if let Some(url) = proxy.as_deref() {
            register_proxy_secrets(&self.dependencies.logs, url);
        }
        self.report(
            WatchLogLevel::Info,
            format!(
                "补抓池启动 {workers} 个会话（可用账号 {}，代理 {}，每次请求间隔 {}ms）",
                accounts.len(),
                proxy.as_deref().map_or("直连".to_owned(), proxy_display),
                config.request_delay_ms
            ),
        );

        let mut sessions = Vec::with_capacity(workers);
        for (index, (username, password)) in accounts.into_iter().take(workers).enumerate() {
            let supervisor = Arc::clone(self);
            let config = config.clone();
            let proxy = proxy.clone();
            sessions.push(tokio::spawn(async move {
                supervisor
                    .run_worker(generation, config, index, username, password, proxy)
                    .await;
            }));
        }
        *self.sessions_tasks.lock().await = sessions;
        let supervisor = Arc::clone(self);
        *self.walk_task.lock().await = Some(tokio::spawn(async move {
            supervisor.run_walk(generation).await;
        }));

        let mut runtime = self.runtime.write();
        runtime.phase = ServicePhase::Running;
        runtime.updated_at = Utc::now();
        Ok(())
    }

    async fn stop(&self) {
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Stopping;
            runtime.updated_at = Utc::now();
        }
        self.stop_tasks().await;
        self.sessions.store(0, Ordering::Relaxed);
        let mut runtime = self.runtime.write();
        runtime.phase = ServicePhase::Stopped;
        runtime.active_revision = None;
        runtime.workers = 0;
        runtime.updated_at = Utc::now();
        drop(runtime);
        self.report(WatchLogLevel::Info, "补抓服务已停止".to_owned());
    }

    async fn stop_tasks(&self) {
        let walk = self.walk_task.lock().await.take();
        if let Some(walk) = walk {
            walk.abort();
            let _ = walk.await;
        }
        self.stop_sessions().await;
    }

    /// Ends every session. Called by `stop`, and by the walk when it has nothing
    /// left to fetch.
    async fn stop_sessions(&self) {
        let tasks = std::mem::take(&mut *self.sessions_tasks.lock().await);
        // Aborted before anything is awaited, so a cancellation part-way through
        // cannot leave a session logged in with no handle left to stop it.
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        self.sessions.store(0, Ordering::Relaxed);
    }

    fn proxy_url(&self, config: &RefetchServiceConfig) -> Option<String> {
        match config.proxy_mode {
            WatchProxyMode::Direct => None,
            WatchProxyMode::Mihomo => Some(self.dependencies.mihomo.proxy_url().to_owned()),
            WatchProxyMode::Custom => config.custom_proxy_url.clone(),
        }
    }

    fn report(&self, level: WatchLogLevel, message: String) {
        match level {
            WatchLogLevel::Error => tracing::error!("{message}"),
            WatchLogLevel::Warn => tracing::warn!("{message}"),
            _ => tracing::info!("{message}"),
        }
        self.dependencies.logs.append(level, LOG_SOURCE, message);
    }

    /// Records a failure on the status so the console shows it, and logs it.
    fn fail(&self, message: &str) {
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Failed;
            runtime.updated_at = Utc::now();
            runtime.last_error = Some(message.to_owned());
        }
        self.report(WatchLogLevel::Error, message.to_owned());
    }

    /// One session, for as long as this generation lasts: log in, answer
    /// requests until the session ends, log in again.
    async fn run_worker(
        self: Arc<Self>,
        generation: u64,
        config: RefetchServiceConfig,
        index: usize,
        username: String,
        password: String,
        proxy: Option<String>,
    ) {
        let source = format!("{LOG_SOURCE}:{index}");
        // Shared with the collectors: the gateway, the package version and the
        // discovered version floor are properties of the Majsoul deployment, not
        // of the account, so the pool benefits from their lookups and they from
        // its.
        let cache_dir = self.dependencies.data_dir.join("watch/cache");
        let (login_worker, pb_worker) = match self.dependencies.watch.module_workers().await {
            Ok(workers) => workers,
            Err(error) => {
                self.fail(&format!("补抓会话 {index} 起不了协议模块：{error}"));
                return;
            }
        };
        let mut client_version = config.client_version.clone();
        loop {
            // Re-read before every login, never remembered. A collector added or
            // re-pointed since this pool started may now hold this account, and
            // logging in would disconnect it — live collection is the one thing
            // here that cannot be done again.
            if self.collector_accounts().contains(&username) {
                self.report(
                    WatchLogLevel::Error,
                    format!(
                        "账号 {} 现在被采集实例占用，补抓会话 {index} 退出，不去抢它",
                        masked_account(&username)
                    ),
                );
                return;
            }
            self.dependencies.logs.append(
                WatchLogLevel::Info,
                &source,
                format!("登录中（账号 {}）", masked_account(&username)),
            );
            match connect(
                &config.server,
                client_version.as_deref(),
                &username,
                &password,
                proxy.as_deref(),
                login_worker.clone(),
                &self.dependencies.logs,
                &source,
                &cache_dir,
            )
            .await
            {
                Ok((transport, negotiated)) => {
                    self.sessions.fetch_add(1, Ordering::Relaxed);
                    let outcome = self
                        .serve(
                            &config,
                            &transport,
                            pb_worker.as_ref(),
                            &negotiated,
                            &source,
                        )
                        .await;
                    self.sessions.fetch_sub(1, Ordering::Relaxed);
                    transport.close().await;
                    if let Err(error) = outcome {
                        self.dependencies.logs.append(
                            WatchLogLevel::Warn,
                            &source,
                            format!("会话断开：{error:#}"),
                        );
                    }
                }
                Err(error) => {
                    let detail = format!("{error:#}");
                    self.dependencies.logs.append(
                        WatchLogLevel::Error,
                        &source,
                        format!("登录失败：{detail}"),
                    );
                    if let Some(refreshed) = refreshed_client_version(
                        &config.server,
                        &username,
                        &password,
                        proxy.as_deref(),
                        login_worker.clone(),
                        &self.dependencies.logs,
                        &source,
                        &cache_dir,
                        client_version.as_deref(),
                        &detail,
                    )
                    .await
                    {
                        client_version = Some(refreshed);
                    }
                }
            }
            if self.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    /// Answers requests on one session until it ends.
    ///
    /// Which failures are the session's and which are the record's is the one
    /// thing this has to get right, and it is not decided here: `ends_the_session`
    /// states that rule once for every fetch loop in the codebase.
    async fn serve(
        &self,
        config: &RefetchServiceConfig,
        transport: &LoginTransport,
        pb_worker: Option<&Arc<PluginWorker>>,
        client_version: &str,
        source: &str,
    ) -> anyhow::Result<()> {
        loop {
            let Some(request) = self.dependencies.broker.claim() else {
                // Parked on the counter rather than on a timer, so a request
                // that arrives a moment from now is served then.
                self.dependencies.broker.wait_for_work(IDLE_POLL).await;
                continue;
            };
            match fetch_game_record(transport, pb_worker, request.uuid(), client_version).await {
                Ok(raw) => request.answer(Ok(raw)),
                Err(error) => {
                    // Answered before the session is torn down, so a waiter
                    // whose request died with it learns immediately instead of
                    // waiting out the full claim timeout. Either way the record
                    // is left untouched and the next pass asks again.
                    request.answer(Err(RefetchError::Refused(format!("{error:#}"))));
                    if ends_the_session(&error) {
                        return Err(error);
                    }
                    // Only to the container log. A refusal per record is the
                    // normal case for a corpus this old, and the console's
                    // buffer holds 500 lines for every service at once — a line
                    // each here would push the collectors' out of it, and the
                    // count is on the page anyway.
                    tracing::debug!(%source, error = %format!("{error:#}"), "雀魂拒绝了一局");
                }
            }
            tokio::time::sleep(Duration::from_millis(config.request_delay_ms)).await;
        }
    }

    /// The corpus walk: repeats until a pass moves nothing, then stops.
    async fn run_walk(self: Arc<Self>, generation: u64) {
        let outcome = self.walk(generation).await;
        // However this run ended, the sessions go with it. A pool that has run
        // out of work — or hit something it cannot get past — has no reason to
        // keep several accounts logged into Mahjong Soul, and leaving them there
        // is exposure bought with nothing.
        if self.generation.load(Ordering::SeqCst) == generation {
            self.stop_sessions().await;
            match outcome {
                Ok(message) => self.finish(&message),
                Err(error) => self.fail(&format!("补抓中止，已替换的部分仍然有效：{error:#}")),
            }
        }
    }

    /// The walk itself: `Ok` carries why it stopped, `Err` why it could not go
    /// on. Split from `run_walk` so that ending the sessions is one statement
    /// covering every way out rather than one before each `return`.
    async fn walk(&self, generation: u64) -> anyhow::Result<String> {
        match self.dependencies.catalog.count_missing_pb().await {
            Ok(backlog) => {
                self.runtime.write().backlog = Some(backlog);
                self.report(
                    WatchLogLevel::Info,
                    format!("索引里有 {backlog} 条记录没有雀魂原始牌谱，开始补抓"),
                );
                if backlog == 0 {
                    return Ok("没有需要补抓的记录".to_owned());
                }
            }
            // Not fatal: the backlog is a number for the console, and the walk
            // finds its own work.
            Err(error) => self.report(
                WatchLogLevel::Warn,
                format!("读不到待补抓的记录条数，进度只报绝对值：{error}"),
            ),
        }

        for pass in 1..=MAX_PASSES {
            self.progress.write().pass = pass;
            let (scanned, replaced) = self.one_pass().await?;
            self.report(
                WatchLogLevel::Info,
                format!("第 {pass} 轮补抓结束：扫描 {scanned} 条，替换 {replaced} 条"),
            );
            // Nothing moved, so another pass would meet the same records and be
            // refused the same way.
            if replaced == 0 {
                return Ok(if scanned == 0 {
                    "补抓完成：索引里已经没有缺少原始牌谱的记录".to_owned()
                } else {
                    "补抓结束：剩下的记录雀魂都不再提供，或者重转结果不能替换原记录".to_owned()
                });
            }
            if self.generation.load(Ordering::SeqCst) != generation {
                return Ok("配置已更换，本轮结束".to_owned());
            }
            tokio::time::sleep(SETTLE_BETWEEN_PASSES).await;
        }
        Ok(format!(
            "补抓已跑满 {MAX_PASSES} 轮仍有记录在变，先停下来；重新启动会接着跑"
        ))
    }

    /// Ends a run that finished on its own terms rather than by failing.
    fn finish(&self, message: &str) {
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Stopped;
            runtime.updated_at = Utc::now();
        }
        self.report(WatchLogLevel::Info, message.to_owned());
    }

    /// One walk over every record that has no protobuf. Returns how many rows it
    /// looked at and how many it replaced.
    async fn one_pass(&self) -> anyhow::Result<(u64, u64)> {
        let filter = RecordFilter {
            missing_pb: true,
            ..RecordFilter::default()
        };
        let mut cursor = None;
        let (mut scanned, mut replaced) = (0u64, 0u64);
        let mut reported = 0u64;
        loop {
            // Paged newest-first, and safe beside live ingest for the reason
            // every walk here is: a row written while this is in flight either
            // sorts above the cursor and is never read, or is read like any
            // other — and a row written now came from the fixed converter with
            // its protobuf attached, so the filter excludes it anyway.
            let (page, next) = self
                .dependencies
                .catalog
                .scan(&filter, cursor, PAGE_SIZE)
                .await?;
            // The number of sessions, not the configured concurrency. They
            // differ whenever the account pool is smaller than the setting, and
            // asking for more requests than there are servers only means the
            // ones at the back wait behind requests that have not started —
            // which, with a claim timeout of three minutes and a pacing delay
            // that goes up to a minute, is how a pool that is merely slow starts
            // reporting that nobody is serving it.
            let in_flight = self.runtime.read().workers.max(1);
            let outcomes = futures_util::stream::iter(page)
                .map(|row| self.one_record(row))
                .buffer_unordered(in_flight)
                .collect::<Vec<_>>()
                .await;
            {
                let mut progress = self.progress.write();
                for outcome in outcomes {
                    scanned += 1;
                    progress.scanned += 1;
                    match outcome? {
                        Outcome::Replaced => {
                            replaced += 1;
                            progress.replaced += 1;
                        }
                        Outcome::Refused => progress.refused += 1,
                        Outcome::Unreadable => progress.unreadable += 1,
                        Outcome::Unconvertible => progress.unconvertible += 1,
                    }
                }
            }
            if scanned - reported >= PROGRESS_EVERY {
                reported = scanned;
                let progress = *self.progress.read();
                self.report(
                    WatchLogLevel::Info,
                    format!(
                        "补抓中：扫描 {} 条，替换 {} 条，雀魂拒绝 {} 条，读不到字节 {} 条，无法替换 {} 条",
                        progress.scanned,
                        progress.replaced,
                        progress.refused,
                        progress.unreadable,
                        progress.unconvertible
                    ),
                );
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok((scanned, replaced)),
            }
        }
    }

    /// One record: read its bytes, fetch the game again, re-convert, replace.
    ///
    /// Only a request nobody answered is an error. Everything else is an
    /// outcome, because a record Mahjong Soul will not serve must not stop the
    /// walk from reaching the next one.
    async fn one_record(&self, row: Record) -> anyhow::Result<Outcome> {
        let raw = match self.dependencies.packs.read(&row.storage).await {
            Ok(raw) => raw,
            Err(error) => {
                // Logged to the container and not to the console's 500-line
                // buffer: one unreachable pack is thousands of these.
                tracing::warn!(record = %row.id, %error, "跳过一条读不到字节的记录");
                return Ok(Outcome::Unreadable);
            }
        };
        // The uuid names the game to fetch and the header names the mode, which
        // the converter writes back into `start_game`. Both come from the record
        // being replaced, so the replacement carries the header the original did.
        let Some((uuid, metadata)) = majsoul_header(&raw) else {
            return Ok(Outcome::Unconvertible);
        };
        let pb = match self.dependencies.broker.fetch(&uuid).await {
            Ok(pb) => pb,
            Err(RefetchError::Unserved) => {
                // With sessions up this is one slow request, not a broken pool —
                // every one of them was reconnecting, or a login took longer
                // than the claim timeout. The record is untouched, so the next
                // pass asks again; ending the run over it would mean a Majsoul
                // maintenance window leaves the service stopped until somebody
                // notices. Only a pool with nothing logged in is a pool that
                // will never answer, and walking the rest of the corpus three
                // minutes at a time is the thing worth refusing to do.
                if self.sessions.load(Ordering::Relaxed) == 0 {
                    anyhow::bail!(
                        "没有会话接手补抓请求（等了 {} 秒，当前登录会话为 0）",
                        CLAIM_TIMEOUT.as_secs()
                    );
                }
                tracing::info!(record = %row.id, "补抓请求超时，留给下一轮");
                return Ok(Outcome::Refused);
            }
            Err(RefetchError::Refused(why)) => {
                tracing::info!(record = %row.id, %why, "雀魂没有提供这局的牌谱");
                return Ok(Outcome::Refused);
            }
        };
        // Off the async runtime: a re-conversion decodes a protobuf, walks
        // every event, gzips the result and parses it back twice, and with
        // `concurrency` of them at once on a small box that is enough to stall
        // the API's own request handling.
        let expected = uuid.clone();
        let (pb, converted) = tokio::task::spawn_blocking(move || {
            let converted = reconverted(&pb, &metadata, &raw, &expected);
            (pb, converted)
        })
        .await?;
        let Some(mjai) = converted else {
            return Ok(Outcome::Unconvertible);
        };
        // Behind the same backlog ceiling live ingest answers `503` on, because
        // this path does not go through `indexer::claim` and would otherwise be
        // the one writer that ignores it. A topic past that ceiling drops
        // records the API has already acknowledged, and the records at risk are
        // the collectors' — the ones nobody can fetch a second time. Waiting is
        // free here: the backlog is somebody else's work being done.
        while self.dependencies.kafka.lag() >= self.dependencies.kafka.max_lag() / 2 {
            tracing::info!("打包队列积压，补抓让出写入位置");
            tokio::time::sleep(BACKLOG_BACKOFF).await;
        }
        indexer::reindex_one(&self.dependencies.kafka, &row, mjai, Some(pb)).await?;
        Ok(Outcome::Replaced)
    }
}

enum Outcome {
    Replaced,
    Refused,
    Unreadable,
    Unconvertible,
}

/// The pool's accounts once the collectors have had theirs: everything in
/// `pool` that no collector holds, each at most once.
///
/// Kept apart from the reading of the two secrets so the rule that protects live
/// collection can be stated — and tested — without a supervisor, a watch service
/// and two files on disk.
fn pool_accounts(
    pool: Vec<(String, String)>,
    collectors: &HashSet<String>,
) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    pool.into_iter()
        // Duplicates matter as much as collisions do: one account listed twice
        // would have the pool log in with it twice and disconnect itself.
        .filter(|(username, _)| !collectors.contains(username) && seen.insert(username.clone()))
        .collect()
}

/// Registers a proxy URL's credentials with the log buffer. Module stderr and
/// error chains can echo the URL they were given.
fn register_proxy_secrets(logs: &WatchLogBuffer, proxy: &str) {
    logs.register_secret(proxy);
    if let Ok(parsed) = reqwest::Url::parse(proxy) {
        if let Some(password) = parsed.password() {
            logs.register_secret(password);
        }
        if !parsed.username().is_empty() {
            logs.register_secret(parsed.username());
        }
    }
}

/// The game uuid and the mode header of a stored record.
fn majsoul_header(raw: &[u8]) -> Option<(String, GameMetadata)> {
    let events = mjai::events(raw).ok()?;
    let start = events
        .iter()
        .find(|event| event.get("type").and_then(|kind| kind.as_str()) == Some("start_game"))?;
    let majsoul = start.get("majsoul")?;
    Some((
        majsoul.get("uuid")?.as_str()?.to_owned(),
        GameMetadata {
            mode_id: majsoul.get("mode_id").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            room: majsoul
                .get("room")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            game_length: majsoul
                .get("game_length")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            players: majsoul.get("players").and_then(|v| v.as_u64()).unwrap_or(0) as u8,
            year: majsoul.get("year").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        },
    ))
}

/// The protobuf converted with today's converter, or `None` if the result is not
/// safe to put in the corpus's place.
///
/// Two guards. The first is that it has to be the same game: the record keeps
/// its `record_id` and only its bytes change, so a protobuf for some other game
/// would not fail anything — it would quietly become this record, and the game
/// that was there would be gone with no trace in the index. Nothing on the way
/// here checks it, since the request travels through a queue and, where an
/// external module is in use, through a subprocess that is handed the uuid and
/// trusted with it.
///
/// The second is an event count. A paipu Mahjong Soul generated only partly, or
/// a conversion that gave up halfway, produces a shorter record — and replacing
/// a complete game with a truncated one would be a loss dressed up as a repair.
/// Equal is allowed and expected: the fixed converter adds fields to events, not
/// events.
fn reconverted(
    pb: &[u8],
    metadata: &GameMetadata,
    stored: &[u8],
    expected_uuid: &str,
) -> Option<Vec<u8>> {
    use std::io::Read;

    let (uuid, compressed) =
        crate::majsoul::convert::convert_record_bytes(pb, Some(metadata)).ok()?;
    if uuid != expected_uuid {
        tracing::error!(%uuid, %expected_uuid, "补抓拿回来的是另一局，拒绝替换");
        return None;
    }
    let mut mjai = Vec::new();
    flate2::read::GzDecoder::new(compressed.as_slice())
        .read_to_end(&mut mjai)
        .ok()?;
    let fresh = mjai::events(&mjai).ok()?.len();
    let before = mjai::events(stored).ok()?.len();
    (fresh >= before).then_some(mjai)
}

/// Where this service keeps its configuration, exposed for the test below and
/// for anyone reading a deployment's data directory.
pub fn config_path(data_dir: &Path) -> PathBuf {
    data_dir.join("refetch/config.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> RefetchServiceConfig {
        RefetchServiceConfig {
            account_secret_ref: "env:MJAI_TEST_REFETCH_ACCOUNTS".into(),
            ..RefetchServiceConfig::default()
        }
    }

    /// The pool logs in with real accounts and asks Mahjong Soul for hundreds of
    /// thousands of records. A deployment that is upgraded must not start doing
    /// that because a new field defaulted to on.
    #[test]
    fn a_fresh_configuration_does_nothing_until_it_is_turned_on() {
        let config = RefetchServiceConfig::default();
        config.validate().unwrap();
        assert!(!config.enabled);
        // And the delay is above the collector's 500ms, because this one sends
        // requests back to back for as long as the backlog lasts.
        assert!(config.request_delay_ms >= 1_000);
    }

    #[test]
    fn rejects_a_plaintext_secret_and_out_of_range_knobs() {
        let plaintext = RefetchServiceConfig {
            account_secret_ref: "user,password".into(),
            ..valid()
        };
        assert!(plaintext.validate().is_err());

        for concurrency in [0, MAX_CONCURRENCY + 1] {
            let config = RefetchServiceConfig {
                concurrency,
                ..valid()
            };
            assert!(
                config.validate().is_err(),
                "concurrency {concurrency} was accepted"
            );
        }
        assert!(
            RefetchServiceConfig {
                request_delay_ms: MAX_REQUEST_DELAY_MS + 1,
                ..valid()
            }
            .validate()
            .is_err()
        );
        // Zero is allowed: an operator who asks for no delay at all has said so
        // deliberately, and the concurrency cap already bounds the rate.
        assert!(
            RefetchServiceConfig {
                request_delay_ms: 0,
                ..valid()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn a_custom_proxy_must_be_a_url_this_can_actually_dial() {
        let missing = RefetchServiceConfig {
            proxy_mode: WatchProxyMode::Custom,
            custom_proxy_url: None,
            ..valid()
        };
        assert!(missing.validate().is_err());
        let wrong_scheme = RefetchServiceConfig {
            proxy_mode: WatchProxyMode::Custom,
            custom_proxy_url: Some("ftp://proxy.example:7890".into()),
            ..valid()
        };
        assert!(wrong_scheme.validate().is_err());
        let good = RefetchServiceConfig {
            proxy_mode: WatchProxyMode::Custom,
            custom_proxy_url: Some("http://proxy.example:7890".into()),
            ..valid()
        };
        assert!(good.validate().is_ok());
    }

    /// The rule that keeps this from destroying the thing it exists to repair.
    ///
    /// Mahjong Soul allows one session per account, so the pool logging in with
    /// an account a collector uses disconnects that collector, and the two then
    /// kick each other off for as long as both run. What is lost while a
    /// collector is locked out is games that were being played at the time —
    /// Mahjong Soul does not serve those a second time, which is the whole
    /// reason this file exists.
    #[test]
    fn the_pool_never_takes_an_account_a_collector_could_use() {
        let account = |name: &str| (name.to_owned(), format!("{name}-password"));
        let collectors: HashSet<String> = ["live@example.com", "sanma@example.com"]
            .into_iter()
            .map(str::to_owned)
            .collect();

        // The shared-file arrangement: the collector takes line one, the pool
        // works through what is left.
        let usable = pool_accounts(
            vec![
                account("live@example.com"),
                account("pool-a@example.com"),
                account("pool-b@example.com"),
            ],
            &collectors,
        );
        assert_eq!(
            usable
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["pool-a@example.com", "pool-b@example.com"]
        );

        // A collector that is configured but switched off still holds its
        // account: an operator may switch it on while the pool is running.
        assert!(pool_accounts(vec![account("sanma@example.com")], &collectors).is_empty());

        // One account listed twice would have the pool disconnect itself.
        let deduplicated = pool_accounts(
            vec![account("pool-a@example.com"), account("pool-a@example.com")],
            &collectors,
        );
        assert_eq!(deduplicated.len(), 1);
    }

    /// A configuration written by an older build has no `proxy_mode`, and one
    /// written by hand may leave it out entirely.
    #[test]
    fn reads_a_configuration_that_predates_the_proxy_mode() {
        let document = serde_json::json!({
            "revision": 4,
            "enabled": true,
            "server": "cn",
            "custom_proxy_url": null,
            "account_secret_ref": "env:POOL",
            "concurrency": 3,
            "request_delay_ms": 2000,
            "client_version": null,
        });
        let config: RefetchServiceConfig = serde_json::from_value(document).unwrap();
        assert_eq!(config.proxy_mode, WatchProxyMode::Mihomo);
        config.validate().unwrap();
    }
}
