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
    indexer::{self, game_claim_hash},
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

/// The one ceiling left. Concurrency has none: it never was a Mahjong Soul
/// limit, and the real limit sits at the point the sessions are opened —
/// `min(concurrency, accounts)`, because a second login on an account ends the
/// first session. A pool of eighty accounts asked for eighty sessions and got
/// sixteen, which is sixty-four accounts idling for a number that was only ever
/// there to catch a typo. What catches a typo now is the account count sitting
/// on the page next to the box.
const MAX_REQUEST_DELAY_MS: u64 = 60_000;

/// The name the 牌谱屋 walk keeps its position under. One row in
/// `refetch_cursor`; the `MissingPb` walk has none, because its own filter is
/// its position — a record it has repaired stops matching `pb_size = 0`.
const PAIPUYA_WALK: &str = "paipuya";

/// What `records.source` says about a game this pool went and got because 牌谱屋
/// listed it. Not `majsoul-watch`: nobody here was in the game, and the column
/// is how an export tells "collected live" from "swept afterwards" apart. It has
/// no bearing on deduplication — that is scoped by the game's own uuid, under a
/// namespace constant that deliberately does not follow this name.
const PAIPUYA_SOURCE: &str = "paipuya";

/// What this field defaulted to before the console grew an account pool.
///
/// It names a path `docker-compose.yml` has never mounted — it always meant
/// "add a bind mount yourself" — so a deployment that never edited this box
/// gets `No such file or directory` on every start, with its accounts sitting
/// in the pool the whole time.
const LEGACY_SECRET_REF: &str = "file:/run/secrets/majsoul_refetch_accounts";

/// Where the walk gets its uuids.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RefetchWork {
    /// Records already in the index that carry no Mahjong Soul protobuf. The
    /// repair this pool was built for, and the default, because it is the one
    /// that finishes.
    #[default]
    MissingPb,
    /// Games 牌谱屋 lists that this corpus has never stored. Unbounded by
    /// comparison — the catalogue is three orders of magnitude larger than the
    /// corpus — so it is a sweep that runs until it is stopped, not a repair
    /// that completes.
    PaipuyaGap,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RefetchServiceConfig {
    pub revision: u64,
    /// Whether the pool starts with the API. Off by default: it logs in with
    /// real accounts and asks Mahjong Soul for hundreds of thousands of
    /// records, which is not something a deployment should begin doing because
    /// it was upgraded.
    pub enabled: bool,
    /// Which backlog the pool works through. Defaulted rather than optional so
    /// a configuration written before it existed keeps meaning the repair it
    /// was written for.
    #[serde(default)]
    pub work: RefetchWork,
    /// Where the 牌谱屋 walk starts when it has no stored position. Ignored once
    /// it has one — the cursor is the position, this is only its seed.
    ///
    /// It matters more than a start date usually does. The catalogue runs from
    /// 2019 and this corpus begins in 2026-07, so a walk left to start at the
    /// beginning spends years on games that were never going to be here, none
    /// of which the claim check can skip, and all of which are the population
    /// nobody has measured Mahjong Soul's retention for (#81). Pointing it at
    /// the recent end first is how a deployment finds out whether the old end
    /// is worth walking at all.
    #[serde(default)]
    pub paipuya_from: Option<DateTime<Utc>>,
    pub server: String,
    #[serde(default)]
    pub proxy_mode: WatchProxyMode,
    pub custom_proxy_url: Option<String>,
    /// Where the accounts come from. `pool:refetch` is the console's account
    /// pool, which is where they live now; `file:` and `env:` still work and
    /// hold one `username,password` per line — the same format a collector's
    /// secret uses, so one file can serve both. A collector reads only the
    /// first line; the pool reads every line and drops the ones a collector
    /// holds.
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
            work: RefetchWork::MissingPb,
            paipuya_from: None,
            server: "cn".into(),
            proxy_mode: WatchProxyMode::Mihomo,
            custom_proxy_url: None,
            account_secret_ref: "pool:refetch".into(),
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

/// Points a configuration that still carries the pre-pool default at the pool,
/// and says whether it changed anything.
///
/// Only that exact string, and only when the path it names is genuinely absent:
/// a deployment that did add the bind mount has a working configuration, and
/// moving it onto the pool would log the sessions in with different accounts
/// than the ones it chose. Anything typed by hand is left alone whatever it
/// says — an operator who wrote `env:` meant `env:`.
fn migrate_legacy_secret_ref(config: &mut RefetchServiceConfig, legacy_file_exists: bool) -> bool {
    if config.account_secret_ref != LEGACY_SECRET_REF || legacy_file_exists {
        return false;
    }
    config.account_secret_ref = "pool:refetch".into();
    // Bumped, because the document on disk is no longer the one the console is
    // holding. Without it a page opened before the restart saves its stale copy
    // over this — the revision check would see no conflict — and puts the
    // deployment back on the path that does not exist.
    config.revision = config.revision.saturating_add(1);
    true
}

impl RefetchServiceConfig {
    pub fn validate(&self) -> Result<(), WatchServiceError> {
        if !matches!(self.server.as_str(), "cn" | "en" | "jp") {
            return Err(WatchServiceError::InvalidConfig(
                "server must be cn, en or jp".into(),
            ));
        }
        if self.concurrency < 1 {
            return Err(WatchServiceError::InvalidConfig(
                "concurrency must be at least 1".into(),
            ));
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
    /// 牌谱屋 walk only: catalogued games this corpus already holds, recognised
    /// before any request was sent. The number the whole "compare first, fetch
    /// second" arrangement exists to make large.
    pub present: u64,
    /// 牌谱屋 walk only: games fetched that turned out to be held after all,
    /// because a claim landed between this page being compared and its games
    /// being fetched. Rare by construction, and worth its own counter precisely
    /// for that: a large one means the comparison is not working and the pool is
    /// spending rate-limited requests on games it already has.
    pub duplicates: u64,
    /// 牌谱屋 walk only: how far into the catalogue the walk has read. The only
    /// honest measure of progress over half a billion games — a percentage would
    /// be this run's fetches over a total nothing in a year will approach.
    pub position: Option<DateTime<Utc>>,
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
    /// `unconvertible` split by cause. One number cannot distinguish "the guard
    /// kept a better record" from "the converter is broken", and those call for
    /// opposite reactions. Fixed fields rather than a map because this struct is
    /// `Copy` — it is read as `*self.progress.read()`.
    pub unconvertible_by: UnconvertibleCounts,
}

#[derive(Clone, Debug, Serialize)]
pub struct RefetchRuntimeStatus {
    pub phase: ServicePhase,
    pub active_revision: Option<u64>,
    /// What the run in progress is actually doing. The console has a dropdown
    /// for this too, but that one is the operator's unsaved edit — labelling a
    /// running pb repair with the sweep's counters because somebody opened the
    /// select is how a number comes to mean something it does not.
    pub active_work: Option<RefetchWork>,
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
    /// Records per second over the last 30 seconds. The counters say how much
    /// has been done; this says whether anything is happening right now.
    pub qps: f64,
    /// The most recent rejections, newest first. Bounded — a display buffer,
    /// not a record of what happened; the service log has that.
    pub failures: Vec<RefetchFailure>,
}

impl Default for RefetchRuntimeStatus {
    fn default() -> Self {
        Self {
            phase: ServicePhase::Stopped,
            active_revision: None,
            active_work: None,
            started_at: None,
            updated_at: Utc::now(),
            last_error: None,
            accounts: 0,
            workers: 0,
            sessions: 0,
            waiting: 0,
            backlog: None,
            progress: RefetchProgress::default(),
            qps: 0.0,
            failures: Vec::new(),
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
    pub accounts: Arc<crate::accounts::AccountPool>,
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
    /// Kept beside `progress` rather than in it: `RefetchProgress` is `Copy` and
    /// these two are not.
    rate: RwLock<RateWindow>,
    failures: RwLock<std::collections::VecDeque<RefetchFailure>>,
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
                let mut config: RefetchServiceConfig = serde_json::from_slice(&bytes)?;
                config.validate()?;
                // Written back, not just held in memory: the console shows this
                // field, and an operator reading `file:` there while the pool
                // logs in from `pool:refetch` is worse than the bug.
                if migrate_legacy_secret_ref(
                    &mut config,
                    Path::new(LEGACY_SECRET_REF.trim_start_matches("file:")).exists(),
                ) {
                    persist_json(&config_path, &config)?;
                }
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
            rate: RwLock::new(RateWindow::default()),
            failures: RwLock::new(std::collections::VecDeque::new()),
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
        status.qps = self.rate.read().qps();
        status.failures = self.failures.read().iter().rev().cloned().collect();
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
                // Said, not stored. `backlog` is documented as what a *run* set
                // out to do, and the console divides by it: seeding it here put
                // a figure under a progress bar for a run that had not started,
                // and in 牌谱屋 mode it would be a figure in the wrong units.
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
        let accounts = load_accounts(&config.account_secret_ref, &self.dependencies.accounts)
            .map_err(|error| WatchServiceError::InvalidConfig(format!("{error:#}")))?;
        Ok(pool_accounts(accounts, &self.collector_accounts()?))
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
    /// A `file:` or `env:` reference that cannot be read stops the pool. What it
    /// named a minute ago is not knowable from here, and a collector reads its
    /// credentials once and keeps logging in with them, so "unreadable" says
    /// nothing about which account is in use.
    ///
    /// A `pool:` reference is not read at all — the store it names is this
    /// process's own and `reserved` below already holds everything in it.
    fn collector_accounts(&self) -> Result<HashSet<String>, WatchServiceError> {
        // Every account the console filed under live collection, whether an
        // instance points at it today or not and whether it is switched on or
        // not. Filing it is the reservation: an operator adds a second collector
        // account before adding the collector, and taking it in between would
        // put the pool on it exactly when the instance appears.
        let mut held = self
            .dependencies
            .accounts
            .reserved(crate::accounts::AccountPurpose::Watch);
        for instance in &self.dependencies.watch.config().instances {
            // A `pool:` instance is already counted. `reserved` above took every
            // account filed under live collection, enabled or not, and that is a
            // superset of anything `pool:watch/…` can resolve to — reading it
            // would add nothing, and failing on it would refuse to start over an
            // instance pointing at an account nobody has added yet. That
            // instance holds no session: it cannot log in either.
            //
            // Which is exactly a deployment nobody has configured live
            // collection on. Its one instance names an account that is not in
            // the pool, and without this the pool it is not competing with could
            // never start.
            if instance.account_secret_ref.starts_with("pool:") {
                continue;
            }
            match load_accounts(&instance.account_secret_ref, &self.dependencies.accounts) {
                // The first line, byte for byte what `load_first_account` gives
                // the collector itself.
                Ok(mut accounts) => {
                    held.insert(accounts.swap_remove(0).0);
                }
                // Not skipped. A collector reads its credentials once, when it
                // starts, and goes on logging in with them for the life of the
                // process — so a reference that stops resolving now says nothing
                // about which account is in use, it only means this pool can no
                // longer tell. Treating that as "holds nothing" is how the pool
                // takes an account out from under a live session, and the two
                // then kick each other off; what is lost meanwhile is games
                // being played, which nothing lists twice. So the pool refuses
                // to start instead, and says which instance it could not read.
                Err(error) => {
                    return Err(WatchServiceError::InvalidConfig(format!(
                        // Not "或者停用它": the loop above reads every instance,
                        // switched off or not, and deliberately so — an
                        // operator turns one back on with a click. Advising a
                        // remedy that changes nothing sends them to a checkbox
                        // and back.
                        "读不到采集实例 {} 的账号（{error:#}），无法确认补抓池会不会和它抢；\
                         把它的账号引用改成控制台账号池里的 pool:watch/账号，或者删掉这个实例",
                        instance.id
                    )));
                }
            }
        }
        Ok(held)
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
            runtime.active_work = Some(config.work);
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
            let proxy = self.account_proxy(&config, &username, proxy.as_deref());
            sessions.push(tokio::spawn(async move {
                supervisor
                    .run_worker(generation, config, index, username, password, proxy)
                    .await;
            }));
        }
        *self.sessions_tasks.lock().await = sessions;
        let supervisor = Arc::clone(self);
        let work = config.work;
        *self.walk_task.lock().await = Some(tokio::spawn(async move {
            supervisor.run_walk(generation, work).await;
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
        runtime.active_work = None;
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

    /// Where one account's session dials.
    ///
    /// The pool's own proxy unless the account names a node and mihomo has a
    /// listener for it — which is what spreads a pool of eighty sessions over
    /// several exits instead of pushing all of them through one. Only in
    /// `mihomo` mode: a deployment that set a custom proxy or turned the proxy
    /// off said where its traffic goes, and a per-account node cannot overrule
    /// that without silently ignoring the setting above it.
    fn account_proxy(
        &self,
        config: &RefetchServiceConfig,
        username: &str,
        lane: Option<&str>,
    ) -> Option<String> {
        if config.proxy_mode != WatchProxyMode::Mihomo {
            return lane.map(str::to_owned);
        }
        let node = self.dependencies.accounts.node_for(username)?;
        self.dependencies
            .mihomo
            .proxy_url_for_node(&node)
            .or_else(|| lane.map(str::to_owned))
    }

    fn proxy_url(&self, config: &RefetchServiceConfig) -> Option<String> {
        match config.proxy_mode {
            WatchProxyMode::Direct => None,
            WatchProxyMode::Mihomo => Some(
                self.dependencies
                    .mihomo
                    .proxy_url_for(crate::mihomo::MihomoLane::Refetch),
            ),
            WatchProxyMode::Custom => config.custom_proxy_url.clone(),
        }
    }

    /// Bounded push onto the console's rejection list.
    fn note_failure(&self, subject: impl Into<String>, rejected: &Rejected) {
        const KEEP: usize = 200;
        let mut failures = self.failures.write();
        if failures.len() >= KEEP {
            failures.pop_front();
        }
        failures.push_back(RefetchFailure {
            at: Utc::now(),
            subject: subject.into(),
            why: rejected.why,
            label: rejected.why.label(),
            detail: rejected.detail.clone(),
        });
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
            let collectors = match self.collector_accounts() {
                Ok(collectors) => collectors,
                // Same rule as at start-up, applied to a session that is already
                // running: unreadable means unknown, and unknown means do not
                // log in. A worker that cannot tell whether a collector holds
                // this account stops rather than risk taking it.
                Err(error) => {
                    self.report(
                        WatchLogLevel::Error,
                        format!("补抓会话 {index} 退出：{error}"),
                    );
                    return;
                }
            };
            if collectors.contains(&username) {
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
    async fn run_walk(self: Arc<Self>, generation: u64, work: RefetchWork) {
        let outcome = self.walk(generation, work).await;
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
    async fn walk(&self, generation: u64, work: RefetchWork) -> anyhow::Result<String> {
        if let Some(done) = match work {
            RefetchWork::MissingPb => self.announce_missing_pb().await?,
            RefetchWork::PaipuyaGap => self.announce_paipuya_gap().await?,
        } {
            return Ok(done);
        }

        for pass in 1..=MAX_PASSES {
            self.progress.write().pass = pass;
            let (scanned, replaced) = match work {
                RefetchWork::MissingPb => self.one_pass(generation).await?,
                RefetchWork::PaipuyaGap => self.one_paipuya_pass(generation).await?,
            };
            self.report(
                WatchLogLevel::Info,
                match work {
                    RefetchWork::MissingPb => {
                        format!("第 {pass} 轮补抓结束：扫描 {scanned} 条，替换 {replaced} 条")
                    }
                    RefetchWork::PaipuyaGap => {
                        format!("第 {pass} 轮补抓结束：走查 {scanned} 局，入库 {replaced} 局")
                    }
                },
            );
            // Nothing moved, so another pass would meet the same records and be
            // refused the same way.
            if replaced == 0 {
                return Ok(match (work, scanned) {
                    (RefetchWork::MissingPb, 0) => {
                        "补抓完成：索引里已经没有缺少原始牌谱的记录".to_owned()
                    }
                    (RefetchWork::MissingPb, _) => {
                        "补抓结束：剩下的记录雀魂都不再提供，或者重转结果不能替换原记录".to_owned()
                    }
                    // Deliberately not "the corpus holds everything 牌谱屋
                    // lists". A pass that resumed near the end of the catalogue
                    // and ran off it looks identical from here, and saying the
                    // gap is closed on the strength of the last few thousand
                    // rows would be a claim about half a billion games.
                    (RefetchWork::PaipuyaGap, 0) => {
                        "本轮没有要抓的对局：走到的这一段牌谱屋收录的本地都有了".to_owned()
                    }
                    (RefetchWork::PaipuyaGap, _) => {
                        "补抓结束：这一轮缺的对局雀魂都不再提供".to_owned()
                    }
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

    /// The `MissingPb` walk's opening figure. `Some` means there is nothing to
    /// do and the run is over before it starts.
    async fn announce_missing_pb(&self) -> anyhow::Result<Option<String>> {
        match self.dependencies.catalog.count_missing_pb().await {
            Ok(backlog) => {
                self.runtime.write().backlog = Some(backlog);
                self.report(
                    WatchLogLevel::Info,
                    format!("索引里有 {backlog} 条记录没有雀魂原始牌谱，开始补抓"),
                );
                Ok((backlog == 0).then(|| "没有需要补抓的记录".to_owned()))
            }
            // Not fatal: the backlog is a number for the console, and the walk
            // finds its own work.
            Err(error) => {
                self.report(
                    WatchLogLevel::Warn,
                    format!("读不到待补抓的记录条数，进度只报绝对值：{error}"),
                );
                Ok(None)
            }
        }
    }

    /// The 牌谱屋 walk's two preconditions, both of which are silent damage
    /// rather than a visible failure if they are skipped.
    ///
    /// An empty catalogue would otherwise walk zero pages and report that this
    /// corpus holds everything 牌谱屋 lists, which on a deployment that has
    /// never synced is the opposite of true.
    ///
    /// The claims backfill is the sharper one. This walk asks
    /// `ingest_idempotency` whether a game has ever been stored, and that answer
    /// is only complete once `write_game_scoped_claims` has covered every record
    /// already in the index. It re-runs on every boot until it does, and
    /// `main.rs` starts this pool *before* it spawns — so a run that started on
    /// a boot where the backfill still had work would see no claim for a million
    /// records this corpus already holds, fetch every one of them again, and
    /// write a second row per game that nothing can ever collapse: `record_id`
    /// is in the sorting key. Refusing to start is the only safe answer, and it
    /// costs one indexed lookup.
    async fn announce_paipuya_gap(&self) -> anyhow::Result<Option<String>> {
        if !self
            .dependencies
            .catalog
            .backfill_completed(crate::backfill::GAME_CLAIMS_NAME)
            .await?
        {
            anyhow::bail!(
                "对局幂等认领回填还没跑完，现在按牌谱屋缺口补抓会把已有的对局再存一份且无法合并；\
                 等这次启动的回填结束后再启动（日志里搜「对局幂等认领」）"
            );
        }
        // No `backlog`: `start()` already cleared it, and there is no figure to
        // put there. The catalogue's size is not what this run set out to fetch
        // — three orders of magnitude of it is already held or never served —
        // so a bar drawn against it would read zero for the life of the sweep.
        // The walk reports where it has read to instead.
        let totals = self.dependencies.catalog.paipuya_totals().await?;
        if totals.games == 0 {
            return Ok(Some(
                "牌谱屋的对局信息还没同步过，没有缺口可抓；先在上面把「牌谱屋同步」跑起来"
                    .to_owned(),
            ));
        }
        let resuming = self
            .dependencies
            .catalog
            .refetch_cursor(PAIPUYA_WALK)
            .await?;
        self.report(
            WatchLogLevel::Info,
            match &resuming {
                Some(position) => format!(
                    "牌谱屋已同步 {} 局，从上次走到的 {} 接着比对",
                    totals.games,
                    position.started_at.format("%Y-%m-%d %H:%M:%S")
                ),
                None => format!("牌谱屋已同步 {} 局，从头开始比对", totals.games),
            },
        );
        Ok(None)
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
    async fn one_pass(&self, generation: u64) -> anyhow::Result<(u64, u64)> {
        let filter = RecordFilter {
            missing_pb: true,
            ..RecordFilter::default()
        };
        let mut cursor = None;
        let (mut scanned, mut replaced) = (0u64, 0u64);
        let mut reported = 0u64;
        loop {
            // Per page rather than per pass. `start()` does not serialise
            // against itself, so a second one can leave this walk running
            // detached, and a pass is not a unit of time here — the 牌谱屋 sweep
            // is half a billion games at a rate-limited pace, so a superseded
            // walk checking only between passes would go on spending the
            // account pool for as long as the deployment lives.
            if self.generation.load(Ordering::SeqCst) != generation {
                return Ok((scanned, replaced));
            }
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
            // Counted as each record lands, not once the page has. Collecting the
            // page first meant `PAGE_SIZE` fetches — minutes at a rate-limited
            // pace — during which the console polled a progress card that still
            // read zero, which is indistinguishable from a stalled walk.
            let mut outcomes = futures_util::stream::iter(page)
                .map(|row| self.one_record(row))
                .buffer_unordered(in_flight);
            while let Some(outcome) = outcomes.next().await {
                let outcome = outcome?;
                scanned += 1;
                self.rate.write().hit();
                {
                    let mut progress = self.progress.write();
                    progress.scanned += 1;
                    match outcome {
                        Outcome::Replaced => {
                            replaced += 1;
                            progress.replaced += 1;
                        }
                        // The repair walk treats the two the same: the record
                        // keeps its `pb_size = 0` row either way, so the next
                        // pass finds it again without anything being remembered.
                        Outcome::Refused | Outcome::Unserved => progress.refused += 1,
                        Outcome::Unreadable => progress.unreadable += 1,
                        Outcome::Unconvertible(why) => {
                            progress.unconvertible += 1;
                            progress.unconvertible_by.bump(why);
                        }
                        // The repair walk replaces a row it already found, so
                        // it never claims and never meets this.
                        Outcome::Duplicate => progress.duplicates += 1,
                    }
                }
                if scanned - reported >= PROGRESS_EVERY {
                    reported = scanned;
                    let progress = *self.progress.read();
                    self.report(
                        WatchLogLevel::Info,
                        format!(
                            "补抓中：扫描 {} 条，替换 {} 条，雀魂拒绝 {} 条，读不到字节 {} 条，无法替换 {} 条{}",
                            progress.scanned,
                            progress.replaced,
                            progress.refused,
                            progress.unreadable,
                            progress.unconvertible,
                            match progress.unconvertible_by.summary() {
                                s if s.is_empty() => String::new(),
                                s => format!("（{s}）"),
                            }
                        ),
                    );
                }
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok((scanned, replaced)),
            }
        }
    }

    /// One walk over the 牌谱屋 catalogue: page it in its own order, drop the
    /// games this corpus already holds, fetch what is left.
    ///
    /// The cursor is durable and the page is the unit of commitment. It advances
    /// to the last row of the page — held or fetched or refused alike — only
    /// after that page's fetches have finished, so a restart re-walks at most one
    /// page and the claim check makes re-walking it free. Reaching the end of the
    /// catalogue clears it, which is what makes the next pass retry everything
    /// Mahjong Soul would not serve this time.
    ///
    /// A page nobody answered is the exception, and the distinction is the whole
    /// reason `RefetchError` has two arms. Refused means Mahjong Soul answered
    /// and would not give the game: an answer, and the cursor moves on. Unserved
    /// means nothing answered at all, which says nothing about the game — and
    /// unlike the pb repair, whose work is re-derived from `pb_size = 0` on every
    /// pass, nothing here would ever record that those games were skipped. So the
    /// pass stops where it is and leaves the cursor for the next one.
    async fn one_paipuya_pass(&self, generation: u64) -> anyhow::Result<(u64, u64)> {
        let catalog = &self.dependencies.catalog;
        let mut cursor = catalog.refetch_cursor(PAIPUYA_WALK).await?.or_else(|| {
            // A seed, not a filter: an empty uuid sorts below every real one, so
            // this is the position just before the first game of that second.
            self.config()
                .paipuya_from
                .map(|started_at| crate::catalog::PaipuyaPosition {
                    started_at,
                    uuid: String::new(),
                })
        });
        let (mut scanned, mut replaced) = (0u64, 0u64);
        let mut reported = 0u64;
        loop {
            if self.generation.load(Ordering::SeqCst) != generation {
                return Ok((scanned, replaced));
            }
            let page = catalog.paipuya_listings(cursor.as_ref(), PAGE_SIZE).await?;
            let Some(last) = page.last().map(|listing| listing.position.clone()) else {
                // The end of the catalogue. Back to the beginning, so the next
                // pass asks Mahjong Soul again for whatever it refused.
                catalog.clear_refetch_cursor(PAIPUYA_WALK).await?;
                return Ok((scanned, replaced));
            };
            scanned += page.len() as u64;

            // One indexed lookup for the whole page, before a single Mahjong
            // Soul request is spent. This is the comparison that matters: the
            // console's card answers "how big is the gap" by start time and
            // player names, which is the right question for a human and the
            // wrong one to bet an account's rate limit on, because a renamed
            // player or a rounded second reads as missing. A game uuid does not
            // drift.
            let uuids: Vec<String> = page
                .iter()
                .map(|listing| listing.position.uuid.clone())
                .collect();
            let hashes: Vec<Vec<u8>> = uuids.iter().map(|uuid| game_claim_hash(uuid)).collect();
            let held = catalog.claimed_games(&hashes).await?;
            let wanted = unclaimed(uuids, &held);
            {
                let mut progress = self.progress.write();
                progress.scanned += page.len() as u64;
                progress.present += (page.len() - wanted.len()) as u64;
                progress.position = Some(last.started_at);
            }

            let in_flight = self.runtime.read().workers.max(1);
            let outcomes = futures_util::stream::iter(wanted)
                .map(|uuid| self.one_game(uuid))
                .buffer_unordered(in_flight)
                .collect::<Vec<_>>()
                .await;
            let mut unserved = 0u64;
            {
                let mut progress = self.progress.write();
                for outcome in outcomes {
                    match outcome? {
                        Outcome::Replaced => {
                            replaced += 1;
                            progress.replaced += 1;
                        }
                        Outcome::Duplicate => progress.duplicates += 1,
                        Outcome::Refused => progress.refused += 1,
                        Outcome::Unserved => {
                            unserved += 1;
                            progress.refused += 1;
                        }
                        Outcome::Unconvertible(why) => {
                            progress.unconvertible += 1;
                            progress.unconvertible_by.bump(why);
                        }
                        // No stored bytes are read on this path.
                        Outcome::Unreadable => progress.unreadable += 1,
                    }
                }
            }
            if unserved > 0 {
                // Left where it was, so the next pass asks these again. Every
                // game on the page that did land is claimed by now, so walking
                // it a second time costs one indexed lookup and no request.
                self.report(
                    WatchLogLevel::Warn,
                    format!(
                        "有 {unserved} 局没有会话接手，本页不推进游标，本轮到此为止；下一轮从同一处再走"
                    ),
                );
                return Ok((scanned, replaced));
            }
            // After the page's fetches, never before: a cursor that ran ahead of
            // them would skip whatever was in flight when the process stopped.
            catalog.set_refetch_cursor(PAIPUYA_WALK, &last).await?;
            cursor = Some(last);

            if scanned - reported >= PROGRESS_EVERY {
                reported = scanned;
                let progress = *self.progress.read();
                self.report(
                    WatchLogLevel::Info,
                    format!(
                        "牌谱屋补抓中：走查 {} 局，本地已有 {} 局，入库 {} 局，雀魂拒绝 {} 局，转换失败 {} 局（已走到 {}）",
                        progress.scanned,
                        progress.present,
                        progress.replaced,
                        progress.refused,
                        progress.unconvertible,
                        progress
                            .position
                            .map(|at| at.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_default()
                    ),
                );
            }
        }
    }

    /// One catalogued game this corpus has never stored: fetch it, convert it,
    /// ingest it as a new record.
    ///
    /// `ingest_one`, never `reindex_one`. There is no row to replace, so the
    /// record needs a claim — and the claim is the only thing anywhere that says
    /// this game has been stored, because the uuid is not a column of the index.
    /// `reindex_one` takes none, so using it here would put a second row in the
    /// index for every game the sweep ever met twice, with `record_id` in the
    /// sorting key and nothing able to collapse them.
    async fn one_game(&self, uuid: String) -> anyhow::Result<Outcome> {
        let pb = match self.dependencies.broker.fetch(&uuid).await {
            Ok(pb) => pb,
            Err(RefetchError::Unserved) => {
                if self.sessions.load(Ordering::Relaxed) == 0 {
                    anyhow::bail!(
                        "没有会话接手补抓请求（等了 {} 秒，当前登录会话为 0）",
                        CLAIM_TIMEOUT.as_secs()
                    );
                }
                tracing::info!(%uuid, "补抓请求超时，留给下一轮");
                return Ok(Outcome::Unserved);
            }
            Err(RefetchError::Refused(why)) => {
                tracing::info!(%uuid, %why, "雀魂没有提供这局的牌谱");
                return Ok(Outcome::Refused);
            }
        };
        // Off the async runtime for the same reason `one_record` is: a
        // conversion decodes a protobuf, walks every event and gzips the result.
        let expected = uuid.clone();
        let (pb, converted) = tokio::task::spawn_blocking(move || {
            let converted = converted_fresh(&pb, &expected);
            (pb, converted)
        })
        .await?;
        let Some(mjai) = converted else {
            // No stored record to compare against on this path, so the only way
            // here is the converter refusing what Mahjong Soul returned.
            let rejected =
                Rejected::new(Unconvertible::ConvertFailed, "转换器读不了雀魂返回的这局");
            self.note_failure(uuid.clone(), &rejected);
            return Ok(Outcome::Unconvertible(rejected.why));
        };

        loop {
            // Behind the same ceiling live ingest answers `503` on. Waiting is
            // free here: the backlog is somebody else's work being done, and the
            // records at risk if the topic overruns are the collectors' — the
            // ones nobody can fetch a second time.
            while self.dependencies.kafka.lag() >= self.dependencies.kafka.max_lag() / 2 {
                tracing::info!("打包队列积压，补抓让出写入位置");
                tokio::time::sleep(BACKLOG_BACKOFF).await;
            }
            match indexer::ingest_one(
                &self.dependencies.catalog,
                &self.dependencies.kafka,
                PAIPUYA_SOURCE,
                &uuid,
                None,
                &mjai,
                Some(&pb),
            )
            .await
            {
                Ok(accepted) if accepted.duplicate => return Ok(Outcome::Duplicate),
                Ok(_) => return Ok(Outcome::Replaced),
                // The gate above reads the same sampled lag `claim` does, so
                // this only happens when the backlog crossed the whole distance
                // from half the ceiling to the ceiling inside one sample. The
                // protobuf is already paid for and still in hand; failing the
                // run over a number that moves on its own would throw it away.
                Err(indexer::IngestError::Backlogged(lag)) => {
                    tracing::info!(lag, "入库时打包队列已经到顶，等一轮再试");
                    tokio::time::sleep(BACKLOG_BACKOFF).await;
                }
                Err(error) => return Err(error.into()),
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
            // Also counted as 「无法替换」, but it never even reached Mahjong Soul:
            // the stored bytes carry no `start_game.majsoul.uuid` to ask for.
            tracing::info!(record = %row.id, "补抓：库里这条读不出 majsoul uuid，没法去抓");
            let rejected = Rejected::new(Unconvertible::NoUuid, "start_game 里没有 majsoul.uuid");
            self.note_failure(row.id.to_string(), &rejected);
            return Ok(Outcome::Unconvertible(rejected.why));
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
                return Ok(Outcome::Unserved);
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
        let mjai = match converted {
            Ok(mjai) => mjai,
            Err(rejected) => {
                self.note_failure(uuid.clone(), &rejected);
                return Ok(Outcome::Unconvertible(rejected.why));
            }
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

/// Why a fetch that arrived intact still could not replace the stored row.
/// They all add up to the console's 「无法替换」, but they mean different things:
/// All four are faults. `Truncated` used to be `NotBetter` and used to be the
/// benign one; see `reconverted` for why that reading was backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unconvertible {
    /// The stored record carries no `start_game.majsoul.uuid`, so there was
    /// nothing to ask Mahjong Soul for. Never even left the process.
    NoUuid,
    /// The converter could not read the protobuf Mahjong Soul returned.
    ConvertFailed,
    /// What came back was a different game than the uuid asked for.
    WrongGame,
    /// The re-conversion is not a whole game: fewer hands than the stored
    /// record, or no `end_game` to close it. Was `NotBetter` and meant "shorter"
    /// — see `reconverted` for why shorter is normal and shape is what matters.
    Truncated,
}

/// A rejection with the detail that made it one, so the console can show the
/// actual reason rather than only its category.
pub struct Rejected {
    pub why: Unconvertible,
    pub detail: String,
}

impl Rejected {
    fn new(why: Unconvertible, detail: impl Into<String>) -> Self {
        Self {
            why,
            detail: detail.into(),
        }
    }
}

/// One rejected record, kept for the console's list. Bounded — this is a
/// display buffer, not a record of what happened; the log has that.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RefetchFailure {
    pub at: DateTime<Utc>,
    /// The game uuid when it is known, the record id when it is not.
    pub subject: String,
    pub why: Unconvertible,
    pub label: &'static str,
    pub detail: String,
}

/// Completion instants over a short window, for the console's live rate. A
/// window rather than "since last poll", so two browsers watching do not each
/// see a rate the other consumed.
#[derive(Default)]
struct RateWindow {
    hits: std::collections::VecDeque<std::time::Instant>,
}

impl RateWindow {
    const WINDOW: std::time::Duration = std::time::Duration::from_secs(30);
    const CAP: usize = 4_096;

    fn hit(&mut self) {
        let now = std::time::Instant::now();
        self.hits.push_back(now);
        while self.hits.len() > Self::CAP
            || self
                .hits
                .front()
                .is_some_and(|at| now.duration_since(*at) > Self::WINDOW)
        {
            self.hits.pop_front();
        }
    }

    /// Records per second over the window. Zero once the walk stops, because
    /// everything in the deque ages out of it.
    fn qps(&self) -> f64 {
        let now = std::time::Instant::now();
        let live = self
            .hits
            .iter()
            .filter(|at| now.duration_since(**at) <= Self::WINDOW)
            .count();
        if live < 2 {
            return 0.0;
        }
        // Against the window, not against first-to-last: the latter reads as a
        // full rate when only two records landed in thirty seconds.
        live as f64 / Self::WINDOW.as_secs_f64()
    }
}

impl Unconvertible {
    fn label(self) -> &'static str {
        match self {
            Self::NoUuid => "库里读不出 uuid",
            Self::ConvertFailed => "转换器读不了",
            Self::WrongGame => "抓回来是另一局",
            Self::Truncated => "重转的不完整",
        }
    }
}

/// The 「无法替换」 counter split by cause, in the order they can happen.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UnconvertibleCounts {
    pub no_uuid: u64,
    pub convert_failed: u64,
    pub wrong_game: u64,
    pub truncated: u64,
}

impl UnconvertibleCounts {
    fn bump(&mut self, why: Unconvertible) {
        let slot = match why {
            Unconvertible::NoUuid => &mut self.no_uuid,
            Unconvertible::ConvertFailed => &mut self.convert_failed,
            Unconvertible::WrongGame => &mut self.wrong_game,
            Unconvertible::Truncated => &mut self.truncated,
        };
        *slot += 1;
    }

    /// Only the causes that actually happened, for the progress line.
    fn summary(self) -> String {
        [
            (Unconvertible::NoUuid, self.no_uuid),
            (Unconvertible::ConvertFailed, self.convert_failed),
            (Unconvertible::WrongGame, self.wrong_game),
            (Unconvertible::Truncated, self.truncated),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(why, n)| format!("{} {n} 条", why.label()))
        .collect::<Vec<_>>()
        .join("，")
    }
}

enum Outcome {
    Replaced,
    Refused,
    Unreadable,
    Unconvertible(Unconvertible),
    /// A game fetched and then found already claimed. Only the 牌谱屋 walk can
    /// produce it, and only when a claim appeared after its page was compared.
    Duplicate,
    /// Nothing answered the request. Counted with the refusals on the console —
    /// an operator wants one "did not get it" number — but kept apart here,
    /// because it is the one outcome that says nothing about the game and so is
    /// the one the 牌谱屋 walk must not move its cursor past.
    Unserved,
}

/// The games in a catalogue page worth spending a Mahjong Soul request on:
/// everything whose game-scoped claim is not already in `held`.
///
/// Kept apart from the page read and the lookup because this is the decision the
/// whole arrangement exists to make, and it is one an integration test could not
/// pin: `held` is what PostgreSQL answered, and getting the filter backwards
/// would look exactly like a corpus that is missing everything.
fn unclaimed(uuids: Vec<String>, held: &HashSet<Vec<u8>>) -> Vec<String> {
    uuids
        .into_iter()
        .filter(|uuid| !held.contains(&game_claim_hash(uuid)))
        .collect()
}

/// A protobuf fetched by uuid alone, converted with today's converter, or `None`
/// if it is not safe to store.
///
/// `None` for the header, deliberately: this game has never been in the index,
/// so there is no stored row to take one from, and since the decoder stopped
/// skipping `head.config` the protobuf says which mode it was — which is what
/// keeps the record from landing with an empty `rule` and vanishing from every
/// query that filters on one.
///
/// The identity check is the same one the repair walk makes and for the same
/// reason: the request travelled through a queue and, where an external module
/// is in use, through a subprocess that was handed the uuid and trusted with it.
/// Here a mismatch would not overwrite anything — it would store somebody else's
/// game under a claim naming this one, and neither would ever be fetched again.
fn converted_fresh(pb: &[u8], expected_uuid: &str) -> Option<Vec<u8>> {
    use std::io::Read;

    let (uuid, compressed) = crate::majsoul::convert::convert_record_bytes(pb, None).ok()?;
    if uuid != expected_uuid {
        tracing::error!(%uuid, %expected_uuid, "补抓拿回来的是另一局，拒绝入库");
        return None;
    }
    let mut mjai = Vec::new();
    flate2::read::GzDecoder::new(compressed.as_slice())
        .read_to_end(&mut mjai)
        .ok()?;
    Some(mjai)
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
pub(crate) fn majsoul_header(raw: &[u8]) -> Option<(String, GameMetadata)> {
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
/// The second is that it has to be a whole game. This used to be an event count
/// — `fresh >= before` — on the reasoning that the fixed converter adds fields
/// to events rather than events. That reasoning was wrong, and the guard was
/// rejecting exactly the records that most needed replacing: `5a09d6b` stopped
/// emitting `reach_accepted` when a riichi declaration tile is ronned, because
/// the riichi never completes and no stick is placed. Every record carrying that
/// bug is one event longer than its own repair, so the count guard kept the
/// version whose scores are wrong for the rest of the hand. Sampling 56
/// unreplaced records from one day found 6 of them, which matches the share the
/// console was reporting as 「重转的还不如原来的」.
///
/// So the guard checks shape instead, which is what "truncated" actually means:
/// the same number of hands, and an `end_game` to close it. A partial paipu or a
/// conversion that gave up halfway fails both. Getting shorter does not.
pub(crate) fn reconverted(
    pb: &[u8],
    metadata: &GameMetadata,
    stored: &[u8],
    expected_uuid: &str,
) -> Result<Vec<u8>, Rejected> {
    use std::io::Read;

    // Every rejection carries its cause out: they all land in the console as one
    // 「无法替换」 counter, and without the split there is no way to tell a
    // converter that choked from a replacement that was simply not an improvement.
    let (uuid, compressed) = match crate::majsoul::convert::convert_record_bytes(pb, Some(metadata))
    {
        Ok(converted) => converted,
        Err(error) => {
            tracing::info!(%expected_uuid, %error, "补抓：转换器读不了雀魂返回的这局");
            return Err(Rejected::new(
                Unconvertible::ConvertFailed,
                error.to_string(),
            ));
        }
    };
    if uuid != expected_uuid {
        tracing::error!(%uuid, %expected_uuid, "补抓拿回来的是另一局，拒绝替换");
        return Err(Rejected::new(
            Unconvertible::WrongGame,
            format!("要的是 {expected_uuid}，回来的是 {uuid}"),
        ));
    }
    let mut mjai = Vec::new();
    if let Err(error) = flate2::read::GzDecoder::new(compressed.as_slice()).read_to_end(&mut mjai) {
        tracing::info!(%expected_uuid, %error, "补抓：重转结果解压失败");
        return Err(Rejected::new(
            Unconvertible::ConvertFailed,
            format!("解压失败：{error}"),
        ));
    }
    let fresh_events = match mjai::events(&mjai) {
        Ok(events) => events,
        Err(error) => {
            tracing::info!(%expected_uuid, %error, "补抓：重转结果解析不出事件");
            return Err(Rejected::new(
                Unconvertible::ConvertFailed,
                format!("重转结果解析不出事件：{error}"),
            ));
        }
    };
    let before_events = match mjai::events(stored) {
        Ok(events) => events,
        Err(error) => {
            tracing::info!(%expected_uuid, %error, "补抓：库里那条本身就解析不出事件");
            return Err(Rejected::new(
                Unconvertible::ConvertFailed,
                format!("库里那条解析不出事件：{error}"),
            ));
        }
    };
    let (fresh, before) = (fresh_events.len(), before_events.len());
    let kind = |event: &serde_json::Value| {
        event
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_owned()
    };
    let hands =
        |events: &[serde_json::Value]| events.iter().filter(|e| kind(e) == "end_kyoku").count();
    let (fresh_hands, before_hands) = (hands(&fresh_events), hands(&before_events));
    let closed = fresh_events.last().map(&kind).as_deref() == Some("end_game");
    if fresh_hands != before_hands || !closed {
        // Not a failure — the guard doing its job. Replacing here would lose
        // events the stored record still has.
        tracing::info!(%expected_uuid, fresh, before, "补抓：重转的事件数比库里那条还少，保留原样");
        // Where they first disagree, not just how many there are: the count
        // alone cannot say whether the converter dropped something or the stored
        // record carried something it should not have.
        let at = fresh_events
            .iter()
            .zip(&before_events)
            .position(|(a, b)| a != b);
        let detail = match at {
            // Diverged mid-stream: the stored event at that index is the one the
            // re-conversion did not produce there.
            Some(i) => format!(
                "重转 {fresh} 个事件，库里那条有 {before} 个；第 {i} 个起就不一样了，库里是 {}，重转是 {}",
                kind(&before_events[i]),
                fresh_events
                    .get(i)
                    .map(kind)
                    .unwrap_or_else(|| "（没有）".into()),
            ),
            // Identical prefix, so the whole difference is at the tail.
            None => format!(
                "重转 {fresh} 个事件，库里那条有 {before} 个；前 {fresh} 个完全一致，库里末尾多出 {}",
                before_events[fresh..]
                    .iter()
                    .map(kind)
                    .collect::<Vec<_>>()
                    .join("、"),
            ),
        };
        return Err(Rejected::new(Unconvertible::Truncated, detail));
    }
    Ok(mjai)
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

    /// The default has to name the place the accounts are actually kept.
    ///
    /// It named a `file:` path nothing mounts, which meant every deployment
    /// that filled in the console's account pool and pressed 启动 got
    /// `No such file or directory` — with the accounts one field away the whole
    /// time.
    #[test]
    fn the_default_reads_the_console_account_pool() {
        assert_eq!(
            RefetchServiceConfig::default().account_secret_ref,
            "pool:refetch"
        );

        // A deployment still carrying the old default moves with it...
        let mut stored = RefetchServiceConfig {
            account_secret_ref: LEGACY_SECRET_REF.into(),
            ..RefetchServiceConfig::default()
        };
        stored.revision = 7;
        assert!(migrate_legacy_secret_ref(&mut stored, false));
        assert_eq!(stored.account_secret_ref, "pool:refetch");
        // The revision moves with it, so a console page opened before the
        // restart cannot save its stale copy back over the migration without
        // being told the document changed.
        assert_eq!(stored.revision, 8);

        // ...unless it did mount that file, in which case the file is the
        // configuration and the accounts in it need not be the pool's.
        let mut mounted = RefetchServiceConfig {
            account_secret_ref: LEGACY_SECRET_REF.into(),
            ..RefetchServiceConfig::default()
        };
        assert!(!migrate_legacy_secret_ref(&mut mounted, true));
        assert_eq!(mounted.account_secret_ref, LEGACY_SECRET_REF);

        // Anything typed by hand is left alone, missing file or not: an
        // operator who wrote a path meant that path.
        let mut typed = RefetchServiceConfig {
            account_secret_ref: "file:/srv/mjai/accounts.txt".into(),
            ..RefetchServiceConfig::default()
        };
        assert!(!migrate_legacy_secret_ref(&mut typed, false));
        assert_eq!(typed.account_secret_ref, "file:/srv/mjai/accounts.txt");
    }

    #[test]
    fn rejects_a_plaintext_secret_and_out_of_range_knobs() {
        let plaintext = RefetchServiceConfig {
            account_secret_ref: "user,password".into(),
            ..valid()
        };
        assert!(plaintext.validate().is_err());

        assert!(
            RefetchServiceConfig {
                concurrency: 0,
                ..valid()
            }
            .validate()
            .is_err(),
            "concurrency 0 was accepted"
        );
        // And nothing above it is refused any more. The ceiling that used to sit
        // here was a typo guard, and it cost a pool of eighty accounts sixty-four
        // idle sessions; what bounds the sessions is `min(concurrency, accounts)`
        // at the point they are opened.
        for concurrency in [17, 64, 500] {
            let config = RefetchServiceConfig {
                concurrency,
                ..valid()
            };
            assert!(
                config.validate().is_ok(),
                "concurrency {concurrency} was refused"
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

    /// A configuration written by an older build has no `proxy_mode` and no
    /// `work`, and one written by hand may leave either out.
    ///
    /// The second of those is the one that matters. A deployment running the
    /// repair walk against a bounded backlog must not come back from an upgrade
    /// sweeping half a billion catalogued games with the same accounts.
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
        assert_eq!(config.work, RefetchWork::MissingPb);
        config.validate().unwrap();

        // And the console's spelling of the other one round-trips.
        let chosen: RefetchServiceConfig = serde_json::from_value(serde_json::json!({
            "revision": 4,
            "enabled": true,
            "work": "paipuya_gap",
            "server": "cn",
            "custom_proxy_url": null,
            "account_secret_ref": "env:POOL",
            "concurrency": 3,
            "request_delay_ms": 2000,
            "client_version": null,
        }))
        .unwrap();
        assert_eq!(chosen.work, RefetchWork::PaipuyaGap);
    }

    /// The decision that spends an account's rate limit.
    ///
    /// A game whose claim is already in PostgreSQL is one this corpus has
    /// stored, and asking Mahjong Soul for it again costs a request and returns
    /// a record that `ingest_one` will only answer `duplicate` to. Inverting
    /// this filter would look, from the console, exactly like a corpus missing
    /// everything 牌谱屋 lists.
    #[test]
    fn only_the_games_with_no_claim_are_worth_a_request() {
        let uuids: Vec<String> = ["260716-a", "260716-b", "260716-c"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let held: HashSet<Vec<u8>> = [game_claim_hash("260716-b")].into_iter().collect();

        assert_eq!(unclaimed(uuids.clone(), &held), ["260716-a", "260716-c"]);
        // Nothing held is everything wanted, and everything held is nothing.
        assert_eq!(unclaimed(uuids.clone(), &HashSet::new()), uuids);
        let all: HashSet<Vec<u8>> = uuids.iter().map(|uuid| game_claim_hash(uuid)).collect();
        assert!(unclaimed(uuids, &all).is_empty());
    }

    /// The guard that used to be `fresh >= before`. A repair is allowed to be
    /// shorter — `5a09d6b` removed a `reach_accepted` that should never have
    /// been emitted — so what it checks is that the game is whole.
    #[test]
    fn shorter_but_whole_is_accepted_truncated_is_not() {
        fn game(hands: usize, extra_reach: usize, closed: bool) -> Vec<u8> {
            let mut lines = vec![r#"{"type":"start_game"}"#.to_owned()];
            for _ in 0..hands {
                lines.push(r#"{"type":"start_kyoku"}"#.to_owned());
                for _ in 0..extra_reach {
                    lines.push(r#"{"type":"reach_accepted","actor":0}"#.to_owned());
                }
                lines.push(r#"{"type":"hora","actor":0,"target":1}"#.to_owned());
                lines.push(r#"{"type":"end_kyoku"}"#.to_owned());
            }
            if closed {
                lines.push(r#"{"type":"end_game"}"#.to_owned());
            }
            lines.join("\n").into_bytes()
        }

        let stored = game(4, 1, true); // the buggy record: one extra reach_accepted per hand
        let repaired = game(4, 0, true); // today's converter: same hands, four events shorter
        let truncated = game(2, 0, true); // half the game
        let unclosed = game(4, 0, false); // conversion gave up before the settlement

        let hands = |bytes: &[u8]| {
            mjai::events(bytes)
                .unwrap()
                .iter()
                .filter(|e| e["type"] == "end_kyoku")
                .count()
        };
        let closed =
            |bytes: &[u8]| mjai::events(bytes).unwrap().last().unwrap()["type"] == "end_game";

        // Shorter than what is stored, and accepted: this is the case the old
        // count guard rejected, and it is the one that matters.
        assert!(mjai::events(&repaired).unwrap().len() < mjai::events(&stored).unwrap().len());
        assert_eq!(hands(&repaired), hands(&stored));
        assert!(closed(&repaired));

        assert_ne!(hands(&truncated), hands(&stored));
        assert!(!closed(&unclosed));
    }
}
