//! Syncing the 牌谱屋 (amae-koromo) game catalogue.
//!
//! It lists games; it does not hold them. What comes back here is one line per
//! game — uuid, mode, start and end, and the four seats — and that is enough to
//! answer the only question worth asking before spending a Mahjong Soul
//! request: does this corpus already have that game. `Catalog::paipuya_gap`
//! answers it by start time and player names, so the expensive half never runs
//! for a game already collected.
//!
//! The whole module is one rate limit with a paging loop around it. 牌谱屋 is
//! somebody else's service, run for free, and its own limiter says what it
//! thinks of scripted callers: five requests a minute for anything identifying
//! as a client library, and, more recently, a captcha gate that answers one
//! request a day without a key. So the pacing here is a hard global ceiling
//! rather than a per-worker sleep — several workers may be in flight at once,
//! but between any two requests leaving this process there is a full interval,
//! whichever worker sent them.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, sleep_until},
};

use crate::{
    catalog::{Catalog, PaipuyaGame},
    majsoul::modes::mode_metadata,
    watch_log::{WatchLogBuffer, WatchLogLevel},
    watch_service::{
        ServicePhase, WatchAction, WatchServiceError, persist_json, redacted_secret,
        restored_secret,
    },
};

/// The ranked modes 牌谱屋 covers, which is exactly the set `mode_metadata`
/// knows: gold, jade and throne, east and south, four players and three. It
/// carries no bronze and no friend rooms.
const ALL_MODES: [i32; 12] = [8, 9, 11, 12, 15, 16, 21, 22, 23, 24, 25, 26];

/// The site's own ceiling. Asking for more is silently clamped to it, so
/// sending more is only a way to make the arithmetic here disagree with what
/// comes back.
const MAX_PAGE: usize = 500;

/// The earliest game 牌谱屋 has. Starting before it costs one empty page per
/// mode and nothing else, but starting *after* it silently skips history.
const CATALOGUE_START: &str = "2019-08-23T00:00:00Z";

/// What identifies this deployment to 牌谱屋.
///
/// An honest name with a way to get in touch, because the operator of a service
/// being swept has every right to know who is doing it and to say stop. The
/// site's limiter does look at this header — dressing the request up as a
/// browser would buy a higher rate, which is exactly why it is not done here.
const USER_AGENT: &str =
    "mjai-management (+https://github.com/AyasukaRan/majsoulmanager; corpus sync)";

const LOG_SOURCE: &str = "paipuya";

/// How many catalogued games between two progress lines. One page is at most
/// 500, so this is a line every twenty pages or so — often enough to see the
/// sweep moving, rare enough not to drown the log a human reads for warnings.
const PROGRESS_EVERY: u64 = 10_000;

/// Ceilings on the two knobs an operator sets.
const MAX_CONCURRENCY: usize = 8;
const MIN_REQUEST_INTERVAL_MS: u64 = 200;

/// A hard ceiling on how often requests leave this process, shared by every
/// worker.
///
/// The point is that concurrency and rate are separate settings. Several
/// requests may be in flight at once — which is what stops one slow response
/// from wasting the allowance — but no two of them *leave* within an interval
/// of each other, whichever worker sent them. A per-worker sleep cannot do
/// this: eight workers sleeping a second each send eight requests a second.
///
/// The wait happens with the lock held, and that is the whole guarantee. An
/// earlier version handed out slots under the lock and waited outside it, which
/// gives the right long-run *average* and nothing more: a worker whose wake-up
/// was delayed past the next worker's slot would have the two requests leave
/// milliseconds apart. Its own test caught that. Holding the lock costs nothing
/// here, because the request itself is sent after `tick` returns.
pub struct Pacer {
    interval: Duration,
    /// The earliest moment the next request may leave.
    next: Mutex<Instant>,
}

impl Pacer {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            next: Mutex::new(Instant::now()),
        }
    }

    /// Returns when this caller may send. Callers queue in arrival order.
    pub async fn tick(&self) {
        let mut next = self.next.lock().await;
        if *next > Instant::now() {
            sleep_until(*next).await;
        }
        // Measured from the moment the caller is actually released rather than
        // from the slot it was promised, so a late wake-up delays the next
        // request instead of being made up for by a burst.
        *next = Instant::now() + self.interval;
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PaipuyaConfig {
    pub revision: u64,
    pub enabled: bool,
    /// Where the API lives, without a trailing path. The site publishes several
    /// mirrors and they are not interchangeable in speed.
    pub base_url: String,
    /// The key 牌谱屋 hands out to people sweeping the data, sent verbatim as
    /// the value of `api_key_header`. Stored here rather than behind a `file:`
    /// reference because an operator pastes it into the console once; it is
    /// never served back — see [`PaipuyaConfig::published`].
    pub api_key: Option<String>,
    /// Which header carries it. `authorization` is what the site's own client
    /// uses, with a `Bearer ` prefix that belongs in the value.
    pub api_key_header: String,
    /// Modes to sweep. Empty means all twelve.
    pub modes: Vec<i32>,
    /// The left edge of the window to sweep.
    ///
    /// A floor, not just a seed: a mode whose cursor sits before it is moved
    /// forward to it. Otherwise moving this later would do nothing on any
    /// deployment that had already swept — the cursor would win every time, and
    /// the field would look like a setting while being a one-off.
    ///
    /// Moving it *earlier* still needs the cursors cleared, because a cursor
    /// past a game is the only record that the game was already asked for.
    /// `POST /api/v1/paipuya/cursors/reset` does that, deliberately as its own
    /// action: re-sweeping is thousands of requests against somebody else's
    /// free service, which is not something a config save should do quietly.
    pub start_from: DateTime<Utc>,
    /// The right edge, or `None` for "keep up with 牌谱屋 forever".
    ///
    /// With one set, a mode stops when its cursor reaches it, and the sweep is
    /// a job that finishes rather than a service that runs. That is what makes
    /// a bounded window — one year, one month — something an operator can ask
    /// for and watch complete.
    #[serde(default)]
    pub sync_until: Option<DateTime<Utc>>,
    /// The floor on the gap between two requests leaving this process, counting
    /// every worker together.
    pub request_interval_ms: u64,
    /// How many modes are swept at once. It does not raise the request rate —
    /// `request_interval_ms` does that alone — it only stops one slow response
    /// from idling the allowance.
    pub concurrency: usize,
    pub page_size: usize,
}

impl Default for PaipuyaConfig {
    fn default() -> Self {
        Self {
            revision: 1,
            enabled: false,
            base_url: "https://5-data.amae-koromo.com".into(),
            api_key: None,
            api_key_header: "authorization".into(),
            modes: Vec::new(),
            start_from: CATALOGUE_START.parse().expect("a valid catalogue start"),
            sync_until: None,
            // One request per second, which is what this deployment asked for.
            // It is far above what the site grants an unkeyed caller, so
            // without a key the sync will spend most of its time being refused.
            request_interval_ms: 1_000,
            concurrency: 4,
            page_size: MAX_PAGE,
        }
    }
}

impl PaipuyaConfig {
    pub fn validate(&self) -> Result<(), WatchServiceError> {
        let url = reqwest::Url::parse(&self.base_url).map_err(|error| {
            WatchServiceError::InvalidConfig(format!("invalid base URL: {error}"))
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(WatchServiceError::InvalidConfig(
                "base URL must use http or https".into(),
            ));
        }
        if self.api_key_header.is_empty()
            || reqwest::header::HeaderName::try_from(self.api_key_header.as_str()).is_err()
        {
            return Err(WatchServiceError::InvalidConfig(
                "api_key_header is not a valid header name".into(),
            ));
        }
        // The site refuses a range narrower than ten seconds, so a window that
        // small is not a small window — it is a sweep that returns immediately
        // and looks finished.
        if let Some(until) = self.sync_until
            && (until - self.start_from).num_seconds() < 10
        {
            return Err(WatchServiceError::InvalidConfig(
                "结束时间要比开始时间至少晚 10 秒（牌谱屋不接受更窄的区间）".into(),
            ));
        }
        if let Some(mode) = self
            .modes
            .iter()
            .find(|mode| mode_metadata(**mode).is_err())
        {
            return Err(WatchServiceError::InvalidConfig(format!(
                "mode {mode} is not one of the ranked modes 牌谱屋 covers"
            )));
        }
        // A floor, not a ceiling: the danger here is asking too often, and this
        // is somebody else's service. An operator who wants it slower can have
        // any interval they like.
        if self.request_interval_ms < MIN_REQUEST_INTERVAL_MS {
            return Err(WatchServiceError::InvalidConfig(format!(
                "request_interval_ms must be at least {MIN_REQUEST_INTERVAL_MS}"
            )));
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.concurrency) {
            return Err(WatchServiceError::InvalidConfig(format!(
                "concurrency must be between 1 and {MAX_CONCURRENCY}"
            )));
        }
        if !(1..=MAX_PAGE).contains(&self.page_size) {
            return Err(WatchServiceError::InvalidConfig(format!(
                "page_size must be between 1 and {MAX_PAGE}"
            )));
        }
        Ok(())
    }

    /// The document as the API serves it. `GET` is open to every console
    /// member, so the key leaves as three asterisks and comes back by being
    /// handed straight in again.
    pub fn published(mut self) -> Self {
        self.api_key = redacted_secret(self.api_key.as_deref());
        self
    }

    fn selected_modes(&self) -> Vec<i32> {
        if self.modes.is_empty() {
            ALL_MODES.to_vec()
        } else {
            self.modes.clone()
        }
    }
}

/// `pl4` or `pl3` — which half of the site a mode lives in.
fn mode_path(mode: i32) -> &'static str {
    if mode < 20 { "pl4" } else { "pl3" }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PaipuyaProgress {
    /// Games written to the catalogue by this run.
    pub synced: u64,
    /// Games the site listed that carry no uuid, so nothing could ever fetch
    /// them. Counted rather than stored: a row with no uuid would be reported
    /// as missing for ever.
    pub without_uuid: u64,
    pub requests: u64,
    /// Requests the site refused, by status. `429` here means the rate is above
    /// what this caller is allowed.
    pub refused: u64,
    /// How far each mode has got.
    pub cursors: BTreeMap<i32, DateTime<Utc>>,
}

/// Where one mode stands, as the console shows it.
///
/// Read from PostgreSQL rather than from [`PaipuyaProgress::cursors`], which
/// only holds the modes that moved during *this* run and is emptied on every
/// start. A stopped deployment answering "nothing" to "how far did you get" is
/// the thing this exists to avoid.
#[derive(Clone, Debug, Serialize)]
pub struct PaipuyaModeProgress {
    pub mode_id: i32,
    /// One of the twelve `{players}p-{room}-{length}` tokens, which the console
    /// already has a Chinese label for — so the name lives in one place rather
    /// than being spelled out again here.
    pub rule: String,
    pub next_from: DateTime<Utc>,
    /// Games this mode has contributed across every run, not just this one.
    pub synced_games: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PaipuyaStatus {
    pub phase: ServicePhase,
    pub active_revision: Option<u64>,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub has_key: bool,
    pub progress: PaipuyaProgress,
    /// The window being swept, so the console can place a cursor in it without
    /// re-deriving what `sync_until` means when it is unset.
    pub window_start: DateTime<Utc>,
    /// `None` is "keep up with live play", which the console draws as now.
    pub window_end: Option<DateTime<Utc>>,
    /// This process's clock at the moment of the read.
    ///
    /// Sent rather than left to the browser because an unbounded window's right
    /// edge is *this* machine's now — the same clock the cursors are stamped
    /// against — and because a percentage computed from the reader's clock
    /// would drift with whatever the reader's clock is doing.
    pub now: DateTime<Utc>,
    pub modes: Vec<PaipuyaModeProgress>,
}

impl Default for PaipuyaStatus {
    fn default() -> Self {
        Self {
            phase: ServicePhase::Stopped,
            active_revision: None,
            updated_at: Utc::now(),
            last_error: None,
            has_key: false,
            progress: PaipuyaProgress::default(),
            window_start: CATALOGUE_START.parse().expect("a valid catalogue start"),
            window_end: None,
            now: Utc::now(),
            modes: Vec::new(),
        }
    }
}

/// Where a mode's next page starts: its own cursor, but never behind the
/// window.
///
/// The clamp is what makes 开始时间 a setting rather than a seed. Without it a
/// cursor always won, so moving the window forward changed nothing on any
/// deployment that had already swept — the box would accept the edit, save it,
/// and the sweep would carry on from 2019.
///
/// It is deliberately one-directional. A cursor *ahead* of the window is the
/// sweep's own progress and is kept; widening the window backwards needs the
/// cursors cleared, because re-reading years of catalogue is a cost somebody
/// should have to ask for.
fn sweep_from(cursor: Option<DateTime<Utc>>, start_from: DateTime<Utc>) -> DateTime<Utc> {
    cursor.map_or(start_from, |cursor| cursor.max(start_from))
}

/// The rule token for a mode, or a readable stand-in for one this build does
/// not know. Never an error: a status read must not fail over a label.
fn rule_token(mode: i32) -> String {
    match mode_metadata(mode) {
        Ok((room, length, players)) => format!("{players}p-{room}-{length}"),
        Err(_) => format!("mode-{mode}"),
    }
}

/// One page as the site answers it. Only the fields this needs; everything else
/// it sends is ignored rather than rejected, because the shape is not ours.
#[derive(Debug, Deserialize)]
struct ApiGame {
    #[serde(default)]
    uuid: String,
    #[serde(rename = "modeId")]
    mode_id: i32,
    #[serde(rename = "startTime")]
    start_time: i64,
    #[serde(rename = "endTime", default)]
    end_time: i64,
    #[serde(default)]
    players: Vec<ApiPlayer>,
}

#[derive(Debug, Deserialize)]
struct ApiPlayer {
    #[serde(rename = "accountId", default)]
    account_id: u64,
    #[serde(default)]
    nickname: String,
    #[serde(default)]
    score: i32,
}

impl ApiGame {
    fn into_game(self) -> Option<PaipuyaGame> {
        // Both are required for the comparison this catalogue exists to serve:
        // without a uuid nothing could fetch the game, and without players
        // nothing could recognise it as one already collected.
        if self.uuid.is_empty() || self.players.is_empty() {
            return None;
        }
        let at = |seconds: i64| Utc.timestamp_opt(seconds, 0).single();
        Some(PaipuyaGame {
            uuid: self.uuid,
            mode_id: self.mode_id,
            started_at: at(self.start_time)?,
            ended_at: at(self.end_time).unwrap_or(at(self.start_time)?),
            players: self.players.iter().map(|p| p.nickname.clone()).collect(),
            account_ids: self.players.iter().map(|p| p.account_id).collect(),
            scores: self.players.iter().map(|p| p.score).collect(),
        })
    }
}

pub struct PaipuyaDependencies {
    pub data_dir: std::path::PathBuf,
    pub catalog: Arc<Catalog>,
    pub logs: Arc<WatchLogBuffer>,
}

pub struct PaipuyaSupervisor {
    config_path: std::path::PathBuf,
    config: RwLock<PaipuyaConfig>,
    runtime: RwLock<PaipuyaStatus>,
    progress: RwLock<PaipuyaProgress>,
    dependencies: PaipuyaDependencies,
    generation: AtomicU64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl PaipuyaSupervisor {
    pub fn new(dependencies: PaipuyaDependencies) -> Result<Self, WatchServiceError> {
        let directory = dependencies.data_dir.join("paipuya");
        std::fs::create_dir_all(&directory)?;
        let config_path = directory.join("config.json");
        let config = match std::fs::read(&config_path) {
            Ok(bytes) => {
                let config: PaipuyaConfig = serde_json::from_slice(&bytes)?;
                config.validate()?;
                config
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let config = PaipuyaConfig::default();
                persist_json(&config_path, &config)?;
                config
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            config_path,
            config: RwLock::new(config),
            runtime: RwLock::new(PaipuyaStatus::default()),
            progress: RwLock::new(PaipuyaProgress::default()),
            dependencies,
            generation: AtomicU64::new(0),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub fn config(&self) -> PaipuyaConfig {
        self.config.read().clone()
    }

    /// The configuration with the key taken out, which is the only form that
    /// leaves the process.
    pub fn published_config(&self) -> PaipuyaConfig {
        self.config().published()
    }

    /// The stored status with the window and every mode's stored position
    /// folded in, so one console poll sees one consistent picture.
    ///
    /// The cursor read can fail — it is a database query on a page that is
    /// mostly about something else — and when it does the answer is the rest of
    /// the status rather than an error: the phase, the counters and the window
    /// are all still true, and a page that renders none of them because
    /// PostgreSQL hiccuped is worse than one missing its progress bars.
    pub async fn status(&self) -> PaipuyaStatus {
        let config = self.config();
        let mut status = self.runtime.read().clone();
        status.has_key = config.api_key.is_some();
        status.progress = self.progress.read().clone();
        status.window_start = config.start_from;
        status.window_end = config.sync_until;
        status.now = Utc::now();
        status.modes = match self.dependencies.catalog.paipuya_cursors().await {
            Ok(cursors) => cursors
                .into_iter()
                .map(|cursor| PaipuyaModeProgress {
                    mode_id: cursor.mode_id,
                    rule: rule_token(cursor.mode_id),
                    next_from: cursor.next_from,
                    synced_games: cursor.synced_games,
                    updated_at: cursor.updated_at,
                })
                .collect(),
            Err(error) => {
                tracing::warn!(%error, "读不到牌谱屋同步进度");
                Vec::new()
            }
        };
        status
    }

    /// Sends every mode back to the start of the window. The catalogue keeps
    /// its games and its totals; what moves is the position.
    ///
    /// Its own action rather than a side effect of saving the window, because
    /// re-sweeping is thousands of requests against somebody else's free
    /// service. It is needed whenever the window moves to somewhere a cursor
    /// has already passed — widening it backwards, or setting an 结束时间 in the
    /// past — because a cursor beyond the window makes every mode return with
    /// nothing to do.
    ///
    /// The sweep is stopped first, and it has to be: `sync_mode` reads its
    /// position once and then holds it in a local for the life of the mode, so
    /// a rewind landing under a running sweep is written straight back after
    /// the next page — for exactly the modes that were in flight, and not for
    /// the others. That is a half-applied rewind reporting success, which is
    /// worse than refusing.
    pub async fn reset_cursors(&self) -> Result<u64, WatchServiceError> {
        let running = self.runtime.read().phase != ServicePhase::Stopped;
        if running {
            self.stop().await;
        }
        let start_from = self.config().start_from;
        let moved = self
            .dependencies
            .catalog
            .rewind_paipuya_cursors(start_from)
            .await
            .map_err(|error| WatchServiceError::InvalidConfig(error.to_string()))?;
        self.report(
            WatchLogLevel::Warn,
            format!(
                "已把 {moved} 个模式的游标退回 {}{}",
                start_from.format("%Y-%m-%d %H:%M:%S"),
                if running {
                    "，同步已停下，重新点「开始同步」才会走"
                } else {
                    "，下次同步从这里重新走一遍"
                }
            ),
        );
        Ok(moved)
    }

    pub async fn update_config(
        self: &Arc<Self>,
        next: PaipuyaConfig,
    ) -> Result<PaipuyaConfig, WatchServiceError> {
        let mut next = next;
        next.validate()?;
        let submitted = next.revision;
        {
            let mut current = self.config.write();
            if submitted != current.revision {
                return Err(WatchServiceError::RevisionConflict {
                    submitted,
                    current: current.revision,
                });
            }
            // A console that read the document got asterisks and hands them
            // back on every save; without this the first save would overwrite
            // the key with them.
            next.api_key = restored_secret(next.api_key, current.api_key.as_deref());
            next.revision = current.revision.saturating_add(1);
            persist_json(&self.config_path, &next)?;
            *current = next.clone();
        }
        if next.enabled {
            self.start().await?;
        } else {
            self.stop().await;
        }
        Ok(next.published())
    }

    pub async fn apply_action(
        self: &Arc<Self>,
        action: WatchAction,
    ) -> Result<PaipuyaStatus, WatchServiceError> {
        match action {
            WatchAction::Start | WatchAction::Reload => self.start().await?,
            WatchAction::Stop => self.stop().await,
        }
        Ok(self.status().await)
    }

    pub async fn start_if_enabled(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        if self.config.read().enabled {
            self.start().await?;
        }
        Ok(())
    }

    async fn start(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        let config = self.config();
        self.stop_tasks().await;
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        *self.progress.write() = PaipuyaProgress::default();
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Running;
            runtime.active_revision = Some(config.revision);
            runtime.updated_at = Utc::now();
            runtime.last_error = None;
        }
        if config.api_key.is_none() {
            self.report(
                WatchLogLevel::Warn,
                "没有配置 API key。牌谱屋对没有 key 的调用方限得很死（实测一天一次），\
                 同步多半会一直被 429 拒绝；先向 SAPikachu 申请授权"
                    .to_owned(),
            );
        }
        // No proxy, and said rather than left to chance. 牌谱屋 is not Mahjong
        // Soul: it rate-limits by API key, it has never cared where a request
        // comes from, and the exits this deployment owns are a scarce thing the
        // re-fetch pool spends on Mahjong Soul. Sending a catalogue sweep
        // through one buys nothing and takes an address away from the half that
        // needs it.
        //
        // `no_proxy` rather than simply not setting one: reqwest reads
        // `HTTPS_PROXY` from the environment by default, so "does not use a
        // proxy" was true only for as long as nobody set that variable on the
        // container — and a deployment that set it to route something else
        // would have moved this sweep without anybody choosing to.
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| WatchServiceError::InvalidConfig(error.to_string()))?;
        let pacer = Arc::new(Pacer::new(Duration::from_millis(
            config.request_interval_ms,
        )));
        self.report(
            WatchLogLevel::Info,
            format!(
                "开始同步牌谱屋对局信息：{} 个模式，每 {}ms 一次请求（全局，不分并发），并发 {}",
                config.selected_modes().len(),
                config.request_interval_ms,
                config.concurrency
            ),
        );

        // One queue of modes drained by `concurrency` workers, rather than a
        // task per mode: the request rate is fixed by the pacer either way, and
        // this keeps the number of live HTTP connections to what was asked for.
        let queue = Arc::new(Mutex::new(config.selected_modes()));
        // Counted down rather than joined. The handles belong to `self.tasks`
        // so that `stop` can abort them, and a task that joined them would have
        // to take them away first. Whichever worker takes this to zero is the
        // one that saw the sweep end.
        let live = Arc::new(AtomicUsize::new(config.concurrency));
        let mut tasks = Vec::with_capacity(config.concurrency);
        for _ in 0..config.concurrency {
            let supervisor = Arc::clone(self);
            let config = config.clone();
            let client = client.clone();
            let pacer = Arc::clone(&pacer);
            let queue = Arc::clone(&queue);
            let live = Arc::clone(&live);
            tasks.push(tokio::spawn(async move {
                loop {
                    let Some(mode) = queue.lock().await.pop() else {
                        break;
                    };
                    if supervisor.generation.load(Ordering::SeqCst) != generation {
                        break;
                    }
                    if let Err(error) = supervisor.sync_mode(&config, &client, &pacer, mode).await {
                        supervisor.fail(&format!("模式 {mode} 同步中止：{error:#}"));
                        break;
                    }
                }
                if live.fetch_sub(1, Ordering::SeqCst) == 1 {
                    supervisor.complete(generation);
                }
            }));
        }
        *self.tasks.lock().await = tasks;
        Ok(())
    }

    async fn stop(&self) {
        // Moved before the abort so that a worker which finishes on its own in
        // the same moment cannot report the sweep as completed: `complete`
        // checks the generation it was started under.
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.stop_tasks().await;
        let mut runtime = self.runtime.write();
        runtime.phase = ServicePhase::Stopped;
        runtime.active_revision = None;
        runtime.updated_at = Utc::now();
        drop(runtime);
        self.report(WatchLogLevel::Info, "牌谱屋同步已停止".to_owned());
    }

    async fn stop_tasks(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().await);
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
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

    /// The last worker's parting note: every mode is done and nothing is left
    /// running.
    ///
    /// Without it a finished sweep leaves the console reading 同步中 for ever.
    /// That was survivable while the sweep never ended — it only stopped when
    /// it caught up with live play, and more games were always coming — and it
    /// stops being survivable the moment 结束时间 makes this a job that
    /// completes.
    fn complete(&self, generation: u64) {
        if self.generation.load(Ordering::SeqCst) != generation {
            return;
        }
        {
            let mut runtime = self.runtime.write();
            // A mode that gave up already said why, and that is the more useful
            // thing for the page to go on showing.
            if runtime.phase == ServicePhase::Failed {
                return;
            }
            runtime.phase = ServicePhase::Stopped;
            runtime.updated_at = Utc::now();
        }
        let progress = self.progress.read().clone();
        self.report(
            WatchLogLevel::Info,
            format!(
                "牌谱屋同步结束：本次收录 {} 局，请求 {} 次，被拒 {} 次",
                progress.synced, progress.requests, progress.refused
            ),
        );
    }

    fn fail(&self, message: &str) {
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Failed;
            runtime.updated_at = Utc::now();
            runtime.last_error = Some(message.to_owned());
        }
        self.report(WatchLogLevel::Error, message.to_owned());
    }

    /// Walks one mode forward from its cursor until the site has nothing newer.
    async fn sync_mode(
        &self,
        config: &PaipuyaConfig,
        client: &reqwest::Client,
        pacer: &Pacer,
        mode: i32,
    ) -> anyhow::Result<()> {
        let mut from = sweep_from(
            self.dependencies.catalog.paipuya_cursor(mode).await?,
            config.start_from,
        );
        loop {
            let to = config.sync_until.unwrap_or_else(Utc::now);
            // The site refuses a range narrower than ten seconds, and a cursor
            // that has reached the right edge is exactly that.
            if (to - from).num_seconds() < 10 {
                // Said out loud only for a bounded window. Without one this is
                // "caught up with live play", which the mode reports when its
                // last page comes back short; with one it is the mode finishing
                // the job, and an operator who set an end time is waiting to be
                // told exactly that.
                if config.sync_until.is_some() {
                    if from > to {
                        // Not finished — never started. The cursor is past the
                        // window, so there is nothing between the two edges to
                        // ask for, and calling that 已同步 would be a completed
                        // job with zero requests behind it. This is the shape a
                        // deployment lands in the moment it asks for a
                        // *historical* window after having caught up, which is
                        // the main reason to want an 结束时间 at all.
                        self.report(
                            WatchLogLevel::Warn,
                            format!(
                                "模式 {mode} 的游标在 {}，已经晚于设定的结束时间（{}），\
                                 这一轮一个请求都没发；要重抓这一段，先点「重置游标」",
                                from.format("%Y-%m-%d %H:%M:%S"),
                                to.format("%Y-%m-%d %H:%M:%S")
                            ),
                        );
                    } else {
                        self.report(
                            WatchLogLevel::Info,
                            format!(
                                "模式 {mode} 已同步到设定的结束时间（{}）",
                                to.format("%Y-%m-%d %H:%M:%S")
                            ),
                        );
                    }
                }
                return Ok(());
            }
            pacer.tick().await;
            let page = self.fetch_page(config, client, mode, from, to).await?;
            self.progress.write().requests += 1;
            let full = page.len() >= config.page_size;
            let mut games = Vec::with_capacity(page.len());
            let mut without_uuid = 0u64;
            let mut newest = from;
            for api_game in page {
                let started = Utc.timestamp_opt(api_game.start_time, 0).single();
                match api_game.into_game() {
                    Some(game) => {
                        newest = newest.max(game.started_at);
                        games.push(game);
                    }
                    None => {
                        without_uuid += 1;
                        if let Some(started) = started {
                            newest = newest.max(started);
                        }
                    }
                }
            }
            let count = games.len() as u64;
            self.dependencies
                .catalog
                .insert_paipuya_games(&games)
                .await?;

            // Re-requested from the last game's own second rather than from the
            // one after it, so a game sharing that second with the boundary is
            // read twice instead of skipped. The catalogue collapses the
            // duplicate; a skipped game would never be noticed.
            let next = if newest > from {
                newest
            } else if full {
                // A whole page inside one second. Stepping over it is the only
                // way forward, and it is worth saying out loud because it is
                // the one case that can lose games.
                self.report(
                    WatchLogLevel::Warn,
                    format!(
                        "模式 {mode} 在 {newest} 这一秒内的对局超过一页，跳过这一秒继续；\
                         这一秒里可能有对局没有同步到"
                    ),
                );
                from + chrono::TimeDelta::seconds(1)
            } else {
                // A short page that ended where it started: caught up.
                return Ok(());
            };
            from = next;
            self.dependencies
                .catalog
                .set_paipuya_cursor(mode, from, count)
                .await?;
            let total = {
                let mut progress = self.progress.write();
                progress.synced += count;
                progress.without_uuid += without_uuid;
                progress.cursors.insert(mode, from);
                progress.synced
            };
            // Said out loud every so often, because the alternative is what this
            // had before: one line when the sweep starts and then silence until
            // it finishes or fails. A sweep of the whole catalogue runs for days,
            // and an operator watching the log had no way to tell a working sync
            // from a wedged one.
            //
            // Counted against the running total rather than per mode, so the
            // interval is a property of the sweep and not of how many modes
            // happen to be in flight.
            if total / PROGRESS_EVERY > (total - count) / PROGRESS_EVERY {
                self.report(
                    WatchLogLevel::Info,
                    format!(
                        "牌谱屋同步中：已收录 {total} 局（模式 {mode} 走到 {}，本页 {count} 局）",
                        from.format("%Y-%m-%d %H:%M:%S")
                    ),
                );
            }
            if !full {
                // Caught up. Worth one line: with twelve modes in the queue this
                // is the only signal that a mode is done rather than stuck.
                self.report(
                    WatchLogLevel::Info,
                    format!(
                        "模式 {mode} 已{}（走到 {}）",
                        if config.sync_until.is_some() {
                            "同步完设定区间"
                        } else {
                            "追平牌谱屋"
                        },
                        from.format("%Y-%m-%d %H:%M:%S")
                    ),
                );
                return Ok(());
            }
        }
    }

    async fn fetch_page(
        &self,
        config: &PaipuyaConfig,
        client: &reqwest::Client,
        mode: i32,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ApiGame>> {
        // The path segments are epoch milliseconds and the site compares them
        // as such; `limit` above 500 is clamped by the site, which would make
        // "was this page full" wrong here.
        let url = format!(
            "{}/api/v2/{}/games/{}/{}?mode={}&limit={}",
            config.base_url.trim_end_matches('/'),
            mode_path(mode),
            from.timestamp_millis(),
            to.timestamp_millis(),
            mode,
            config.page_size.min(MAX_PAGE),
        );
        let mut request = client.get(&url);
        if let Some(key) = config.api_key.as_deref() {
            request = request.header(config.api_key_header.as_str(), key);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            self.progress.write().refused += 1;
            let body = response.text().await.unwrap_or_default();
            let hint = if status.as_u16() == 429 {
                "：牌谱屋在限流。没有 key 的调用方额度极低，先向 SAPikachu 申请授权，\
                 或者把请求间隔调大"
            } else {
                ""
            };
            anyhow::bail!(
                "牌谱屋返回 {status}{hint}（{}）",
                body.chars().take(200).collect::<String>()
            );
        }
        Ok(response.json().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_configuration_syncs_nothing_until_it_is_turned_on() {
        let config = PaipuyaConfig::default();
        config.validate().unwrap();
        assert!(!config.enabled);
        assert!(config.api_key.is_none());
        assert_eq!(config.selected_modes().len(), 12, "all ranked modes");
    }

    /// The window's left edge has to beat a cursor behind it, or 开始时间 is a
    /// box that accepts an edit and changes nothing.
    #[test]
    fn the_window_drags_a_lagging_cursor_forward_but_never_pushes_one_back() {
        let day = |text: &str| text.parse::<DateTime<Utc>>().expect("a valid moment");
        let window = day("2026-01-01T00:00:00Z");

        // Never swept: the window is where it starts.
        assert_eq!(sweep_from(None, window), window);
        // Swept, but from before the window — moving 开始时间 forward skips
        // ahead rather than being ignored.
        assert_eq!(
            sweep_from(Some(day("2019-09-01T00:00:00Z")), window),
            window
        );
        // Swept past it: that is progress, and it is kept. Going back is what
        // clearing the cursors is for.
        let ahead = day("2026-06-01T00:00:00Z");
        assert_eq!(sweep_from(Some(ahead), window), ahead);
    }

    /// A window narrower than the site's own minimum range is a sweep that
    /// returns immediately and reads as finished.
    #[test]
    fn a_window_must_be_wide_enough_for_the_site_to_answer() {
        let start = "2026-01-01T00:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("a valid moment");
        let with_until = |seconds: i64| PaipuyaConfig {
            start_from: start,
            sync_until: Some(start + chrono::TimeDelta::seconds(seconds)),
            ..PaipuyaConfig::default()
        };
        assert!(with_until(9).validate().is_err());
        assert!(
            with_until(-86_400).validate().is_err(),
            "an end before the start"
        );
        assert!(with_until(10).validate().is_ok());
        // And no end at all is the default: keep up with live play.
        assert!(PaipuyaConfig::default().sync_until.is_none());
    }

    /// A configuration written before the window had a right edge.
    #[test]
    fn a_stored_configuration_without_an_end_keeps_meaning_forever() {
        let document = serde_json::json!({
            "revision": 4,
            "enabled": true,
            "base_url": "https://5-data.amae-koromo.com",
            "api_key": null,
            "api_key_header": "authorization",
            "modes": [],
            "start_from": "2019-08-23T00:00:00Z",
            "request_interval_ms": 1_000,
            "concurrency": 4,
            "page_size": 500,
        });
        let config: PaipuyaConfig =
            serde_json::from_value(document).expect("a pre-window document still loads");
        assert!(config.sync_until.is_none());
        config.validate().unwrap();
    }

    #[test]
    fn rejects_a_rate_faster_than_the_floor_and_an_unknown_mode() {
        let faster = PaipuyaConfig {
            request_interval_ms: MIN_REQUEST_INTERVAL_MS - 1,
            ..PaipuyaConfig::default()
        };
        assert!(faster.validate().is_err());
        // Slower is always allowed: the danger is asking too often.
        let slower = PaipuyaConfig {
            request_interval_ms: 60_000,
            ..PaipuyaConfig::default()
        };
        assert!(slower.validate().is_ok());

        let bronze = PaipuyaConfig {
            // Bronze. 牌谱屋 does not carry it and `mode_metadata` has no row.
            modes: vec![2],
            ..PaipuyaConfig::default()
        };
        assert!(bronze.validate().is_err());

        let bad_header = PaipuyaConfig {
            api_key_header: "not a header".into(),
            ..PaipuyaConfig::default()
        };
        assert!(bad_header.validate().is_err());
    }

    /// The console reads the document, edits one field and submits all of it
    /// back. Without the restore, the first save after any read would overwrite
    /// the key with the asterisks it was shown.
    #[test]
    fn the_key_never_leaves_and_a_round_trip_keeps_it() {
        let stored = PaipuyaConfig {
            api_key: Some("Bearer real-secret".into()),
            ..PaipuyaConfig::default()
        };
        let published = stored.clone().published();
        assert_eq!(published.api_key.as_deref(), Some("***"));

        assert_eq!(
            restored_secret(published.api_key, stored.api_key.as_deref()).as_deref(),
            Some("Bearer real-secret"),
        );
        // An operator actually typing a new key replaces it.
        assert_eq!(
            restored_secret(Some("Bearer new".into()), stored.api_key.as_deref()).as_deref(),
            Some("Bearer new"),
        );
        // And clearing it clears it.
        assert_eq!(restored_secret(None, stored.api_key.as_deref()), None);
    }

    #[test]
    fn a_game_with_no_uuid_or_no_players_is_not_catalogued() {
        let complete = ApiGame {
            uuid: "260716-abc".into(),
            mode_id: 16,
            start_time: 1_784_211_956,
            end_time: 1_784_213_000,
            players: vec![ApiPlayer {
                account_id: 7,
                nickname: "someone".into(),
                score: 25_000,
            }],
        };
        let game = complete.into_game().expect("a complete listing");
        assert_eq!(game.players, vec!["someone".to_owned()]);
        assert_eq!(game.started_at.timestamp(), 1_784_211_956);

        // 牌谱屋 lists games whose uuid it will not give out. They are counted
        // and dropped: a row with no uuid could never be fetched, and it would
        // be reported as missing on every comparison for ever.
        let masked = ApiGame {
            uuid: String::new(),
            mode_id: 16,
            start_time: 1_784_211_956,
            end_time: 0,
            players: vec![ApiPlayer {
                account_id: 7,
                nickname: "someone".into(),
                score: 0,
            }],
        };
        assert!(masked.into_game().is_none());

        let seatless = ApiGame {
            uuid: "260716-abc".into(),
            mode_id: 16,
            start_time: 1_784_211_956,
            end_time: 0,
            players: Vec::new(),
        };
        assert!(seatless.into_game().is_none());
    }

    #[test]
    fn four_player_modes_and_three_player_modes_live_at_different_paths() {
        assert_eq!(mode_path(16), "pl4");
        assert_eq!(mode_path(9), "pl4");
        assert_eq!(mode_path(21), "pl3");
        assert_eq!(mode_path(26), "pl3");
    }

    /// The whole point of splitting rate from concurrency: eight workers must
    /// not mean eight requests an interval. Slots are handed out one interval
    /// apart whoever asks, and a worker that is slow to use its slot does not
    /// hold up the next one.
    #[tokio::test]
    async fn the_pacer_holds_one_request_per_interval_across_every_worker() {
        // Real time rather than a paused clock, so the interval is small and
        // the assertions are one-sided: never *faster* than the interval. A
        // loaded machine can only make the gaps larger, which is not the
        // failure being guarded against.
        const INTERVAL: Duration = Duration::from_millis(40);
        let pacer = Arc::new(Pacer::new(INTERVAL));
        let started = Instant::now();
        let mut workers = Vec::new();
        for _ in 0..5 {
            let pacer = Arc::clone(&pacer);
            workers.push(tokio::spawn(async move {
                pacer.tick().await;
                Instant::now()
            }));
        }
        let mut moments = Vec::new();
        for worker in workers {
            moments.push(worker.await.unwrap());
        }
        moments.sort();
        // Five requests at one per interval: the last leaves four intervals in,
        // not immediately. This is the assertion that fails if the pacer ever
        // becomes a per-worker sleep.
        assert!(
            moments[4] - started >= INTERVAL * 4,
            "five requests left in under four intervals"
        );
        for pair in moments.windows(2) {
            assert!(
                pair[1] - pair[0] >= INTERVAL,
                "two requests left within one interval of each other"
            );
        }
    }
}
