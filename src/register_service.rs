//! Creating Mahjong Soul accounts from the console.
//!
//! The work is done by an installed `register` module, for the reason
//! `modules/register/curl-chrome/module.py` states at length: rustls cannot
//! produce Chrome's ClientHello, and a brand new account has nothing else to be
//! judged on. There is no builtin fallback — without a module this refuses.
//!
//! One account at a time, in the background. A registration sends a code, polls
//! a mailbox until it arrives, and then runs a session that deliberately pauses
//! where a person filling in a form would; several minutes each, tens of minutes
//! for a batch. Nothing about that fits in a request.
//!
//! Accounts land in the pool **disabled** as each one finishes, not at the end.
//! An account that was created and not stored is gone — its password only ever
//! existed inside the run that made it — so a browser closed halfway, or a
//! process restarted, must not be able to lose the ones already made.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::accounts::{AccountPool, AccountPurpose, StoredAccount};
use crate::watch_log::{WatchLogBuffer, WatchLogLevel};
use crate::watch_service::{ModuleKind, ModuleStore, WatchServiceError};

const LOG_SOURCE: &str = "register";

/// How many accounts one run may be asked for.
///
/// Not a resource limit — it is a typo guard. Each account takes minutes, so a
/// thousand-line paste is three weeks of registering, and the way that failure
/// shows up otherwise is a run that appears to have started normally and is
/// still going tomorrow.
const MAX_BATCH: usize = 200;

/// How many failures in a row end a run early.
///
/// A batch that fails on every account is failing for one reason, and none of
/// those reasons get better by trying another ninety-seven times: a module too
/// old for the request, a mailbox service out of quota, a proxy that is down.
/// Observed the expensive way — a hundred accounts each reported the same error
/// from a module that predated the field being sent.
///
/// Three rather than one, so a single flaky account does not end a batch.
const GIVE_UP_AFTER: usize = 3;

/// How many accounts may be registered at once.
///
/// Each one is its own module process and its own TLS session, so this is
/// bounded by what a deployment can sanely have talking to Mahjong Soul from
/// one place at one time — not by anything in this file. Sixteen because past
/// that the exits stop being distinguishable: a run wider than the node list
/// puts several accounts on the same address at the same moment, which is the
/// thing spreading them out was for.
const MAX_CONCURRENCY: usize = 16;

/// A Cloud Mail instance, used as the mailbox supply.
///
/// With one of these a run needs no mailbox list at all: the module opens a
/// fresh address per account through the open API and reads the code back out
/// of the same instance. That is the whole point — third-party mailboxes are a
/// consumable somebody has to keep buying, and the last batch ran out.
///
/// Only `base_url` is really required. The token can be minted from the
/// administrator's own credentials, and the domains are read off the instance,
/// so the usual case is an address and a password.
///
/// It has to be an instance you administer. The open API authenticates with a
/// token and never sees Turnstile; the other way in — registering an ordinary
/// user per account — does have to pass Turnstile, and public instances
/// generally also close 一个账号多个邮箱, which caps you at one address.
///
/// Both `token` and `admin_password` are secrets: they go to the module and
/// nowhere else, never into a log line or a status response.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CloudMailConfig {
    /// Where the instance lives, e.g. `https://mail.example.com`. Paths are
    /// appended by the module.
    pub base_url: String,
    /// An open API token that already exists. Skips minting one — which
    /// matters, because minting invalidates whatever token was there before.
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub admin_email: String,
    #[serde(default)]
    pub admin_password: String,
    /// Pin the new addresses to one domain. Empty spreads them over every
    /// domain the instance says it receives — which is the better default, since
    /// a batch that all shares one suffix is itself something to be caught by.
    #[serde(default)]
    pub domain: String,
}

impl CloudMailConfig {
    fn validate(&self) -> Result<(), WatchServiceError> {
        if self.base_url.trim().is_empty() {
            return Err(WatchServiceError::InvalidConfig(
                "Cloud Mail 的地址是空的".to_owned(),
            ));
        }
        if !self.base_url.trim().starts_with("http") {
            return Err(WatchServiceError::InvalidConfig(
                "Cloud Mail 地址要带 http:// 或 https://".to_owned(),
            ));
        }
        // One of the two ways in has to be there. Neither means the run would
        // get as far as the first account before finding out.
        if self.token.trim().is_empty()
            && !(!self.admin_email.trim().is_empty() && !self.admin_password.is_empty())
        {
            return Err(WatchServiceError::InvalidConfig(
                "Cloud Mail 要么给开放 API 令牌，要么给管理员邮箱和密码".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A temp-mail service that hands out an address per request.
///
/// The least setup of the three sources: no instance to run, no mailbox to
/// create, no domain to discover. It returns a fresh address on every call, on a
/// different domain each time, with a local part that reads like a person's —
/// all three of which are things this codebase does worse by hand.
///
/// The cost is trust. Those mailboxes sit with whoever runs the service: they
/// can read the verification mail, which means the recovery address of every
/// account made this way is not exclusively yours. Fine for collectors that only
/// ever log in from here; not where you would put anything you need to keep.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TempMailConfig {
    /// Left empty the module uses its own default, so a run needs only the key.
    #[serde(default)]
    pub base_url: String,
    pub api_key: String,
}

impl TempMailConfig {
    fn validate(&self) -> Result<(), WatchServiceError> {
        if self.api_key.trim().is_empty() {
            return Err(WatchServiceError::InvalidConfig(
                "临时邮箱的 API key 是空的".to_owned(),
            ));
        }
        if !self.base_url.trim().is_empty() && !self.base_url.trim().starts_with("http") {
            return Err(WatchServiceError::InvalidConfig(
                "临时邮箱地址要带 http:// 或 https://".to_owned(),
            ));
        }
        Ok(())
    }
}

/// What one run was asked to do.
#[derive(Clone, Debug, Deserialize)]
pub struct AccountRegisterRequest {
    /// One mailbox credential per entry. The e-mail address is read out of the
    /// string and the whole string is the key that reads its inbox, so this is
    /// a secret — it never appears in a log line or in a status response.
    ///
    /// Empty when `cloud_mail` is supplying the addresses instead.
    #[serde(default)]
    pub mailboxes: Vec<String>,
    /// Open the addresses on a Cloud Mail instance instead of consuming a
    /// prepared list. Takes precedence over the other two when several are given.
    #[serde(default)]
    pub cloud_mail: Option<CloudMailConfig>,
    /// Or get them from a temp-mail service, which needs only a key.
    #[serde(default)]
    pub temp_mail: Option<TempMailConfig>,
    /// How many accounts to make. Only read when the addresses are opened on
    /// demand — with a list, the list length is the count.
    #[serde(default)]
    pub count: usize,
    /// What the accounts are for once an operator enables them.
    #[serde(default = "default_purpose")]
    pub purpose: AccountPurpose,
    #[serde(default)]
    pub note: String,
    #[serde(default)]
    pub proxy: Option<String>,
    /// Reproduce a real client's session shape — heartbeats, the lobby fetches,
    /// the pauses. Off makes each account about four minutes faster and is only
    /// there as a control group; see the ban investigation.
    #[serde(default = "default_mimic")]
    pub mimic: bool,
    #[serde(default = "default_poll_tries")]
    pub poll_tries: u32,
    #[serde(default = "default_poll_interval")]
    pub poll_interval: f64,
    /// How many accounts to register at once. Clamped to
    /// [`MAX_CONCURRENCY`]; one is the old behaviour.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Give each account its own mihomo exit instead of sending the whole batch
    /// out of one address.
    ///
    /// Only nodes that already have a listener are used — see
    /// `exits_for`. Registering does not create listeners: that
    /// would rewrite the outbound assignment the re-fetch pool is running on.
    #[serde(default)]
    pub random_node: bool,
}

fn default_purpose() -> AccountPurpose {
    AccountPurpose::Refetch
}
fn default_mimic() -> bool {
    true
}
fn default_poll_tries() -> u32 {
    40
}
fn default_poll_interval() -> f64 {
    3.0
}
fn default_concurrency() -> usize {
    1
}

/// One account's result, as the console reads it.
///
/// No password: it went into the pool, which is the only place it belongs. A
/// status endpoint that also served it would put every password of the run
/// behind a poll that the page repeats every few seconds.
#[derive(Clone, Debug, Serialize)]
pub struct AccountRegisterOutcome {
    pub email: String,
    pub ok: bool,
    pub account_id: Option<u64>,
    pub nickname: Option<String>,
    /// Which step failed, for a failure. Empty on success.
    pub stage: String,
    pub detail: String,
    pub at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AccountRegisterProgress {
    pub running: bool,
    pub total: usize,
    pub done: usize,
    pub succeeded: usize,
    pub failed: usize,
    /// The address being worked on, so a run that is inside a mailbox poll does
    /// not look stalled.
    pub current: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub message: Option<String>,
    pub outcomes: Vec<AccountRegisterOutcome>,
}

pub struct RegisterService {
    modules: Arc<ModuleStore>,
    accounts: Arc<AccountPool>,
    /// Only read to find the per-node listeners a run may go out of. Never
    /// written: the outbound assignment belongs to the account pool, and
    /// rewriting it here would move the exits of the re-fetch sessions that are
    /// running at the time.
    mihomo: Arc<crate::mihomo::MihomoManager>,
    logs: Arc<WatchLogBuffer>,
    /// Where the collectors leave the client versions they discovered. Read
    /// only — registration has no account to log in with, so it cannot search
    /// for a version floor itself and lives off what they found.
    cache_dir: PathBuf,
    progress: RwLock<AccountRegisterProgress>,
    /// Bumped by `stop`. The loop checks it between accounts, which is the only
    /// place stopping is safe: an account abandoned mid-registration exists on
    /// Mahjong Soul's side with a password nobody kept.
    generation: AtomicU64,
}

impl RegisterService {
    pub fn new(
        modules: Arc<ModuleStore>,
        accounts: Arc<AccountPool>,
        mihomo: Arc<crate::mihomo::MihomoManager>,
        logs: Arc<WatchLogBuffer>,
        data_dir: &Path,
    ) -> Self {
        Self {
            modules,
            accounts,
            mihomo,
            logs,
            // The same directory the collectors and the re-fetch pool share.
            cache_dir: data_dir.join("watch/cache"),
            progress: RwLock::new(AccountRegisterProgress::default()),
            generation: AtomicU64::new(0),
        }
    }

    pub fn status(&self) -> AccountRegisterProgress {
        self.progress.read().clone()
    }

    /// Ends the run after the account in flight. Returns whether one was running.
    pub fn stop(&self) -> bool {
        let running = self.progress.read().running;
        if running {
            self.generation.fetch_add(1, Ordering::SeqCst);
            self.report(WatchLogLevel::Info, "收到停止，本个账号注册完就结束");
        }
        running
    }

    pub fn start(
        self: &Arc<Self>,
        request: AccountRegisterRequest,
    ) -> Result<(), WatchServiceError> {
        let jobs = jobs_for(&request)?;
        // Checked before the task starts so "没装注册模块" is the answer to the
        // button press rather than a log line somebody finds later.
        let module = self.modules.sole(ModuleKind::Register)?;

        {
            let mut progress = self.progress.write();
            if progress.running {
                return Err(WatchServiceError::InvalidConfig(
                    "已经有一批在注册了，等它结束或者先停掉".to_owned(),
                ));
            }
            // Outcomes reset with the run: a page showing the previous batch's
            // failures beside this batch's counters is unreadable.
            *progress = AccountRegisterProgress {
                running: true,
                total: jobs.len(),
                started_at: Some(Utc::now()),
                ..Default::default()
            };
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let outcome = Arc::clone(&service)
                .run(generation, module, jobs, request)
                .await;
            let mut progress = service.progress.write();
            progress.running = false;
            progress.current = None;
            progress.finished_at = Some(Utc::now());
            progress.message = Some(match outcome {
                Ok(message) => message,
                Err(error) => format!("注册中断：{error}"),
            });
            let message = progress.message.clone().unwrap_or_default();
            drop(progress);
            service.report(WatchLogLevel::Info, message);
        });
        Ok(())
    }

    async fn run(
        self: Arc<Self>,
        generation: u64,
        module: crate::watch_service::ModuleRef,
        jobs: Vec<Option<String>>,
        request: AccountRegisterRequest,
    ) -> Result<String, WatchServiceError> {
        let total = jobs.len();
        let concurrency = request
            .concurrency
            .clamp(1, MAX_CONCURRENCY)
            .min(total.max(1));
        self.report(
            WatchLogLevel::Info,
            format!(
                "开始注册 {} 个账号，{}，用模块 {}@{}{}",
                total,
                if concurrency > 1 {
                    format!("{concurrency} 个一起跑")
                } else {
                    "一个一个来".to_owned()
                },
                module.name,
                module.version,
                if request.mimic {
                    "（拟真会话，每个约 3~5 分钟）"
                } else {
                    "（不拟真，只做对照用）"
                }
            ),
        );

        // The version this run reports, decided once. Majsoul validates the
        // version string as a lower bound with a tolerance of about three
        // patches, so a pinned constant fails the whole batch with 151 a few
        // weeks after it is written. The collectors already hit that wall,
        // search out the new floor and leave it in the cache — this reads their
        // answer rather than pinning one of its own.
        let versions = Arc::new(crate::managed_watch::current_client_versions(
            &self.cache_dir,
        ));
        self.report(
            WatchLogLevel::Info,
            format!("客户端版本 {}（包 {}）", versions.0, versions.1),
        );

        // Which exits the accounts leave from, decided once. Empty means every
        // account uses whatever `proxy` says, which is the old behaviour.
        let exits = if request.random_node {
            let found = self.exits().await;
            if found.is_empty() {
                return Err(WatchServiceError::InvalidConfig(
                    "没有可用的出口节点：mihomo 的按节点出站是跟着账号池里\
                     「已启用的补抓账号绑了哪些节点」走的，先去账号池给几个补抓账号选上节点并保存"
                        .to_owned(),
                ));
            }
            self.report(
                WatchLogLevel::Info,
                format!("出口节点 {} 个：{}", found.len(), found.join("、")),
            );
            found
        } else {
            Vec::new()
        };

        let worker = self
            .modules
            .worker(ModuleKind::Register, &module)
            .await?
            .ok_or_else(|| {
                WatchServiceError::InvalidConfig("注册没有内建实现，必须装模块".to_owned())
            })?;

        // Once for the whole run, not once per account: the token Cloud Mail
        // mints is the only one it has, so minting per account would have each
        // one invalidate the token the previous account is still polling with.
        // Also the point where a wrong password or a domain the instance does
        // not receive is reported — before any mailbox or code is spent.
        let cloud = match &request.cloud_mail {
            Some(config) => {
                let resolved = worker
                    .request(
                        "cloud_mail_resolve",
                        serde_json::json!({ "cloud_mail": config, "proxy": request.proxy }),
                    )
                    .await
                    .inspect_err(|error| {
                        self.report(WatchLogLevel::Warn, format!("Cloud Mail 连不上：{error}"))
                    })?;
                self.report(
                    WatchLogLevel::Info,
                    format!(
                        "Cloud Mail 可用域名 {}",
                        resolved
                            .get("domains")
                            .and_then(|domains| domains.as_array())
                            .map(|domains| domains
                                .iter()
                                .filter_map(|domain| domain.as_str())
                                .collect::<Vec<_>>()
                                .join("、"))
                            .unwrap_or_default()
                    ),
                );
                // The instance address is not in the resolved answer, and the
                // token in it replaces whatever was configured.
                Some(serde_json::json!({
                    "base_url": config.base_url,
                    "token": resolved.get("token"),
                    "domains": resolved.get("domains"),
                    "min_prefix": resolved.get("min_prefix"),
                }))
            }
            None => None,
        };

        // The resolve worker has done its one job. Each account gets its own
        // process below: `PluginWorker` is one request at a time — the input
        // lock is dropped before the response is read — so sharing one across
        // concurrent registrations would have them read each other's answers.
        worker.shutdown().await;

        let request = Arc::new(request);
        let cloud = Arc::new(cloud);
        // A permit is one account in flight. Taking it before spawning is what
        // makes the loop itself the queue: it blocks here until somebody
        // finishes, so `jobs` is never all resident at once.
        let gate = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let streak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        let mut launched = 0usize;
        let mut stopped = false;

        for (index, mailbox) in jobs.into_iter().enumerate() {
            let permit = Arc::clone(&gate)
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            if self.generation.load(Ordering::SeqCst) != generation {
                stopped = true;
                break;
            }
            // Checked here rather than inside the task so the ones already in
            // flight are allowed to finish — an account abandoned mid-flight
            // exists on Mahjong Soul's side with a password nobody kept.
            if streak.load(Ordering::SeqCst) >= GIVE_UP_AFTER {
                break;
            }

            // The address is derived here only to have something to show and log
            // while the module works. The credential itself never leaves this
            // function. With Cloud Mail there is nothing to derive it from yet —
            // the module opens the address — so the counter stands in until the
            // answer comes back with the real one.
            let address = match &mailbox {
                Some(credential) => {
                    address_of(credential).unwrap_or_else(|| "（读不出邮箱）".to_owned())
                }
                None => format!("第 {}/{total} 个（正在开邮箱）", index + 1),
            };
            // Round robin rather than random: with N exits and N accounts in
            // flight, this is the only assignment where no two of them share an
            // address at the same moment.
            let node = (!exits.is_empty()).then(|| exits[index % exits.len()].clone());

            let service = Arc::clone(&self);
            let module = module.clone();
            let request = Arc::clone(&request);
            let cloud = Arc::clone(&cloud);
            let streak = Arc::clone(&streak);
            let versions = Arc::clone(&versions);
            launched += 1;
            tasks.spawn(async move {
                let _permit = permit;
                service
                    .one_account(
                        module, address, mailbox, cloud, node, request, streak, versions,
                    )
                    .await;
            });
        }
        while tasks.join_next().await.is_some() {}

        if stopped {
            return Ok("已停止".to_owned());
        }
        if streak.load(Ordering::SeqCst) >= GIVE_UP_AFTER {
            let skipped = total - launched;
            let reason = self.last_failure();
            let hint = version_hint(&reason, &versions.0);
            return Ok(format!(
                "连着 {GIVE_UP_AFTER} 个都失败了，剩下 {skipped} 个没有再试。\
                 最后一个的原因：{reason}{hint}"
            ));
        }
        let progress = self.progress.read();
        Ok(format!(
            "注册结束：成功 {}，失败 {}",
            progress.succeeded, progress.failed
        ))
    }

    /// The nodes a run may go out of: those mihomo already has a listener on.
    ///
    /// Deliberately read-only. The outbound assignment is derived from the
    /// account pool — which nodes the *enabled re-fetch* accounts name — and
    /// registering into it would be a write with two problems: `set_outbound_nodes`
    /// is set-replacement, so adding one node drops the slots of every node the
    /// pool is using, and the new assignment is not live until mihomo has
    /// reloaded, which takes up to a minute. An account made here is disabled,
    /// so it would be dropped from that list again at the next save anyway.
    ///
    /// So: use the exits that exist. To have more of them, bind more nodes in
    /// the account pool.
    async fn exits(&self) -> Vec<String> {
        usable_exits(self.mihomo.status().await.outbounds)
    }

    /// The most recent failure, for the message that ends a given-up run.
    fn last_failure(&self) -> String {
        self.progress
            .read()
            .outcomes
            .iter()
            .rev()
            .find(|outcome| !outcome.ok)
            .map(|outcome| format!("[{}] {}", outcome.stage, outcome.detail))
            .unwrap_or_else(|| "（没记下原因）".to_owned())
    }

    /// One account, start to finish, in its own module process.
    #[allow(clippy::too_many_arguments)]
    async fn one_account(
        self: Arc<Self>,
        module: crate::watch_service::ModuleRef,
        address: String,
        mailbox: Option<String>,
        cloud: Arc<Option<serde_json::Value>>,
        node: Option<String>,
        request: Arc<AccountRegisterRequest>,
        streak: Arc<std::sync::atomic::AtomicUsize>,
        versions: Arc<(String, String)>,
    ) {
        // A node with no listener falls back to whatever `proxy` says rather
        // than to no proxy at all: dialling nothing is not a quieter exit, it is
        // this host's own address.
        let proxy = node
            .as_deref()
            .and_then(|node| self.mihomo.proxy_url_for_node(node))
            .or_else(|| request.proxy.clone());

        self.progress.write().current = Some(address.clone());
        self.report(
            WatchLogLevel::Info,
            match &node {
                Some(node) => format!("{address} 注册中（出口 {node}）"),
                None => format!("{address} 注册中"),
            },
        );

        let answer = match self.modules.worker(ModuleKind::Register, &module).await {
            Ok(Some(worker)) => {
                let answer = worker
                    .request(
                        "register",
                        serde_json::json!({
                            "mailbox": mailbox,
                            "cloud_mail": *cloud,
                            // No resolve step for this one: there is no token to
                            // mint and no domain to discover, so the config the
                            // operator typed is already everything the module needs.
                            "temp_mail": request.temp_mail,
                            "proxy": proxy,
                            "mimic": request.mimic,
                            "poll_tries": request.poll_tries,
                            "poll_interval": request.poll_interval,
                            // The one the server checks, and the one it does not
                            // but a real client still reports — see `versions`.
                            "client_version": versions.0,
                            "package_version": versions.1,
                        }),
                    )
                    .await;
                worker.shutdown().await;
                answer
            }
            Ok(None) => Err(WatchServiceError::InvalidConfig(
                "注册没有内建实现，必须装模块".to_owned(),
            )),
            Err(error) => Err(error),
        };

        let outcome = self.settle(&address, answer, &request, node);
        let ok = outcome.ok;
        self.progress.write().outcomes.push(outcome);
        note_result(&streak, ok);
    }

    /// Turns one module answer into an outcome, storing the account first.
    ///
    /// Order matters: the pool write happens before the counter moves, so an
    /// account that could not be stored is reported as a failure rather than
    /// counted as a success nobody can log in with.
    fn settle(
        &self,
        address: &str,
        answer: Result<serde_json::Value, WatchServiceError>,
        request: &AccountRegisterRequest,
        node: Option<String>,
    ) -> AccountRegisterOutcome {
        let mut outcome = AccountRegisterOutcome {
            email: address.to_owned(),
            ok: false,
            account_id: None,
            nickname: None,
            stage: String::new(),
            detail: String::new(),
            at: Utc::now(),
        };
        let value = match answer {
            Ok(value) => value,
            Err(error) => {
                outcome.stage = "module".to_owned();
                outcome.detail = error.to_string();
                return self.finish(outcome);
            }
        };
        outcome.email = reported_email(&value, address);

        if !value.get("ok").and_then(|ok| ok.as_bool()).unwrap_or(false) {
            outcome.stage = value
                .get("stage")
                .and_then(|stage| stage.as_str())
                .unwrap_or("unknown")
                .to_owned();
            outcome.detail = value
                .get("error")
                .and_then(|error| error.as_str())
                .unwrap_or("模块没有说明失败原因")
                .to_owned();
            return self.finish(outcome);
        }

        let email = outcome.email.clone();
        let password = value
            .get("password")
            .and_then(|password| password.as_str())
            .unwrap_or_default()
            .to_owned();
        outcome.account_id = value.get("account_id").and_then(|id| id.as_u64());
        outcome.nickname = value
            .get("nickname")
            .and_then(|nickname| nickname.as_str())
            .map(str::to_owned);
        if password.is_empty() {
            // The module reported success without the one field that makes the
            // account usable. Storing a row with an empty password would fail
            // validation and take the rest of the batch with it.
            outcome.stage = "password".to_owned();
            outcome.detail = "模块说成功但没给密码，这个号没法用".to_owned();
            return self.finish(outcome);
        }

        // Disabled: an account nobody has looked at must not start logging in on
        // its own. `note` carries whatever the operator typed for the batch, so
        // a run can be told apart from the next one in the list.
        //
        // `node` is where this account was made from. Recording it means that
        // once somebody enables it, the re-fetch pool logs in from the same
        // address it registered from — the alternative is an account born on one
        // exit and used from another, which is a thing to be noticed.
        let stored = StoredAccount {
            id: String::new(),
            username: email.clone(),
            password,
            purpose: request.purpose,
            note: request.note.clone(),
            enabled: false,
            node: node.unwrap_or_default(),
        };
        match self.accounts.append(vec![stored]) {
            Ok(1) => {
                outcome.ok = true;
                outcome.detail = match (&outcome.nickname, outcome.account_id) {
                    (Some(nickname), Some(id)) => format!("昵称 {nickname}，account_id {id}"),
                    (Some(nickname), None) => format!("昵称 {nickname}"),
                    (None, Some(id)) => format!("account_id {id}，昵称没设上"),
                    (None, None) => "注册成功，但昵称和 account_id 都没拿到".to_owned(),
                };
            }
            // Not an error: the address was already in the pool, which is what a
            // re-run over the same mailbox file looks like. The account exists
            // either way, so this is reported rather than retried.
            Ok(_) => {
                outcome.stage = "pool".to_owned();
                outcome.detail = "账号池里已经有这个邮箱了，没有重复写入".to_owned();
            }
            Err(error) => {
                outcome.stage = "pool".to_owned();
                outcome.detail = format!("写进账号池失败：{error}");
            }
        }
        self.finish(outcome)
    }

    fn finish(&self, outcome: AccountRegisterOutcome) -> AccountRegisterOutcome {
        {
            let mut progress = self.progress.write();
            progress.done += 1;
            if outcome.ok {
                progress.succeeded += 1;
            } else {
                progress.failed += 1;
            }
        }
        self.report(
            if outcome.ok {
                WatchLogLevel::Info
            } else {
                WatchLogLevel::Warn
            },
            if outcome.ok {
                format!("{} ✓ {}", outcome.email, outcome.detail)
            } else {
                format!("{} ✗ [{}] {}", outcome.email, outcome.stage, outcome.detail)
            },
        );
        outcome
    }

    fn report(&self, level: WatchLogLevel, message: impl Into<String>) {
        self.logs.append(level, LOG_SOURCE, message);
    }
}

/// The e-mail address inside a mailbox credential string.
///
/// The credential is a provider-specific blob that happens to start with the
/// address. Only the address is ever shown or logged; the rest is the key to
/// that inbox.
fn address_of(credential: &str) -> Option<String> {
    let candidate: String = credential
        .trim()
        .split(|c: char| c.is_whitespace() || c == ',' || c == '-')
        .find(|part| {
            let Some((local, domain)) = part.split_once('@') else {
                return false;
            };
            !local.is_empty() && domain.contains('.') && !domain.starts_with('.')
        })?
        .to_owned();
    Some(candidate)
}

/// The work list for one run: one entry per account.
///
/// `Some(credential)` is a prepared mailbox; `None` means the module opens the
/// address itself on Cloud Mail. Nothing downstream needs to know which it was —
/// the real address comes back in the module's answer either way.
fn jobs_for(request: &AccountRegisterRequest) -> Result<Vec<Option<String>>, WatchServiceError> {
    // Both on-demand sources look the same from here: the module opens the
    // address, so a job carries no credential and the count is all there is.
    let on_demand = match (&request.cloud_mail, &request.temp_mail) {
        (Some(cloud), _) => Some(cloud.validate()),
        (None, Some(temp)) => Some(temp.validate()),
        (None, None) => None,
    };
    if let Some(validated) = on_demand {
        validated?;
        if request.count == 0 {
            return Err(WatchServiceError::InvalidConfig(
                "要注册几个？填个数量".to_owned(),
            ));
        }
        if request.count > MAX_BATCH {
            return Err(too_many(request.count));
        }
        return Ok(vec![None; request.count]);
    }
    let mailboxes: Vec<Option<String>> = request
        .mailboxes
        .iter()
        .map(|line| line.trim().to_owned())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(Some)
        .collect();
    if mailboxes.is_empty() {
        return Err(WatchServiceError::InvalidConfig(
            "一个邮箱凭据都没有；每行一个，行首 # 会被跳过。或者换个来源，让它现开".to_owned(),
        ));
    }
    if mailboxes.len() > MAX_BATCH {
        return Err(too_many(mailboxes.len()));
    }
    Ok(mailboxes)
}

/// The outbounds a batch may actually leave from.
///
/// `available` alone is not enough. A group exists from the moment it is
/// written, but until mihomo has applied the selection it is still on the
/// shared exit — `selected_node` says `MAJSOUL` rather than the node. Taking
/// those would put the whole batch back on one address while the log claimed it
/// was spread over several, which is worse than not spreading it at all.
fn usable_exits(outbounds: Vec<crate::mihomo::MihomoOutboundStatus>) -> Vec<String> {
    outbounds
        .into_iter()
        .filter(|outbound| {
            outbound.available && outbound.selected_node.as_deref() == Some(&outbound.node)
        })
        .map(|outbound| outbound.node)
        .collect()
}

/// Records one result in the shared failure streak, returning the new value.
///
/// In a row, not in total: a batch where every third account fails is annoying
/// but working, and a running total would stop it at the third failure.
///
/// Shared, because accounts run concurrently. Under concurrency "in a row"
/// means "since the last success", which is the property that matters — a run
/// where nothing has succeeded for three finishes is a run to stop, whatever
/// order they landed in. Both branches are single atomic operations, so a
/// success can never be lost to a failure racing it.
fn note_result(streak: &std::sync::atomic::AtomicUsize, ok: bool) -> usize {
    if ok {
        streak.store(0, Ordering::SeqCst);
        0
    } else {
        streak.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// The address to report for one account: the one the module opened, falling
/// back to the placeholder the run started with.
///
/// Read for failures too, not just successes. On the on-demand mailbox sources
/// the address does not exist until the module opens it, so the placeholder is
/// all a failed account would otherwise carry — and by the time signup fails,
/// that mailbox and its verification code have already been spent. Knowing
/// which one is the difference between a reusable address and a lost one.
fn reported_email(answer: &serde_json::Value, fallback: &str) -> String {
    answer
        .get("email")
        .and_then(|email| email.as_str())
        .filter(|email| !email.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}

/// What to add to a given-up run's message when the wall it hit was 151.
///
/// 151 is about the version string and nothing else — not the account, not the
/// mailbox, not the exit. It fails every account in the batch identically, so
/// the message has to name the cause or the next thing tried will be the
/// mailbox source. Registration cannot search out the new floor itself: the
/// search is a series of logins and there is no account here to make them with.
fn version_hint(reason: &str, current: &str) -> String {
    if !reason.contains("151") {
        return String::new();
    }
    format!(
        "\n151 = 雀魂把客户端版本的下限抬过去了，跟账号、邮箱、出口都无关。\
         这批报的是 {current}。注册这边没有能登录的账号，探不出新的下限；\
         让牌谱补抓或者采集跑一次，它们撞到同一堵墙会自动探出来存下，\
         之后注册开跑就读到了。"
    )
}

fn too_many(count: usize) -> WatchServiceError {
    WatchServiceError::InvalidConfig(format!(
        "一次最多 {MAX_BATCH} 个，这次是 {count}；一个号要几分钟，分批跑"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cloud() -> CloudMailConfig {
        CloudMailConfig {
            base_url: "https://mail.example.com".to_owned(),
            token: "9f4e298e-7431-4c76-bc15-4931c3a73984".to_owned(),
            ..Default::default()
        }
    }

    fn request(
        mailboxes: &[&str],
        cloud_mail: Option<CloudMailConfig>,
        count: usize,
    ) -> AccountRegisterRequest {
        AccountRegisterRequest {
            mailboxes: mailboxes.iter().map(|line| (*line).to_owned()).collect(),
            cloud_mail,
            temp_mail: None,
            count,
            purpose: default_purpose(),
            note: String::new(),
            proxy: None,
            mimic: true,
            poll_tries: default_poll_tries(),
            poll_interval: default_poll_interval(),
            concurrency: default_concurrency(),
            random_node: false,
        }
    }

    /// Cloud Mail replaces the list rather than supplementing it.
    ///
    /// Both filled is what a console left on the other tab produces. Registering
    /// the pasted list *and* `count` fresh ones would quietly do twice the work
    /// the operator asked for, on mailboxes they thought they had switched away
    /// from.
    #[test]
    fn cloud_mail_wins_over_a_pasted_list() {
        let jobs = jobs_for(&request(&["a@b.com----pw"], Some(cloud()), 3)).unwrap();
        assert_eq!(jobs, vec![None, None, None]);
    }

    #[test]
    fn a_list_run_keeps_its_credentials_and_drops_blanks_and_comments() {
        let jobs = jobs_for(&request(
            &[" a@b.com----pw ", "", "# 注释", "c@d.com"],
            None,
            0,
        ))
        .unwrap();
        assert_eq!(
            jobs,
            vec![Some("a@b.com----pw".to_owned()), Some("c@d.com".to_owned())]
        );
    }

    /// A count of zero is the state the form starts in, not a request to do
    /// nothing: it has to be refused at the button rather than start a run that
    /// finishes instantly having made nothing.
    #[test]
    fn cloud_mail_needs_a_count_and_respects_the_batch_cap() {
        assert!(jobs_for(&request(&[], Some(cloud()), 0)).is_err());
        assert!(jobs_for(&request(&[], Some(cloud()), MAX_BATCH + 1)).is_err());
        assert!(jobs_for(&request(&[], Some(cloud()), MAX_BATCH)).is_ok());
    }

    #[test]
    fn an_incomplete_cloud_mail_is_refused_at_the_button() {
        for broken in [
            CloudMailConfig {
                base_url: String::new(),
                ..cloud()
            },
            // Neither way in: no token, and half-filled administrator credentials.
            CloudMailConfig {
                token: "  ".to_owned(),
                ..cloud()
            },
            CloudMailConfig {
                token: String::new(),
                admin_email: "admin@example.com".to_owned(),
                ..cloud()
            },
            // A bare hostname would be pasted onto the path and requested as a
            // relative URL; better to say so than to fail per account.
            CloudMailConfig {
                base_url: "mail.example.com".to_owned(),
                ..cloud()
            },
        ] {
            assert!(jobs_for(&request(&[], Some(broken), 1)).is_err());
        }
    }

    /// The temp-mail source needs a key and nothing else — no instance, no
    /// domain — and it takes its count the same way Cloud Mail does.
    #[test]
    fn a_temp_mail_key_is_a_complete_source_on_its_own() {
        let with_key = |key: &str, count: usize| AccountRegisterRequest {
            temp_mail: Some(TempMailConfig {
                base_url: String::new(),
                api_key: key.to_owned(),
            }),
            ..request(&[], None, count)
        };
        assert!(jobs_for(&with_key("sk-abc", 3)).is_ok());
        assert_eq!(jobs_for(&with_key("sk-abc", 3)).unwrap().len(), 3);
        // Same two guards as the other on-demand source.
        assert!(jobs_for(&with_key("sk-abc", 0)).is_err());
        assert!(jobs_for(&with_key("  ", 3)).is_err());
        assert!(jobs_for(&with_key("sk-abc", MAX_BATCH + 1)).is_err());
    }

    /// Cloud Mail wins when both on-demand sources are filled. Same reasoning as
    /// the pasted list: two sources both running is nobody's intent.
    #[test]
    fn cloud_mail_wins_over_temp_mail() {
        let both = AccountRegisterRequest {
            temp_mail: Some(TempMailConfig {
                base_url: String::new(),
                api_key: "sk-abc".to_owned(),
            }),
            // Incomplete on purpose: if this one is the one being read, it fails,
            // which is how the test can tell which branch ran.
            cloud_mail: Some(CloudMailConfig {
                base_url: "mail.example.com".to_owned(),
                ..cloud()
            }),
            ..request(&[], None, 2)
        };
        assert!(jobs_for(&both).is_err());
    }

    /// The domain is optional — the module reads it off the instance. An address
    /// and a way in is the whole of it, which is the point of "给个地址就行".
    #[test]
    fn an_address_and_administrator_credentials_are_enough() {
        let config = CloudMailConfig {
            base_url: "https://mail.example.com".to_owned(),
            admin_email: "admin@example.com".to_owned(),
            admin_password: "hunter2".to_owned(),
            ..Default::default()
        };
        assert!(jobs_for(&request(&[], Some(config), 2)).is_ok());
    }

    /// A run ends early only on failures that are actually consecutive.
    ///
    /// Counting totals instead would kill a batch that is mostly working —
    /// three scattered failures out of a hundred is a normal batch, three in a
    /// row is a batch where something is wrong with every account.
    #[test]
    fn a_success_clears_the_failure_streak() {
        let streak = std::sync::atomic::AtomicUsize::new(0);
        for ok in [false, false, true, false, false] {
            note_result(&streak, ok);
        }
        assert_eq!(streak.load(Ordering::SeqCst), 2, "中间那次成功该把连败清零");
        assert!(
            streak.load(Ordering::SeqCst) < GIVE_UP_AFTER,
            "这样的一批不该被中止"
        );

        let streak = std::sync::atomic::AtomicUsize::new(0);
        for ok in [false, false, false] {
            note_result(&streak, ok);
        }
        assert!(
            streak.load(Ordering::SeqCst) >= GIVE_UP_AFTER,
            "连着三个失败就该停"
        );

        // A success landing between concurrent failures still clears it — that
        // is the whole reason this is one atomic op rather than load-then-store.
        assert_eq!(note_result(&streak, true), 0);
    }

    /// A node that mihomo has not actually switched to is not an exit.
    ///
    /// The group is written before it is applied, so between those two moments
    /// it exists, reports `available`, and still points at the shared exit.
    /// Counting it would put the batch back on one address while the log said
    /// otherwise — the failure would look like success.
    #[test]
    fn only_the_outbounds_mihomo_has_switched_over_count() {
        use crate::mihomo::MihomoOutboundStatus;
        let outbound = |node: &str, selected: Option<&str>, available: bool| MihomoOutboundStatus {
            node: node.to_owned(),
            group: format!("MAJSOUL-OUT-{node}"),
            proxy_url: "http://mihomo:7901/".to_owned(),
            selected_node: selected.map(str::to_owned),
            available,
        };
        let exits = usable_exits(vec![
            outbound("日本 07", Some("日本 07"), true),
            // Written but not applied yet — still on the shared exit.
            outbound("新加坡 02", Some("MAJSOUL"), true),
            // The group is gone from mihomo entirely.
            outbound("香港 11", Some("香港 11"), false),
            // Never selected at all.
            outbound("美国 03", None, true),
        ]);
        assert_eq!(exits, vec!["日本 07".to_owned()]);
    }

    /// A failed account has to say which mailbox it burned.
    ///
    /// The regression this pins: the failure branch used to return before the
    /// address was read, so every failure on an on-demand source displayed the
    /// placeholder. A batch of sixteen that all failed at signup showed
    /// sixteen 「正在开邮箱」 and not one of the addresses whose verification
    /// codes had already been spent.
    #[test]
    fn a_failure_reports_the_address_the_module_opened() {
        let placeholder = "第 7/16 个（正在开邮箱）";
        let failed = serde_json::json!({
            "ok": false,
            "email": "hj28xk@example.com",
            "stage": "signup",
            "error": "151 ERR_CLIENT_VERSION",
        });
        assert_eq!(reported_email(&failed, placeholder), "hj28xk@example.com");

        // Failing before the mailbox exists is the one case the placeholder is
        // the only thing there is.
        let no_mailbox = serde_json::json!({"ok": false, "email": "", "stage": "mailbox"});
        assert_eq!(reported_email(&no_mailbox, placeholder), placeholder);
        assert_eq!(
            reported_email(&serde_json::json!({"ok": false}), placeholder),
            placeholder
        );
    }

    /// 151 fails a whole batch identically, so the message has to name it.
    #[test]
    fn giving_up_on_151_says_it_is_the_version_and_not_the_mailbox() {
        let hint = version_hint("[signup] 151 ERR_CLIENT_VERSION", "0.16.265");
        assert!(hint.contains("0.16.265"), "报的是哪个版本要说出来");
        assert!(hint.contains("补抓"), "指向真正能探出新版本的那条路");
        // Every other failure is about the account, the mailbox or the exit,
        // and a version lecture on those would send the operator the wrong way.
        assert_eq!(version_hint("[fetch_code] 取码超时", "0.16.265"), "");
        assert_eq!(
            version_hint("[signup] 1002 ERR_ACC_NOT_EXIST", "0.16.265"),
            ""
        );
    }

    /// Concurrency is clamped, not trusted.
    ///
    /// It arrives from a request body, and both ends of the range are real: 0
    /// would spawn nothing and hang on a semaphore that never issues, and a
    /// large number would have more sessions talking to Mahjong Soul from this
    /// deployment at once than it has exits to spread them over.
    #[test]
    fn concurrency_is_clamped_to_the_range_the_form_offers() {
        let clamp = |asked: usize, jobs: usize| asked.clamp(1, MAX_CONCURRENCY).min(jobs.max(1));
        assert_eq!(clamp(0, 10), 1, "0 会挂在永远发不出的名额上");
        assert_eq!(clamp(1, 10), 1);
        assert_eq!(clamp(16, 100), 16);
        assert_eq!(clamp(999, 100), MAX_CONCURRENCY);
        // Never wider than the work: eight permits for three accounts just
        // leaves five idle.
        assert_eq!(clamp(8, 3), 3);
    }

    /// Only the address, never the credential.
    ///
    /// This is what every log line and every status response shows, and the rest
    /// of the string reads that mailbox. Widening it by accident would publish
    /// the key to somebody's inbox in a panel that refreshes every few seconds.
    #[test]
    fn reads_the_address_out_of_a_credential_without_the_rest() {
        assert_eq!(
            address_of("abcd1234@outlook.com----Pa55word----client-id----refresh-token"),
            Some("abcd1234@outlook.com".to_owned())
        );
        assert_eq!(
            address_of("  someone@example.co.uk  "),
            Some("someone@example.co.uk".to_owned())
        );
        // A password containing an @ must not be mistaken for the address.
        assert_eq!(
            address_of("real@example.com----p@ssword"),
            Some("real@example.com".to_owned())
        );
        assert_eq!(address_of("no-address-here"), None);
        assert_eq!(address_of("broken@nodot"), None);
    }
}
