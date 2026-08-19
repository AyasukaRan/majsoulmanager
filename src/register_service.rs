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
    /// Open the addresses on a self-hosted instance instead of consuming a
    /// prepared list. Takes precedence over `mailboxes` when both are given.
    #[serde(default)]
    pub cloud_mail: Option<CloudMailConfig>,
    /// How many accounts to make. Only read in the `cloud_mail` case — with a
    /// list, the list length is the count.
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
    logs: Arc<WatchLogBuffer>,
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
        logs: Arc<WatchLogBuffer>,
    ) -> Self {
        Self {
            modules,
            accounts,
            logs,
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
            let outcome = service.run(generation, module, jobs, request).await;
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
        &self,
        generation: u64,
        module: crate::watch_service::ModuleRef,
        jobs: Vec<Option<String>>,
        request: AccountRegisterRequest,
    ) -> Result<String, WatchServiceError> {
        let total = jobs.len();
        self.report(
            WatchLogLevel::Info,
            format!(
                "开始注册 {} 个账号，用模块 {}@{}{}",
                total,
                module.name,
                module.version,
                if request.mimic {
                    "（拟真会话，每个约 3~5 分钟）"
                } else {
                    "（不拟真，只做对照用）"
                }
            ),
        );
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

        for (index, mailbox) in jobs.into_iter().enumerate() {
            if self.generation.load(Ordering::SeqCst) != generation {
                worker.shutdown().await;
                return Ok("已停止".to_owned());
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
            self.progress.write().current = Some(address.clone());
            self.report(WatchLogLevel::Info, format!("{address} 注册中"));

            let answer = worker
                .request(
                    "register",
                    serde_json::json!({
                        "mailbox": mailbox,
                        "cloud_mail": cloud,
                        "proxy": request.proxy,
                        "mimic": request.mimic,
                        "poll_tries": request.poll_tries,
                        "poll_interval": request.poll_interval,
                    }),
                )
                .await;
            let outcome = self.settle(&address, answer, &request);
            self.progress.write().outcomes.push(outcome);
        }
        worker.shutdown().await;
        let progress = self.progress.read();
        Ok(format!(
            "注册结束：成功 {}，失败 {}",
            progress.succeeded, progress.failed
        ))
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

        let email = value
            .get("email")
            .and_then(|email| email.as_str())
            .unwrap_or(address)
            .to_owned();
        let password = value
            .get("password")
            .and_then(|password| password.as_str())
            .unwrap_or_default()
            .to_owned();
        outcome.email = email.clone();
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
        let stored = StoredAccount {
            id: String::new(),
            username: email.clone(),
            password,
            purpose: request.purpose,
            note: request.note.clone(),
            enabled: false,
            node: String::new(),
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
    if let Some(cloud) = &request.cloud_mail {
        cloud.validate()?;
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
            "一个邮箱凭据都没有；每行一个，行首 # 会被跳过。或者填 Cloud Mail，让它现开".to_owned(),
        ));
    }
    if mailboxes.len() > MAX_BATCH {
        return Err(too_many(mailboxes.len()));
    }
    Ok(mailboxes)
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
            count,
            purpose: default_purpose(),
            note: String::new(),
            proxy: None,
            mimic: true,
            poll_tries: default_poll_tries(),
            poll_interval: default_poll_interval(),
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
