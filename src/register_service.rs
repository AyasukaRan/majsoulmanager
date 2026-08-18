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

/// What one run was asked to do.
#[derive(Clone, Debug, Deserialize)]
pub struct AccountRegisterRequest {
    /// One mailbox credential per entry. The e-mail address is read out of the
    /// string and the whole string is the key that reads its inbox, so this is
    /// a secret — it never appears in a log line or in a status response.
    pub mailboxes: Vec<String>,
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
        let mailboxes: Vec<String> = request
            .mailboxes
            .iter()
            .map(|line| line.trim().to_owned())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect();
        if mailboxes.is_empty() {
            return Err(WatchServiceError::InvalidConfig(
                "一个邮箱凭据都没有；每行一个，行首 # 会被跳过".to_owned(),
            ));
        }
        if mailboxes.len() > MAX_BATCH {
            return Err(WatchServiceError::InvalidConfig(format!(
                "一次最多 {MAX_BATCH} 个，这次给了 {}；一个号要几分钟，分批跑",
                mailboxes.len()
            )));
        }
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
                total: mailboxes.len(),
                started_at: Some(Utc::now()),
                ..Default::default()
            };
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let outcome = service.run(generation, module, mailboxes, request).await;
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
        mailboxes: Vec<String>,
        request: AccountRegisterRequest,
    ) -> Result<String, WatchServiceError> {
        self.report(
            WatchLogLevel::Info,
            format!(
                "开始注册 {} 个账号，用模块 {}@{}{}",
                mailboxes.len(),
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

        for mailbox in mailboxes {
            if self.generation.load(Ordering::SeqCst) != generation {
                worker.shutdown().await;
                return Ok("已停止".to_owned());
            }
            // The address is derived here only to have something to show and log
            // while the module works. The credential itself never leaves this
            // function.
            let address = address_of(&mailbox).unwrap_or_else(|| "（读不出邮箱）".to_owned());
            self.progress.write().current = Some(address.clone());
            self.report(WatchLogLevel::Info, format!("{address} 注册中"));

            let answer = worker
                .request(
                    "register",
                    serde_json::json!({
                        "mailbox": mailbox,
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

#[cfg(test)]
mod tests {
    use super::*;

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
