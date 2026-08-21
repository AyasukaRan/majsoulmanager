//! The Mahjong Soul accounts this deployment logs in with, kept where the
//! console can edit them.
//!
//! Until now an account reached the process only as `file:` or `env:` — a path
//! mounted into the container or a variable in the compose file — and the
//! password never entered a document the API could serve. That is still the
//! safer arrangement and it still works; what it is not is operable. Adding an
//! account meant an SSH session, an edit to `.env` and a restart, which is a
//! long way to go to put one more session on the re-fetch pool.
//!
//! So this holds them instead, and pays for it the way the rest of this codebase
//! pays for a stored credential: the file is `0600`, a read of the document
//! answers `***` rather than the password, and writing `***` back keeps what is
//! stored. That is the same treatment `subscription.json` and the 牌谱屋 API key
//! already get. It is a real reduction from "the password is only ever in a file
//! nothing here reads into a document" — worth stating rather than glossing.
//!
//! Accounts are separated by what they are for, because the two uses cannot
//! share one. Mahjong Soul allows a single session per account, so an account a
//! collector might log in with is an account the re-fetch pool must never touch:
//! the pool taking it disconnects the collector, the two then kick each other
//! off, and what is lost meanwhile is games that were being played at the time —
//! which nothing can fetch a second time.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::watch_service::{WatchServiceError, redacted_secret, restored_secret};

/// What an account is for. There is no "either": an account that both halves
/// might use is an account they will fight over.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountPurpose {
    /// Live collection. One collector instance, one account.
    Watch,
    /// The re-fetch pool, which opens one session per account it is given.
    Refetch,
}

impl AccountPurpose {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "watch" => Some(Self::Watch),
            "refetch" => Some(Self::Refetch),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredAccount {
    /// Assigned here when a row arrives without one, and never changed after.
    ///
    /// It exists so a password survives a rename. The restore that keeps a
    /// stored password when the console hands back `***` has to know which
    /// stored row the submitted one is, and the only other candidate — the
    /// username — is the field an operator renames. Matching on that made
    /// correcting a typo in an account name silently resolve to "no stored
    /// password", which the validator then rejected as 没有密码 while the box
    /// on screen showed `***`.
    #[serde(default)]
    pub id: String,
    pub username: String,
    /// Served as `***` and never in full. A save that hands the asterisks back
    /// keeps whatever is stored, so editing the note does not wipe the password.
    pub password: String,
    pub purpose: AccountPurpose,
    /// Whatever the operator wants to remember about it — which phone number it
    /// is registered to, which of them is the paid one. Never used for anything.
    #[serde(default)]
    pub note: String,
    /// Off means "do not log in with this", not "forget it". A collector's
    /// account stays reserved either way: an operator may switch it back on at
    /// any moment, and the re-fetch pool must not have taken it meanwhile.
    pub enabled: bool,
    /// Which mihomo node this account's session goes out of, by the node's own
    /// name. Empty means "whatever this half is on", which is what every
    /// account was before this field existed.
    ///
    /// It is a name and not an index because the operator picks it from the
    /// list mihomo reports, and because a subscription that reorders its nodes
    /// must not silently move an account onto a different one. A name that has
    /// since disappeared from the subscription resolves to nothing and the
    /// account falls back to the lane — a pool session on the wrong node beats
    /// a pool session that cannot dial at all.
    #[serde(default)]
    pub node: String,
    /// When Mahjong Soul answered a login with `503`, if it ever did.
    ///
    /// The one verdict worth keeping across a restart. [`AccountHealth`] is
    /// deliberately runtime-only, and for every other state that is right — a
    /// refusal may have been `1005 ERR_ACC_ALREADY_LOGIN`, an unreachable may
    /// have been the proxy blinking. But `503` is not a conclusion about a
    /// login, it is a fact about the account, and it never clears. Forgetting it
    /// means every restart spends a session slot finding it out again.
    #[serde(default)]
    pub banned_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AccountDocument {
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub accounts: Vec<StoredAccount>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

impl AccountDocument {
    /// The document as the API serves it: every password replaced by three
    /// asterisks. The only form that leaves this process.
    pub fn published(mut self) -> Self {
        for account in &mut self.accounts {
            account.password = redacted_secret(Some(&account.password)).unwrap_or_default();
        }
        self
    }

    fn validate(&self) -> Result<(), WatchServiceError> {
        let mut seen = HashSet::new();
        for account in &self.accounts {
            let username = account.username.trim();
            if username.is_empty() {
                return Err(WatchServiceError::InvalidConfig("账号不能为空".to_owned()));
            }
            if username.contains(char::is_whitespace) || username.contains('/') {
                return Err(WatchServiceError::InvalidConfig(format!(
                    "账号 {username} 含有空白或 /，这两个都是引用里的分隔符"
                )));
            }
            if account.password.trim().is_empty() {
                return Err(WatchServiceError::InvalidConfig(format!(
                    "账号 {username} 没有密码"
                )));
            }
            // One account cannot appear twice, whatever the two rows say it is
            // for. The whole point of the split is that a session is exclusive;
            // an account listed under both purposes is the collision this file
            // exists to prevent, spelled out.
            // Compared case-insensitively: these are e-mail logins, Mahjong
            // Soul treats them as one account, and a pair differing only in
            // case is the collision this check exists to catch wearing a
            // disguise.
            if !seen.insert(username.to_lowercase()) {
                return Err(WatchServiceError::InvalidConfig(format!(
                    "账号 {username} 出现了两次；雀魂一个账号只能开一个会话，\
                     两处同时用会互相把对方踢下线"
                )));
            }
        }
        Ok(())
    }
}

/// A reference to accounts the console manages, as it appears in a service's
/// `account_secret_ref`.
///
/// `pool:refetch` is every enabled account of that purpose, which is what the
/// pool wants. `pool:watch/<username>` is one, which is what a collector needs:
/// several collectors pointed at the same many-account reference would each take
/// its first line and disconnect each other.
pub enum PoolRef<'a> {
    All(AccountPurpose),
    One(AccountPurpose, &'a str),
}

impl<'a> PoolRef<'a> {
    /// Parses the part after `pool:`, or `None` if it names neither.
    pub fn parse(target: &'a str) -> Option<Self> {
        match target.split_once('/') {
            Some((purpose, username)) if !username.is_empty() => {
                Some(Self::One(AccountPurpose::parse(purpose)?, username))
            }
            Some(_) => None,
            None => Some(Self::All(AccountPurpose::parse(target)?)),
        }
    }
}

/// What the last attempt to log in with an account did.
///
/// Deliberately not a boolean. "Failed" lumps together three things an operator
/// has to act on differently: an account Mahjong Soul will never let in again, a
/// refusal that says nothing bad about the account (it is logged in elsewhere,
/// or this client's version is stale), and a network that was down. Only the
/// first is worth replacing an account over.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountState {
    /// Nothing has tried to log in since this process started. Not the same as
    /// healthy, and shown as its own thing rather than as a green tick.
    #[default]
    Unknown,
    Ok,
    /// `503`. Terminal: the session that meets it will meet it again on every
    /// retry, for ever.
    Banned,
    /// The server answered and refused with something other than `503`.
    ///
    /// Kept apart from `Unreachable` because an answer is evidence and a timeout
    /// is not — but *not* called rejected, because several of these codes say
    /// nothing bad about the account: `1005 ERR_ACC_ALREADY_LOGIN` means it is
    /// logged in somewhere else and `151 ERR_CLIENT_VERSION` is this client's
    /// own fault. The code is in `detail`; guessing at its meaning here would
    /// put a wrong verdict on the screen.
    Refused,
    /// Nothing answered: the gateway, the network, the proxy.
    Unreachable,
}

impl AccountState {
    /// Whether logging in again could plausibly work.
    ///
    /// Only `Banned` is terminal. A refusal might be the account being logged in
    /// elsewhere, which clears on its own, and an unreachable gateway is not
    /// about the account at all — retrying both is right.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Banned)
    }
}

/// The last thing that happened when this deployment logged in with an account.
///
/// Runtime only, never persisted. It describes this process's experience, and a
/// verdict restored from disk after a restart would be a claim about a login
/// nothing here made.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AccountHealth {
    pub state: AccountState,
    /// When that happened, or `None` for an account nothing has tried. A
    /// timestamp of "now" for an untried account would read as a fresh verdict.
    pub at: Option<DateTime<Utc>>,
    /// The error as it was reported, for a human to read. Empty when `Ok`.
    pub detail: String,
    /// Consecutive failures, reset by a success. An account failing once is
    /// weather; the same one failing forty times is the thing to look at.
    pub failures: u32,
}

pub struct AccountPool {
    path: PathBuf,
    document: RwLock<AccountDocument>,
    /// Keyed by lower-cased username, the same way every other lookup in this
    /// file treats these — they are e-mail logins and Mahjong Soul folds case.
    health: RwLock<HashMap<String, AccountHealth>>,
}

impl AccountPool {
    pub fn open(data_dir: &Path) -> Result<Self, WatchServiceError> {
        let directory = data_dir.join("accounts");
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("pool.json");
        let document = match std::fs::read(&path) {
            Ok(bytes) => {
                let document: AccountDocument = serde_json::from_slice(&bytes)?;
                document.validate()?;
                document
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                AccountDocument::default()
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            document: RwLock::new(document),
            health: RwLock::new(HashMap::new()),
        })
    }

    pub fn published(&self) -> AccountDocument {
        self.document.read().clone().published()
    }

    /// Records what logging in with an account just did.
    ///
    /// Called by whichever half opened the session. The pool is the only place
    /// both halves already share, which is why the verdict lives here rather
    /// than in either service's own status.
    pub fn record_login(&self, username: &str, state: AccountState, detail: impl Into<String>) {
        let detail = detail.into();
        let mut health = self.health.write();
        let entry = health.entry(username.to_lowercase()).or_default();
        entry.failures = if state == AccountState::Ok {
            0
        } else {
            entry.failures.saturating_add(1)
        };
        entry.state = state;
        entry.at = Some(Utc::now());
        entry.detail = detail;
    }

    /// Switches off a pool account Mahjong Soul says is banned, and records when.
    ///
    /// `503` is the one refusal that never clears, so the account is not coming
    /// back and the only question is whether this deployment remembers that
    /// across a restart. Without this it does not: the health map is runtime
    /// state by design, so every start hands a session slot to an account that
    /// will spend it being refused again — and tells Mahjong Soul, once per
    /// restart, that this address is still trying banned credentials.
    ///
    /// Only the pool's own. A collector is bound to one account by its
    /// configuration, and switching that off would end live collection on its
    /// own initiative — the one thing here that cannot be done again. That half
    /// keeps retrying and says why on the account list instead.
    ///
    /// Returns whether anything changed, so the caller can log it once rather
    /// than on every reconnect.
    pub fn retire_banned(&self, username: &str) -> Result<bool, WatchServiceError> {
        let mut document = self.document.write();
        let mut next = document.clone();
        let Some(account) = next
            .accounts
            .iter_mut()
            .find(|account| account.username.eq_ignore_ascii_case(username.trim()))
        else {
            return Ok(false);
        };
        if account.purpose != AccountPurpose::Refetch || !account.enabled {
            return Ok(false);
        }
        account.enabled = false;
        account.banned_at = Some(Utc::now());
        next.revision = next.revision.saturating_add(1);
        next.updated_at = Some(Utc::now());
        persist_secret_json(&self.path, &next)?;
        *document = next;
        Ok(true)
    }

    /// Every account's last known state, by username as the document spells it.
    ///
    /// Driven by the document rather than by the health map, so an account that
    /// nothing has tried yet appears as `Unknown` instead of not appearing —
    /// "no row" and "not tried" look identical on a screen, and only one of them
    /// is a reason to worry.
    pub fn health(&self) -> BTreeMap<String, AccountHealth> {
        let health = self.health.read();
        self.document
            .read()
            .accounts
            .iter()
            .map(|account| {
                let known = health
                    .get(&account.username.to_lowercase())
                    .cloned()
                    .unwrap_or_default();
                (account.username.clone(), known)
            })
            .collect()
    }

    /// Adds accounts the registrar just created, without the console's revision
    /// check.
    ///
    /// A registration run lasts tens of minutes and produces one account at a
    /// time. Demanding the revision an operator's browser last read would throw
    /// away every account after the first note they edit meanwhile — and an
    /// account that was created and then not stored is gone: its password only
    /// ever existed in the run that made it.
    ///
    /// Appending is safe where a whole-document PUT is not: no existing row is
    /// read, changed or reordered, and a username already in the pool is skipped
    /// rather than overwritten. Returns how many were added.
    pub fn append(&self, accounts: Vec<StoredAccount>) -> Result<usize, WatchServiceError> {
        if accounts.is_empty() {
            return Ok(0);
        }
        let mut document = self.document.write();
        // Built into a candidate document and validated before anything is
        // committed: a bad row must not leave the in-memory pool holding
        // accounts that were never written to disk.
        let mut next = document.clone();
        let mut seen: HashSet<String> = next
            .accounts
            .iter()
            .map(|account| account.username.trim().to_lowercase())
            .collect();
        let mut added = 0;
        for mut account in accounts {
            account.username = account.username.trim().to_owned();
            // `insert` is the duplicate check: it covers both "already in the
            // pool" and "twice in this batch", and the second one is real —
            // two mailbox lines can carry the same address.
            if account.username.is_empty() || !seen.insert(account.username.to_lowercase()) {
                continue;
            }
            if account.id.is_empty() {
                account.id = uuid::Uuid::new_v4().to_string();
            }
            next.accounts.push(account);
            added += 1;
        }
        if added == 0 {
            return Ok(0);
        }
        next.validate()?;
        next.revision = next.revision.saturating_add(1);
        next.updated_at = Some(Utc::now());
        persist_secret_json(&self.path, &next)?;
        *document = next;
        Ok(added)
    }

    pub fn update(&self, next: AccountDocument) -> Result<AccountDocument, WatchServiceError> {
        let mut next = next;
        let submitted = next.revision;
        let mut document = self.document.write();
        if submitted != document.revision {
            return Err(WatchServiceError::RevisionConflict {
                submitted,
                current: document.revision,
            });
        }
        // Restored before validation, so a document whose passwords are all
        // asterisks — which is every document the console ever read — is judged
        // on what it will actually store.
        let stored: Vec<(String, String, Option<DateTime<Utc>>)> = document
            .accounts
            .iter()
            .map(|account| {
                (
                    account.id.clone(),
                    account.password.clone(),
                    account.banned_at,
                )
            })
            .collect();
        for account in &mut next.accounts {
            account.username = account.username.trim().to_owned();
            let previous = stored
                .iter()
                .find(|(id, _, _)| !id.is_empty() && *id == account.id);
            account.password = restored_secret(
                Some(account.password.clone()),
                previous.map(|(_, password, _)| password.as_str()),
            )
            .unwrap_or_default();
            // Restored the same way a password is, and for a related reason: it
            // is not a form field. It records what Mahjong Soul answered, and a
            // console that never sent it back — or sent back a stale copy —
            // must not be able to erase that.
            //
            // Switching the account on is the one thing that clears it, because
            // that is an operator saying this account works, which is the only
            // claim that outranks the observation.
            account.banned_at = if account.enabled {
                None
            } else {
                previous.and_then(|(_, _, banned_at)| *banned_at)
            };
            if account.id.is_empty() {
                account.id = uuid::Uuid::new_v4().to_string();
            }
        }
        // A row the console handed back with `***` and no matching stored row is
        // a new account with no password typed into it. Saying so beats the
        // validator's 没有密码, which points at a box that is not empty.
        if let Some(account) = next
            .accounts
            .iter()
            .find(|account| account.password == "***")
        {
            return Err(WatchServiceError::InvalidConfig(format!(
                "账号 {} 是新加的，密码栏里的 *** 不是密码，请输入真正的密码",
                account.username
            )));
        }
        next.validate()?;
        next.revision = document.revision.saturating_add(1);
        next.updated_at = Some(Utc::now());
        persist_secret_json(&self.path, &next)?;
        *document = next.clone();
        Ok(next.published())
    }

    /// The accounts a reference resolves to, in stored order, disabled ones
    /// dropped. Empty is not an error here — the caller says what an empty
    /// answer means for it.
    pub fn resolve(&self, reference: &PoolRef<'_>) -> Vec<(String, String)> {
        let document = self.document.read();
        document
            .accounts
            .iter()
            .filter(|account| account.enabled)
            .filter(|account| match reference {
                PoolRef::All(purpose) => account.purpose == *purpose,
                // Case-insensitive, the same as the rule that stops one account
                // being filed twice. These are e-mail logins; a reference that
                // differs only in case names the same session, and resolving it
                // to nothing would make a collector fail to start over a capital
                // letter.
                PoolRef::One(purpose, username) => {
                    account.purpose == *purpose && account.username.eq_ignore_ascii_case(username)
                }
            })
            .map(|account| (account.username.clone(), account.password.clone()))
            .collect()
    }

    /// Every username of a purpose, enabled or not.
    ///
    /// Disabled ones are included deliberately, and only this method includes
    /// them: it answers "could a collector log in with this account", which an
    /// operator makes true by clicking one checkbox. The re-fetch pool asks it
    /// to decide what it may take, and taking an account that is switched back
    /// on a minute later is the failure the split exists to prevent.
    pub fn reserved(&self, purpose: AccountPurpose) -> HashSet<String> {
        self.document
            .read()
            .accounts
            .iter()
            .filter(|account| account.purpose == purpose)
            .map(|account| account.username.clone())
            .collect()
    }

    /// The node an account is bound to, or `None` for "follow this half".
    ///
    /// Looked up by username rather than carried alongside the password,
    /// because the password reaches the pool through `load_accounts`, which
    /// also serves `file:` and `env:` references — and an account that came
    /// from a file has no row here to carry a node on. Those resolve to `None`,
    /// which is the same answer they gave before this existed.
    pub fn node_for(&self, username: &str) -> Option<String> {
        self.document
            .read()
            .accounts
            .iter()
            .find(|account| account.username.eq_ignore_ascii_case(username))
            .map(|account| account.node.trim().to_owned())
            .filter(|node| !node.is_empty())
    }

    /// Which node each enabled re-fetch account is bound to, for the balancer to
    /// count from. Accounts on the shared lane are left out: they name no node,
    /// and nothing here can attribute what they fetched.
    pub fn refetch_bindings(&self) -> Vec<(String, String)> {
        self.document
            .read()
            .accounts
            .iter()
            .filter(|account| account.enabled && account.purpose == AccountPurpose::Refetch)
            .filter_map(|account| {
                let node = account.node.trim();
                (!node.is_empty()).then(|| (account.username.clone(), node.to_owned()))
            })
            .collect()
    }

    /// Re-points named accounts at other nodes, in one write.
    ///
    /// Only the binding moves. Nothing else about the account is touched and no
    /// node is created or removed here — a move to a node no listener exists for
    /// would put that session on the shared lane, so the caller is required to
    /// pick from nodes that are already bound. See the balancer, which is the
    /// only caller and holds every node at one account or more for exactly that
    /// reason.
    pub fn rebind_nodes(&self, moves: &[(String, String)]) -> Result<usize, WatchServiceError> {
        if moves.is_empty() {
            return Ok(0);
        }
        let mut document = self.document.read().clone();
        let mut moved = 0usize;
        for (username, node) in moves {
            if let Some(account) = document.accounts.iter_mut().find(|account| {
                account.username.eq_ignore_ascii_case(username)
                    && account.purpose == AccountPurpose::Refetch
            }) && account.node != *node
            {
                account.node = node.clone();
                moved += 1;
            }
        }
        // Through `update` like every other write: it validates, bumps the
        // revision and keeps the passwords the console never saw.
        self.update(document)?;
        Ok(moved)
    }

    /// Every distinct node the enabled re-fetch accounts name, in a stable
    /// order. What mihomo has to grow an outbound for.
    ///
    /// Only the re-fetch half: live collection is one session on one node and
    /// already has a lane of its own, and a collector that started before a
    /// node was added goes on using the exit it logged in through anyway.
    pub fn refetch_nodes(&self) -> Vec<String> {
        let mut nodes: Vec<String> = self
            .document
            .read()
            .accounts
            .iter()
            .filter(|account| account.enabled && account.purpose == AccountPurpose::Refetch)
            .map(|account| account.node.trim().to_owned())
            .filter(|node| !node.is_empty())
            .collect();
        nodes.sort();
        nodes.dedup();
        nodes
    }

    /// Spreads the enabled re-fetch accounts over `nodes`, round robin.
    ///
    /// The pool's whole point is that ninety-odd accounts do not go out of one
    /// address, and until now that meant an operator picking a node in a
    /// dropdown ninety times — so in practice most of them were left on
    /// 「跟随补抓出站」, which is one address. This is that job done in one
    /// press, against the nodes mihomo says can currently reach Mahjong Soul.
    ///
    /// Round robin over a caller-shuffled list rather than one node per account:
    /// there are more accounts than nodes and more nodes than listeners, so
    /// sharing is the normal case and the only question is whether it is even.
    ///
    /// Disabled accounts are assigned too. They are accounts the operator may
    /// switch back on, and leaving them out would mean the spread changed the
    /// moment one was re-enabled. Watch accounts are never touched: a collector
    /// is one session that already has a lane.
    ///
    /// An empty `nodes` clears the bindings, which is a real answer — it puts
    /// every session back on the re-fetch lane, and it is what an operator whose
    /// subscription just expired wants rather than ninety sessions dialling
    /// listeners for nodes that no longer exist.
    pub fn spread_over(&self, nodes: &[String]) -> Result<usize, WatchServiceError> {
        let mut document = self.document.read().clone();
        let mut spread = 0usize;
        for (index, account) in document
            .accounts
            .iter_mut()
            .filter(|account| account.purpose == AccountPurpose::Refetch)
            .enumerate()
        {
            account.node = match nodes.is_empty() {
                true => String::new(),
                false => nodes[index % nodes.len()].clone(),
            };
            spread += 1;
        }
        // Through `update`, never straight to the file: it is what validates,
        // bumps the revision and keeps the passwords the console never saw.
        self.update(document)?;
        Ok(spread)
    }

    /// Every re-fetch account as `username,password`, one per line.
    ///
    /// The one place a stored password leaves this process, and it exists
    /// because the pool is the only copy: an operator who has registered ninety
    /// accounts through the console has them nowhere else, and a data directory
    /// is not a backup. The format is the one a `file:` or `env:` secret already
    /// uses, so what comes out goes back in.
    ///
    /// Disabled accounts are included and banned ones are not: a `503` is
    /// permanent, and an export is something you keep, so carrying accounts
    /// Mahjong Soul will never accept again into the copy you keep is carrying
    /// a list of dead accounts forward for ever.
    pub fn export_refetch(&self) -> String {
        let document = self.document.read();
        let mut out = String::new();
        for account in &document.accounts {
            if account.purpose != AccountPurpose::Refetch || account.banned_at.is_some() {
                continue;
            }
            out.push_str(account.username.trim());
            out.push(',');
            out.push_str(&account.password);
            out.push('\n');
        }
        out
    }

    /// The account a submitted document would take away from a collector, if
    /// there is one. `in_use` is lowercased, as `referenced_accounts` returns
    /// it.
    ///
    /// Only an account this store actually holds can be taken away here. A
    /// collector reached through `file:` or `env:` names one that was never in
    /// the pool, and measuring the submission against those reads as "you just
    /// deleted it" every single time — which is not a refusal to delete, it is
    /// the page refusing to save at all.
    pub fn taken_from_a_collector(
        &self,
        next: &AccountDocument,
        in_use: &HashSet<String>,
    ) -> Option<String> {
        self.document
            .read()
            .accounts
            .iter()
            .find(|account| {
                in_use.contains(&account.username.to_lowercase())
                    && !next
                        .accounts
                        .iter()
                        .any(|kept| kept.username.eq_ignore_ascii_case(&account.username))
            })
            .map(|account| account.username.clone())
    }

    /// Every password held, for the log buffer to redact. Registered at start
    /// rather than at use, because a password can reach a log through a module's
    /// stderr or an error chain that never passed through this file.
    pub fn secrets(&self) -> Vec<String> {
        self.document
            .read()
            .accounts
            .iter()
            .map(|account| account.password.clone())
            .collect()
    }
}

/// Written `0600`, like the mihomo subscription: the file holds passwords in
/// full and the data directory is shared with every other part of the
/// deployment.
fn persist_secret_json<T: Serialize>(path: &Path, value: &T) -> Result<(), WatchServiceError> {
    let mut body = serde_json::to_vec_pretty(value)?;
    body.push(b'\n');
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    std::fs::write(&temporary, &body)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Set on the temporary file, before the rename: a mode applied
        // afterwards leaves a window in which the real path is world readable.
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(username: &str, purpose: AccountPurpose) -> StoredAccount {
        StoredAccount {
            id: String::new(),
            username: username.to_owned(),
            password: format!("{username}-password"),
            purpose,
            note: String::new(),
            enabled: true,
            node: String::new(),
            banned_at: None,
        }
    }

    /// Every account appears, whether or not anything has logged in with it.
    ///
    /// The absent row is the failure worth guarding against: "no entry" and
    /// "never tried" render identically on a screen, and only one of them is a
    /// reason to go and look.
    #[test]
    fn an_account_nothing_has_tried_is_reported_as_unknown_rather_than_missing() {
        let store = pool(vec![
            account("tried@example.com", AccountPurpose::Refetch),
            account("untouched@example.com", AccountPurpose::Refetch),
        ]);
        store.record_login("tried@example.com", AccountState::Ok, "");

        let health = store.health();
        assert_eq!(health.len(), 2, "every account in the document has a row");
        assert_eq!(health["tried@example.com"].state, AccountState::Ok);
        assert!(health["tried@example.com"].at.is_some());

        let untouched = &health["untouched@example.com"];
        assert_eq!(untouched.state, AccountState::Unknown);
        assert!(
            untouched.at.is_none(),
            "an untried account has no verdict, and no time for one either"
        );
    }

    /// Case is not identity anywhere else in this file, and it is not here.
    /// A collector configured with one spelling and the pool document with
    /// another name the same Mahjong Soul account.
    #[test]
    fn a_verdict_recorded_under_one_spelling_is_found_under_the_other() {
        let store = pool(vec![account("Live@Example.com", AccountPurpose::Watch)]);
        store.record_login("live@example.com", AccountState::Banned, "503 banned");

        let health = store.health();
        // Keyed by the document's spelling, which is what an operator reads.
        assert_eq!(health["Live@Example.com"].state, AccountState::Banned);
        assert_eq!(health["Live@Example.com"].detail, "503 banned");
    }

    /// Only a ban is terminal.
    ///
    /// The pool retires a session on `is_terminal`, so a `true` here for
    /// `Refused` would end a session over `1005 ERR_ACC_ALREADY_LOGIN` — an
    /// account that is perfectly good and merely logged in somewhere else.
    #[test]
    fn only_a_ban_stops_the_pool_from_trying_again() {
        assert!(AccountState::Banned.is_terminal());
        assert!(!AccountState::Refused.is_terminal());
        assert!(!AccountState::Unreachable.is_terminal());
        assert!(!AccountState::Unknown.is_terminal());
        assert!(!AccountState::Ok.is_terminal());
    }

    /// Consecutive failures accumulate and a success clears them, because the
    /// number is there to tell weather from a wall.
    #[test]
    fn a_run_of_failures_is_counted_and_a_success_ends_it() {
        let store = pool(vec![account("flaky@example.com", AccountPurpose::Refetch)]);
        for _ in 0..3 {
            store.record_login("flaky@example.com", AccountState::Unreachable, "网关超时");
        }
        assert_eq!(store.health()["flaky@example.com"].failures, 3);

        store.record_login("flaky@example.com", AccountState::Ok, "");
        let recovered = &store.health()["flaky@example.com"];
        assert_eq!(recovered.failures, 0);
        assert_eq!(recovered.state, AccountState::Ok);
        assert!(
            recovered.detail.is_empty(),
            "a success carries no error text"
        );
    }

    /// What the registrar needs from `append`: a repeat is skipped, not an
    /// error, and it does not take the rest of the batch with it.
    ///
    /// The registrar calls this once per account, mid-run, and a run is normally
    /// started over a mailbox file that partly overlaps the last one. If a
    /// duplicate failed the call, the accounts after it in that batch would be
    /// created on Mahjong Soul's side and then dropped — and a created account
    /// that was not stored is gone, because its password only existed inside the
    /// run. The revision has to move too, or a console reading r0 would think
    /// nothing had happened.
    #[test]
    fn appending_skips_what_is_already_there_and_keeps_the_rest() {
        let store = pool(vec![account("taken@example.com", AccountPurpose::Refetch)]);
        let before = store.published().revision;

        let added = store
            .append(vec![
                account("taken@example.com", AccountPurpose::Refetch),
                account("fresh@example.com", AccountPurpose::Refetch),
                // Twice in one batch, which two mailbox lines carrying the same
                // address produce. Both must not land: the document would fail
                // its own duplicate check and the whole append would be lost.
                account("fresh@example.com", AccountPurpose::Refetch),
                // Case is not a difference: Mahjong Soul treats these as one
                // account, so a pair differing only in case is the collision the
                // pool exists to prevent wearing a disguise.
                account("TAKEN@example.com", AccountPurpose::Refetch),
            ])
            .unwrap();
        assert_eq!(added, 1, "only the one that was not already there");

        let document = store.published();
        assert_eq!(document.accounts.len(), 2);
        assert!(
            document.accounts.iter().all(|row| !row.id.is_empty()),
            "every appended row gets an id, which is what keeps its password \
             through a later rename"
        );
        assert!(
            document.revision > before,
            "a console holding the old revision has to be told the pool moved"
        );

        // Nothing at all is not an error either — it is what re-running the same
        // mailbox file looks like.
        assert_eq!(
            store
                .append(vec![account("fresh@example.com", AccountPurpose::Refetch)])
                .unwrap(),
            0
        );
    }

    fn pool(accounts: Vec<StoredAccount>) -> AccountPool {
        let directory =
            std::env::temp_dir().join(format!("mjai-accounts-{}", uuid::Uuid::new_v4()));
        let store = AccountPool::open(&directory).unwrap();
        store
            .update(AccountDocument {
                revision: 0,
                accounts,
                updated_at: None,
            })
            .unwrap();
        store
    }

    /// The console reads the document, edits one field and submits all of it
    /// back. Without the restore, the first save after any read would replace
    /// every password with the asterisks it was shown — and the passwords are
    /// the only copy.
    #[test]
    fn a_read_edit_write_round_trip_keeps_every_password() {
        let store = pool(vec![
            account("live@example.com", AccountPurpose::Watch),
            account("pool-a@example.com", AccountPurpose::Refetch),
        ]);
        let mut published = store.published();
        assert!(
            published
                .accounts
                .iter()
                .all(|account| account.password == "***"),
            "a password left the process"
        );

        published.accounts[1].note = "第二个池子".into();
        store.update(published).unwrap();
        assert_eq!(
            store.resolve(&PoolRef::All(AccountPurpose::Refetch)),
            [(
                "pool-a@example.com".to_owned(),
                "pool-a@example.com-password".to_owned()
            )]
        );

        // And typing a new one replaces it.
        let mut published = store.published();
        published.accounts[1].password = "typed-a-new-one".into();
        store.update(published).unwrap();
        assert_eq!(
            store.resolve(&PoolRef::All(AccountPurpose::Refetch))[0].1,
            "typed-a-new-one"
        );
    }

    /// Renaming an account keeps its password.
    ///
    /// The console only ever holds `***`, so a rename that lost the match to
    /// the stored row would submit three asterisks as the password — which the
    /// validator then rejects, pointing at a field the operator can see is not
    /// empty. Correcting a typo in an account name has to work.
    #[test]
    fn renaming_an_account_keeps_the_password_it_never_saw() {
        let store = pool(vec![account("typo@example.com", AccountPurpose::Refetch)]);
        let mut published = store.published();
        assert_eq!(published.accounts[0].password, "***");
        assert!(!published.accounts[0].id.is_empty(), "an id was assigned");
        published.accounts[0].username = "fixed@example.com".into();
        store.update(published).unwrap();

        assert_eq!(
            store.resolve(&PoolRef::All(AccountPurpose::Refetch)),
            [(
                "fixed@example.com".to_owned(),
                "typo@example.com-password".to_owned()
            )]
        );

        // A genuinely new row carrying the asterisks says so, rather than
        // failing as "no password" on a box that shows three characters.
        let mut published = store.published();
        published.accounts.push(StoredAccount {
            password: "***".into(),
            ..account("fresh@example.com", AccountPurpose::Refetch)
        });
        let refused = store.update(published).unwrap_err().to_string();
        assert!(refused.contains("fresh@example.com"), "{refused}");
    }

    /// One account cannot be in both halves. Mahjong Soul allows one session
    /// per account, so the two would kick each other off for as long as both
    /// ran, and what is lost meanwhile is live games nothing can fetch twice.
    #[test]
    fn one_account_cannot_serve_both_halves() {
        let both = AccountDocument {
            revision: 0,
            accounts: vec![
                account("shared@example.com", AccountPurpose::Watch),
                // Differing only in case. These are e-mail logins and Mahjong
                // Soul treats them as one account.
                account("Shared@Example.com", AccountPurpose::Refetch),
            ],
            updated_at: None,
        };
        assert!(both.validate().is_err());

        // And a collector's account stays reserved while it is switched off,
        // because switching it back on is one click.
        let store = pool(vec![StoredAccount {
            enabled: false,
            ..account("resting@example.com", AccountPurpose::Watch)
        }]);
        assert!(
            store
                .resolve(&PoolRef::All(AccountPurpose::Watch))
                .is_empty(),
            "a disabled account must not be logged in with"
        );
        assert!(
            store
                .reserved(AccountPurpose::Watch)
                .contains("resting@example.com"),
            "a disabled account is still spoken for"
        );
    }

    /// A collector pointed at a `file:` or `env:` reference names an account
    /// that is not in the pool and never was. Counting it as one this save
    /// deleted made every save fail: nothing could be added, edited or
    /// imported while any instance used a reference the pool does not own.
    #[test]
    fn only_an_account_the_pool_holds_can_be_taken_from_a_collector() {
        let store = pool(vec![account("live@example.com", AccountPurpose::Watch)]);
        // As `referenced_accounts` answers it: lowercased, and holding one
        // account this store has never seen.
        let in_use: HashSet<String> = ["live@example.com", "from-a-file@example.com"]
            .into_iter()
            .map(str::to_owned)
            .collect();

        // Adding a re-fetch account is a save like any other.
        let mut grown = store.published();
        grown
            .accounts
            .push(account("pool-a@example.com", AccountPurpose::Refetch));
        assert_eq!(store.taken_from_a_collector(&grown, &in_use), None);

        // Dropping the one the collector actually reads is still refused.
        let emptied = AccountDocument {
            revision: grown.revision,
            accounts: Vec::new(),
            updated_at: None,
        };
        assert_eq!(
            store.taken_from_a_collector(&emptied, &in_use).as_deref(),
            Some("live@example.com")
        );
    }

    /// A collector names one account; the pool names a whole purpose. Several
    /// collectors pointed at one many-account reference would each take its
    /// first line and disconnect each other.
    #[test]
    fn a_reference_names_either_one_account_or_a_whole_purpose() {
        let store = pool(vec![
            account("one@example.com", AccountPurpose::Watch),
            account("two@example.com", AccountPurpose::Watch),
        ]);
        let named = PoolRef::parse("watch/two@example.com").expect("a valid reference");
        assert_eq!(
            store.resolve(&named),
            [(
                "two@example.com".to_owned(),
                "two@example.com-password".to_owned()
            )]
        );
        assert_eq!(
            store
                .resolve(&PoolRef::parse("watch").expect("a valid reference"))
                .len(),
            2
        );
        // A purpose that does not exist is not a reference to everything.
        assert!(PoolRef::parse("collector").is_none());
        assert!(PoolRef::parse("watch/").is_none());
        // And a named account of the wrong purpose resolves to nothing rather
        // than to the account.
        assert!(
            store
                .resolve(&PoolRef::parse("refetch/two@example.com").unwrap())
                .is_empty()
        );
    }

    /// A `503` retires the account, and the retirement outlives this process.
    ///
    /// The health map is runtime state on purpose, so without a stored mark the
    /// next start would hand a session slot to an account that will spend it
    /// being refused again — from this address, with credentials Mahjong Soul
    /// has already banned.
    #[test]
    fn a_banned_pool_account_is_switched_off_and_stays_off() {
        let store = pool(vec![
            account("pool@example.com", AccountPurpose::Refetch),
            account("collector@example.com", AccountPurpose::Watch),
        ]);

        assert!(
            store.retire_banned("POOL@example.com").unwrap(),
            "大小写不该影响"
        );
        let document = store.published();
        let retired = document
            .accounts
            .iter()
            .find(|a| a.username == "pool@example.com")
            .unwrap();
        assert!(!retired.enabled);
        assert!(retired.banned_at.is_some());
        // Gone from what the pool would log in with, which is the point.
        assert!(
            store
                .resolve(&PoolRef::parse("refetch").unwrap())
                .is_empty()
        );

        // Idempotent: a second session meeting the same ban must not move the
        // revision again, or every reconnect would look like an edit.
        assert!(!store.retire_banned("pool@example.com").unwrap());

        // A collector's account is left alone. Switching it off would end live
        // collection on this process's own initiative — the one thing here that
        // cannot be done again.
        assert!(!store.retire_banned("collector@example.com").unwrap());
        assert!(
            store
                .published()
                .accounts
                .iter()
                .find(|a| a.username == "collector@example.com")
                .unwrap()
                .enabled
        );

        // A console save cannot erase the mark: it is an observation, not a form
        // field, and the browser may be holding a copy from before the ban.
        let mut saved = store.published();
        for account in &mut saved.accounts {
            account.note = "改了个备注".to_owned();
        }
        store.update(saved).unwrap();
        assert!(
            store
                .published()
                .accounts
                .iter()
                .find(|a| a.username == "pool@example.com")
                .unwrap()
                .banned_at
                .is_some(),
            "保存备注把封禁记录抹掉了"
        );

        // Switching it back on is the one thing that clears it — an operator
        // saying the account works outranks what was observed.
        let mut revived = store.published();
        for account in &mut revived.accounts {
            account.enabled = true;
        }
        store.update(revived).unwrap();
        assert!(
            store
                .published()
                .accounts
                .iter()
                .find(|a| a.username == "pool@example.com")
                .unwrap()
                .banned_at
                .is_none()
        );
    }
}
