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
    collections::HashSet,
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

pub struct AccountPool {
    path: PathBuf,
    document: RwLock<AccountDocument>,
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
        })
    }

    pub fn published(&self) -> AccountDocument {
        self.document.read().clone().published()
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
        let stored: Vec<(String, String)> = document
            .accounts
            .iter()
            .map(|account| (account.id.clone(), account.password.clone()))
            .collect();
        for account in &mut next.accounts {
            account.username = account.username.trim().to_owned();
            let previous = stored
                .iter()
                .find(|(id, _)| !id.is_empty() && *id == account.id)
                .map(|(_, password)| password.as_str());
            account.password =
                restored_secret(Some(account.password.clone()), previous).unwrap_or_default();
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
        }
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
}
