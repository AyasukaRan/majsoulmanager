use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    Admin,
    Member,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    PendingVerification,
    Active,
    Disabled,
}

#[derive(Clone, Debug, Serialize)]
pub struct UserView {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    pub role: UserRole,
    pub status: UserStatus,
    pub email_verified_at: Option<DateTime<Utc>>,
    pub last_login_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredUser {
    id: Uuid,
    email: String,
    name: String,
    role: UserRole,
    status: UserStatus,
    password_hash: String,
    email_verified_at: Option<DateTime<Utc>>,
    last_login_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl From<&StoredUser> for UserView {
    fn from(user: &StoredUser) -> Self {
        Self {
            id: user.id,
            email: user.email.clone(),
            name: user.name.clone(),
            role: user.role,
            status: user.status,
            email_verified_at: user.email_verified_at,
            last_login_at: user.last_login_at,
            created_at: user.created_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthSettings {
    pub registration_enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Verification {
    user_id: Uuid,
    token_hash: String,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredAuth {
    settings: AuthSettings,
    users: Vec<StoredUser>,
    #[serde(default)]
    verifications: Vec<Verification>,
}

struct Session {
    user_id: Uuid,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct VerifyEmailRequest {
    pub token: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateUserRequest {
    pub name: String,
    pub email: String,
    pub password: String,
    pub role: UserRole,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpdateUserRequest {
    pub name: Option<String>,
    pub role: Option<UserRole>,
    pub status: Option<UserStatus>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LoginResponse {
    pub session_token: String,
    pub expires_at: DateTime<Utc>,
    pub user: UserView,
}

#[derive(Clone, Debug, Serialize)]
pub struct RegistrationStatus {
    pub registration_enabled: bool,
    pub email_delivery_configured: bool,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("invalid email or password")]
    InvalidCredentials,
    #[error("email verification is required")]
    EmailNotVerified,
    #[error("this account is disabled")]
    Disabled,
    #[error("registration is disabled")]
    RegistrationDisabled,
    #[error("email delivery is not configured")]
    EmailUnavailable,
    #[error("that email address is already registered")]
    EmailExists,
    #[error("verification link is invalid or expired")]
    InvalidVerification,
    #[error("user not found")]
    NotFound,
    #[error("administrator access is required")]
    Forbidden,
    #[error("invalid account data: {0}")]
    InvalidInput(String),
    #[error("authentication storage error: {0}")]
    Io(#[from] std::io::Error),
    #[error("authentication serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("email delivery failed: {0}")]
    Email(String),
}

pub struct AuthStore {
    path: PathBuf,
    state: RwLock<StoredAuth>,
    sessions: RwLock<HashMap<String, Session>>,
    email_api_url: Option<String>,
    email_api_token: Option<String>,
    email_from: String,
    email_client: Client,
    public_url: String,
}

impl AuthStore {
    pub fn new(
        data_dir: &Path,
        admin_email: &str,
        admin_password: &str,
        public_url: String,
        email_api_url: Option<String>,
        email_api_token: Option<String>,
        email_from: String,
    ) -> Result<Self, AuthError> {
        let path = data_dir.join("auth.json");
        let state = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let admin = StoredUser {
                    id: Uuid::new_v4(),
                    email: normalize_email(admin_email)?,
                    name: "Administrator".into(),
                    role: UserRole::Admin,
                    status: UserStatus::Active,
                    password_hash: hash_password(admin_password)?,
                    email_verified_at: Some(Utc::now()),
                    last_login_at: None,
                    created_at: Utc::now(),
                };
                let state = StoredAuth {
                    settings: AuthSettings {
                        registration_enabled: false,
                    },
                    users: vec![admin],
                    verifications: Vec::new(),
                };
                persist(&path, &state)?;
                state
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            state: RwLock::new(state),
            sessions: RwLock::new(HashMap::new()),
            email_api_url: non_empty(email_api_url),
            email_api_token: non_empty(email_api_token),
            email_from,
            email_client: Client::new(),
            public_url,
        })
    }

    pub fn registration_status(&self) -> RegistrationStatus {
        RegistrationStatus {
            registration_enabled: self.state.read().settings.registration_enabled,
            email_delivery_configured: self.email_api_url.is_some(),
        }
    }

    pub fn login(&self, request: LoginRequest) -> Result<LoginResponse, AuthError> {
        let email = normalize_email(&request.email)?;
        let mut state = self.state.write();
        let user = state
            .users
            .iter_mut()
            .find(|user| user.email == email)
            .ok_or(AuthError::InvalidCredentials)?;
        verify_password(&user.password_hash, &request.password)?;
        match user.status {
            UserStatus::PendingVerification => return Err(AuthError::EmailNotVerified),
            UserStatus::Disabled => return Err(AuthError::Disabled),
            UserStatus::Active => {}
        }
        user.last_login_at = Some(Utc::now());
        let view = UserView::from(&*user);
        persist(&self.path, &state)?;
        drop(state);
        let token = new_token();
        let expires_at = Utc::now() + Duration::days(7);
        self.sessions.write().insert(
            hash_token(&token),
            Session {
                user_id: view.id,
                expires_at,
            },
        );
        Ok(LoginResponse {
            session_token: token,
            expires_at,
            user: view,
        })
    }

    pub fn user_for_session(&self, token: &str) -> Result<UserView, AuthError> {
        let hash = hash_token(token);
        let sessions = self.sessions.read();
        let session = sessions.get(&hash).ok_or(AuthError::InvalidCredentials)?;
        if session.expires_at <= Utc::now() {
            return Err(AuthError::InvalidCredentials);
        }
        let state = self.state.read();
        let user = state
            .users
            .iter()
            .find(|user| user.id == session.user_id)
            .ok_or(AuthError::InvalidCredentials)?;
        if user.status != UserStatus::Active {
            return Err(AuthError::Disabled);
        }
        Ok(UserView::from(user))
    }

    pub fn logout(&self, token: &str) {
        self.sessions.write().remove(&hash_token(token));
    }

    pub async fn register(&self, request: RegisterRequest) -> Result<(), AuthError> {
        if !self.state.read().settings.registration_enabled {
            return Err(AuthError::RegistrationDisabled);
        }
        let email_api_url = self
            .email_api_url
            .as_ref()
            .ok_or(AuthError::EmailUnavailable)?;
        validate_password(&request.password)?;
        let email = normalize_email(&request.email)?;
        let name = validate_name(&request.name)?;
        let token = new_token();
        let user_id = Uuid::new_v4();
        {
            let mut state = self.state.write();
            if state.users.iter().any(|user| user.email == email) {
                return Err(AuthError::EmailExists);
            }
            state.users.push(StoredUser {
                id: user_id,
                email: email.clone(),
                name,
                role: UserRole::Member,
                status: UserStatus::PendingVerification,
                password_hash: hash_password(&request.password)?,
                email_verified_at: None,
                last_login_at: None,
                created_at: Utc::now(),
            });
            state.verifications.push(Verification {
                user_id,
                token_hash: hash_token(&token),
                expires_at: Utc::now() + Duration::hours(24),
            });
            persist(&self.path, &state)?;
        }
        let verify_url = format!(
            "{}/verify-email?token={}",
            self.public_url.trim_end_matches('/'),
            token
        );
        let mut delivery = self
            .email_client
            .post(email_api_url)
            .json(&serde_json::json!({
                "from": self.email_from,
                "to": [email],
                "subject": "验证你的 mjai 管理台邮箱",
                "text": format!("请在 24 小时内打开以下链接完成邮箱验证：\n\n{verify_url}\n"),
            }));
        if let Some(token) = &self.email_api_token {
            delivery = delivery.bearer_auth(token);
        }
        let delivery_result = delivery
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| AuthError::Email(error.to_string()));
        if let Err(error) = delivery_result {
            let mut state = self.state.write();
            state.users.retain(|user| user.id != user_id);
            state
                .verifications
                .retain(|verification| verification.user_id != user_id);
            persist(&self.path, &state)?;
            return Err(error);
        }
        Ok(())
    }

    pub fn verify_email(&self, request: VerifyEmailRequest) -> Result<(), AuthError> {
        let hash = hash_token(&request.token);
        let mut state = self.state.write();
        let verification = state
            .verifications
            .iter()
            .find(|item| item.token_hash == hash && item.expires_at > Utc::now())
            .cloned()
            .ok_or(AuthError::InvalidVerification)?;
        let user = state
            .users
            .iter_mut()
            .find(|user| user.id == verification.user_id)
            .ok_or(AuthError::InvalidVerification)?;
        user.status = UserStatus::Active;
        user.email_verified_at = Some(Utc::now());
        state
            .verifications
            .retain(|item| item.user_id != verification.user_id);
        persist(&self.path, &state)?;
        Ok(())
    }

    pub fn require_admin(&self, token: &str) -> Result<UserView, AuthError> {
        let user = self.user_for_session(token)?;
        if user.role != UserRole::Admin {
            return Err(AuthError::Forbidden);
        }
        Ok(user)
    }

    pub fn users(&self, token: &str) -> Result<Vec<UserView>, AuthError> {
        self.require_admin(token)?;
        Ok(self.state.read().users.iter().map(UserView::from).collect())
    }

    pub fn create_user(
        &self,
        token: &str,
        request: CreateUserRequest,
    ) -> Result<UserView, AuthError> {
        self.require_admin(token)?;
        validate_password(&request.password)?;
        let email = normalize_email(&request.email)?;
        let mut state = self.state.write();
        if state.users.iter().any(|user| user.email == email) {
            return Err(AuthError::EmailExists);
        }
        let user = StoredUser {
            id: Uuid::new_v4(),
            email,
            name: validate_name(&request.name)?,
            role: request.role,
            status: UserStatus::Active,
            password_hash: hash_password(&request.password)?,
            email_verified_at: Some(Utc::now()),
            last_login_at: None,
            created_at: Utc::now(),
        };
        let view = UserView::from(&user);
        state.users.push(user);
        persist(&self.path, &state)?;
        Ok(view)
    }

    pub fn update_user(
        &self,
        token: &str,
        id: Uuid,
        request: UpdateUserRequest,
    ) -> Result<UserView, AuthError> {
        let admin = self.require_admin(token)?;
        let mut state = self.state.write();
        let user = state
            .users
            .iter_mut()
            .find(|user| user.id == id)
            .ok_or(AuthError::NotFound)?;
        if user.id == admin.id && request.status == Some(UserStatus::Disabled) {
            return Err(AuthError::InvalidInput(
                "you cannot disable your own account".into(),
            ));
        }
        if user.id == admin.id && request.role == Some(UserRole::Member) {
            return Err(AuthError::InvalidInput(
                "you cannot remove your own administrator role".into(),
            ));
        }
        if let Some(name) = request.name {
            user.name = validate_name(&name)?;
        }
        if let Some(role) = request.role {
            user.role = role;
        }
        if let Some(status) = request.status {
            user.status = status;
        }
        let view = UserView::from(&*user);
        persist(&self.path, &state)?;
        Ok(view)
    }

    pub fn settings(&self, token: &str) -> Result<RegistrationStatus, AuthError> {
        self.require_admin(token)?;
        Ok(self.registration_status())
    }

    pub fn update_settings(
        &self,
        token: &str,
        settings: AuthSettings,
    ) -> Result<RegistrationStatus, AuthError> {
        self.require_admin(token)?;
        if settings.registration_enabled && self.email_api_url.is_none() {
            return Err(AuthError::EmailUnavailable);
        }
        let mut state = self.state.write();
        state.settings = settings;
        persist(&self.path, &state)?;
        drop(state);
        Ok(self.registration_status())
    }
}

fn normalize_email(value: &str) -> Result<String, AuthError> {
    let value = value.trim().to_ascii_lowercase();
    if value.len() > 254 || !value.contains('@') || value.starts_with('@') {
        return Err(AuthError::InvalidInput("email address is invalid".into()));
    }
    Ok(value)
}

fn validate_name(value: &str) -> Result<String, AuthError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 80 {
        return Err(AuthError::InvalidInput(
            "name must be between 1 and 80 characters".into(),
        ));
    }
    Ok(value.to_owned())
}

fn validate_password(value: &str) -> Result<(), AuthError> {
    if value.len() < 10 || value.len() > 128 {
        return Err(AuthError::InvalidInput(
            "password must be between 10 and 128 characters".into(),
        ));
    }
    Ok(())
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    })
}

fn hash_password(value: &str) -> Result<String, AuthError> {
    validate_password(value)?;
    let salt = Uuid::new_v4().as_bytes().to_vec();
    let iterations = 120_000;
    let hash = pbkdf2_sha256(value.as_bytes(), &salt, iterations);
    Ok(format!(
        "pbkdf2-sha256${iterations}${}${}",
        hex::encode(salt),
        hex::encode(hash)
    ))
}

fn verify_password(hash: &str, value: &str) -> Result<(), AuthError> {
    if value.len() > 128 {
        return Err(AuthError::InvalidCredentials);
    }
    let parts: Vec<_> = hash.split('$').collect();
    if parts.len() != 4 || parts[0] != "pbkdf2-sha256" {
        return Err(AuthError::InvalidCredentials);
    }
    let iterations = parts[1]
        .parse::<u32>()
        .map_err(|_| AuthError::InvalidCredentials)?;
    if !(10_000..=1_000_000).contains(&iterations) {
        return Err(AuthError::InvalidCredentials);
    }
    let salt = hex::decode(parts[2]).map_err(|_| AuthError::InvalidCredentials)?;
    let expected = hex::decode(parts[3]).map_err(|_| AuthError::InvalidCredentials)?;
    let actual = pbkdf2_sha256(value.as_bytes(), &salt, iterations);
    if expected.len() == actual.len() && bool::from(expected.as_slice().ct_eq(actual.as_slice())) {
        Ok(())
    } else {
        Err(AuthError::InvalidCredentials)
    }
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;

    let mut first = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
    first.update(salt);
    first.update(&1_u32.to_be_bytes());
    let mut previous: [u8; 32] = first.finalize().into_bytes().into();
    let mut result = previous;

    for _ in 1..iterations {
        let mut hmac = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
        hmac.update(&previous);
        previous = hmac.finalize().into_bytes().into();
        for (target, value) in result.iter_mut().zip(previous) {
            *target ^= value;
        }
    }
    result
}

fn new_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn hash_token(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn persist(path: &Path, state: &StoredAuth) -> Result<(), AuthError> {
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let mut bytes = serde_json::to_vec_pretty(state)?;
    bytes.push(b'\n');
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
