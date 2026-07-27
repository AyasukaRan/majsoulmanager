use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    task::JoinHandle,
};

use crate::managed_watch::ManagedWatchDependencies;
use crate::watch::{WatchRegistry, WatchSummary};
use crate::watch_log::{WatchLogBuffer, WatchLogEntry, WatchLogLevel};

pub const MODULE_PROTOCOL_VERSION: u32 = 1;
pub const BUILTIN_MODULE_VERSION: &str = "majsoul2mjai-da985809";
const MAX_MODULE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleKind {
    Login,
    PbFetch,
}

impl ModuleKind {
    fn directory(self) -> &'static str {
        match self {
            Self::Login => "login",
            Self::PbFetch => "pb_fetch",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ModuleRef {
    pub name: String,
    pub version: String,
}

impl ModuleRef {
    pub fn builtin() -> Self {
        Self {
            name: "builtin".into(),
            version: BUILTIN_MODULE_VERSION.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchProxyMode {
    Direct,
    #[default]
    Mihomo,
    Custom,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ModuleManifest {
    pub protocol_version: u32,
    pub kind: ModuleKind,
    pub name: String,
    pub version: String,
    pub executable: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct InstalledModule {
    pub kind: ModuleKind,
    pub name: String,
    pub version: String,
    pub protocol_version: u32,
    pub builtin: bool,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InstallModuleRequest {
    pub manifest: ModuleManifest,
    pub artifact_base64: String,
}

/// One collector: an account watching one room and player count. Everything
/// here is per-instance; anything shared by every collector (server, proxy,
/// modules, pacing) stays on [`WatchServiceConfig`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WatchInstance {
    /// Stable slug. Also names this instance's state file and tags its log
    /// lines, so it is validated as a plain identifier.
    pub id: String,
    pub enabled: bool,
    pub room: String,
    pub players: u8,
    pub modes: Vec<String>,
    pub account_secret_ref: String,
    pub client_version: Option<String>,
}

impl Default for WatchInstance {
    fn default() -> Self {
        Self {
            id: "default".into(),
            enabled: true,
            room: "jade".into(),
            players: 4,
            modes: vec!["east".into(), "south".into()],
            account_secret_ref: "file:/run/secrets/majsoul_accounts".into(),
            client_version: None,
        }
    }
}

impl WatchInstance {
    fn validate(&self) -> Result<(), WatchServiceError> {
        validate_identifier("instance id", &self.id)?;
        if !matches!(self.room.as_str(), "gold" | "jade" | "throne" | "all") {
            return Err(WatchServiceError::InvalidConfig(
                "room must be gold, jade, throne or all".into(),
            ));
        }
        if !matches!(self.players, 3 | 4) {
            return Err(WatchServiceError::InvalidConfig(
                "players must be 3 or 4".into(),
            ));
        }
        if self.modes.is_empty()
            || self
                .modes
                .iter()
                .any(|mode| !matches!(mode.as_str(), "east" | "south"))
        {
            return Err(WatchServiceError::InvalidConfig(
                "modes must contain east and/or south".into(),
            ));
        }
        validate_secret_ref(&self.account_secret_ref)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WatchServiceConfig {
    pub revision: u64,
    /// Master switch. An instance runs only when both this and its own
    /// `enabled` are set.
    pub enabled: bool,
    pub server: String,
    #[serde(default)]
    pub proxy_mode: WatchProxyMode,
    pub custom_proxy_url: Option<String>,
    pub poll_interval_secs: u64,
    pub request_delay_ms: u64,
    pub login_module: ModuleRef,
    pub pb_fetch_module: ModuleRef,
    pub instances: Vec<WatchInstance>,
}

impl Default for WatchServiceConfig {
    fn default() -> Self {
        Self {
            revision: 1,
            enabled: false,
            server: "cn".into(),
            proxy_mode: WatchProxyMode::Mihomo,
            custom_proxy_url: None,
            poll_interval_secs: 10,
            request_delay_ms: 500,
            login_module: ModuleRef::builtin(),
            pb_fetch_module: ModuleRef::builtin(),
            instances: vec![WatchInstance::default()],
        }
    }
}

/// Fold a pre-multi-instance config into the current shape: the five
/// per-collector keys that used to sit at the top level become a single
/// instance. Leaves an already-migrated document untouched, so it is safe to
/// run on every load.
fn migrate_legacy_config(document: &mut serde_json::Value) {
    let Some(object) = document.as_object_mut() else {
        return;
    };
    let already_migrated = object
        .get("instances")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|instances| !instances.is_empty());
    if already_migrated {
        return;
    }
    let legacy_keys = ["room", "players", "modes", "account_secret_ref"];
    if !legacy_keys.iter().any(|key| object.contains_key(*key)) {
        // Nothing to fold — an "instances": [] written by hand. Synthesising a
        // partial instance here would fail to deserialize and take the whole
        // API down, so leave it for validate() to reject with a clear message.
        return;
    }
    let mut instance = serde_json::Map::new();
    instance.insert("id".into(), LEGACY_INSTANCE_ID.into());
    instance.insert("enabled".into(), true.into());
    for key in legacy_keys {
        if let Some(value) = object.remove(key) {
            instance.insert(key.into(), value);
        }
    }
    // client_version is optional everywhere, so a missing key is not a
    // migration failure — only a present one needs moving.
    instance.insert(
        "client_version".into(),
        object
            .remove("client_version")
            .unwrap_or(serde_json::Value::Null),
    );
    object.insert(
        "instances".into(),
        serde_json::Value::Array(vec![serde_json::Value::Object(instance)]),
    );
}

/// Instance id given to a migrated legacy configuration. Also the name its
/// pre-migration state file is moved to.
pub const LEGACY_INSTANCE_ID: &str = "default";

impl WatchServiceConfig {
    /// Instances that should actually be running: the master switch and the
    /// instance's own switch both have to be on.
    pub fn active_instances(&self) -> impl Iterator<Item = &WatchInstance> {
        self.instances
            .iter()
            .filter(|instance| self.enabled && instance.enabled)
    }

    pub fn validate(&self) -> Result<(), WatchServiceError> {
        if self.instances.is_empty() {
            return Err(WatchServiceError::InvalidConfig(
                "at least one watch instance is required".into(),
            ));
        }
        for instance in &self.instances {
            instance.validate()?;
        }
        // Ids name state files and tag log lines, so duplicates would make two
        // collectors silently share state.
        let mut seen = std::collections::BTreeSet::new();
        if let Some(duplicate) = self
            .instances
            .iter()
            .find(|instance| !seen.insert(&instance.id))
        {
            return Err(WatchServiceError::InvalidConfig(format!(
                "duplicate watch instance id {}",
                duplicate.id
            )));
        }
        // Majsoul allows one session per account, so two collectors sharing a
        // secret would kick each other off in a reconnect loop forever.
        let mut accounts = std::collections::BTreeSet::new();
        if let Some(shared) = self
            .instances
            .iter()
            .find(|instance| !accounts.insert(&instance.account_secret_ref))
        {
            return Err(WatchServiceError::InvalidConfig(format!(
                "watch instance {} reuses another instance's account_secret_ref; \
                 each collector needs its own account",
                shared.id
            )));
        }
        if !matches!(self.server.as_str(), "cn" | "en" | "jp") {
            return Err(WatchServiceError::InvalidConfig(
                "server must be cn, en or jp".into(),
            ));
        }
        if !(3..=300).contains(&self.poll_interval_secs) {
            return Err(WatchServiceError::InvalidConfig(
                "poll_interval_secs must be between 3 and 300".into(),
            ));
        }
        if self.request_delay_ms > 60_000 {
            return Err(WatchServiceError::InvalidConfig(
                "request_delay_ms must not exceed 60000".into(),
            ));
        }
        match self.proxy_mode {
            WatchProxyMode::Direct | WatchProxyMode::Mihomo => {}
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
            }
        }
        validate_module_ref(&self.login_module)?;
        validate_module_ref(&self.pb_fetch_module)?;
        Ok(())
    }
}

fn validate_secret_ref(value: &str) -> Result<(), WatchServiceError> {
    let Some((scheme, target)) = value.split_once(':') else {
        return Err(WatchServiceError::InvalidConfig(
            "account_secret_ref must use file: or env:".into(),
        ));
    };
    if target.is_empty() || !matches!(scheme, "file" | "env") {
        return Err(WatchServiceError::InvalidConfig(
            "account_secret_ref must use file: or env:".into(),
        ));
    }
    Ok(())
}

fn validate_module_ref(value: &ModuleRef) -> Result<(), WatchServiceError> {
    validate_identifier("module name", &value.name)?;
    validate_identifier("module version", &value.version)
}

fn validate_identifier(label: &str, value: &str) -> Result<(), WatchServiceError> {
    if value.is_empty()
        || value.len() > 96
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(WatchServiceError::InvalidConfig(format!(
            "{label} contains unsupported characters"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServicePhase {
    Stopped,
    Starting,
    Running,
    Reloading,
    Stopping,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct WatchRuntimeStatus {
    pub phase: ServicePhase,
    pub active_revision: Option<u64>,
    pub login_module: Option<ModuleRef>,
    pub pb_fetch_module: Option<ModuleRef>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
}

impl Default for WatchRuntimeStatus {
    fn default() -> Self {
        Self {
            phase: ServicePhase::Stopped,
            active_revision: None,
            login_module: None,
            pb_fetch_module: None,
            started_at: None,
            updated_at: Utc::now(),
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct WatchDashboard {
    #[serde(flatten)]
    pub records: WatchSummary,
    pub service: WatchRuntimeStatus,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchAction {
    Start,
    Stop,
    Reload,
}

#[derive(Debug, Error)]
pub enum WatchServiceError {
    #[error("invalid watch configuration: {0}")]
    InvalidConfig(String),
    #[error("module not installed: {0}/{1}")]
    ModuleNotInstalled(String, String),
    #[error("module package is invalid: {0}")]
    InvalidModule(String),
    #[error("module failed health check: {0}")]
    ModuleHealth(String),
    #[error("watch service IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("watch service serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct ModuleStore {
    root: PathBuf,
    logs: Arc<WatchLogBuffer>,
}

impl ModuleStore {
    fn new(root: PathBuf, logs: Arc<WatchLogBuffer>) -> Result<Self, WatchServiceError> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, logs })
    }

    pub fn list(
        &self,
        login_active: &ModuleRef,
        pb_active: &ModuleRef,
    ) -> Result<Vec<InstalledModule>, WatchServiceError> {
        let mut result = vec![
            InstalledModule {
                kind: ModuleKind::Login,
                name: "builtin".into(),
                version: BUILTIN_MODULE_VERSION.into(),
                protocol_version: MODULE_PROTOCOL_VERSION,
                builtin: true,
                active: login_active == &ModuleRef::builtin(),
            },
            InstalledModule {
                kind: ModuleKind::PbFetch,
                name: "builtin".into(),
                version: BUILTIN_MODULE_VERSION.into(),
                protocol_version: MODULE_PROTOCOL_VERSION,
                builtin: true,
                active: pb_active == &ModuleRef::builtin(),
            },
        ];
        if !self.root.exists() {
            return Ok(result);
        }
        for kind in [ModuleKind::Login, ModuleKind::PbFetch] {
            let kind_dir = self.root.join(kind.directory());
            let Ok(names) = std::fs::read_dir(kind_dir) else {
                continue;
            };
            for name in names.flatten() {
                let Ok(versions) = std::fs::read_dir(name.path()) else {
                    continue;
                };
                for version in versions.flatten() {
                    let path = version.path().join("manifest.json");
                    let Ok(raw) = std::fs::read(&path) else {
                        continue;
                    };
                    let Ok(manifest) = serde_json::from_slice::<ModuleManifest>(&raw) else {
                        continue;
                    };
                    let selected = ModuleRef {
                        name: manifest.name.clone(),
                        version: manifest.version.clone(),
                    };
                    result.push(InstalledModule {
                        kind,
                        name: manifest.name,
                        version: manifest.version,
                        protocol_version: manifest.protocol_version,
                        builtin: false,
                        active: match kind {
                            ModuleKind::Login => &selected == login_active,
                            ModuleKind::PbFetch => &selected == pb_active,
                        },
                    });
                }
            }
        }
        result.sort_by(|left, right| {
            (left.kind.directory(), &left.name, &left.version).cmp(&(
                right.kind.directory(),
                &right.name,
                &right.version,
            ))
        });
        Ok(result)
    }

    pub async fn install(
        &self,
        request: InstallModuleRequest,
    ) -> Result<InstalledModule, WatchServiceError> {
        validate_manifest(&request.manifest)?;
        let bytes = STANDARD
            .decode(&request.artifact_base64)
            .map_err(|error| WatchServiceError::InvalidModule(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > MAX_MODULE_BYTES {
            return Err(WatchServiceError::InvalidModule(format!(
                "artifact must be between 1 byte and {MAX_MODULE_BYTES} bytes"
            )));
        }
        let digest = hex::encode(Sha256::digest(&bytes));
        if !digest.eq_ignore_ascii_case(&request.manifest.sha256) {
            return Err(WatchServiceError::InvalidModule(
                "artifact sha256 does not match manifest".into(),
            ));
        }

        let directory = self.module_dir(&request.manifest);
        let temporary = directory.with_extension(format!("install-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&temporary).await?;
        let executable = temporary.join(&request.manifest.executable);
        tokio::fs::write(&executable, bytes).await?;
        set_executable(&executable)?;
        tokio::fs::write(
            temporary.join("manifest.json"),
            serde_json::to_vec_pretty(&request.manifest)?,
        )
        .await?;

        let worker = PluginWorker::spawn(
            executable,
            &request.manifest.args,
            &request.manifest.name,
            Arc::clone(&self.logs),
        )
        .await?;
        worker.health().await?;
        worker.shutdown().await;

        if tokio::fs::try_exists(&directory).await? {
            tokio::fs::remove_dir_all(&temporary).await?;
            return Err(WatchServiceError::InvalidModule(
                "that module version is already installed".into(),
            ));
        }
        if let Some(parent) = directory.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::rename(temporary, directory).await?;
        Ok(InstalledModule {
            kind: request.manifest.kind,
            name: request.manifest.name,
            version: request.manifest.version,
            protocol_version: request.manifest.protocol_version,
            builtin: false,
            active: false,
        })
    }

    async fn probe(&self, kind: ModuleKind, module: &ModuleRef) -> Result<(), WatchServiceError> {
        if module == &ModuleRef::builtin() {
            return Ok(());
        }
        let manifest = self.load_manifest(kind, module)?;
        let executable = self.module_dir(&manifest).join(&manifest.executable);
        let worker = PluginWorker::spawn(
            executable,
            &manifest.args,
            &manifest.name,
            Arc::clone(&self.logs),
        )
        .await?;
        worker.health().await?;
        worker.shutdown().await;
        Ok(())
    }

    async fn worker(
        &self,
        kind: ModuleKind,
        module: &ModuleRef,
    ) -> Result<Option<Arc<PluginWorker>>, WatchServiceError> {
        if module == &ModuleRef::builtin() {
            return Ok(None);
        }
        let manifest = self.load_manifest(kind, module)?;
        let executable = self.module_dir(&manifest).join(&manifest.executable);
        let worker = Arc::new(
            PluginWorker::spawn(
                executable,
                &manifest.args,
                &manifest.name,
                Arc::clone(&self.logs),
            )
            .await?,
        );
        worker.health().await?;
        Ok(Some(worker))
    }

    fn load_manifest(
        &self,
        kind: ModuleKind,
        module: &ModuleRef,
    ) -> Result<ModuleManifest, WatchServiceError> {
        let path = self
            .root
            .join(kind.directory())
            .join(&module.name)
            .join(&module.version)
            .join("manifest.json");
        let bytes = std::fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                WatchServiceError::ModuleNotInstalled(module.name.clone(), module.version.clone())
            } else {
                WatchServiceError::Io(error)
            }
        })?;
        let manifest: ModuleManifest = serde_json::from_slice(&bytes)?;
        validate_manifest(&manifest)?;
        if manifest.kind != kind
            || manifest.name != module.name
            || manifest.version != module.version
        {
            return Err(WatchServiceError::InvalidModule(
                "manifest identity does not match its installation path".into(),
            ));
        }
        Ok(manifest)
    }

    fn module_dir(&self, manifest: &ModuleManifest) -> PathBuf {
        self.root
            .join(manifest.kind.directory())
            .join(&manifest.name)
            .join(&manifest.version)
    }
}

fn validate_manifest(manifest: &ModuleManifest) -> Result<(), WatchServiceError> {
    if manifest.protocol_version != MODULE_PROTOCOL_VERSION {
        return Err(WatchServiceError::InvalidModule(format!(
            "protocol_version must be {MODULE_PROTOCOL_VERSION}"
        )));
    }
    validate_identifier("module name", &manifest.name)?;
    validate_identifier("module version", &manifest.version)?;
    let executable = Path::new(&manifest.executable);
    if executable.as_os_str().is_empty()
        || executable.is_absolute()
        || executable
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(WatchServiceError::InvalidModule(
            "executable must be a relative file path without parent components".into(),
        ));
    }
    if manifest.sha256.len() != 64 || !manifest.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(WatchServiceError::InvalidModule(
            "sha256 must be a 64-character hexadecimal digest".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), WatchServiceError> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), WatchServiceError> {
    Ok(())
}

pub(crate) struct PluginWorker {
    child: Mutex<Child>,
    input: Mutex<ChildStdin>,
    output: Mutex<BufReader<ChildStdout>>,
    sequence: AtomicU64,
}

impl PluginWorker {
    async fn spawn(
        path: PathBuf,
        args: &[String],
        module_name: &str,
        logs: Arc<WatchLogBuffer>,
    ) -> Result<Self, WatchServiceError> {
        let mut child = Command::new(path)
            .args(args)
            .arg("--mjai-module-stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(WatchServiceError::Io)?;
        let input = child.stdin.take().ok_or_else(|| {
            WatchServiceError::ModuleHealth("module stdin was not available".into())
        })?;
        let output = child.stdout.take().ok_or_else(|| {
            WatchServiceError::ModuleHealth("module stdout was not available".into())
        })?;
        if let Some(stderr) = child.stderr.take() {
            let source = format!("module:{module_name}");
            tokio::spawn(async move {
                // 按字节读并做有界缓冲:非 UTF-8 行不能让循环退出(否则管道
                // 关闭,模块下次写 stderr 会被 SIGPIPE 杀死),超长行也不能
                // 无上限地占用内存。EOF 或真实 IO 错误才结束。
                const MAX_LINE_BYTES: u64 = 8 * 1024;
                let mut reader = BufReader::new(stderr);
                let mut buf = Vec::with_capacity(256);
                let mut discarding_overlong_tail = false;
                loop {
                    buf.clear();
                    let read = (&mut reader)
                        .take(MAX_LINE_BYTES)
                        .read_until(b'\n', &mut buf)
                        .await;
                    match read {
                        Ok(0) => break,
                        Ok(_) => {
                            let complete_line = buf.last() == Some(&b'\n');
                            if discarding_overlong_tail {
                                discarding_overlong_tail = !complete_line;
                                continue;
                            }
                            discarding_overlong_tail = !complete_line;
                            let line = String::from_utf8_lossy(&buf);
                            let line = line.trim_end_matches(['\n', '\r']);
                            if !line.is_empty() {
                                logs.append(WatchLogLevel::Info, &source, line);
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
        Ok(Self {
            child: Mutex::new(child),
            input: Mutex::new(input),
            output: Mutex::new(BufReader::new(output)),
            sequence: AtomicU64::new(1),
        })
    }

    async fn health(&self) -> Result<(), WatchServiceError> {
        self.request("health", serde_json::json!({})).await?;
        Ok(())
    }

    pub(crate) async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, WatchServiceError> {
        let id = self.sequence.fetch_add(1, Ordering::Relaxed);
        let request = serde_json::json!({
            "id": id,
            "protocol_version": MODULE_PROTOCOL_VERSION,
            "method": method,
            "params": params
        });
        let mut input = self.input.lock().await;
        input.write_all(request.to_string().as_bytes()).await?;
        input.write_all(b"\n").await?;
        input.flush().await?;
        drop(input);

        let mut line = String::new();
        let read = async { self.output.lock().await.read_line(&mut line).await };
        let bytes = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .map_err(|_| WatchServiceError::ModuleHealth("health check timed out".into()))??;
        if bytes == 0 {
            return Err(WatchServiceError::ModuleHealth(
                "module exited during health check".into(),
            ));
        }
        let response: serde_json::Value = serde_json::from_str(&line)?;
        if response.get("id").and_then(|value| value.as_u64()) != Some(id) {
            return Err(WatchServiceError::ModuleHealth(
                "module returned a response with the wrong request id".into(),
            ));
        }
        if response.get("ok").and_then(|value| value.as_bool()) != Some(true) {
            return Err(WatchServiceError::ModuleHealth(
                response
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("module request failed")
                    .to_owned(),
            ));
        }
        Ok(response
            .get("result")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})))
    }

    async fn shutdown(&self) {
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

pub struct WatchSupervisor {
    config_path: PathBuf,
    config: RwLock<WatchServiceConfig>,
    runtime: RwLock<WatchRuntimeStatus>,
    modules: ModuleStore,
    registry: Arc<WatchRegistry>,
    dependencies: Arc<ManagedWatchDependencies>,
    logs: Arc<WatchLogBuffer>,
    generation: AtomicU64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl WatchSupervisor {
    pub fn new(
        data_dir: &Path,
        registry: Arc<WatchRegistry>,
        dependencies: Arc<ManagedWatchDependencies>,
        logs: Arc<WatchLogBuffer>,
    ) -> Result<Self, WatchServiceError> {
        let watch_dir = data_dir.join("watch");
        std::fs::create_dir_all(&watch_dir)?;
        let config_path = watch_dir.join("config.json");
        let config = match std::fs::read(&config_path) {
            Ok(bytes) => {
                let mut document: serde_json::Value = serde_json::from_slice(&bytes)?;
                let legacy = document.get("instances").is_none();
                migrate_legacy_config(&mut document);
                let config: WatchServiceConfig = serde_json::from_value(document)?;
                if legacy {
                    // The single collector kept its state in an unsuffixed
                    // file; move it so the migrated instance picks up its
                    // in-flight games instead of restarting from nothing.
                    // Never overwrite: a state file already under the new name
                    // is newer than anything the legacy path left behind.
                    let migrated_state = watch_dir.join(format!("state-{LEGACY_INSTANCE_ID}.json"));
                    if !migrated_state.exists() {
                        let _ = std::fs::rename(watch_dir.join("state.json"), migrated_state);
                    }
                    persist_json(&config_path, &config)?;
                }
                config
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let config = WatchServiceConfig::default();
                persist_json(&config_path, &config)?;
                config
            }
            Err(error) => return Err(error.into()),
        };
        config.validate()?;
        Ok(Self {
            config_path,
            config: RwLock::new(config),
            runtime: RwLock::new(WatchRuntimeStatus::default()),
            modules: ModuleStore::new(watch_dir.join("modules"), Arc::clone(&logs))?,
            registry,
            dependencies,
            logs,
            generation: AtomicU64::new(0),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub fn logs_after(&self, after: u64, limit: usize) -> Vec<WatchLogEntry> {
        self.logs.entries_after(after, limit)
    }

    pub fn log_buffer(&self) -> Arc<WatchLogBuffer> {
        Arc::clone(&self.logs)
    }

    pub fn config(&self) -> WatchServiceConfig {
        self.config.read().clone()
    }

    pub fn dashboard(&self, state: Option<&str>, limit: usize) -> WatchDashboard {
        WatchDashboard {
            records: self.registry.summary(state, limit),
            service: self.runtime.read().clone(),
        }
    }

    pub fn modules(&self) -> Result<Vec<InstalledModule>, WatchServiceError> {
        let config = self.config.read();
        self.modules
            .list(&config.login_module, &config.pb_fetch_module)
    }

    pub async fn install_module(
        &self,
        request: InstallModuleRequest,
    ) -> Result<InstalledModule, WatchServiceError> {
        self.modules.install(request).await
    }

    pub async fn update_config(
        self: &Arc<Self>,
        mut next: WatchServiceConfig,
    ) -> Result<WatchServiceConfig, WatchServiceError> {
        next.validate()?;
        self.probe_modules(&next).await?;
        let current = self.config();
        next.revision = current.revision.saturating_add(1);
        persist_json(&self.config_path, &next)?;
        *self.config.write() = next.clone();
        if next.enabled {
            self.reload().await?;
        } else {
            self.stop().await;
        }
        Ok(next)
    }

    pub async fn apply_action(
        self: &Arc<Self>,
        action: WatchAction,
    ) -> Result<WatchRuntimeStatus, WatchServiceError> {
        match action {
            WatchAction::Start => self.start().await?,
            WatchAction::Stop => self.stop().await,
            WatchAction::Reload => self.reload().await?,
        }
        Ok(self.runtime.read().clone())
    }

    pub async fn start_if_enabled(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        if self.config.read().enabled {
            self.start().await?;
        }
        Ok(())
    }

    async fn probe_modules(&self, config: &WatchServiceConfig) -> Result<(), WatchServiceError> {
        self.modules
            .probe(ModuleKind::Login, &config.login_module)
            .await?;
        self.modules
            .probe(ModuleKind::PbFetch, &config.pb_fetch_module)
            .await
    }

    async fn start(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        let config = self.config();
        self.logs
            .append(WatchLogLevel::Info, "service", "watch 服务启动中");
        if let Err(error) = self.probe_modules(&config).await {
            self.logs.append(
                WatchLogLevel::Error,
                "service",
                format!("watch 服务启动失败：模块探测未通过 ({error})"),
            );
            return Err(error);
        }
        self.stop_tasks().await;
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Starting;
            runtime.updated_at = Utc::now();
            runtime.last_error = None;
        }
        let mut tasks = Vec::new();
        for instance in config.active_instances().cloned().collect::<Vec<_>>() {
            let supervisor = Arc::clone(self);
            let config = config.clone();
            tasks.push(tokio::spawn(async move {
                supervisor.run_instance(generation, config, instance).await;
            }));
        }
        if tasks.is_empty() {
            // Nothing will ever move the phase off Starting, so settle it here.
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Stopped;
            runtime.updated_at = Utc::now();
            drop(runtime);
            self.logs.append(
                WatchLogLevel::Warn,
                "service",
                "没有启用的采集实例，watch 服务不会连接任何账号",
            );
        }
        *self.tasks.lock().await = tasks;
        Ok(())
    }

    async fn reload(self: &Arc<Self>) -> Result<(), WatchServiceError> {
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Reloading;
            runtime.updated_at = Utc::now();
        }
        self.logs
            .append(WatchLogLevel::Info, "service", "watch 服务重新加载中");
        self.start().await
    }

    async fn stop(&self) {
        {
            let mut runtime = self.runtime.write();
            runtime.phase = ServicePhase::Stopping;
            runtime.updated_at = Utc::now();
        }
        self.logs
            .append(WatchLogLevel::Info, "service", "watch 服务停止中");
        self.stop_tasks().await;
        let mut runtime = self.runtime.write();
        runtime.phase = ServicePhase::Stopped;
        runtime.updated_at = Utc::now();
        runtime.active_revision = None;
        runtime.login_module = None;
        runtime.pb_fetch_module = None;
        drop(runtime);
        self.logs
            .append(WatchLogLevel::Info, "service", "watch 服务已停止");
    }

    async fn stop_tasks(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().await);
        // Abort everything before awaiting anything: dropping a JoinHandle
        // detaches its task, so if this future is cancelled part-way through
        // the loop, any handle not yet aborted would leave a collector logging
        // in forever with no way to stop it.
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    /// Run one collector for the lifetime of `generation`. Instances are
    /// independent: one failing leaves the others collecting, and the shared
    /// runtime status reports the first failure with the instance that caused
    /// it, since per-instance detail is already tagged in the log stream.
    async fn run_instance(
        self: Arc<Self>,
        generation: u64,
        config: WatchServiceConfig,
        instance: WatchInstance,
    ) {
        {
            let mut runtime = self.runtime.write();
            // A sibling that already failed keeps the service marked Failed;
            // start() resets the phase for each new generation.
            if runtime.phase != ServicePhase::Failed {
                runtime.phase = ServicePhase::Running;
            }
            runtime.active_revision = Some(config.revision);
            runtime.login_module = Some(config.login_module.clone());
            runtime.pb_fetch_module = Some(config.pb_fetch_module.clone());
            runtime.started_at = Some(Utc::now());
            runtime.updated_at = Utc::now();
            // last_error is deliberately not cleared here: start() already
            // cleared it for this generation, and clearing it per instance
            // lets a slow-starting collector erase a sibling's failure.
        }
        let id = instance.id.clone();
        self.logs.append(
            WatchLogLevel::Info,
            "service",
            format!("采集实例 {id} 已启动 (generation {generation})"),
        );

        let login_worker = self
            .modules
            .worker(ModuleKind::Login, &config.login_module)
            .await;
        let pb_worker = self
            .modules
            .worker(ModuleKind::PbFetch, &config.pb_fetch_module)
            .await;
        let result = match (login_worker, pb_worker) {
            (Ok(login), Ok(pb)) => {
                crate::managed_watch::run(
                    config,
                    instance,
                    Arc::clone(&self.dependencies),
                    login,
                    pb,
                )
                .await
            }
            (Err(error), _) | (_, Err(error)) => Err(anyhow::Error::msg(error.to_string())),
        };
        if self.generation.load(Ordering::SeqCst) == generation {
            let mut runtime = self.runtime.write();
            runtime.updated_at = Utc::now();
            match result {
                Ok(()) => {
                    drop(runtime);
                    self.logs.append(
                        WatchLogLevel::Info,
                        "service",
                        format!("采集实例 {id} 已退出 (generation {generation})"),
                    );
                }
                Err(error) => {
                    runtime.phase = ServicePhase::Failed;
                    runtime.last_error = Some(format!("[{id}] {error:#}"));
                    drop(runtime);
                    self.logs.append(
                        WatchLogLevel::Error,
                        "service",
                        format!("采集实例 {id} 失败: {error:#}"),
                    );
                }
            }
        }
    }
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<(), WatchServiceError> {
    let parent = path.parent().ok_or_else(|| {
        WatchServiceError::InvalidConfig("configuration path has no parent".into())
    })?;
    std::fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let mut body = serde_json::to_vec_pretty(value)?;
    body.push(b'\n');
    std::fs::write(&temporary, body)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

pub fn module_protocol_contract() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("transport", "newline-delimited JSON over stdin/stdout"),
        ("binary_fields", "base64"),
        (
            "health_request",
            r#"{"id":1,"protocol_version":1,"method":"health","params":{}}"#,
        ),
        (
            "health_response",
            r#"{"id":1,"ok":true,"result":{"version":"..."}}"#,
        ),
        ("login_methods", "open_session, rpc, close_session"),
        (
            "pb_fetch_methods",
            "build_live_list_request, parse_live_list_response, build_record_request, parse_record_response",
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_plaintext_secret_in_online_config() {
        let config = WatchServiceConfig {
            instances: vec![WatchInstance {
                account_secret_ref: "my-password".into(),
                ..WatchInstance::default()
            }],
            ..WatchServiceConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn default_config_is_safe_and_disabled() {
        let config = WatchServiceConfig::default();
        config.validate().unwrap();
        assert!(!config.enabled);
        assert_eq!(config.login_module, ModuleRef::builtin());
    }

    #[test]
    fn migrates_a_pre_multi_instance_config() {
        let mut document = serde_json::json!({
            "revision": 18,
            "enabled": true,
            "room": "throne",
            "players": 3,
            "modes": ["south"],
            "server": "cn",
            "account_secret_ref": "file:/var/lib/mjai/majsoul_accounts",
            "proxy_mode": "direct",
            "custom_proxy_url": null,
            "client_version": "0.16.254",
            "poll_interval_secs": 10,
            "request_delay_ms": 500,
            "login_module": {"name": "builtin", "version": BUILTIN_MODULE_VERSION},
            "pb_fetch_module": {"name": "builtin", "version": BUILTIN_MODULE_VERSION},
        });
        migrate_legacy_config(&mut document);
        let config: WatchServiceConfig = serde_json::from_value(document).unwrap();
        config.validate().unwrap();

        let instance = &config.instances[0];
        assert_eq!(config.instances.len(), 1);
        assert_eq!(instance.id, LEGACY_INSTANCE_ID);
        assert_eq!(instance.room, "throne");
        assert_eq!(instance.players, 3);
        assert_eq!(instance.client_version.as_deref(), Some("0.16.254"));
        assert!(instance.enabled, "a migrated collector must keep running");
        assert_eq!(config.server, "cn");
    }

    #[test]
    fn migration_leaves_an_already_migrated_config_alone() {
        let original = serde_json::to_value(WatchServiceConfig::default()).unwrap();
        let mut document = original.clone();
        migrate_legacy_config(&mut document);
        assert_eq!(document, original);
    }

    #[test]
    fn rejects_duplicate_instance_ids() {
        let config = WatchServiceConfig {
            instances: vec![WatchInstance::default(), WatchInstance::default()],
            ..WatchServiceConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_two_instances_sharing_one_account() {
        let config = WatchServiceConfig {
            instances: vec![
                WatchInstance {
                    id: "four".into(),
                    ..WatchInstance::default()
                },
                WatchInstance {
                    id: "three".into(),
                    players: 3,
                    ..WatchInstance::default()
                },
            ],
            ..WatchServiceConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn migration_leaves_a_hand_written_empty_instance_list_to_validation() {
        // Synthesising an instance from nothing would fail to deserialize and
        // take the API down; a clear validation error is the better failure.
        let mut document = serde_json::json!({"instances": []});
        migrate_legacy_config(&mut document);
        assert_eq!(document["instances"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn rejects_an_instance_id_that_would_escape_the_state_directory() {
        let config = WatchServiceConfig {
            instances: vec![WatchInstance {
                id: "../../etc/passwd".into(),
                ..WatchInstance::default()
            }],
            ..WatchServiceConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn an_instance_runs_only_when_both_switches_are_on() {
        let mut config = WatchServiceConfig {
            enabled: true,
            instances: vec![
                WatchInstance {
                    id: "four".into(),
                    ..WatchInstance::default()
                },
                WatchInstance {
                    id: "three".into(),
                    enabled: false,
                    players: 3,
                    ..WatchInstance::default()
                },
            ],
            ..WatchServiceConfig::default()
        };
        assert_eq!(config.active_instances().count(), 1);
        config.enabled = false;
        assert_eq!(config.active_instances().count(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn installs_only_a_health_checked_module_artifact() {
        let root = std::env::temp_dir().join(format!("mjai-module-test-{}", uuid::Uuid::new_v4()));
        let store = ModuleStore::new(root.clone(), Arc::new(WatchLogBuffer::default())).unwrap();
        let artifact =
            b"#!/bin/sh\nIFS= read -r request\nprintf '%s\\n' '{\"id\":1,\"ok\":true,\"result\":{\"version\":\"test\"}}'\n";
        let manifest = ModuleManifest {
            protocol_version: MODULE_PROTOCOL_VERSION,
            kind: ModuleKind::Login,
            name: "test-login".into(),
            version: "1.0.0".into(),
            executable: "module".into(),
            args: Vec::new(),
            sha256: hex::encode(Sha256::digest(artifact)),
        };
        let installed = store
            .install(InstallModuleRequest {
                manifest,
                artifact_base64: STANDARD.encode(artifact),
            })
            .await
            .unwrap();
        assert_eq!(installed.name, "test-login");
        assert!(root.join("login/test-login/1.0.0/manifest.json").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
