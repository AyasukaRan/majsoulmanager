pub mod api;
pub mod auth;
pub mod catalog;
pub mod clickhouse;
pub mod config;
pub mod majsoul;
pub mod managed_watch;
pub mod mihomo;
pub mod mjai;
pub mod objects;
pub mod pack;
pub mod recovery;
pub mod watch;
pub mod watch_log;
pub mod watch_service;

use std::{path::PathBuf, sync::Arc};

use auth::AuthStore;
use catalog::Catalog;
use config::Config;
use managed_watch::ManagedWatchDependencies;
use mihomo::MihomoManager;
use pack::PackStore;
use watch::WatchRegistry;
use watch_log::WatchLogBuffer;
use watch_service::WatchSupervisor;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub auth: Arc<AuthStore>,
    pub catalog: Arc<Catalog>,
    pub mihomo: Arc<MihomoManager>,
    pub packs: Arc<PackStore>,
    pub watch: Arc<WatchRegistry>,
    pub watch_service: Arc<WatchSupervisor>,
    pub export_dir: PathBuf,
}

impl AppState {
    pub async fn local(config: Config) -> anyhow::Result<Self> {
        let data_dir = config.data_dir.clone();
        let pack_dir = data_dir.join("packs");
        let export_dir = data_dir.join("exports");
        std::fs::create_dir_all(&export_dir)?;
        let packs = Arc::new(PackStore::new(pack_dir, config.pack_target_bytes)?);
        let catalog = Arc::new(Catalog::connect(&config).await?);
        // Before anything is served: an index missing rows would let the API
        // report a record as absent while its bytes sit in a pack.
        let recovered = recovery::recover(&catalog, &packs).await?;
        if recovered > 0 {
            tracing::info!(recovered, "re-indexed pack entries missing from the index");
        }
        let auth = Arc::new(AuthStore::new(
            &data_dir,
            &config.admin_email,
            &config.admin_password,
            config.public_url.clone(),
            config.email_api_url.clone(),
            config.email_api_token.clone(),
            config.email_from.clone(),
        )?);
        let watch = Arc::new(WatchRegistry::default());
        let mihomo = Arc::new(MihomoManager::new(
            data_dir.join("mihomo"),
            &config.mihomo_controller_url,
            config.mihomo_secret.clone(),
            config.mihomo_proxy_url.clone(),
        )?);
        let watch_logs = Arc::new(WatchLogBuffer::default());
        let dependencies = Arc::new(ManagedWatchDependencies {
            data_dir: data_dir.clone(),
            catalog: Arc::clone(&catalog),
            packs: Arc::clone(&packs),
            registry: Arc::clone(&watch),
            mihomo: Arc::clone(&mihomo),
            logs: Arc::clone(&watch_logs),
        });
        let watch_service = Arc::new(WatchSupervisor::new(
            &data_dir,
            Arc::clone(&watch),
            dependencies,
            watch_logs,
        )?);
        Ok(Self {
            auth,
            packs,
            catalog,
            mihomo,
            watch,
            watch_service,
            config: Arc::new(config),
            export_dir,
        })
    }
}
