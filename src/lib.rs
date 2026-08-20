pub mod accounts;
pub mod api;
pub mod auth;
pub mod backfill;
pub mod catalog;
pub mod clickhouse;
pub mod config;
pub mod gc;
pub mod indexer;
pub mod kafka;
pub mod majsoul;
pub mod managed_watch;
pub mod mihomo;
pub mod mjai;
pub mod objects;
pub mod pack;
pub mod paipuya;
pub mod recovery;
pub mod refetch;
pub mod refetch_service;
pub mod register_service;
pub mod replay;
pub mod watch;
pub mod watch_log;
pub mod watch_service;

use std::{path::PathBuf, sync::Arc};

use auth::AuthStore;
use catalog::Catalog;
use config::Config;
use kafka::Kafka;
use managed_watch::ManagedWatchDependencies;
use mihomo::MihomoManager;
use objects::Objects;
use pack::PackStore;
use paipuya::{PaipuyaDependencies, PaipuyaSupervisor};
use refetch_service::{RefetchDependencies, RefetchSupervisor};
use register_service::RegisterService;
use watch::WatchRegistry;
use watch_log::WatchLogBuffer;
use watch_service::WatchSupervisor;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub auth: Arc<AuthStore>,
    pub accounts: Arc<accounts::AccountPool>,
    pub catalog: Arc<Catalog>,
    pub kafka: Arc<Kafka>,
    pub mihomo: Arc<MihomoManager>,
    pub objects: Arc<Objects>,
    pub packs: Arc<PackStore>,
    pub watch: Arc<WatchRegistry>,
    pub watch_service: Arc<WatchSupervisor>,
    /// Where the re-fetch walk leaves work. Served by the re-fetch pool's own
    /// sessions, and by any collector with spare time: see `src/refetch.rs`.
    pub refetch: Arc<refetch::RefetchBroker>,
    pub refetch_service: Arc<RefetchSupervisor>,
    pub paipuya: Arc<PaipuyaSupervisor>,
    /// Creating accounts. Holds the collectors' module store because that
    /// is where an installed registrar lives, and the pool because a new
    /// account has to be stored the moment it exists.
    pub register: Arc<RegisterService>,
    pub export_dir: PathBuf,
}

impl AppState {
    pub async fn local(config: Config) -> anyhow::Result<Self> {
        let data_dir = config.data_dir.clone();
        let export_dir = data_dir.join("exports");
        std::fs::create_dir_all(&export_dir)?;
        let objects = Arc::new(Objects::new(
            &config.s3_endpoint_url,
            &config.s3_bucket,
            &config.s3_region,
            &config.s3_access_key,
            &config.s3_secret_key,
        )?);
        let packs = Arc::new(PackStore::new(&data_dir, Arc::clone(&objects))?);
        // Before the recovery scan, and never re-indexed instead: a staging
        // pack holds nothing but records whose Kafka offset was never
        // committed, so the broker still owes every one of them. Indexing it
        // here *and* letting Kafka redeliver it would give one record two rows
        // under two pack keys.
        let discarded = packs.discard_staging()?;
        if discarded > 0 {
            tracing::info!(discarded, "discarded staging packs left by a previous run");
        }
        let catalog = Arc::new(Catalog::connect(&config).await?);
        // Before anything is served: an index missing rows would let the API
        // report a record as absent while its bytes sit in a pack.
        let recovered = recovery::recover(&catalog, &packs).await?;
        if recovered > 0 {
            tracing::info!(recovered, "re-indexed pack entries missing from the index");
        }
        // After the databases, because a broker that is up while ClickHouse is
        // not would let ingest accept records into a topic nothing can drain.
        let kafka = Arc::new(Kafka::connect(&config).await?);
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
        let refetch = Arc::new(refetch::RefetchBroker::default());
        // Opened before anything that reads an account, and shared by both
        // halves: the split between them is the only thing keeping a collector
        // and the re-fetch pool from logging in with one account.
        let accounts = Arc::new(accounts::AccountPool::open(&data_dir)?);
        for password in accounts.secrets() {
            watch_logs.register_secret(password);
        }
        let dependencies = Arc::new(ManagedWatchDependencies {
            data_dir: data_dir.clone(),
            refetch: Arc::clone(&refetch),
            catalog: Arc::clone(&catalog),
            kafka: Arc::clone(&kafka),
            registry: Arc::clone(&watch),
            accounts: Arc::clone(&accounts),
            mihomo: Arc::clone(&mihomo),
            logs: Arc::clone(&watch_logs),
        });
        let watch_service = Arc::new(WatchSupervisor::new(
            &data_dir,
            Arc::clone(&watch),
            dependencies,
            Arc::clone(&watch_logs),
        )?);
        // After the collectors, and holding them: the pool asks them which
        // accounts are spoken for and which protocol modules to use. Nothing
        // points back the other way, so the two `Arc`s do not form a cycle.
        let refetch_service = Arc::new(RefetchSupervisor::new(RefetchDependencies {
            data_dir: data_dir.clone(),
            catalog: Arc::clone(&catalog),
            packs: Arc::clone(&packs),
            kafka: Arc::clone(&kafka),
            broker: Arc::clone(&refetch),
            accounts: Arc::clone(&accounts),
            mihomo: Arc::clone(&mihomo),
            logs: Arc::clone(&watch_logs),
            watch: Arc::clone(&watch_service),
        })?);
        let register = Arc::new(RegisterService::new(
            watch_service.module_store(),
            Arc::clone(&accounts),
            Arc::clone(&mihomo),
            Arc::clone(&watch_logs),
            &data_dir,
        ));
        // Its own service, sharing only the log buffer: it never touches
        // Mahjong Soul, and the catalogue it writes is not the corpus.
        let paipuya = Arc::new(PaipuyaSupervisor::new(PaipuyaDependencies {
            data_dir: data_dir.clone(),
            catalog: Arc::clone(&catalog),
            logs: watch_logs,
        })?);
        Ok(Self {
            accounts,
            auth,
            packs,
            catalog,
            kafka,
            mihomo,
            objects,
            watch,
            watch_service,
            refetch,
            refetch_service,
            paipuya,
            register,
            config: Arc::new(config),
            export_dir,
        })
    }
}
