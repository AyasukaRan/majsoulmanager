pub mod api;
pub mod catalog;
pub mod config;
pub mod mjai;
pub mod pack;

use std::{path::PathBuf, sync::Arc};

use catalog::Catalog;
use config::Config;
use pack::PackStore;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub catalog: Arc<Catalog>,
    pub packs: Arc<PackStore>,
    pub export_dir: PathBuf,
}

impl AppState {
    pub fn local(config: Config) -> anyhow::Result<Self> {
        let data_dir = config.data_dir.clone();
        let pack_dir = data_dir.join("packs");
        let export_dir = data_dir.join("exports");
        std::fs::create_dir_all(&export_dir)?;
        Ok(Self {
            packs: Arc::new(PackStore::new(pack_dir, config.pack_target_bytes)?),
            catalog: Arc::new(Catalog::default()),
            config: Arc::new(config),
            export_dir,
        })
    }
}
