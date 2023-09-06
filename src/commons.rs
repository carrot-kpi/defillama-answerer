use std::{collections::HashMap, net::Ipv4Addr, path::Path, sync::Arc};

use anyhow::Context;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::types::Address;
use serde::{Deserialize, Serialize};

use crate::http_client::HttpClient;

#[derive(Debug, Serialize, Deserialize)]
pub struct ContractConfig {
    pub address: Address,
    pub deployment_block: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChainConfig {
    pub answerer_private_key: String,
    pub ws_rpc_endpoint: String,
    pub logs_blocks_range: Option<u64>,
    pub template_id: u64,
    pub factory: ContractConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiConfig {
    pub host: Ipv4Addr,
    pub port: u16,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: Ipv4Addr::new(127, 0, 0, 1),
            port: 8080,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Config {
    pub ipfs_api_endpoint: String,
    pub db_connection_string: String,
    pub web3_storage_api_key: Option<String>,
    pub api: ApiConfig,
    pub chain_configs: HashMap<u64, ChainConfig>,
}

pub struct ChainExecutionContext {
    pub chain_id: u64,
    pub ws_rpc_endpoint: Arc<String>,
    pub logs_blocks_range: Option<u64>,
    pub template_id: u64,
    pub answerer_private_key: Arc<String>,
    pub ipfs_http_client: Arc<HttpClient>,
    pub defillama_http_client: Arc<HttpClient>,
    pub web3_storage_http_client: Option<Arc<HttpClient>>,
    pub db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    pub factory_config: ContractConfig,
}

pub fn get_config(alt_path: Option<String>) -> anyhow::Result<Config> {
    let default_path = confy::get_configuration_file_path("", "carrot-defillama-answerer")
        .context("could not get default config path for platform")?
        .to_string_lossy()
        .to_string();
    let raw_path = alt_path.unwrap_or(default_path);
    let path = Path::new(raw_path.as_str())
        .canonicalize()
        .context(format!("could not canonicalize config path {raw_path}"))?
        .to_string_lossy()
        .to_string();

    tracing::info!("using path {} to read config", path);
    confy::load_path::<Config>(path).context("could not read config")
}
