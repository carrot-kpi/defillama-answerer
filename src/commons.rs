use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, time::Duration};

use carrot_commons::http_client::HttpClient;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::types::Address;
use serde::{Deserialize, Serialize};

pub const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
pub const FETCH_SPECIFICATION_JSON_MAX_ELAPSED_TIME: Duration = Duration::from_secs(6_000);
pub const PIN_CID_LOCALLY_MAX_ELAPSED_TIME: Duration = Duration::from_secs(6_000);
pub const PIN_CID_WEB3_STORAGE_MAX_ELAPSED_TIME: Duration = Duration::from_secs(6_000);

#[derive(Debug, Serialize, Deserialize)]
pub struct ContractConfig {
    pub address: Address,
    pub deployment_block: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChainConfig {
    pub answerer_private_key: String,
    pub rpc_endpoint: String,
    pub logs_blocks_range: Option<u64>,
    pub logs_polling_interval_seconds: Option<u64>,
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
    pub dev_mode: Option<bool>,
    pub api: ApiConfig,
    pub chain_configs: HashMap<u64, ChainConfig>,
}

pub struct ChainExecutionContext {
    pub chain_id: u64,
    pub rpc_endpoint: Arc<String>,
    pub logs_blocks_range: Option<u64>,
    pub blocks_polling_interval_seconds: Option<u64>,
    pub template_id: u64,
    pub answerer_private_key: Arc<String>,
    pub ipfs_http_client: Arc<HttpClient>,
    pub defillama_http_client: Arc<HttpClient>,
    pub web3_storage_http_client: Option<Arc<HttpClient>>,
    pub db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    pub factory_config: ContractConfig,
    pub dev_mode: bool,
}
