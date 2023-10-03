pub mod api;
pub mod commons;
pub mod contracts;
pub mod db;
pub mod http_client;
pub mod ipfs;
pub mod scanner;
pub mod signer;
pub mod specification;
pub mod telemetry;

use std::{env, num::NonZeroU32, process::exit, sync::Arc};

use anyhow::Context;
use governor::{Quota, RateLimiter};
use tokio::task::JoinSet;
use tracing::{info_span, Instrument};

use crate::{commons::ChainExecutionContext, http_client::HttpClient};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

const MAX_CALLS_PER_SECOND_DEFILLAMA: u32 = 7;
const MAX_CALLS_PER_SECOND_WEB3_STORAGE: u32 = 3;

pub async fn main() {
    if let Err(error) = telemetry::init().context("could not initialize logging system") {
        tracing::error!("{:#}", error);
        exit(1);
    }

    let alt_config_path = env::var("CONFIG_PATH").ok();
    let config = match commons::get_config(alt_config_path).context("could not read config") {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("{:#}", error);
            exit(1);
        }
    };

    if config.dev_mode {
        tracing::info!("running in dev mode, past-indexer disabled");
    }

    tracing::info!("connecting to database");
    let db_connection_pool = match db::connect(&config.db_connection_string) {
        Ok(db_connection_pool) => db_connection_pool,
        Err(error) => {
            tracing::error!("{:#}", error);
            exit(1);
        }
    };

    {
        let mut db_connection = match db_connection_pool
            .get()
            .context("could not get connection to database to run migrations")
        {
            Ok(db_connection) => db_connection,
            Err(error) => {
                tracing::error!("{:#}", error);
                exit(1);
            }
        };

        tracing::info!("running pending database migrations");
        db_connection.run_pending_migrations(MIGRATIONS).unwrap();
    }

    tracing::info!("ipfs api endpoint: {}", config.ipfs_api_endpoint);
    let ipfs_http_client = match HttpClient::builder()
        .base_url(config.ipfs_api_endpoint.to_owned())
        .build()
    {
        Ok(ipfs_http_client) => ipfs_http_client,
        Err(error) => {
            tracing::error!("{:#}", error);
            exit(1);
        }
    };
    let ipfs_http_client = Arc::new(ipfs_http_client);

    let web3_storage_http_client = config.web3_storage_api_key.map(|token| {
        let web3_storage_http_client = match HttpClient::builder()
            .base_url("https://api.web3.storage".to_owned())
            .bearer_auth_token(token)
            .rate_limiter(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(MAX_CALLS_PER_SECOND_WEB3_STORAGE).unwrap(),
            )))
            .build()
        {
            Ok(web3_storage_http_client) => web3_storage_http_client,
            Err(error) => {
                tracing::error!("{:#}", error);
                exit(1);
            }
        };
        let web3_storage_http_client = Arc::new(web3_storage_http_client);
        tracing::info!("web3.storage pinning is enabled");
        web3_storage_http_client
    });

    let defillama_http_client = match HttpClient::builder()
        .base_url("https://api.llama.fi".to_owned())
        .rate_limiter(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(MAX_CALLS_PER_SECOND_DEFILLAMA).unwrap(),
        )))
        .build()
    {
        Ok(defillama_http_client) => defillama_http_client,
        Err(error) => {
            tracing::error!("{:#}", error);
            exit(1);
        }
    };
    let defillama_http_client = Arc::new(defillama_http_client);

    let mut join_set = JoinSet::new();
    for (chain_id, chain_config) in config.chain_configs.into_iter() {
        let ws_rpc_endpoint = chain_config.ws_rpc_endpoint.as_str();

        tracing::info!(
            "setting up listener for chain with id {} with ws rpc endpoint: {}",
            chain_id,
            ws_rpc_endpoint
        );

        let execution_context = Arc::new(ChainExecutionContext {
            chain_id,
            ws_rpc_endpoint: Arc::new(chain_config.ws_rpc_endpoint),
            logs_blocks_range: chain_config.logs_blocks_range,
            template_id: chain_config.template_id,
            answerer_private_key: Arc::new(chain_config.answerer_private_key),
            ipfs_http_client: ipfs_http_client.clone(),
            defillama_http_client: defillama_http_client.clone(),
            web3_storage_http_client: web3_storage_http_client.clone(),
            db_connection_pool: db_connection_pool.clone(),
            factory_config: chain_config.factory,
            dev_mode: config.dev_mode,
        });

        join_set.spawn(scanner::scan(execution_context));
    }

    join_set.spawn(
        api::serve(
            config.api.host,
            config.api.port,
            defillama_http_client.clone(),
        )
        .instrument(info_span!("api-server")),
    );

    // wait forever unless some task stops with an error
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(result) => {
                if let Err(error) = result {
                    tracing::error!("a task unexpectedly stopped with an error:\n\n{:#}", error);
                    exit(1);
                }
            }
            Err(error) => {
                tracing::error!("an error happened while joining a task:\n\n{:#}", error);
                exit(1);
            }
        }
    }
}
