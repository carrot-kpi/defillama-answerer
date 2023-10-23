pub mod api;
pub mod commons;
pub mod contracts;
pub mod db;
pub mod http_client;
pub mod ipfs;
pub mod listener;
pub mod specification;

use std::{env, num::NonZeroU32, ops::Deref, process::exit, sync::Arc, time::Duration};

use anyhow::Context;
use ethers::{
    contract::EthEvent,
    middleware::SignerMiddleware,
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
    types::Filter,
};
use governor::{Quota, RateLimiter};
use mibs::{chain_config::ChainConfig, MibsBuilder};
use tracing_subscriber::{filter::LevelFilter, EnvFilter, FmtSubscriber};

use crate::{
    contracts::factory::CreateTokenFilter, db::models, http_client::HttpClient, listener::Listener,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

const DEFAULT_LOGS_POLLING_INTERVAL_SECONDS: u64 = 30;
const MAX_CALLS_PER_SECOND_DEFILLAMA: u32 = 7;
const MAX_CALLS_PER_SECOND_WEB3_STORAGE: u32 = 3;

fn setup_logging() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .json()
        .with_span_list(true)
        .with_current_span(false)
        .with_env_filter(
            EnvFilter::builder()
                .with_env_var("LOG_LEVEL")
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .context("could not get log level")?,
        )
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).context("tracing initialization failed")
}

pub async fn main() -> anyhow::Result<()> {
    if let Err(error) = setup_logging().context("could not initialize logging system") {
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

    let mut mibs_builder = MibsBuilder::new();
    for (chain_id, chain_config) in config.chain_configs.into_iter() {
        let rpc_endpoint = chain_config.rpc_endpoint.as_str();

        tracing::info!(
            "setting up mibs config for chain with id {} with rpc endpoint: {}",
            chain_id,
            rpc_endpoint
        );

        let mut db_connection = db_connection_pool
            .clone()
            .get()
            .context("could not get database connection")?;
        let checkpoint_block = models::Checkpoint::get_for_chain_id(&mut db_connection, chain_id)
            .context("could not get checkpoint block")?
            .map(|checkpoint| {
                // realistically, the following should never happen
                u64::try_from(checkpoint.block_number).expect(
                    format!(
                        "could not convert checkpoint block number {} to unsigned integer",
                        checkpoint.block_number
                    )
                    .as_str(),
                )
            })
            .unwrap_or(chain_config.factory.deployment_block);
        drop(db_connection);

        let rpc_url = chain_config.rpc_endpoint;
        let answerer_wallet = chain_config
            .answerer_private_key
            .parse::<LocalWallet>()
            .context("could not parse private key to local wallet")?;

        let provider = Arc::new(
            Provider::<Http>::try_from(rpc_url.clone())
                .context(format!("could not get provider for chain {chain_id}"))?,
        );

        let signer = Arc::new(SignerMiddleware::new(
            Provider::<Http>::try_from(rpc_url)
                .context(format!("could not get signer for chain {chain_id}"))?,
            answerer_wallet.with_chain_id(chain_id),
        ));

        let chain_config_builder = ChainConfig::builder(
            chain_id,
            provider.clone(),
            checkpoint_block,
            Filter::new()
                .address(vec![chain_config.factory.address])
                .event(CreateTokenFilter::abi_signature().deref()),
            Listener::new(
                chain_id,
                chain_config.template_id,
                signer.clone(),
                db_connection_pool.clone(),
                ipfs_http_client.clone(),
                defillama_http_client.clone(),
                web3_storage_http_client.clone(),
            ),
        )
        .past_events_query_max_rps(Some(1))
        .past_events_query_range(chain_config.logs_blocks_range)
        .present_events_polling_interval(Duration::from_secs(
            chain_config
                .logs_polling_interval_seconds
                .unwrap_or(DEFAULT_LOGS_POLLING_INTERVAL_SECONDS),
        ))
        .skip_past(config.dev_mode);

        mibs_builder = mibs_builder.chain_config(chain_config_builder.build());
    }

    mibs_builder
        .build()
        .scan()
        .await
        .map_err(|err| anyhow::anyhow!(err))
}
