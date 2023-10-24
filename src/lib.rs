pub mod api;
pub mod commons;
pub mod contracts;
pub mod db;
pub mod listener;
pub mod specification;

use std::{
    env, num::NonZeroU32, ops::Deref, path::PathBuf, process::exit, sync::Arc, time::Duration,
};

use anyhow::Context;
use carrot_commons::{config::get_config, http_client::HttpClient};
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::{
    contract::EthEvent,
    middleware::SignerMiddleware,
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
    types::Filter,
};
use governor::{Quota, RateLimiter};
use mibs::{chain_config::ChainConfig, MibsBuilder};
use tokio::task::JoinSet;
use tracing::info_span;
use tracing_futures::Instrument;
use tracing_subscriber::{filter::LevelFilter, EnvFilter, FmtSubscriber};

use crate::{
    commons::{Config, HTTP_TIMEOUT},
    contracts::factory::CreateTokenFilter,
    db::models,
    listener::Listener,
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

pub async fn main() {
    if let Err(error) = setup_logging().context("could not initialize logging system") {
        tracing::error!("{:#}", error);
        exit(1);
    }

    let alt_config_path = if let Some(alt_config_path) = env::var("CONFIG_PATH").ok() {
        let mut path = PathBuf::new();
        path.push(alt_config_path);
        Some(path)
    } else {
        None
    };
    let config: Config =
        match get_config("defillama-answerer", alt_config_path).context("could not read config") {
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
    let ipfs_http_client = match HttpClient::builder(config.ipfs_api_endpoint, HTTP_TIMEOUT).build()
    {
        Ok(ipfs_http_client) => ipfs_http_client,
        Err(error) => {
            tracing::error!("{:#}", error);
            exit(1);
        }
    };
    let ipfs_http_client = Arc::new(ipfs_http_client);

    let web3_storage_http_client = config.web3_storage_api_key.map(|token| {
        let web3_storage_http_client =
            match HttpClient::builder("https://api.web3.storage", HTTP_TIMEOUT)
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

    let defillama_http_client = match HttpClient::builder("https://api.llama.fi", HTTP_TIMEOUT)
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

        let checkpoint_block_number = get_checkpoint_block_number(
            chain_id,
            db_connection_pool.clone(),
            chain_config.factory.deployment_block,
        );

        let rpc_url = chain_config.rpc_endpoint;
        let provider = Arc::new(get_provider(chain_id, rpc_url.clone()));
        let signer = Arc::new(get_signer(
            chain_id,
            rpc_url,
            chain_config.answerer_private_key,
        ));

        let chain_config_builder = ChainConfig::builder(
            chain_id,
            provider,
            checkpoint_block_number,
            Filter::new()
                .address(vec![chain_config.factory.address])
                .event(CreateTokenFilter::abi_signature().deref()),
            Listener::new(
                chain_id,
                chain_config.template_id,
                signer,
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

    let mut join_set = JoinSet::new();
    join_set.spawn(
        async {
            mibs_builder
                .build()
                .scan()
                .await
                .map_err(|err| anyhow::anyhow!(err))
        }
        .instrument(info_span!("mibs")),
    );
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
                    tracing::error!("a task unexpectedly stopped with an error: {:#}", error);
                    exit(1);
                }
            }
            Err(error) => {
                tracing::error!("an error happened while joining a task: {:#}", error);
                exit(1);
            }
        }
    }
}

fn get_checkpoint_block_number(
    chain_id: u64,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    factory_deployment_block: u64,
) -> u64 {
    let mut db_connection = match db_connection_pool.get() {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!("could not get database connection to get checkpoint block: {err:#}");
            exit(1);
        }
    };

    let checkpoint_block = match models::Checkpoint::get_for_chain_id(&mut db_connection, chain_id)
    {
        Ok(checkpoint_block) => checkpoint_block,
        Err(err) => {
            tracing::error!("could not get checkpoint block: {err:#}");
            exit(1);
        }
    };

    checkpoint_block
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
        .unwrap_or(factory_deployment_block)
}

fn get_provider(chain_id: u64, rpc_url: String) -> Provider<Http> {
    match Provider::<Http>::try_from(rpc_url.clone()) {
        Ok(provider) => provider,
        Err(err) => {
            tracing::error!("could not get provider for chain {chain_id}: {err:#}");
            exit(1);
        }
    }
}

fn get_signer(
    chain_id: u64,
    rpc_url: String,
    answerer_private_key: String,
) -> SignerMiddleware<Provider<Http>, LocalWallet> {
    let answerer_wallet = match answerer_private_key.parse::<LocalWallet>().context("t") {
        Ok(wallet) => wallet,
        Err(err) => {
            tracing::error!("could not parse private key to local wallet: {err:#}");
            exit(1);
        }
    };

    let provider = get_provider(chain_id, rpc_url.clone());
    SignerMiddleware::new(provider, answerer_wallet.with_chain_id(chain_id))
}
