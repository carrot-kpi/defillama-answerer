use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use backoff::ExponentialBackoffBuilder;
use carrot_commons::{data, http_client::HttpClient};
use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use ethers::{
    abi::RawLog,
    contract::{EthLogDecode, Multicall},
    middleware::SignerMiddleware,
    providers::{Http, Provider},
    signers::LocalWallet,
    types::{Address, Log, U256},
};
use tokio::task::JoinSet;
use tracing::info_span;
use tracing_futures::Instrument;

use crate::{
    commons::{FETCH_SPECIFICATION_JSON_MAX_ELAPSED_TIME, STORE_CID_MAX_ELAPSED_TIME},
    contracts::{
        defi_llama_oracle::{DefiLlamaOracle, Template},
        factory::FactoryEvents,
        kpi_token::KPIToken,
    },
    db::models::{self},
    specification::{self, Specification},
};

pub struct DefiLlamaOracleData {
    address: Address,
    measurement_timestamp: SystemTime,
    specification_cid: String,
    expiration: SystemTime,
}

pub async fn parse_kpi_token_creation_log(
    chain_id: u64,
    signer: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    log: Log,
    oracle_template_id: u64,
) -> anyhow::Result<Vec<DefiLlamaOracleData>> {
    let raw_log = RawLog {
        topics: log.topics,
        data: log.data.to_vec(),
    };
    let token_address = match FactoryEvents::decode_log(&raw_log) {
        Ok(FactoryEvents::CreateTokenFilter(data)) => data.token,
        _ => {
            tracing::warn!("tried to decode an invalid log");
            return Ok(Vec::new());
        }
    };

    let mut data = Vec::new();
    let mut multicall = Multicall::new_with_chain_id(signer.clone(), None, Some(chain_id))?;
    let kpi_token = KPIToken::new(token_address, signer.clone());
    let (oracle_addresses, kpi_token_expiration) = multicall
        .add_call(kpi_token.oracles(), false)
        .add_call(kpi_token.expiration(), false)
        .call::<(Vec<Address>, U256)>()
        .await
        .context(format!(
            "could not get oracles and expiration for kpi token 0x{:x}",
            token_address
        ))?;
    let kpi_token_expiration = UNIX_EPOCH + Duration::from_secs(kpi_token_expiration.as_u64());
    multicall.clear_calls();
    for oracle_address in oracle_addresses.into_iter() {
        let oracle = DefiLlamaOracle::new(oracle_address, signer.clone());
        match multicall
            .add_call(oracle.finalized(), false)
            .add_call(oracle.template(), false)
            .call::<(bool, Template)>()
            .await
        {
            Ok((finalized, template)) => {
                if finalized {
                    tracing::info!(
                        "oracle with address 0x{:x} already finalized, skipping",
                        oracle_address
                    );
                    continue;
                }

                if template.id.as_u64() != oracle_template_id {
                    tracing::info!(
                        "oracle with address 0x{:x} doesn't have the right template id, skipping",
                        oracle_address
                    );
                    continue;
                }

                let specification = match oracle.specification().await {
                    Ok(specification) => specification,
                    Err(error) => {
                        tracing::error!(
                            "could not fetch specification cid for oracle at address {}, skipping - {:#}",
                            oracle_address,
                            error
                        );
                        continue;
                    }
                };

                let measurement_timestamp = match oracle.measurement_timestamp().await {
                    Ok(measurement_timestamp) => measurement_timestamp,
                    Err(error) => {
                        tracing::error!(
                            "could not fetch measurement timestamp for oracle at address {}, skipping - {:#}",
                            oracle_address,
                            error
                        );
                        continue;
                    }
                };
                let measurement_timestamp =
                    UNIX_EPOCH + Duration::from_secs(measurement_timestamp.as_u64());

                data.push(DefiLlamaOracleData {
                    address: oracle_address,
                    measurement_timestamp,
                    specification_cid: specification,
                    expiration: kpi_token_expiration,
                });
            }
            Err(error) => {
                tracing::error!(
                    "could not fetch multicall data from oracle 0x{:x}: {:#}",
                    oracle_address,
                    error
                );
            }
        };
    }

    Ok(data)
}

pub async fn acknowledge_active_oracles(
    chain_id: u64,
    oracles_data: Vec<DefiLlamaOracleData>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    data_cdn_http_client: Arc<HttpClient>,
    data_manager_http_client: Arc<HttpClient>,
    ipfs_gateway_http_client: Arc<HttpClient>,
    defillama_http_client: Arc<HttpClient>,
) {
    let mut join_set = JoinSet::new();
    for data in oracles_data.into_iter() {
        let oracle_address = format!("0x{:x}", data.address);
        join_set.spawn(
            acknowledge_active_oracle(
                chain_id,
                data,
                db_connection_pool.clone(),
                data_cdn_http_client.clone(),
                data_manager_http_client.clone(),
                ipfs_gateway_http_client.clone(),
                defillama_http_client.clone(),
            )
            .instrument(tracing::error_span!("ack", chain_id, oracle_address)),
        );
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(result) => {
                if let Err(error) = result {
                    tracing::error!("an active oracle acknowledgement task unexpectedly stopped with an error: {:#}", error);
                }
            }
            Err(error) => {
                tracing::error!(
                    "an unexpected error happened while joining a task: {:#}",
                    error
                );
            }
        }
    }
}

pub async fn acknowledge_active_oracle(
    chain_id: u64,
    oracle_data: DefiLlamaOracleData,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    data_cdn_http_client: Arc<HttpClient>,
    data_manager_http_client: Arc<HttpClient>,
    ipfs_gateway_http_client: Arc<HttpClient>,
    defillama_http_client: Arc<HttpClient>,
) -> anyhow::Result<()> {
    match data::fetch_json_with_retry::<Specification>(
        oracle_data.specification_cid.clone(),
        data_cdn_http_client.clone(),
        ipfs_gateway_http_client.clone(),
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(FETCH_SPECIFICATION_JSON_MAX_ELAPSED_TIME))
            .build(),
    )
    .await
    {
        Ok(specification) => {
            if !specification::validate(&specification, defillama_http_client).await {
                tracing::error!("specification validation failed for oracle at address 0x{:x}, this won't be handled", oracle_data.address);
                return Ok(());
            }

            let database_connection = &mut db_connection_pool
                .get()
                .context("could not get new connection from pool")?;

            models::ActiveOracle::create(
                database_connection,
                oracle_data.address,
                chain_id,
                oracle_data.measurement_timestamp,
                specification,
                oracle_data.expiration,
            )
            .context("could not insert new active oracle into database")?;

            let cid = oracle_data.specification_cid;
            tokio::spawn(
                store_cid_ipfs(cid.clone(), data_manager_http_client.clone())
                    .instrument(info_span!("storing", cid)),
            );

            tracing::info!(
                "oracle with address 0x{:x} saved to database",
                oracle_data.address
            );

            Ok(())
        }
        Err(error) => {
            tracing::error!("{:#}", error);
            Ok(())
        }
    }
}

async fn store_cid_ipfs(cid: String, data_manager_http_client: Arc<HttpClient>) {
    match data::store_cid_ipfs_with_retry(
        cid.clone(),
        data_manager_http_client.clone(),
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(STORE_CID_MAX_ELAPSED_TIME))
            .build(),
    )
    .await
    {
        Ok(_) => {}
        Err(err) => {
            tracing::error!(
                "could not store cid {cid} on ipfs through the data manager service: {err:?}"
            );
        }
    }
}
