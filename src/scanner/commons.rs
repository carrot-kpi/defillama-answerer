use std::sync::Arc;

use anyhow::Context;
use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use ethers::{
    abi::{Detokenize, RawLog, Token, Tokenizable},
    contract::{EthLogDecode, Multicall},
    types::{Address, Bytes, Log},
};
use tokio::task::JoinSet;
use tracing_futures::Instrument;

use crate::{
    contracts::{
        defi_llama_oracle::{DefiLlamaOracle, Template},
        factory::FactoryEvents,
        kpi_token::KPIToken,
    },
    db::models,
    http_client::HttpClient,
    ipfs,
    signer::Signer,
    specification,
};

pub struct DefiLlamaOracleData {
    address: Address,
    specification_cid: String,
}

pub async fn parse_kpi_token_creation_logs(
    chain_id: u64,
    signer: Arc<Signer>,
    logs: Vec<Log>,
    oracle_template_id: u64,
) -> Vec<DefiLlamaOracleData> {
    let mut oracles_data = Vec::with_capacity(logs.len());
    for log in logs.into_iter() {
        match parse_kpi_token_creation_log(chain_id, signer.clone(), log, oracle_template_id).await
        {
            Ok(oracle_data) => oracles_data.extend(oracle_data),
            Err(error) => {
                tracing::warn!("could not extract oracle data from log - {}", error);
                continue;
            }
        };
    }
    oracles_data
}

pub async fn parse_kpi_token_creation_log(
    chain_id: u64,
    signer: Arc<Signer>,
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
    let oracle_addresses = KPIToken::new(token_address, signer.clone())
        .oracles()
        .call()
        .await
        .context(format!(
            "could not get oracles for kpi token {}",
            token_address
        ))?;
    for oracle_address in oracle_addresses.into_iter() {
        let oracle = DefiLlamaOracle::new(oracle_address, signer.clone());
        match Multicall::new_with_chain_id(signer.clone(), None, Some(chain_id))?
            .add_call(oracle.finalized(), false)
            .add_call(oracle.template(), false)
            .add_call(oracle.specification(), true)
            .call_raw()
            .await
        {
            Ok(results) => {
                let results_len = results.len();
                if results_len != 3 {
                    tracing::error!(
                        "inconsistent results len {} (expected 3) while using multicall to fetch data for oracle at address {}",
                        results_len,
                        oracle_address
                    );
                    continue;
                }

                let mut results_iter = results.into_iter();

                let specification =
                    match parse_multicall_result::<String>(results_iter.nth(2).unwrap()) {
                        Some(specification) => specification,
                        None => {
                            // if the oracle has no specification function it automatically is not a defillama
                            // oracle, so we're not interested in it
                            continue;
                        }
                    };

                let finalized = match parse_multicall_result::<bool>(results_iter.nth(0).unwrap()) {
                    Some(finalized) => finalized,
                    None => {
                        tracing::error!(
                            "could not fetch finalization status for oracle at address {}",
                            oracle_address
                        );
                        continue;
                    }
                };

                let template =
                    match parse_multicall_result::<Template>(results_iter.nth(1).unwrap()) {
                        Some(template) => template,
                        None => {
                            tracing::error!(
                                "could not fetch template for oracle at address {}",
                                oracle_address
                            );
                            continue;
                        }
                    };

                if !finalized && template.id.as_u64() == oracle_template_id {
                    data.push(DefiLlamaOracleData {
                        address: oracle_address,
                        specification_cid: specification,
                    });
                }
            }
            Err(_) => continue,
        };
    }

    Ok(data)
}

fn parse_multicall_result<D: Tokenizable + Detokenize>(result: Result<Token, Bytes>) -> Option<D> {
    match result {
        Ok(token) => <(bool, D)>::from_token(token)
            .map(|(_, result)| result)
            .ok(),
        Err(_) => None,
    }
}

pub async fn acknowledge_active_oracles(
    chain_id: u64,
    oracles_data: Vec<DefiLlamaOracleData>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    ipfs_http_client: Arc<HttpClient>,
    web3_storage_http_client: Option<Arc<HttpClient>>,
) {
    let mut join_set = JoinSet::new();
    for data in oracles_data.into_iter() {
        join_set.spawn(
            acknowledge_active_oracle(
                chain_id,
                data.address,
                data.specification_cid,
                db_connection_pool.clone(),
                ipfs_http_client.clone(),
                web3_storage_http_client.clone(),
            )
            .instrument(tracing::error_span!("ack", chain_id)),
        );
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Err(error) => {
                tracing::error!("error while handling oracle creation - {}", error)
            }
            _ => {}
        };
    }
}

pub async fn acknowledge_active_oracle(
    chain_id: u64,
    oracle_address: Address,
    specification_cid: String,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    ipfs_http_client: Arc<HttpClient>,
    web3_storage_http_client: Option<Arc<HttpClient>>,
) -> anyhow::Result<()> {
    match ipfs::fetch_specification_with_retry(ipfs_http_client.clone(), &specification_cid).await {
        Ok(specification) => {
            if !specification::validate(&specification).await {
                // TODO: maybe add some sort of warning here?
                return Ok(());
            }

            let database_connection = &mut db_connection_pool
                .get()
                .context("could not get new connection from pool")?;

            models::ActiveOracle::create(
                database_connection,
                oracle_address,
                chain_id,
                specification,
            )
            .context("could not insert new active oracle into database")?;

            if let Some(web3_storage_http_client) = web3_storage_http_client {
                ipfs::pin_cid_web3_storage_with_retry(
                    ipfs_http_client,
                    web3_storage_http_client,
                    &specification_cid,
                )
                .await?;
            }

            Ok(())
        }
        Err(error) => {
            tracing::error!("{}", error);
            Ok(())
        }
    }
}
