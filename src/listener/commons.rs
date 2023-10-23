use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use ethers::{
    abi::RawLog,
    contract::{EthLogDecode, Multicall},
    middleware::{Middleware, SignerMiddleware},
    providers::{Http, Provider},
    signers::LocalWallet,
    types::{Address, Log, U256},
    utils,
};
use tokio::task::JoinSet;
use tracing::info_span;
use tracing_futures::Instrument;

use crate::{
    contracts::{
        defi_llama_oracle::{DefiLlamaOracle, Template},
        factory::FactoryEvents,
        kpi_token::KPIToken,
    },
    db::models::{self, ActiveOracle},
    http_client::HttpClient,
    ipfs, specification,
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
    ipfs_http_client: Arc<HttpClient>,
    defillama_http_client: Arc<HttpClient>,
    web3_storage_http_client: Option<Arc<HttpClient>>,
) {
    let mut join_set = JoinSet::new();
    for data in oracles_data.into_iter() {
        let oracle_address = format!("0x{:x}", data.address);
        join_set.spawn(
            acknowledge_active_oracle(
                chain_id,
                data,
                db_connection_pool.clone(),
                ipfs_http_client.clone(),
                defillama_http_client.clone(),
                web3_storage_http_client.clone(),
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
    ipfs_http_client: Arc<HttpClient>,
    defillama_http_client: Arc<HttpClient>,
    web3_storage_http_client: Option<Arc<HttpClient>>,
) -> anyhow::Result<()> {
    match ipfs::fetch_specification_with_retry(
        ipfs_http_client.clone(),
        &oracle_data.specification_cid,
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

            if let Some(web3_storage_http_client) = web3_storage_http_client {
                let oracle_address = format!("0x{:x}", oracle_data.address);
                tokio::spawn(ipfs::pin_on_web3_storage_with_retry(
                    ipfs_http_client,
                    web3_storage_http_client,
                    oracle_data.specification_cid.clone(),
                ))
                .instrument(info_span!(
                    "web3-storage-pinner",
                    chain_id,
                    oracle_address
                ));
            }

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

pub async fn handle_active_oracles_answering(
    chain_id: u64,
    signer: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    defillama_http_client: Arc<HttpClient>,
) -> anyhow::Result<()> {
    let mut db_connection = match db_connection_pool.get() {
        Ok(connection) => connection,
        Err(error) => {
            tracing::error!("could not get new connection from pool: {:#}", error);
            return Ok(());
        }
    };

    let active_oracles =
        match models::ActiveOracle::get_all_answerable_for_chain_id(&mut db_connection, chain_id) {
            Ok(oracles) => oracles,
            Err(error) => {
                tracing::error!(
                    "could not get currently active oracles in chain with id {}: {:#}",
                    chain_id,
                    error
                );
                return Ok(());
            }
        };
    drop(db_connection);

    let active_oracles_len = active_oracles.len();
    if active_oracles_len > 0 {
        tracing::info!("trying to answer {} active oracles", active_oracles_len);
    }

    let chain_id = chain_id;
    for active_oracle in active_oracles.into_iter() {
        let oracle_address = format!("0x{:x}", active_oracle.address.0);
        let oracle_address_clone = oracle_address.clone();
        if let Err(err) = answer_active_oracle(
            signer.clone(),
            db_connection_pool.clone(),
            defillama_http_client.clone(),
            active_oracle,
        )
        .instrument(info_span!("answer", chain_id, oracle_address))
        .await
        {
            tracing::error!(
                "error while answering oracle {}, ADDRESS IMMEDIATELY: {:#}",
                oracle_address_clone,
                err
            );
        }
    }

    Ok(())
}

async fn answer_active_oracle(
    signer: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    defillama_http_client: Arc<HttpClient>,
    mut active_oracle: models::ActiveOracle,
) -> anyhow::Result<()> {
    if let Some(tx_hash) = active_oracle.answer_tx_hash {
        tracing::warn!(
            "answering procedure already active for oracle with tx hash 0x{:x}, skipping",
            tx_hash.0
        );
        return Ok(());
    }

    match is_active_oracle_expired(
        db_connection_pool.clone(),
        signer.clone(),
        &mut active_oracle,
    )
    .await
    {
        Ok(expired) => {
            if expired {
                let mut db_connection = match db_connection_pool
                    .get()
                    .context("could not get new connection from pool")
                {
                    Ok(db_connection) => db_connection,
                    Err(error) => {
                        tracing::error!(
                    "could not get database connection while trying to update oracle's answer tx hash: {:#}",
                    error
                );
                        return Ok(());
                    }
                };

                tracing::warn!("oracle is expired, skipping and deleting");
                if let Err(error) = active_oracle.delete(&mut db_connection) {
                    tracing::error!("{:#}", error);
                }
                return Ok(());
            }
        }
        Err(error) => {
            tracing::error!("could not get expiration status for oracle: {:#}", error);
            return Ok(());
        }
    }

    let answer = match &active_oracle.answer {
        Some(answer) => {
            tracing::info!("reusing saved answer {}", answer.0);
            Some(answer.0)
        }
        None => {
            let answer =
                specification::answer(&active_oracle.specification, defillama_http_client).await;
            if let Some(answer) = answer {
                let mut db_connection = match db_connection_pool
                    .get()
                    .context("could not get new connection from pool")
                {
                    Ok(db_connection) => db_connection,
                    Err(error) => {
                        tracing::error!(
                    "could not get database connection while trying to update oracle's answer: {:#}",
                    error
                );
                        return Ok(());
                    }
                };

                if let Err(error) = active_oracle.update_answer(&mut db_connection, answer) {
                    tracing::error!("{:#}", error);
                    return Ok(());
                }
            }
            answer
        }
    };
    if let Some(answer) = answer {
        // if we arrive here, an answer is available and we should submit it

        tracing::info!("answering with value {}", answer);
        let oracle = DefiLlamaOracle::new(active_oracle.address.0, signer.clone());
        let mut call = oracle.finalize(answer);

        match signer.fill_transaction(&mut call.tx, None).await {
            Ok(()) => {}
            Err(error) => {
                tracing::error!("could not fill answer call: {:#}", error);
                return Ok(());
            }
        };

        let tx = match call.send().await {
            Ok(tx) => tx,
            Err(error) => {
                tracing::error!(
                    "error while submitting answer transaction {:?}: {:#}",
                    call.tx,
                    error
                );
                return Ok(());
            }
        };

        {
            let mut db_connection = match db_connection_pool
                .get()
                .context("could not get new connection from pool")
            {
                Ok(db_connection) => db_connection,
                Err(error) => {
                    tracing::error!(
                        "could not get database connection while trying to update oracle's answer tx hash: {:#}",
                        error
                    );
                    return Ok(());
                }
            };

            if let Err(error) =
                active_oracle.update_answer_tx_hash(&mut db_connection, tx.tx_hash())
            {
                tracing::error!("{:#}", error);
                return Ok(());
            }
        }

        let debug_tx = format!("{:?}", tx);
        let receipt = match tx.await {
            Ok(receipt) => receipt,
            Err(error) => {
                // we need to throw the following errors as these needs to be addressed immediately.
                // not being able to delete the tx hash for an oracle once an answer task errors out
                // might cause a deadlock preventing any answering task from starting in the future

                tracing::error!(
                    "error while confirming answer transaction {}: {:#}",
                    debug_tx,
                    error
                );
                let mut db_connection = db_connection_pool
                    .get()
                    .context("could not get database connection while trying to delete oracle's answer tx hash")?;
                active_oracle.delete_answer_tx_hash(&mut db_connection).context("could not delete active answer transaction hash; the oracle resolution process is now stuck, ACT IMMEDIATELY")?;

                return Ok(());
            }
        };

        if let Some(receipt) = receipt {
            if let (Some(gas_used), Some(effective_gas_price)) =
                (receipt.gas_used, receipt.effective_gas_price)
            {
                // assuming it's always 18 decimals
                let fee = gas_used * effective_gas_price;
                let formatted = match utils::format_units(gas_used * effective_gas_price, 18) {
                    Ok(formatted) => formatted,
                    Err(error) => {
                        tracing::error!("could not format units for raw fee {}: {:#}", fee, error);
                        return Ok(());
                    }
                };
                tracing::info!("paid {} to answer oracle", formatted);
            }
        } else {
            tracing::warn!("could not determine paid amount to answer oracle");
        }

        let mut db_connection = match db_connection_pool
            .get()
            .context("could not get new connection from pool")
        {
            Ok(db_connection) => db_connection,
            Err(error) => {
                tracing::error!(
                        "could not get database connection while trying to update oracle's answer tx hash: {:#}",
                        error
                    );
                return Ok(());
            }
        };
        if let Err(error) = active_oracle.delete(&mut db_connection) {
            tracing::error!("could not delete oracle from database: {:#}", error);
            return Ok(());
        }

        tracing::info!("oracle successfully finalized with value {}", answer);
    }

    Ok(())
}

async fn is_active_oracle_expired(
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    signer: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    active_oracle: &mut ActiveOracle,
) -> anyhow::Result<bool> {
    let expiration = match active_oracle.expiration {
        Some(expiration) => expiration,
        None => {
            let expiration =
                fetch_active_oracle_expiration(signer.clone(), active_oracle.address.0)
                    .await
                    .context(format!(
                        "could not fetch active oracle 0x{:x} expiration",
                        active_oracle.address.0
                    ))?;
            let mut db_connection = db_connection_pool.get().context(format!(
                "could not get database connection while trying to update oracle's expiration"
            ))?;
            active_oracle.update_expiration(&mut db_connection, expiration)?;
            expiration
        }
    };

    Ok(expiration <= SystemTime::now())
}

async fn fetch_active_oracle_expiration(
    signer: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    address: Address,
) -> anyhow::Result<SystemTime> {
    let oracle = DefiLlamaOracle::new(address, signer.clone());
    let kpi_token_address = oracle.kpi_token().call().await.context(format!(
        "could not fetch kpi token address for oracle 0x{:x}",
        address
    ))?;
    let kpi_token = KPIToken::new(kpi_token_address, signer.clone());
    let expiration = kpi_token.expiration().call().await.context(format!(
        "could not fetch expiration timestamp for kpi token 0x{:x}",
        kpi_token_address
    ))?;
    Ok(UNIX_EPOCH + Duration::from_secs(expiration.as_u64()))
}
