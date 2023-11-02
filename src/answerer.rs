use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use carrot_commons::http_client::HttpClient;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::{
    middleware::{Middleware, SignerMiddleware},
    providers::{Http, Provider},
    signers::LocalWallet,
    types::Address,
    utils,
};
use tokio::time::interval;
use tracing::{info_span, Instrument};

use crate::{
    commons::{ChainConfig, ANSWERING_TASK_INTERVAL_SECONDS},
    contracts::{defi_llama_oracle::DefiLlamaOracle, kpi_token::KPIToken},
    db::models::{self, ActiveOracle},
    specification,
};

pub async fn answer_active_oracles(
    chain_id: u64,
    chain_config: ChainConfig,
    signer: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    defillama_http_client: Arc<HttpClient>,
) -> anyhow::Result<()> {
    let duration = chain_config
        .answering_task_interval_seconds
        .map(|seconds| Duration::from_secs(seconds))
        .unwrap_or(ANSWERING_TASK_INTERVAL_SECONDS);
    let mut interval = interval(duration);

    tracing::info!("answering active oracles every {}s", duration.as_secs());

    loop {
        interval.tick().await;

        if let Err(error) = handle_active_oracles_answering(
            chain_id,
            signer.clone(),
            db_connection_pool.clone(),
            defillama_http_client.clone(),
        )
        .await
        {
            tracing::error!("error while handling active oracles answering: {:#}", error);
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
