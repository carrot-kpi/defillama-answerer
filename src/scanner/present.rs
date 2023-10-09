use std::{
    num::NonZeroU32,
    ops::Deref,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use backoff::ExponentialBackoffBuilder;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::{
    abi::Address,
    contract::EthEvent,
    providers::{Middleware, StreamExt},
    types::{Block, Filter, H256},
    utils,
};
use governor::{Quota, RateLimiter};
use tokio::sync::{oneshot, Mutex};
use tracing::info_span;
use tracing_futures::Instrument;

use crate::{
    commons::ChainExecutionContext,
    contracts::{
        defi_llama_oracle::DefiLlamaOracle, factory::CreateTokenFilter, kpi_token::KPIToken,
    },
    db::models::{self, ActiveOracle, Checkpoint},
    http_client::HttpClient,
    scanner::commons::{acknowledge_active_oracles, parse_kpi_token_creation_logs},
    signer::{get_signer, Signer},
    specification::{self},
};

pub async fn scan(
    receiver: oneshot::Receiver<bool>,
    context: Arc<ChainExecutionContext>,
) -> anyhow::Result<()> {
    let update_snapshot_block_number = Arc::new(Mutex::new(false));

    tokio::spawn(
        message_receiver(receiver, update_snapshot_block_number.clone())
            .instrument(info_span!("message-receiver")),
    );

    // enforces a minimum wait time of 1 sec before attempting to reconnect in
    // the loop
    let rate_limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(1u32).unwrap()));

    loop {
        rate_limiter.until_ready().await;

        let signer = get_signer(
            context.ws_rpc_endpoint.clone(),
            context.answerer_private_key.clone(),
            context.chain_id,
        )
        .await?;

        let mut stream = match signer
            .subscribe_blocks()
            .await
            .context("could not watch for blocks")
        {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!("{:#}", error);
                continue;
            }
        };

        tracing::info!("watching blocks");

        while let Some(block) = stream.next().await {
            let block_number = block.number.unwrap();

            handle_new_active_oracles(signer.clone(), block, context.clone()).await;

            if let Err(error) =
                handle_active_oracles_answering(signer.clone(), context.clone()).await
            {
                tracing::error!("error while handling active oracles answering: {:#}", error);
            }

            if *update_snapshot_block_number.lock().await {
                let database_connection = &mut context
                    .db_connection_pool
                    .get()
                    .context("could not get new connection from pool")?;
                if let Err(error) = Checkpoint::update(
                    database_connection,
                    context.chain_id,
                    block_number.as_u32() as i64,
                ) {
                    tracing::error!("could not update snapshot block number: {:#}", error);
                }
            }
        }
    }
}

async fn message_receiver(
    receiver: oneshot::Receiver<bool>,
    update_snapshot_block_number: Arc<Mutex<bool>>,
) {
    match receiver.await {
        Ok(value) => {
            if !value {
                panic!("snapshot updates ownership channel receiver received a false value: this should never happen");
            } else {
                *update_snapshot_block_number.lock().await = value;
                tracing::info!("snapshot updates ownership taken");
            }
        }
        Err(error) => {
            tracing::error!("error while receiving control over snapshot block number update from past indexer: {:#}", error);
        }
    }
}

async fn handle_new_active_oracles(
    signer: Arc<Signer>,
    block: Block<H256>,
    context: Arc<ChainExecutionContext>,
) -> () {
    let block_number = block.number.unwrap();

    let filter = Filter::new()
        .address(context.factory_config.address)
        .event(CreateTokenFilter::abi_signature().deref())
        .at_block_hash(block.hash.unwrap());

    let fetch_logs = || async {
        signer
            .get_logs(&filter)
            .await
            .map_err(|err| backoff::Error::Transient {
                err: anyhow!("error fetching logs from block {}: {:#}", block_number, err),
                retry_after: None,
            })
    };

    let kpi_token_creation_logs = match backoff::future::retry(
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(20)))
            .build(),
        fetch_logs,
    )
    .await
    {
        Ok(logs) => logs,
        Err(error) => {
            tracing::error!(
                "could not get kpi token creation logs for block {}: {:#}",
                block_number,
                error
            );
            return;
        }
    };

    let oracles_data = parse_kpi_token_creation_logs(
        context.chain_id,
        signer,
        kpi_token_creation_logs,
        context.template_id,
    )
    .await;

    if oracles_data.len() > 0 {
        tracing::info!(
            "block {}: detected {} new active oracle(s)",
            block_number,
            oracles_data.len()
        );
    }

    acknowledge_active_oracles(
        context.chain_id,
        oracles_data,
        context.db_connection_pool.clone(),
        context.ipfs_http_client.clone(),
        context.defillama_http_client.clone(),
        context.web3_storage_http_client.clone(),
    )
    .await;
}

async fn handle_active_oracles_answering(
    signer: Arc<Signer>,
    context: Arc<ChainExecutionContext>,
) -> anyhow::Result<()> {
    let mut db_connection = match context.db_connection_pool.get() {
        Ok(connection) => connection,
        Err(error) => {
            tracing::error!("could not get new connection from pool: {:#}", error);
            return Ok(());
        }
    };

    let active_oracles = match models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut db_connection,
        context.chain_id,
    ) {
        Ok(oracles) => oracles,
        Err(error) => {
            tracing::error!(
                "could not get currently active oracles in chain with id {}: {:#}",
                context.chain_id,
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

    let chain_id = context.chain_id;
    for active_oracle in active_oracles.into_iter() {
        let oracle_address = format!("0x{:x}", active_oracle.address.0);
        let oracle_address_clone = oracle_address.clone();
        if let Err(err) = answer_active_oracle(
            signer.clone(),
            context.db_connection_pool.clone(),
            context.defillama_http_client.clone(),
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
    signer: Arc<Signer>,
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
    signer: Arc<Signer>,
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
    signer: Arc<Signer>,
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
