use std::{ops::Deref, sync::Arc};

use anyhow::Context;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::{
    abi::AbiEncode,
    contract::EthEvent,
    providers::{Middleware, StreamExt},
    types::{Block, Filter, H256},
    utils,
};
use tokio::{
    sync::{oneshot, Mutex},
    task::JoinSet,
};
use tracing::info_span;
use tracing_futures::Instrument;

use crate::{
    commons::ChainExecutionContext,
    contracts::{defi_llama_oracle::DefiLlamaOracle, factory::CreateTokenFilter},
    db::models::{self, Checkpoint},
    defillama::DefiLlamaClient,
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

    loop {
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
                    tracing::error!("could not update snapshot block number - {:#}", error);
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
                todo!()
            } else {
                *update_snapshot_block_number.lock().await = value;
                tracing::info!("snapshot updates ownership taken");
            }
        }
        Err(error) => {
            tracing::error!("error while receiving control over snapshot block number update from past indexer\n\n{:#}", error);
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
        .from_block(block_number);
    let kpi_token_creation_logs = match signer.get_logs(&filter).await {
        Ok(logs) => logs,
        Err(error) => {
            tracing::error!(
                "could not get kpi token creation logs for block {} - {}",
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
            "block {} - detected {} new active oracle(s)",
            block_number,
            oracles_data.len()
        );
    }

    acknowledge_active_oracles(
        context.chain_id,
        oracles_data,
        context.db_connection_pool.clone(),
        context.ipfs_http_client.clone(),
        context.defillama_client.clone(),
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
            tracing::error!("could not get new connection from pool - {:#}", error);
            return Ok(());
        }
    };

    let active_oracles =
        match models::ActiveOracle::get_all_for_chain_id(&mut db_connection, context.chain_id) {
            Ok(oracles) => oracles,
            Err(error) => {
                tracing::error!(
                    "could not get currently active oracles in chain with id {} - {:#}",
                    context.chain_id,
                    error
                );
                return Ok(());
            }
        };

    let active_oracles_len = active_oracles.len();
    if active_oracles_len > 0 {
        tracing::info!("trying to answer {} active oracles", active_oracles_len);
    }

    let mut join_set: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();
    for active_oracle in active_oracles.into_iter() {
        let chain_id = context.chain_id;
        let oracle_address = active_oracle.address.encode_hex();
        join_set.spawn(
            answer_active_oracle(
                signer.clone(),
                context.db_connection_pool.clone(),
                context.defillama_client.clone(),
                active_oracle,
            )
            .instrument(info_span!("answer", chain_id, oracle_address)),
        );
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(result) => {
                if let Err(error) = result {
                    tracing::error!("a task unexpectedly stopped with an error:\n\n{:#}", error);
                }
            }
            Err(error) => {
                tracing::error!("an error happened while joining a task:\n\n{:#}", error);
            }
        }
    }

    Ok(())
}

async fn answer_active_oracle(
    signer: Arc<Signer>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    defillama_client: Arc<DefiLlamaClient>,
    mut active_oracle: models::ActiveOracle,
) -> anyhow::Result<()> {
    if let Some(tx_hash) = active_oracle.answer_tx_hash {
        tracing::info!(
            "answering procedure already active for oracle with tx hash {}, skipping",
            tx_hash.0.encode_hex()
        );
        return Ok(());
    }

    if let Some(answer) =
        specification::answer(&active_oracle.specification, defillama_client).await
    {
        // if we arrive here, an answer is available and we should submit it

        tracing::info!(
            "answering active oracle {} with value {}",
            active_oracle.address.encode_hex(),
            answer
        );
        let oracle = DefiLlamaOracle::new(active_oracle.address.0, signer);
        let tx = oracle.finalize(answer);
        let tx = match tx.send().await {
            Ok(tx) => tx,
            Err(error) => {
                tracing::error!(
                    "error while sending answer transaction to oracle {} - {}",
                    active_oracle.address.deref(),
                    error,
                );
                return Ok(());
            }
        };

        let mut db_connection = match db_connection_pool
            .get()
            .context("could not get new connection from pool")
        {
            Ok(db_connection) => db_connection,
            Err(error) => {
                tracing::error!(
                    "could not get database connection while trying to delete oracle from database - {}",
                    error
                );
                return Ok(());
            }
        };
        if let Err(error) = active_oracle.update_answer_tx_hash(&mut db_connection, tx.tx_hash()) {
            tracing::error!("{:#}", error);
            return Ok(());
        }

        let receipt = match tx.await {
            Ok(receipt) => receipt,
            Err(error) => {
                tracing::error!(
                    "error while confirming answer transaction to oracle {} - {}",
                    active_oracle.address.deref(),
                    error,
                );

                // we need to throw this error as this needs to be addressed immediately.
                // not being able to delete the tx hash for an oracle once an answer task
                // errors out might cause a deadlock preventing any answering task from
                // starting in the future
                active_oracle.delete_answer_tx_hash(&mut db_connection)?;

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
                        tracing::error!("could not format units for raw fee {} - {:#}", fee, error);
                        return Ok(());
                    }
                };
                tracing::info!(
                    "paid {} to answer oracle {}",
                    formatted,
                    active_oracle.address.deref()
                );
            }
        } else {
            tracing::warn!(
                "could not determine paid amount to answer oracle {}",
                active_oracle.address.deref()
            );
        }

        let active_oracle_address = active_oracle.address.0.clone();
        if let Err(error) = active_oracle.delete(&mut db_connection) {
            tracing::error!("could not delete oracle from database - {}", error);
            return Ok(());
        }

        tracing::info!(
            "oracle {} successfully finalized with value {}",
            active_oracle_address.encode_hex(),
            answer
        );
    }

    Ok(())
}
