use std::{ops::Deref, sync::Arc};

use anyhow::Context;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use ethers::{
    contract::EthEvent,
    providers::{Middleware, StreamExt},
    types::{Block, Filter, H256},
};
use tokio::{
    sync::{oneshot, Mutex},
    task::JoinSet,
};
use tracing::error_span;
use tracing_futures::Instrument;

use crate::{
    commons::ChainExecutionContext,
    contracts::{defi_llama_oracle::DefiLlamaOracle, factory::CreateTokenFilter},
    db::models::{self, Snapshot},
    scanner::commons::{acknowledge_active_oracles, parse_kpi_token_creation_logs},
    signer::{get_signer, Signer},
    specification::{self},
};

pub async fn scan(
    receiver: oneshot::Receiver<bool>,
    context: Arc<ChainExecutionContext>,
) -> anyhow::Result<()> {
    let update_snapshot_block_number = Arc::new(Mutex::new(false));

    tokio::spawn(message_receiver(
        receiver,
        update_snapshot_block_number.clone(),
    ));

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
                tracing::error!("{}", error);
                continue;
            }
        };

        tracing::info!("watching blocks");

        while let Some(block) = stream.next().await {
            let block_number = block.number.unwrap();

            tracing::info!("handling block {}", block_number);

            handle_new_active_oracles(signer.clone(), block, context.clone()).await;
            handle_active_oracles_answering(signer.clone(), context.clone()).await;

            

            if *update_snapshot_block_number.lock().await {
                let database_connection = &mut context
                    .db_connection_pool
                    .get()
                    .context("could not get new connection from pool")?;
                if let Err(error) = Snapshot::update(
                    database_connection,
                    context.chain_id,
                    block_number.as_u32() as i64,
                ) {
                    tracing::error!("could not update snapshot block number - {}", error);
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
            tracing::error!("error while receiving control over snapshot block number update from past indexer - {}", error);
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
            "block {} - detected {} new active oracles",
            block_number,
            oracles_data.len()
        );
    }

    acknowledge_active_oracles(
        context.chain_id,
        oracles_data,
        context.db_connection_pool.clone(),
        context.ipfs_http_client.clone(),
        context.web3_storage_http_client.clone(),
    )
    .await;
}

async fn handle_active_oracles_answering(
    signer: Arc<Signer>,
    context: Arc<ChainExecutionContext>,
) -> () {
    let mut db_connection = match context.db_connection_pool.get() {
        Ok(connection) => connection,
        Err(error) => {
            tracing::error!("could not get new connection from pool - {}", error);
            return;
        }
    };

    let active_oracles =
        match models::ActiveOracle::get_all_for_chain_id(&mut db_connection, context.chain_id) {
            Ok(oracles) => oracles,
            Err(error) => {
                tracing::error!(
                    "could not get currently active oracles in chain with id {} - {}",
                    context.chain_id,
                    error
                );
                return;
            }
        };

    let mut join_set: JoinSet<Result<(), anyhow::Error>> = JoinSet::new();
    for active_oracle in active_oracles.into_iter() {
        let chain_id = context.chain_id;
        join_set.spawn(
            answer_active_oracle(
                signer.clone(),
                context.db_connection_pool.clone(),
                active_oracle,
            )
            .instrument(error_span!("answer", chain_id)),
        );
    }
}

async fn answer_active_oracle(
    signer: Arc<Signer>,
    db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    active_oracle: models::ActiveOracle,
) -> anyhow::Result<()> {
    // TODO: properly implement this
    if let Some(answer) = specification::answer(&active_oracle.specification).await {
        let oracle = DefiLlamaOracle::new(active_oracle.address.0, signer);
        let tx = oracle.finalize(answer);
        let pending_tx = tx.send().await?;
        let _mined_tx = pending_tx.await?;

        let database_connection = &mut db_connection_pool
            .get()
            .context("could not get new connection from pool")?;
        active_oracle.delete(database_connection)?;
    }
    Ok(())
}
