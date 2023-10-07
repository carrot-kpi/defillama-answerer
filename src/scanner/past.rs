use std::{num::NonZeroU32, ops::Deref, sync::Arc, time::Duration};

use anyhow::{anyhow, Context};
use backoff::ExponentialBackoffBuilder;
use ethers::{contract::EthEvent, providers::Middleware, types::Filter};
use governor::{Quota, RateLimiter};
use tokio::sync::oneshot;

use crate::{
    commons::ChainExecutionContext,
    contracts::factory::CreateTokenFilter,
    db::models,
    scanner::commons::{acknowledge_active_oracles, parse_kpi_token_creation_logs},
    signer::get_signer,
};

// default blocks that are scanned per request
const DEFAULT_LOGS_CHUNK_SIZE: u64 = 10_000;

pub async fn scan<'a>(
    sender: oneshot::Sender<bool>,
    context: Arc<ChainExecutionContext>,
) -> anyhow::Result<()> {
    let mut signer = get_signer(
        context.ws_rpc_endpoint.clone(),
        context.answerer_private_key.clone(),
        context.chain_id,
    )
    .await?;

    let block_number = signer
        .get_block_number()
        .await
        .context("could not get current block number")?
        .as_u64();

    let mut db_connection = context
        .db_connection_pool
        .clone()
        .get()
        .context("could not get database connection")?;
    let checkpoint_block =
        models::Checkpoint::get_for_chain_id(&mut db_connection, context.chain_id)
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
            });
    drop(db_connection);

    let initial_block = checkpoint_block.unwrap_or(context.factory_config.deployment_block);
    let mut from_block = initial_block;
    let full_range = block_number - initial_block;
    let chunk_size = context.logs_blocks_range.unwrap_or(DEFAULT_LOGS_CHUNK_SIZE);

    tracing::info!(
        "scanning {} past blocks {} blocks at a time, starting from {}",
        full_range,
        chunk_size,
        block_number
    );

    // limit requests to infura to fetch past logs to a maximum of 2 per second
    let rate_limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(2u32).unwrap()));

    loop {
        let to_block = if from_block + chunk_size > block_number {
            block_number
        } else {
            from_block + chunk_size
        };

        let filter = Filter::new()
            .address(vec![context.factory_config.address])
            .event(CreateTokenFilter::abi_signature().deref())
            .from_block(from_block)
            .to_block(to_block);

        // apply rate limiting
        rate_limiter.until_ready().await;

        let fetch_logs = || async {
            signer
                .get_logs(&filter)
                .await
                .map_err(|err| backoff::Error::Transient {
                    err: anyhow!(err),
                    retry_after: None,
                })
        };

        let logs = match backoff::future::retry(
            ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(Some(Duration::from_secs(8)))
                .build(),
            fetch_logs,
        )
        .await
        {
            Ok(logs) => logs,
            Err(error) => {
                tracing::error!(
                    "error fetching logs from block {} to {}, forcing reconnection: {:#}",
                    from_block,
                    to_block,
                    error
                );
                signer = match get_signer(
                    context.ws_rpc_endpoint.clone(),
                    context.answerer_private_key.clone(),
                    context.chain_id,
                )
                .await
                {
                    Ok(signer) => signer,
                    Err(err) => {
                        tracing::error!("error while forcing signer reconnection: {:#}", err);
                        continue;
                    }
                };
                continue;
            }
        };

        let oracles_data = parse_kpi_token_creation_logs(
            context.chain_id,
            signer.clone(),
            logs,
            context.template_id,
        )
        .await;

        let oracles_data_len = oracles_data.len();
        if oracles_data_len > 0 {
            tracing::info!(
                "{} -> {} - {} oracle creations detected - scanned {}% of past blocks",
                from_block,
                to_block,
                oracles_data_len,
                ((to_block as f32 - initial_block as f32) / full_range as f32) * 100f32
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

        let database_connection = &mut context
            .db_connection_pool
            .get()
            .context("could not get new connection from pool")?;
        if let Err(error) =
            models::Checkpoint::update(database_connection, context.chain_id, to_block as i64)
        {
            tracing::error!("could not update snapshot block number: {:#}", error);
        }

        if to_block == block_number {
            break;
        }
        from_block = to_block + 1;
    }

    match sender.send(true) {
        Err(error) => {
            return Err(anyhow::anyhow!(
                "could not send checkpoint updates ownership message to present indexer - {:#}",
                error
            ))
        }
        _ => {}
    };

    tracing::info!(
        "finished scanning past blocks, checkpoint updates ownership transferred to present indexer"
    );

    Ok(())
}
