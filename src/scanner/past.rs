use std::{num::NonZeroU32, ops::Deref, sync::Arc};

use anyhow::Context;
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
    let signer = get_signer(
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
                        "could not convert checkpoint block number {} to usigned integer",
                        checkpoint.block_number
                    )
                    .as_str(),
                )
            });
    let mut from_block = checkpoint_block.unwrap_or(context.factory_config.deployment_block);
    let full_range = block_number - from_block;
    let chunk_size = context.logs_blocks_range.unwrap_or(DEFAULT_LOGS_CHUNK_SIZE);

    tracing::info!(
        "scanning {} past blocks, analyzing {} blocks at a time",
        block_number - from_block,
        chunk_size
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

        let logs = match signer.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(error) => {
                tracing::error!(
                    "error fetching logs from block {} to {}: {:#}",
                    from_block,
                    to_block,
                    error
                );
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

        tracing::info!(
            "{} -> {} - {} oracle creations detected - scanned {}% of past blocks",
            from_block,
            to_block,
            oracles_data.len(),
            ((to_block as f32 - context.factory_config.deployment_block as f32)
                / full_range as f32)
                * 100f32
        );

        acknowledge_active_oracles(
            context.chain_id,
            oracles_data,
            context.db_connection_pool.clone(),
            context.ipfs_http_client.clone(),
            context.defillama_client.clone(),
            context.web3_storage_http_client.clone(),
        )
        .await;

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
