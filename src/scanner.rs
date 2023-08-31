mod commons;
mod past;
mod present;

use std::sync::Arc;

use anyhow::Context;
use tokio::{sync::oneshot, task::JoinSet};
use tracing_futures::Instrument;

use crate::commons::ChainExecutionContext;

pub async fn scan(context: Arc<ChainExecutionContext>) -> anyhow::Result<()> {
    let chain_id = context.chain_id;

    // When the past indexer is running it has control over the checkpoint block number update.
    // This control is passed over to the present indexer only when the past indexer is finished
    // in order to avoid creating holes in the indexing history, In short: the checkpoint block number
    // must always be the minimum block number possible between what the present and past indexers
    // are currently analyzing in order to avoid outcomes resulting in a wrong history when the
    // service is restarted.
    // This dynamic is handled with a oneshot channel. When the past indexer has finished processing
    // its share, it communicates it to the present indexer which can then take over the checkpoint
    // block number updates.
    let (tx, rx) = oneshot::channel();

    let present_indexing_future =
        present::scan(rx, context.clone()).instrument(tracing::info_span!("present", chain_id));

    let past_indexing_future =
        past::scan(tx, context.clone()).instrument(tracing::info_span!("past", chain_id));

    let mut join_set = JoinSet::new();
    join_set.spawn(past_indexing_future);
    join_set.spawn(present_indexing_future);

    // wait forever unless some task stops with an error
    while let Some(join_result) = join_set.join_next().await {
        let task_result = join_result.context("error while joining tasks")?;
        task_result.context("task unexpectedly stopped")?;
    }

    Ok(())
}
