mod documentation;
mod specifications;

use std::{net::Ipv4Addr, sync::Arc};

use warp::Filter;

use crate::{defillama::DefiLlamaClient, specification};

pub async fn serve(
    host: Ipv4Addr,
    port: u16,
    defillama_client: Arc<DefiLlamaClient>,
) -> anyhow::Result<()> {
    warp::serve(documentation::handlers().or(specifications::handlers(defillama_client)))
        .run((host, port))
        .await;

    Ok(())
}
