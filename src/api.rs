mod documentation;
mod specifications;

use std::{net::Ipv4Addr, sync::Arc};

use warp::Filter;

use crate::http_client::HttpClient;

pub async fn serve(
    host: Ipv4Addr,
    port: u16,
    defillama_http_client: Arc<HttpClient>,
) -> anyhow::Result<()> {
    warp::serve(documentation::handlers().or(specifications::handlers(defillama_http_client)))
        .run((host, port))
        .await;

    Ok(())
}
