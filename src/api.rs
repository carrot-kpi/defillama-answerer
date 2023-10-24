mod documentation;
mod specifications;

use std::{net::Ipv4Addr, sync::Arc};

use carrot_commons::http_client::HttpClient;
use warp::Filter;

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
