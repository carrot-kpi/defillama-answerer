mod documentation;
mod specifications;

use std::net::Ipv4Addr;

use warp::Filter;

use crate::specification;

pub async fn serve(host: Ipv4Addr, port: u16) -> anyhow::Result<()> {
    warp::serve(documentation::handlers().or(specifications::handlers()))
        .run((host, port))
        .await;

    Ok(())
}
