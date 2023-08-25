use async_trait::async_trait;
use ethers::types::U256;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    commons::get_unix_timestamp,
    defillama::get_current_tvl,
    specification::{Answer, Validate},
};

#[derive(Serialize, Deserialize, Debug, PartialEq, ToSchema)]
pub struct TvlPayload {
    pub protocol: String,
    pub timestamp: u64,
}

pub struct TvlHandler;

#[async_trait]
impl<'a> Validate<'a, TvlPayload> for TvlHandler {
    async fn validate(payload: &TvlPayload) -> anyhow::Result<bool> {
        let current_timestamp = get_unix_timestamp()?;
        Ok(payload.timestamp > current_timestamp
            && get_current_tvl(&payload.protocol).await.is_ok())
    }
}

#[async_trait]
impl<'a> Answer<'a, TvlPayload> for TvlHandler {
    async fn answer(payload: &TvlPayload) -> anyhow::Result<Option<U256>> {
        let current_timestamp = get_unix_timestamp()?;
        if payload.timestamp > current_timestamp {
            return Ok(None);
        }

        let raw_tvl = get_current_tvl(&payload.protocol).await?;
        Ok(Some(U256::from(raw_tvl) * U256::from(10 ^ 18)))
    }
}
