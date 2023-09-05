use std::{num::NonZeroU32, str::FromStr};

use anyhow::Context;
use governor::{
    clock::{QuantaClock, QuantaInstant},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use reqwest::{Method, Url};
use rust_decimal::Decimal;

use crate::http_client::HttpClient;

const MAX_CALLS_PER_SECOND: u32 = 10;

// TODO: implement rate limiting
pub struct DefiLlamaClient {
    http_client: HttpClient,
    rate_limiter: RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>,
}

impl DefiLlamaClient {
    pub fn new(base_url: Url) -> Self {
        Self {
            http_client: HttpClient::new(base_url),
            rate_limiter: RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(MAX_CALLS_PER_SECOND).unwrap(),
            )),
        }
    }

    pub async fn get_current_tvl(&self, protocol: &String) -> anyhow::Result<Decimal> {
        self.rate_limiter.until_ready().await;
        let raw = self
            .http_client
            .request(Method::GET, format!("/tvl/{protocol}"))?
            .send()
            .await
            .context(format!(
                "could not get current tvl for protocol {}",
                protocol
            ))?
            // FIXME: maybe handle this in a safer way to avoid conversion error if
            // dealing with extremely big numbers
            .text()
            .await
            .context(format!(
                "could not convert raw protocol tvl response to number for protocol {}",
                protocol
            ))?;
        Ok(Decimal::from_str(raw.as_str())
            .context(format!("could not convert {} to decimal", raw))?)
    }
}
