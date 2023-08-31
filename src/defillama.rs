use std::str::FromStr;

use anyhow::Context;
use reqwest::{Method, Url};
use rust_decimal::Decimal;

use crate::http_client::HttpClient;

// TODO: implement rate limiting
pub struct DefiLlamaClient {
    http_client: HttpClient,
}

impl DefiLlamaClient {
    pub fn new(base_url: Url) -> Self {
        Self {
            http_client: HttpClient::new(base_url),
        }
    }

    pub async fn get_current_tvl(&self, protocol: &String) -> anyhow::Result<Decimal> {
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
