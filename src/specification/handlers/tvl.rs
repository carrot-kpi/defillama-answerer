use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use ethers::types::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    commons::get_unix_timestamp,
    defillama::DefiLlamaClient,
    specification::{Answer, Validate},
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema)]
pub struct TvlPayload {
    pub protocol: String,
    pub timestamp: u64,
}

pub struct TvlHandler;

#[async_trait]
impl<'a> Validate<'a, TvlPayload> for TvlHandler {
    async fn validate(
        payload: &TvlPayload,
        defillama_client: Arc<DefiLlamaClient>,
    ) -> anyhow::Result<bool> {
        let current_timestamp = get_unix_timestamp()?;
        if payload.timestamp <= current_timestamp {
            return Ok(false);
        }

        match defillama_client.get_current_tvl(&payload.protocol).await {
            Ok(_) => Ok(true),
            Err(error) => {
                tracing::error!(
                    "error fetching tvl from defillama for protocol {} - {:#}",
                    payload.protocol,
                    error
                );
                Ok(false)
            }
        }
    }
}

#[async_trait]
impl<'a> Answer<'a, TvlPayload> for TvlHandler {
    async fn answer(
        payload: &TvlPayload,
        defillama_client: Arc<DefiLlamaClient>,
    ) -> anyhow::Result<Option<U256>> {
        let current_timestamp = get_unix_timestamp()?;
        if payload.timestamp > current_timestamp {
            return Ok(None);
        }

        let raw_tvl = defillama_client.get_current_tvl(&payload.protocol).await?;
        let scaled_tvl = raw_tvl
            .checked_mul(Decimal::new(1e18 as i64, 0))
            .context(format!(
                "could not correctly scale tvl value {} to 18 decimals",
                raw_tvl
            ))?;
        let converted: u128 = scaled_tvl.try_into().context(format!(
            "could not correctly truncate tvl value {} to u128",
            scaled_tvl
        ))?;
        Ok(Some(U256::from(converted)))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use ethers::types::U256;
    use reqwest::Url;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use crate::{
        defillama::DefiLlamaClient,
        specification::{handlers::tvl::TvlHandler, Answer},
    };

    use super::TvlPayload;

    #[tokio::test]
    async fn answer_active_oracle_not_ready() {
        let payload = TvlPayload {
            protocol: "foo".to_owned(),
            timestamp: u64::MAX,
        };

        let defillama_client = Arc::new(DefiLlamaClient::new(
            reqwest::Url::parse("http://foo.bar").unwrap(), // guaranteed to be a valid url
        ));

        assert!(TvlHandler::answer(&payload, defillama_client)
            .await
            .unwrap()
            .is_none())
    }

    #[tokio::test]
    async fn answer_active_oracle_get_current_tvl_fail() {
        let protocol = "foo".to_owned();
        let payload = TvlPayload {
            protocol: protocol.clone(),
            timestamp: 0,
        };

        let defillama_mock_server = MockServer::start().await;
        let defillama_client = Arc::new(DefiLlamaClient::new(
            defillama_mock_server.uri().parse::<Url>().unwrap(), // guaranteed to be a valid url
        ));
        Mock::given(method("GET"))
            .and(path(format!("/tvl/{protocol}")))
            .respond_with(ResponseTemplate::new(400))
            .mount(&defillama_mock_server)
            .await;

        assert!(TvlHandler::answer(&payload, defillama_client)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn answer_active_oracle_success() {
        let protocol = "foo".to_owned();
        let payload = TvlPayload {
            protocol: protocol.clone(),
            timestamp: 0,
        };

        let defillama_mock_server = MockServer::start().await;
        let defillama_client = Arc::new(DefiLlamaClient::new(
            defillama_mock_server.uri().parse::<Url>().unwrap(), // guaranteed to be a valid url
        ));
        Mock::given(method("GET"))
            .and(path(format!("/tvl/{protocol}")))
            .respond_with(ResponseTemplate::new(200).set_body_string("1234.5678"))
            .mount(&defillama_mock_server)
            .await;

        assert_eq!(
            TvlHandler::answer(&payload, defillama_client.clone())
                .await
                .unwrap(),
            Some(U256::from_dec_str("1234567800000000000000").unwrap())
        );

        defillama_mock_server.reset().await;

        // make sure that even in the very remote case the return value has more than 18
        // decimals everything works fine
        Mock::given(method("GET"))
            .and(path(format!("/tvl/{protocol}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("1234.56789101112131415161718192021222324252627"),
            )
            .mount(&defillama_mock_server)
            .await;

        assert_eq!(
            TvlHandler::answer(&payload, defillama_client)
                .await
                .unwrap(),
            Some(U256::from_dec_str("1234567891011121314151").unwrap())
        );
    }
}
