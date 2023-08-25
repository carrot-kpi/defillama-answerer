pub mod handlers;

use std::fmt::Debug;

use async_trait::async_trait;
use diesel::{sql_types::Jsonb, AsExpression, FromSqlRow};
use ethers::types::U256;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::specification::handlers::tvl::TvlHandler;

use self::handlers::tvl::TvlPayload;

#[derive(FromSqlRow, AsExpression, Serialize, Deserialize, Debug, PartialEq, ToSchema)]
#[serde(tag = "metric", content = "payload")]
#[serde(rename_all = "camelCase")]
#[diesel(sql_type = Jsonb)]
pub enum Specification {
    Tvl(TvlPayload),
}

#[async_trait]
pub trait Validate<'a, P: Serialize + Deserialize<'a> + Debug + PartialEq> {
    async fn validate(payload: &P) -> anyhow::Result<bool>;
}

#[async_trait]
pub trait Answer<'a, P: Serialize + Deserialize<'a> + Debug + PartialEq> {
    async fn answer(payload: &P) -> anyhow::Result<Option<U256>>;
}

macro_rules! impl_spec_validation_and_handling {
    ($($spec_variant: ident => $handler: ident),*) => {
    pub async fn validate<'a>(specification: &Specification) -> bool {
            let result = match specification {
                $(Specification::$spec_variant(payload) => $handler::validate(&payload),)*
            }.await;
            match result {
                Ok(val) => val,
                Err(error) => {
                    tracing::error!("validation failed for specification - {}", error);
                    return false;
                }
            }
        }

        pub async fn answer<'a>(specification: &Specification) -> Option<U256> {
            let result = match specification {
                $(Specification::$spec_variant(payload) => $handler::answer(&payload),)*
            }.await;
            match result {
                Ok(val) => val,
                Err(error) => {
                    tracing::error!("answering failed for specification - {}", error);
                    return None;
                }
            }
        }
    };
}

impl_spec_validation_and_handling!(
    Tvl => TvlHandler
);

#[cfg(test)]
mod test {
    use serde_json::error::Category;

    use crate::specification::handlers::tvl::TvlPayload;

    use super::Specification;

    #[test]
    fn serialize_tvl() {
        let metric = Specification::Tvl(TvlPayload {
            protocol: "aave".to_owned(),
            timestamp: 10,
        });

        assert_eq!(
            serde_json::to_string(&metric).unwrap(),
            r#"{"metric":"tvl","payload":{"protocol":"aave","timestamp":10}}"#
        );
    }

    #[test]
    fn deserialize_tvl() {
        // just gibberish
        assert!(serde_json::from_str::<Specification>("foo").is_err());

        // malformed json
        assert_eq!(
            serde_json::from_str::<Specification>(r#"{"foo:"bar"}"#)
                .unwrap_err()
                .classify(),
            Category::Syntax
        );

        // valid json, no metric
        assert_eq!(
            serde_json::from_str::<Specification>(r#"{"foo":"bar","payload":{"bar":"foo"}}"#)
                .unwrap_err()
                .classify(),
            Category::Data
        );

        // valid json, invalid metric
        assert_eq!(
            serde_json::from_str::<Specification>(r#"{"metric":"foo","payload":{"bar":"foo"}}"#)
                .unwrap_err()
                .classify(),
            Category::Data
        );

        // valid json, valid metric, no payload
        assert_eq!(
            serde_json::from_str::<Specification>(r#"{"metric":"tvl"}"#)
                .unwrap_err()
                .classify(),
            Category::Data
        );

        // valid json, valid metric, invalid payload
        assert_eq!(
            serde_json::from_str::<Specification>(r#"{"metric":"tvl","payload":"foo"}"#)
                .unwrap_err()
                .classify(),
            Category::Data
        );

        // valid json, valid metric, valid payload
        assert_eq!(
            serde_json::from_str::<Specification>(
                r#"{"metric":"tvl","payload":{"protocol":"foo","timestamp":10}}"#,
            )
            .unwrap(),
            Specification::Tvl(TvlPayload {
                protocol: "foo".to_owned(),
                timestamp: 10
            })
        );
    }
}
