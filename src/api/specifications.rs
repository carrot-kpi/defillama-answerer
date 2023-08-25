use std::convert::Infallible;

use serde_json::Value;
use warp::{body, http, path, post, reply, Filter, Rejection, Reply};

use crate::specification::{self, Specification};

pub fn handlers() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let validate = path("specifications")
        .and(path("validations"))
        .and(post())
        .and(path::end())
        .and(body::json())
        .and_then(validate_specification);

    validate
}

/// Validates specifications.
///
/// Validates a DefiLlama metric request based on the metrics and modifiers the service currently supports.
#[utoipa::path(
    post,
    path = "/specifications/validations",
    request_body = Specification,
    responses(
        (status = 204, description = "Validation was successful and the given specification conforms to a correct schema."),
        (status = 400, description = "Validation was unsuccessful and the given specification does not conform to any correct schema.")
    )
)]
pub async fn validate_specification(raw_specification: Value) -> Result<impl Reply, Infallible> {
    match serde_json::from_value::<Specification>(raw_specification) {
        Ok(specification) => Ok(reply::with_status(
            reply::reply(),
            if specification::validate(&specification).await {
                http::StatusCode::NO_CONTENT
            } else {
                http::StatusCode::BAD_REQUEST
            },
        )),
        Err(_) => {
            Ok(reply::with_status(
                reply::reply(),
                http::StatusCode::BAD_REQUEST,
            ))
        }
    }
}
