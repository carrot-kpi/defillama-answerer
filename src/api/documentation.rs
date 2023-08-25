use std::sync::Arc;

use utoipa::OpenApi;
use utoipa_swagger_ui::Config;
use warp::{
    http,
    hyper::{Response, StatusCode},
    path::{FullPath, Tail},
    redirect, Filter, Rejection, Reply,
};

use super::{specification, specifications};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "DefiLlama answerer",
        description = "DefiLlama answerer API",
        contact(name = "Carrot Labs", email = "tech@carrot-labs.xyz",)
    ),
    paths(specifications::validate_specification),
    components(schemas(specification::Specification, specification::handlers::tvl::TvlPayload))
)]
struct ApiDoc;

pub fn handlers() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let swagger_json = warp::path("swagger.json")
        .and(warp::get())
        .map(|| warp::reply::json(&ApiDoc::openapi()));

    let config = Arc::new(Config::from("/swagger.json"));

    let swagger_ui = warp::path("documentation")
        .and(warp::get())
        .and(warp::path::full())
        .and(warp::path::tail())
        .and(warp::any().map(move || config.clone()))
        .and_then(serve_swagger);

    swagger_json.or(swagger_ui)
}

async fn serve_swagger(
    full_path: FullPath,
    tail: Tail,
    config: Arc<Config<'static>>,
) -> Result<Box<dyn Reply + 'static>, Rejection> {
    if full_path.as_str() == "/documentation" {
        return Ok(Box::new(redirect::found(http::Uri::from_static(
            "/documentation/",
        ))));
    }

    let path = tail.as_str();
    match utoipa_swagger_ui::serve(path, config) {
        Ok(file) => {
            if let Some(file) = file {
                Ok(Box::new(
                    Response::builder()
                        .header("Content-Type", file.content_type)
                        .body(file.bytes),
                ))
            } else {
                Ok(Box::new(StatusCode::NOT_FOUND))
            }
        }
        Err(error) => Ok(Box::new(
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(error.to_string()),
        )),
    }
}
