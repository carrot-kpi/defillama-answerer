use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context};
use backoff::{future::retry, ExponentialBackoffBuilder};
use reqwest::{Body, Method};
use serde::Deserialize;

use crate::{http_client::HttpClient, specification::Specification};

pub async fn fetch_specification_with_retry(
    ipfs_http_client: Arc<HttpClient>,
    cid: &String,
) -> anyhow::Result<Specification> {
    let fetch = || async {
        let response = match ipfs_http_client
            .request(Method::POST, format!("/api/v0/cat?arg={cid}"))
            .await?
            .send()
            .await
            .context(format!("could not fetch cid {cid}"))
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("{:#}", err);
                return Err(backoff::Error::Transient {
                    err,
                    retry_after: None,
                });
            }
        };

        match response.json::<Specification>().await.context(format!(
            "could not deserialize raw response to json spec for {cid}"
        )) {
            Ok(specification) => Ok(specification),
            Err(err) => {
                tracing::error!("{:#}", err);
                // if we can't parse the json to text, we just
                // stop retrying
                Err(backoff::Error::Permanent(err))
            }
        }
    };

    retry(
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(
                // retry for 10 minutes
                Duration::from_secs(6_000),
            ))
            .build(),
        fetch,
    )
    .await
    .context(format!("could not fetch cid {} from ipfs", cid))
}

#[derive(Deserialize)]
struct CARUploadResponse {
    cid: String,
}

pub async fn pin_on_web3_storage_with_retry(
    ipfs_http_client: Arc<HttpClient>,
    web3_storage_http_client: Arc<HttpClient>,
    cid: String,
) -> anyhow::Result<()> {
    let pin = || async {
        // export dag from ipfs
        let car_response = match ipfs_http_client
            .request(
                Method::POST,
                format!("/api/v0/dag/export?arg={cid}&progress=false"),
            )
            .await?
            .send()
            .await
            .context(format!("could not get car for {cid}"))
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("{}", err);
                return Err(backoff::Error::Transient {
                    err,
                    retry_after: None,
                });
            }
        };

        // upload car to web3.storage
        let car_upload_response = match web3_storage_http_client
            .request(Method::POST, "/car")
            .await?
            .body(Body::wrap_stream(car_response.bytes_stream()))
            .send()
            .await
            .context(format!("could not pin {cid} to web3.storage"))
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("{}", err);
                return Err(backoff::Error::Transient {
                    err,
                    retry_after: None,
                });
            }
        };

        let car_upload_response = match car_upload_response
            .json::<CARUploadResponse>()
            .await
            .context(format!(
                "could not convert web3.storage response for {cid} to json"
            )) {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("{}", err);
                return Err(backoff::Error::Transient {
                    err,
                    retry_after: None,
                });
            }
        };

        if car_upload_response.cid != *cid {
            return Err(backoff::Error::Permanent(anyhow!(
                "cid mismatch between local pin and web3.storage: got {}, expected {}",
                car_upload_response.cid,
                cid
            )));
        }

        tracing::info!("specification with cid {} backed up on web3.storage", cid);

        Ok(())
    };

    retry(
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(86_400)))
            .build(),
        pin,
    )
    .await
    .context(format!("could not back up {} on web3.storage", cid))
}
