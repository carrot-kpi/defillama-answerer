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
        ipfs_http_client
            .request(Method::POST, format!("/api/v0/cat?arg={cid}"))
            .await
            .map_err(|err| {
                tracing::warn!(
                    "could not prepare request to fetch cid {}: {:#}",
                    cid,
                    err
                );
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?
            .send()
            .await
            .map_err(|err| {
                tracing::warn!("could not fetch cid {}: {:#}", cid, err);
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?
            .json::<Specification>()
            .await
            .map_err(|err| {
                tracing::error!(
                    "could not deserialize raw response to json spec for {}, exiting early: {:#}",
                    cid,
                    err
                );
                backoff::Error::Permanent(anyhow::anyhow!(err))
            })
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
        let car_response = ipfs_http_client
            .request(
                Method::POST,
                format!("/api/v0/dag/export?arg={cid}&progress=false"),
            )
            .await
            .map_err(|err| {
                tracing::warn!(
                    "could not prepare request to get car for {}: {:#}",
                    cid,
                    err
                );
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?
            .send()
            .await
            .map_err(|err| {
                tracing::warn!("could not get car for {}: {:#}", cid, err);
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?;

        // upload car to web3.storage
        let car_upload_response = web3_storage_http_client
            .request(Method::POST, "/car")
            .await.map_err(|err| {
                tracing::warn!(
                    "could not prepare request to upload car to web3.storage for {}: {:#}",
                    cid,
                    err
                );
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?
            .body(Body::wrap_stream(car_response.bytes_stream()))
            .send()
            .await.map_err(|err| {
                tracing::warn!(
                    "could not pin {} to web3.storage: {:#}",
                    cid,
                    err
                );
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?;

        let car_upload_response = car_upload_response
            .json::<CARUploadResponse>()
            .await
            .map_err(|err| {
                tracing::warn!(
                    "could not convert web3.storage response for {} to json: {:#}",
                    cid,
                    err
                );
                backoff::Error::Transient {
                    err: anyhow::anyhow!(err),
                    retry_after: None,
                }
            })?;

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
            .with_max_elapsed_time(Some(
                // retry for 10 minutes
                Duration::from_secs(6_000),
            ))
            .build(),
        pin,
    )
    .await
    .context(format!("could not back up {} on web3.storage", cid))
}
