use std::{sync::Arc, time::Duration};

use anyhow::Context;
use backoff::{future::retry, ExponentialBackoffBuilder};
use reqwest::Method;

use crate::{http_client::HttpClient, specification::Specification};

// #[derive(Deserialize)]
// struct CARUploadResponse {
//     cid: String,
// }

pub async fn fetch_specification_with_retry(
    ipfs_http_client: Arc<HttpClient>,
    cid: &String,
) -> anyhow::Result<Specification> {
    let fetch = || async {
        let response = match ipfs_http_client
            .request(Method::GET, format!("/api/v0/get?arg={cid}"))?
            .send()
            .await
            .context(format!("could not fetch cid {cid}"))
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

        match response.json::<Specification>().await.context(format!(
            "could not deserialize raw response to json spec for {cid}"
        )) {
            Ok(spec) => Ok(spec),
            Err(err) => {
                tracing::error!("{}", err);
                // if we can't parse the raw spec to a valid spec, we just
                // stop retrying
                return Err(backoff::Error::Permanent(err));
            }
        }
    };

    retry(
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(
                // retry for 10 minutes
                Duration::from_secs(6000),
            ))
            .build(),
        fetch,
    )
    .await
    .context(format!("could not fetch cid {} from ipfs", cid))
}

pub async fn pin_cid_web3_storage_with_retry(
    ipfs_http_client: Arc<HttpClient>,
    web3_storage_http_client: Arc<HttpClient>,
    cid: &String,
) -> anyhow::Result<()> {
    // TODO: actually implement this. It coudl also be simplified by directly
    // pinning the raw specification content instead of fetching the CAR etc,
    // since we already have to fetch it anyway to process it

    // limit requests to web3.storage to a maximum of 2 per second
    // let rate_limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(2u32).unwrap()));

    // let pin = || async {
    //     // export dag from ipfs
    //     let car_response = match ipfs_http_client
    //         .request(
    //             Method::GET,
    //             format!("/api/v0/dag/export?arg={cid}&progress=false"),
    //         )?
    //         .send()
    //         .await
    //         .map_err(|e| anyhow!(e))
    //         .context(format!("could not get car for {cid}"))
    //     {
    //         Ok(res) => res,
    //         Err(err) => {
    //             tracing::error!("{}", err);
    //             return Err(backoff::Error::Transient {
    //                 err,
    //                 retry_after: None,
    //             });
    //         }
    //     };

    //     // apply rate limiting
    //     rate_limiter.until_ready().await;

    //     // upload car to web3.storage
    //     let car_upload_response = match web3_storage_http_client
    //         .request(Method::POST, "/car")?
    //         .body(Body::wrap_stream(car_response.bytes_stream()))
    //         .send()
    //         .await
    //         .map_err(|e| anyhow!(e))
    //         .context(format!("could not pin {cid} to web3.storage"))
    //     {
    //         Ok(res) => res,
    //         Err(err) => {
    //             tracing::error!("{}", err);
    //             return Err(backoff::Error::Transient {
    //                 err,
    //                 retry_after: None,
    //             });
    //         }
    //     };

    //     let car_upload_response = match car_upload_response
    //         .json::<CARUploadResponse>()
    //         .await
    //         .map_err(|e| anyhow!(e))
    //         .context(format!(
    //             "could not convert web3.storage response for {cid} to json"
    //         )) {
    //         Ok(res) => res,
    //         Err(err) => {
    //             tracing::error!("{}", err);
    //             return Err(backoff::Error::Transient {
    //                 err,
    //                 retry_after: None,
    //             });
    //         }
    //     };

    //     // check if cids are matching
    //     if &car_upload_response.cid != cid {
    //         return Err(backoff::Error::Transient {
    //             err: anyhow!("cid mismatch between local pin and web3.storage"),
    //             retry_after: None,
    //         });
    //     }

    //     Ok(())
    // };

    // match retry(
    //     ExponentialBackoffBuilder::new()
    //         .with_max_elapsed_time(Some(Duration::from_secs(86_400)))
    //         .build(),
    //     pin,
    // )
    // .await
    // {
    //     Ok(_) => tracing::info!("backed up {} on web3.storage", cid),
    //     Err(error) => tracing::error!("could back up {} on web3.storage - {}", cid, error),
    // }

    Ok(())
}
