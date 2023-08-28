use std::fmt::Display;

use anyhow::Context;

fn get_api_endpoint<T: AsRef<str> + Display>(path_and_maybe_query: T) -> String {
    format!("https://api.llama.fi{path_and_maybe_query}")
}

pub async fn get_current_tvl(protocol: &String) -> anyhow::Result<f64> {
    Ok(reqwest::get(get_api_endpoint(format!("/tvl/{protocol}")))
        .await
        .context(format!(
            "could not get currentl tvl for protocol {}",
            protocol
        ))?
        // FIXME: maybe handle this in a safer way to avoid conversion error if
        // dealing with extremely big numbers
        .json::<f64>()
        .await
        .context(format!(
            "could not convert raw protocol tvl response to number for protocol {}",
            protocol
        ))?)
}
