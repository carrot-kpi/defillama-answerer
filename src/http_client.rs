use std::time::Duration;

use anyhow::Context;

const HTTP_REQUEST_TIMEOUT: u64 = 30_000;

pub struct HttpClient {
    inner: reqwest::Client,
    base: reqwest::Url,
    bearer_auth_token: Option<String>,
}

impl HttpClient {
    fn base_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT))
            .build()
            .unwrap() // panic on error
    }

    pub fn new(base: reqwest::Url) -> Self {
        Self {
            inner: Self::base_client(),
            base,
            bearer_auth_token: None,
        }
    }

    pub fn new_with_bearer_auth(base: reqwest::Url, token: String) -> Self {
        Self {
            inner: Self::base_client(),
            base: base,
            bearer_auth_token: Some(token),
        }
    }

    pub fn request<S: Into<String>>(
        &self,
        method: reqwest::Method,
        path: S,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let path_owned = path.into();
        let url = self.base.join(path_owned.as_ref()).context(format!(
            "could not join {} and {} in an url",
            self.base, path_owned,
        ))?;

        let builder = self.inner.request(method, url);
        Ok(match &self.bearer_auth_token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        })
    }
}
