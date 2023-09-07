use std::time::Duration;

use anyhow::Context;
use governor::{
    clock::{QuantaClock, QuantaInstant},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    RateLimiter,
};

const HTTP_REQUEST_TIMEOUT: u64 = 30_000;

pub struct HttpClient {
    inner: reqwest::Client,
    base_url: reqwest::Url,
    bearer_auth_token: Option<String>,
    rate_limiter:
        Option<RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>>,
}

impl HttpClient {
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::new()
    }

    pub async fn request<S: Into<String>>(
        &self,
        method: reqwest::Method,
        path: S,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let path_owned = path.into();
        let url = self.base_url.join(path_owned.as_ref()).context(format!(
            "could not join {} and {} in an url",
            self.base_url, path_owned,
        ))?;

        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.until_ready().await;
        }

        let builder = self.inner.request(method, url);
        Ok(match &self.bearer_auth_token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        })
    }
}

pub struct HttpClientBuilder {
    base_url: String,
    bearer_auth_token: Option<String>,
    rate_limiter:
        Option<RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>>,
}

impl HttpClientBuilder {
    fn base_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT))
            .build()
            .unwrap() // panic on error
    }

    pub fn new() -> Self {
        Self {
            base_url: "".to_owned(),
            bearer_auth_token: None,
            rate_limiter: None,
        }
    }

    pub fn build(self) -> anyhow::Result<HttpClient> {
        Ok(HttpClient {
            inner: Self::base_client(),
            base_url: reqwest::Url::parse(self.base_url.as_str())
                .context(format!("could not parse url {}", self.base_url))?,
            bearer_auth_token: self.bearer_auth_token,
            rate_limiter: self.rate_limiter,
        })
    }

    pub fn base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }
    
    pub fn bearer_auth_token(mut self, token: String) -> Self {
        self.bearer_auth_token = Some(token);
        self
    }

    pub fn rate_limiter(
        mut self,
        rate_limiter: RateLimiter<
            NotKeyed,
            InMemoryState,
            QuantaClock,
            NoOpMiddleware<QuantaInstant>,
        >,
    ) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }
}
