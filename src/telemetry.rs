use anyhow::Context;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub fn init() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .json()
        .with_span_list(true)
        .with_current_span(false)
        .with_target(false)
        .with_env_filter(
            EnvFilter::builder()
                .with_env_var("LOG_LEVEL")
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .context("could not get log level")?,
        )
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).context("tracing initialization failed")
}
