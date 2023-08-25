use std::sync::Arc;

use anyhow::{anyhow, Context};
use ethers::{
    middleware::SignerMiddleware,
    providers::{Middleware, Provider, Ws},
    signers::LocalWallet,
};

pub type Signer = SignerMiddleware<Provider<Ws>, LocalWallet>;

pub async fn get_signer(
    url: Arc<String>,
    answerer_private_key: Arc<String>,
    expected_chain_id: u64,
) -> anyhow::Result<Arc<Signer>> {
    let answerer_wallet = answerer_private_key
        .parse::<LocalWallet>()
        .context("could not parse private key to local wallet")?;

    let provider = Provider::<Ws>::connect_with_reconnects((*url).clone(), usize::MAX)
        .await
        .context(format!(
            "could not get ws provider for chain {expected_chain_id}"
        ))?;

    let chain_id_from_provider = provider.get_chainid().await.context(format!(
        "could not get chain id from provider for chain {expected_chain_id}"
    ))?;

    if chain_id_from_provider.as_u64() != expected_chain_id {
        Err(anyhow!("chain id mismatch, provider gave {chain_id_from_provider} while {expected_chain_id} was expected"))
    } else {
        Ok(Arc::new(SignerMiddleware::new(provider, answerer_wallet)))
    }
}
