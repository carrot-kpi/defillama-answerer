use std::sync::Arc;

use anyhow::{anyhow, Context};
use ethers::{
    middleware::{
        gas_escalator::{Frequency, GasEscalatorMiddleware, GeometricGasPrice},
        SignerMiddleware,
    },
    providers::{Middleware, Provider, Ws},
    signers::{LocalWallet, Signer as EthersSigner},
};

pub type Signer = GasEscalatorMiddleware<SignerMiddleware<Provider<Ws>, LocalWallet>>;

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
        // increase gas price 30% every 10 seconds
        // FIXME: maybe find a way to enforce a maximum price
        let escalator = GeometricGasPrice::new(1.3, 10u64, None::<u64>);
        let signer = GasEscalatorMiddleware::new(
            SignerMiddleware::new(provider, answerer_wallet.with_chain_id(expected_chain_id)),
            escalator,
            Frequency::PerBlock,
        );

        Ok(Arc::new(signer))
    }
}
