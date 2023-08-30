#[tokio::main]
async fn main() -> anyhow::Result<()> {
    defillama_answerer::main().await
}
