use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,holograph_server=debug")),
        )
        .with_target(false)
        .compact()
        .init();

    holograph_server::core::core::run().await
}
