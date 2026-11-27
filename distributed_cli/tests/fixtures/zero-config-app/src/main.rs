#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let addr = std::env::var("BIND")
        .or_else(|_| std::env::var("BIND_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let service = zero_config_app::service::build_with_graphql().await?;
    let result = distributed::microsvc::serve(service, &addr).await;
    result?;
    Ok(())
}
