use std::sync::Arc;

use distributed::{
    microsvc::{Routes, Service, ServiceDescriptor},
    AggregateRepository, SqliteRepository,
};

use crate::handlers;

pub type ServiceRepo = SqliteRepository;

pub fn build(repo: ServiceRepo) -> Arc<Service> {
    Arc::new(build_service(repo))
}

fn build_service(repo: ServiceRepo) -> Service {
    // Under `distributed dev`, only the active generation may write.
    let service = Service::new()
        .named("zero-config-app")
        .with_lifecycle(distributed::microsvc::LifecycleGate::from_env());
    let service = service.routes(
        Routes::new()
            .with_repo(AggregateRepository::<_, crate::models::CommandAggregate>::new(repo.clone()))
            .typed_command(handlers::zero_config_app_create::command())
            .handle(handlers::zero_config_app_create::handle),
    );
    service
}

/// Build the service with GraphQL mounted at `POST /graphql`.
///
/// Reads `DATABASE_URL` (defaults to "sqlite::memory:" for local dev).
pub async fn build_with_graphql() -> Result<Arc<Service>, Box<dyn std::error::Error + Send + Sync>>
{
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    let repo = ServiceRepo::connect_and_migrate(&database_url).await?;
    let service = build_service(repo.clone());
    let protocol_token_key = graphql_protocol_token_key()?;
    let engine = crate::query::build_engine(&repo, &service, protocol_token_key)?;
    Ok(Arc::new(service.try_with_graphql(engine)?))
}

fn graphql_protocol_token_key() -> Result<[u8; 32], Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{Error, ErrorKind};

    let secret = std::env::var("DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY").map_err(|_| {
        Error::new(
            ErrorKind::NotFound,
            "DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY must be a stable 32-byte deployment secret",
        )
    })?;
    let bytes = secret.into_bytes();
    let key: [u8; 32] = bytes.try_into().map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY must be exactly 32 UTF-8 bytes",
        )
    })?;
    if key.iter().all(|byte| *byte == 0) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY must not be all zero",
        )
        .into());
    }
    Ok(key)
}

pub fn descriptor() -> ServiceDescriptor {
    ServiceDescriptor::new("zero-config-app")
        .command(handlers::zero_config_app_create::COMMAND)
        .transport("http")
}
