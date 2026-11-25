mod service;

pub mod handlers;
pub mod models;

pub use models::CheckoutSaga;
pub use service::service;

#[cfg(feature = "grpc")]
pub use service::start_grpc_service;
#[cfg(feature = "http")]
pub use service::start_http_service;

use distributed::{AggregateRepository, InMemoryLockManager, InMemoryRepository, QueuedRepository};

pub type CheckoutRepo =
    AggregateRepository<QueuedRepository<InMemoryRepository, InMemoryLockManager>, CheckoutSaga>;
