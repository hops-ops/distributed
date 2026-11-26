mod service;

pub mod handlers;
pub mod models;

pub use models::CheckoutSaga;
pub use service::service;

#[cfg(feature = "grpc")]
pub use service::start_grpc_service;
#[cfg(feature = "http")]
pub use service::start_http_service;

use distributed::lock::InMemoryLockManager;
use distributed::{AggregateRepository, InMemoryRepository, QueuedRepository};

pub type CheckoutRepo =
    AggregateRepository<QueuedRepository<InMemoryRepository, InMemoryLockManager>, CheckoutSaga>;
