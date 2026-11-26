mod service;

pub mod handlers;
pub mod models;

pub use models::Seat;
pub use service::service;

use distributed::lock::InMemoryLockManager;
use distributed::{AggregateRepository, InMemoryRepository, QueuedRepository};

pub type SeatRepo =
    AggregateRepository<QueuedRepository<InMemoryRepository, InMemoryLockManager>, Seat>;
