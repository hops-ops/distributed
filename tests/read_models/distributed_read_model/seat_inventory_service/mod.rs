mod service;

pub mod handlers;
pub mod models;

pub use models::Seat;
pub use service::service;

use distributed::{AggregateRepository, InMemoryLockManager, InMemoryRepository, QueuedRepository};

pub type SeatRepo =
    AggregateRepository<QueuedRepository<InMemoryRepository, InMemoryLockManager>, Seat>;
