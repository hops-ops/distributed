//! Board write service: owns the `Board` aggregate, which holds its cards as
//! aggregate state, and publishes board snapshots through its outbox.

pub mod models;

mod handlers;
mod service;

use distributed::{AggregateRepository, InMemoryLockManager, InMemoryRepository, QueuedRepository};

pub use models::{AddCard, Board, BoardSnapshot, MoveCard, OpenBoard, RemoveCard};
pub use service::model_service;

pub type BoardRepo =
    AggregateRepository<QueuedRepository<InMemoryRepository, InMemoryLockManager>, Board>;
