//! Outbox - Atomic commit of aggregates with publishable outbox messages.
//!
//! This module provides the outbox message type and commit helpers:
//! - `OutboxMessage` - publishable message envelope plus delivery state
//! - `OutboxMessageStatus` - Message status (Pending, InFlight, Failed); a
//!   published message is deleted
//! - `AggregateCommit` - Helper for aggregate + outbox commits
//!
//! Outbox messages are durable publication work items. Their payload can be a
//! domain event, integration event, command, or generic transport message.
//! Aggregate `EventRecord`s are replayable model history and are not
//! automatically published as domain events.
//!
//! ## Separation of Concerns
//!
//! The outbox pattern has two distinct phases:
//! 1. **Commit phase** (this module) - Atomically commit aggregate event records + outbox message
//! 2. **Worker phase** (see `outbox_worker` module) - Drain outbox and publish messages to external systems
//!
//! Outbox messages are explicit publication records. Aggregate event records are
//! replayable write-side history; they do not automatically become domain or
//! integration events until application code creates an `OutboxMessage` for that
//! publication.
//!
//! ## Example
//!
//! ```ignore
//! use distributed::outbox::OutboxMessage;
//!
//! // Create aggregate and domain event outbox message
//! let mut order = Order::new();
//! order.create("order-1", ...);
//!
//! let outbox = OutboxMessage::create("order-1:created", "OrderCreated", payload);
//!
//! // Commit in one async repository batch
//! repo.outbox(outbox).commit(&mut order).await?;
//! ```

mod commit;
mod message;

// Outbox message record
pub(crate) use message::PreparedDomainEvent;
pub use message::{OutboxMessage, OutboxMessageStatus};

// Commit helpers
#[allow(unused_imports)] // used by graphql causal commit
pub use commit::{AggregateCommit, CommitReceipt, OutboxPublisherConfig};
