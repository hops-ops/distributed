//! Saga service handlers — coordinates the order fulfillment flow.

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::lock::InMemoryLockManager;
use distributed::microsvc::{CausalCommandContext, Context, HandlerError};
use distributed::{AggregateRepository, InMemoryRepository, QueuedRepository};
use serde::Serialize;
use serde_json::{json, Value};

use super::messages::*;
use crate::sagas::order::OrderFulfillmentSaga;

pub type Repo = AggregateRepository<
    QueuedRepository<InMemoryRepository, InMemoryLockManager>,
    OrderFulfillmentSaga,
>;

pub mod on_inventory_reserved;
pub mod on_order_completed;
pub mod on_order_created;
pub mod on_payment_succeeded;
pub mod start;
