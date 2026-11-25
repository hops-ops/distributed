//! Shared message types for inter-service communication.
//!
//! Each message is serialized as JSON via [`json_outbox_to`]: a command is
//! the receiving service's typed command input, an event is what the saga's
//! event handler decodes with `ctx.input`.

use distributed::microsvc::HandlerError;
use distributed::OutboxMessage;
use serde::{Deserialize, Serialize};

use crate::sagas::order::OrderItem;

/// Create a JSON-serialized outbox message routed to a destination queue.
///
/// `OutboxMessage::encode_to` uses bitcode internally, but the receiving
/// services decode the bus message payload as JSON. This helper bridges the
/// gap with `serde_json::to_vec` + `OutboxMessage::create_to`.
pub fn json_outbox_to<T: Serialize>(
    id: &str,
    event_type: &str,
    destination: &str,
    payload: &T,
) -> Result<OutboxMessage, HandlerError> {
    let bytes = serde_json::to_vec(payload).map_err(|e| HandlerError::Other(Box::new(e)))?;
    let message = OutboxMessage::create_to(id, event_type, destination, bytes)?;
    Ok(message)
}

// === Saga → Order Service ===

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct CreateOrderMsg {
    pub saga_id: String,
    pub order_id: String,
    pub customer_id: String,
    pub items: Vec<OrderItem>,
    pub total_cents: u32,
}

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct CompleteOrderMsg {
    pub saga_id: String,
    pub order_id: String,
}

// === Order Service → Saga ===

#[derive(Serialize, Deserialize)]
pub struct OrderInitializedMsg {
    pub saga_id: String,
    pub order_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct OrderCompletedMsg {
    pub saga_id: String,
    pub order_id: String,
}

// === Saga → Inventory Service ===

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct ReserveInventoryMsg {
    pub saga_id: String,
    pub order_id: String,
    pub sku: String,
    pub quantity: u32,
}

// === Inventory Service → Saga ===

#[derive(Serialize, Deserialize)]
pub struct InventoryReservedMsg {
    pub saga_id: String,
    pub order_id: String,
}

// === Saga → Payment Service ===

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct ProcessPaymentMsg {
    pub saga_id: String,
    pub order_id: String,
    pub amount_cents: u32,
}

// === Payment Service → Saga ===

#[derive(Serialize, Deserialize)]
pub struct PaymentSucceededMsg {
    pub saga_id: String,
    pub order_id: String,
}

// === Command Inputs ===

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct StartSagaInput {
    pub saga_id: String,
    pub order_id: String,
    pub customer_id: String,
    pub items: Vec<OrderItem>,
    pub total_cents: u32,
}

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct InitInventoryInput {
    pub sku: String,
    pub stock: u32,
}
