use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

use super::OrderItem;

/// Saga status - tracks the overall state machine
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SagaStatus {
    #[default]
    Started,
    InventoryReserved,
    PaymentProcessed,
    Completed,
    Compensating,
    Failed,
}

/// Tracks what compensating actions are needed
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CompensationState {
    pub inventory_reserved: bool,
    pub payment_processed: bool,
}

/// OrderFulfillmentSaga - an event-sourced aggregate that coordinates
/// the order fulfillment process across multiple aggregates.
///
/// Flow:
/// 1. Start saga with order details
/// 2. Reserve inventory for each item
/// 3. Process payment
/// 4. Mark order as completed
///
/// On failure at any step, compensate by:
/// - Releasing inventory reservations
/// - Refunding payment (if processed)
/// - Cancelling order
#[derive(Default)]
pub struct OrderFulfillmentSaga {
    pub entity: Entity,
    order_id: String,
    customer_id: String,
    items: Vec<OrderItem>,
    total_cents: u32,
    status: SagaStatus,
    compensation: CompensationState,
    failure_reason: Option<String>,
}

#[allow(dead_code)]
#[sourced(entity)]
impl OrderFulfillmentSaga {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    pub fn customer_id(&self) -> &str {
        &self.customer_id
    }

    pub fn items(&self) -> &[OrderItem] {
        &self.items
    }

    pub fn total_cents(&self) -> u32 {
        self.total_cents
    }

    pub fn status(&self) -> SagaStatus {
        self.status
    }

    pub fn compensation(&self) -> &CompensationState {
        &self.compensation
    }

    pub fn failure_reason(&self) -> Option<&str> {
        self.failure_reason.as_deref()
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.status, SagaStatus::Completed | SagaStatus::Failed)
    }

    pub fn needs_inventory_compensation(&self) -> bool {
        self.status == SagaStatus::Compensating && self.compensation.inventory_reserved
    }

    pub fn needs_payment_compensation(&self) -> bool {
        self.status == SagaStatus::Compensating && self.compensation.payment_processed
    }

    // === Saga Commands ===

    #[event("started")]
    pub fn start(
        &mut self,
        saga_id: String,
        order_id: String,
        customer_id: String,
        items: Vec<OrderItem>,
        total_cents: u32,
    ) {
        self.entity.set_id(&saga_id);
        self.order_id = order_id;
        self.customer_id = customer_id;
        self.items = items;
        self.total_cents = total_cents;
        self.status = SagaStatus::Started;
    }

    #[event("inventory_reservation_succeeded", when = self.status == SagaStatus::Started)]
    pub fn inventory_reserved(&mut self) {
        self.status = SagaStatus::InventoryReserved;
        self.compensation.inventory_reserved = true;
    }

    #[event("succeeded", when = self.status == SagaStatus::InventoryReserved)]
    pub fn payment_succeeded(&mut self) {
        self.status = SagaStatus::PaymentProcessed;
        self.compensation.payment_processed = true;
    }

    #[event("completed", when = self.status == SagaStatus::PaymentProcessed)]
    pub fn complete(&mut self) {
        self.status = SagaStatus::Completed;
    }

    // === Failure and Compensation ===

    #[event("step_failed", when = !self.is_complete())]
    pub fn step_failed(&mut self, step: String, reason: String) {
        self.status = SagaStatus::Compensating;
        self.failure_reason = Some(format!("{}: {}", step, reason));
    }

    #[event("inventory_compensated", when = self.needs_inventory_compensation())]
    pub fn inventory_compensated(&mut self) {
        self.compensation.inventory_reserved = false;
    }

    #[event("payment_compensated", when = self.needs_payment_compensation())]
    pub fn payment_compensated(&mut self) {
        self.compensation.payment_processed = false;
    }

    #[event("failed", when = self.status == SagaStatus::Compensating && !self.compensation.inventory_reserved && !self.compensation.payment_processed)]
    pub fn mark_failed(&mut self) {
        self.status = SagaStatus::Failed;
    }

    pub fn snapshot(&self) -> OrderFulfillmentSagaSnapshot {
        OrderFulfillmentSagaSnapshot {
            id: self.entity.id().to_string(),
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            items: self.items.clone(),
            total_cents: self.total_cents,
            status: self.status,
            compensation: self.compensation.clone(),
            failure_reason: self.failure_reason.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderFulfillmentSagaSnapshot {
    pub id: String,
    pub order_id: String,
    pub customer_id: String,
    pub items: Vec<OrderItem>,
    pub total_cents: u32,
    pub status: SagaStatus,
    pub compensation: CompensationState,
    pub failure_reason: Option<String>,
}
