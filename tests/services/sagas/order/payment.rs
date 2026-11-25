use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

/// Payment status
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentStatus {
    #[default]
    Pending,
    Authorized,
    Captured,
    Failed,
    Refunded,
}

/// Payment aggregate - represents a payment attempt for an order
#[derive(Default)]
pub struct Payment {
    pub entity: Entity,
    order_id: String,
    amount_cents: u32,
    status: PaymentStatus,
    failure_reason: Option<String>,
    transaction_id: Option<String>,
}

#[allow(dead_code)]
#[sourced(entity)]
impl Payment {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    pub fn amount_cents(&self) -> u32 {
        self.amount_cents
    }

    pub fn status(&self) -> PaymentStatus {
        self.status
    }

    pub fn is_successful(&self) -> bool {
        matches!(
            self.status,
            PaymentStatus::Authorized | PaymentStatus::Captured
        )
    }

    pub fn failure_reason(&self) -> Option<&str> {
        self.failure_reason.as_deref()
    }

    #[event("initiated")]
    pub fn initiate(&mut self, id: String, order_id: String, amount_cents: u32) {
        self.entity.set_id(&id);
        self.order_id = order_id;
        self.amount_cents = amount_cents;
        self.status = PaymentStatus::Pending;
    }

    #[event("authorized", when = self.status == PaymentStatus::Pending)]
    pub fn authorize(&mut self, transaction_id: String) {
        self.status = PaymentStatus::Authorized;
        self.transaction_id = Some(transaction_id);
    }

    #[event("captured", when = self.status == PaymentStatus::Authorized)]
    pub fn capture(&mut self) {
        self.status = PaymentStatus::Captured;
    }

    #[event("failed", when = self.status == PaymentStatus::Pending || self.status == PaymentStatus::Authorized)]
    pub fn fail(&mut self, reason: String) {
        self.status = PaymentStatus::Failed;
        self.failure_reason = Some(reason);
    }

    #[event("refunded", when = self.status == PaymentStatus::Captured)]
    pub fn refund(&mut self) {
        self.status = PaymentStatus::Refunded;
    }

    pub fn snapshot(&self) -> PaymentSnapshot {
        PaymentSnapshot {
            id: self.entity.id().to_string(),
            order_id: self.order_id.clone(),
            amount_cents: self.amount_cents,
            status: self.status,
            failure_reason: self.failure_reason.clone(),
            transaction_id: self.transaction_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaymentSnapshot {
    pub id: String,
    pub order_id: String,
    pub amount_cents: u32,
    pub status: PaymentStatus,
    pub failure_reason: Option<String>,
    pub transaction_id: Option<String>,
}
