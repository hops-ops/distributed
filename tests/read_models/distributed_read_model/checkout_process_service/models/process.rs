use distributed::{sourced, Entity, Snapshot};

use crate::distributed_read_model::checkout::{CHECKOUT_SEAT_RESERVED, CHECKOUT_STARTED};

/// Checkout process aggregate, keyed by checkout id. It records checkout-process
/// facts and emits process events; it does not call the seat aggregate directly.
#[derive(Default, Snapshot)]
pub struct CheckoutProcess {
    pub entity: Entity,
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
    pub status: String,
    pub reserved_seat_id: String,
}

#[sourced(entity, aggregate_type = "checkout_process")]
impl CheckoutProcess {
    #[event("started", when = !checkout_id.is_empty() && !seat_id.is_empty())]
    pub fn start(&mut self, checkout_id: String, seat_id: String, seat_category: String) {
        self.entity.set_id(&checkout_id);
        self.checkout_id = checkout_id;
        self.seat_id = seat_id;
        self.seat_category = seat_category;
        self.status = CHECKOUT_STARTED.to_string();
    }

    #[event(
        "checkout_process.seat_reservation_completed",
        when = self.status == CHECKOUT_STARTED && self.seat_id == seat_id
    )]
    pub fn set_reserved_seat(&mut self, seat_id: String) {
        self.reserved_seat_id = seat_id;
        self.status = CHECKOUT_SEAT_RESERVED.to_string();
    }
}
