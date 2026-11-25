use distributed::{sourced, Entity};

use super::checkout::{CHECKOUT_SEAT_RESERVED_STATUS, CHECKOUT_STARTED_STATUS};

#[derive(Default)]
pub struct CheckoutSaga {
    pub entity: Entity,
    pub checkout_id: String,
    pub status: String,
    pub seat_id: String,
    pub seat_category: String,
    pub reserved_seat_id: String,
}

#[sourced(entity, aggregate_type = "conformance.checkout_saga")]
impl CheckoutSaga {
    #[event("started", when = !checkout_id.is_empty() && !seat_id.is_empty())]
    pub fn start(&mut self, checkout_id: String, seat_id: String, seat_category: String) {
        self.entity.set_id(&checkout_id);
        self.checkout_id = checkout_id;
        self.status = CHECKOUT_STARTED_STATUS.to_string();
        self.seat_id = seat_id;
        self.seat_category = seat_category;
    }

    #[event(
        "checkout_saga.seat_reservation_completed",
        when = self.status == CHECKOUT_STARTED_STATUS
            && self.entity.id() == checkout_id.as_str()
            && (self.seat_id.is_empty() || self.seat_id == seat_id)
            && (self.seat_category.is_empty() || self.seat_category == seat_category)
            && (self.reserved_seat_id.is_empty() || self.reserved_seat_id == seat_id)
    )]
    pub fn record_seat_reserved(
        &mut self,
        checkout_id: String,
        seat_id: String,
        seat_category: String,
    ) {
        self.checkout_id = checkout_id;
        self.seat_category = seat_category;
        self.status = CHECKOUT_SEAT_RESERVED_STATUS.to_string();
        self.reserved_seat_id = seat_id;
    }
}
