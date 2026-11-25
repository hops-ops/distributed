use distributed::{sourced, Entity, Snapshot};

use crate::distributed_read_model::checkout::{SEAT_AVAILABLE, SEAT_RESERVED};

/// Seat inventory aggregate, keyed by seat id. It owns seat availability and
/// reservation invariants.
#[derive(Default, Snapshot)]
pub struct Seat {
    pub entity: Entity,
    pub seat_id: String,
    pub category: String,
    pub status: String,
    pub checkout_id: String,
}

#[sourced(entity, aggregate_type = "seat")]
impl Seat {
    #[event("added", when = !seat_id.is_empty() && !category.is_empty())]
    pub fn add(&mut self, seat_id: String, category: String) {
        self.entity.set_id(&seat_id);
        self.seat_id = seat_id;
        self.category = category;
        self.status = SEAT_AVAILABLE.to_string();
        self.checkout_id.clear();
    }

    #[event(
        "seat.reserved",
        when = self.status == SEAT_AVAILABLE && !checkout_id.is_empty()
    )]
    pub fn reserve(&mut self, checkout_id: String) {
        self.status = SEAT_RESERVED.to_string();
        self.checkout_id = checkout_id;
    }
}
