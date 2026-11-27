use distributed::{sourced, Entity};

use super::checkout::{SEAT_AVAILABLE_STATUS, SEAT_RESERVED_STATUS};

#[derive(Default)]
pub struct Seat {
    pub entity: Entity,
    pub category: String,
    pub status: String,
    pub checkout_id: String,
}

#[sourced(entity, aggregate_type = "conformance.seat")]
impl Seat {
    #[event("added", when = !seat_id.is_empty() && !category.is_empty())]
    pub fn add(&mut self, seat_id: String, category: String) {
        self.entity.set_id(&seat_id);
        self.category = category;
        self.status = SEAT_AVAILABLE_STATUS.to_string();
        self.checkout_id.clear();
    }

    #[event(
        "seat.reserved",
        when = self.status == SEAT_AVAILABLE_STATUS
            && !checkout_id.is_empty()
            && self.entity.id() == seat_id.as_str()
            && self.category == seat_category
    )]
    pub fn reserve(&mut self, checkout_id: String, seat_id: String, seat_category: String) {
        self.status = SEAT_RESERVED_STATUS.to_string();
        self.checkout_id = checkout_id;
    }
}
