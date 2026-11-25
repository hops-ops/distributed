use serde::{Deserialize, Serialize};

pub const CHECKOUT_STARTED_STATUS: &str = "started";
pub const CHECKOUT_SEAT_RESERVED_STATUS: &str = "seat_reserved";
pub const SEAT_AVAILABLE_STATUS: &str = "available";
pub const SEAT_RESERVED_STATUS: &str = "reserved";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatAdded {
    pub seat_id: String,
    pub category: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckoutStarted {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatReserved {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatReservationCompleted {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}
