//! Message vocabulary shared by the seat checkout services.
//!
//! Commands are present-tense requests. Events are past-tense facts.

use distributed::microsvc::HandlerError;
use distributed::outbox::OutboxMessage;
use serde::{Deserialize, Serialize};

pub mod checkout_command {
    pub const START: &str = "checkout.start";
}

pub mod checkout_event {
    pub const STARTED: &str = "checkout_saga.started";
    pub const SEAT_RESERVATION_COMPLETED: &str = "checkout_saga.seat_reservation_completed";
}

pub mod seat_command {
    pub const ADD: &str = "seat.add";
}

pub mod seat_event {
    pub const ADDED: &str = "seat.added";
    pub const RESERVED: &str = "seat.reserved";
}

pub const SEAT_AVAILABLE: &str = "available";
pub const SEAT_RESERVED: &str = "reserved";
pub const CHECKOUT_STARTED: &str = "started";
pub const CHECKOUT_SEAT_RESERVED: &str = "seat_reserved";
pub const RESERVING_SEAT_MESSAGE: &str = "reserving seat...";
pub const SEAT_RESERVED_MESSAGE: &str = "your seat is reserved, enter your payment info";

#[derive(Clone, Debug, Serialize, Deserialize, distributed::command::CommandInput)]
pub struct StartCheckout {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, distributed::command::CommandInput)]
pub struct AddSeat {
    pub seat_id: String,
    pub category: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckoutStarted {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeatAdded {
    pub seat_id: String,
    pub category: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeatReserved {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeatReservationCompleted {
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
}

pub fn json_outbox_event<T>(
    entity_id: &str,
    event_type: &str,
    payload: &T,
) -> Result<OutboxMessage, HandlerError>
where
    T: Serialize,
{
    let bytes =
        serde_json::to_vec(payload).map_err(|err| HandlerError::DecodeFailed(err.to_string()))?;
    OutboxMessage::create(format!("{entity_id}:{event_type}"), event_type, bytes)
        .map_err(HandlerError::from)
}
