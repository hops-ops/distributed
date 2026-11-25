use distributed::microsvc::{Context, HandlerError};
use serde_json::{json, Value};

use crate::distributed_read_model::checkout::{
    checkout_event, json_outbox_event, seat_event, SeatReservationCompleted, SeatReserved,
    CHECKOUT_SEAT_RESERVED,
};
use crate::distributed_read_model::checkout_saga_service::CheckoutRepo;

pub const EVENT: &str = seat_event::RESERVED;

pub fn guard(ctx: &Context<CheckoutRepo>) -> bool {
    ctx.has_fields(&["checkout_id", "seat_id", "seat_category"])
}

pub async fn handle(ctx: &Context<'_, CheckoutRepo>) -> Result<Value, HandlerError> {
    let msg = ctx.input::<SeatReserved>()?;
    let mut saga = ctx
        .repo()
        .get(&msg.checkout_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(msg.checkout_id.clone()))?;

    if saga.status == CHECKOUT_SEAT_RESERVED {
        return Ok(json!({ "checkout_id": msg.checkout_id }));
    }

    saga.set_reserved_seat(msg.seat_id.clone())?;
    let event = SeatReservationCompleted {
        checkout_id: msg.checkout_id.clone(),
        seat_id: msg.seat_id.clone(),
        seat_category: msg.seat_category.clone(),
    };
    let out = json_outbox_event(
        &msg.checkout_id,
        checkout_event::SEAT_RESERVATION_COMPLETED,
        &event,
    )?;
    ctx.repo().outbox(out).commit(&mut saga).await?;

    Ok(json!({ "checkout_id": msg.checkout_id }))
}
