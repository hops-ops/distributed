use distributed::microsvc::{Context, HandlerError};
use serde_json::{json, Value};

use crate::distributed_read_model::checkout::{
    checkout_event, json_outbox_event, seat_event, CheckoutStarted, SeatReserved, SEAT_AVAILABLE,
    SEAT_RESERVED,
};
use crate::distributed_read_model::seat_inventory_service::SeatRepo;

pub const EVENT: &str = checkout_event::STARTED;

pub fn guard(ctx: &Context<SeatRepo>) -> bool {
    ctx.has_fields(&["checkout_id", "seat_id", "seat_category"])
}

pub async fn handle(ctx: &Context<'_, SeatRepo>) -> Result<Value, HandlerError> {
    let msg = ctx.input::<CheckoutStarted>()?;
    let mut seat = ctx
        .repo()
        .get(&msg.seat_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(msg.seat_id.clone()))?;

    if seat.checkout_id == msg.checkout_id && seat.status == SEAT_RESERVED {
        return Ok(json!({ "seat_id": msg.seat_id }));
    }

    if seat.status != SEAT_AVAILABLE {
        return Err(HandlerError::Rejected(format!(
            "seat {} is not available",
            msg.seat_id
        )));
    }

    if seat.category != msg.seat_category {
        return Err(HandlerError::Rejected(format!(
            "seat {} is not in category {}",
            msg.seat_id, msg.seat_category
        )));
    }

    seat.reserve(msg.checkout_id.clone())?;
    let event = SeatReserved {
        checkout_id: msg.checkout_id.clone(),
        seat_id: msg.seat_id.clone(),
        seat_category: msg.seat_category.clone(),
    };
    let out = json_outbox_event(&msg.checkout_id, seat_event::RESERVED, &event)?;
    ctx.repo().outbox(out).commit(&mut seat).await?;

    Ok(json!({ "seat_id": msg.seat_id }))
}
