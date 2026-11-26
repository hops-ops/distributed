use distributed::microsvc::{Context, HandlerError};
use distributed::read_model::ReadModelWorkspaceExt;
use serde_json::{json, Value};

use crate::distributed_read_model::checkout::{
    checkout_event, CheckoutStarted, SeatReservationCompleted, CHECKOUT_SEAT_RESERVED,
    CHECKOUT_STARTED, RESERVING_SEAT_MESSAGE, SEAT_RESERVED_MESSAGE,
};
use crate::distributed_read_model::projection_service::ProjectionDependencies;
use crate::distributed_read_model::read_models::{CheckoutStepView, CheckoutView};

pub const EVENTS: &[&str] = &[
    checkout_event::STARTED,
    checkout_event::SEAT_RESERVATION_COMPLETED,
];

pub fn guard(ctx: &Context<ProjectionDependencies>) -> bool {
    ctx.message().id().is_some()
}

pub async fn handle(ctx: &Context<'_, ProjectionDependencies>) -> Result<Value, HandlerError> {
    match ctx.message().name() {
        checkout_event::STARTED => {
            let msg: CheckoutStarted = serde_json::from_slice(ctx.message().payload())
                .map_err(|err| HandlerError::DecodeFailed(format!("checkout started: {err}")))?;
            let checkout = CheckoutView {
                checkout_id: msg.checkout_id.clone(),
                seat_id: msg.seat_id.clone(),
                seat_category: msg.seat_category,
                status: CHECKOUT_STARTED.to_string(),
                screen_message: RESERVING_SEAT_MESSAGE.to_string(),
                steps: Vec::new(),
                seat: None,
            };
            let step = checkout_step(&msg.checkout_id, "started", "checkout started");

            let mut workspace = ctx.read_model_store().workspace();
            workspace
                .upsert(&checkout)
                .map_err(super::read_model_error)?;
            workspace.upsert(&step).map_err(super::read_model_error)?;
            workspace.commit().await.map_err(super::read_model_error)?;
        }
        checkout_event::SEAT_RESERVATION_COMPLETED => {
            let msg: SeatReservationCompleted = serde_json::from_slice(ctx.message().payload())
                .map_err(|err| {
                    HandlerError::DecodeFailed(format!("seat reservation completed: {err}"))
                })?;
            let checkout = CheckoutView {
                checkout_id: msg.checkout_id.clone(),
                seat_id: msg.seat_id,
                seat_category: msg.seat_category,
                status: CHECKOUT_SEAT_RESERVED.to_string(),
                screen_message: SEAT_RESERVED_MESSAGE.to_string(),
                steps: Vec::new(),
                seat: None,
            };
            let step = checkout_step(
                &msg.checkout_id,
                "seat_reservation_completed",
                "seat reservation completed",
            );

            let mut workspace = ctx.read_model_store().workspace();
            workspace
                .upsert(&checkout)
                .map_err(super::read_model_error)?;
            workspace.upsert(&step).map_err(super::read_model_error)?;
            workspace.commit().await.map_err(super::read_model_error)?;
        }
        other => return Err(HandlerError::UnknownCommand(other.to_string())),
    }

    Ok(json!({ "event_id": ctx.message().id() }))
}

fn checkout_step(checkout_id: &str, step: &str, detail: &str) -> CheckoutStepView {
    CheckoutStepView {
        checkout_id: checkout_id.to_string(),
        step: step.to_string(),
        detail: detail.to_string(),
    }
}
