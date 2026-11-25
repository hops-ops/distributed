use distributed::microsvc::{Context, HandlerError};
use distributed::ReadModelWorkspaceExt;
use serde_json::{json, Value};

use crate::distributed_read_model::checkout::{
    seat_event, SeatAdded, SeatReserved, SEAT_AVAILABLE, SEAT_RESERVED,
};
use crate::distributed_read_model::projection_service::ProjectionDependencies;
use crate::distributed_read_model::read_models::{CheckoutStepView, SeatView};

pub const EVENTS: &[&str] = &[seat_event::ADDED, seat_event::RESERVED];

pub fn guard(ctx: &Context<ProjectionDependencies>) -> bool {
    ctx.message().id().is_some()
}

pub async fn handle(ctx: &Context<'_, ProjectionDependencies>) -> Result<Value, HandlerError> {
    match ctx.message().name() {
        seat_event::ADDED => {
            let msg: SeatAdded = serde_json::from_slice(ctx.message().payload())
                .map_err(|err| HandlerError::DecodeFailed(format!("seat added: {err}")))?;
            let row = SeatView {
                seat_id: msg.seat_id,
                category: msg.category,
                status: SEAT_AVAILABLE.to_string(),
                checkout_id: String::new(),
            };

            let mut workspace = ctx.read_model_store().workspace();
            workspace.upsert(&row).map_err(super::read_model_error)?;
            workspace.commit().await.map_err(super::read_model_error)?;
        }
        seat_event::RESERVED => {
            let msg: SeatReserved = serde_json::from_slice(ctx.message().payload())
                .map_err(|err| HandlerError::DecodeFailed(format!("seat reserved: {err}")))?;
            let seat = SeatView {
                seat_id: msg.seat_id.clone(),
                category: msg.seat_category,
                status: SEAT_RESERVED.to_string(),
                checkout_id: msg.checkout_id.clone(),
            };
            let step = CheckoutStepView {
                checkout_id: msg.checkout_id,
                step: "seat_reserved".to_string(),
                detail: "seat reserved".to_string(),
            };

            let mut workspace = ctx.read_model_store().workspace();
            workspace.upsert(&seat).map_err(super::read_model_error)?;
            workspace.upsert(&step).map_err(super::read_model_error)?;
            workspace.commit().await.map_err(super::read_model_error)?;
        }
        other => return Err(HandlerError::UnknownCommand(other.to_string())),
    }

    Ok(json!({ "event_id": ctx.message().id() }))
}
