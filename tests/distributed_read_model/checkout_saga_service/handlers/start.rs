use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::checkout::{
    checkout_command, checkout_event, json_outbox_event, CheckoutStarted, StartCheckout,
};
use crate::checkout_saga_service::CheckoutSaga;

pub const COMMAND: &str = checkout_command::START;

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub checkout_id: String,
}

pub fn command() -> TypedCommand<StartCheckout, Succeeded<Output>> {
    typed_command::<StartCheckout, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, CheckoutSaga>,
    msg: StartCheckout,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut saga = ctx.repo().create();
    saga.start(
        msg.checkout_id.clone(),
        msg.seat_id.clone(),
        msg.seat_category.clone(),
    )?;

    let event = CheckoutStarted {
        checkout_id: msg.checkout_id.clone(),
        seat_id: msg.seat_id.clone(),
        seat_category: msg.seat_category.clone(),
    };
    let out = json_outbox_event(&msg.checkout_id, checkout_event::STARTED, &event)?;
    ctx.repo().outbox(out).commit(saga)?.succeeded(Output {
        checkout_id: msg.checkout_id,
    })
}
