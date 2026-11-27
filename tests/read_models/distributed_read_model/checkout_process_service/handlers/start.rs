use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::distributed_read_model::checkout::{
    checkout_command, checkout_event, json_outbox_event, CheckoutStarted, StartCheckout,
};
use crate::distributed_read_model::checkout_process_service::CheckoutProcess;

pub const COMMAND: &str = checkout_command::START;

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub checkout_id: String,
}

pub fn command() -> TypedCommand<StartCheckout, Succeeded<Output>> {
    typed_command::<StartCheckout, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, CheckoutProcess>,
    msg: StartCheckout,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut process = ctx.repo().create();
    process.start(
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
    ctx.repo().outbox(out).commit(process)?.succeeded(Output {
        checkout_id: msg.checkout_id,
    })
}
