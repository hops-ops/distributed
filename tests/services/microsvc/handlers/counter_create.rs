//! Handler: counter.initialize
//!
//! Follows the microsvc handler convention:
//! - `COMMAND` — the command name this handler responds to
//! - `command()` — the typed declaration: input, outcome and roles
//! - `handle` — the command handler

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError, TRANSPORT_ROLE};
use serde::{Deserialize, Serialize};

use super::{counter_state_message, USER_ROLE};
use crate::microsvc::models::counter::Counter;

pub const COMMAND: &str = "counter.initialize";

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct Input {
    pub id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub id: String,
}

pub fn command() -> TypedCommand<Input, Succeeded<Output>> {
    typed_command::<Input, Succeeded<Output>>(COMMAND).roles([USER_ROLE, TRANSPORT_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Counter>,
    input: Input,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    if ctx.repo().get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "counter {} already exists",
            input.id
        )));
    }

    let mut counter = ctx.repo().create();
    counter.create(input.id.clone())?;

    let message = counter_state_message(&counter, "counter.initialized")?;

    ctx.repo()
        .outbox(message)
        .commit(counter)?
        .succeeded(Output { id: input.id })
}
