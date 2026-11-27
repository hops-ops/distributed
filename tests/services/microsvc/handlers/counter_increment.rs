//! Handler: counter.increment

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError, TRANSPORT_ROLE};
use serde::{Deserialize, Serialize};

use super::{counter_state_message, USER_ROLE};
use crate::microsvc::models::counter::Counter;

pub const COMMAND: &str = "counter.increment";

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct Input {
    pub id: String,
    pub amount: i64,
}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub id: String,
    pub value: i64,
}

pub fn command() -> TypedCommand<Input, Succeeded<Output>> {
    typed_command::<Input, Succeeded<Output>>(COMMAND).roles([USER_ROLE, TRANSPORT_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Counter>,
    input: Input,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut counter = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;

    counter.increment(input.amount)?;

    let message = counter_state_message(&counter, "counter.incremented")?;
    let value = counter.value;

    ctx.repo()
        .outbox(message)
        .commit(counter)?
        .succeeded(Output {
            id: input.id,
            value,
        })
}
