//! Handler: whoami
//!
//! Returns the authenticated user's ID from the session. It declares no
//! role, so any caller may run it; the handler itself requires a user.

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::{Deserialize, Serialize};

use crate::microsvc::models::counter::Counter;

pub const COMMAND: &str = "session.identify";

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct Input {}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub user_id: String,
}

pub fn command() -> TypedCommand<Input, Succeeded<Output>> {
    typed_command::<Input, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Counter>,
    _input: Input,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let user_id = ctx.user_id()?.to_string();
    PreparedCommand::prepare(Output { user_id })
        .map_err(|error| HandlerError::Other(Box::new(error)))
}
