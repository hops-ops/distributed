use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};

use crate::models::{CommandAggregate, CommandInput, CommandOutput};

pub const COMMAND: &str = "zero_config_app.create";
pub const MODEL: &str = "CommandAggregate";

pub fn command() -> TypedCommand<CommandInput, Succeeded<CommandOutput>> {
    typed_command::<CommandInput, Succeeded<CommandOutput>>(COMMAND).roles(["user"])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, CommandAggregate>,
    input: CommandInput,
) -> Result<PreparedCommand<Succeeded<CommandOutput>>, HandlerError> {
    let mut aggregate = match ctx.load(&input.id).await? {
        Some(aggregate) => aggregate,
        None => ctx.create(),
    };
    aggregate.record_command(COMMAND.to_string(), input.id.clone(), input.name.clone())?;
    ctx.commit(aggregate)?.succeeded(CommandOutput {
        command: COMMAND.to_string(),
        id: input.id,
        model: MODEL.to_string(),
        name: input.name,
    })
}
