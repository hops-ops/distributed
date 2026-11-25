use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::distributed_read_model_board::board_service::handlers::board_state_message;
use crate::distributed_read_model_board::board_service::{Board, OpenBoard};

pub const COMMAND: &str = "board.open";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub id: String,
}

pub fn command() -> TypedCommand<OpenBoard, Succeeded<Output>> {
    typed_command::<OpenBoard, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Board>,
    input: OpenBoard,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    if ctx.repo().get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "board {} already exists",
            input.id
        )));
    }

    let mut board = ctx.repo().create();
    board.open(input.id.clone(), input.name.clone())?;

    let outbox = board_state_message(&board, "board.opened")?;
    ctx.repo()
        .outbox(outbox)
        .commit(board)?
        .succeeded(Output { id: input.id })
}
