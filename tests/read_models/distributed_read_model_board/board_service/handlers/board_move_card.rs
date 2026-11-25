use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::distributed_read_model_board::board_service::handlers::board_state_message;
use crate::distributed_read_model_board::board_service::{Board, MoveCard};

pub const COMMAND: &str = "board.move_card";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub id: String,
    pub card_id: String,
    pub column: String,
}

pub fn command() -> TypedCommand<MoveCard, Succeeded<Output>> {
    typed_command::<MoveCard, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Board>,
    input: MoveCard,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut board = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    board.move_card(input.card_id.clone(), input.column.clone())?;

    let outbox = board_state_message(&board, "board.card_moved")?;
    ctx.repo().outbox(outbox).commit(board)?.succeeded(Output {
        id: input.id,
        card_id: input.card_id,
        column: input.column,
    })
}
