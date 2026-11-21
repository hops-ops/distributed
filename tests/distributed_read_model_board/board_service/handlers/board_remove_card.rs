use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::board_service::handlers::board_state_message;
use crate::board_service::{Board, RemoveCard};

pub const COMMAND: &str = "board.remove_card";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub id: String,
    pub card_id: String,
}

pub fn command() -> TypedCommand<RemoveCard, Succeeded<Output>> {
    typed_command::<RemoveCard, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Board>,
    input: RemoveCard,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut board = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    board.remove_card(input.card_id.clone())?;

    let outbox = board_state_message(&board, "board.card_removed")?;
    ctx.repo().outbox(outbox).commit(board)?.succeeded(Output {
        id: input.id,
        card_id: input.card_id,
    })
}
