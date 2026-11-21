use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::board_service::handlers::board_state_message;
use crate::board_service::{AddCard, Board};

pub const COMMAND: &str = "board.add_card";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub id: String,
    pub card_id: String,
}

pub fn command() -> TypedCommand<AddCard, Succeeded<Output>> {
    typed_command::<AddCard, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Board>,
    input: AddCard,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut board = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    board.add_card(
        input.card_id.clone(),
        input.column.clone(),
        input.title.clone(),
        input.labels.clone(),
        input.assignee.clone(),
    )?;

    let outbox = board_state_message(&board, "board.card_added")?;
    ctx.repo().outbox(outbox).commit(board)?.succeeded(Output {
        id: input.id,
        card_id: input.card_id,
    })
}
