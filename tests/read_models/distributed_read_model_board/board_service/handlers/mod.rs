pub(super) mod board_add_card;
pub(super) mod board_move_card;
pub(super) mod board_open;
pub(super) mod board_remove_card;

use distributed::{OutboxMessage, SourcedResult};

use crate::distributed_read_model_board::board_service::Board;

/// Compatibility envelope for this legacy demo projector.
///
/// The snapshot use is deliberate and local: the fluent commit API never
/// turns snapshots into published contracts implicitly.
pub(super) fn board_state_message(board: &Board, event_type: &str) -> SourcedResult<OutboxMessage> {
    OutboxMessage::encode_for_entity(
        format!(
            "{}:{event_type}:{}",
            board.entity.id(),
            board.entity.version()
        ),
        event_type,
        &board.snapshot(),
        &board.entity,
    )
}
