//! Projects board events into `boards` + `cards`. The board snapshot is the
//! updated view, so workspace sync reflects added/moved/removed cards as
//! inserts/patches/deletes. A monotonic `source_version` guard ignores stale
//! snapshots under out-of-order delivery.

use distributed::microsvc::{Context, HandlerError};
use distributed::read_model::ReadModelWorkspaceExt;
use distributed::BitcodePayloadCodec;
use serde_json::{json, Value};

use crate::distributed_read_model_board::board_service::BoardSnapshot;
use crate::distributed_read_model_board::projections_service::{
    read_model_error, ProjectionDependencies,
};
use crate::distributed_read_model_board::read_models::{
    board_key, BoardView, CardPayload, CardView,
};

pub const EVENTS: &[&str] = &[
    "board.opened",
    "board.card_added",
    "board.card_moved",
    "board.card_removed",
];

pub fn guard(ctx: &Context<ProjectionDependencies>) -> bool {
    ctx.message().id().is_some()
}

pub async fn handle(ctx: &Context<'_, ProjectionDependencies>) -> Result<Value, HandlerError> {
    let message_id = ctx
        .message()
        .id()
        .ok_or_else(|| HandlerError::DecodeFailed("board projection message has no id".into()))?;
    let snapshot: BoardSnapshot = BitcodePayloadCodec::decode(ctx.message().payload())
        .map_err(|err| HandlerError::DecodeFailed(format!("board snapshot: {err}")))?;
    let version = event_version(message_id)?;
    let updated_view = updated_board_view(&snapshot, version);

    let mut workspace = ctx.read_model_store().workspace();
    let existing = workspace
        .load::<BoardView>(board_key(&updated_view.board_id))
        .include("cards")
        .one()
        .await
        .map_err(read_model_error)?;

    match existing {
        Some(current) if current.data.source_version >= version => {}
        Some(_) => {
            workspace.sync(updated_view).map_err(read_model_error)?;
        }
        None => {
            workspace.upsert(&updated_view).map_err(read_model_error)?;
            for card in &updated_view.cards {
                workspace.upsert(card).map_err(read_model_error)?;
            }
        }
    }

    workspace.commit().await.map_err(read_model_error)?;

    Ok(json!({ "event_id": message_id }))
}

fn updated_board_view(snapshot: &BoardSnapshot, version: i64) -> BoardView {
    let cards = snapshot
        .cards
        .iter()
        .map(|card| CardView {
            board_id: snapshot.id.clone(),
            card_id: card.card_id.clone(),
            column: card.column.clone(),
            title: card.title.clone(),
            payload: CardPayload {
                labels: card.labels.clone(),
                assignee: card.assignee.clone(),
            },
            board: None,
        })
        .collect();

    BoardView {
        board_id: snapshot.id.clone(),
        name: snapshot.name.clone(),
        source_version: version,
        cards,
    }
}

/// The aggregate version is the trailing segment of the outbox event id
/// (`<aggregate-id>:<event-type>:<version>`).
fn event_version(id: &str) -> Result<i64, HandlerError> {
    let segment = id.rsplit(':').next().ok_or_else(|| {
        HandlerError::DecodeFailed(
            "board projection event id should include a version segment".into(),
        )
    })?;
    segment.parse().map_err(|_| {
        HandlerError::DecodeFailed(
            "board projection event id should end with a numeric aggregate version".into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_version_parses_trailing_outbox_segment() {
        assert_eq!(event_version("board-1:board.card_added:42").unwrap(), 42);
    }

    #[test]
    fn event_version_rejects_malformed_outbox_segment() {
        let err = event_version("board-1:board.card_added:bad").unwrap_err();
        assert!(matches!(err, HandlerError::DecodeFailed(_)));
    }
}
