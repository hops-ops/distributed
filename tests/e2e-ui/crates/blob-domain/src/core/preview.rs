//! The browser preview of `blob.move`: [`simulate_move`] over the known
//! `BlobGames` row, compiled to WASM by `distributed build`.

use serde::Deserialize;

use super::{simulate_move, Direction, MovePreview, Tile};

/// The `BlobGames` columns a move reads.
#[derive(Debug, Deserialize, distributed::table::JsonType)]
pub struct MoveRow {
    pub map: Vec<Vec<Tile>>,
    pub score: i64,
}

/// The `blob.move` input the preview needs.
#[derive(Debug, Deserialize, distributed::table::JsonType)]
pub struct MoveArgs {
    pub direction: Direction,
}

/// The row after the move, or `None` (no optimism) when the move is illegal.
pub fn preview_move(row: MoveRow, args: MoveArgs) -> Option<MovePreview> {
    simulate_move(&row.map, row.score, args.direction).ok()
}

distributed::wasm_pure! {
    /// Client optimism for `blob.move`: the patched `BlobGames` columns.
    pub const PREVIEW_MOVE = preview_move(MoveRow, MoveArgs) -> MovePreview;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_bridges_the_row_json_and_fails_closed() {
        let row = r#"{"game_id":"g1","map":[["player","unvisited"]],"score":2}"#;
        let moved =
            distributed::command::__run_wasm_pure(row, r#"{"direction":"right"}"#, preview_move)
                .unwrap();
        let moved: serde_json::Value = serde_json::from_str(&moved).unwrap();
        assert_eq!(moved["map"], serde_json::json!([["visited", "player"]]));
        assert_eq!(moved["score"], 3);
        assert_eq!(moved["status"], "level_complete");
        assert_eq!(moved["moves"], serde_json::json!(["left"]));
        assert_eq!(
            distributed::command::__run_wasm_pure(row, r#"{"direction":"left"}"#, preview_move),
            None,
            "an edge move has no preview"
        );
        assert_eq!(
            distributed::command::__run_wasm_pure(row, r#"{"direction":"sideways"}"#, preview_move),
            None
        );
    }
}
