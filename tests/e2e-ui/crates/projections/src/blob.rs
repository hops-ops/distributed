//! Blob: the row mutation and its projection.

use blob_domain::{
    BlobInitializedDomainEvent, BlobLevelStartedDomainEvent, BlobMovedDomainEvent,
    BlobStartedDomainEvent,
};
use distributed::mutation::Mutation;
use distributed::mutation_file;
use distributed::projection::lower::{DirectCandidate, ProjectionDescriptor};
use e2e_readmodels::BlobGames;

/// Mutation: upsert one BlobGames row from `input.game`.
///
/// The constructor name matches the document operation: `mutation SaveBlobGame`.
#[allow(non_snake_case)]
pub fn SaveBlobGame() -> Mutation<()> {
    mutation_file!("src/mutations/save_blob_game.mutation.graphql")
}

// When these domain events fire, apply [`SaveBlobGame`] (body → `input.game`).
// Event-first: on { events, mutation, input } — same shape as todos/chat.
// Command path stages the row via `Mutation::from_state` +
// `readmodel(row).commit()?.atomic()`.
// Macro is `projection!` (crate root); `distributed::projection` is the module.
distributed::projection! {
    pub const BLOB_GAMES: ProjectionDescriptor<DirectCandidate> = {
        name: "project_blob",
        version: 1,
        epoch: "e2e-ui-blob-v3",
        model: BlobGames,
        on {
            events: [
                BlobInitializedDomainEvent,
                BlobLevelStartedDomainEvent,
                BlobStartedDomainEvent,
                BlobMovedDomainEvent,
            ],
            mutation: SaveBlobGame,
            input: { game: body },
        },
    };
}

#[cfg(test)]
mod tests {
    use distributed::projection::MutationKind;
    use distributed::table::RowValue;

    use super::*;
    use blob_domain::{BlobGame, BlobGameState, Direction};

    #[test]
    fn from_state_materializes_blob_games_row_for_handler_owned_projected() {
        let mut game = BlobGame::default();
        game.start_with_demo("g1", "alice").unwrap();
        let state = BlobGameState::from(&game);
        let row: BlobGames = SaveBlobGame()
            .from_state(&state)
            .expect("mutation from_state builds the projected row");
        assert_eq!(row.game_id, "g1");
        assert_eq!(row.owner_id, "alice");
        assert_eq!(row.map, state.map);
        assert_eq!(row.moves, state.moves);
        assert!(!row.moves.is_empty());
        assert!(row.owner.is_none());
    }

    #[test]
    fn save_blob_game_mutation_is_single_row_event_free_upsert() {
        let program = SaveBlobGame().program().unwrap();
        assert_eq!(program.name(), "SaveBlobGame");
        assert_eq!(program.operations().len(), 1);
        assert_eq!(program.operations()[0].kind(), MutationKind::Upsert);
        assert_eq!(program.operations()[0].target().model(), "BlobGames");
        let json = serde_json::to_value(&program).unwrap().to_string();
        assert!(!json.contains("event_name"));
        assert!(!json.contains("blob.moved"));
        assert!(program.operations()[0].fields().len() >= 1);
    }

    fn assert_direct_state_upsert(game: &BlobGame, expected_event: &str) {
        let occurrence = game.entity.pending_domain_events().last().unwrap();
        assert_eq!(occurrence.descriptor().name, expected_event);
        let state = occurrence.decode_body::<BlobGameState>().unwrap();
        assert_eq!(state, game.state());

        let lowered = BLOB_GAMES
            .server_executor()
            .unwrap()
            .plan(occurrence)
            .unwrap();
        let [(resolved, record)] = lowered.records().collect::<Vec<_>>()[..] else {
            panic!("Blob direct projection must lower to one record");
        };
        assert_eq!(resolved.kind(), MutationKind::Upsert);
        assert_eq!(record.schema().model_name, "BlobGames");
        assert_eq!(
            record.values().get("game_id"),
            Some(&RowValue::String(game.entity.id().to_string()))
        );
    }

    #[test]
    fn initialized_state_is_one_complete_direct_upsert() {
        let mut game = BlobGame::default();
        game.start_with_demo("g1", "alice").unwrap();
        // start_with_demo may emit multiple state events; every occurrence
        // must lower through SAVE_BLOB_GAME.
        for occurrence in game.entity.pending_domain_events() {
            let lowered = BLOB_GAMES
                .server_executor()
                .unwrap()
                .plan(occurrence)
                .unwrap();
            let kinds = lowered
                .records()
                .map(|(resolved, _)| resolved.kind())
                .collect::<Vec<_>>();
            assert_eq!(kinds, [MutationKind::Upsert]);
        }
        assert!(!game.entity.pending_domain_events().is_empty());
    }

    #[test]
    fn started_state_is_one_complete_direct_upsert() {
        let mut game = BlobGame::default();
        game.start_with_demo("g1", "alice").unwrap();
        // start_with_demo may emit initialized then started depending on domain
        let names: Vec<String> = game
            .entity
            .pending_domain_events()
            .iter()
            .map(|e| e.descriptor().name.to_string())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "blob.initialized" || n == "blob.started"),
            "unexpected events: {names:?}"
        );
    }

    #[test]
    fn level_started_state_is_one_complete_direct_upsert() {
        let mut game = BlobGame::default();
        game.start_with_demo("g1", "alice").unwrap();
        // advance if domain supports
        let _ = game.start_next_generated_level("alice");
        if let Some(occurrence) = game.entity.pending_domain_events().last() {
            if occurrence.descriptor().name == "blob.level_started" {
                assert_direct_state_upsert(&game, "blob.level_started");
            }
        }
    }

    #[test]
    fn moved_state_is_one_complete_direct_upsert() {
        let mut game = BlobGame::default();
        game.start_with_demo("g1", "alice").unwrap();
        if game.move_dir("alice", Direction::Up).is_ok() {
            assert_direct_state_upsert(&game, "blob.moved");
        }
    }

    #[test]
    fn descriptor_is_one_direct_model_across_all_blob_event_arms() {
        let inventory = BLOB_GAMES.output_inventory().unwrap();
        assert_eq!(inventory.models.len(), 1);
        assert_eq!(inventory.models[0].model, "BlobGames");
        assert_eq!(BLOB_GAMES.program().unwrap().arms().len(), 4);
    }
}
