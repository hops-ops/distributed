//! Portable Blob command declarations.
//!
//! Commands shard by `game_id`, so a cell is `BlobGame:{game_id}`. Client
//! preview wasm remains in the crate's wasm module. Each command and its
//! GraphQL types live in one module.

mod move_dir;
mod start;
mod start_level;
mod support;

pub use move_dir::{handle_move, move_dir, BlobMoveInput, Move};
pub use start::{handle_start, start, BlobStartInput, Start};
pub use start_level::{handle_start_level, start_level, BlobStartLevelInput, StartLevel};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlobGame;
    use distributed::microsvc::Routes;
    use distributed::{AggregateBuilder, InMemoryRepository};

    #[test]
    fn atomic_commands_mount_with_their_projection_contracts() {
        let specs = Routes::new()
            .with_repo(InMemoryRepository::new().aggregate::<BlobGame>())
            .mount(start())
            .mount(move_dir())
            .mount(start_level())
            .command_specs()
            .expect("blob command declarations compile");

        for command in ["blob.start", "blob.move", "blob.start_level"] {
            let spec = specs
                .iter()
                .find(|spec| spec.id == command)
                .unwrap_or_else(|| panic!("missing {command}"));
            let model = spec.projected_model.as_deref().unwrap_or("");
            assert!(
                model == "BlobGames" || model == "blob_games",
                "{command} should be Atomic<BlobGames>, got {model:?}"
            );
        }

        let move_spec = specs
            .iter()
            .find(|spec| spec.id == "blob.move")
            .expect("blob.move");
        let projection = move_spec.projection_contract.to_string();
        assert!(projection.contains("blob.simulate_move"), "{projection}");
        assert!(projection.contains("blobSimulateMove"), "{projection}");
    }
}
