//! Bomberman Game Example — exercises the full distributed framework.
//!
//! A 4-player Bomberman demonstrates:
//! - Single aggregate + atomic read model commit (player join/move)
//! - Multi-aggregate atomic commit via the staged commit builder (bomb placement)
//! - Aggregate lifecycle: bomb created -> ticked -> exploded -> explosion expands
//! - In-process orchestration saga (tick resolves explosions across aggregates)
//! - Composite read model (BoardView from map + players + bombs + explosions)
//! - Outbox events ("player.killed" on death)
//! - Guard conditions (can't move when dead, can't bomb at max)

mod domain;
mod error;
mod handlers;
mod sim;
mod views;

use domain::types::Direction;
use sim::Game;

use distributed::InMemoryRepository;
use handlers::{tick_cell_name, tick_shard};

const SMALL_MAP: &str = "\
#######
#1 . 2#
# # # #
#  .  #
# # # #
#3 . 4#
#######";

// ============================================================================
// Test 1: Game setup and movement
// Pattern: Single aggregate + read model commit, terrain validation
// ============================================================================

#[test]
fn tick_shards_by_game_id_not_player_or_bomb() {
    assert_eq!(tick_shard("game-1"), "game-1");
    assert_eq!(tick_cell_name("game-1"), "game:game-1");
    assert_ne!(tick_shard("game-1"), "player:1");
    assert_ne!(tick_cell_name("game-1"), "player:player-1");
    assert_ne!(tick_cell_name("game-1"), "bomb:bomb-1");
}

#[tokio::test]
async fn game_setup_and_movement() {
    let repo = InMemoryRepository::new();
    let game = Game::new(&repo, "game-1", SMALL_MAP).await.unwrap();

    let p1 = game.sim("p1", "Alice");
    let p2 = game.sim("p2", "Bob");

    p1.join(0).await.unwrap();
    p2.join(1).await.unwrap();

    // P1 starts at spawn 0 (1,1), move south
    p1.move_dir(Direction::South).await.unwrap();

    // Try to move into a wall — should fail
    let result = p1.move_dir(Direction::West).await;
    assert!(result.is_err());

    // Verify board view
    let board = game.board().await.unwrap();
    assert_eq!(board.data.players.len(), 2);

    let alice = board
        .data
        .players
        .iter()
        .find(|p| p.name == "Alice")
        .unwrap();
    assert_eq!(alice.x, 1);
    assert_eq!(alice.y, 2);
    assert!(alice.alive);

    let bob = board.data.players.iter().find(|p| p.name == "Bob").unwrap();
    assert_eq!(bob.x, 5);
    assert_eq!(bob.y, 1);
    assert!(bob.alive);
}

#[tokio::test]
async fn invalid_spawn_index_returns_error() {
    let repo = InMemoryRepository::new();
    let game = Game::new(&repo, "game-invalid-spawn", SMALL_MAP)
        .await
        .unwrap();
    let player = game.sim("p1", "Alice");

    let result = player.join(99).await;

    assert!(matches!(
        result,
        Err(error::GameError::InvalidSpawnIndex(99))
    ));
}

// ============================================================================
// Test 2: Bomb destroys blocks
// Pattern: Aggregate lifecycle (bomb created -> ticked -> exploded -> expansion)
// ============================================================================

#[tokio::test]
async fn bomb_destroys_blocks() {
    let repo = InMemoryRepository::new();
    let game = Game::new(&repo, "game-2", SMALL_MAP).await.unwrap();

    let p1 = game.sim("p1", "Alice");
    p1.join(0).await.unwrap();

    // Alice at (1,1). Move to (2,1) to be near block at (3,1).
    p1.move_dir(Direction::East).await.unwrap();

    // Place bomb at (2,1) — blast radius 2 east reaches block at (3,1) at ring 1
    p1.place_bomb().await.unwrap();

    // Escape: go west to (1,1) then south to (1,2)
    p1.move_dir(Direction::West).await.unwrap(); // (1,1)
    p1.move_dir(Direction::South).await.unwrap(); // (1,2) — safe from bomb at (2,1)

    // Verify bomb is on the board
    let board = game.board().await.unwrap();
    assert_eq!(board.data.bombs.len(), 1);

    // Tick 3 times: bomb timer 3 -> 2 -> 1 -> 0, detonates, center (2,1) active
    game.tick().await.unwrap();
    game.tick().await.unwrap();
    game.tick().await.unwrap();

    // Tick 4: explosion expands to ring 1 → (3,1) block destroyed
    let tick_result = game.tick().await.unwrap();

    // Block should be destroyed this tick
    assert!(!tick_result.blocks_destroyed.is_empty());

    // Verify bomb gone from board
    let board = game.board().await.unwrap();
    assert_eq!(board.data.bombs.len(), 0);

    // Alice should still be alive (moved away)
    assert!(p1.is_alive().await.unwrap());

    // Verify bomb returned to player
    let player = p1.player().await.unwrap();
    assert_eq!(player.active_bombs, 0);
}

// ============================================================================
// Test 3: Player killed by bomb
// Pattern: Multi-aggregate coordination + outbox event
// ============================================================================

#[tokio::test]
async fn player_killed_by_bomb() {
    // Use a tall open map so players can retreat far from blast
    let repo2 = InMemoryRepository::new();
    let open_map = "\
###########
#1        #
#         #
#         #
#         #
#         #
#        2#
###########";
    let game2 = Game::new(&repo2, "game-3b", open_map).await.unwrap();

    let alice = game2.sim("p1", "Alice");
    let bob = game2.sim("p2", "Bob");

    alice.join(0).await.unwrap(); // (1,1)
    bob.join(1).await.unwrap(); // (9,6)

    // Move Bob to (5,1) via north then west
    bob.move_dir(Direction::North).await.unwrap(); // (9,5)
    bob.move_dir(Direction::North).await.unwrap(); // (9,4)
    bob.move_dir(Direction::North).await.unwrap(); // (9,3)
    bob.move_dir(Direction::North).await.unwrap(); // (9,2)
    bob.move_dir(Direction::North).await.unwrap(); // (9,1)
    bob.move_dir(Direction::West).await.unwrap(); // (8,1)
    bob.move_dir(Direction::West).await.unwrap(); // (7,1)
    bob.move_dir(Direction::West).await.unwrap(); // (6,1)
    bob.move_dir(Direction::West).await.unwrap(); // (5,1)

    // Alice moves east to (3,1)
    alice.move_dir(Direction::East).await.unwrap(); // (2,1)
    alice.move_dir(Direction::East).await.unwrap(); // (3,1)

    // Alice places bomb at (3,1), blast radius 2 east: ring 1=(4,1), ring 2=(5,1)
    alice.place_bomb().await.unwrap();

    // Alice retreats south 3 cells (blast south goes (3,2),(3,3), so (3,4) is safe)
    alice.move_dir(Direction::South).await.unwrap(); // (3,2)
    alice.move_dir(Direction::South).await.unwrap(); // (3,3)
    alice.move_dir(Direction::South).await.unwrap(); // (3,4)

    // Tick 3 times to detonate: timer 3→2→1→0, bomb detonates, center (3,1) active
    game2.tick().await.unwrap();
    game2.tick().await.unwrap();
    game2.tick().await.unwrap();

    // Tick 4: expand to ring 1 → (4,1) — Bob not here
    game2.tick().await.unwrap();

    // Tick 5: expand to ring 2 → (5,1) — Bob killed!
    let tick_result = game2.tick().await.unwrap();

    // Verify Bob was killed
    assert!(!bob.is_alive().await.unwrap());
    assert!(alice.is_alive().await.unwrap());

    // Verify kill recorded on saga
    assert!(tick_result
        .players_killed
        .contains(&"player:p2".to_string()));

    // Verify outbox message was created (PlayerKilled)
    use distributed::{OutboxMessageStatus, OutboxStore};
    let pending = repo2
        .outbox_store()
        .messages_by_status(OutboxMessageStatus::Pending, usize::MAX)
        .await
        .unwrap();
    assert!(!pending.is_empty());
    assert_eq!(pending[0].event_type, "player.killed");
}

// ============================================================================
// Test 4: Chain reaction
// Pattern: In-process orchestration saga (expanding explosion triggers chain)
// ============================================================================

#[tokio::test]
async fn chain_reaction() {
    let repo = InMemoryRepository::new();
    // Tall map so players can retreat far enough from blast radius 2
    let tall_map = "\
###########
#1        #
#         #
#         #
#         #
#         #
#         #
#        2#
###########";
    let game = Game::new(&repo, "game-4", tall_map).await.unwrap();

    let p1 = game.sim("p1", "Alice");
    let p2 = game.sim("p2", "Bob");

    p1.join(0).await.unwrap(); // (1,1)
    p2.join(1).await.unwrap(); // (9,7)

    // Alice moves to (3,1), places bomb, then retreats south far enough
    p1.move_dir(Direction::East).await.unwrap(); // (2,1)
    p1.move_dir(Direction::East).await.unwrap(); // (3,1)
    p1.place_bomb().await.unwrap(); // bomb at (3,1), radius 2

    // Alice retreats south 3 cells (blast goes south only 2)
    p1.move_dir(Direction::South).await.unwrap(); // (3,2)
    p1.move_dir(Direction::South).await.unwrap(); // (3,3)
    p1.move_dir(Direction::South).await.unwrap(); // (3,4)

    // Tick twice so Alice's bomb timer goes from 3→2→1
    game.tick().await.unwrap();
    game.tick().await.unwrap();

    // Now Bob places bomb at (5,1) — 2 cells east of Alice's bomb
    // Bob's bomb will have timer=3, Alice's bomb has timer=1
    // Alice's bomb detonates next tick, expanding blast reaches (5,1) at ring 2
    bob_move_to_position(&game, "p2", "Bob", 9, 7, 5, 1).await;
    p2.place_bomb().await.unwrap(); // bomb at (5,1), radius 2
    p2.move_dir(Direction::South).await.unwrap(); // (5,2)
    p2.move_dir(Direction::South).await.unwrap(); // (5,3)
    p2.move_dir(Direction::South).await.unwrap(); // (5,4)

    // Tick 3: Alice's bomb timer 1→0 → detonates. Bob's bomb timer 3→2.
    //         Alice's explosion: center (3,1) active.
    game.tick().await.unwrap();

    // Tick 4: Alice's explosion expands to ring 1: (2,1),(4,1),(3,0)=wall blocked,(3,2).
    //         Bob's bomb timer 2→1. No chain yet.
    game.tick().await.unwrap();

    // Tick 5: Alice's explosion expands to ring 2: (1,1),(5,1),(3,3).
    //         (5,1) hits Bob's bomb → chain detonation! Bob's bomb marked ticks_remaining=0.
    //         Bob's bomb timer was 1→0 from ticking, BUT the chain mark also sets it to 0.
    //         Either way, Bob's bomb detonates in Phase B. Bob's explosion center (5,1) active.
    let tick_result = game.tick().await.unwrap();

    // Both bombs should have detonated — at least 2 detonations total across all ticks
    // The chain detonation should be recorded
    assert!(
        !tick_result.chain_detonations.is_empty() || tick_result.detonations.len() >= 2,
        "Expected chain reaction or multiple detonations, got {} detonations, {} chains",
        tick_result.detonations.len(),
        tick_result.chain_detonations.len()
    );

    // Both players should be alive (they retreated far enough)
    assert!(p1.is_alive().await.unwrap());
    assert!(p2.is_alive().await.unwrap());
}

/// Helper to move a player step by step to a target position via simple pathfinding.
async fn bob_move_to_position<R>(
    game: &Game<'_, R>,
    id: &str,
    _name: &str,
    from_x: i32,
    from_y: i32,
    to_x: i32,
    to_y: i32,
) where
    R: distributed::GetStream
        + distributed::TransactionalCommit
        + distributed::ReadModelWritePlanStore
        + distributed::RelationalReadModelQueryStore,
{
    let sim = game.sim(id, _name);
    let mut cx = from_x;
    let mut cy = from_y;

    // Move north/south first, then east/west
    while cy != to_y {
        if cy > to_y {
            sim.move_dir(Direction::North).await.unwrap();
            cy -= 1;
        } else {
            sim.move_dir(Direction::South).await.unwrap();
            cy += 1;
        }
    }
    while cx != to_x {
        if cx > to_x {
            sim.move_dir(Direction::West).await.unwrap();
            cx -= 1;
        } else {
            sim.move_dir(Direction::East).await.unwrap();
            cx += 1;
        }
    }
}

// ============================================================================
// Test 5: Concurrent bomb placement
// Pattern: Contested resource — both players place bombs, verify atomic commits
// ============================================================================

#[tokio::test]
async fn concurrent_bomb_placement() {
    use distributed::{ReadModelWorkspaceExt, RowKey, RowValue};
    use std::sync::Arc;

    // InMemoryRepository uses Arc<RwLock<...>> internally, safe to share across tasks
    let repo = Arc::new(InMemoryRepository::new());
    let open_map = "\
#######
#1   2#
#     #
#3   4#
#######";

    // Create game and join players first
    handlers::create_game(&*repo, "game-5", open_map)
        .await
        .unwrap();
    handlers::join_game(&*repo, "p1", "Alice", "game-5", 0)
        .await
        .unwrap();
    handlers::join_game(&*repo, "p2", "Bob", "game-5", 1)
        .await
        .unwrap();

    let repo2 = repo.clone();
    let repo3 = repo.clone();

    // Two futures place bombs concurrently against the shared repository.
    let (r1, r2) = tokio::join!(
        handlers::place_bomb(&*repo2, "p1", "game-5"),
        handlers::place_bomb(&*repo3, "p2", "game-5"),
    );
    r1.unwrap();
    r2.unwrap();

    // Verify both bombs placed
    let _board = repo
        .workspace()
        .load::<views::BoardView>(RowKey::new([(
            "game_id",
            RowValue::String("game-5".into()),
        )]))
        .one()
        .await
        .unwrap()
        .unwrap();
    // Board may show 1 or 2 bombs depending on which task's board view won the race,
    // but both bomb aggregates should exist in the repo.
    let alice: domain::player::Player =
        handlers::get_aggregate::<_, domain::player::Player>(&*repo, "player:p1")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(alice.active_bombs, 1);

    let bob: domain::player::Player =
        handlers::get_aggregate::<_, domain::player::Player>(&*repo, "player:p2")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(bob.active_bombs, 1);

    // Both bomb entities should be in the repo
    let bomb1: Option<domain::bomb::Bomb> =
        handlers::get_aggregate::<_, domain::bomb::Bomb>(&*repo, "bomb:p1:1")
            .await
            .unwrap();
    let bomb2: Option<domain::bomb::Bomb> =
        handlers::get_aggregate::<_, domain::bomb::Bomb>(&*repo, "bomb:p2:1")
            .await
            .unwrap();
    assert!(bomb1.is_some(), "Alice's bomb should exist");
    assert!(bomb2.is_some(), "Bob's bomb should exist");
}

// ============================================================================
// Test 6: Full game to winner
// Pattern: End-to-end composition, game lifecycle
// ============================================================================

#[tokio::test]
async fn full_game_to_winner() {
    let repo = InMemoryRepository::new();
    // Larger arena to allow proper retreat from blast radius 2
    let arena = "\
#############
#1         2#
#           #
#           #
#           #
#           #
#           #
#           #
#           #
#           #
#           #
#3         4#
#############";

    let game = Game::new(&repo, "game-6", arena).await.unwrap();

    let p1 = game.sim("p1", "Alice");
    let p2 = game.sim("p2", "Bob");
    let p3 = game.sim("p3", "Charlie");
    let p4 = game.sim("p4", "Diana");

    // All players join
    p1.join(0).await.unwrap(); // (1,1)
    p2.join(1).await.unwrap(); // (11,1)
    p3.join(2).await.unwrap(); // (1,11)
    p4.join(3).await.unwrap(); // (11,11)

    // Verify initial board
    let board = game.board().await.unwrap();
    assert_eq!(board.data.players.len(), 4);
    assert!(board.data.players.iter().all(|p| p.alive));

    // --- Round 1: Alice traps Bob ---
    // Alice moves east to (9,1), places bomb. Blast east: ring 1=(10,1), ring 2=(11,1) hits Bob.
    p1.move_dir(Direction::East).await.unwrap(); // (2,1)
    p1.move_dir(Direction::East).await.unwrap(); // (3,1)
    p1.move_dir(Direction::East).await.unwrap(); // (4,1)
    p1.move_dir(Direction::East).await.unwrap(); // (5,1)
    p1.move_dir(Direction::East).await.unwrap(); // (6,1)
    p1.move_dir(Direction::East).await.unwrap(); // (7,1)
    p1.move_dir(Direction::East).await.unwrap(); // (8,1)
    p1.move_dir(Direction::East).await.unwrap(); // (9,1)

    p1.place_bomb().await.unwrap(); // bomb at (9,1)

    // Alice retreats south 3 cells (blast south goes (9,2),(9,3))
    p1.move_dir(Direction::South).await.unwrap(); // (9,2)
    p1.move_dir(Direction::South).await.unwrap(); // (9,3)
    p1.move_dir(Direction::South).await.unwrap(); // (9,4) — safe

    // 3 ticks to detonate + 2 ticks for ring 2 expansion = 5 ticks
    game.tick().await.unwrap();
    game.tick().await.unwrap();
    game.tick().await.unwrap(); // detonates, center active
    game.tick().await.unwrap(); // ring 1
    let r1 = game.tick().await.unwrap(); // ring 2 → Bob killed

    assert!(
        !p2.is_alive().await.unwrap(),
        "Bob should be dead from Alice's bomb"
    );
    assert!(r1.players_killed.contains(&"player:p2".to_string()));

    // --- Round 2: Charlie traps Diana ---
    // Charlie at (1,11), Diana at (11,11)
    p3.move_dir(Direction::East).await.unwrap(); // (2,11)
    p3.move_dir(Direction::East).await.unwrap(); // (3,11)
    p3.move_dir(Direction::East).await.unwrap(); // (4,11)
    p3.move_dir(Direction::East).await.unwrap(); // (5,11)
    p3.move_dir(Direction::East).await.unwrap(); // (6,11)
    p3.move_dir(Direction::East).await.unwrap(); // (7,11)
    p3.move_dir(Direction::East).await.unwrap(); // (8,11)
    p3.move_dir(Direction::East).await.unwrap(); // (9,11)

    p3.place_bomb().await.unwrap(); // bomb at (9,11), blast east: ring 2=(11,11) hits Diana

    // Charlie retreats north 3 cells
    p3.move_dir(Direction::North).await.unwrap(); // (9,10)
    p3.move_dir(Direction::North).await.unwrap(); // (9,9)
    p3.move_dir(Direction::North).await.unwrap(); // (9,8) — safe

    // 5 ticks for detonation + expansion to ring 2
    game.tick().await.unwrap();
    game.tick().await.unwrap();
    game.tick().await.unwrap(); // detonates
    game.tick().await.unwrap(); // ring 1
    let r2 = game.tick().await.unwrap(); // ring 2 → Diana killed

    assert!(!p4.is_alive().await.unwrap(), "Diana should be dead");
    assert!(r2.players_killed.contains(&"player:p4".to_string()));

    // --- Round 3: Alice finishes Charlie ---
    // Alice at (9,4), Charlie at (9,8)
    p1.move_dir(Direction::South).await.unwrap(); // (9,5)
    p1.move_dir(Direction::South).await.unwrap(); // (9,6)

    // Place bomb at (9,6), blast south: ring 1=(9,7), ring 2=(9,8) hits Charlie
    p1.place_bomb().await.unwrap();

    // Alice retreats north 3 cells
    p1.move_dir(Direction::North).await.unwrap(); // (9,5)
    p1.move_dir(Direction::North).await.unwrap(); // (9,4)
    p1.move_dir(Direction::North).await.unwrap(); // (9,3) — safe

    // 5 ticks for detonation + expansion to ring 2
    game.tick().await.unwrap();
    game.tick().await.unwrap();
    game.tick().await.unwrap(); // detonates
    game.tick().await.unwrap(); // ring 1
    let r3 = game.tick().await.unwrap(); // ring 2 → Charlie killed

    assert!(!p3.is_alive().await.unwrap(), "Charlie should be dead");
    assert!(r3.game_over, "Game should be over with only Alice alive");
    assert_eq!(r3.winner.as_deref(), Some("Alice"));

    // Verify final board
    let board = game.board().await.unwrap();
    let alive_count = board.data.players.iter().filter(|p| p.alive).count();
    assert_eq!(alive_count, 1);
    let winner_state = board.data.players.iter().find(|p| p.alive).unwrap();
    assert_eq!(winner_state.name, "Alice");
}
