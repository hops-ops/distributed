use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

use crate::levels::generate_level;

use super::{BlobError, BlobGameState};
use crate::core::{legal_moves, simulate_move, status_of, Direction, SimulateError, Tile};

/// Tiny 3×3 map for unit tests (player top-left, no holes).
pub fn test_map_no_holes() -> Vec<Vec<Tile>> {
    use Tile::*;
    vec![
        vec![Player, Unvisited, Unvisited],
        vec![Unvisited, Unvisited, Unvisited],
        vec![Unvisited, Unvisited, Unvisited],
    ]
}

pub fn test_map_with_hole() -> Vec<Vec<Tile>> {
    use Tile::*;
    vec![
        vec![Player, Hole, Unvisited],
        vec![Unvisited, Unvisited, Unvisited],
        vec![Unvisited, Unvisited, Unvisited],
    ]
}

fn validate_map(map: &[Vec<Tile>]) -> Result<(), BlobError> {
    if map.is_empty() || map[0].is_empty() {
        return Err(BlobError::InvalidMap("empty map".into()));
    }
    let w = map[0].len();
    let mut players = 0usize;
    for (r, row) in map.iter().enumerate() {
        if row.len() != w {
            return Err(BlobError::InvalidMap(format!("ragged row {r}")));
        }
        for &t in row {
            if t == Tile::Player {
                players += 1;
            }
        }
    }
    if players != 1 {
        return Err(BlobError::InvalidMap(format!(
            "expected exactly one player tile, found {players}"
        )));
    }
    Ok(())
}

fn map_simulate_err(err: SimulateError) -> BlobError {
    match err {
        SimulateError::NoActiveLevel => BlobError::NoActiveLevel,
        SimulateError::CannotMove(msg) => BlobError::CannotMove(msg.into()),
    }
}

#[cfg(test)]
fn player_pos_in(map: &[Vec<Tile>]) -> Result<(usize, usize), BlobError> {
    for (r, row) in map.iter().enumerate() {
        for (c, &t) in row.iter().enumerate() {
            if t == Tile::Player {
                return Ok((r, c));
            }
        }
    }
    Err(BlobError::NoActiveLevel)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlobGame {
    #[serde(skip, default)]
    pub entity: Entity,
    pub game_id: String,
    pub owner_id: String,
    pub score: i64,
    pub player_dead: bool,
    /// 0 = no level yet; 1+ = active level index (1-based like JS).
    pub current_level: i64,
    pub current_level_completed: bool,
    /// Current level map only (demo keeps one active map in state).
    pub map: Vec<Vec<Tile>>,
}

impl BlobGame {
    pub fn is_created(&self) -> bool {
        !self.game_id.is_empty()
    }

    pub fn ensure_owner(&self, owner_id: &str) -> Result<(), BlobError> {
        if !self.is_created() {
            return Err(BlobError::NotCreated);
        }
        if self.owner_id != owner_id {
            return Err(BlobError::NotOwner);
        }
        Ok(())
    }

    /// Capture the stable public state used by Blob domain events.
    pub fn state(&self) -> BlobGameState {
        BlobGameState {
            game_id: self.game_id.clone(),
            owner_id: self.owner_id.clone(),
            score: self.score,
            player_dead: self.player_dead,
            current_level: self.current_level,
            current_level_completed: self.current_level_completed,
            map: self.map.clone(),
            moves: legal_moves(&self.map),
            status: status_of(self.player_dead, self.current_level_completed),
        }
    }

    #[cfg(test)]
    fn player_pos(&self) -> Result<(usize, usize), BlobError> {
        player_pos_in(&self.map)
    }
}

#[sourced(
    entity,
    events = "BlobGameEvent",
    aggregate_type = "blob",
    domain_state = BlobGameState,
)]
impl BlobGame {
    /// Create game shell (no map yet). Level 0 is "completed" so first level can start.
    pub fn initialize(
        &mut self,
        game_id: impl Into<String>,
        owner_id: impl Into<String>,
    ) -> Result<(), BlobError> {
        if self.is_created() {
            return Err(BlobError::AlreadyExists);
        }
        let game_id = game_id.into();
        let owner_id = owner_id.into();
        if game_id.trim().is_empty() {
            return Err(BlobError::EmptyId);
        }
        if owner_id.trim().is_empty() {
            return Err(BlobError::EmptyOwner);
        }
        self.record_initialized(game_id, owner_id)?;
        Ok(())
    }

    #[event("blob.initialized", version = 1, domain, known(player_dead: false, current_level_completed: true))]
    fn record_initialized(&mut self, game_id: String, owner_id: String) {
        self.entity.set_id(&game_id);
        self.game_id = game_id;
        self.owner_id = owner_id;
        self.score = 0;
        self.player_dead = false;
        self.current_level = 0;
        self.current_level_completed = true;
        self.map = Vec::new();
    }

    /// Append/start a level map. Requires current level complete and player alive.
    pub fn start_level(&mut self, owner_id: &str, map: Vec<Vec<Tile>>) -> Result<(), BlobError> {
        self.ensure_owner(owner_id)?;
        if self.player_dead {
            return Err(BlobError::PlayerDead);
        }
        if !self.current_level_completed {
            return Err(BlobError::LevelNotComplete);
        }
        validate_map(&map)?;
        let next = self.current_level + 1;
        self.record_level_started(next, map)?;
        Ok(())
    }

    #[event("blob.level_started", version = 1, domain, known(current_level_completed: false, player_dead: false))]
    fn record_level_started(&mut self, current_level: i64, map: Vec<Vec<Tile>>) {
        self.current_level = current_level;
        self.map = map;
        self.current_level_completed = false;
        self.player_dead = false;
    }

    /// Create game and start a **generated** passable level (`blob.started`).
    pub fn start_with_demo(
        &mut self,
        game_id: impl Into<String>,
        owner_id: impl Into<String>,
    ) -> Result<(), BlobError> {
        self.start_with_map(game_id, owner_id, generate_level(1))
    }

    /// Create game with an explicit map (tests / fixtures).
    pub fn start_with_map(
        &mut self,
        game_id: impl Into<String>,
        owner_id: impl Into<String>,
        map: Vec<Vec<Tile>>,
    ) -> Result<(), BlobError> {
        if self.is_created() {
            return Err(BlobError::AlreadyExists);
        }
        let game_id = game_id.into();
        let owner_id = owner_id.into();
        if game_id.trim().is_empty() {
            return Err(BlobError::EmptyId);
        }
        if owner_id.trim().is_empty() {
            return Err(BlobError::EmptyOwner);
        }
        validate_map(&map)?;
        self.record_started(game_id, owner_id, 1, map)?;
        Ok(())
    }

    /// Start next level with a freshly generated passable map.
    pub fn start_next_generated_level(&mut self, owner_id: &str) -> Result<(), BlobError> {
        let next = self.current_level + 1;
        let map = generate_level(next as u32);
        self.start_level(owner_id, map)
    }

    #[event("blob.started", version = 1, domain, known(player_dead: false, current_level_completed: false))]
    fn record_started(
        &mut self,
        game_id: String,
        owner_id: String,
        current_level: i64,
        map: Vec<Vec<Tile>>,
    ) {
        self.entity.set_id(&game_id);
        self.game_id = game_id;
        self.owner_id = owner_id;
        self.score = 0;
        self.player_dead = false;
        self.current_level = current_level;
        self.current_level_completed = false;
        self.map = map;
    }

    pub fn move_dir(&mut self, owner_id: &str, direction: Direction) -> Result<(), BlobError> {
        self.ensure_owner(owner_id)?;
        if self.player_dead {
            return Err(BlobError::PlayerDead);
        }
        if self.current_level == 0 || self.map.is_empty() {
            return Err(BlobError::NoActiveLevel);
        }
        let preview = simulate_move(&self.map, self.score, direction).map_err(map_simulate_err)?;
        self.record_moved(
            preview.score,
            preview.player_dead,
            preview.current_level_completed,
            preview.map,
            direction.as_str().to_string(),
        )?;
        Ok(())
    }

    #[event("blob.moved", version = 1, domain)]
    fn record_moved(
        &mut self,
        score: i64,
        player_dead: bool,
        current_level_completed: bool,
        map: Vec<Vec<Tile>>,
        _direction: String,
    ) {
        self.score = score;
        self.player_dead = player_dead;
        self.current_level_completed = current_level_completed;
        self.map = map;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::levels::{demo_map, is_hamiltonian_passable};

    fn game_with_map(map: Vec<Vec<Tile>>) -> BlobGame {
        let mut g = BlobGame::default();
        g.initialize("g1", "alice").unwrap();
        g.start_level("alice", map).unwrap();
        g
    }

    #[test]
    fn initialize_and_start_level() {
        let mut g = BlobGame::default();
        g.initialize("g1", "alice").unwrap();
        assert!(g.current_level_completed);
        assert_eq!(g.current_level, 0);
        g.start_level("alice", test_map_no_holes()).unwrap();
        assert_eq!(g.current_level, 1);
        assert!(!g.current_level_completed);
        assert_eq!(g.player_pos().unwrap(), (0, 0));
    }

    #[test]
    fn simulate_move_matches_move_dir() {
        let mut g = BlobGame::default();
        g.start_with_map("g1", "alice", test_map_no_holes())
            .unwrap();
        let preview = simulate_move(&g.map, g.score, Direction::Right).unwrap();
        g.move_dir("alice", Direction::Right).unwrap();
        assert_eq!(g.map, preview.map);
        assert_eq!(g.score, preview.score);
        assert_eq!(g.player_dead, preview.player_dead);
        assert_eq!(g.current_level_completed, preview.current_level_completed);
        assert_eq!(g.state().moves, preview.moves);
        assert_eq!(g.state().status, preview.status);
    }

    #[test]
    fn start_level_requires_complete() {
        let mut g = game_with_map(test_map_no_holes());
        let err = g.start_level("alice", test_map_no_holes()).unwrap_err();
        assert_eq!(err, BlobError::LevelNotComplete);
    }

    #[test]
    fn move_scores_unvisited() {
        let mut g = game_with_map(test_map_no_holes());
        g.move_dir("alice", Direction::Right).unwrap();
        assert_eq!(g.score, 1);
        assert_eq!(g.player_pos().unwrap(), (0, 1));
        assert_eq!(g.map[0][0], Tile::Visited);
    }

    #[test]
    fn die_on_hole() {
        let mut g = game_with_map(test_map_with_hole());
        g.move_dir("alice", Direction::Right).unwrap();
        assert!(g.player_dead);
        assert_eq!(g.map[0][1], Tile::DeadByHole);
        assert_eq!(g.score, 0);
    }

    #[test]
    fn die_on_revisit() {
        let mut g = game_with_map(test_map_no_holes());
        g.move_dir("alice", Direction::Right).unwrap();
        g.move_dir("alice", Direction::Left).unwrap();
        assert!(g.player_dead);
        assert_eq!(g.map[0][0], Tile::DeadBySuicide);
    }

    #[test]
    fn edge_move_rejected() {
        let mut g = game_with_map(test_map_no_holes());
        let err = g.move_dir("alice", Direction::Up).unwrap_err();
        assert!(matches!(err, BlobError::CannotMove(_)));
        assert_eq!(g.score, 0);
        assert_eq!(g.player_pos().unwrap(), (0, 0));
    }

    #[test]
    fn complete_level_when_no_unvisited() {
        // 2×2: player + 3 unvisited
        use Tile::*;
        let map = vec![vec![Player, Unvisited], vec![Unvisited, Unvisited]];
        let mut g = game_with_map(map);
        g.move_dir("alice", Direction::Right).unwrap();
        g.move_dir("alice", Direction::Down).unwrap();
        g.move_dir("alice", Direction::Left).unwrap();
        assert!(g.current_level_completed);
        assert!(!g.player_dead);
        assert_eq!(g.score, 3);
        assert_eq!(g.state().status, "level_complete");
    }

    #[test]
    fn not_owner_rejected() {
        let mut g = game_with_map(test_map_no_holes());
        assert_eq!(
            g.move_dir("bob", Direction::Right).unwrap_err(),
            BlobError::NotOwner
        );
    }

    #[test]
    fn complete_then_start_next_level() {
        use Tile::*;
        let map = vec![vec![Player, Unvisited], vec![Unvisited, Unvisited]];
        let mut g = BlobGame::default();
        g.initialize("g1", "alice").unwrap();
        g.start_level("alice", map).unwrap();
        g.move_dir("alice", Direction::Right).unwrap();
        g.move_dir("alice", Direction::Down).unwrap();
        g.move_dir("alice", Direction::Left).unwrap();
        assert!(g.current_level_completed);
        assert_eq!(g.current_level, 1);
        g.start_next_generated_level("alice").unwrap();
        assert_eq!(g.current_level, 2);
        assert!(!g.current_level_completed);
        assert!(!g.map.is_empty());
        // hydrate round-trip
        let g2: BlobGame = distributed::hydrate(g.entity.clone()).unwrap();
        assert_eq!(g2.current_level, 2);
        assert!(!g2.current_level_completed);
    }

    #[test]
    fn hydrate_preserves_level_complete_before_next() {
        use Tile::*;
        let map = vec![vec![Player, Unvisited], vec![Unvisited, Unvisited]];
        let mut g = BlobGame::default();
        g.initialize("g1", "alice").unwrap();
        g.start_level("alice", map).unwrap();
        g.move_dir("alice", Direction::Right).unwrap();
        g.move_dir("alice", Direction::Down).unwrap();
        g.move_dir("alice", Direction::Left).unwrap();
        assert!(g.current_level_completed);
        let mut g2: BlobGame = distributed::hydrate(g.entity.clone()).unwrap();
        assert!(
            g2.current_level_completed,
            "completed flag must survive hydrate"
        );
        g2.start_next_generated_level("alice").unwrap();
        assert_eq!(g2.current_level, 2);
    }

    #[test]
    fn replay_suppresses_blob_domain_state_recapture() {
        let mut game = BlobGame::default();
        game.start_with_map("g1", "alice", test_map_no_holes())
            .unwrap();
        game.entity.mark_committed();
        game.entity.mark_domain_events_committed().unwrap();

        let replayed: BlobGame = distributed::hydrate(game.entity.clone()).unwrap();
        assert!(replayed.entity.pending_domain_events().is_empty());
        assert_eq!(replayed.state().status, "active");
    }

    #[test]
    fn demo_map_valid() {
        validate_map(&demo_map()).unwrap();
    }

    #[test]
    fn start_with_demo_uses_generated_passable_map() {
        let mut g = BlobGame::default();
        g.start_with_demo("g-rand", "alice").unwrap();
        assert_eq!(g.map[0][0], Tile::Player);
        assert!(is_hamiltonian_passable(&g.map));
        assert!(g.map.len() >= 5);
    }
}
