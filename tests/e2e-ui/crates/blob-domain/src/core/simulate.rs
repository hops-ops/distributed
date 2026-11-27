//! Pure post-move board snapshot.

use serde::Serialize;

use super::{Direction, Tile};

/// The game columns a move changes, named as `BlobGames` columns so the
/// client preview patches them directly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, distributed::table::JsonType)]
pub struct MovePreview {
    pub map: Vec<Vec<Tile>>,
    /// Directions that stay on the board from the new position.
    pub moves: Vec<Direction>,
    pub score: i64,
    pub player_dead: bool,
    pub current_level_completed: bool,
    /// `active` | `dead` | `level_complete`
    pub status: String,
}

/// Failures from pure board simulation (fail-closed on the client).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulateError {
    NoActiveLevel,
    CannotMove(&'static str),
}

pub fn status_of(player_dead: bool, level_complete: bool) -> String {
    if player_dead {
        "dead".into()
    } else if level_complete {
        "level_complete".into()
    } else {
        "active".into()
    }
}

/// The directions a live player can take without leaving the board. Empty
/// when there is no player (no level, or the player died).
pub fn legal_moves(map: &[Vec<Tile>]) -> Vec<Direction> {
    let Ok(at) = player_pos_in(map) else {
        return Vec::new();
    };
    Direction::ALL
        .into_iter()
        .filter(|direction| step(map, at, *direction).is_ok())
        .collect()
}

fn step(
    map: &[Vec<Tile>],
    (r, c): (usize, usize),
    direction: Direction,
) -> Result<(usize, usize), SimulateError> {
    match direction {
        Direction::Up if r == 0 => Err(SimulateError::CannotMove("row already 0")),
        Direction::Up => Ok((r - 1, c)),
        Direction::Down if r + 1 >= map.len() => {
            Err(SimulateError::CannotMove("already at bottom edge"))
        }
        Direction::Down => Ok((r + 1, c)),
        Direction::Left if c == 0 => Err(SimulateError::CannotMove("column already 0")),
        Direction::Left => Ok((r, c - 1)),
        Direction::Right if c + 1 >= map[r].len() => {
            Err(SimulateError::CannotMove("already at right edge"))
        }
        Direction::Right => Ok((r, c + 1)),
    }
}

/// Apply one direction to a map + score.
pub fn simulate_move(
    map: &[Vec<Tile>],
    score: i64,
    direction: Direction,
) -> Result<MovePreview, SimulateError> {
    if map.is_empty() || map[0].is_empty() {
        return Err(SimulateError::NoActiveLevel);
    }
    let (r, c) = player_pos_in(map)?;
    let (nr, nc) = step(map, (r, c), direction)?;

    let mut next_map = map.to_vec();
    let mut score = score;
    next_map[r][c] = Tile::Visited;
    next_map[nr][nc] = match next_map[nr][nc] {
        Tile::Hole => Tile::DeadByHole,
        Tile::Unvisited | Tile::Player => {
            score += 1;
            Tile::Player
        }
        Tile::Visited | Tile::DeadBySuicide | Tile::DeadByHole => Tile::DeadBySuicide,
    };
    let player_dead = next_map
        .iter()
        .flatten()
        .any(|tile| matches!(tile, Tile::DeadByHole | Tile::DeadBySuicide));
    let level_complete = !player_dead
        && !next_map
            .iter()
            .flatten()
            .any(|tile| *tile == Tile::Unvisited);

    Ok(MovePreview {
        moves: legal_moves(&next_map),
        map: next_map,
        score,
        player_dead,
        current_level_completed: level_complete,
        status: status_of(player_dead, level_complete),
    })
}

fn player_pos_in(map: &[Vec<Tile>]) -> Result<(usize, usize), SimulateError> {
    for (r, row) in map.iter().enumerate() {
        for (c, &t) in row.iter().enumerate() {
            if t == Tile::Player {
                return Ok((r, c));
            }
        }
    }
    Err(SimulateError::NoActiveLevel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use Tile::*;

    fn tiny() -> Vec<Vec<Tile>> {
        vec![
            vec![Player, Unvisited, Unvisited],
            vec![Unvisited, Unvisited, Unvisited],
            vec![Unvisited, Unvisited, Unvisited],
        ]
    }

    #[test]
    fn move_right_increments_score() {
        let preview = simulate_move(&tiny(), 0, Direction::Right).unwrap();
        assert_eq!(preview.score, 1);
        assert!(!preview.player_dead);
        assert_eq!(preview.map[0][0], Visited);
        assert_eq!(preview.map[0][1], Player);
        assert_eq!(
            preview.moves,
            [Direction::Down, Direction::Left, Direction::Right]
        );
        assert_eq!(preview.status, "active");
    }

    #[test]
    fn edge_fails_closed() {
        assert!(matches!(
            simulate_move(&tiny(), 0, Direction::Up),
            Err(SimulateError::CannotMove(_))
        ));
        assert_eq!(legal_moves(&tiny()), [Direction::Down, Direction::Right]);
    }

    #[test]
    fn a_dead_player_has_no_moves() {
        let preview = simulate_move(&[vec![Player, Hole]], 0, Direction::Right).unwrap();
        assert!(preview.player_dead);
        assert!(preview.moves.is_empty());
        assert_eq!(preview.status, "dead");
    }
}
