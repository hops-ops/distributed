use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{GetStream, ReadModel, ReadModelWritePlanStore, RepositoryError};
use serde::{Deserialize, Serialize};

use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::types::Tile;
use crate::bomberman::error::GameError;
use crate::bomberman::handlers::{
    get_aggregate, load_active_explosions, load_game_players, load_live_bombs,
};

#[derive(Clone, Debug, Serialize, Deserialize, distributed::JsonType)]
pub struct PlayerState {
    pub id: String,
    pub name: String,
    pub x: i32,
    pub y: i32,
    pub alive: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, distributed::JsonType)]
pub struct BombState {
    pub id: String,
    pub owner: String,
    pub x: i32,
    pub y: i32,
    pub ticks: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, distributed::JsonType)]
pub struct ExplosionState {
    pub id: String,
    pub owner: String,
    pub cells: Vec<(i32, i32)>,
}

/// Whole-board view stored in the `boards` read-model table. Only
/// [`project_board`] writes it; commands never read it.
#[derive(Clone, Debug, Serialize, Deserialize, ReadModel)]
#[table("boards")]
pub struct BoardView {
    #[id]
    pub game_id: String,
    pub width: usize,
    pub height: usize,
    #[readmodel(jsonb)]
    pub tiles: Vec<Vec<Tile>>,
    #[readmodel(jsonb)]
    pub players: Vec<PlayerState>,
    #[readmodel(jsonb)]
    pub bombs: Vec<BombState>,
    #[readmodel(jsonb)]
    pub explosions: Vec<ExplosionState>,
    pub turn: u32,
    #[serde(default)]
    pub explosions_created: u64,
}

impl BoardView {
    pub fn new(game_id: &str, width: usize, height: usize, tiles: Vec<Vec<Tile>>) -> Self {
        Self {
            game_id: game_id.to_string(),
            width,
            height,
            tiles,
            players: vec![],
            bombs: vec![],
            explosions: vec![],
            turn: 0,
            explosions_created: 0,
        }
    }
}

pub fn build_board(
    game_id: &str,
    map: &crate::bomberman::domain::game_map::GameMap,
    players: &[&crate::bomberman::domain::player::Player],
    bombs: &[&crate::bomberman::domain::bomb::Bomb],
    explosions: &[&crate::bomberman::domain::explosion::Explosion],
    turn: u32,
    explosions_created: u64,
) -> BoardView {
    let mut board = BoardView::new(game_id, map.width, map.height, map.tiles.clone());
    board.turn = turn;
    board.explosions_created = explosions_created;

    for p in players {
        board.players.push(PlayerState {
            id: p.entity.id().to_string(),
            name: p.name.clone(),
            x: p.x,
            y: p.y,
            alive: p.alive,
        });
    }

    for b in bombs {
        if !b.exploded {
            board.bombs.push(BombState {
                id: b.entity.id().to_string(),
                owner: b.owner_id.clone(),
                x: b.x,
                y: b.y,
                ticks: b.ticks_remaining,
            });
        }
    }

    for e in explosions {
        if e.active {
            board.explosions.push(ExplosionState {
                id: e.entity.id().to_string(),
                owner: e.owner.clone(),
                cells: e.all_active_cells(),
            });
        }
    }

    board
}

/// The board projection: rebuild the game's [`BoardView`] from its aggregates
/// and write it. It derives everything from committed state, so running it
/// again (a redelivery, a rebuild) writes the same row.
pub async fn project_board<R>(repo: &R, game_id: &str) -> Result<BoardView, GameError>
where
    R: GetStream + ReadModelWritePlanStore,
{
    let map: GameMap = get_aggregate::<R, GameMap>(repo, game_id)
        .await?
        .ok_or(GameError::GameNotFound)?;
    let players = load_game_players(repo, &map).await?;
    let bombs = load_live_bombs(repo, &players).await?;
    let explosions = load_active_explosions(repo, &map).await?;
    let board = build_board(
        game_id,
        &map,
        &players.iter().collect::<Vec<_>>(),
        &bombs.iter().collect::<Vec<_>>(),
        &explosions.iter().collect::<Vec<_>>(),
        map.turn,
        map.explosions_created,
    );
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&board).map_err(RepositoryError::from)?;
    read_models
        .commit(repo)
        .await
        .map_err(RepositoryError::from)?;
    Ok(board)
}
