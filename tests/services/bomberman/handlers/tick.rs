use distributed::{
    Aggregate, CommitBatch, GetStream, OutboxMessage, RepositoryError, StreamIdentity, StreamWrite,
    TransactionalCommit,
};

use super::shared::{get_aggregate, load_active_explosions, load_game_players, load_live_bombs};
use crate::bomberman::domain::bomb::Bomb;
use crate::bomberman::domain::explosion::Explosion;
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::player::Player;
use crate::bomberman::domain::tick_saga::{Detonation, TickSaga};
use crate::bomberman::domain::types::{Direction, Tile};
use crate::bomberman::error::GameError;

/// Parent shard for a cell host (`PCH-REQ-006` / `PCH-AC-005.1`).
///
/// Tick (and every other game command) addresses **one game cell**, not a
/// player or bomb cell. Child streams — map, players, bombs, explosions,
/// saga — live inside that cell's SQLite and commit in one [`CommitBatch`].
pub fn tick_shard(game_id: &str) -> String {
    game_id.to_string()
}

/// Cell instance name: `game:{game_id}`.
pub fn tick_cell_name(game_id: &str) -> String {
    format!("game:{game_id}")
}

#[derive(Default)]
struct DamageReport {
    blocks_destroyed: Vec<(i32, i32)>,
    players_killed: Vec<String>,
    chain_detonations: Vec<String>,
}

impl DamageReport {
    fn has_damage(&self) -> bool {
        !self.blocks_destroyed.is_empty()
            || !self.players_killed.is_empty()
            || !self.chain_detonations.is_empty()
    }
}

struct KillAttribution {
    player_id: String,
    bomb_id: String,
    bomb_owner: String,
}

pub async fn tick<R>(repo: &R, game_id: &str) -> Result<TickSaga, GameError>
where
    R: GetStream + TransactionalCommit,
{
    let mut map: GameMap = get_aggregate::<R, GameMap>(repo, game_id)
        .await?
        .ok_or(GameError::GameNotFound)?;

    let mut players = load_game_players(repo, &map).await?;
    let mut bombs = load_live_bombs(repo, &players).await?;
    let mut explosions = load_active_explosions(repo, &map).await?;

    let bombs_ticked = bombs.len();
    for bomb in &mut bombs {
        bomb.tick()?;
    }

    let tick_num = map.turn + 1;

    let mut saga = TickSaga::default();
    saga.start(
        format!("tick:{}:{}", game_id, tick_num),
        game_id.to_string(),
        bombs_ticked,
    )?;

    let mut explosion_counter = map.explosions_created;
    let mut kill_attributions = Vec::new();

    for explosion in &mut explosions {
        if explosion.is_fully_expanded() {
            saga.record_dissipation(explosion.entity.id().to_string())?;
            explosion.dissipate()?;
        } else {
            explosion.expand()?;

            let new_cells = explosion.newly_reached_cells().to_vec();
            let damage = apply_damage(&new_cells, &mut map, &mut players, &mut bombs, None)?;
            record_kill_attributions(
                &mut kill_attributions,
                &damage.players_killed,
                &explosion.bomb_id,
                &explosion.owner,
            );

            if damage.has_damage() {
                saga.record_damage(
                    damage.blocks_destroyed,
                    damage.players_killed,
                    damage.chain_detonations,
                )?;
            }
        }
    }

    let mut any_exploded = true;
    while any_exploded {
        any_exploded = false;

        let ready: Vec<usize> = (0..bombs.len())
            .filter(|&i| bombs[i].is_ready_to_explode())
            .collect();

        for idx in ready {
            bombs[idx].explode()?;
            any_exploded = true;

            let rings = calculate_blast_rings(&bombs[idx], &map);
            let bomb_id = bombs[idx].entity.id().to_string();
            let bomb_owner = bombs[idx].owner_id.clone();

            explosion_counter += 1;
            let explosion_id = format!("explosion:{}:{}", game_id, explosion_counter);

            let mut explosion = Explosion::default();
            explosion.start(
                explosion_id.clone(),
                bomb_id.clone(),
                bomb_owner.clone(),
                (bombs[idx].x, bombs[idx].y),
                bombs[idx].blast_radius,
                rings,
            )?;

            let center_cells = explosion.newly_reached_cells().to_vec();
            let damage =
                apply_damage(&center_cells, &mut map, &mut players, &mut bombs, Some(idx))?;
            record_kill_attributions(
                &mut kill_attributions,
                &damage.players_killed,
                &bomb_id,
                &bomb_owner,
            );

            saga.record_detonation(Detonation {
                bomb_id,
                owner: bomb_owner,
                explosion_id,
            })?;

            if damage.has_damage() {
                saga.record_damage(
                    damage.blocks_destroyed,
                    damage.players_killed,
                    damage.chain_detonations,
                )?;
            }

            explosions.push(explosion);
        }
    }

    for bomb in &bombs {
        if bomb.exploded {
            if let Some(player) = players
                .iter_mut()
                .find(|p| p.entity.id() == format!("player:{}", bomb.owner_id))
            {
                player.return_bomb()?;
            }
        }
    }

    let alive_players: Vec<&Player> = players.iter().filter(|p| p.alive).collect();
    let game_over = alive_players.len() <= 1 && players.len() > 1;
    let winner = if game_over {
        alive_players.first().map(|p| p.name.clone())
    } else {
        None
    };

    saga.complete(game_over, winner)?;

    map.record_tick(
        tick_num,
        explosion_counter,
        explosions
            .iter()
            .filter(|explosion| explosion.active)
            .map(|explosion| explosion.entity.id().to_string())
            .collect(),
    )?;

    let mut outbox_messages = Vec::new();
    for killed_id in &saga.players_killed {
        let attribution = kill_attributions
            .iter()
            .find(|attribution| attribution.player_id.as_str() == killed_id.as_str());
        let outbox = OutboxMessage::create(
            format!("player-killed:{}", killed_id),
            "player.killed",
            serde_json::to_vec(&serde_json::json!({
                "player_id": killed_id,
                "killed_by_bomb": attribution
                    .map(|attribution| attribution.bomb_id.as_str())
                    .unwrap_or("unknown"),
                "bomb_owner": attribution
                    .map(|attribution| attribution.bomb_owner.as_str())
                    .unwrap_or("unknown"),
            }))
            .map_err(|err| {
                GameError::Repository(RepositoryError::Model(format!(
                    "outbox payload serialize: {err}"
                )))
            })?,
        )?;
        outbox_messages.push(outbox);
    }

    // Stage every touched aggregate stream under its own type's stream identity.
    // These siblings belong to one parent shard (`tick_shard` / `tick_cell_name`);
    // a cell host writes them to one store. They are not per-player/bomb cells.
    let mut streams: Vec<StreamWrite<'_>> = Vec::new();
    let map_identity = StreamIdentity::new(GameMap::aggregate_type(), map.entity.id())
        .map_err(GameError::Repository)?;
    streams.push(StreamWrite::new(map_identity, map.entity_mut()));
    for player in &mut players {
        let identity = StreamIdentity::new(Player::aggregate_type(), player.entity.id())
            .map_err(GameError::Repository)?;
        streams.push(StreamWrite::new(identity, player.entity_mut()));
    }
    for bomb in &mut bombs {
        let identity = StreamIdentity::new(Bomb::aggregate_type(), bomb.entity.id())
            .map_err(GameError::Repository)?;
        streams.push(StreamWrite::new(identity, bomb.entity_mut()));
    }
    for explosion in &mut explosions {
        let identity = StreamIdentity::new(Explosion::aggregate_type(), explosion.entity.id())
            .map_err(GameError::Repository)?;
        streams.push(StreamWrite::new(identity, explosion.entity_mut()));
    }
    let saga_identity = StreamIdentity::new(TickSaga::aggregate_type(), saga.entity.id())
        .map_err(GameError::Repository)?;
    streams.push(StreamWrite::new(saga_identity, saga.entity_mut()));

    let mut batch = CommitBatch::new(streams);
    batch.outbox_messages = outbox_messages;
    repo.commit_batch(batch).await?;

    Ok(saga)
}

fn record_kill_attributions(
    attributions: &mut Vec<KillAttribution>,
    killed_ids: &[String],
    bomb_id: &str,
    bomb_owner: &str,
) {
    attributions.extend(killed_ids.iter().map(|player_id| KillAttribution {
        player_id: player_id.clone(),
        bomb_id: bomb_id.to_string(),
        bomb_owner: bomb_owner.to_string(),
    }));
}

fn apply_damage(
    cells: &[(i32, i32)],
    map: &mut GameMap,
    players: &mut [Player],
    bombs: &mut [Bomb],
    skip_bomb_idx: Option<usize>,
) -> Result<DamageReport, GameError> {
    let mut report = DamageReport::default();

    for &(cx, cy) in cells {
        if map.is_in_bounds(cx, cy) && *map.tile_at(cx, cy) == Tile::Block {
            map.destroy_block(cx, cy)?;
            report.blocks_destroyed.push((cx, cy));
        }

        for player in players.iter_mut() {
            if player.alive && player.x == cx && player.y == cy {
                player.kill()?;
                report.players_killed.push(player.entity.id().to_string());
            }
        }

        for (i, bomb) in bombs.iter_mut().enumerate() {
            if Some(i) == skip_bomb_idx {
                continue;
            }
            if !bomb.exploded && bomb.x == cx && bomb.y == cy {
                bomb.ticks_remaining = 0;
                report.chain_detonations.push(bomb.entity.id().to_string());
            }
        }
    }

    Ok(report)
}

fn calculate_blast_rings(bomb: &Bomb, map: &GameMap) -> Vec<Vec<(i32, i32)>> {
    let radius = bomb.blast_radius as usize;
    let mut rings: Vec<Vec<(i32, i32)>> = vec![Vec::new(); radius + 1];

    rings[0].push((bomb.x, bomb.y));

    let directions = [
        Direction::North,
        Direction::South,
        Direction::East,
        Direction::West,
    ];

    for dir in &directions {
        let mut cx = bomb.x;
        let mut cy = bomb.y;

        for ring in rings.iter_mut().take(radius + 1).skip(1) {
            let (nx, ny) = dir.apply(cx, cy);

            if !map.is_in_bounds(nx, ny) {
                break;
            }

            match map.tile_at(nx, ny) {
                Tile::Wall => break,
                Tile::Block => {
                    ring.push((nx, ny));
                    break;
                }
                _ => ring.push((nx, ny)),
            }

            cx = nx;
            cy = ny;
        }
    }

    rings
}
