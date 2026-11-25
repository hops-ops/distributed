use distributed::{sourced, Entity, SourcedResult};

use super::types::{PowerUp, Tile};

pub type ParsedMap = (usize, usize, Vec<Vec<Tile>>, Vec<(i32, i32)>);

#[derive(Default)]
pub struct GameMap {
    pub entity: Entity,
    pub width: usize,
    pub height: usize,
    pub tiles: Vec<Vec<Tile>>,
    pub spawn_points: Vec<(i32, i32)>,
    pub power_ups: Vec<((i32, i32), PowerUp)>,
}

#[sourced(entity)]
impl GameMap {
    #[event("initialized")]
    pub fn create(
        &mut self,
        id: String,
        width: usize,
        height: usize,
        tiles: Vec<Vec<Tile>>,
        spawn_points: Vec<(i32, i32)>,
    ) {
        self.entity.set_id(&id);
        self.width = width;
        self.height = height;
        self.tiles = tiles;
        self.spawn_points = spawn_points;
        self.power_ups = vec![];
    }

    #[event("block_destroyed", when = self.tile_at(x, y) == &Tile::Block)]
    pub fn destroy_block(&mut self, x: i32, y: i32) {
        self.tiles[y as usize][x as usize] = Tile::Floor;
        // 50% chance to reveal a power-up based on position parity
        if (x + y) % 2 == 0 {
            self.power_ups.push(((x, y), PowerUp::BombUp));
        } else {
            self.power_ups.push(((x, y), PowerUp::FireUp));
        }
    }

    pub fn collect_power_up(&mut self, x: i32, y: i32) -> SourcedResult<Option<PowerUp>> {
        let Some(power_up) = self
            .power_ups
            .iter()
            .find(|((px, py), _)| *px == x && *py == y)
            .map(|(_, power_up)| power_up.clone())
        else {
            return Ok(None);
        };

        self.record_power_up_collected(x, y, power_up.clone())?;
        Ok(Some(power_up))
    }

    #[event("power_up_collected")]
    fn record_power_up_collected(&mut self, x: i32, y: i32, power_up: PowerUp) {
        if let Some(idx) = self
            .power_ups
            .iter()
            .position(|((px, py), existing)| *px == x && *py == y && existing == &power_up)
        {
            self.power_ups.remove(idx);
        }
    }

    pub fn is_passable(&self, x: i32, y: i32) -> bool {
        if !self.is_in_bounds(x, y) {
            return false;
        }
        matches!(
            self.tiles[y as usize][x as usize],
            Tile::Floor | Tile::Spawn
        )
    }

    pub fn is_in_bounds(&self, x: i32, y: i32) -> bool {
        x >= 0 && y >= 0 && (x as usize) < self.width && (y as usize) < self.height
    }

    pub fn tile_at(&self, x: i32, y: i32) -> &Tile {
        &self.tiles[y as usize][x as usize]
    }

    /// Parse an ASCII map into tiles and spawn points.
    ///
    /// Legend:
    /// - `#` = Wall
    /// - `.` = Block (destructible)
    /// - ` ` = Floor
    /// - `1`-`4` = Spawn points (stored as Floor tiles)
    pub fn from_ascii(ascii: &str) -> ParsedMap {
        let lines: Vec<&str> = ascii.lines().filter(|l| !l.is_empty()).collect();
        let height = lines.len();
        let width = lines.iter().map(|l| l.len()).max().unwrap_or(0);

        let mut tiles = Vec::with_capacity(height);
        let mut spawn_points: Vec<Option<(i32, i32)>> = vec![None; 4];

        for (y, line) in lines.iter().enumerate() {
            let mut row = Vec::with_capacity(width);
            for (x, ch) in line.chars().enumerate() {
                match ch {
                    '#' => row.push(Tile::Wall),
                    '.' => row.push(Tile::Block),
                    '1' => {
                        row.push(Tile::Spawn);
                        spawn_points[0] = Some((x as i32, y as i32));
                    }
                    '2' => {
                        row.push(Tile::Spawn);
                        spawn_points[1] = Some((x as i32, y as i32));
                    }
                    '3' => {
                        row.push(Tile::Spawn);
                        spawn_points[2] = Some((x as i32, y as i32));
                    }
                    '4' => {
                        row.push(Tile::Spawn);
                        spawn_points[3] = Some((x as i32, y as i32));
                    }
                    _ => row.push(Tile::Floor),
                }
            }
            // Pad row to width
            while row.len() < width {
                row.push(Tile::Floor);
            }
            tiles.push(row);
        }

        let spawns: Vec<(i32, i32)> = spawn_points.into_iter().flatten().collect();
        (width, height, tiles, spawns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_power_up_without_match_does_not_record_event() {
        let mut map = GameMap::default();

        let collected = map.collect_power_up(1, 1).unwrap();

        assert_eq!(collected, None);
        assert!(map.entity.events().is_empty());
    }

    #[test]
    fn collect_power_up_with_match_records_event() {
        let mut map = GameMap::default();
        map.power_ups.push(((1, 1), PowerUp::BombUp));

        let collected = map.collect_power_up(1, 1).unwrap();

        assert_eq!(collected, Some(PowerUp::BombUp));
        assert_eq!(map.entity.events().len(), 1);
        assert_eq!(map.entity.events()[0].event_name, "power_up_collected");
        assert_eq!(
            map.entity.events()[0]
                .decode::<(i32, i32, PowerUp)>()
                .unwrap(),
            (1, 1, PowerUp::BombUp)
        );
    }
}
