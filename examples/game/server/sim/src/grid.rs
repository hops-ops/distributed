//! The seeded world generation (from the Endless Grid demo): hashing,
//! value noise, biomes and props of 32 m cells. Mob Pit's rooms build on
//! it ([`crate::mobpit::pit_room`]: a room's biome); it is a pure function
//! of `(world seed, cell x, cell y, CONTENT_VERSION)`, compiled natively
//! and to WebAssembly.
//!
//! Rules that keep them identical:
//! integer hashing on `u32` only; floats are binary32 using only
//! `+ - * /`, `sqrt` and `floor` (see `distributed_game::prediction`).
//!
//! Layout: a cell is [`CELL`] m square with local coordinates `0..CELL` on
//! both axes; cell `(cx, cy)` covers world `cx*CELL .. (cx+1)*CELL`. Each
//! cell is split into 4×4 sub-cells of 8 m holding at most one prop, kept
//! [`PROP_MARGIN`] m away from the sub-cell edges: every line where x or y
//! is a multiple of 8 m is a free path ("trail"), and props never reach
//! across a cell border.

use distributed_game::codec::{Wire, WireType};
use distributed_game::protocol::{DecodeError, Reader, Writer};
use distributed_game::GameRecord;

/// Cell edge length (m).
pub const CELL: f32 = 32.0;
/// Bumped whenever generation changes (part of every hash).
pub const CONTENT_VERSION: u32 = 1;
/// Default world seed.
pub const DEFAULT_SEED: u32 = 0x00C0_FFEE;
/// Sub-cells per cell axis.
pub const SUB: i32 = 4;
/// Sub-cell edge length (m).
pub const SUB_SIZE: f32 = 8.0;
/// Prop distance from sub-cell edges (m).
pub const PROP_MARGIN: f32 = 2.0;
/// Prop placement span inside a sub-cell: `SUB_SIZE - 2 * PROP_MARGIN`.
pub const PROP_SPAN: f32 = 4.0;
/// Walker radius (m).
pub const PLAYER_RADIUS: f32 = 0.45;

/// Biomes, by index.
pub const BIOMES: [&str; 4] = ["highlands", "steppe", "forest", "meadow"];
const PROP_DENSITY: [f32; 4] = [0.4375, 0.1875, 0.75, 0.3125];
const SALT_HEIGHT: [u32; 3] = [1, 2, 3];
const SALT_MOISTURE: u32 = 7;
const SALT_PROP: u32 = 100;

/// Prop kinds (on the wire: their `u8` index into [`PROP_KINDS`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PropKind {
    /// Solid trunk, big canopy (cosmetic).
    Tree = 0,
    /// Solid boulder.
    Rock = 1,
    /// Walk-through shrub.
    Bush = 2,
}

/// Names of the [`PropKind`]s, by index.
pub const PROP_KINDS: [&str; 3] = ["tree", "rock", "bush"];

impl Wire for PropKind {
    fn wire_type() -> WireType {
        WireType::U8
    }
    fn encode(&self, w: &mut Writer) {
        w.u8(*self as u8);
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match r.u8()? {
            0 => Ok(Self::Tree),
            1 => Ok(Self::Rock),
            2 => Ok(Self::Bush),
            _ => Err(DecodeError::Invalid("unknown prop kind")),
        }
    }
}

/// One static prop in cell-local coordinates.
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Prop {
    /// Kind.
    pub kind: PropKind,
    /// Local x.
    pub x: f32,
    /// Local y.
    pub y: f32,
    /// Collision (trunk / boulder) radius.
    pub r: f32,
    /// Blocks walkers.
    pub solid: bool,
    /// Cosmetic variation (0..=255).
    pub variant: u8,
}

/// lowbias32 integer hash.
pub fn hash32(mut x: u32) -> u32 {
    x ^= x >> 16;
    x = x.wrapping_mul(0x7feb_352d);
    x ^= x >> 15;
    x = x.wrapping_mul(0x846c_a68b);
    x ^= x >> 16;
    x
}

/// Hash of `(seed, salt, a, b)` including [`CONTENT_VERSION`].
pub fn mix(seed: u32, salt: u32, a: i32, b: i32) -> u32 {
    let mut h = hash32(seed ^ hash32(salt.wrapping_add(CONTENT_VERSION.wrapping_mul(0x9e37_79b9))));
    h = hash32(h ^ a as u32);
    hash32(h.wrapping_add((b as u32).wrapping_mul(0x85eb_ca6b)))
}

/// Top 24 bits as an exact binary32 in `[0, 1)`.
pub fn unit(h: u32) -> f32 {
    (h >> 8) as f32 * (1.0 / 16_777_216.0)
}

/// Lattice index and fraction along one axis for local coordinate `l`
/// (`0..=CELL`) of cell `c` and lattice `spacing` (divides 32 or is a
/// multiple of 32). Integer-based so a cell border samples identically
/// from both sides.
fn lattice(c: i32, l: f32, spacing: i32) -> (i32, f32) {
    let s = spacing as f32;
    if spacing <= CELL as i32 {
        let per = CELL as i32 / spacing;
        let k = ((l / s).floor() as i32).clamp(0, per);
        let frac = (l - (k * spacing) as f32) / s;
        (c.wrapping_mul(per).wrapping_add(k), frac)
    } else {
        let n = spacing / CELL as i32;
        let mut i = c.div_euclid(n);
        let mut off = (c - i * n) as f32 * CELL + l;
        if off >= s {
            // The far border is the next lattice point at fraction 0.
            i = i.wrapping_add(1);
            off -= s;
        }
        (i, off / s)
    }
}

fn smooth(t: f32) -> f32 {
    t * t * (3.0 - 2.0 * t)
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a + (b - a) * t
}

/// Value noise in `[0, 1)` with lattice `spacing` (m).
pub fn noise(seed: u32, salt: u32, cell: (i32, i32), l: (f32, f32), spacing: i32) -> f32 {
    let (ix, fx) = lattice(cell.0, l.0, spacing);
    let (iy, fy) = lattice(cell.1, l.1, spacing);
    let v = |dx: i32, dy: i32| unit(mix(seed, salt, ix.wrapping_add(dx), iy.wrapping_add(dy)));
    let (sx, sy) = (smooth(fx), smooth(fy));
    lerp(lerp(v(0, 0), v(1, 0), sx), lerp(v(0, 1), v(1, 1), sx), sy)
}

/// Terrain height in `[0, 1)` (three octaves; the client draws `4 m × h`).
pub fn height(seed: u32, cell: (i32, i32), l: (f32, f32)) -> f32 {
    noise(seed, SALT_HEIGHT[0], cell, l, 64) * 0.5625
        + noise(seed, SALT_HEIGHT[1], cell, l, 16) * 0.3125
        + noise(seed, SALT_HEIGHT[2], cell, l, 8) * 0.125
}

/// Moisture in `[0, 1)` (one broad octave).
pub fn moisture(seed: u32, cell: (i32, i32), l: (f32, f32)) -> f32 {
    noise(seed, SALT_MOISTURE, cell, l, 128)
}

/// Biome index into [`BIOMES`].
pub fn biome(seed: u32, cell: (i32, i32), l: (f32, f32)) -> u8 {
    biome_of(height(seed, cell, l), moisture(seed, cell, l))
}

/// Biome of a point with height `h` and moisture `m`.
pub fn biome_of(h: f32, m: f32) -> u8 {
    if h > 0.6875 {
        0
    } else if m < 0.375 {
        1
    } else if m > 0.625 {
        2
    } else {
        3
    }
}

/// The props of a cell, sub-cell by sub-cell (row-major).
pub fn cell_props(seed: u32, cell: (i32, i32)) -> Vec<Prop> {
    let mut props = Vec::new();
    for j in 0..SUB {
        for i in 0..SUB {
            let gx = cell.0.wrapping_mul(SUB).wrapping_add(i);
            let gy = cell.1.wrapping_mul(SUB).wrapping_add(j);
            let h0 = mix(seed, SALT_PROP, gx, gy);
            let center = (
                i as f32 * SUB_SIZE + SUB_SIZE / 2.0,
                j as f32 * SUB_SIZE + SUB_SIZE / 2.0,
            );
            let b = biome(seed, cell, center);
            if unit(h0) >= PROP_DENSITY[b as usize] {
                continue;
            }
            let h1 = hash32(h0 ^ 0x68e3_1da4);
            let h2 = hash32(h1 ^ 0xb529_7a4d);
            let h3 = hash32(h2 ^ 0x1b56_c4e9);
            let h4 = hash32(h3 ^ 0x2545_f491);
            let x = i as f32 * SUB_SIZE + PROP_MARGIN + unit(h1) * PROP_SPAN;
            let y = j as f32 * SUB_SIZE + PROP_MARGIN + unit(h2) * PROP_SPAN;
            let k = unit(h3);
            let kind = match b {
                0 if k < 0.8125 => PropKind::Rock,
                0 => PropKind::Tree,
                1 if k < 0.5 => PropKind::Rock,
                1 => PropKind::Bush,
                2 if k < 0.8125 => PropKind::Tree,
                2 if k < 0.875 => PropKind::Rock,
                2 => PropKind::Bush,
                _ if k < 0.375 => PropKind::Tree,
                _ if k < 0.5625 => PropKind::Rock,
                _ => PropKind::Bush,
            };
            let u = unit(h4);
            let r = match kind {
                PropKind::Tree => 0.5 + u * 0.3125,
                PropKind::Rock => 0.625 + u * 0.375,
                PropKind::Bush => 0.5 + u * 0.25,
            };
            props.push(Prop {
                kind,
                x,
                y,
                r,
                solid: kind != PropKind::Bush,
                variant: (h4 >> 24) as u8,
            });
        }
    }
    props
}

/// Push a circle of radius `radius` at `(x, y)` out of every solid prop.
pub fn collide(mut x: f32, mut y: f32, radius: f32, props: &[Prop]) -> (f32, f32) {
    for p in props.iter().filter(|p| p.solid) {
        let ox = x - p.x;
        let oy = y - p.y;
        let d2 = ox * ox + oy * oy;
        let min = radius + p.r;
        if d2 < min * min {
            let d = d2.sqrt();
            let (nx, ny) = if d > 1e-6 {
                (ox / d, oy / d)
            } else {
                (1.0, 0.0)
            };
            x = p.x + nx * min;
            y = p.y + ny * min;
        }
    }
    (x, y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borders_match_and_trails_stay_free() {
        let seed = DEFAULT_SEED;
        for cx in -3..3 {
            for cy in -3..3 {
                // The shared edge samples the same lattice from both cells.
                for k in 0..=16 {
                    let l = k as f32 * 2.0;
                    assert_eq!(
                        height(seed, (cx, cy), (32.0, l)),
                        height(seed, (cx + 1, cy), (0.0, l))
                    );
                    assert_eq!(
                        height(seed, (cx, cy), (l, 32.0)),
                        height(seed, (cx, cy + 1), (l, 0.0))
                    );
                }
                for p in cell_props(seed, (cx, cy)) {
                    let reach = if p.solid { p.r + PLAYER_RADIUS } else { 0.0 };
                    for line in [0.0, 8.0, 16.0, 24.0, 32.0] {
                        assert!((p.x - line).abs() > reach && (p.y - line).abs() > reach);
                    }
                    // Props keep clear of the cell border.
                    let clear = 0.5 + PLAYER_RADIUS;
                    assert!(p.x - p.r >= clear && p.x + p.r <= CELL - clear);
                    assert!(p.y - p.r >= clear && p.y + p.r <= CELL - clear);
                }
            }
        }
    }
}
