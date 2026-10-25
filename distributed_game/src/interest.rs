//! Interest management: a uniform spatial grid and circular views.
//!
//! Each connected client has a [`View`] (center + radius) on the zone's 2D
//! interest plane (games in 3D usually map x/z). An entity *enters* a view
//! when within `radius` and *leaves* only beyond `radius * (1 + leave_margin)`
//! (hysteresis prevents create/remove flicker at the edge). Entities marked
//! global are visible to everyone regardless of distance.
//!
//! [`UpdateRates`] thin out updates by distance: a known entity far from
//! the view centre is updated only every few patches (creates, removes and
//! global entities are never delayed).

use std::collections::HashMap;

/// A client's area of interest.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct View {
    /// Center x.
    pub x: f32,
    /// Center y.
    pub y: f32,
    /// Enter radius.
    pub radius: f32,
}

impl View {
    /// View at (x, y) with `radius`.
    pub fn new(x: f32, y: f32, radius: f32) -> Self {
        Self { x, y, radius }
    }

    /// Distance from the view center.
    pub fn distance(&self, x: f32, y: f32) -> f32 {
        ((x - self.x).powi(2) + (y - self.y).powi(2)).sqrt()
    }
}

/// Grid, hysteresis and update-rate settings.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InterestConfig {
    /// Grid cell edge length (world units); ~view radius / 2 works well.
    pub cell_size: f32,
    /// Extra fraction of the radius before a known entity leaves the view.
    pub leave_margin: f32,
    /// Distance-based update rates ([`UpdateRates::every_patch`] turns them
    /// off).
    pub update_rates: UpdateRates,
}

impl Default for InterestConfig {
    fn default() -> Self {
        Self {
            cell_size: 16.0,
            leave_margin: 0.1,
            update_rates: UpdateRates::default(),
        }
    }
}

/// Distance-based update rates (level of detail), as fractions of the view
/// radius. A changed entity the client already has is an update candidate
/// every patch within `near × radius`, every `mid_every`-th patch within
/// `mid × radius` and every `far_every`-th patch beyond. Which patches is
/// staggered by entity id, so a far entity's latest state arrives at most
/// `far_every - 1` patches late (budget permitting; keep `mid_every` a
/// divisor of `far_every`). Creates, removes, global entities and kinds
/// marked `#[replicated(.., every_patch)]` are never delayed. The default
/// is `near = 0.5`, `mid = 0.8`, every 2nd and every 4th patch: at 20
/// patches/s, 10 Hz past half the radius and 5 Hz near its edge.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UpdateRates {
    /// Fraction of the radius updated every patch.
    pub near: f32,
    /// Fraction of the radius updated every `mid_every`-th patch.
    pub mid: f32,
    /// Patch interval between `near` and `mid` (1 = every patch).
    pub mid_every: u32,
    /// Patch interval beyond `mid` (1 = every patch).
    pub far_every: u32,
}

impl UpdateRates {
    /// Every entity every patch (no distance-based rates).
    pub fn every_patch() -> Self {
        Self {
            near: f32::INFINITY,
            mid: f32::INFINITY,
            mid_every: 1,
            far_every: 1,
        }
    }

    /// Patch interval at `distance` from a view of `radius`.
    pub fn interval(&self, distance: f32, radius: f32) -> u32 {
        if distance <= self.near * radius {
            1
        } else if distance <= self.mid * radius {
            self.mid_every.max(1)
        } else {
            self.far_every.max(1)
        }
    }
}

impl Default for UpdateRates {
    fn default() -> Self {
        Self {
            near: 0.5,
            mid: 0.8,
            mid_every: 2,
            far_every: 4,
        }
    }
}

/// Uniform grid of entity indices.
#[derive(Debug, Clone)]
pub struct SpatialGrid {
    cell: f32,
    cells: HashMap<(i32, i32), Vec<u32>>,
}

impl SpatialGrid {
    /// Empty grid with `cell_size` (clamped to > 0).
    pub fn new(cell_size: f32) -> Self {
        Self {
            cell: if cell_size > 0.0 { cell_size } else { 1.0 },
            cells: HashMap::new(),
        }
    }

    fn key(&self, x: f32, y: f32) -> (i32, i32) {
        (
            (x / self.cell).floor() as i32,
            (y / self.cell).floor() as i32,
        )
    }

    /// Remove everything.
    pub fn clear(&mut self) {
        self.cells.clear();
    }

    /// Index an item at (x, y).
    pub fn insert(&mut self, item: u32, x: f32, y: f32) {
        let key = self.key(x, y);
        self.cells.entry(key).or_default().push(item);
    }

    /// Items in cells overlapping the circle's bounding box (caller filters
    /// by exact distance).
    pub fn query(&self, x: f32, y: f32, radius: f32, out: &mut Vec<u32>) {
        let r = radius.max(0.0);
        let (x0, y0) = self.key(x - r, y - r);
        let (x1, y1) = self.key(x + r, y + r);
        // Large views degrade to scanning occupied cells.
        let span = (i64::from(x1) - i64::from(x0) + 1) * (i64::from(y1) - i64::from(y0) + 1);
        if span > self.cells.len() as i64 {
            for (&(cx, cy), items) in &self.cells {
                if (x0..=x1).contains(&cx) && (y0..=y1).contains(&cy) {
                    out.extend_from_slice(items);
                }
            }
            return;
        }
        for cx in x0..=x1 {
            for cy in y0..=y1 {
                if let Some(items) = self.cells.get(&(cx, cy)) {
                    out.extend_from_slice(items);
                }
            }
        }
    }
}
