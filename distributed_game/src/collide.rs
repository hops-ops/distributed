//! Static colliders and the one raycast games use for hits.
//!
//! A zone declares its level geometry once, in [`Zone::on_create`] (which
//! runs on every wake): vertical cylinders (columns, trees) and axis-aligned
//! boxes (walls, crates), each extruded from the floor (`z = 0`) to a
//! height.
//!
//! ```ignore
//! ctx.set_colliders(
//!     Colliders::new()
//!         .with(Collider::cylinder([4.0, 2.0], 0.8, 2.4))
//!         .with(Collider::aabb([-20.0, 19.5], [20.0, 20.0], 3.0)),
//! );
//! ```
//!
//! A [`Ray`] then finds the closest hit among the recorded entities
//! ([`Zone::record_rewind`]) and the colliders, with the point and surface
//! normal for placing the impact:
//!
//! ```ignore
//! let ray = Ray::new(muzzle, aim, 100.0).ignoring(shooter.0);
//! if let Some(hit) = ctx.raycast_shot(timing, &ray) {      // lag-compensated
//!     spawn_impact(hit.point, hit.normal);
//!     if let Target::Entity(id) = hit.target { damage(id); }
//! }
//! ```
//!
//! **Slopes and bounces.** [`Collider::ramp`] is a wedge rising along one
//! axis. [`Colliders::ground_at`] gives the top surface under a point (its
//! height and normal), and [`walkable`] says whether that normal is floor:
//! up component above [`WALKABLE_MIN_NZ`], about 45°. [`bounce`] follows a
//! ray, reflecting it ([`reflect`]) off each static surface it meets:
//!
//! ```ignore
//! let trace = bounce(Ray::new(muzzle, aim, 100.0), 4, |r| ctx.raycast_shot(timing, r));
//! for hit in &trace.hits { spark(hit.point, hit.normal); }
//! ```
//!
//! [`Trace::stop_at`] cuts a trace at the first collider on some layers
//! (walls a shot never bounces past), and [`Trace::exit_box`] finds where
//! it leaves the zone's square, for a shot that carries on next door.
//!
//! **Layers.** Every entity and collider sits on a set of layer bits
//! ([`Historic::on_layers`], [`Collider::on_layers`]; [`layer::DEFAULT`]
//! unless set) and a ray only meets those its [`Ray::mask`] shares; a
//! [`Ray::ignoring`] id is never hit (the shooter). Put an entity on no
//! layer (`0`) to make it transparent (a dead mob).
//!
//! Distances are in world units along the ray; `dir` need not be
//! normalised. A ray starting inside a shape hits it at distance 0 with the
//! normal pointing back along the ray. Queries are a linear scan: fine for
//! rooms of tens of shapes and entities, not for large open worlds.
//!
//! [`Zone::on_create`]: crate::Zone::on_create
//! [`Zone::record_rewind`]: crate::Zone::record_rewind
//! [`Historic::on_layers`]: crate::rewind::Historic::on_layers

use crate::rewind::{add, norm, scale, sub, Historic, HitQuery, Vec3};

/// Layer bits.
pub mod layer {
    /// Where entities and colliders sit unless told otherwise.
    pub const DEFAULT: u32 = 1;
    /// Every layer (a ray's default mask).
    pub const ALL: u32 = u32::MAX;
    /// No layer: never hit.
    pub const NONE: u32 = 0;
}

/// A ray: `origin + t·dir` for `t` in `0..=max_dist`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ray {
    /// Start (e.g. the muzzle).
    pub origin: Vec3,
    /// Unit direction (zero for a degenerate ray, which hits nothing).
    pub dir: Vec3,
    /// Maximum distance.
    pub max_dist: f32,
    /// Layers this ray meets.
    pub mask: u32,
    /// Entity never hit (the shooter).
    pub ignore: Option<u32>,
}

impl Ray {
    /// Ray from `origin` along `dir` (need not be normalised) up to
    /// `max_dist`, meeting every layer.
    pub fn new(origin: Vec3, dir: Vec3, max_dist: f32) -> Self {
        let n = norm(dir);
        Self {
            origin,
            dir: if n > 0.0 {
                scale(dir, 1.0 / n)
            } else {
                [0.0; 3]
            },
            max_dist: if n > 0.0 { max_dist.max(0.0) } else { 0.0 },
            mask: layer::ALL,
            ignore: None,
        }
    }

    /// Only meet entities and colliders on these layers.
    pub fn mask(mut self, layers: u32) -> Self {
        self.mask = layers;
        self
    }

    /// Never hit this entity (the shooter).
    pub fn ignoring(mut self, id: u32) -> Self {
        self.ignore = Some(id);
        self
    }

    /// The point at distance `t`.
    pub fn at(&self, t: f32) -> Vec3 {
        add(self.origin, scale(self.dir, t))
    }

    /// The same segment as a [`HitQuery`] (for [`Rewind`](crate::Rewind)
    /// queries that take one).
    pub fn query(&self) -> HitQuery {
        HitQuery::Segment {
            from: self.origin,
            to: self.at(self.max_dist),
        }
    }

    fn meets(&self, layers: u32) -> bool {
        self.mask & layers != 0
    }
}

/// What a ray hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// A recorded entity (its id).
    Entity(u32),
    /// A static collider (its index in [`Colliders`]).
    Static(usize),
    /// The terrain ([`Colliders::with_terrain`]).
    Terrain,
}

/// The closest hit of a [`Ray`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RayHit {
    /// Entity or collider.
    pub target: Target,
    /// Distance from the ray's origin.
    pub distance: f32,
    /// Where the ray entered.
    pub point: Vec3,
    /// Unit surface normal there (facing the ray).
    pub normal: Vec3,
}

/// A surface counts as walkable floor when its normal's up component is
/// above this (`cos 45.57°`).
pub const WALKABLE_MIN_NZ: f32 = 0.7;

/// Whether a surface with this unit normal is floor to walk on.
pub fn walkable(normal: Vec3) -> bool {
    normal[2] > WALKABLE_MIN_NZ
}

/// `v` mirrored about the plane with unit normal `n`: `v − 2(v·n)n`.
pub fn reflect(v: Vec3, n: Vec3) -> Vec3 {
    let d = v[0] * n[0] + v[1] * n[1] + v[2] * n[2];
    sub(v, scale(n, 2.0 * d))
}

/// One piece of level geometry, from the floor (`z = 0`) up to `height`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Collider {
    /// Footprint.
    pub shape: Footprint,
    /// Top (`z`); a ramp's high edge.
    pub height: f32,
    /// A ramp ([`Collider::ramp`]): the way its top climbs from `0` to
    /// `height`. `None`: a flat top.
    pub uphill: Option<Uphill>,
    /// Layer bits ([`layer::DEFAULT`] unless set).
    pub layers: u32,
}

/// The way a ramp climbs (over its rectangle, from one edge to the other).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Uphill {
    /// Low at `min.x`, high at `max.x`.
    PosX,
    /// Low at `max.x`, high at `min.x`.
    NegX,
    /// Low at `min.y`, high at `max.y`.
    PosY,
    /// Low at `max.y`, high at `min.y`.
    NegY,
}

impl Uphill {
    /// The horizontal unit vector it climbs along.
    pub fn dir(self) -> [f32; 2] {
        match self {
            Uphill::PosX => [1.0, 0.0],
            Uphill::NegX => [-1.0, 0.0],
            Uphill::PosY => [0.0, 1.0],
            Uphill::NegY => [0.0, -1.0],
        }
    }
}

/// The top surface under a point ([`Colliders::ground_at`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ground {
    /// What it stands on: a collider ([`Target::Static`]) or the terrain.
    pub target: Target,
    /// Height of the surface there.
    pub height: f32,
    /// Its unit normal (up for a flat top).
    pub normal: Vec3,
}

/// Where a [`bounce`] went.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Trace {
    /// Each surface met, in order; the last is an entity when one was hit.
    pub hits: Vec<RayHit>,
    /// Where it stopped.
    pub end: Vec3,
}

impl Trace {
    /// The entity it stopped at.
    pub fn entity(&self) -> Option<u32> {
        match self.hits.last()?.target {
            Target::Entity(id) => Some(id),
            Target::Static(_) | Target::Terrain => None,
        }
    }

    /// Cut it at the first static hit on a collider sharing `layers` (it
    /// ends there; anything it met after that never happened). For a ray
    /// that bounces off some surfaces but is stopped by others.
    pub fn stop_at(&mut self, colliders: &Colliders, layers: u32) {
        let stop = self.hits.iter().position(|h| match h.target {
            Target::Static(i) => colliders.get(i).is_some_and(|c| c.layers & layers != 0),
            Target::Entity(_) | Target::Terrain => false,
        });
        if let Some(i) = stop {
            self.hits.truncate(i + 1);
            self.end = self.hits[i].point;
        }
    }

    /// Where the path from `origin` through each hit to the end first
    /// leaves the square of half-size `half` centred on the floor origin
    /// (the zone's bounds, for a shot that carries on next door). A leg
    /// that only reaches a side (it stopped at a wall there) does not leave.
    pub fn exit_box(&self, origin: Vec3, half: f32) -> Option<Exit> {
        let points: Vec<Vec3> = std::iter::once(origin)
            .chain(self.hits.iter().map(|h| h.point))
            .chain(std::iter::once(self.end))
            .collect();
        let mut travelled = 0.0f32;
        for (leg, pair) in points.windows(2).enumerate() {
            let (from, to) = (pair[0], pair[1]);
            let len = norm(sub(to, from));
            if len < 1e-4 {
                continue;
            }
            let dir = scale(sub(to, from), 1.0 / len);
            let mut best: Option<(f32, [i32; 2])> = None;
            for (axis, sign) in [(0usize, 1.0f32), (0, -1.0), (1, 1.0), (1, -1.0)] {
                let v = dir[axis] * sign;
                // Going out through this side, from inside.
                if v <= 1e-6 || from[axis] * sign > half {
                    continue;
                }
                let t = (half - from[axis] * sign) / v;
                if t > len - EXIT_EPS {
                    continue;
                }
                let other = from[1 - axis] + dir[1 - axis] * t;
                if other.abs() > half || best.is_some_and(|b| b.0 <= t) {
                    continue;
                }
                let mut side = [0, 0];
                side[axis] = sign as i32;
                best = Some((t, side));
            }
            if let Some((t, side)) = best {
                return Some(Exit {
                    side,
                    point: add(from, scale(dir, t)),
                    dir,
                    travelled: travelled + t,
                    leg,
                });
            }
            travelled += len;
        }
        None
    }
}

/// How far short of a leg's end a side must be for the leg to leave
/// through it ([`Trace::exit_box`]): a leg ending on the side stopped there.
const EXIT_EPS: f32 = 1e-3;

/// Where a [`Trace`] leaves a box ([`Trace::exit_box`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Exit {
    /// The side it leaves by: `[1, 0]` is +x, `[0, -1]` is −y.
    pub side: [i32; 2],
    /// The point on that side.
    pub point: Vec3,
    /// Its unit direction there.
    pub dir: Vec3,
    /// Path length from the origin to `point`.
    pub travelled: f32,
    /// Which leg leaves (0: before any bounce; also the bounces used).
    pub leg: usize,
}

/// [`Heightfield`] sample with no ground: a hole.
pub const HOLE: f32 = f32::NEG_INFINITY;

/// Terrain: heights sampled on a regular grid and bilinear between the
/// samples (rolling hills), with [`HOLE`] samples where there is no
/// ground. A cell with a hole corner is open: rays pass and movers fall.
#[derive(Debug, Clone, PartialEq)]
pub struct Heightfield {
    min: [f32; 2],
    cell: f32,
    nx: usize,
    ny: usize,
    heights: Vec<f32>,
    top: f32,
}

impl Heightfield {
    /// `nx × ny` samples (each ≥ 2), the first at `min`, `cell` apart, of
    /// `height(x, y)`.
    pub fn new(
        min: [f32; 2],
        cell: f32,
        nx: usize,
        ny: usize,
        mut height: impl FnMut(f32, f32) -> f32,
    ) -> Self {
        let mut heights = Vec::with_capacity(nx * ny);
        for j in 0..ny {
            for i in 0..nx {
                heights.push(height(min[0] + i as f32 * cell, min[1] + j as f32 * cell));
            }
        }
        Self::from_samples(min, cell, nx, ny, heights)
    }

    /// From samples, row by row (`y` outer, `x` inner).
    pub fn from_samples(min: [f32; 2], cell: f32, nx: usize, ny: usize, heights: Vec<f32>) -> Self {
        assert!(nx >= 2 && ny >= 2 && heights.len() == nx * ny && cell > 0.0);
        let top = heights.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        Self {
            min,
            cell,
            nx,
            ny,
            heights,
            top,
        }
    }

    /// The first sample's position.
    pub fn min(&self) -> [f32; 2] {
        self.min
    }

    /// Sample spacing.
    pub fn cell(&self) -> f32 {
        self.cell
    }

    /// Samples along x and y.
    pub fn size(&self) -> (usize, usize) {
        (self.nx, self.ny)
    }

    /// Every sample, row by row.
    pub fn samples(&self) -> &[f32] {
        &self.heights
    }

    /// Highest sample.
    pub fn top(&self) -> f32 {
        self.top
    }

    /// Height and unit normal at `[x, y]`; `None` off the grid or over a
    /// hole.
    pub fn height_at(&self, at: [f32; 2]) -> Option<(f32, Vec3)> {
        let fx = (at[0] - self.min[0]) / self.cell;
        let fy = (at[1] - self.min[1]) / self.cell;
        let (lx, ly) = ((self.nx - 1) as f32, (self.ny - 1) as f32);
        if !(0.0..=lx).contains(&fx) || !(0.0..=ly).contains(&fy) {
            return None;
        }
        let i = (fx.floor() as usize).min(self.nx - 2);
        let j = (fy.floor() as usize).min(self.ny - 2);
        let (tx, ty) = (fx - i as f32, fy - j as f32);
        let h = |a: usize, b: usize| self.heights[b * self.nx + a];
        let (h00, h10, h01, h11) = (h(i, j), h(i + 1, j), h(i, j + 1), h(i + 1, j + 1));
        if [h00, h10, h01, h11].contains(&HOLE) {
            return None;
        }
        let height =
            (h00 * (1.0 - tx) + h10 * tx) * (1.0 - ty) + (h01 * (1.0 - tx) + h11 * tx) * ty;
        let dx = ((h10 - h00) * (1.0 - ty) + (h11 - h01) * ty) / self.cell;
        let dy = ((h01 - h00) * (1.0 - tx) + (h11 - h10) * tx) / self.cell;
        let raw = [-dx, -dy, 1.0];
        Some((height, scale(raw, 1.0 / norm(raw))))
    }

    /// Where `ray` first meets the surface (marched half a cell at a time,
    /// then bisected); starting under it = 0.
    pub fn raycast(&self, ray: &Ray) -> Option<(f32, Vec3)> {
        if ray.max_dist <= 0.0 || (ray.dir[2] >= 0.0 && ray.origin[2] > self.top) {
            return None;
        }
        let under = |t: f32| {
            let p = ray.at(t);
            self.height_at([p[0], p[1]]).is_some_and(|(h, _)| p[2] <= h)
        };
        if under(0.0) {
            return Some((0.0, scale(ray.dir, -1.0)));
        }
        let step = self.cell * 0.5;
        let (mut lo, mut t) = (0.0f32, step.min(ray.max_dist));
        loop {
            if under(t) {
                let mut hi = t;
                for _ in 0..14 {
                    let mid = 0.5 * (lo + hi);
                    if under(mid) {
                        hi = mid;
                    } else {
                        lo = mid;
                    }
                }
                let p = ray.at(hi);
                let normal = self
                    .height_at([p[0], p[1]])
                    .map_or([0.0, 0.0, 1.0], |(_, n)| n);
                return Some((hi, normal));
            }
            if t >= ray.max_dist {
                return None;
            }
            lo = t;
            t = (t + step).min(ray.max_dist);
        }
    }
}

/// How far off a surface a bounced ray restarts.
const BOUNCE_LIFT: f32 = 1e-3;

/// Follow `ray` for its whole `max_dist`, reflecting it off each static
/// surface [`cast`] reports (up to `max_bounces` times) and stopping at the
/// first entity. Each leg keeps the ray's mask and ignore.
pub fn bounce(ray: Ray, max_bounces: usize, mut cast: impl FnMut(&Ray) -> Option<RayHit>) -> Trace {
    let mut trace = Trace::default();
    let mut leg = ray;
    loop {
        let Some(hit) = cast(&leg) else {
            trace.end = leg.at(leg.max_dist);
            return trace;
        };
        trace.hits.push(hit);
        trace.end = hit.point;
        if matches!(hit.target, Target::Entity(_)) || trace.hits.len() > max_bounces {
            return trace;
        }
        let left = leg.max_dist - hit.distance;
        if left <= BOUNCE_LIFT {
            return trace;
        }
        leg = Ray {
            origin: add(hit.point, scale(hit.normal, BOUNCE_LIFT)),
            ..Ray::new(hit.point, reflect(leg.dir, hit.normal), left)
        }
        .mask(ray.mask);
        leg.ignore = ray.ignore;
    }
}

/// A collider's footprint on the floor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Footprint {
    /// Circle: a vertical cylinder.
    Circle {
        /// Centre `[x, y]`.
        center: [f32; 2],
        /// Radius.
        radius: f32,
    },
    /// Axis-aligned rectangle: a box.
    Rect {
        /// Lowest `[x, y]`.
        min: [f32; 2],
        /// Highest `[x, y]`.
        max: [f32; 2],
    },
}

impl Collider {
    /// Vertical cylinder (a column, a tree trunk).
    pub fn cylinder(center: [f32; 2], radius: f32, height: f32) -> Self {
        Self {
            shape: Footprint::Circle { center, radius },
            height,
            uphill: None,
            layers: layer::DEFAULT,
        }
    }

    /// Axis-aligned box between two floor corners (a wall, a crate).
    pub fn aabb(a: [f32; 2], b: [f32; 2], height: f32) -> Self {
        Self {
            shape: Footprint::Rect {
                min: [a[0].min(b[0]), a[1].min(b[1])],
                max: [a[0].max(b[0]), a[1].max(b[1])],
            },
            height,
            uphill: None,
            layers: layer::DEFAULT,
        }
    }

    /// A ramp over the rectangle between two floor corners: its top climbs
    /// from the floor at one edge to `height` at the opposite one
    /// (`uphill`), with vertical sides and a vertical high end.
    pub fn ramp(a: [f32; 2], b: [f32; 2], height: f32, uphill: Uphill) -> Self {
        Self {
            uphill: Some(uphill),
            ..Self::aabb(a, b, height)
        }
    }

    /// Height and unit normal of the top at `[x, y]`, if over the footprint.
    pub fn top_at(&self, at: [f32; 2]) -> Option<(f32, Vec3)> {
        match self.shape {
            Footprint::Circle { center, radius } => {
                let (dx, dy) = (at[0] - center[0], at[1] - center[1]);
                (dx * dx + dy * dy <= radius * radius).then_some((self.height, [0.0, 0.0, 1.0]))
            }
            Footprint::Rect { min, max } => {
                if at[0] < min[0] || at[0] > max[0] || at[1] < min[1] || at[1] > max[1] {
                    return None;
                }
                match self.ramp_plane(min, max) {
                    None => Some((self.height, [0.0, 0.0, 1.0])),
                    Some((normal, d)) => {
                        // n·(x, y, z) = d on the top.
                        let z = (d - normal[0] * at[0] - normal[1] * at[1]) / normal[2];
                        Some((z.clamp(0.0, self.height), normal))
                    }
                }
            }
        }
    }

    /// A ramp's top as the plane `n·p = d` (unit `n`, pointing up and
    /// downhill).
    fn ramp_plane(&self, min: [f32; 2], max: [f32; 2]) -> Option<(Vec3, f32)> {
        let uphill = self.uphill?;
        let (low, run) = match uphill {
            Uphill::PosX => (min[0], max[0] - min[0]),
            Uphill::NegX => (max[0], max[0] - min[0]),
            Uphill::PosY => (min[1], max[1] - min[1]),
            Uphill::NegY => (max[1], max[1] - min[1]),
        };
        if run <= 0.0 {
            return None;
        }
        let k = self.height / run;
        let [ux, uy] = uphill.dir();
        // z = k·(u·p − u·low_edge)  ⇔  −k·u·p + z = −k·(u·low_edge).
        let low_along = match uphill {
            Uphill::PosX | Uphill::PosY => low,
            Uphill::NegX | Uphill::NegY => -low,
        };
        let raw = [-k * ux, -k * uy, 1.0];
        let n = norm(raw);
        Some((scale(raw, 1.0 / n), -k * low_along / n))
    }

    /// Put it on these layers.
    pub fn on_layers(mut self, layers: u32) -> Self {
        self.layers = layers;
        self
    }

    /// Entry distance and normal of `ray` (ignoring its mask).
    pub fn raycast(&self, ray: &Ray) -> Option<(f32, Vec3)> {
        if ray.max_dist <= 0.0 {
            return None;
        }
        let (t, normal) = match self.shape {
            Footprint::Circle { center, radius } => {
                ray_cylinder(ray.origin, ray.dir, center, radius, self.height)?
            }
            Footprint::Rect { min, max } => {
                let (lo, hi) = ([min[0], min[1], 0.0], [max[0], max[1], self.height]);
                match self.ramp_plane(min, max) {
                    None => ray_box(ray.origin, ray.dir, lo, hi)?,
                    Some(top) => ray_wedge(ray.origin, ray.dir, lo, hi, top)?,
                }
            }
        };
        (t <= ray.max_dist).then_some((t, normal))
    }

    /// Push a circle of `radius` at `[x, y]` out of this footprint, if it
    /// overlaps (heights ignored: for movers on the floor).
    fn push_out(&self, at: [f32; 2], radius: f32) -> Option<[f32; 2]> {
        match self.shape {
            Footprint::Circle { center, radius: r } => {
                let (ox, oy) = (at[0] - center[0], at[1] - center[1]);
                let d2 = ox * ox + oy * oy;
                let min = radius + r;
                if d2 >= min * min {
                    return None;
                }
                let d = d2.sqrt();
                let (ux, uy) = if d > 1e-4 {
                    (ox / d, oy / d)
                } else {
                    (1.0, 0.0)
                };
                Some([center[0] + ux * min, center[1] + uy * min])
            }
            Footprint::Rect { min, max } => {
                let cx = at[0].clamp(min[0], max[0]);
                let cy = at[1].clamp(min[1], max[1]);
                let (ox, oy) = (at[0] - cx, at[1] - cy);
                let d2 = ox * ox + oy * oy;
                if d2 >= radius * radius {
                    return None;
                }
                if d2 > 1e-8 {
                    let d = d2.sqrt();
                    return Some([cx + ox / d * radius, cy + oy / d * radius]);
                }
                // Centre inside: out through the nearest side.
                let exits = [
                    (at[0] - min[0], [min[0] - radius, at[1]]),
                    (max[0] - at[0], [max[0] + radius, at[1]]),
                    (at[1] - min[1], [at[0], min[1] - radius]),
                    (max[1] - at[1], [at[0], max[1] + radius]),
                ];
                exits
                    .into_iter()
                    .min_by(|a, b| a.0.total_cmp(&b.0))
                    .map(|(_, p)| p)
            }
        }
    }
}

/// A zone's level geometry ([`Ctx::set_colliders`](crate::Ctx::set_colliders)).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Colliders {
    shapes: Vec<Collider>,
    terrain: Option<(Heightfield, u32)>,
}

impl Colliders {
    /// No geometry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a collider (builder).
    pub fn with(mut self, collider: Collider) -> Self {
        self.shapes.push(collider);
        self
    }

    /// Add a collider; returns its index ([`Target::Static`]).
    pub fn push(&mut self, collider: Collider) -> usize {
        self.shapes.push(collider);
        self.shapes.len() - 1
    }

    /// Terrain under everything, on `layers` (builder).
    pub fn with_terrain(mut self, field: Heightfield, layers: u32) -> Self {
        self.terrain = Some((field, layers));
        self
    }

    /// The terrain, if any.
    pub fn terrain(&self) -> Option<&Heightfield> {
        self.terrain.as_ref().map(|(field, _)| field)
    }

    /// The collider at `index`.
    pub fn get(&self, index: usize) -> Option<&Collider> {
        self.shapes.get(index)
    }

    /// Every collider, by index.
    pub fn iter(&self) -> impl Iterator<Item = &Collider> {
        self.shapes.iter()
    }

    /// How many.
    pub fn len(&self) -> usize {
        self.shapes.len()
    }

    /// None at all.
    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }

    /// Closest collider on `ray` (its mask applies).
    pub fn raycast(&self, ray: &Ray) -> Option<RayHit> {
        let mut best: Option<RayHit> = None;
        for (index, collider) in self.shapes.iter().enumerate() {
            if !ray.meets(collider.layers) {
                continue;
            }
            if let Some((distance, normal)) = collider.raycast(ray) {
                if best.is_none_or(|b| distance < b.distance) {
                    best = Some(RayHit {
                        target: Target::Static(index),
                        distance,
                        point: ray.at(distance),
                        normal,
                    });
                }
            }
        }
        if let Some((field, layers)) = &self.terrain {
            if ray.meets(*layers) {
                // Only nearer than the best so far.
                let near = Ray {
                    max_dist: best.map_or(ray.max_dist, |b| b.distance),
                    ..*ray
                };
                if let Some((distance, normal)) = field.raycast(&near) {
                    if best.is_none_or(|b| distance < b.distance) {
                        best = Some(RayHit {
                            target: Target::Terrain,
                            distance,
                            point: ray.at(distance),
                            normal,
                        });
                    }
                }
            }
        }
        best
    }

    /// The highest top surface on `mask` under `[x, y]`.
    pub fn ground_at(&self, at: [f32; 2], mask: u32) -> Option<Ground> {
        let mut best: Option<Ground> = None;
        for (index, collider) in self.shapes.iter().enumerate() {
            if mask & collider.layers == 0 {
                continue;
            }
            if let Some((height, normal)) = collider.top_at(at) {
                if best.is_none_or(|b| height > b.height) {
                    best = Some(Ground {
                        target: Target::Static(index),
                        height,
                        normal,
                    });
                }
            }
        }
        if let Some((field, layers)) = &self.terrain {
            if mask & layers != 0 {
                if let Some((height, normal)) = field.height_at(at) {
                    if best.is_none_or(|b| height > b.height) {
                        best = Some(Ground {
                            target: Target::Terrain,
                            height,
                            normal,
                        });
                    }
                }
            }
        }
        best
    }

    /// Where a circle of `radius` at `[x, y]` ends up once pushed out of
    /// every collider on `mask` it overlaps (a few passes, so it settles
    /// between neighbours). Floor movers only: heights are ignored.
    pub fn push_out(&self, at: [f32; 2], radius: f32, mask: u32) -> [f32; 2] {
        let mut at = at;
        for _ in 0..3 {
            let mut moved = false;
            for collider in self.shapes.iter().filter(|c| mask & c.layers != 0) {
                if let Some(next) = collider.push_out(at, radius) {
                    at = next;
                    moved = true;
                }
            }
            if !moved {
                break;
            }
        }
        at
    }
}

/// Closest of `entities` (their layers, the ray's ignore, `accept`) and
/// `colliders`.
pub(crate) fn raycast(
    entities: &[Historic],
    colliders: &Colliders,
    ray: &Ray,
    mut accept: impl FnMut(u32) -> bool,
) -> Option<RayHit> {
    let mut best = colliders.raycast(ray);
    if ray.max_dist <= 0.0 {
        return best;
    }
    let query = ray.query();
    for e in entities {
        if !ray.meets(e.layers) || ray.ignore == Some(e.id) || !accept(e.id) {
            continue;
        }
        let Some(distance) = crate::rewind::intersect(&query, e) else {
            continue;
        };
        if best.is_none_or(|b| distance < b.distance) {
            let point = ray.at(distance);
            best = Some(RayHit {
                target: Target::Entity(e.id),
                distance,
                point,
                normal: entity_normal(e, point, ray.dir),
            });
        }
    }
    best
}

/// Outward normal of an entity's volume at `point` (back along `dir` when
/// the point is on its axis).
fn entity_normal(e: &Historic, point: Vec3, dir: Vec3) -> Vec3 {
    let (pa, pb) = e.shape.segment(e.pos);
    let axis = crate::rewind::closest_on_segment(point, pa, pb);
    unit_or(sub(point, axis), scale(dir, -1.0))
}

fn unit_or(v: Vec3, fallback: Vec3) -> Vec3 {
    let n = norm(v);
    if n > 1e-6 {
        scale(v, 1.0 / n)
    } else {
        fallback
    }
}

/// Entry of `ro + t·rd` (`rd` unit) into a vertical cylinder standing on
/// the floor: side wall or top cap; inside = 0.
fn ray_cylinder(ro: Vec3, rd: Vec3, c: [f32; 2], r: f32, h: f32) -> Option<(f32, Vec3)> {
    let (ox, oy) = (ro[0] - c[0], ro[1] - c[1]);
    let inside_xy = ox * ox + oy * oy <= r * r;
    if inside_xy && (0.0..=h).contains(&ro[2]) {
        return Some((0.0, scale(rd, -1.0)));
    }
    let mut best: Option<(f32, Vec3)> = None;
    let mut consider = |t: f32, normal: Vec3| {
        if t >= 0.0 && best.is_none_or(|(b, _)| t < b) {
            best = Some((t, normal));
        }
    };
    // Side wall.
    let a = rd[0] * rd[0] + rd[1] * rd[1];
    if a > 1e-12 {
        let b = ox * rd[0] + oy * rd[1];
        let k = ox * ox + oy * oy - r * r;
        let disc = b * b - a * k;
        if disc >= 0.0 {
            let t = (-b - disc.sqrt()) / a;
            let z = ro[2] + rd[2] * t;
            if (0.0..=h).contains(&z) {
                let (px, py) = (ox + rd[0] * t, oy + rd[1] * t);
                consider(t, unit_or([px, py, 0.0], scale(rd, -1.0)));
            }
        }
    }
    // Top cap (from above) and floor cap (from below the floor).
    if rd[2].abs() > 1e-12 {
        for (z, nz) in [(h, 1.0), (0.0, -1.0)] {
            let t = (z - ro[2]) / rd[2];
            let (px, py) = (ox + rd[0] * t, oy + rd[1] * t);
            if rd[2] * nz < 0.0 && px * px + py * py <= r * r {
                consider(t, [0.0, 0.0, nz]);
            }
        }
    }
    best
}

/// Entry of `ro + t·rd` into the box `lo..hi` (slab test); inside = 0.
fn ray_box(ro: Vec3, rd: Vec3, lo: Vec3, hi: Vec3) -> Option<(f32, Vec3)> {
    let inside = (0..3).all(|i| ro[i] >= lo[i] && ro[i] <= hi[i]);
    if inside {
        return Some((0.0, scale(rd, -1.0)));
    }
    let mut t_enter = f32::NEG_INFINITY;
    let mut t_exit = f32::INFINITY;
    let mut normal = [0.0; 3];
    for i in 0..3 {
        if rd[i].abs() < 1e-12 {
            if ro[i] < lo[i] || ro[i] > hi[i] {
                return None;
            }
            continue;
        }
        let (mut t0, mut t1) = ((lo[i] - ro[i]) / rd[i], (hi[i] - ro[i]) / rd[i]);
        let mut side = -1.0;
        // By direction, not by order: a flat box (a floor) has t0 == t1.
        if rd[i] < 0.0 {
            std::mem::swap(&mut t0, &mut t1);
            side = 1.0;
        }
        if t0 > t_enter {
            t_enter = t0;
            normal = [0.0; 3];
            normal[i] = side;
        }
        t_exit = t_exit.min(t1);
        if t_enter > t_exit {
            return None;
        }
    }
    (t_enter >= 0.0).then_some((t_enter, normal))
}

/// Entry of `ro + t·rd` into a box `lo..hi` whose top is cut by the plane
/// `n·p ≤ d` (a ramp): clip the ray by each half-space; inside = 0.
fn ray_wedge(ro: Vec3, rd: Vec3, lo: Vec3, hi: Vec3, top: (Vec3, f32)) -> Option<(f32, Vec3)> {
    let planes = [
        ([-1.0, 0.0, 0.0], -lo[0]),
        ([1.0, 0.0, 0.0], hi[0]),
        ([0.0, -1.0, 0.0], -lo[1]),
        ([0.0, 1.0, 0.0], hi[1]),
        ([0.0, 0.0, -1.0], -lo[2]),
        ([0.0, 0.0, 1.0], hi[2]),
        top,
    ];
    let dot = |a: Vec3, b: Vec3| a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    if planes.iter().all(|&(n, d)| dot(n, ro) <= d) {
        return Some((0.0, scale(rd, -1.0)));
    }
    let mut t_enter = f32::NEG_INFINITY;
    let mut t_exit = f32::INFINITY;
    let mut normal = [0.0; 3];
    for (n, d) in planes {
        let along = dot(n, rd);
        let over = dot(n, ro) - d;
        if along.abs() < 1e-12 {
            if over > 0.0 {
                return None;
            }
            continue;
        }
        let t = -over / along;
        if along < 0.0 {
            if t > t_enter {
                t_enter = t;
                normal = n;
            }
        } else {
            t_exit = t_exit.min(t);
        }
        if t_enter > t_exit {
            return None;
        }
    }
    (t_enter >= 0.0).then_some((t_enter, normal))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-4
    }

    fn close3(a: Vec3, b: Vec3) -> bool {
        (0..3).all(|i| close(a[i], b[i]))
    }

    #[test]
    fn a_ray_meets_a_cylinder_side_and_passes_over_its_top() {
        let column = Colliders::new().with(Collider::cylinder([5.0, 0.0], 1.0, 2.0));
        let hit = column
            .raycast(&Ray::new([0.0, 0.0, 1.0], [1.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert_eq!(hit.target, Target::Static(0));
        assert!(close(hit.distance, 4.0), "{hit:?}");
        assert!(close3(hit.point, [4.0, 0.0, 1.0]));
        assert!(close3(hit.normal, [-1.0, 0.0, 0.0]));
        // Over the top.
        assert!(column
            .raycast(&Ray::new([0.0, 0.0, 2.5], [1.0, 0.0, 0.0], 100.0))
            .is_none());
        // Too short.
        assert!(column
            .raycast(&Ray::new([0.0, 0.0, 1.0], [1.0, 0.0, 0.0], 3.9))
            .is_none());
    }

    #[test]
    fn a_falling_ray_lands_on_the_top_cap() {
        let column = Colliders::new().with(Collider::cylinder([0.0, 0.0], 1.0, 2.0));
        let hit = column
            .raycast(&Ray::new([0.0, 0.0, 5.0], [0.0, 0.0, -1.0], 100.0))
            .unwrap();
        assert!(close(hit.distance, 3.0));
        assert!(close3(hit.normal, [0.0, 0.0, 1.0]));
    }

    #[test]
    fn a_ray_enters_a_box_through_the_face_it_meets() {
        let wall = Colliders::new().with(Collider::aabb([2.0, -1.0], [3.0, 1.0], 3.0));
        let hit = wall
            .raycast(&Ray::new([0.0, 0.0, 1.0], [2.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert!(close(hit.distance, 2.0));
        assert!(close3(hit.normal, [-1.0, 0.0, 0.0]));
        let from_far_side = wall
            .raycast(&Ray::new([6.0, 0.5, 1.0], [-1.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert!(close(from_far_side.distance, 3.0));
        assert!(close3(from_far_side.normal, [1.0, 0.0, 0.0]));
        let onto_top = wall
            .raycast(&Ray::new([2.5, 0.0, 9.0], [0.0, 0.0, -1.0], 100.0))
            .unwrap();
        assert!(close3(onto_top.normal, [0.0, 0.0, 1.0]));
        assert!(wall
            .raycast(&Ray::new([0.0, 5.0, 1.0], [1.0, 0.0, 0.0], 100.0))
            .is_none());
        assert!(wall
            .raycast(&Ray::new([0.0, 0.0, 3.5], [1.0, 0.0, 0.0], 100.0))
            .is_none());
    }

    #[test]
    fn a_flat_box_is_a_floor_from_either_side() {
        let floor = Colliders::new().with(Collider::aabb([-5.0, -5.0], [5.0, 5.0], 0.0));
        let down = floor
            .raycast(&Ray::new([0.0, 0.0, 2.0], [1.0, 0.0, -1.0], 100.0))
            .unwrap();
        assert!(
            close(down.point[2], 0.0) && close(down.point[0], 2.0),
            "{down:?}"
        );
        assert!(close3(down.normal, [0.0, 0.0, 1.0]));
        let up = floor
            .raycast(&Ray::new([0.0, 0.0, -1.0], [0.0, 0.0, 1.0], 100.0))
            .unwrap();
        assert!(close3(up.normal, [0.0, 0.0, -1.0]));
        assert!(floor
            .raycast(&Ray::new([0.0, 0.0, 1.0], [1.0, 0.0, 0.0], 100.0))
            .is_none());
    }

    #[test]
    fn a_ray_starting_inside_hits_at_once() {
        let wall = Colliders::new().with(Collider::aabb([-1.0, -1.0], [1.0, 1.0], 3.0));
        let hit = wall
            .raycast(&Ray::new([0.0, 0.0, 1.0], [1.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert_eq!(hit.distance, 0.0);
        assert!(close3(hit.normal, [-1.0, 0.0, 0.0]));
    }

    #[test]
    fn the_closest_of_walls_and_entities_wins_and_masks_apply() {
        const MOBS: u32 = 1 << 1;
        let walls = Colliders::new()
            .with(Collider::aabb([10.0, -1.0], [11.0, 1.0], 3.0))
            .with(Collider::cylinder([3.0, 0.0], 0.5, 1.0));
        let mob = Historic::sphere(7, [6.0, 0.0, 1.5], 0.5).on_layers(MOBS);
        let shooter = Historic::sphere(1, [0.0, 0.0, 1.5], 0.5);
        let entities = [shooter, mob];
        // Over the short column, the mob before the wall; the shooter's own
        // volume (the ray starts inside it) is skipped.
        let ray = Ray::new([0.0, 0.0, 1.5], [1.0, 0.0, 0.0], 100.0).ignoring(1);
        let hit = raycast(&entities, &walls, &ray, |_| true).unwrap();
        assert_eq!(hit.target, Target::Entity(7));
        assert!(close(hit.distance, 5.5));
        assert!(close3(hit.normal, [-1.0, 0.0, 0.0]));
        // Lower: the column first.
        let low = Ray::new([0.0, 0.0, 0.5], [1.0, 0.0, 0.0], 100.0).ignoring(1);
        assert_eq!(
            raycast(&entities, &walls, &low, |_| true).unwrap().target,
            Target::Static(1)
        );
        // A mask without mobs: the wall.
        let walls_only = ray.mask(layer::DEFAULT);
        assert_eq!(
            raycast(&entities, &walls, &walls_only, |_| true)
                .unwrap()
                .target,
            Target::Static(0)
        );
        // Without the ignore, the shooter is hit at 0.
        let careless = Ray::new([0.0, 0.0, 1.5], [1.0, 0.0, 0.0], 100.0);
        assert_eq!(
            raycast(&entities, &walls, &careless, |_| true)
                .unwrap()
                .target,
            Target::Entity(1)
        );
        // A transparent mob: the wall.
        let ghost = [Historic::sphere(7, [6.0, 0.0, 1.5], 0.5).on_layers(layer::NONE)];
        assert_eq!(
            raycast(&ghost, &walls, &ray, |_| true).unwrap().target,
            Target::Static(0)
        );
    }

    #[test]
    fn a_degenerate_ray_hits_nothing() {
        let walls = Colliders::new().with(Collider::cylinder([0.0, 0.0], 1.0, 2.0));
        let ray = Ray::new([5.0, 0.0, 1.0], [0.0, 0.0, 0.0], 100.0);
        assert!(raycast(
            &[Historic::sphere(1, [5.0, 0.0, 1.0], 1.0)],
            &walls,
            &ray,
            |_| true
        )
        .is_none());
    }

    /// A 30° ramp climbing +x over x 0..√3·2, 2 m high.
    fn ramp30() -> Collider {
        Collider::ramp([0.0, -1.0], [2.0 * 3f32.sqrt(), 1.0], 2.0, Uphill::PosX)
    }

    #[test]
    fn a_ray_meets_a_ramp_on_its_slope_its_side_and_its_high_end() {
        let ramp = Colliders::new().with(ramp30());
        let (s, c) = (0.5f32, 3f32.sqrt() / 2.0);
        // Straight down onto the middle of the slope.
        let down = ramp
            .raycast(&Ray::new([3f32.sqrt(), 0.0, 5.0], [0.0, 0.0, -1.0], 100.0))
            .unwrap();
        assert!(close(down.distance, 4.0), "{down:?}");
        assert!(close3(down.normal, [-s, 0.0, c]), "{down:?}");
        // Along +x, low: into the slope near the low edge.
        let along = ramp
            .raycast(&Ray::new([-1.0, 0.0, 0.5], [1.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert!(close(along.point[0], 0.5 * 3f32.sqrt()), "{along:?}");
        assert!(close3(along.normal, [-s, 0.0, c]));
        // From the side: the vertical face.
        let side = ramp
            .raycast(&Ray::new([3.0, -5.0, 0.5], [0.0, 1.0, 0.0], 100.0))
            .unwrap();
        assert!(close3(side.normal, [0.0, -1.0, 0.0]), "{side:?}");
        // From behind: the vertical high end.
        let back = ramp
            .raycast(&Ray::new([9.0, 0.0, 1.0], [-1.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert!(close3(back.normal, [1.0, 0.0, 0.0]), "{back:?}");
        // Over it, and past its low edge.
        assert!(ramp
            .raycast(&Ray::new([-1.0, 0.0, 2.5], [1.0, 0.0, 0.0], 100.0))
            .is_none());
        assert!(ramp
            .raycast(&Ray::new([-0.5, 0.0, 5.0], [0.0, 0.0, -1.0], 100.0))
            .is_none());
    }

    #[test]
    fn every_way_up_has_its_own_slope() {
        for (uphill, n) in [
            (Uphill::PosX, [-1.0, 0.0]),
            (Uphill::NegX, [1.0, 0.0]),
            (Uphill::PosY, [0.0, -1.0]),
            (Uphill::NegY, [0.0, 1.0]),
        ] {
            let ramp = Collider::ramp([0.0, 0.0], [4.0, 4.0], 4.0, uphill);
            let (h, normal) = ramp.top_at([1.0, 1.0]).unwrap();
            let low = match uphill {
                Uphill::PosX | Uphill::PosY => 1.0,
                Uphill::NegX | Uphill::NegY => 3.0,
            };
            assert!(close(h, low), "{uphill:?} {h}");
            let r = std::f32::consts::FRAC_1_SQRT_2;
            assert!(
                close3(normal, [n[0] * r, n[1] * r, r]),
                "{uphill:?} {normal:?}"
            );
            // The ray straight down agrees with the surface.
            let (t, _) = ramp
                .raycast(&Ray::new([1.0, 1.0, 9.0], [0.0, 0.0, -1.0], 100.0))
                .unwrap();
            assert!(close(9.0 - t, low), "{uphill:?} {t}");
        }
    }

    #[test]
    fn ground_is_the_highest_top_and_walkable_means_under_about_45_degrees() {
        let steep = Collider::ramp([5.0, -1.0], [6.0, 1.0], 2.0, Uphill::PosX); // 63°
        let world = Colliders::new()
            .with(ramp30())
            .with(steep)
            .with(Collider::cylinder([3.0, 0.0], 0.2, 2.5));
        let on_ramp = world.ground_at([1.0, 0.5], layer::ALL).unwrap();
        assert_eq!(on_ramp.target, Target::Static(0));
        assert!(close(on_ramp.height, 1.0 / 3f32.sqrt()), "{on_ramp:?}");
        assert!(walkable(on_ramp.normal));
        let column = world.ground_at([3.0, 0.0], layer::ALL).unwrap();
        assert_eq!((column.target, column.height), (Target::Static(2), 2.5));
        assert!(walkable(column.normal));
        assert!(!walkable(
            world.ground_at([5.5, 0.0], layer::ALL).unwrap().normal
        ));
        assert!(world.ground_at([9.0, 0.0], layer::ALL).is_none());
        assert!(world.ground_at([1.0, 0.5], layer::NONE).is_none());
        // 45.0° is walkable, 46° is not.
        let at = |deg: f32| {
            let t = deg.to_radians().tan();
            Collider::ramp([0.0, 0.0], [1.0, 1.0], t, Uphill::PosY)
                .top_at([0.5, 0.5])
                .unwrap()
                .1
        };
        assert!(walkable(at(45.0)));
        assert!(!walkable(at(46.0)));
    }

    #[test]
    fn reflect_mirrors_about_the_normal() {
        assert!(close3(
            reflect([1.0, -1.0, 0.0], [0.0, 1.0, 0.0]),
            [1.0, 1.0, 0.0]
        ));
        assert!(close3(
            reflect([0.0, 0.0, -2.0], [0.0, 0.0, 1.0]),
            [0.0, 0.0, 2.0]
        ));
    }

    #[test]
    fn a_bounce_reflects_off_walls_and_stops_at_an_entity() {
        // A wall at x = 5 facing -x; a mob on the mirrored line.
        let walls = Colliders::new().with(Collider::aabb([5.0, -50.0], [6.0, 50.0], 3.0));
        let mob = [Historic::sphere(9, [0.0, 10.0, 1.0], 0.5)];
        let ray = Ray::new([0.0, 0.0, 1.0], [1.0, 1.0, 0.0], 100.0);
        let trace = bounce(ray, 4, |r| raycast(&mob, &walls, r, |_| true));
        assert_eq!(trace.hits.len(), 2, "{trace:?}");
        assert_eq!(trace.hits[0].target, Target::Static(0));
        assert!(close3(trace.hits[0].point, [5.0, 5.0, 1.0]));
        assert!(close3(trace.hits[0].normal, [-1.0, 0.0, 0.0]));
        assert_eq!(trace.entity(), Some(9));
        // Nothing but the wall: it flies off for the rest of its length.
        let alone = bounce(ray, 4, |r| walls.raycast(r));
        assert_eq!(alone.hits.len(), 1);
        let left = 100.0 - 50f32.sqrt();
        let far = [5.0 - left * 0.70710677, 5.0 + left * 0.70710677, 1.0];
        assert!(
            (0..3).all(|i| (alone.end[i] - far[i]).abs() < 1e-2),
            "{alone:?}"
        );
        // No bounces allowed: it stops at the wall.
        let once = bounce(ray, 0, |r| walls.raycast(r));
        assert_eq!(once.hits.len(), 1);
        assert!(close3(once.end, [5.0, 5.0, 1.0]));
        // Between two walls it runs out of bounces.
        let corridor = Colliders::new()
            .with(Collider::aabb([5.0, -50.0], [6.0, 50.0], 3.0))
            .with(Collider::aabb([-6.0, -50.0], [-5.0, 50.0], 3.0));
        let pinball = bounce(Ray::new([0.0, 0.0, 1.0], [1.0, 0.1, 0.0], 100.0), 3, |r| {
            corridor.raycast(r)
        });
        assert_eq!(pinball.hits.len(), 4);
        assert_eq!(pinball.end, pinball.hits[3].point);
    }

    /// A hill `1 − (x² + y²)/16` (r 4) on a 20 m grid, a hole at x > 6.
    fn hill() -> Heightfield {
        Heightfield::new([-10.0, -10.0], 0.5, 41, 41, |x, y| {
            if x > 6.0 {
                HOLE
            } else {
                (1.0 - (x * x + y * y) / 16.0).max(0.0)
            }
        })
    }

    #[test]
    fn terrain_has_heights_normals_and_holes() {
        let field = hill();
        let (h, n) = field.height_at([0.0, 0.0]).unwrap();
        assert!(close(h, 1.0) && n[2] > 0.99, "{h} {n:?}");
        // On its flank the normal leans away from the top.
        let (h, n) = field.height_at([2.0, 0.0]).unwrap();
        assert!((h - 0.75).abs() < 0.02, "{h}");
        assert!(n[0] > 0.2 && n[1].abs() < 0.05 && n[2] > 0.9, "{n:?}");
        assert!(walkable(n));
        assert!(field.height_at([8.0, 0.0]).is_none(), "a hole");
        assert!(field.height_at([11.0, 0.0]).is_none(), "off the grid");
        let world = Colliders::new()
            .with(Collider::aabb([-1.0, 5.0], [1.0, 7.0], 1.5))
            .with_terrain(field, layer::DEFAULT);
        assert_eq!(
            world.ground_at([0.0, 0.0], layer::ALL).unwrap().target,
            Target::Terrain
        );
        let platform = world.ground_at([0.0, 6.0], layer::ALL).unwrap();
        assert_eq!((platform.target, platform.height), (Target::Static(0), 1.5));
        assert!(world.ground_at([8.0, 0.0], layer::ALL).is_none());
    }

    #[test]
    fn a_ray_lands_on_a_hill_and_bounces_off_its_flank() {
        let world = Colliders::new().with_terrain(hill(), layer::DEFAULT);
        let down = world
            .raycast(&Ray::new([2.0, 0.0, 5.0], [0.0, 0.0, -1.0], 100.0))
            .unwrap();
        assert_eq!(down.target, Target::Terrain);
        assert!((down.point[2] - 0.75).abs() < 0.02, "{down:?}");
        // Level, low: into the hill's side, thrown up and back.
        let level = world
            .raycast(&Ray::new([-9.0, 0.0, 0.5], [1.0, 0.0, 0.0], 100.0))
            .unwrap();
        assert!(level.point[0] < -2.0 && level.point[0] > -3.2, "{level:?}");
        let out = reflect([1.0, 0.0, 0.0], level.normal);
        assert!(out[0] < 1.0 && out[2] > 0.0, "{out:?}");
        // Over the top, and down the hole.
        assert!(world
            .raycast(&Ray::new([-9.0, 0.0, 1.2], [1.0, 0.0, 0.0], 100.0))
            .is_none());
        assert!(world
            .raycast(&Ray::new([8.0, 0.0, 5.0], [0.0, 0.0, -1.0], 100.0))
            .is_none());
        // A nearer wall still wins.
        let walled = Colliders::new()
            .with(Collider::aabb([-6.0, -1.0], [-5.5, 1.0], 3.0))
            .with_terrain(hill(), layer::DEFAULT);
        assert_eq!(
            walled
                .raycast(&Ray::new([-9.0, 0.0, 0.5], [1.0, 0.0, 0.0], 100.0))
                .unwrap()
                .target,
            Target::Static(0)
        );
    }

    #[test]
    fn movers_are_pushed_out_of_columns_and_boxes() {
        let walls = Colliders::new()
            .with(Collider::cylinder([0.0, 0.0], 1.0, 2.0))
            .with(Collider::aabb([5.0, -1.0], [7.0, 1.0], 2.0));
        let out = walls.push_out([0.5, 0.0], 0.5, layer::ALL);
        assert!(close(out[0], 1.5) && close(out[1], 0.0), "{out:?}");
        let beside = walls.push_out([4.8, 0.0], 0.5, layer::ALL);
        assert!(close(beside[0], 4.5), "{beside:?}");
        let deep = walls.push_out([6.9, 0.2], 0.5, layer::ALL);
        assert!(close(deep[0], 7.5), "{deep:?}");
        let clear = walls.push_out([3.0, 3.0], 0.5, layer::ALL);
        assert_eq!(clear, [3.0, 3.0]);
        assert_eq!(walls.push_out([0.5, 0.0], 0.5, layer::NONE), [0.5, 0.0]);
    }

    fn statics(points: &[Vec3], end: Vec3) -> Trace {
        Trace {
            hits: points
                .iter()
                .enumerate()
                .map(|(i, &point)| RayHit {
                    target: Target::Static(i),
                    distance: 0.0,
                    point,
                    normal: [-1.0, 0.0, 0.0],
                })
                .collect(),
            end,
        }
    }

    #[test]
    fn a_trace_stops_at_the_first_collider_on_the_stop_layers() {
        let colliders = Colliders::new()
            .with(Collider::aabb([0.0, 0.0], [1.0, 1.0], 1.0))
            .with(Collider::aabb([2.0, 0.0], [3.0, 1.0], 1.0).on_layers(1 << 4))
            .with(Collider::aabb([4.0, 0.0], [5.0, 1.0], 1.0));
        let mut trace = statics(&[[1.0, 0.0, 0.5], [2.0, 0.0, 0.5], [4.0, 0.0, 0.5]], [9.0; 3]);
        trace.stop_at(&colliders, 1 << 4);
        assert_eq!(trace.hits.len(), 2);
        assert_eq!(trace.end, [2.0, 0.0, 0.5]);
        // Nothing on those layers: unchanged.
        let mut other = statics(&[[1.0, 0.0, 0.5]], [9.0; 3]);
        other.stop_at(&colliders, 1 << 5);
        assert_eq!(other.end, [9.0; 3]);
    }

    #[test]
    fn a_leg_that_ends_on_a_side_does_not_leave_the_box() {
        let half = 10.0;
        let stopped = statics(&[[half, 8.0, 3.0]], [half, 8.0, 3.0]);
        assert!(stopped.exit_box([0.0, 8.0, 1.5], half).is_none());
        // A hair short of the side in floating point: stays too.
        let short = statics(&[[half - 1e-5, 8.0, 3.0]], [half - 1e-5, 8.0, 3.0]);
        assert!(short.exit_box([0.0, 8.0, 1.5], half).is_none());
    }

    #[test]
    fn a_trace_leaves_by_the_first_side_it_crosses() {
        let half = 10.0;
        let through = statics(&[], [60.0, 0.0, 1.5]);
        let exit = through.exit_box([0.0, 0.0, 1.5], half).expect("leaves +x");
        assert_eq!(exit.side, [1, 0]);
        assert!(close3(exit.point, [half, 0.0, 1.5]));
        assert!(close(exit.travelled, half));
        assert_eq!(exit.leg, 0);
        // Off a wall at x = 5, back out through −x on the second leg.
        let bounced = statics(&[[5.0, 0.0, 1.0]], [-30.0, 0.0, 1.0]);
        let exit = bounced.exit_box([0.0, 0.0, 1.0], half).expect("leaves −x");
        assert_eq!(exit.side, [-1, 0]);
        assert_eq!(exit.leg, 1);
        assert!(close(exit.travelled, 5.0 + 15.0));
        assert!(close3(exit.dir, [-1.0, 0.0, 0.0]));
        // Diagonal through a corner region: the nearer side wins.
        let diagonal = statics(&[], [30.0, 15.0, 0.0]);
        assert_eq!(diagonal.exit_box([0.0, 0.0, 0.0], half).unwrap().side, [1, 0]);
    }
}
