//! Lag compensation: bounded per-entity history and hit tests at the
//! shooter's view time.
//!
//! A client renders remote entities in the past (it interpolates between the
//! last few Deltas). When it fires, it aimed at what it *saw*, not at where
//! targets are on the server now. The host therefore keeps a short ring
//! buffer of entity hit volumes per tick ([`Zone::record_rewind`]) and a zone
//! resolves a shot against that history:
//!
//! ```text
//! view_time = time_of(client_tick) - interp_delay        (client-reported)
//! age       = latest_recorded_time - view_time
//! age       = min(age, max_rewind)                       (anti-cheat clamp)
//! ```
//!
//! `client_tick` is the input's [`SequencedInput::client_tick`]; the
//! interpolation delay is game-defined and travels **inside the game's input
//! payload** (no wire change), e.g. as a `u16` of milliseconds. Whatever the
//! client claims, the rewind never exceeds [`RewindConfig::max_rewind`] nor
//! the recorded history, and never goes into the future.
//!
//! Inside a hook: `ctx.rewind().hit_test_shot(timing, &HitQuery::ray(..), |id| id != me)`.
//!
//! Coordinates are 3D (`[x, y, z]`); 2D games use `z = 0` and spheres.
//! Capsules are vertical (axis along `z`), centred on the entity position.
//!
//! [`Zone::record_rewind`]: crate::Zone::record_rewind
//! [`SequencedInput::client_tick`]: crate::SequencedInput::client_tick

use std::collections::VecDeque;
use std::time::Duration;

/// 3D vector.
pub type Vec3 = [f32; 3];

/// Rewind tuning (part of [`HostConfig`](crate::HostConfig)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RewindConfig {
    /// Furthest a shot may be evaluated in the past (cheat clamp).
    pub max_rewind: Duration,
    /// Hard cap on stored ticks regardless of tick rate.
    pub max_frames: usize,
}

impl Default for RewindConfig {
    fn default() -> Self {
        Self {
            max_rewind: Duration::from_millis(500),
            max_frames: 256,
        }
    }
}

/// Hit volume of a historical entity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Shape {
    /// Sphere centred on the entity position.
    Sphere {
        /// Radius.
        radius: f32,
    },
    /// Vertical capsule centred on the entity position.
    Capsule {
        /// Distance from the centre to each cap centre along `z`.
        half_height: f32,
        /// Radius.
        radius: f32,
    },
}

impl Shape {
    fn radius(&self) -> f32 {
        match *self {
            Self::Sphere { radius } | Self::Capsule { radius, .. } => radius,
        }
    }

    fn segment(&self, pos: Vec3) -> (Vec3, Vec3) {
        match *self {
            Self::Sphere { .. } => (pos, pos),
            Self::Capsule { half_height, .. } => (
                [pos[0], pos[1], pos[2] - half_height],
                [pos[0], pos[1], pos[2] + half_height],
            ),
        }
    }
}

/// One entity's state at one tick.
#[derive(Debug, Clone, PartialEq)]
pub struct Historic {
    /// Entity id (same id space as replication).
    pub id: u32,
    /// Position.
    pub pos: Vec3,
    /// Hit volume.
    pub shape: Shape,
    /// Arbitrary numeric fields (interpolated linearly by [`Rewind::at_time`]).
    pub fields: Vec<f32>,
}

impl Historic {
    /// Entity with a sphere hit volume.
    pub fn sphere(id: u32, pos: Vec3, radius: f32) -> Self {
        Self {
            id,
            pos,
            shape: Shape::Sphere { radius },
            fields: Vec::new(),
        }
    }

    /// Entity with a vertical capsule hit volume.
    pub fn capsule(id: u32, pos: Vec3, half_height: f32, radius: f32) -> Self {
        Self {
            id,
            pos,
            shape: Shape::Capsule {
                half_height,
                radius,
            },
            fields: Vec::new(),
        }
    }

    /// Attach numeric fields.
    pub fn with_fields(mut self, fields: Vec<f32>) -> Self {
        self.fields = fields;
        self
    }
}

/// All recorded entities at one tick.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RewindFrame {
    tick: u64,
    time: Duration,
    entities: Vec<Historic>,
}

impl RewindFrame {
    /// Record an entity (filled by [`Zone::record_rewind`](crate::Zone::record_rewind)).
    pub fn push(&mut self, entity: Historic) {
        self.entities.push(entity);
    }

    /// Tick this frame describes (state after that tick's `on_tick`).
    pub fn tick(&self) -> u64 {
        self.tick
    }

    /// Simulation time of that tick.
    pub fn time(&self) -> Duration {
        self.time
    }

    /// Entities, sorted by id.
    pub fn entities(&self) -> &[Historic] {
        &self.entities
    }

    pub(crate) fn into_entities(self) -> Vec<Historic> {
        self.entities
    }

    /// One entity.
    pub fn get(&self, id: u32) -> Option<&Historic> {
        self.entities
            .binary_search_by_key(&id, |e| e.id)
            .ok()
            .map(|i| &self.entities[i])
    }
}

/// What the client reports with a shot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShotTiming {
    /// Server tick the client's view was based on (input `client_tick`).
    pub client_tick: u32,
    /// How far behind `client_tick` the client was rendering.
    pub interp_delay: Duration,
}

/// A shot time after clamping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedTime {
    /// Simulation time the shot is evaluated at.
    pub time: Duration,
    /// How far behind the latest recorded tick that is.
    pub age: Duration,
    /// Age the client asked for.
    pub requested_age: Duration,
    /// True when the request was limited by `max_rewind` or the history.
    pub clamped: bool,
}

/// Geometric query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HitQuery {
    /// Segment `from → to` (rays are segments with a max distance).
    Segment {
        /// Start.
        from: Vec3,
        /// End.
        to: Vec3,
    },
    /// Sphere (melee, explosion, point-in-radius with `radius = 0`).
    Sphere {
        /// Centre.
        center: Vec3,
        /// Radius.
        radius: f32,
    },
}

impl HitQuery {
    /// Ray from `origin` along `dir` (need not be normalised) up to `max_dist`.
    pub fn ray(origin: Vec3, dir: Vec3, max_dist: f32) -> Self {
        let n = norm(dir);
        let to = if n > 0.0 {
            add(origin, scale(dir, max_dist / n))
        } else {
            origin
        };
        Self::Segment { from: origin, to }
    }

    /// Point-in-radius test.
    pub fn point(at: Vec3, radius: f32) -> Self {
        Self::Sphere { center: at, radius }
    }
}

/// Nearest hit of a query.
#[derive(Debug, Clone, PartialEq)]
pub struct RewindHit {
    /// Entity hit.
    pub id: u32,
    /// Distance along the segment to the entry point (segments), or
    /// centre-to-surface distance (spheres; 0 when overlapping the centre).
    pub distance: f32,
    /// The entity as it was at the evaluated time.
    pub entity: Historic,
}

/// Bounded ring buffer of [`RewindFrame`]s. Reached through
/// [`Ctx::rewind`](crate::Ctx::rewind).
#[derive(Debug, Clone)]
pub struct Rewind {
    config: RewindConfig,
    capacity: usize,
    frames: VecDeque<RewindFrame>,
}

impl Rewind {
    /// History sized for `max_rewind` at `tick_rate` (plus slack), capped by
    /// `max_frames`.
    pub fn new(config: RewindConfig, tick_rate: u32) -> Self {
        let per_tick = 1.0 / f64::from(tick_rate.max(1));
        let needed = (config.max_rewind.as_secs_f64() / per_tick).ceil() as usize + 2;
        Self {
            capacity: needed.clamp(2, config.max_frames.max(2)),
            config,
            frames: VecDeque::new(),
        }
    }

    /// Configuration.
    pub fn config(&self) -> &RewindConfig {
        &self.config
    }

    /// Stored frames bound.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Store a frame (called by the host after each tick). Entities are
    /// sorted by id; frames must arrive in tick order.
    pub fn record(&mut self, tick: u64, time: Duration, mut entities: Vec<Historic>) {
        entities.sort_by_key(|e| e.id);
        if self.frames.len() >= self.capacity {
            self.frames.pop_front();
        }
        self.frames.push_back(RewindFrame {
            tick,
            time,
            entities,
        });
    }

    /// Forget everything (e.g. after a teleport-everything reset).
    pub fn clear(&mut self) {
        self.frames.clear();
    }

    /// Newest frame (the state clients were last sent).
    pub fn latest(&self) -> Option<&RewindFrame> {
        self.frames.back()
    }

    /// Oldest stored frame.
    pub fn oldest(&self) -> Option<&RewindFrame> {
        self.frames.front()
    }

    /// Stored frame count.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// True when nothing has been recorded.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Exact frame for `tick`.
    pub fn at(&self, tick: u64) -> Option<&RewindFrame> {
        let first = self.frames.front()?.tick;
        let index = tick.checked_sub(first)? as usize;
        self.frames
            .get(index)
            .filter(|f| f.tick == tick)
            .or_else(|| self.frames.iter().find(|f| f.tick == tick))
    }

    /// Simulation time of `tick` (32-bit wire tick, unwrapped against the
    /// latest frame). Ticks outside the history are extrapolated with the
    /// latest step; future ticks map to the latest time.
    pub fn time_of(&self, client_tick: u32) -> Option<Duration> {
        let latest = self.frames.back()?;
        let behind = (latest.tick as u32).wrapping_sub(client_tick) as i32;
        if behind <= 0 {
            return Some(latest.time);
        }
        let tick = latest.tick.saturating_sub(behind as u64);
        if let Some(frame) = self.at(tick) {
            return Some(frame.time);
        }
        let step = self.step();
        Some(
            latest
                .time
                .saturating_sub(step.saturating_mul(behind as u32)),
        )
    }

    fn step(&self) -> Duration {
        let n = self.frames.len();
        if n < 2 {
            return Duration::ZERO;
        }
        let (a, b) = (&self.frames[n - 2], &self.frames[n - 1]);
        let ticks = b.tick.saturating_sub(a.tick).max(1) as u32;
        b.time.saturating_sub(a.time) / ticks
    }

    /// Clamp a client's view time: never older than `max_rewind` or the
    /// oldest frame, never newer than the latest frame.
    pub fn resolve(&self, timing: ShotTiming) -> Option<ResolvedTime> {
        let latest = self.frames.back()?.time;
        let oldest = self.frames.front()?.time;
        let wanted = self
            .time_of(timing.client_tick)?
            .saturating_sub(timing.interp_delay);
        let requested_age = latest.saturating_sub(wanted);
        let floor = latest.saturating_sub(self.config.max_rewind).max(oldest);
        let time = wanted.max(floor);
        Some(ResolvedTime {
            time,
            age: latest - time,
            requested_age,
            clamped: time != wanted,
        })
    }

    /// Entities at `time`, interpolated between the surrounding frames
    /// (positions and numeric fields lerp; shape and membership come from
    /// the earlier frame). Clamped to the stored range.
    pub fn at_time(&self, time: Duration) -> Option<Vec<Historic>> {
        let (a, b, alpha) = self.bracket(time)?;
        Some(
            a.entities
                .iter()
                .map(|e| match b.get(e.id) {
                    Some(next) if alpha > 0.0 => lerp_entity(e, next, alpha),
                    _ => e.clone(),
                })
                .collect(),
        )
    }

    /// One entity at `time` (see [`at_time`](Self::at_time)).
    pub fn entity_at(&self, id: u32, time: Duration) -> Option<Historic> {
        let (a, b, alpha) = self.bracket(time)?;
        let e = a.get(id)?;
        Some(match b.get(id) {
            Some(next) if alpha > 0.0 => lerp_entity(e, next, alpha),
            _ => e.clone(),
        })
    }

    fn bracket(&self, time: Duration) -> Option<(&RewindFrame, &RewindFrame, f32)> {
        let first = self.frames.front()?;
        let last = self.frames.back()?;
        if time <= first.time {
            return Some((first, first, 0.0));
        }
        if time >= last.time {
            return Some((last, last, 0.0));
        }
        let index = self.frames.partition_point(|f| f.time <= time);
        let (a, b) = (&self.frames[index - 1], &self.frames[index]);
        let span = (b.time - a.time).as_secs_f32();
        let alpha = if span > 0.0 {
            (time - a.time).as_secs_f32() / span
        } else {
            0.0
        };
        Some((a, b, alpha))
    }

    /// Nearest entity hit by `query` at `time` among those `filter` accepts.
    pub fn hit_test(
        &self,
        time: Duration,
        query: &HitQuery,
        mut filter: impl FnMut(u32) -> bool,
    ) -> Option<RewindHit> {
        let entities = self.at_time(time)?;
        let mut best: Option<RewindHit> = None;
        for e in entities {
            if !filter(e.id) {
                continue;
            }
            if let Some(distance) = intersect(query, &e) {
                if best.as_ref().is_none_or(|b| distance < b.distance) {
                    best = Some(RewindHit {
                        id: e.id,
                        distance,
                        entity: e,
                    });
                }
            }
        }
        best
    }

    /// Every entity overlapping `query` at `time` (area effects).
    pub fn overlaps(&self, time: Duration, query: &HitQuery) -> Vec<RewindHit> {
        let mut hits: Vec<RewindHit> = self
            .at_time(time)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|e| {
                intersect(query, &e).map(|distance| RewindHit {
                    id: e.id,
                    distance,
                    entity: e,
                })
            })
            .collect();
        hits.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        hits
    }

    /// Resolve the client's view time (clamped) and hit-test there.
    pub fn hit_test_shot(
        &self,
        timing: ShotTiming,
        query: &HitQuery,
        filter: impl FnMut(u32) -> bool,
    ) -> Option<(RewindHit, ResolvedTime)> {
        let resolved = self.resolve(timing)?;
        self.hit_test(resolved.time, query, filter)
            .map(|hit| (hit, resolved))
    }
}

fn lerp_entity(a: &Historic, b: &Historic, t: f32) -> Historic {
    Historic {
        id: a.id,
        pos: [
            lerp(a.pos[0], b.pos[0], t),
            lerp(a.pos[1], b.pos[1], t),
            lerp(a.pos[2], b.pos[2], t),
        ],
        shape: a.shape,
        fields: a
            .fields
            .iter()
            .enumerate()
            .map(|(i, v)| b.fields.get(i).map_or(*v, |w| lerp(*v, *w, t)))
            .collect(),
    }
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a + (b - a) * t
}

fn add(a: Vec3, b: Vec3) -> Vec3 {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

fn sub(a: Vec3, b: Vec3) -> Vec3 {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

fn scale(a: Vec3, s: f32) -> Vec3 {
    [a[0] * s, a[1] * s, a[2] * s]
}

fn dot(a: Vec3, b: Vec3) -> f32 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn norm(a: Vec3) -> f32 {
    dot(a, a).sqrt()
}

/// Closest point to `p` on segment `a → b`.
fn closest_on_segment(p: Vec3, a: Vec3, b: Vec3) -> Vec3 {
    let ab = sub(b, a);
    let len2 = dot(ab, ab);
    if len2 <= f32::EPSILON {
        return a;
    }
    let t = (dot(sub(p, a), ab) / len2).clamp(0.0, 1.0);
    add(a, scale(ab, t))
}

/// Entry distance of the ray `ro + t·rd` (`rd` unit) into a sphere, if any
/// with `t >= 0` (0 when starting inside).
fn ray_sphere(ro: Vec3, rd: Vec3, center: Vec3, r: f32) -> Option<f32> {
    let oc = sub(ro, center);
    let c = dot(oc, oc) - r * r;
    if c <= 0.0 {
        return Some(0.0);
    }
    let b = dot(oc, rd);
    let h = b * b - c;
    if h < 0.0 || b > 0.0 {
        return None;
    }
    Some(-b - h.sqrt())
}

/// Entry distance of a ray into a capsule `pa–pb` of radius `r`.
fn ray_capsule(ro: Vec3, rd: Vec3, pa: Vec3, pb: Vec3, r: f32) -> Option<f32> {
    let closest = closest_on_segment(ro, pa, pb);
    if dot(sub(ro, closest), sub(ro, closest)) <= r * r {
        return Some(0.0);
    }
    let ba = sub(pb, pa);
    let oa = sub(ro, pa);
    let baba = dot(ba, ba);
    let bard = dot(ba, rd);
    let baoa = dot(ba, oa);
    let rdoa = dot(rd, oa);
    let oaoa = dot(oa, oa);
    let a = baba - bard * bard;
    let mut best: Option<f32> = None;
    if a.abs() > 1e-9 {
        let b = baba * rdoa - baoa * bard;
        let c = baba * oaoa - baoa * baoa - r * r * baba;
        let h = b * b - a * c;
        if h >= 0.0 {
            let t = (-b - h.sqrt()) / a;
            let y = baoa + t * bard;
            if t >= 0.0 && y > 0.0 && y < baba {
                best = Some(t);
            }
        }
    }
    for cap in [pa, pb] {
        if let Some(t) = ray_sphere(ro, rd, cap, r) {
            best = Some(best.map_or(t, |b: f32| b.min(t)));
        }
    }
    best
}

fn intersect(query: &HitQuery, e: &Historic) -> Option<f32> {
    let r = e.shape.radius();
    let (pa, pb) = e.shape.segment(e.pos);
    match *query {
        HitQuery::Segment { from, to } => {
            let d = sub(to, from);
            let len = norm(d);
            if len <= f32::EPSILON {
                let c = closest_on_segment(from, pa, pb);
                return (norm(sub(from, c)) <= r).then_some(0.0);
            }
            let rd = scale(d, 1.0 / len);
            ray_capsule(from, rd, pa, pb, r).filter(|t| *t <= len)
        }
        HitQuery::Sphere { center, radius } => {
            let c = closest_on_segment(center, pa, pb);
            let d = norm(sub(center, c));
            (d <= r + radius).then_some((d - r).max(0.0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    /// 30 Hz history of a target moving +x at 10 u/s along y = 10.
    fn history(config: RewindConfig, ticks: u64) -> Rewind {
        let mut rw = Rewind::new(config, 30);
        let dt = Duration::from_secs(1) / 30;
        for tick in 1..=ticks {
            let t = dt * tick as u32;
            let x = t.as_secs_f32() * 10.0;
            rw.record(
                tick,
                t,
                vec![
                    Historic::sphere(7, [x, 10.0, 0.0], 0.5).with_fields(vec![x]),
                    Historic::capsule(9, [-50.0, 0.0, 1.0], 0.8, 0.4),
                ],
            );
        }
        rw
    }

    #[test]
    fn ring_buffer_is_bounded_by_max_rewind() {
        let rw = history(RewindConfig::default(), 200);
        // 500 ms at 30 Hz = 15 ticks + 2 slack.
        assert_eq!(rw.capacity(), 17);
        assert_eq!(rw.len(), 17);
        assert_eq!(rw.latest().unwrap().tick(), 200);
        assert_eq!(rw.oldest().unwrap().tick(), 184);
        assert!(rw.at(183).is_none());
        assert_eq!(rw.at(190).unwrap().tick(), 190);
    }

    #[test]
    fn at_time_interpolates_positions_and_fields() {
        let rw = history(RewindConfig::default(), 60);
        // Halfway between tick 50 and 51.
        let t = (Duration::from_secs(1) / 30) * 50 + Duration::from_secs(1) / 60;
        let e = rw.entity_at(7, t).unwrap();
        let expected = t.as_secs_f32() * 10.0;
        assert!(
            (e.pos[0] - expected).abs() < 1e-3,
            "{} vs {expected}",
            e.pos[0]
        );
        assert!((e.fields[0] - expected).abs() < 1e-3);
    }

    #[test]
    fn laggy_shot_misses_now_but_hits_at_view_time() {
        let rw = history(RewindConfig::default(), 60);
        let latest = rw.latest().unwrap().time();
        // Client saw tick 54 rendered 100 ms behind: view = t(54) - 100 ms.
        let timing = ShotTiming {
            client_tick: 54,
            interp_delay: MS(100),
        };
        let view = rw.resolve(timing).unwrap();
        assert!(!view.clamped);
        let seen_x = view.time.as_secs_f32() * 10.0;
        // Shoot straight up (+y) through where the target was on screen.
        let ray = HitQuery::ray([seen_x, 0.0, 0.0], [0.0, 1.0, 0.0], 100.0);
        assert!(
            rw.hit_test(latest, &ray, |_| true).is_none(),
            "present-time target has moved on"
        );
        let (hit, resolved) = rw.hit_test_shot(timing, &ray, |_| true).unwrap();
        assert_eq!(hit.id, 7);
        assert!((hit.distance - 9.5).abs() < 1e-3, "{}", hit.distance);
        assert_eq!(resolved, view);
    }

    #[test]
    fn rewind_beyond_max_is_clamped() {
        let rw = history(
            RewindConfig {
                max_rewind: MS(250),
                max_frames: 1024,
            },
            90,
        );
        let latest = rw.latest().unwrap().time();
        // Cheater claims a 2 s old view to hit where the target was then.
        let timing = ShotTiming {
            client_tick: 90,
            interp_delay: Duration::from_secs(2),
        };
        let resolved = rw.resolve(timing).unwrap();
        assert!(resolved.clamped);
        assert_eq!(resolved.age, MS(250));
        assert_eq!(resolved.requested_age, Duration::from_secs(2));
        let old_x = (latest - Duration::from_secs(2)).as_secs_f32() * 10.0;
        let ray = HitQuery::ray([old_x, 0.0, 0.0], [0.0, 1.0, 0.0], 100.0);
        assert!(rw.hit_test_shot(timing, &ray, |_| true).is_none());
        // Within the clamp the same request style still works.
        let ok_x = (latest - MS(200)).as_secs_f32() * 10.0;
        let ray = HitQuery::ray([ok_x, 0.0, 0.0], [0.0, 1.0, 0.0], 100.0);
        let timing = ShotTiming {
            client_tick: 90,
            interp_delay: MS(200),
        };
        assert_eq!(rw.hit_test_shot(timing, &ray, |_| true).unwrap().0.id, 7);
    }

    #[test]
    fn future_ticks_resolve_to_latest() {
        let rw = history(RewindConfig::default(), 30);
        let timing = ShotTiming {
            client_tick: 40,
            interp_delay: Duration::ZERO,
        };
        let r = rw.resolve(timing).unwrap();
        assert_eq!(r.time, rw.latest().unwrap().time());
        assert_eq!(r.age, Duration::ZERO);
    }

    #[test]
    fn capsule_segment_and_point_queries() {
        let rw = history(RewindConfig::default(), 5);
        let now = rw.latest().unwrap().time();
        // Horizontal shot through the capsule's cylinder.
        let shot = HitQuery::ray([-60.0, 0.0, 1.0], [1.0, 0.0, 0.0], 50.0);
        let hit = rw.hit_test(now, &shot, |id| id == 9).unwrap();
        assert!(
            (hit.distance - (10.0 - 0.4)).abs() < 1e-3,
            "{}",
            hit.distance
        );
        // Through the upper cap: entry = 10 - sqrt(0.4² - 0.2²).
        let cap = HitQuery::ray([-60.0, 0.0, 2.0], [1.0, 0.0, 0.0], 50.0);
        let hit = rw.hit_test(now, &cap, |id| id == 9).unwrap();
        assert!((hit.distance - (10.0 - 0.12f32.sqrt())).abs() < 1e-3);
        // Above the capsule (z = 2.3 > 1.8 + 0.4) misses.
        let over = HitQuery::ray([-60.0, 0.0, 2.3], [1.0, 0.0, 0.0], 50.0);
        assert!(rw.hit_test(now, &over, |id| id == 9).is_none());
        // Too short a segment misses.
        let short = HitQuery::ray([-60.0, 0.0, 1.0], [1.0, 0.0, 0.0], 5.0);
        assert!(rw.hit_test(now, &short, |_| true).is_none());
        // Point-in-radius.
        assert_eq!(
            rw.overlaps(now, &HitQuery::point([-50.2, 0.0, 0.5], 0.0))[0].id,
            9
        );
        assert!(rw
            .overlaps(now, &HitQuery::point([-49.0, 0.0, 0.5], 0.2))
            .is_empty());
        // Filter excludes the shooter.
        assert!(rw.hit_test(now, &shot, |id| id != 9).is_none());
    }
}
