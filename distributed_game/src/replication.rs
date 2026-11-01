//! Per-client delta replication with interest and a byte budget.
//!
//! Each patch the zone describes its replicated entities into a
//! [`WorldView`]. Field values are encoded once per patch; every client then
//! gets a Delta computed against **what was last sent to that client**
//! (valid because the transport is reliable and ordered):
//!
//! - entities entering the client's [`View`] → *create* (all fields);
//! - known entities with changed encoded fields → *update* (bitmask + fields);
//! - known entities gone or out of view → *remove* (id only, never budgeted).
//!
//! Creates and updates compete for the per-client byte budget. Each candidate
//! gets `priority = kind.priority × entity.priority × max(field priority) ×
//! 1/(1 + distance/radius)` added to a per-entity **accumulator**; candidates
//! are sent in accumulator order until the budget is spent and sent entities
//! reset to zero. Starved entities keep accumulating, so everything is
//! eventually sent. At least one candidate is sent per patch even if it alone
//! exceeds the budget. Change detection compares encoded bytes, so
//! sub-quantum jitter never costs bandwidth.
//!
//! **Encode once.** Each distinct encoded state of an entity is a shared
//! *revision*; a client remembers the revision it was sent, not a copy. A
//! world built with [`EncodedWorld::next`] carries, per changed entity, the
//! update record from its previous state, so a client that is current costs
//! a revision compare and a copy. Clients with an older baseline (deferred
//! by the budget or by update rates) share one record per baseline, encoded
//! on first use in the patch. The bytes are those of encoding per client.
//!
//! **Update rates.** A changed entity the client already has is an update
//! candidate only every [`UpdateRates::interval`]-th patch for its distance
//! (staggered by id) unless it is global or its kind is `every_patch`; held
//! entities keep their accumulated priority. Creates and removes are never
//! held.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

#[cfg(doc)]
use crate::interest::UpdateRates;
use crate::interest::{InterestConfig, SpatialGrid, View};
use crate::protocol::{tag, Writer};
use crate::schema::{EntitySchema, FieldValue};

/// One replicated entity as described by the zone for this patch.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityState {
    /// Game-assigned id (unique within the zone).
    pub id: u32,
    /// Kind id from the schema.
    pub kind: u16,
    /// Interest-plane x.
    pub x: f32,
    /// Interest-plane y.
    pub y: f32,
    /// Priority multiplier (default 1).
    pub priority: f32,
    /// Visible to every client regardless of view.
    pub global: bool,
    /// Field values in schema order (missing trailing fields encode as zero).
    pub fields: Vec<FieldValue>,
}

impl EntityState {
    /// Spatial entity at (x, y).
    pub fn new(id: u32, kind: u16, x: f32, y: f32, fields: Vec<FieldValue>) -> Self {
        Self {
            id,
            kind,
            x,
            y,
            priority: 1.0,
            global: false,
            fields,
        }
    }

    /// Mark visible to everyone.
    pub fn global(mut self) -> Self {
        self.global = true;
        self
    }

    /// Set the priority multiplier.
    pub fn priority(mut self, p: f32) -> Self {
        self.priority = p;
        self
    }
}

/// Collector the zone fills in `Zone::replicate`.
#[derive(Debug, Clone, Default)]
pub struct WorldView {
    entities: Vec<EntityState>,
}

impl WorldView {
    /// Add an entity.
    pub fn push(&mut self, entity: EntityState) {
        self.entities.push(entity);
    }

    /// Entities pushed so far.
    pub fn entities(&self) -> &[EntityState] {
        &self.entities
    }

    pub(crate) fn last_mut(&mut self) -> Option<&mut EntityState> {
        self.entities.last_mut()
    }
}

/// Multiplicative hasher for the integer keys of the per-client maps, hashed
/// once per visible entity per client per patch (ids are assigned by the
/// game, not chosen by clients).
#[derive(Debug, Default, Clone, Copy)]
struct IdHasher(u64);

impl IdHasher {
    const K: u64 = 0x517c_c1b7_2722_0a95;

    fn mix(&mut self, n: u64) {
        self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(Self::K);
    }
}

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.mix(u64::from(*b));
        }
    }

    fn write_u32(&mut self, n: u32) {
        self.mix(u64::from(n));
    }

    fn write_u64(&mut self, n: u64) {
        self.mix(n);
    }

    fn write_usize(&mut self, n: usize) {
        self.mix(n as u64);
    }
}

type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdHasher>>;

/// Revisions are unique across all worlds: equal revisions are equal bytes,
/// whichever world or replica they came from.
static NEXT_REV: AtomicU64 = AtomicU64::new(1);

/// One encoded state of an entity, shared by the worlds it is unchanged in
/// and by every client it was sent to.
#[derive(Debug)]
struct Payload {
    rev: u64,
    data: Vec<u8>,
    offsets: Vec<usize>,
    /// The create record: `varuint id, varuint kind, data`.
    create: Vec<u8>,
}

impl Payload {
    fn new(id: u32, kind: u16, data: Vec<u8>, offsets: Vec<usize>) -> Self {
        let mut create = Writer::new();
        create.varuint(id).varuint(u32::from(kind)).raw(&data);
        Self {
            rev: NEXT_REV.fetch_add(1, Ordering::Relaxed),
            data,
            offsets,
            create: create.into_bytes(),
        }
    }

    fn field(&self, index: usize) -> &[u8] {
        &self.data[self.offsets[index]..self.offsets[index + 1]]
    }

    /// Mask of the first `fields` fields whose bytes differ from `old`.
    fn changed_since(&self, old: &Payload, fields: usize) -> u32 {
        let mut mask = 0u32;
        for i in 0..fields {
            if old.field(i) != self.field(i) {
                mask |= 1 << i;
            }
        }
        mask
    }

    /// The update record `varuint id, varuint mask, masked fields`.
    fn update(&self, id: u32, mask: u32, w: &mut Writer) {
        w.varuint(id).varuint(mask);
        for i in 0..self.offsets.len() - 1 {
            if mask & (1 << i) != 0 {
                w.raw(self.field(i));
            }
        }
    }
}

/// An entity's update record from revision `base` to its current one.
#[derive(Debug)]
struct SharedUpdate {
    base: u64,
    mask: u32,
    record: Vec<u8>,
}

#[derive(Debug, Clone)]
struct EncodedEntity {
    id: u32,
    kind: u16,
    x: f32,
    y: f32,
    priority: f32,
    global: bool,
    payload: Arc<Payload>,
    /// From its state in the previous world (kept while unchanged).
    update: Option<Arc<SharedUpdate>>,
}

/// Update records for older baselines: `(entity index, baseline rev)` →
/// `(mask, record range)`, encoded on first use in a patch.
#[derive(Debug, Default)]
struct UpdateCache {
    records: IdMap<(u32, u64), (u32, Range<usize>)>,
    bytes: Vec<u8>,
}

/// A patch's encoded world, shared by every client.
#[derive(Debug)]
pub struct EncodedWorld {
    entities: Vec<EncodedEntity>,
    by_id: IdMap<u32, usize>,
    grid: SpatialGrid,
    globals: Vec<u32>,
    patch: u32,
    /// Records encoded building this world (changed entities' payloads and
    /// shared updates).
    encoded: usize,
    cache: Mutex<UpdateCache>,
}

impl Clone for EncodedWorld {
    fn clone(&self) -> Self {
        Self {
            entities: self.entities.clone(),
            by_id: self.by_id.clone(),
            grid: self.grid.clone(),
            globals: self.globals.clone(),
            patch: self.patch,
            encoded: self.encoded,
            cache: Mutex::default(),
        }
    }
}

impl EncodedWorld {
    /// Encode `view` against `schema` as the first patch. Entities of
    /// unknown kinds or duplicate ids are skipped.
    pub fn build(schema: &EntitySchema, view: WorldView, interest: &InterestConfig) -> Self {
        Self::encode(schema, view, interest, None)
    }

    /// Encode the patch after `self`: unchanged entities keep their
    /// revision, changed ones get one update record from their state here,
    /// shared by every client that has it. Update rates count patches along
    /// this chain, so build every patch after the first with `next`.
    pub fn next(&self, schema: &EntitySchema, view: WorldView, interest: &InterestConfig) -> Self {
        Self::encode(schema, view, interest, Some(self))
    }

    fn encode(
        schema: &EntitySchema,
        view: WorldView,
        interest: &InterestConfig,
        previous: Option<&EncodedWorld>,
    ) -> Self {
        let mut world = Self {
            entities: Vec::with_capacity(view.entities.len()),
            by_id: IdMap::default(),
            grid: SpatialGrid::new(interest.cell_size),
            globals: Vec::new(),
            patch: previous.map_or(0, |p| p.patch.wrapping_add(1)),
            encoded: 0,
            cache: Mutex::default(),
        };
        let zero = FieldValue::U(0);
        for e in view.entities {
            let Some(kind) = schema.get(e.kind) else {
                continue;
            };
            if world.by_id.contains_key(&e.id) {
                continue;
            }
            let mut w = Writer::new();
            let mut offsets = Vec::with_capacity(kind.fields.len() + 1);
            for (index, def) in kind.fields.iter().enumerate() {
                offsets.push(w.len());
                def.ty.encode(&mut w, e.fields.get(index).unwrap_or(&zero));
            }
            offsets.push(w.len());
            let data = w.into_bytes();
            let before = previous
                .and_then(|p| p.by_id.get(&e.id).map(|&i| &p.entities[i]))
                .filter(|p| p.kind == e.kind && p.payload.offsets.len() == offsets.len());
            let (payload, update) = match before {
                Some(p) if p.payload.data == data && p.payload.offsets == offsets => {
                    (p.payload.clone(), p.update.clone())
                }
                _ => {
                    let payload = Arc::new(Payload::new(e.id, e.kind, data, offsets));
                    world.encoded += 1 + usize::from(before.is_some());
                    let update = before.map(|p| {
                        let mask = payload.changed_since(&p.payload, kind.fields.len());
                        let mut w = Writer::new();
                        payload.update(e.id, mask, &mut w);
                        Arc::new(SharedUpdate {
                            base: p.payload.rev,
                            mask,
                            record: w.into_bytes(),
                        })
                    });
                    (payload, update)
                }
            };
            let index = world.entities.len() as u32;
            if e.global {
                world.globals.push(index);
            } else {
                world.grid.insert(index, e.x, e.y);
            }
            world.by_id.insert(e.id, index as usize);
            world.entities.push(EncodedEntity {
                id: e.id,
                kind: e.kind,
                x: e.x,
                y: e.y,
                priority: e.priority,
                global: e.global,
                payload,
                update,
            });
        }
        world
    }

    /// Entity count.
    pub fn len(&self) -> usize {
        self.entities.len()
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    /// Entity records encoded for this patch so far: one payload (with its
    /// create record) and one shared update per changed entity, plus the
    /// update records for older baselines built for clients. Encoding per
    /// client would cost one per record sent or deferred instead.
    pub fn encodes(&self) -> usize {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        self.encoded + cache.records.len()
    }
}

/// Counters from one Delta build.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeltaStats {
    /// Creates sent.
    pub creates: usize,
    /// Updates sent.
    pub updates: usize,
    /// Removes sent.
    pub removes: usize,
    /// Candidates deferred by the budget.
    pub deferred: usize,
    /// Changed known entities held back by the update rates this patch.
    pub held: usize,
    /// Create+update section bytes (what the budget limits).
    pub budget_bytes: usize,
}

/// A visible or known entity of one client.
#[derive(Debug, Clone, Default)]
struct Slot {
    /// What it was last sent.
    known: Option<Arc<Payload>>,
    accum: f32,
    /// Build that last saw it visible.
    seen: u32,
}

/// What one client has been sent.
#[derive(Debug, Clone, Default)]
pub struct ClientReplica {
    slots: IdMap<u32, Slot>,
    builds: u32,
}

enum Candidate {
    Create,
    Shared,
    Cached(Range<usize>),
}

impl ClientReplica {
    /// Fresh replica (client knows nothing).
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget everything (e.g. after reconnect the client gets fresh creates).
    pub fn reset(&mut self) {
        self.slots.clear();
    }

    /// Ids the client currently knows.
    pub fn known_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.slots
            .iter()
            .filter(|(_, slot)| slot.known.is_some())
            .map(|(id, _)| *id)
    }

    /// Build this client's Delta frame and update the last-sent state.
    #[allow(clippy::too_many_arguments)]
    pub fn build_delta(
        &mut self,
        schema: &EntitySchema,
        world: &EncodedWorld,
        interest: &InterestConfig,
        view: &View,
        budget: usize,
        server_tick: u32,
        ack_seq: u32,
    ) -> (Vec<u8>, DeltaStats) {
        self.builds = self.builds.wrapping_add(1);
        let stamp = self.builds;
        let radius = view.radius.max(0.0);
        let stay_radius = radius * (1.0 + interest.leave_margin.max(0.0));
        let mut hits = Vec::new();
        world.grid.query(view.x, view.y, stay_radius, &mut hits);
        hits.extend_from_slice(&world.globals);
        let mut cache = world.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let cache = &mut *cache;

        // Visible entities (indices are unique) become candidates with
        // accumulated priority.
        let mut visible = 0usize;
        let mut held = 0usize;
        let mut candidates: Vec<(usize, Candidate, f32)> = Vec::new();
        for index in hits {
            let index = index as usize;
            let e = &world.entities[index];
            let d = if e.global {
                0.0
            } else {
                view.distance(e.x, e.y)
            };
            let slot = match self.slots.entry(e.id) {
                Entry::Occupied(slot) => {
                    let limit = if slot.get().known.is_some() {
                        stay_radius
                    } else {
                        radius
                    };
                    if !e.global && d > limit {
                        continue;
                    }
                    slot.into_mut()
                }
                Entry::Vacant(slot) => {
                    if !e.global && d > radius {
                        continue;
                    }
                    slot.insert(Slot::default())
                }
            };
            slot.seen = stamp;
            visible += 1;
            let Some(kind) = schema.get(e.kind) else {
                slot.accum = 0.0;
                continue;
            };
            let (candidate, field_prio) = match &slot.known {
                None => (
                    Candidate::Create,
                    kind.fields
                        .iter()
                        .map(|f| f.priority)
                        .fold(1.0_f32, f32::max),
                ),
                Some(known) => {
                    if known.rev == e.payload.rev {
                        slot.accum = 0.0;
                        continue;
                    }
                    if !e.global && !kind.every_patch {
                        let every = interest.update_rates.interval(d, radius);
                        if every > 1 && !world.patch.wrapping_add(e.id).is_multiple_of(every) {
                            // Held: keeps its accumulated priority.
                            held += 1;
                            continue;
                        }
                    }
                    let fields = kind.fields.len();
                    let (mask, candidate) = match &e.update {
                        Some(update)
                            if update.base == known.rev
                                && fields + 1 == e.payload.offsets.len() =>
                        {
                            (update.mask, Candidate::Shared)
                        }
                        _ => {
                            let (mask, range) = cache
                                .records
                                .entry((index as u32, known.rev))
                                .or_insert_with(|| {
                                    let mask = e.payload.changed_since(known, fields);
                                    let start = cache.bytes.len();
                                    let mut w = Writer::new();
                                    e.payload.update(e.id, mask, &mut w);
                                    cache.bytes.extend_from_slice(w.as_bytes());
                                    (mask, start..cache.bytes.len())
                                })
                                .clone();
                            (mask, Candidate::Cached(range))
                        }
                    };
                    if mask == 0 {
                        slot.accum = 0.0;
                        continue;
                    }
                    let prio = kind
                        .fields
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| mask & (1 << i) != 0)
                        .fold(0.0_f32, |p, (_, def)| p.max(def.priority));
                    (candidate, prio)
                }
            };
            let distance_factor = 1.0 / (1.0 + d / radius.max(f32::EPSILON));
            let gain = (kind.priority * e.priority * field_prio * distance_factor).max(1e-6);
            slot.accum += gain;
            candidates.push((index, candidate, slot.accum));
        }

        // Known entities gone or out of view.
        let mut removes: Vec<u32> = Vec::new();
        if visible != self.slots.len() {
            self.slots.retain(|id, slot| {
                if slot.seen != stamp && slot.known.is_some() {
                    removes.push(*id);
                }
                slot.seen == stamp
            });
            removes.sort_unstable();
        }
        candidates.sort_by(|a, b| {
            b.2.total_cmp(&a.2)
                .then(world.entities[a.0].id.cmp(&world.entities[b.0].id))
        });

        let mut stats = DeltaStats {
            removes: removes.len(),
            held,
            ..DeltaStats::default()
        };
        let mut creates = Writer::new();
        let mut updates = Writer::new();
        for (index, candidate, _) in candidates {
            let e = &world.entities[index];
            let record = match &candidate {
                Candidate::Create => &e.payload.create[..],
                Candidate::Shared => &e.update.as_ref().expect("shared update").record[..],
                Candidate::Cached(range) => &cache.bytes[range.clone()],
            };
            let used = creates.len() + updates.len();
            if used > 0 && used + record.len() > budget {
                stats.deferred += 1;
                continue;
            }
            if let Candidate::Create = candidate {
                creates.raw(record);
                stats.creates += 1;
            } else {
                updates.raw(record);
                stats.updates += 1;
            }
            let slot = self.slots.get_mut(&e.id).expect("candidate slot");
            slot.accum = 0.0;
            slot.known = Some(e.payload.clone());
        }
        stats.budget_bytes = creates.len() + updates.len();

        let mut frame = Writer::new();
        frame
            .u8(tag::DELTA)
            .varuint(server_tick)
            .varuint(ack_seq)
            .varuint(removes.len() as u32);
        for id in &removes {
            frame.varuint(*id);
        }
        frame
            .varuint(stats.creates as u32)
            .raw(creates.as_bytes())
            .varuint(stats.updates as u32)
            .raw(updates.as_bytes());
        (frame.into_bytes(), stats)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::interest::UpdateRates;
    use crate::rng::ZoneRng;
    use crate::schema::{FieldType, KindDef};

    struct Known {
        rev: u64,
        data: Vec<u8>,
        offsets: Vec<usize>,
    }

    enum RefCandidate {
        Create,
        Update(u32),
    }

    /// The per-client path encode-once replaced (each client compares,
    /// encodes and copies every entity itself) with the same update-rate
    /// hold: the reference for byte equality, and "before" in the bench.
    #[derive(Default)]
    struct Reference {
        known: HashMap<u32, Known>,
        accum: HashMap<u32, f32>,
    }

    impl Reference {
        fn reset(&mut self) {
            self.known.clear();
            self.accum.clear();
        }

        #[allow(clippy::too_many_arguments)]
        fn build_delta(
            &mut self,
            schema: &EntitySchema,
            world: &EncodedWorld,
            interest: &InterestConfig,
            view: &View,
            budget: usize,
            server_tick: u32,
            ack_seq: u32,
        ) -> (Vec<u8>, DeltaStats) {
            let radius = view.radius.max(0.0);
            let stay_radius = radius * (1.0 + interest.leave_margin.max(0.0));
            let mut hits = Vec::new();
            world.grid.query(view.x, view.y, stay_radius, &mut hits);
            hits.extend_from_slice(&world.globals);

            let mut visible: Vec<(usize, f32)> = Vec::new();
            let mut seen = HashSet::new();
            for index in hits {
                let index = index as usize;
                let e = &world.entities[index];
                if !seen.insert(e.id) {
                    continue;
                }
                let d = if e.global {
                    0.0
                } else {
                    view.distance(e.x, e.y)
                };
                let limit = if self.known.contains_key(&e.id) {
                    stay_radius
                } else {
                    radius
                };
                if e.global || d <= limit {
                    visible.push((index, d));
                }
            }

            let visible_ids: HashSet<u32> =
                visible.iter().map(|(i, _)| world.entities[*i].id).collect();
            let mut removes: Vec<u32> = self
                .known
                .keys()
                .copied()
                .filter(|id| !visible_ids.contains(id))
                .collect();
            removes.sort_unstable();
            for id in &removes {
                self.known.remove(id);
            }

            let mut held = HashSet::new();
            let mut candidates: Vec<(usize, RefCandidate, f32)> = Vec::new();
            for (index, d) in visible {
                let e = &world.entities[index];
                let p = &e.payload;
                let Some(kind) = schema.get(e.kind) else {
                    continue;
                };
                let (candidate, field_prio) = match self.known.get(&e.id) {
                    None => (
                        RefCandidate::Create,
                        kind.fields
                            .iter()
                            .map(|f| f.priority)
                            .fold(1.0_f32, f32::max),
                    ),
                    Some(known) => {
                        let every = interest.update_rates.interval(d, radius);
                        if known.rev != p.rev
                            && !e.global
                            && !kind.every_patch
                            && every > 1
                            && !world.patch.wrapping_add(e.id).is_multiple_of(every)
                        {
                            held.insert(e.id);
                            continue;
                        }
                        let mut mask = 0u32;
                        let mut prio = 0.0_f32;
                        for (i, def) in kind.fields.iter().enumerate() {
                            let old = &known.data[known.offsets[i]..known.offsets[i + 1]];
                            if old != p.field(i) {
                                mask |= 1 << i;
                                prio = prio.max(def.priority);
                            }
                        }
                        if mask == 0 {
                            continue;
                        }
                        (RefCandidate::Update(mask), prio)
                    }
                };
                let distance_factor = 1.0 / (1.0 + d / radius.max(f32::EPSILON));
                let gain = (kind.priority * e.priority * field_prio * distance_factor).max(1e-6);
                let accum = self.accum.entry(e.id).or_insert(0.0);
                *accum += gain;
                candidates.push((index, candidate, *accum));
            }
            let candidate_ids: HashSet<u32> = candidates
                .iter()
                .map(|(i, _, _)| world.entities[*i].id)
                .collect();
            self.accum
                .retain(|id, _| candidate_ids.contains(id) || held.contains(id));
            candidates.sort_by(|a, b| {
                b.2.total_cmp(&a.2)
                    .then(world.entities[a.0].id.cmp(&world.entities[b.0].id))
            });

            let mut stats = DeltaStats {
                removes: removes.len(),
                held: held.len(),
                ..DeltaStats::default()
            };
            let mut creates = Writer::new();
            let mut updates = Writer::new();
            let mut scratch = Writer::new();
            for (index, candidate, _) in candidates {
                let e = &world.entities[index];
                let p = &e.payload;
                scratch.clear();
                match candidate {
                    RefCandidate::Create => {
                        scratch
                            .varuint(e.id)
                            .varuint(u32::from(e.kind))
                            .raw(&p.data);
                    }
                    RefCandidate::Update(mask) => {
                        scratch.varuint(e.id).varuint(mask);
                        for i in 0..p.offsets.len() - 1 {
                            if mask & (1 << i) != 0 {
                                scratch.raw(p.field(i));
                            }
                        }
                    }
                }
                let used = creates.len() + updates.len();
                if used > 0 && used + scratch.len() > budget {
                    stats.deferred += 1;
                    continue;
                }
                match candidate {
                    RefCandidate::Create => {
                        creates.raw(scratch.as_bytes());
                        stats.creates += 1;
                    }
                    RefCandidate::Update(_) => {
                        updates.raw(scratch.as_bytes());
                        stats.updates += 1;
                    }
                }
                self.accum.remove(&e.id);
                self.known.insert(
                    e.id,
                    Known {
                        rev: p.rev,
                        data: p.data.clone(),
                        offsets: p.offsets.clone(),
                    },
                );
            }
            stats.budget_bytes = creates.len() + updates.len();

            let mut frame = Writer::new();
            frame
                .u8(tag::DELTA)
                .varuint(server_tick)
                .varuint(ack_seq)
                .varuint(removes.len() as u32);
            for id in &removes {
                frame.varuint(*id);
            }
            frame
                .varuint(stats.creates as u32)
                .raw(creates.as_bytes())
                .varuint(stats.updates as u32)
                .raw(updates.as_bytes());
            (frame.into_bytes(), stats)
        }
    }

    fn schema() -> EntitySchema {
        let pos = FieldType::QuantF32 {
            min: -200.0,
            max: 200.0,
            bits: 14,
        };
        EntitySchema::new()
            .kind(
                KindDef::new(1, "thing")
                    .field("x", pos)
                    .field("y", pos)
                    .field_with_priority("hp", FieldType::U16, 3.0)
                    .field("name", FieldType::ShortStr)
                    .field("up", FieldType::Bool),
            )
            .kind(
                KindDef::new(2, "tag")
                    .field("x", FieldType::F32)
                    .field("y", FieldType::F32)
                    .field_with_priority("label", FieldType::ShortStr, 0.5)
                    .priority(0.5),
            )
            .kind(
                KindDef::new(3, "beacon")
                    .field("x", pos)
                    .field("y", pos)
                    .field("blob", FieldType::Bytes)
                    .priority(2.0)
                    .every_patch(),
            )
    }

    /// Random entity pool evolving patch by patch: moves, small value sets
    /// (so fields revert), spawns, despawns, duplicates and unknown kinds.
    struct Pool {
        rng: ZoneRng,
        alive: Vec<Option<EntityState>>,
    }

    impl Pool {
        fn new(seed: u64, n: usize) -> Self {
            Self {
                rng: ZoneRng::from_seed(seed),
                alive: vec![None; n],
            }
        }

        fn fields(rng: &mut ZoneRng, kind: u16, x: f32, y: f32) -> Vec<FieldValue> {
            let names = ["", "a", "bc", "def"];
            match kind {
                1 => vec![
                    FieldValue::F(x),
                    FieldValue::F(y),
                    FieldValue::U(rng.range_u32(0, 4) * 50),
                    FieldValue::Str(names[rng.range_u32(0, 4) as usize].into()),
                    FieldValue::Bool(rng.chance(0.5)),
                ],
                2 => vec![
                    FieldValue::F(x),
                    FieldValue::F(y),
                    FieldValue::Str(names[rng.range_u32(0, 4) as usize].into()),
                ],
                _ => vec![
                    FieldValue::F(x),
                    FieldValue::F(y),
                    FieldValue::Bytes(vec![1; rng.range_u32(0, 3) as usize]),
                ],
            }
        }

        fn step(&mut self) -> WorldView {
            let rng = &mut self.rng;
            for (i, slot) in self.alive.iter_mut().enumerate() {
                let id = i as u32 + 1;
                match slot {
                    None if rng.chance(0.2) => {
                        // A reused id keeps its kind: a kind change under a
                        // known id is outside the delta model.
                        let kind = 1 + (id % 3) as u16;
                        let (x, y) = (rng.next_f32() * 160.0 - 80.0, rng.next_f32() * 160.0 - 80.0);
                        let mut e = EntityState::new(id, kind, x, y, Self::fields(rng, kind, x, y))
                            .priority(0.5 + rng.next_f32() * 2.0);
                        e.global = id.is_multiple_of(9);
                        *slot = Some(e);
                    }
                    Some(_) if rng.chance(0.03) => *slot = None,
                    Some(e) => {
                        if rng.chance(0.6) {
                            e.x = (e.x + rng.next_f32() * 6.0 - 3.0).clamp(-100.0, 100.0);
                            e.y = (e.y + rng.next_f32() * 6.0 - 3.0).clamp(-100.0, 100.0);
                            e.fields[0] = FieldValue::F(e.x);
                            e.fields[1] = FieldValue::F(e.y);
                        }
                        if rng.chance(0.2) {
                            let fresh = Self::fields(rng, e.kind, e.x, e.y);
                            let i = 2 + rng.range_u32(0, fresh.len() as u32 - 2) as usize;
                            e.fields[i] = fresh[i].clone();
                        }
                    }
                    None => {}
                }
            }
            let mut view = WorldView::default();
            for e in self.alive.iter().flatten() {
                view.push(e.clone());
            }
            if rng.chance(0.3) {
                if let Some(e) = self.alive.iter().flatten().next() {
                    let mut dup = e.clone();
                    dup.x += 1.0;
                    view.push(dup);
                }
            }
            if rng.chance(0.2) {
                view.push(EntityState::new(999, 9, 0.0, 0.0, vec![]));
            }
            view
        }
    }

    struct Pair {
        new: ClientReplica,
        old: Reference,
        view: View,
        budget: usize,
    }

    fn compare(seed: u64, rates: UpdateRates) {
        let schema = schema();
        let interest = InterestConfig {
            cell_size: 12.0,
            update_rates: rates,
            ..InterestConfig::default()
        };
        let mut pool = Pool::new(seed, 50);
        let mut rng = ZoneRng::from_seed(seed ^ 0x5eed);
        let budgets = [12, 40, 160, 600, 4096, 1 << 20];
        let mut pairs: Vec<Pair> = (0..6)
            .map(|_| Pair {
                new: ClientReplica::new(),
                old: Reference::default(),
                view: View::new(0.0, 0.0, 20.0 + rng.next_f32() * 60.0),
                budget: budgets[rng.range_u32(0, 6) as usize],
            })
            .collect();
        let mut world: Option<EncodedWorld> = None;
        for patch in 0..150u32 {
            let view = pool.step();
            let next = match &world {
                // Sometimes a fresh chain: no shared updates, patch 0.
                Some(prev) if !rng.chance(0.05) => prev.next(&schema, view, &interest),
                _ => EncodedWorld::build(&schema, view, &interest),
            };
            for (i, pair) in pairs.iter_mut().enumerate() {
                pair.view.x = (pair.view.x + rng.next_f32() * 8.0 - 4.0).clamp(-90.0, 90.0);
                pair.view.y = (pair.view.y + rng.next_f32() * 8.0 - 4.0).clamp(-90.0, 90.0);
                if rng.chance(0.05) {
                    pair.budget = budgets[rng.range_u32(0, 6) as usize];
                }
                if rng.chance(0.01) {
                    pair.new.reset();
                    pair.old.reset();
                }
                // Skipped clients fall behind: their baselines go stale.
                if rng.chance(0.15) {
                    continue;
                }
                let new = pair.new.build_delta(
                    &schema,
                    &next,
                    &interest,
                    &pair.view,
                    pair.budget,
                    patch,
                    i as u32,
                );
                let old = pair.old.build_delta(
                    &schema,
                    &next,
                    &interest,
                    &pair.view,
                    pair.budget,
                    patch,
                    i as u32,
                );
                assert_eq!(new, old, "seed {seed} patch {patch} client {i}");
                let mut known: Vec<u32> = pair.new.known_ids().collect();
                let mut want: Vec<u32> = pair.old.known.keys().copied().collect();
                known.sort_unstable();
                want.sort_unstable();
                assert_eq!(known, want, "seed {seed} patch {patch} client {i}");
            }
            world = Some(next);
        }
    }

    #[test]
    fn encode_once_matches_per_client_encoding() {
        for seed in 0..24 {
            compare(seed, UpdateRates::every_patch());
            compare(seed, UpdateRates::default());
            compare(
                seed,
                UpdateRates {
                    near: 0.2,
                    mid: 0.6,
                    mid_every: 3,
                    far_every: 6,
                },
            );
        }
    }

    /// `n` players inside a disk of radius 10, each viewing 24 around
    /// itself (everyone sees everyone), all moving every patch.
    fn dense_room(n: u32, patches: u32, rates: UpdateRates, before: bool) -> (Duration, usize) {
        let pos = FieldType::QuantF32 {
            min: -40.0,
            max: 40.0,
            bits: 14,
        };
        let schema = EntitySchema::new().kind(
            KindDef::new(1, "player")
                .field("x", pos)
                .field("y", pos)
                .field("hp", FieldType::U8)
                .field("facing", FieldType::Angle { bits: 8 })
                .priority(2.0),
        );
        let interest = InterestConfig {
            update_rates: rates,
            ..InterestConfig::default()
        };
        let mut rng = ZoneRng::from_seed(u64::from(n));
        let mut players: Vec<(f32, f32, f32)> = (0..n)
            .map(|_| {
                let a = rng.next_f32() * std::f32::consts::TAU;
                let r = rng.next_f32().sqrt() * 10.0;
                (r * a.cos(), r * a.sin(), a)
            })
            .collect();
        let mut new: Vec<ClientReplica> = (0..n).map(|_| ClientReplica::new()).collect();
        let mut old: Vec<Reference> = (0..n).map(|_| Reference::default()).collect();
        let mut world: Option<EncodedWorld> = None;
        let (mut spent, mut bytes) = (Duration::ZERO, 0usize);
        let warmup = 10;
        for patch in 0..warmup + patches {
            let mut view = WorldView::default();
            for (i, p) in players.iter_mut().enumerate() {
                p.2 += 0.05;
                p.0 = (p.0 + p.2.cos() * 0.2).clamp(-10.0, 10.0);
                p.1 = (p.1 + p.2.sin() * 0.2).clamp(-10.0, 10.0);
                view.push(EntityState::new(
                    i as u32 + 1,
                    1,
                    p.0,
                    p.1,
                    vec![
                        FieldValue::F(p.0),
                        FieldValue::F(p.1),
                        FieldValue::U(100),
                        FieldValue::F(p.2),
                    ],
                ));
            }
            let started = Instant::now();
            let next = match (&world, before) {
                (Some(prev), false) => prev.next(&schema, view, &interest),
                _ => EncodedWorld::build(&schema, view, &interest),
            };
            let mut sent = 0;
            for (i, p) in players.iter().enumerate() {
                let v = View::new(p.0, p.1, 24.0);
                let (frame, _) = if before {
                    old[i].build_delta(&schema, &next, &interest, &v, 4096, patch, 0)
                } else {
                    new[i].build_delta(&schema, &next, &interest, &v, 4096, patch, 0)
                };
                sent += frame.len();
            }
            if patch >= warmup {
                spent += started.elapsed();
                bytes += sent;
            }
            world = Some(next);
        }
        (spent / patches, bytes / (patches as usize * n as usize))
    }

    /// `cargo test -p distributed_game --release --lib dense_room_patch_build -- --ignored --nocapture`
    #[test]
    #[ignore = "timing"]
    fn dense_room_patch_build() {
        for n in [64, 128] {
            let (before, b0) = dense_room(n, 200, UpdateRates::every_patch(), true);
            let (once, b1) = dense_room(n, 200, UpdateRates::every_patch(), false);
            let (rates, b2) = dense_room(n, 200, UpdateRates::default(), false);
            println!(
                "{n} clients: per-client {before:?} ({b0} B/client), encode once {once:?} ({b1} B), + update rates {rates:?} ({b2} B)"
            );
        }
    }
}
