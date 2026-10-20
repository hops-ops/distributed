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

use std::collections::{HashMap, HashSet};

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

#[derive(Debug, Clone)]
struct EncodedEntity {
    id: u32,
    kind: u16,
    x: f32,
    y: f32,
    priority: f32,
    global: bool,
    data: Vec<u8>,
    offsets: Vec<usize>,
}

impl EncodedEntity {
    fn field(&self, index: usize) -> &[u8] {
        &self.data[self.offsets[index]..self.offsets[index + 1]]
    }
}

/// A patch's encoded world, shared by every client.
#[derive(Debug, Clone)]
pub struct EncodedWorld {
    entities: Vec<EncodedEntity>,
    by_id: HashMap<u32, usize>,
    grid: SpatialGrid,
    globals: Vec<u32>,
}

impl EncodedWorld {
    /// Encode `view` against `schema`. Entities of unknown kinds or duplicate
    /// ids are skipped.
    pub fn build(schema: &EntitySchema, view: WorldView, interest: &InterestConfig) -> Self {
        let mut world = Self {
            entities: Vec::with_capacity(view.entities.len()),
            by_id: HashMap::new(),
            grid: SpatialGrid::new(interest.cell_size),
            globals: Vec::new(),
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
                data: w.into_bytes(),
                offsets,
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
}

#[derive(Debug, Clone)]
struct Known {
    data: Vec<u8>,
    offsets: Vec<usize>,
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
    /// Create+update section bytes (what the budget limits).
    pub budget_bytes: usize,
}

/// What one client has been sent.
#[derive(Debug, Clone, Default)]
pub struct ClientReplica {
    known: HashMap<u32, Known>,
    accum: HashMap<u32, f32>,
}

enum Candidate {
    Create,
    Update(u32),
}

impl ClientReplica {
    /// Fresh replica (client knows nothing).
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget everything (e.g. after reconnect the client gets fresh creates).
    pub fn reset(&mut self) {
        self.known.clear();
        self.accum.clear();
    }

    /// Ids the client currently knows.
    pub fn known_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.known.keys().copied()
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
        let radius = view.radius.max(0.0);
        let stay_radius = radius * (1.0 + interest.leave_margin.max(0.0));
        let mut hits = Vec::new();
        world.grid.query(view.x, view.y, stay_radius, &mut hits);
        hits.extend_from_slice(&world.globals);

        // Visible entities with their distance.
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

        // Candidates with accumulated priority.
        let mut candidates: Vec<(usize, Candidate, f32)> = Vec::new();
        for (index, d) in visible {
            let e = &world.entities[index];
            let Some(kind) = schema.get(e.kind) else {
                continue;
            };
            let (candidate, field_prio) = match self.known.get(&e.id) {
                None => (
                    Candidate::Create,
                    kind.fields
                        .iter()
                        .map(|f| f.priority)
                        .fold(1.0_f32, f32::max),
                ),
                Some(known) => {
                    let mut mask = 0u32;
                    let mut prio = 0.0_f32;
                    for (i, def) in kind.fields.iter().enumerate() {
                        let old = &known.data[known.offsets[i]..known.offsets[i + 1]];
                        if old != e.field(i) {
                            mask |= 1 << i;
                            prio = prio.max(def.priority);
                        }
                    }
                    if mask == 0 {
                        continue;
                    }
                    (Candidate::Update(mask), prio)
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
        self.accum.retain(|id, _| candidate_ids.contains(id));
        candidates.sort_by(|a, b| {
            b.2.total_cmp(&a.2)
                .then(world.entities[a.0].id.cmp(&world.entities[b.0].id))
        });

        let mut stats = DeltaStats {
            removes: removes.len(),
            ..DeltaStats::default()
        };
        let mut creates = Writer::new();
        let mut updates = Writer::new();
        let mut scratch = Writer::new();
        for (index, candidate, _) in candidates {
            let e = &world.entities[index];
            scratch.clear();
            match candidate {
                Candidate::Create => {
                    scratch
                        .varuint(e.id)
                        .varuint(u32::from(e.kind))
                        .raw(&e.data);
                }
                Candidate::Update(mask) => {
                    scratch.varuint(e.id).varuint(mask);
                    for i in 0..e.offsets.len() - 1 {
                        if mask & (1 << i) != 0 {
                            scratch.raw(e.field(i));
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
                Candidate::Create => {
                    creates.raw(scratch.as_bytes());
                    stats.creates += 1;
                }
                Candidate::Update(_) => {
                    updates.raw(scratch.as_bytes());
                    stats.updates += 1;
                }
            }
            self.accum.remove(&e.id);
            self.known.insert(
                e.id,
                Known {
                    data: e.data.clone(),
                    offsets: e.offsets.clone(),
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
