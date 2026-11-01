//! Zone entities and the views projected from them.
//!
//! A zone is the **aggregate root**: one authoritative boundary that owns
//! its state and changes it only on its own tick. The things inside it
//! (players, mobs, projectiles) are **entities**: server state with an
//! [`EntityId`], including data no client may see (targets, spawn points,
//! credits, principals). What clients receive are **views**, the zone's
//! **read models**: `#[derive(Replicated)]` structs projected from an
//! entity each patch. A view is a projection, not a copy: it may convert
//! (HP to a percent), derive (a telegraph from a timer), flatten nested
//! state, or omit the entity entirely (a dead mob). Durable outcomes leave
//! the zone as **facts**, its **domain events**, committed by cells.
//!
//! | game runtime | business side |
//! |---|---|
//! | zone | aggregate root |
//! | zone entity ([`Entity`]) | entity inside the aggregate |
//! | view ([`Replicated`], [`Project`]) | read model / projection |
//! | fact ([`Fact`](crate::Fact)) | domain event |
//!
//! **Declaring a projection.** A view names its entity type with
//! `#[replicated(.., of = Mob)]` and marks the fields that are straight
//! copies `#[from]` (same name) or `#[from(state.x)]` (a field path on the
//! entity). The derive then generates `MobView::project(&mob, MobViewRest
//! { .. })` where `MobViewRest` holds only the hand-filled fields, and the
//! zone writes the projection once:
//!
//! ```ignore
//! impl Project<MobView> for Mob {
//!     fn view(&self) -> Option<MobView> {
//!         (self.state != MobState::Dead).then(|| {
//!             MobView::project(self, MobViewRest { hp: self.hp_percent(), tele: self.tele() })
//!         })
//!     }
//! }
//! ```
//!
//! A view whose fields are all `#[from]` gets the `Project` impl generated.
//! A view without `of` is unchanged: build it by hand or implement
//! `Project` for any type.
//!
//! **Registering collections.** [`Zone::entities`](crate::Zone::entities)
//! returns the zone's entity collections once, at creation; every patch
//! the host calls [`Project::view`] on each entity (`None` omits it) and
//! then [`Zone::replicate`](crate::Zone::replicate) for anything else:
//!
//! ```ignore
//! fn entities() -> Entities<Self> {
//!     Entities::new()
//!         .each(|z: &Self| &z.players)
//!         .each(|z: &Self| &z.mobs)
//!         .one(|z: &Self| z) // the zone itself projects its HUD view
//! }
//! ```
//!
//! Collections are visited in registration order and each in its own order
//! (`Vec`, slices, `VecDeque`, `BTreeMap` values, `Option`): the projected
//! world is deterministic, which the replicated state hash relies on.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use crate::authoring::{Pushed, Replicated};
use crate::replication::WorldView;

/// Id of a replicated zone entity (unique within the zone).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct EntityId(pub u32);

impl EntityId {
    /// The raw id.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for EntityId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<EntityId> for u32 {
    fn from(id: EntityId) -> Self {
        id.0
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A zone entity: anything the zone replicates under its own id.
pub trait Entity {
    /// Its id.
    fn entity_id(&self) -> EntityId;
}

/// Projection of a zone entity (or the zone itself) into view `V`.
pub trait Project<V: Replicated> {
    /// The view this patch; `None` leaves the entity out (clients that
    /// know it get a remove).
    fn view(&self) -> Option<V>;
}

/// Something holding entities, visited in a deterministic order.
pub trait EntityCollection {
    /// Entity type.
    type Item;
    /// Visit every entity.
    fn visit(&self, f: &mut dyn FnMut(&Self::Item));
}

impl<E> EntityCollection for [E] {
    type Item = E;
    fn visit(&self, f: &mut dyn FnMut(&E)) {
        self.iter().for_each(f);
    }
}

impl<E> EntityCollection for Vec<E> {
    type Item = E;
    fn visit(&self, f: &mut dyn FnMut(&E)) {
        self.iter().for_each(f);
    }
}

impl<E, const N: usize> EntityCollection for [E; N] {
    type Item = E;
    fn visit(&self, f: &mut dyn FnMut(&E)) {
        self.iter().for_each(f);
    }
}

impl<E> EntityCollection for VecDeque<E> {
    type Item = E;
    fn visit(&self, f: &mut dyn FnMut(&E)) {
        self.iter().for_each(f);
    }
}

impl<K, E> EntityCollection for BTreeMap<K, E> {
    type Item = E;
    fn visit(&self, f: &mut dyn FnMut(&E)) {
        self.values().for_each(f);
    }
}

impl<E> EntityCollection for Option<E> {
    type Item = E;
    fn visit(&self, f: &mut dyn FnMut(&E)) {
        self.iter().for_each(f);
    }
}

type Projection<Z> = Box<dyn Fn(&Z, &mut WorldView) + Send + Sync>;

/// A zone's registered entity collections ([`Zone::entities`](crate::Zone::entities)).
pub struct Entities<Z> {
    projections: Vec<Projection<Z>>,
}

impl<Z> Default for Entities<Z> {
    fn default() -> Self {
        Self {
            projections: Vec::new(),
        }
    }
}

impl<Z> fmt::Debug for Entities<Z> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entities")
            .field("collections", &self.projections.len())
            .finish()
    }
}

impl<Z: 'static> Entities<Z> {
    /// No collections.
    pub fn new() -> Self {
        Self::default()
    }

    /// Project every entity of a collection into view `V` (inferred when
    /// the entity type projects into one view; otherwise
    /// `.each::<MobView, _>(..)`).
    pub fn each<V, C>(mut self, collection: fn(&Z) -> &C) -> Self
    where
        V: Replicated,
        C: EntityCollection + ?Sized + 'static,
        C::Item: Entity + Project<V>,
    {
        self.projections.push(Box::new(move |zone, out| {
            collection(zone).visit(&mut |entity| {
                out.project::<V, _>(entity);
            });
        }));
        self
    }

    /// Project a single entity (the zone itself, `|z| z`, for a HUD view).
    pub fn one<V, E>(mut self, entity: fn(&Z) -> &E) -> Self
    where
        V: Replicated,
        E: Entity + Project<V> + ?Sized + 'static,
    {
        self.projections.push(Box::new(move |zone, out| {
            out.project::<V, _>(entity(zone));
        }));
        self
    }

    /// Collections registered.
    pub fn len(&self) -> usize {
        self.projections.len()
    }

    /// True when nothing is registered.
    pub fn is_empty(&self) -> bool {
        self.projections.is_empty()
    }

    /// Push every registered entity's view into `out`.
    pub fn project(&self, zone: &Z, out: &mut WorldView) {
        for projection in &self.projections {
            projection(zone, out);
        }
    }
}

impl WorldView {
    /// Push `entity`'s view `V` under its id; `None` when the projection
    /// omits it. For hand-written [`Zone::replicate`](crate::Zone::replicate).
    pub fn project<V, E>(&mut self, entity: &E) -> Option<Pushed<'_>>
    where
        V: Replicated,
        E: Entity + Project<V> + ?Sized,
    {
        let view = entity.view()?;
        Some(self.add(entity.entity_id(), &view))
    }
}
