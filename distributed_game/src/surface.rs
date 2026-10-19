//! The game surface manifest: everything a generated client needs to know
//! about one zone type, built from the authoring derives.
//!
//! A [`GameSurface`] lists the zone's replicated kinds (fields, wire types,
//! priorities, client correction defaults), its input layout, its events and
//! the records their payloads mention. It is versioned
//! ([`GAME_SURFACE_SCHEMA_VERSION`]) and serialized as JSON by an app export
//! function (conventionally `game_surface`), which `distributed
//! game-manifest` runs and `distributed game-client` turns into typed
//! TypeScript.
//!
//! ```ignore
//! pub fn game_surface() -> GameSurface {
//!     GameSurface::for_zone::<Arena>("arena").events::<ArenaEvent>()
//! }
//! ```
//!
//! It is deliberately separate from the strict GraphQL client manifest: game
//! clients speak the binary zone protocol (`PROTOCOL.md`), not GraphQL.

use serde::{Deserialize, Serialize};

use crate::codec::WireType;
use crate::protocol::PROTOCOL_VERSION;
use crate::schema::{Correction, EntitySchema};
use crate::{GameEvent, GameInput, Zone};

/// Version of the [`GameSurface`] JSON shape.
pub const GAME_SURFACE_SCHEMA_VERSION: u32 = 1;

/// One zone type's client-facing surface. See the module docs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GameSurface {
    /// [`GAME_SURFACE_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Surface name (generated TypeScript prefixes exports with it).
    pub name: String,
    /// Zone wire protocol version (`PROTOCOL_VERSION`).
    pub protocol_version: u32,
    /// Replicated kinds in schema order.
    pub kinds: Vec<KindSurface>,
    /// Input layout, if the zone takes typed input.
    pub input: Option<InputSurface>,
    /// Events by id.
    pub events: Vec<EventSurface>,
    /// Records mentioned by inputs or events, by name.
    pub records: Vec<RecordSurface>,
}

/// One replicated kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KindSurface {
    /// Kind id.
    pub id: u16,
    /// Kind name.
    pub name: String,
    /// Server-side priority multiplier.
    pub priority: f32,
    /// Ordered fields.
    pub fields: Vec<EntityFieldSurface>,
}

/// One entity field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntityFieldSurface {
    /// Wire name (the key in `RenderedEntity.fields`).
    pub name: String,
    /// Wire type (always an entity field type).
    #[serde(rename = "type")]
    pub ty: WireType,
    /// Server-side priority multiplier.
    pub priority: f32,
    /// Client correction default.
    pub correction: Option<Correction>,
}

/// A payload field (input, event or record).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PayloadField {
    /// Rust field name (generated TypeScript uses its camelCase form).
    pub name: String,
    /// Wire type.
    #[serde(rename = "type")]
    pub ty: WireType,
    /// Input sanitize range `[min, max]` (inputs only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<[f64; 2]>,
    /// Button names, bit 0 first (input bit-flag fields only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buttons: Vec<String>,
}

impl PayloadField {
    /// Plain field.
    pub fn new(name: impl Into<String>, ty: WireType) -> Self {
        Self {
            name: name.into(),
            ty,
            range: None,
            buttons: Vec::new(),
        }
    }
}

/// The input layout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputSurface {
    /// Rust type name.
    pub name: String,
    /// Fields in wire order.
    pub fields: Vec<PayloadField>,
}

/// One event type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventSurface {
    /// Stable type id (the Event message `type_id`).
    pub id: u32,
    /// Event name (snake_case).
    pub name: String,
    /// Payload: inline fields or one value.
    pub payload: EventPayload,
}

/// An event payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventPayload {
    /// Named fields in wire order (struct events, struct-like variants).
    Fields {
        /// Fields.
        fields: Vec<PayloadField>,
    },
    /// The payload is one value's encoding (`Variant(T)`), typically a
    /// record shared with other code.
    Value {
        /// Value type.
        #[serde(rename = "type")]
        ty: WireType,
    },
}

/// A named record (nested payload struct).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordSurface {
    /// Rust type name.
    pub name: String,
    /// Fields in wire order.
    pub fields: Vec<PayloadField>,
}

impl GameSurface {
    /// Empty surface named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema_version: GAME_SURFACE_SCHEMA_VERSION,
            name: name.into(),
            protocol_version: PROTOCOL_VERSION,
            kinds: Vec::new(),
            input: None,
            events: Vec::new(),
            records: Vec::new(),
        }
    }

    /// Kinds from the zone's [`Zone::schema`] and its typed input.
    pub fn for_zone<Z>(name: impl Into<String>) -> Self
    where
        Z: Zone,
        Z::Input: GameInput,
    {
        Self::new(name).schema(&Z::schema()).input::<Z::Input>()
    }

    /// Add every kind of `schema` (derived or hand-written).
    pub fn schema(mut self, schema: &EntitySchema) -> Self {
        for kind in schema.kinds() {
            assert!(
                self.kinds.iter().all(|k| k.id != kind.id),
                "duplicate kind id {} in game surface {}",
                kind.id,
                self.name
            );
            self.kinds.push(KindSurface {
                id: kind.id,
                name: kind.name.clone(),
                priority: kind.priority,
                fields: kind
                    .fields
                    .iter()
                    .map(|field| EntityFieldSurface {
                        name: field.name.clone(),
                        ty: WireType::of_field(field.ty),
                        priority: field.priority,
                        correction: field.correction,
                    })
                    .collect(),
            });
        }
        self
    }

    /// Set the input layout.
    pub fn input<I: GameInput>(mut self) -> Self {
        let mut records = Vec::new();
        self.input = Some(I::input_surface(&mut records));
        self.add_records(records);
        self
    }

    /// Add an event type (a `#[derive(GameEvent)]` enum adds all its
    /// variants). Event ids and names must be unique across the surface.
    pub fn events<E: GameEvent>(mut self) -> Self {
        let mut records = Vec::new();
        for event in E::event_surfaces(&mut records) {
            assert!(
                self.events
                    .iter()
                    .all(|e| e.id != event.id && e.name != event.name),
                "duplicate event id {} / name {} in game surface {}",
                event.id,
                event.name,
                self.name
            );
            self.events.push(event);
        }
        self.events.sort_by_key(|e| e.id);
        self.add_records(records);
        self
    }

    fn add_records(&mut self, records: Vec<RecordSurface>) {
        for record in records {
            match self.records.iter().find(|r| r.name == record.name) {
                Some(existing) => assert_eq!(
                    existing, &record,
                    "two different records named {} in game surface {}",
                    record.name, self.name
                ),
                None => self.records.push(record),
            }
        }
        self.records.sort_by(|a, b| a.name.cmp(&b.name));
    }

    /// Pretty JSON (deterministic: kinds in schema order, events by id,
    /// records by name).
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("game surface serializes")
    }
}
