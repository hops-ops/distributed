//! The game surface manifest: everything a generated client needs to know
//! about one zone type, built from the authoring derives.
//!
//! A [`GameSurface`] lists the zone's replicated kinds (fields, wire types,
//! priorities, client correction defaults), its input layout, its events and
//! the records their payloads mention; optionally its client-predicted step
//! ([`GameSurface::predict`]), pure functions compiled to WebAssembly
//! ([`GameSurface::pure`]), constants the client needs
//! ([`GameSurface::constant`]) and standalone records it decodes
//! ([`GameSurface::record`]). It is versioned
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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::codec::WireType;
use crate::prediction::{PredictContext, PredictStep};
use crate::pure::GamePure;
use crate::schema::{Correction, EntitySchema};
use crate::{GameEvent, GameInput, Wire, Zone};

/// Wire protocol version whose payload encodings (fields, inputs, events)
/// a surface describes. They have not changed since 1: protocol 2 only
/// added handshake negotiation (Welcome `features` and `origin`), which the
/// SDK handles, so generated clients stay valid for both.
pub const SURFACE_PROTOCOL_VERSION: u32 = 1;

/// Version of the [`GameSurface`] JSON shape.
pub const GAME_SURFACE_SCHEMA_VERSION: u32 = 3;

/// One zone type's client-facing surface. See the module docs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GameSurface {
    /// [`GAME_SURFACE_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Surface name (generated TypeScript prefixes exports with it).
    pub name: String,
    /// Zone wire protocol version of the payload encodings
    /// ([`SURFACE_PROTOCOL_VERSION`]).
    pub protocol_version: u32,
    /// Replicated kinds in schema order.
    pub kinds: Vec<KindSurface>,
    /// Input layout, if the zone takes typed input.
    pub input: Option<InputSurface>,
    /// Events by id.
    pub events: Vec<EventSurface>,
    /// Records mentioned by inputs, events or the predicted step, by name.
    pub records: Vec<RecordSurface>,
    /// The client-predicted step, compiled to WebAssembly (see
    /// [`crate::prediction`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predict: Option<PredictSurface>,
    /// Pure functions compiled into the same WebAssembly module, by export
    /// (see [`crate::pure`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pures: Vec<PureSurface>,
    /// Constants emitted as generated TypeScript data, by name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constants: Vec<ConstantSurface>,
    /// Records with a standalone codec in the generated client
    /// (`encode<Record>` / `decode<Record>`), by name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub codecs: Vec<String>,
    /// Zone inbox messages ([`Zone::Message`], [`crate::inbox`]) by id:
    /// what services may send into a running zone. Same payload encoding
    /// as events.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<EventSurface>,
}

/// A pure function compiled to WebAssembly ([`crate::pure::GamePure`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PureSurface {
    /// Export suffix: the module exports `dg_pure_<export>`.
    pub export: String,
    /// Cargo package declaring it (the surface's module is built from it).
    pub package: String,
    /// Argument type, if it takes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<WireType>,
    /// Result type.
    pub output: WireType,
    /// Binary ABI version ([`crate::prediction::ABI_VERSION`]).
    pub abi: u32,
}

/// A constant value of the client (static data defined once in Rust).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConstantSurface {
    /// `snake_case` name (generated TypeScript: `SCREAMING_SNAKE`).
    pub name: String,
    /// Wire type of the value.
    #[serde(rename = "type")]
    pub ty: WireType,
    /// The value as a client decodes it ([`crate::codec::to_json_value`]):
    /// records keyed by Rust field name, exact float values.
    pub value: serde_json::Value,
}

/// The zone's client-predicted step ([`crate::prediction::PredictStep`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PredictSurface {
    /// Predicted kind (the controlled entity's kind).
    pub kind: String,
    /// Cargo package declaring the step (compiled to WebAssembly).
    pub package: String,
    /// Export suffix: the module exports `dg_predict_<export>`.
    pub export: String,
    /// State record (fields named after the kind's fields).
    pub state: String,
    /// Context record, if the step takes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Binary ABI version ([`crate::prediction::ABI_VERSION`]).
    pub abi: u32,
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
            protocol_version: SURFACE_PROTOCOL_VERSION,
            kinds: Vec::new(),
            input: None,
            events: Vec::new(),
            records: Vec::new(),
            predict: None,
            pures: Vec::new(),
            constants: Vec::new(),
            codecs: Vec::new(),
            messages: Vec::new(),
        }
    }

    /// Kinds from the zone's [`Zone::schema`], its typed input and its
    /// inbox messages ([`Zone::Message`]).
    pub fn for_zone<Z>(name: impl Into<String>) -> Self
    where
        Z: Zone,
        Z::Input: GameInput,
    {
        Self::new(name)
            .schema(&Z::schema())
            .input::<Z::Input>()
            .messages::<Z::Message>()
    }

    /// Add the zone inbox message type (done by [`for_zone`](Self::for_zone)).
    /// Message ids and names must be unique among messages.
    pub fn messages<M: GameEvent>(mut self) -> Self {
        let mut records = Vec::new();
        for message in M::event_surfaces(&mut records) {
            assert!(
                self.messages
                    .iter()
                    .all(|m| m.id != message.id && m.name != message.name),
                "duplicate message id {} / name {} in game surface {}",
                message.id,
                message.name,
                self.name
            );
            self.messages.push(message);
        }
        self.messages.sort_by_key(|m| m.id);
        self.add_records(records);
        self
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

    /// Declare the zone's client-predicted step (`#[predict]`). Checks that
    /// it predicts a kind of this surface, takes the surface's input, and
    /// that every state field names a field of the kind with a matching
    /// scalar type; panics otherwise (surface export time).
    pub fn predict<P: PredictStep>(mut self) -> Self {
        if let Some(pure) = self.pures.first() {
            assert_eq!(
                pure.package,
                P::PACKAGE,
                "predicted step {} of game surface {} must be declared in the Cargo package of its pure functions",
                P::EXPORT,
                self.name
            );
        }
        let mut records = Vec::new();
        let input = P::Input::input_surface(&mut records);
        assert_eq!(
            self.input.as_ref(),
            Some(&input),
            "predicted step {} of game surface {} must take the zone's input type",
            P::EXPORT,
            self.name
        );
        let kind = self
            .kinds
            .iter()
            .find(|k| k.name == P::KIND)
            .unwrap_or_else(|| {
                panic!(
                    "predicted step {} names kind {:?}, which game surface {} does not replicate",
                    P::EXPORT,
                    P::KIND,
                    self.name
                )
            })
            .clone();
        let state = record_name(P::State::wire_type(), "state", P::EXPORT);
        P::State::records(&mut records);
        let state_record = records
            .iter()
            .find(|r| r.name == state)
            .expect("a record pushes its own definition")
            .clone();
        for field in &state_record.fields {
            let entity = kind
                .fields
                .iter()
                .find(|f| f.name == field.name)
                .unwrap_or_else(|| {
                    panic!(
                        "predicted state {state} field {} is not a field of kind {}",
                        field.name, kind.name
                    )
                });
            assert!(
                state_field_matches(&field.ty, &entity.ty),
                "predicted state {state} field {} ({:?}) does not match kind {} field type {:?}",
                field.name,
                field.ty,
                kind.name,
                entity.ty
            );
        }
        let context = P::Context::context_type().map(|ty| record_name(ty, "context", P::EXPORT));
        P::Context::context_records(&mut records);
        self.predict = Some(PredictSurface {
            kind: P::KIND.to_string(),
            package: P::PACKAGE.to_string(),
            export: P::EXPORT.to_string(),
            state,
            context,
            abi: crate::prediction::ABI_VERSION,
        });
        self.add_records(records);
        self
    }

    /// Add a pure function (`#[game_pure]`) to the surface's WebAssembly
    /// module. Every compiled function of a surface (and its predicted step)
    /// must come from one Cargo package; export names must be unique.
    pub fn pure<F: GamePure>(mut self) -> Self {
        assert!(
            self.pures.iter().all(|p| p.export != F::EXPORT),
            "pure function {} added twice to game surface {}",
            F::EXPORT,
            self.name
        );
        let package = self
            .predict
            .as_ref()
            .map(|p| p.package.as_str())
            .or(self.pures.first().map(|p| p.package.as_str()));
        if let Some(package) = package {
            assert_eq!(
                package,
                F::PACKAGE,
                "pure function {} of game surface {} is declared in Cargo package {}, but the surface's module is built from {package}",
                F::EXPORT,
                self.name,
                F::PACKAGE
            );
        }
        let mut records = Vec::new();
        let arg = crate::pure::arg_type::<F>(&mut records);
        F::Output::records(&mut records);
        self.pures.push(PureSurface {
            export: F::EXPORT.to_string(),
            package: F::PACKAGE.to_string(),
            arg,
            output: F::Output::wire_type(),
            abi: crate::prediction::ABI_VERSION,
        });
        self.pures.sort_by(|a, b| a.export.cmp(&b.export));
        self.add_records(records);
        self
    }

    /// Add a constant the generated client exports as TypeScript data
    /// (`constant("arena_floor", &floor)` → `ARENA_FLOOR: ArenaFloor`). The
    /// value is stored as the client would decode it (exact floats,
    /// quantized fields quantized); its records join the surface. Names are
    /// `snake_case` and unique; panics on a non-finite float.
    pub fn constant<T: Wire>(mut self, name: &str, value: &T) -> Self {
        assert!(
            !name.is_empty()
                && name.starts_with(|c: char| c.is_ascii_lowercase())
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "constant name {name:?} of game surface {} must be snake_case",
            self.name
        );
        assert!(
            self.constants.iter().all(|c| c.name != name),
            "constant {name} added twice to game surface {}",
            self.name
        );
        let value = crate::codec::to_json_value(value).unwrap_or_else(|error| {
            panic!(
                "constant {name} of game surface {} has no client value: {error:?}",
                self.name
            )
        });
        let mut records = Vec::new();
        T::records(&mut records);
        self.constants.push(ConstantSurface {
            name: name.to_string(),
            ty: T::wire_type(),
            value,
        });
        self.constants.sort_by(|a, b| a.name.cmp(&b.name));
        self.add_records(records);
        self
    }

    /// Give a record (`#[derive(GameRecord)]`) a standalone codec in the
    /// generated client (`encode<Name>` / `decode<Name>`), e.g. for a
    /// payload served over HTTP.
    pub fn record<T: Wire>(mut self) -> Self {
        let name = match T::wire_type() {
            WireType::Record { name } => name,
            other => {
                panic!("GameSurface::record takes a #[derive(GameRecord)] struct, not {other:?}")
            }
        };
        let mut records = Vec::new();
        T::records(&mut records);
        if !self.codecs.contains(&name) {
            self.codecs.push(name);
            self.codecs.sort();
        }
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

    /// Structural validity of a surface, however it was produced (the
    /// builders above, or JSON read back by `distributed game-client`):
    /// the versions this crate speaks, unique kinds, events, records,
    /// fields and buttons, replicable kind field types, known and
    /// non-recursive records, and a consistent predicted step, pure
    /// functions, constants and codecs.
    ///
    /// Naming rules of one target language (collisions after case
    /// conversion, reserved words) belong to that generator.
    ///
    /// # Errors
    ///
    /// The first violated rule, as a message naming the offending item.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != GAME_SURFACE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported game surface schemaVersion {}; this version is {GAME_SURFACE_SCHEMA_VERSION}",
                self.schema_version
            ));
        }
        if self.protocol_version != SURFACE_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported game surface protocolVersion {}; this version is {SURFACE_PROTOCOL_VERSION}",
                self.protocol_version
            ));
        }
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(format!("invalid surface name {:?}", self.name));
        }
        let mut records = BTreeMap::new();
        for record in &self.records {
            if records.insert(record.name.as_str(), record).is_some() {
                return Err(format!("duplicate record {}", record.name));
            }
        }
        let mut kind_ids = BTreeSet::new();
        let mut kind_names = BTreeSet::new();
        for kind in &self.kinds {
            if !kind_ids.insert(kind.id) {
                return Err(format!("duplicate kind id {}", kind.id));
            }
            if kind.name.is_empty() || !kind_names.insert(kind.name.as_str()) {
                return Err(format!("duplicate or empty kind name {:?}", kind.name));
            }
            let mut names = BTreeSet::new();
            for field in &kind.fields {
                if field.ty.field_type().is_none() {
                    return Err(format!(
                        "kind {} field {} has a non-entity wire type",
                        kind.name, field.name
                    ));
                }
                if !names.insert(field.name.as_str()) {
                    return Err(format!(
                        "kind {} fields collide as `{}`",
                        kind.name, field.name
                    ));
                }
            }
        }
        let mut event_ids = BTreeSet::new();
        let mut event_names = BTreeSet::new();
        for event in &self.events {
            if !event_ids.insert(event.id) || !event_names.insert(event.name.as_str()) {
                return Err(format!("duplicate event {} / {}", event.id, event.name));
            }
            match &event.payload {
                EventPayload::Fields { fields } => check_fields(fields, &event.name, &records)?,
                EventPayload::Value { ty } => check_type(ty, &event.name, &records)?,
            }
        }
        for record in &self.records {
            check_fields(&record.fields, &record.name, &records)?;
        }
        if let Some(input) = &self.input {
            check_fields(&input.fields, &input.name, &records)?;
            let mut buttons = BTreeSet::new();
            for button in input.fields.iter().flat_map(|field| &field.buttons) {
                if !buttons.insert(button.as_str()) {
                    return Err(format!("duplicate input button {button}"));
                }
            }
        }
        self.records_in_dependency_order()?;
        self.validate_predict(&records)?;
        self.validate_pures(&records)?;
        let mut constants = BTreeSet::new();
        for constant in &self.constants {
            let snake_case = constant.name.starts_with(|c: char| c.is_ascii_lowercase())
                && constant
                    .name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !snake_case || !constants.insert(constant.name.as_str()) {
                return Err(format!(
                    "constant name {:?} is not snake_case or is declared twice",
                    constant.name
                ));
            }
            check_type(&constant.ty, &constant.name, &records)?;
        }
        for codec in &self.codecs {
            if !records.contains_key(codec.as_str()) {
                return Err(format!("codec record {codec} is missing"));
            }
        }
        Ok(())
    }

    /// Records with every record a record uses before it.
    ///
    /// # Errors
    ///
    /// A record that (transitively) contains itself, or names a record the
    /// surface does not list.
    pub fn records_in_dependency_order(&self) -> Result<Vec<&RecordSurface>, String> {
        fn visit<'r>(
            name: &str,
            records: &BTreeMap<&'r str, &'r RecordSurface>,
            done: &mut BTreeSet<&'r str>,
            visiting: &mut BTreeSet<&'r str>,
            order: &mut Vec<&'r RecordSurface>,
        ) -> Result<(), String> {
            if done.contains(name) {
                return Ok(());
            }
            let record = *records
                .get(name)
                .ok_or_else(|| format!("unknown record {name}"))?;
            if !visiting.insert(record.name.as_str()) {
                return Err(format!(
                    "record {name} is recursive; recursive records are not supported"
                ));
            }
            for field in &record.fields {
                if let Some(used) = record_of(&field.ty) {
                    visit(used, records, done, visiting, order)?;
                }
            }
            visiting.remove(name);
            done.insert(record.name.as_str());
            order.push(record);
            Ok(())
        }
        let records: BTreeMap<&str, &RecordSurface> = self
            .records
            .iter()
            .map(|record| (record.name.as_str(), record))
            .collect();
        let mut order = Vec::new();
        let (mut done, mut visiting) = (BTreeSet::new(), BTreeSet::new());
        for name in records.keys() {
            visit(name, &records, &mut done, &mut visiting, &mut order)?;
        }
        Ok(order)
    }

    /// The predicted step names a kind of this surface, a state record whose
    /// fields are fields of that kind (plain scalar types), an optional
    /// context record, and this crate's ABI.
    fn validate_predict(&self, records: &BTreeMap<&str, &RecordSurface>) -> Result<(), String> {
        let Some(predict) = &self.predict else {
            return Ok(());
        };
        if predict.abi != crate::prediction::ABI_VERSION {
            return Err(format!(
                "predicted step uses ABI v{}; this version speaks ABI v{}",
                predict.abi,
                crate::prediction::ABI_VERSION
            ));
        }
        if !is_export_name(&predict.export) {
            return Err(format!(
                "invalid predicted step export {:?}",
                predict.export
            ));
        }
        if predict.package.is_empty() {
            return Err("predicted step names no Cargo package".into());
        }
        if self.input.is_none() {
            return Err("a predicted step needs the zone's input type".into());
        }
        let kind = self
            .kinds
            .iter()
            .find(|kind| kind.name == predict.kind)
            .ok_or_else(|| format!("predicted kind {:?} is not replicated", predict.kind))?;
        let state = records
            .get(predict.state.as_str())
            .ok_or_else(|| format!("predicted state record {} is missing", predict.state))?;
        if state.fields.is_empty() {
            return Err(format!("predicted state {} has no fields", state.name));
        }
        for field in &state.fields {
            let entity = kind
                .fields
                .iter()
                .find(|entity| entity.name == field.name)
                .ok_or_else(|| {
                    format!(
                        "predicted state {} field {} is not a field of kind {}",
                        state.name, field.name, kind.name
                    )
                })?;
            if !state_field_matches(&field.ty, &entity.ty) {
                return Err(format!(
                    "predicted state {} field {} does not match the type of kind {} field {}",
                    state.name, field.name, kind.name, entity.name
                ));
            }
        }
        if let Some(context) = &predict.context {
            if !records.contains_key(context.as_str()) {
                return Err(format!("predicted context record {context} is missing"));
            }
        }
        Ok(())
    }

    /// Pure functions: valid unique exports, one Cargo package (the
    /// predicted step's, if any), known types, this crate's ABI.
    fn validate_pures(&self, records: &BTreeMap<&str, &RecordSurface>) -> Result<(), String> {
        let mut exports = BTreeSet::new();
        let package = self
            .predict
            .as_ref()
            .map(|predict| predict.package.as_str())
            .or_else(|| self.pures.first().map(|pure| pure.package.as_str()));
        for pure in &self.pures {
            if pure.abi != crate::prediction::ABI_VERSION {
                return Err(format!(
                    "pure function {} uses ABI v{}; this version speaks ABI v{}",
                    pure.export,
                    pure.abi,
                    crate::prediction::ABI_VERSION
                ));
            }
            if !is_export_name(&pure.export) {
                return Err(format!("invalid pure function export {:?}", pure.export));
            }
            if !exports.insert(pure.export.as_str()) {
                return Err(format!("duplicate pure function {}", pure.export));
            }
            if pure.package.is_empty() {
                return Err(format!(
                    "pure function {} names no Cargo package",
                    pure.export
                ));
            }
            let expected = package.expect("a surface with pures has a package");
            if pure.package != expected {
                return Err(format!(
                    "pure function {} is declared in Cargo package {}, but the surface's module is built from {expected}",
                    pure.export, pure.package
                ));
            }
            if let Some(arg) = &pure.arg {
                check_type(arg, &pure.export, records)?;
            }
            check_type(&pure.output, &pure.export, records)?;
        }
        Ok(())
    }
}

/// A WebAssembly export suffix (`dg_predict_<export>`, `dg_pure_<export>`).
fn is_export_name(export: &str) -> bool {
    !export.is_empty()
        && export
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The record a type names, through lists and optionals.
fn record_of(ty: &WireType) -> Option<&str> {
    match ty {
        WireType::List { item } | WireType::Optional { item } => record_of(item),
        WireType::Record { name } => Some(name),
        _ => None,
    }
}

fn check_type(
    ty: &WireType,
    owner: &str,
    records: &BTreeMap<&str, &RecordSurface>,
) -> Result<(), String> {
    match record_of(ty) {
        Some(name) if !records.contains_key(name) => Err(format!("{owner}: unknown record {name}")),
        _ => Ok(()),
    }
}

fn check_fields(
    fields: &[PayloadField],
    owner: &str,
    records: &BTreeMap<&str, &RecordSurface>,
) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for field in fields {
        if field.name.is_empty() || !names.insert(field.name.as_str()) {
            return Err(format!(
                "{owner}: field `{}` is empty or collides with another as `{}`",
                field.name, field.name
            ));
        }
        check_type(&field.ty, owner, records)?;
    }
    Ok(())
}

fn record_name(ty: WireType, what: &str, export: &str) -> String {
    match ty {
        WireType::Record { name } => name,
        other => panic!(
            "predicted step {export}: the {what} must be a #[derive(GameRecord)] struct, not {other:?}"
        ),
    }
}

/// A state field holds the plain value of an entity field: `f32` for float
/// fields (quantized or not, the step works on full binary32), the same
/// integer / bool type otherwise.
fn state_field_matches(state: &WireType, entity: &WireType) -> bool {
    match state {
        WireType::F32 => matches!(
            entity,
            WireType::F32 | WireType::Quant { .. } | WireType::Angle { .. }
        ),
        WireType::Bool | WireType::U8 | WireType::U16 | WireType::U32 | WireType::I32 => {
            state == entity
        }
        _ => false,
    }
}
