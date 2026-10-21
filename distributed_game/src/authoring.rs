//! Traits implemented by the authoring derives (`distributed_game_macros`,
//! re-exported from this crate): [`Replicated`] views, [`GameInput`] frames
//! and [`GameEvent`] messages. Implement them by hand only when a derive does
//! not fit; the derives keep the schema, the encoding and the surface in one
//! declaration so they cannot drift.

use crate::codec::Wire;
use crate::protocol::wire::{DecodeError, Reader, Writer};
use crate::replication::{EntityState, WorldView};
use crate::schema::{FieldValue, KindDef};
use crate::surface::{EventSurface, InputSurface, RecordSurface};

/// A struct describing one replicated entity kind (`#[derive(Replicated)]`).
///
/// The derive generates the [`KindDef`] and the field values from the same
/// field list, so the schema order and the encoding order cannot drift.
pub trait Replicated {
    /// Kind id.
    const KIND_ID: u16;
    /// Kind name.
    const KIND_NAME: &'static str;
    /// Every entity of this kind is visible to every client by default.
    const GLOBAL: bool;

    /// The kind definition (field names, wire types, priorities, client
    /// correction defaults).
    fn kind_def() -> KindDef;

    /// Interest-plane position (the `x` / `y` fields unless overridden).
    fn position(&self) -> (f32, f32);

    /// Field values in schema order.
    fn field_values(&self) -> Vec<FieldValue>;

    /// The entity for a [`WorldView`].
    fn entity(&self, id: u32) -> EntityState {
        let (x, y) = self.position();
        let mut entity = EntityState::new(id, Self::KIND_ID, x, y, self.field_values());
        entity.global = Self::GLOBAL;
        entity
    }
}

/// Builds an [`EntitySchema`](crate::EntitySchema) from `Replicated` view
/// types, in order: `schema![PlayerView, DroneView]`.
#[macro_export]
macro_rules! schema {
    ($($kind:ty),* $(,)?) => {
        $crate::EntitySchema::new()
            $(.kind(<$kind as $crate::Replicated>::kind_def()))*
    };
}

/// Handle to an entity just pushed with [`WorldView::add`].
#[derive(Debug)]
pub struct Pushed<'a>(&'a mut EntityState);

impl Pushed<'_> {
    /// Visible to every client regardless of view.
    pub fn global(self) -> Self {
        self.0.global = true;
        self
    }

    /// Only within views (overrides a `global` kind).
    pub fn local(self) -> Self {
        self.0.global = false;
        self
    }

    /// Priority multiplier for this entity.
    pub fn priority(self, p: f32) -> Self {
        self.0.priority = p;
        self
    }

    /// Interest-plane position override.
    pub fn at(self, x: f32, y: f32) -> Self {
        self.0.x = x;
        self.0.y = y;
        self
    }
}

impl WorldView {
    /// Push entity `id` described by a `Replicated` view.
    pub fn add<R: Replicated>(&mut self, id: impl Into<crate::EntityId>, view: &R) -> Pushed<'_> {
        self.push(view.entity(id.into().get()));
        Pushed(self.last_mut().expect("just pushed"))
    }
}

/// A compact binary input frame (`#[derive(GameInput)]`).
///
/// [`from_payload`](Self::from_payload) decodes **and sanitizes**, so a
/// zone's `decode_input` is a one-liner:
///
/// ```ignore
/// fn decode_input(&self, _: PlayerId, payload: &[u8]) -> Option<MoveInput> {
///     MoveInput::from_payload(payload)
/// }
/// ```
pub trait GameInput: Sized {
    /// Field layout (pushes any records it mentions).
    fn input_surface(records: &mut Vec<RecordSurface>) -> InputSurface;
    /// Append the raw encoding (no sanitizing).
    fn encode_input(&self, w: &mut Writer);
    /// Decode without sanitizing.
    fn decode_input(r: &mut Reader<'_>) -> Result<Self, DecodeError>;
    /// Clamp `#[range]` fields, clear unknown `#[buttons]` bits, reject
    /// non-finite floats (`None` drops the input).
    fn sanitize(self) -> Option<Self>;

    /// Encoded payload bytes.
    fn to_payload(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode_input(&mut w);
        w.into_bytes()
    }

    /// Decode exactly one frame (trailing bytes reject) and sanitize it.
    fn from_payload(payload: &[u8]) -> Option<Self> {
        let mut r = Reader::new(payload);
        let input = Self::decode_input(&mut r).ok()?;
        r.finish().ok()?;
        input.sanitize()
    }
}

/// A reliable zone event with a stable type id (`#[derive(GameEvent)]` on a
/// struct, or on an enum whose variants are the zone's events).
///
/// Send with [`Ctx::send`](crate::Ctx::send) /
/// [`Ctx::broadcast`](crate::Ctx::broadcast).
pub trait GameEvent: Sized {
    /// Type id of this value.
    fn event_id(&self) -> u32;
    /// Append the payload encoding.
    fn encode_payload(&self, w: &mut Writer);
    /// Decode an Event message: `Ok(None)` when `type_id` is not one of
    /// this type's events.
    fn decode_event(type_id: u32, payload: &[u8]) -> Result<Option<Self>, DecodeError>;
    /// Every event this type declares (pushes any records they mention).
    fn event_surfaces(records: &mut Vec<RecordSurface>) -> Vec<EventSurface>;

    /// Encoded payload bytes.
    fn to_payload(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode_payload(&mut w);
        w.into_bytes()
    }
}

/// Decode a whole event payload as `T` (generated code).
#[doc(hidden)]
pub fn decode_whole<T>(
    payload: &[u8],
    decode: impl FnOnce(&mut Reader<'_>) -> Result<T, DecodeError>,
) -> Result<T, DecodeError> {
    let mut r = Reader::new(payload);
    let value = decode(&mut r)?;
    r.finish()?;
    Ok(value)
}

/// Push `T`'s records (generated code).
#[doc(hidden)]
pub fn records_of<T: Wire>(out: &mut Vec<RecordSurface>) {
    T::records(out);
}
