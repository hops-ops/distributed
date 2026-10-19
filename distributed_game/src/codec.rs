//! Typed payload codec behind the authoring derives.
//!
//! Entity fields, inputs and events all use **one** value codec. Its scalar
//! encodings are exactly the entity [`FieldType`] encodings (`PROTOCOL.md`
//! §4), so a value means the same bytes whether it is replicated, sent as an
//! input or carried in an event. Payloads (inputs, events) may additionally
//! use types an entity field cannot: `i8`, `i16`, `u64`, `f64`, long
//! strings, lists, options and nested records.
//!
//! | Rust | [`WireType`] | bytes |
//! |---|---|---|
//! | `bool` | `bool` | `u8` 0/1 |
//! | `u8` / `i8` | `u8` / `i8` | one byte (two's complement for `i8`) |
//! | `u16` / `u32` | `u16` / `u32` | varuint (`u16` checked ≤ 65535) |
//! | `i16` / `i32` | `i16` / `i32` | zigzag varint (`i16` range-checked) |
//! | `u64` | `u64` | varuint up to `2^53 - 1` (saturating; exact in JS) |
//! | `f32` / `f64` | `f32` / `f64` | binary32 / binary64 LE |
//! | `f32` + `#[quant(..)]` | `quant` | varuint (§4.1) |
//! | `f32` + `#[angle(..)]` | `angle` | varuint (§4.2) |
//! | `String` | `short_str` | `u8` length + UTF-8 (truncated to 255 bytes) |
//! | `String` + `#[long]` | `str` | varuint length + UTF-8 |
//! | `Vec<u8>` | `bytes` | varuint length + bytes |
//! | `Vec<T>` | `list` | varuint count + items |
//! | `Option<T>` | `optional` | `u8` 0 (none) / 1 + value |
//! | `#[derive(GameRecord)]` struct | `record` | fields in declaration order |
//!
//! The derives in `distributed_game_macros` generate the calls; use this
//! module directly only for hand-written payloads.

use serde::{Deserialize, Serialize};

use crate::protocol::wire::{DecodeError, Reader, Writer};
use crate::schema::{FieldType, FieldValue};
use crate::surface::RecordSurface;

/// Wire type of one payload or entity field, as exported in the
/// [`GameSurface`](crate::surface::GameSurface).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireType {
    /// `u8` 0/1.
    Bool,
    /// One byte.
    U8,
    /// One byte, two's complement.
    I8,
    /// varuint ≤ 65535.
    U16,
    /// zigzag varint in `i16` range.
    I16,
    /// varuint.
    U32,
    /// zigzag varint.
    I32,
    /// varuint up to `2^53 - 1`.
    U64,
    /// binary32 LE.
    F32,
    /// binary64 LE.
    F64,
    /// Clamped linear quantization (`FieldType::QuantF32`).
    Quant {
        /// Lower bound.
        min: f32,
        /// Upper bound.
        max: f32,
        /// Precision in bits (1..=32).
        bits: u8,
    },
    /// Wrapped angle quantization (`FieldType::Angle`).
    Angle {
        /// Precision in bits (1..=32).
        bits: u8,
    },
    /// `u8` length + UTF-8.
    ShortStr,
    /// varuint length + UTF-8.
    Str,
    /// varuint length + bytes.
    Bytes,
    /// varuint count + items.
    List {
        /// Item type.
        item: Box<WireType>,
    },
    /// `u8` presence + value.
    Optional {
        /// Value type.
        item: Box<WireType>,
    },
    /// A named record (see the surface's `records`).
    Record {
        /// Record name (the Rust type name).
        name: String,
    },
}

impl WireType {
    /// The equivalent entity [`FieldType`], if this type can be replicated.
    pub fn field_type(&self) -> Option<FieldType> {
        Some(match self {
            Self::Bool => FieldType::Bool,
            Self::U8 => FieldType::U8,
            Self::U16 => FieldType::U16,
            Self::U32 => FieldType::U32,
            Self::I32 => FieldType::I32,
            Self::F32 => FieldType::F32,
            Self::Quant { min, max, bits } => FieldType::QuantF32 {
                min: *min,
                max: *max,
                bits: *bits,
            },
            Self::Angle { bits } => FieldType::Angle { bits: *bits },
            Self::ShortStr => FieldType::ShortStr,
            Self::Bytes => FieldType::Bytes,
            _ => return None,
        })
    }

    /// The payload type of an entity field type.
    pub fn of_field(ty: FieldType) -> Self {
        match ty {
            FieldType::Bool => Self::Bool,
            FieldType::U8 => Self::U8,
            FieldType::U16 => Self::U16,
            FieldType::U32 => Self::U32,
            FieldType::I32 => Self::I32,
            FieldType::F32 => Self::F32,
            FieldType::QuantF32 { min, max, bits } => Self::Quant { min, max, bits },
            FieldType::Angle { bits } => Self::Angle { bits },
            FieldType::ShortStr => Self::ShortStr,
            FieldType::Bytes => Self::Bytes,
        }
    }
}

/// A Rust type with a default payload encoding (see the module table).
///
/// Implemented for the scalar types, `String`, `Vec<T>`, `Option<T>`, and by
/// `#[derive(GameRecord)]` / `#[derive(GameEvent)]` for structs.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no game payload encoding",
    label = "unsupported payload field type",
    note = "supported: bool, u8, i8, u16, i16, u32, i32, u64, f32, f64, String, Vec<T>, Option<T> and `#[derive(GameRecord)]` structs"
)]
pub trait Wire: Sized {
    /// Wire type descriptor.
    fn wire_type() -> WireType;
    /// Append the encoding.
    fn encode(&self, w: &mut Writer);
    /// Decode one value.
    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError>;
    /// Push the definitions of every record this type mentions (itself
    /// included), depth first; duplicates are removed by the surface.
    fn records(_out: &mut Vec<RecordSurface>) {}
}

macro_rules! wire_scalar {
    ($ty:ty, $wire:ident, |$w:ident, $v:ident| $enc:expr, |$r:ident| $dec:expr) => {
        impl Wire for $ty {
            fn wire_type() -> WireType {
                WireType::$wire
            }
            fn encode(&self, $w: &mut Writer) {
                let $v = *self;
                $enc;
            }
            fn decode($r: &mut Reader<'_>) -> Result<Self, DecodeError> {
                $dec
            }
        }
    };
}

wire_scalar!(bool, Bool, |w, v| w.bool(v), |r| r.bool());
wire_scalar!(u8, U8, |w, v| w.u8(v), |r| r.u8());
wire_scalar!(i8, I8, |w, v| w.u8(v as u8), |r| Ok(r.u8()? as i8));
wire_scalar!(u16, U16, |w, v| w.varuint(u32::from(v)), |r| {
    u16::try_from(r.varuint()?).map_err(|_| DecodeError::Invalid("u16 out of range"))
});
wire_scalar!(i16, I16, |w, v| w.varint(i32::from(v)), |r| {
    i16::try_from(r.varint()?).map_err(|_| DecodeError::Invalid("i16 out of range"))
});
wire_scalar!(u32, U32, |w, v| w.varuint(v), |r| r.varuint());
wire_scalar!(i32, I32, |w, v| w.varint(v), |r| r.varint());
wire_scalar!(u64, U64, |w, v| w.varuint64(v), |r| r.varuint64());
wire_scalar!(f32, F32, |w, v| w.f32(v), |r| r.f32());
wire_scalar!(f64, F64, |w, v| w.f64(v), |r| r.f64());

impl Wire for String {
    fn wire_type() -> WireType {
        WireType::ShortStr
    }
    fn encode(&self, w: &mut Writer) {
        w.short_string(self);
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        r.short_string()
    }
}

impl<T: Wire> Wire for Vec<T> {
    fn wire_type() -> WireType {
        WireType::List {
            item: Box::new(T::wire_type()),
        }
    }
    fn encode(&self, w: &mut Writer) {
        w.varuint(u32::try_from(self.len()).unwrap_or(u32::MAX));
        for item in self {
            item.encode(w);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let count = r.varuint()? as usize;
        // Every item takes at least one byte: bound the allocation.
        if count > r.remaining() {
            return Err(DecodeError::UnexpectedEof);
        }
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(T::decode(r)?);
        }
        Ok(out)
    }
    fn records(out: &mut Vec<RecordSurface>) {
        T::records(out);
    }
}

impl<T: Wire> Wire for Option<T> {
    fn wire_type() -> WireType {
        WireType::Optional {
            item: Box::new(T::wire_type()),
        }
    }
    fn encode(&self, w: &mut Writer) {
        match self {
            None => {
                w.u8(0);
            }
            Some(value) => {
                w.u8(1);
                value.encode(w);
            }
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match r.u8()? {
            0 => Ok(None),
            1 => Ok(Some(T::decode(r)?)),
            _ => Err(DecodeError::Invalid("option tag must be 0 or 1")),
        }
    }
    fn records(out: &mut Vec<RecordSurface>) {
        T::records(out);
    }
}

/// `#[quant(..)]` / `#[angle(..)]` / `#[long]` field encodings (used by the
/// generated code).
pub mod special {
    use super::*;

    /// An `f32` encoded as entity field type `ty` (quantized or angle).
    pub fn encode_f32(w: &mut Writer, ty: FieldType, v: f32) {
        ty.encode(w, &FieldValue::F(v));
    }

    /// Inverse of [`encode_f32`] (range-checked).
    pub fn decode_f32(r: &mut Reader<'_>, ty: FieldType) -> Result<f32, DecodeError> {
        match ty.decode(r)? {
            FieldValue::F(v) => Ok(v),
            _ => Err(DecodeError::Invalid("expected a float field type")),
        }
    }

    /// varuint-length string (`#[long]`).
    pub fn encode_long(w: &mut Writer, v: &str) {
        w.string(v);
    }

    /// Inverse of [`encode_long`].
    pub fn decode_long(r: &mut Reader<'_>) -> Result<String, DecodeError> {
        r.string()
    }
}

/// Encode a whole value into a fresh buffer.
pub fn to_bytes<T: Wire>(value: &T) -> Vec<u8> {
    let mut w = Writer::new();
    value.encode(&mut w);
    w.into_bytes()
}

/// Decode a whole value; trailing bytes are an error.
pub fn from_bytes<T: Wire>(bytes: &[u8]) -> Result<T, DecodeError> {
    let mut r = Reader::new(bytes);
    let value = T::decode(&mut r)?;
    r.finish()?;
    Ok(value)
}

/// A Rust type usable as a replicated entity field (`#[derive(Replicated)]`).
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a replicated entity field",
    label = "unsupported entity field type",
    note = "supported: bool, u8, u16, u32, i32, f32 (optionally #[quant]/#[angle]), String (short_str) and Vec<u8> (bytes)"
)]
pub trait EntityField {
    /// Default wire type (overridden by `#[quant]` / `#[angle]` on `f32`).
    const FIELD_TYPE: FieldType;
    /// This value as a [`FieldValue`].
    fn to_field_value(&self) -> FieldValue;
}

macro_rules! entity_field {
    ($ty:ty, $ft:expr, |$v:ident| $value:expr) => {
        impl EntityField for $ty {
            const FIELD_TYPE: FieldType = $ft;
            fn to_field_value(&self) -> FieldValue {
                let $v = self;
                $value
            }
        }
    };
}

entity_field!(bool, FieldType::Bool, |v| FieldValue::Bool(*v));
entity_field!(u8, FieldType::U8, |v| FieldValue::U(u32::from(*v)));
entity_field!(u16, FieldType::U16, |v| FieldValue::U(u32::from(*v)));
entity_field!(u32, FieldType::U32, |v| FieldValue::U(*v));
entity_field!(i32, FieldType::I32, |v| FieldValue::I(*v));
entity_field!(f32, FieldType::F32, |v| FieldValue::F(*v));
entity_field!(String, FieldType::ShortStr, |v| FieldValue::Str(v.clone()));
entity_field!(Vec<u8>, FieldType::Bytes, |v| FieldValue::Bytes(v.clone()));

/// Marker: a field type that accepts `#[quant(..)]` / `#[angle(..)]`.
#[diagnostic::on_unimplemented(
    message = "`#[quant]` / `#[angle]` need an `f32` field, not `{Self}`",
    label = "quantize an f32"
)]
pub trait QuantizedF32 {
    /// The value.
    fn get(&self) -> f32;
    /// From a decoded value.
    fn from_f32(v: f32) -> Self;
}

impl QuantizedF32 for f32 {
    fn get(&self) -> f32 {
        *self
    }
    fn from_f32(v: f32) -> Self {
        v
    }
}

/// Marker: a field type that accepts `#[long]`.
#[diagnostic::on_unimplemented(
    message = "`#[long]` needs a `String` field, not `{Self}`",
    label = "long strings are Strings"
)]
pub trait LongString {
    /// The text.
    fn text(&self) -> &str;
    /// From decoded text.
    fn from_text(v: String) -> Self;
}

impl LongString for String {
    fn text(&self) -> &str {
        self
    }
    fn from_text(v: String) -> Self {
        v
    }
}

/// A scalar usable in a `#[derive(GameInput)]` struct, and how its
/// `#[range(..)]` clamps.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a game input field",
    label = "unsupported input field type",
    note = "inputs are small fixed scalars: bool, u8, i8, u16, i16, u32, i32 or f32 (optionally #[quant]/#[angle])"
)]
pub trait InputField: Wire + Copy {
    /// Clamp into `[min, max]` (given as `f64`); `None` rejects the input
    /// (non-finite floats).
    fn sanitize_range(self, min: f64, max: f64) -> Option<Self>;
    /// Validity without a range (floats must be finite).
    fn sanitize_plain(self) -> Option<Self> {
        Some(self)
    }
}

macro_rules! input_int {
    ($($ty:ty),*) => {$(
        impl InputField for $ty {
            fn sanitize_range(self, min: f64, max: f64) -> Option<Self> {
                Some((self as f64).clamp(min, max) as $ty)
            }
        }
    )*};
}

input_int!(u8, i8, u16, i16, u32, i32);

impl InputField for bool {
    fn sanitize_range(self, _min: f64, _max: f64) -> Option<Self> {
        Some(self)
    }
}

impl InputField for f32 {
    fn sanitize_range(self, min: f64, max: f64) -> Option<Self> {
        self.is_finite()
            .then(|| f64::from(self).clamp(min, max) as f32)
    }
    fn sanitize_plain(self) -> Option<Self> {
        self.is_finite().then_some(self)
    }
}

/// A bit-flag field (`#[buttons(..)]`): unknown bits are cleared.
#[diagnostic::on_unimplemented(
    message = "`#[buttons]` needs a u8, u16 or u32 field, not `{Self}`",
    label = "button bits live in an unsigned integer"
)]
pub trait ButtonBits: Copy {
    /// Keep only the bits of `mask`.
    fn mask(self, mask: u32) -> Self;
}

macro_rules! button_bits {
    ($($ty:ty),*) => {$(
        impl ButtonBits for $ty {
            fn mask(self, mask: u32) -> Self {
                (u32::from(self) & mask) as $ty
            }
        }
    )*};
}

button_bits!(u8, u16, u32);

#[cfg(test)]
mod tests {
    use super::*;

    fn field_bytes(ty: FieldType, value: &FieldValue) -> Vec<u8> {
        let mut w = Writer::new();
        ty.encode(&mut w, value);
        w.into_bytes()
    }

    #[test]
    fn scalars_match_entity_field_encodings() {
        for (bytes, field) in [
            (
                to_bytes(&true),
                field_bytes(FieldType::Bool, &FieldValue::Bool(true)),
            ),
            (
                to_bytes(&200u8),
                field_bytes(FieldType::U8, &FieldValue::U(200)),
            ),
            (
                to_bytes(&300u16),
                field_bytes(FieldType::U16, &FieldValue::U(300)),
            ),
            (
                to_bytes(&70_000u32),
                field_bytes(FieldType::U32, &FieldValue::U(70_000)),
            ),
            (
                to_bytes(&-5i32),
                field_bytes(FieldType::I32, &FieldValue::I(-5)),
            ),
            (
                to_bytes(&1.5f32),
                field_bytes(FieldType::F32, &FieldValue::F(1.5)),
            ),
            (
                to_bytes(&"hé".to_string()),
                field_bytes(FieldType::ShortStr, &FieldValue::Str("hé".into())),
            ),
        ] {
            assert_eq!(bytes, field);
        }
    }

    #[test]
    fn composite_types_round_trip() {
        let value: (Vec<Option<i8>>, u64) = (vec![Some(-128), None, Some(127)], (1 << 53) - 1);
        let mut w = Writer::new();
        value.0.encode(&mut w);
        value.1.encode(&mut w);
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        assert_eq!(Vec::<Option<i8>>::decode(&mut r).unwrap(), value.0);
        assert_eq!(u64::decode(&mut r).unwrap(), value.1);
        r.finish().unwrap();
        // u64 saturates at 2^53 - 1 so JavaScript decodes it exactly.
        assert_eq!(
            from_bytes::<u64>(&to_bytes(&u64::MAX)).unwrap(),
            (1 << 53) - 1
        );
        assert!(from_bytes::<u16>(&to_bytes(&70_000u32)).is_err());
        assert!(from_bytes::<Option<u8>>(&[2]).is_err());
        assert!(from_bytes::<Vec<u8>>(&[200]).is_err());
    }

    #[test]
    fn input_ranges_clamp_and_floats_must_be_finite() {
        assert_eq!((-128i8).sanitize_range(-127.0, 127.0), Some(-127));
        assert_eq!(9u8.sanitize_range(0.0, 4.0), Some(4));
        assert_eq!(f32::NAN.sanitize_range(-1.0, 1.0), None);
        assert_eq!(f32::INFINITY.sanitize_plain(), None);
        assert_eq!(2.5f32.sanitize_range(-1.0, 1.0), Some(1.0));
        assert_eq!(0b111u8.mask(0b101), 0b101);
    }

    #[test]
    fn wire_types_serialize_tagged() {
        let ty = Vec::<Option<f32>>::wire_type();
        let json = serde_json::to_value(&ty).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "list", "item": {"type": "optional", "item": {"type": "f32"}}})
        );
        assert_eq!(serde_json::from_value::<WireType>(json).unwrap(), ty);
        assert_eq!(
            WireType::of_field(FieldType::quant_centered(16.0, 12)).field_type(),
            Some(FieldType::quant_centered(16.0, 12))
        );
    }
}
