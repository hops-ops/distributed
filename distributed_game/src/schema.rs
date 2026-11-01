//! Entity schema: kinds, component fields, field types and quantization.
//!
//! The game describes what it replicates once ([`EntitySchema`]). The schema
//! is serialized into every Welcome so a generic client can decode Deltas
//! without generated code. Each kind has at most [`MAX_FIELDS`] fields; field
//! order is the wire order and the bit order of update masks.
//!
//! Quantization is specified bit-exactly in `PROTOCOL.md` §4 and computed in
//! `f64` so a JavaScript client (whose numbers are `f64`) reproduces the same
//! integers and decoded `f32` values. Use [`FieldType::quantize_linear`] /
//! [`FieldType::canonicalize`] on the server when prediction must match what
//! clients will see.

use crate::protocol::wire::{DecodeError, Reader, Writer};

/// Maximum fields per entity kind (update masks are a 32-bit varuint).
pub const MAX_FIELDS: usize = 32;

/// `2π` as used by angle quantization.
pub const TAU: f64 = std::f64::consts::TAU;

/// Wire type of one component field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldType {
    /// `u8` 0/1. Tag 0.
    Bool,
    /// One raw byte. Tag 1.
    U8,
    /// varuint, 0..=65535. Tag 2.
    U16,
    /// varuint. Tag 3.
    U32,
    /// zigzag varint. Tag 4.
    I32,
    /// binary32 LE. Tag 5.
    F32,
    /// Clamped linear quantization to `bits` (1..=32), sent as varuint. Tag 6.
    QuantF32 {
        /// Lower bound (inclusive).
        min: f32,
        /// Upper bound (inclusive).
        max: f32,
        /// Precision in bits.
        bits: u8,
    },
    /// Radians wrapped to `[0, 2π)` and quantized to `bits` (1..=32). Tag 7.
    Angle {
        /// Precision in bits.
        bits: u8,
    },
    /// `u8` length + UTF-8 (<= 255 bytes). Tag 8.
    ShortStr,
    /// varuint length + raw bytes. Tag 9.
    Bytes,
}

/// A field value. Decoded values use the canonical variant for the type:
/// Bool→`Bool`, U8/U16/U32→`U`, I32→`I`, F32/QuantF32/Angle→`F`,
/// ShortStr→`Str`, Bytes→`Bytes`. Encoding coerces numeric variants to the
/// field's type (saturating), so games may supply any numeric variant.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// Boolean.
    Bool(bool),
    /// Unsigned integer.
    U(u32),
    /// Signed integer.
    I(i32),
    /// Float.
    F(f32),
    /// Text.
    Str(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
}

impl FieldValue {
    fn as_f64(&self) -> f64 {
        match self {
            Self::Bool(v) => f64::from(u8::from(*v)),
            Self::U(v) => f64::from(*v),
            Self::I(v) => f64::from(*v),
            Self::F(v) => f64::from(*v),
            Self::Str(_) | Self::Bytes(_) => 0.0,
        }
    }
}

impl FieldType {
    /// `QuantF32` over `[-range, range - range / 2^(bits-1)]`: the grid step
    /// is `range / 2^(bits-1)` and **zero is exactly representable**.
    ///
    /// A symmetric `[-r, r]` range has an odd number of intervals
    /// (`2^bits - 1`), so 0 falls between two grid points: a velocity of 0
    /// decodes as ±half a step and a resting entity creeps. Use this for
    /// signed quantities (velocities, offsets). `bits` must be 1..=32.
    pub const fn quant_centered(range: f32, bits: u8) -> Self {
        let half = (1u64 << (bits - 1)) as f32;
        Self::QuantF32 {
            min: -range,
            max: range - range / half,
            bits,
        }
    }

    /// Clamped linear quantization (`PROTOCOL.md` §4.1).
    pub fn quantize_linear(v: f32, min: f32, max: f32, bits: u8) -> u32 {
        let maxq = max_q(bits);
        let (lo, hi) = (f64::from(min), f64::from(max));
        if hi <= lo {
            return 0;
        }
        let mut x = f64::from(v);
        if x.is_nan() {
            x = lo;
        }
        let x = x.clamp(lo, hi);
        let t = (x - lo) / (hi - lo);
        (t * maxq + 0.5).floor().min(maxq) as u32
    }

    /// Inverse of [`quantize_linear`](Self::quantize_linear).
    pub fn dequantize_linear(q: u32, min: f32, max: f32, bits: u8) -> f32 {
        let maxq = max_q(bits);
        let (lo, hi) = (f64::from(min), f64::from(max));
        (lo + (f64::from(q) / maxq) * (hi - lo)) as f32
    }

    /// Angle quantization (`PROTOCOL.md` §4.2).
    pub fn quantize_angle(v: f32, bits: u8) -> u32 {
        let steps = steps(bits);
        let mut a = f64::from(v);
        if !a.is_finite() {
            return 0;
        }
        a %= TAU;
        if a < 0.0 {
            a += TAU;
        }
        let q = ((a / TAU) * steps + 0.5).floor();
        (q % steps) as u32
    }

    /// Inverse of [`quantize_angle`](Self::quantize_angle).
    pub fn dequantize_angle(q: u32, bits: u8) -> f32 {
        ((f64::from(q) * TAU) / steps(bits)) as f32
    }

    /// The value a client will decode for `value` (round-trip through the wire).
    pub fn canonicalize(&self, value: &FieldValue) -> FieldValue {
        let mut w = Writer::new();
        self.encode(&mut w, value);
        self.decode(&mut Reader::new(w.as_bytes()))
            .expect("encoded field must decode")
    }

    /// Wire tag.
    pub fn tag(&self) -> u8 {
        match self {
            Self::Bool => 0,
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 3,
            Self::I32 => 4,
            Self::F32 => 5,
            Self::QuantF32 { .. } => 6,
            Self::Angle { .. } => 7,
            Self::ShortStr => 8,
            Self::Bytes => 9,
        }
    }

    /// Encode a value of this type (coercing numerics).
    pub fn encode(&self, w: &mut Writer, value: &FieldValue) {
        let n = value.as_f64();
        match self {
            Self::Bool => {
                w.bool(n != 0.0);
            }
            Self::U8 => {
                w.u8(n.clamp(0.0, 255.0) as u8);
            }
            Self::U16 => {
                w.varuint(n.clamp(0.0, 65535.0) as u32);
            }
            Self::U32 => {
                w.varuint(match value {
                    FieldValue::U(v) => *v,
                    _ => n.clamp(0.0, f64::from(u32::MAX)) as u32,
                });
            }
            Self::I32 => {
                w.varint(match value {
                    FieldValue::I(v) => *v,
                    _ => n.clamp(f64::from(i32::MIN), f64::from(i32::MAX)) as i32,
                });
            }
            Self::F32 => {
                w.f32(n as f32);
            }
            Self::QuantF32 { min, max, bits } => {
                w.varuint(Self::quantize_linear(n as f32, *min, *max, *bits));
            }
            Self::Angle { bits } => {
                w.varuint(Self::quantize_angle(n as f32, *bits));
            }
            Self::ShortStr => {
                w.short_string(match value {
                    FieldValue::Str(s) => s,
                    _ => "",
                });
            }
            Self::Bytes => {
                w.bytes(match value {
                    FieldValue::Bytes(b) => b,
                    FieldValue::Str(s) => s.as_bytes(),
                    _ => &[],
                });
            }
        }
    }

    /// Decode a value of this type.
    pub fn decode(&self, r: &mut Reader<'_>) -> Result<FieldValue, DecodeError> {
        Ok(match self {
            Self::Bool => FieldValue::Bool(r.bool()?),
            Self::U8 => FieldValue::U(u32::from(r.u8()?)),
            Self::U16 => {
                let v = r.varuint()?;
                if v > 65535 {
                    return Err(DecodeError::Invalid("u16 out of range"));
                }
                FieldValue::U(v)
            }
            Self::U32 => FieldValue::U(r.varuint()?),
            Self::I32 => FieldValue::I(r.varint()?),
            Self::F32 => FieldValue::F(r.f32()?),
            Self::QuantF32 { min, max, bits } => {
                let q = r.varuint()?;
                if f64::from(q) > max_q(*bits) {
                    return Err(DecodeError::Invalid("quantized value out of range"));
                }
                FieldValue::F(Self::dequantize_linear(q, *min, *max, *bits))
            }
            Self::Angle { bits } => {
                let q = r.varuint()?;
                if f64::from(q) >= steps(*bits) {
                    return Err(DecodeError::Invalid("angle out of range"));
                }
                FieldValue::F(Self::dequantize_angle(q, *bits))
            }
            Self::ShortStr => FieldValue::Str(r.short_string()?),
            Self::Bytes => FieldValue::Bytes(r.bytes()?.to_vec()),
        })
    }

    fn write_def(&self, w: &mut Writer) {
        w.u8(self.tag());
        match self {
            Self::QuantF32 { min, max, bits } => {
                w.f32(*min).f32(*max).u8(*bits);
            }
            Self::Angle { bits } => {
                w.u8(*bits);
            }
            _ => {}
        }
    }

    fn read_def(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let tag = r.u8()?;
        Ok(match tag {
            0 => Self::Bool,
            1 => Self::U8,
            2 => Self::U16,
            3 => Self::U32,
            4 => Self::I32,
            5 => Self::F32,
            6 => {
                let (min, max, bits) = (r.f32()?, r.f32()?, r.u8()?);
                check_bits(bits)?;
                Self::QuantF32 { min, max, bits }
            }
            7 => {
                let bits = r.u8()?;
                check_bits(bits)?;
                Self::Angle { bits }
            }
            8 => Self::ShortStr,
            9 => Self::Bytes,
            other => return Err(DecodeError::UnknownFieldType(other)),
        })
    }
}

fn check_bits(bits: u8) -> Result<(), DecodeError> {
    if (1..=32).contains(&bits) {
        Ok(())
    } else {
        Err(DecodeError::Invalid("quantization bits must be 1..=32"))
    }
}

fn max_q(bits: u8) -> f64 {
    ((1u64 << bits.clamp(1, 32)) - 1) as f64
}

fn steps(bits: u8) -> f64 {
    (1u64 << bits.clamp(1, 32)) as f64
}

/// How a predicting client resolves a misprediction of one field (the JS
/// SDK's `CorrectionPolicy`). Client metadata only: never sent on the wire,
/// exported in the [`GameSurface`](crate::surface::GameSurface) so generated
/// clients get per-field defaults.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Correction {
    /// Adopt the rebased value, easing the visual error out over `duration_ms`.
    Ease {
        /// Ease duration (ms).
        #[serde(rename = "durationMs")]
        duration_ms: u32,
        /// An error larger than this cuts instead, every eased field of the
        /// entity at once (a teleport — a respawn, a server-side move — is a
        /// cut, not a misprediction to slide over).
        #[serde(rename = "snapAbove", default, skip_serializing_if = "Option::is_none")]
        snap_above: Option<f32>,
    },
    /// Adopt the rebased value immediately.
    Snap,
    /// Server-owned: show the authoritative value, never replay inputs over it.
    Replace,
}

/// One component field of a kind.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    /// Name (informational for generic clients).
    pub name: String,
    /// Wire type.
    pub ty: FieldType,
    /// Server-side replication priority multiplier (not sent on the wire).
    pub priority: f32,
    /// Client correction default (not sent on the wire; see [`Correction`]).
    pub correction: Option<Correction>,
}

/// An entity kind: id, name and ordered fields.
#[derive(Debug, Clone, PartialEq)]
pub struct KindDef {
    /// Kind id (varuint on the wire).
    pub id: u16,
    /// Name.
    pub name: String,
    /// Ordered fields (<= [`MAX_FIELDS`]).
    pub fields: Vec<FieldDef>,
    /// Server-side priority multiplier (not sent on the wire).
    pub priority: f32,
    /// Exempt from distance-based update rates
    /// ([`UpdateRates`](crate::UpdateRates); server-side, not on the wire).
    pub every_patch: bool,
}

impl KindDef {
    /// New kind with priority 1.
    pub fn new(id: u16, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            fields: Vec::new(),
            priority: 1.0,
            every_patch: false,
        }
    }

    /// Append a field with priority 1.
    pub fn field(self, name: impl Into<String>, ty: FieldType) -> Self {
        self.field_with_priority(name, ty, 1.0)
    }

    /// Append a field with an explicit priority.
    pub fn field_with_priority(mut self, name: impl Into<String>, ty: FieldType, p: f32) -> Self {
        assert!(self.fields.len() < MAX_FIELDS, "at most 32 fields per kind");
        if let FieldType::QuantF32 { bits, .. } | FieldType::Angle { bits } = ty {
            assert!((1..=32).contains(&bits), "quantization bits must be 1..=32");
        }
        self.fields.push(FieldDef {
            name: name.into(),
            ty,
            priority: p,
            correction: None,
        });
        self
    }

    /// Set the client correction default of the last added field.
    ///
    /// # Panics
    /// If the kind has no fields yet.
    pub fn correct(mut self, correction: Correction) -> Self {
        self.fields
            .last_mut()
            .expect("add a field before its correction")
            .correction = Some(correction);
        self
    }

    /// Set the kind priority multiplier.
    pub fn priority(mut self, p: f32) -> Self {
        self.priority = p;
        self
    }

    /// Update every patch at any distance (no distance-based rates).
    pub fn every_patch(mut self) -> Self {
        self.every_patch = true;
        self
    }
}

/// All replicated kinds of a zone type.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EntitySchema {
    kinds: Vec<KindDef>,
}

impl EntitySchema {
    /// Empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a kind (ids must be unique).
    pub fn kind(mut self, kind: KindDef) -> Self {
        assert!(self.get(kind.id).is_none(), "duplicate kind id {}", kind.id);
        self.kinds.push(kind);
        self
    }

    /// Kinds in declaration order.
    pub fn kinds(&self) -> &[KindDef] {
        &self.kinds
    }

    /// Look up a kind.
    pub fn get(&self, id: u16) -> Option<&KindDef> {
        self.kinds.iter().find(|kind| kind.id == id)
    }

    /// Serialize (`PROTOCOL.md` §5).
    pub fn encode(&self, w: &mut Writer) {
        w.varuint(self.kinds.len() as u32);
        for kind in &self.kinds {
            w.varuint(u32::from(kind.id))
                .string(&kind.name)
                .varuint(kind.fields.len() as u32);
            for field in &kind.fields {
                w.string(&field.name);
                field.ty.write_def(w);
            }
        }
    }

    /// Deserialize; server-only priorities decode as 1.
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let count = r.varuint()?;
        let mut schema = Self::new();
        for _ in 0..count {
            let id = r.varuint()?;
            let id = u16::try_from(id).map_err(|_| DecodeError::Invalid("kind id > u16"))?;
            let name = r.string()?;
            let field_count = r.varuint()? as usize;
            if field_count > MAX_FIELDS {
                return Err(DecodeError::Invalid("more than 32 fields"));
            }
            let mut kind = KindDef::new(id, name);
            for _ in 0..field_count {
                let name = r.string()?;
                let ty = FieldType::read_def(r)?;
                kind = kind.field(name, ty);
            }
            if schema.get(id).is_some() {
                return Err(DecodeError::Invalid("duplicate kind id"));
            }
            schema.kinds.push(kind);
        }
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centered_quantization_represents_zero_exactly() {
        for (range, bits) in [
            (16.0, 12),
            (16.0, 14),
            (20.0, 18),
            (8.0, 12),
            (64.0, 16),
            (1.0, 2),
        ] {
            let ty = FieldType::quant_centered(range, bits);
            assert_eq!(ty.canonicalize(&FieldValue::F(0.0)), FieldValue::F(0.0));
            assert_eq!(
                ty.canonicalize(&FieldValue::F(-range)),
                FieldValue::F(-range)
            );
            let step = range / (1u64 << (bits - 1)) as f32;
            assert_eq!(ty.canonicalize(&FieldValue::F(step)), FieldValue::F(step));
        }
        // The symmetric range cannot: 0 lands half a step off.
        let symmetric = FieldType::QuantF32 {
            min: -16.0,
            max: 16.0,
            bits: 12,
        };
        assert_ne!(
            symmetric.canonicalize(&FieldValue::F(0.0)),
            FieldValue::F(0.0)
        );
    }

    #[test]
    fn linear_quantization_rounds_half_up_and_clamps() {
        let q = |v| FieldType::quantize_linear(v, -10.0, 10.0, 8);
        assert_eq!(q(-10.0), 0);
        assert_eq!(q(10.0), 255);
        assert_eq!(q(99.0), 255);
        assert_eq!(q(f32::NAN), 0);
        assert_eq!(q(0.0), 128); // 127.5 rounds up
        let back = FieldType::dequantize_linear(128, -10.0, 10.0, 8);
        assert!((back - 0.039_215_688).abs() < 1e-6);
    }

    #[test]
    fn angle_wraps() {
        let bits = 8;
        assert_eq!(FieldType::quantize_angle(0.0, bits), 0);
        assert_eq!(FieldType::quantize_angle(-0.000_001, bits), 0);
        assert_eq!(FieldType::quantize_angle(std::f32::consts::PI, bits), 128);
        assert_eq!(
            FieldType::quantize_angle(-std::f32::consts::FRAC_PI_2, bits),
            192
        );
    }

    #[test]
    fn schema_round_trips() {
        let schema = EntitySchema::new().kind(
            KindDef::new(3, "orb")
                .field(
                    "x",
                    FieldType::QuantF32 {
                        min: 0.0,
                        max: 64.0,
                        bits: 12,
                    },
                )
                .field("yaw", FieldType::Angle { bits: 10 })
                .field("name", FieldType::ShortStr),
        );
        let mut w = Writer::new();
        schema.encode(&mut w);
        let mut r = Reader::new(w.as_bytes());
        assert_eq!(EntitySchema::decode(&mut r).unwrap(), schema);
        r.finish().unwrap();
    }
}
