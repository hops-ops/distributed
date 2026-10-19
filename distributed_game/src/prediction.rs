//! Client prediction steps, declared once in Rust and compiled to
//! WebAssembly for the browser.
//!
//! A zone's **predicted step** is a pure function `(state, input, context,
//! dt) -> state` over the locally controlled entity. The server runs it in
//! `on_tick` (directly or through the code it wraps); the browser runs the
//! *same* function compiled to `wasm32-unknown-unknown`, so client prediction
//! cannot drift from the simulation. Declare it with the `#[predict]`
//! attribute:
//!
//! ```ignore
//! /// What a client predicts for its own `player` entity.
//! #[predict(ArenaStep, kind = "player")]
//! pub fn arena_step(state: &MoveState, input: &MoveInput, _: &(), _dt: f32) -> MoveState {
//!     movement::step(*state, *input)
//! }
//!
//! pub fn game_surface() -> GameSurface {
//!     GameSurface::for_zone::<Arena>("arena").predict::<ArenaStep>()
//! }
//! ```
//!
//! - `State` is a `#[derive(GameRecord)]` struct whose fields are named after
//!   fields of the predicted kind (`x`, `vx`, `cd`, …) and use their plain
//!   scalar types (`f32` for a quantized field). The client rebases on the
//!   authoritative copy of those fields.
//! - `Input` is the zone's `#[derive(GameInput)]` type; the WASM side decodes
//!   it with `from_payload`, so it sees exactly what the server simulates
//!   (ranges clamped, unknown buttons cleared).
//! - `Context` is `()` or a `#[derive(GameRecord)]` struct the client sets
//!   (`prediction.setContext(..)`), e.g. the world seed and cell a step
//!   collides against.
//! - `dt` is the input's frame length in seconds as the client sent it.
//!   Prefer fixed command frames (one input = one tick) and ignore it.
//!
//! # Binary ABI
//!
//! The attribute emits, on `wasm32` only, one export per step:
//! `dg_predict_<fn>(state_len, input_len, context_len, dt) -> i32`. The
//! module also exports `dg_predict_buffer() -> *mut u8` (a
//! [`SCRATCH_BYTES`] scratch area), `dg_predict_abi() -> u32`
//! ([`ABI_VERSION`]) and its memory, and imports nothing. The caller writes
//! `state ‖ input ‖ context` (codec encodings: `state` and `context` with
//! [`Wire`], `input` as its payload) into the buffer and calls the step; a
//! non-negative result is the length of the next state, written at the
//! start of the buffer; negative values are [`AbiError`] codes. No allocation
//! crosses the boundary and nothing is JSON: floats stay binary32.
//!
//! Pure functions (`#[game_pure]`, [`crate::pure`]) live in the same module
//! and share the buffer and ABI version: `dg_pure_<fn>(arg_len) -> i32`.
//!
//! # Determinism
//!
//! Native and WASM results are bit-identical when the step uses only
//! IEEE-754 operations that are correctly rounded on both targets: `+ - * /`,
//! `sqrt`, `abs`, `min`/`max`, `floor`/`ceil`/`trunc`/`round`, comparisons
//! and integer arithmetic. Rust never contracts `a * b + c` into an FMA on
//! its own. **Transcendentals are not portable**: `sin`, `cos`, `atan2`,
//! `exp`, `ln`, `powf` call the platform's libm natively and a different
//! implementation on `wasm32`, which may round differently. Use a
//! deterministic implementation on both sides (the `libm` crate, or a
//! polynomial in plain arithmetic) for anything the step computes with them.
//! NaN payloads may differ, so a step must not produce NaN.

use crate::authoring::GameInput;
use crate::codec::{Wire, WireType};
use crate::protocol::wire::{DecodeError, Reader, Writer};
use crate::surface::RecordSurface;

/// Version of the binary ABI exported as `dg_predict_abi`.
pub const ABI_VERSION: u32 = 1;

/// Size of the scratch buffer behind `dg_predict_buffer` (state, input and
/// context together; the next state is written back into it).
pub const SCRATCH_BYTES: usize = 64 * 1024;

/// A zone's client-predicted step (see the module docs). Implemented by the
/// `#[predict]` attribute; implement it by hand only when the attribute
/// does not fit (and then also export the step, see [`run_abi`]).
pub trait PredictStep {
    /// Name of the predicted kind (the controlled entity's kind).
    const KIND: &'static str;
    /// Cargo package that declares the step (`env!("CARGO_PKG_NAME")`); the
    /// CLI compiles this package to WebAssembly.
    const PACKAGE: &'static str;
    /// Export suffix: the step is `dg_predict_<EXPORT>` in the module.
    const EXPORT: &'static str;
    /// Predicted fields of the kind (a `#[derive(GameRecord)]` struct).
    type State: Wire;
    /// The zone's input frame.
    type Input: GameInput;
    /// Read-only data the client provides (`()` for none).
    type Context: PredictContext;

    /// Advance one input frame.
    fn step(
        state: &Self::State,
        input: &Self::Input,
        context: &Self::Context,
        dt: f32,
    ) -> Self::State;
}

/// The context type of a [`PredictStep`]: `()` (nothing) or any record.
pub trait PredictContext: Sized {
    /// Wire type, or `None` for `()`.
    fn context_type() -> Option<WireType>;
    /// Push the records it mentions.
    fn context_records(out: &mut Vec<RecordSurface>);
    /// Decode a whole context.
    fn decode_context(bytes: &[u8]) -> Result<Self, DecodeError>;
    /// Encode (native tools and tests).
    fn encode_context(&self) -> Vec<u8>;
}

impl PredictContext for () {
    fn context_type() -> Option<WireType> {
        None
    }
    fn context_records(_out: &mut Vec<RecordSurface>) {}
    fn decode_context(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::Invalid("this step takes no context"))
        }
    }
    fn encode_context(&self) -> Vec<u8> {
        Vec::new()
    }
}

impl<T: Wire> PredictContext for T {
    fn context_type() -> Option<WireType> {
        Some(T::wire_type())
    }
    fn context_records(out: &mut Vec<RecordSurface>) {
        T::records(out);
    }
    fn decode_context(bytes: &[u8]) -> Result<Self, DecodeError> {
        crate::codec::from_bytes(bytes)
    }
    fn encode_context(&self) -> Vec<u8> {
        crate::codec::to_bytes(self)
    }
}

/// Negative results of a step export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum AbiError {
    /// The lengths exceed the scratch buffer.
    TooLarge = -1,
    /// The state bytes do not decode.
    State = -2,
    /// The input payload does not decode or is rejected by sanitize.
    Input = -3,
    /// The context bytes do not decode.
    Context = -4,
    /// The next state (or a pure function's output) does not fit the
    /// scratch buffer.
    Output = -5,
    /// A pure function's argument does not decode ([`crate::pure`]).
    Argument = -6,
}

/// One step over encoded bytes: what the WASM export does, callable
/// natively (tools, tests). Returns the encoded next state.
pub fn step_bytes<P: PredictStep>(
    state: &[u8],
    input: &[u8],
    context: &[u8],
    dt: f32,
) -> Result<Vec<u8>, AbiError> {
    let state = crate::codec::from_bytes::<P::State>(state).map_err(|_| AbiError::State)?;
    let input = P::Input::from_payload(input).ok_or(AbiError::Input)?;
    let context = P::Context::decode_context(context).map_err(|_| AbiError::Context)?;
    let next = P::step(&state, &input, &context, dt);
    let mut w = Writer::new();
    next.encode(&mut w);
    Ok(w.into_bytes())
}

/// The ABI over a buffer laid out as `state ‖ input ‖ context`: decodes,
/// steps and writes the next state at the start of `buffer`. Returns its
/// length or a negative [`AbiError`] code. The WASM exports call this on
/// the scratch buffer.
pub fn run_abi<P: PredictStep>(
    buffer: &mut [u8],
    state_len: u32,
    input_len: u32,
    context_len: u32,
    dt: f32,
) -> i32 {
    let (s, i, c) = (state_len as usize, input_len as usize, context_len as usize);
    let Some(total) = s.checked_add(i).and_then(|n| n.checked_add(c)) else {
        return AbiError::TooLarge as i32;
    };
    if total > buffer.len() {
        return AbiError::TooLarge as i32;
    }
    let next = match step_bytes::<P>(&buffer[..s], &buffer[s..s + i], &buffer[s + i..total], dt) {
        Ok(next) => next,
        Err(error) => return error as i32,
    };
    if next.len() > buffer.len() {
        return AbiError::Output as i32;
    }
    buffer[..next.len()].copy_from_slice(&next);
    next.len() as i32
}

/// Decode a state (native tools and tests).
pub fn decode_state<P: PredictStep>(bytes: &[u8]) -> Result<P::State, DecodeError> {
    let mut r = Reader::new(bytes);
    let state = P::State::decode(&mut r)?;
    r.finish()?;
    Ok(state)
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::cell::UnsafeCell;

    struct Scratch(UnsafeCell<[u8; super::SCRATCH_BYTES]>);
    // wasm32-unknown-unknown without atomics is single-threaded.
    unsafe impl Sync for Scratch {}

    static SCRATCH: Scratch = Scratch(UnsafeCell::new([0; super::SCRATCH_BYTES]));

    /// Address of the scratch buffer (`state ‖ input ‖ context` in, next state out).
    #[unsafe(no_mangle)]
    pub extern "C" fn dg_predict_buffer() -> *mut u8 {
        SCRATCH.0.get().cast()
    }

    /// [`super::ABI_VERSION`].
    #[unsafe(no_mangle)]
    pub extern "C" fn dg_predict_abi() -> u32 {
        super::ABI_VERSION
    }

    /// The scratch buffer (generated step and pure function exports).
    pub fn scratch() -> &'static mut [u8] {
        // SAFETY: single-threaded; each export call borrows it once and
        // returns before the next call.
        unsafe { &mut *SCRATCH.0.get() }
    }
}

/// The module's scratch buffer (`wasm32` only).
#[cfg(target_arch = "wasm32")]
pub(crate) fn scratch_buffer() -> &'static mut [u8] {
    wasm::scratch()
}

/// Body of a generated step export (`#[predict]`, `wasm32` only).
#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub fn wasm_export<P: PredictStep>(
    state_len: u32,
    input_len: u32,
    context_len: u32,
    dt: f32,
) -> i32 {
    run_abi::<P>(wasm::scratch(), state_len, input_len, context_len, dt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GameInput, GameRecord};

    #[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
    struct Dot {
        x: f32,
        n: u8,
    }

    #[derive(Debug, Clone, Copy, PartialEq, GameInput)]
    struct Push {
        #[range(min = -1, max = 1)]
        dx: i8,
    }

    #[derive(Debug, Clone, PartialEq, GameRecord)]
    struct Wind {
        speed: f32,
    }

    struct DotStep;
    impl PredictStep for DotStep {
        const KIND: &'static str = "dot";
        const PACKAGE: &'static str = "test";
        const EXPORT: &'static str = "dot";
        type State = Dot;
        type Input = Push;
        type Context = Wind;
        fn step(state: &Dot, input: &Push, context: &Wind, dt: f32) -> Dot {
            Dot {
                x: state.x + f32::from(input.dx) * context.speed * dt,
                n: state.n.wrapping_add(1),
            }
        }
    }

    #[test]
    fn abi_steps_in_place_and_sanitizes_input() {
        let state = crate::codec::to_bytes(&Dot { x: 1.5, n: 7 });
        let context = Wind { speed: 2.0 }.encode_context();
        let mut buffer = vec![0u8; 64];
        let mut at = 0;
        for part in [&state[..], &[5u8][..], &context[..]] {
            buffer[at..at + part.len()].copy_from_slice(part);
            at += part.len();
        }
        let n = run_abi::<DotStep>(
            &mut buffer,
            state.len() as u32,
            1,
            context.len() as u32,
            0.5,
        );
        assert!(n > 0);
        // dx = 5 is clamped to 1, as the server would.
        assert_eq!(
            decode_state::<DotStep>(&buffer[..n as usize]).unwrap(),
            Dot { x: 2.5, n: 8 }
        );
    }

    #[test]
    fn abi_reports_errors() {
        let mut buffer = vec![0u8; 8];
        assert_eq!(
            run_abi::<DotStep>(&mut buffer, 6, 3, 0, 0.0),
            AbiError::TooLarge as i32
        );
        assert_eq!(
            run_abi::<DotStep>(&mut buffer, 1, 1, 4, 0.0),
            AbiError::State as i32
        );
        assert_eq!(
            step_bytes::<DotStep>(
                &crate::codec::to_bytes(&Dot { x: 0.0, n: 0 }),
                &[],
                &[0; 4],
                0.0
            ),
            Err(AbiError::Input)
        );
        assert_eq!(
            step_bytes::<DotStep>(
                &crate::codec::to_bytes(&Dot { x: 0.0, n: 0 }),
                &[0],
                &[0; 3],
                0.0
            ),
            Err(AbiError::Context)
        );
        assert!(<() as PredictContext>::decode_context(&[1]).is_err());
    }
}
