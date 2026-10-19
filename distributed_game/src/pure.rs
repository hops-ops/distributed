//! Pure game functions, declared once in Rust and compiled to WebAssembly
//! for the browser next to the zone's predicted step.
//!
//! Some client code is neither prediction nor protocol but must still agree
//! with the server exactly: procedural world content (terrain, props), a
//! loot table preview, a path cost. Declare such a function with
//! `#[game_pure]`, name it in the zone's [`GameSurface`](crate::GameSurface)
//! and the generated client calls the *same* code, compiled to
//! `wasm32-unknown-unknown`:
//!
//! ```ignore
//! /// Terrain and props of one cell.
//! #[game_pure(GridCellFn)]
//! pub fn grid_cell(cell: &CellRequest) -> CellContent {
//!     /* … */
//! }
//!
//! pub fn game_surface() -> GameSurface {
//!     GameSurface::for_zone::<Grid>("grid").pure::<GridCellFn>()
//! }
//! ```
//!
//! The function takes nothing or one shared reference to a [`Wire`] value
//! (typically a `#[derive(GameRecord)]` struct) and returns a [`Wire`]
//! value. `distributed game-client` emits `pures.ts`, a typed wrapper per
//! function (`gridCell(cell: CellRequest): CellContent`), and `distributed
//! game-wasm` compiles it into the surface's module.
//!
//! # Binary ABI
//!
//! Pure functions share the module and the ABI of the predicted step
//! ([`crate::prediction`], v1): the same `memory`, `dg_predict_abi` and
//! `dg_predict_buffer` scratch area, and one export per function,
//! `dg_pure_<fn>(arg_len) -> i32`. The caller writes the argument's codec
//! encoding into the scratch buffer and calls the export; a non-negative
//! result is the length of the output's encoding, written at the start of
//! the buffer; negative values are [`AbiError`] codes. The output must fit
//! the 64 KiB buffer. No JSON, no allocation crosses the boundary.
//!
//! # Determinism
//!
//! The rules of [`crate::prediction`] apply: only correctly rounded IEEE-754
//! operations (`+ - * /`, `sqrt`, `floor`, …), no platform transcendentals,
//! no NaN. The function must be a pure function of its argument (caching
//! inside is fine; the module is single-threaded).

use crate::codec::{Wire, WireType};
use crate::prediction::{AbiError, PredictContext};
use crate::protocol::wire::Writer;
use crate::surface::RecordSurface;

/// A pure game function compiled to WebAssembly (see the module docs).
/// Implemented by the `#[game_pure]` attribute.
pub trait GamePure {
    /// Cargo package that declares the function (`env!("CARGO_PKG_NAME")`).
    const PACKAGE: &'static str;
    /// Export suffix: the function is `dg_pure_<EXPORT>` in the module.
    const EXPORT: &'static str;
    /// Argument: `()` for none, otherwise any [`Wire`] value.
    type Arg: PureArg;
    /// Result.
    type Output: Wire;

    /// Run the function.
    fn call(arg: &Self::Arg) -> Self::Output;
}

/// The argument of a [`GamePure`]: `()` (none) or any [`Wire`] value. Same
/// shape as a predicted step's context.
pub trait PureArg: PredictContext {}

impl<T: PredictContext> PureArg for T {}

/// The wire type of `T`'s argument (`None` for `()`) and the records it
/// mentions.
pub(crate) fn arg_type<F: GamePure>(records: &mut Vec<RecordSurface>) -> Option<WireType> {
    F::Arg::context_records(records);
    F::Arg::context_type()
}

/// One call over encoded bytes: what the WASM export does, callable natively
/// (tools, tests). Returns the encoded output.
pub fn call_bytes<F: GamePure>(arg: &[u8]) -> Result<Vec<u8>, AbiError> {
    let arg = F::Arg::decode_context(arg).map_err(|_| AbiError::Argument)?;
    let out = F::call(&arg);
    let mut w = Writer::new();
    out.encode(&mut w);
    Ok(w.into_bytes())
}

/// The ABI over a buffer holding the encoded argument: decodes, calls and
/// writes the encoded output at the start of `buffer`. Returns its length or
/// a negative [`AbiError`] code. The WASM exports call this on the scratch
/// buffer.
pub fn run_abi<F: GamePure>(buffer: &mut [u8], arg_len: u32) -> i32 {
    let len = arg_len as usize;
    if len > buffer.len() {
        return AbiError::TooLarge as i32;
    }
    let out = match call_bytes::<F>(&buffer[..len]) {
        Ok(out) => out,
        Err(error) => return error as i32,
    };
    if out.len() > buffer.len() {
        return AbiError::Output as i32;
    }
    buffer[..out.len()].copy_from_slice(&out);
    out.len() as i32
}

/// Body of a generated `dg_pure_<fn>` export (`#[game_pure]`, `wasm32` only).
#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub fn wasm_export<F: GamePure>(arg_len: u32) -> i32 {
    run_abi::<F>(crate::prediction::scratch_buffer(), arg_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GameRecord;

    #[derive(Debug, Clone, PartialEq, GameRecord)]
    struct Span {
        from: i32,
        to: i32,
    }

    struct Squares;
    impl GamePure for Squares {
        const PACKAGE: &'static str = "test";
        const EXPORT: &'static str = "squares";
        type Arg = Span;
        type Output = Vec<u32>;
        fn call(span: &Span) -> Vec<u32> {
            (span.from..span.to).map(|i| (i * i) as u32).collect()
        }
    }

    struct Answer;
    impl GamePure for Answer {
        const PACKAGE: &'static str = "test";
        const EXPORT: &'static str = "answer";
        type Arg = ();
        type Output = u8;
        fn call(_: &()) -> u8 {
            42
        }
    }

    #[test]
    fn abi_calls_in_place() {
        let arg = crate::codec::to_bytes(&Span { from: -2, to: 3 });
        let mut buffer = vec![0u8; 64];
        buffer[..arg.len()].copy_from_slice(&arg);
        let n = run_abi::<Squares>(&mut buffer, arg.len() as u32);
        assert!(n > 0);
        let out: Vec<u32> = crate::codec::from_bytes(&buffer[..n as usize]).unwrap();
        assert_eq!(out, [4, 1, 0, 1, 4]);
        assert_eq!(call_bytes::<Answer>(&[]).unwrap(), [42]);
    }

    #[test]
    fn abi_reports_errors() {
        let mut buffer = vec![0u8; 4];
        assert_eq!(
            run_abi::<Squares>(&mut buffer, 9),
            AbiError::TooLarge as i32
        );
        assert_eq!(
            run_abi::<Squares>(&mut buffer, 1),
            AbiError::Argument as i32
        );
        assert_eq!(call_bytes::<Answer>(&[1]), Err(AbiError::Argument));
        let arg = crate::codec::to_bytes(&Span { from: 0, to: 100 });
        let mut small = vec![0u8; 8];
        small[..arg.len()].copy_from_slice(&arg);
        assert_eq!(
            run_abi::<Squares>(&mut small, arg.len() as u32),
            AbiError::Output as i32
        );
    }
}
