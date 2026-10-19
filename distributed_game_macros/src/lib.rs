//! Authoring derives for `distributed_game`. Use them through the runtime
//! crate's re-exports (`distributed_game::Replicated`, …); generated code
//! refers to `distributed_game` (renames are resolved).
//!
//! | derive | on | generates |
//! |---|---|---|
//! | [`Replicated`] | struct | `Replicated`: `KindDef` + field values from one field list |
//! | [`GameInput`] | struct | `GameInput`: compact codec, sanitize from `#[range]` / `#[buttons]` |
//! | [`GameEvent`] | struct / enum | `GameEvent`: stable ids, typed payload codec |
//! | [`GameRecord`] | struct | `Wire`: a nested payload record |
//! | [`FactCommand`] | struct | `FactCommand`: the cell command name of a fact input |
//!
//! Field attributes (where they apply):
//!
//! - `#[quant(min = A, max = B, bits = N)]` / `#[quant(centered = R, bits = N)]`
//!   (zero exactly representable, `FieldType::quant_centered`) and
//!   `#[angle(bits = N)]` on `f32` fields; `#[field_type(EXPR)]` uses any
//!   `FieldType` expression (e.g. a shared `const POS: FieldType`).
//! - `#[long]` on a `String` payload field: varuint length instead of the
//!   255-byte short string.
//! - Replicated only: `#[priority(P)]`, `#[correct(ease = MS | ease | snap | replace)]`.
//! - GameInput only: `#[range(min = A, max = B)]` (clamp), `#[buttons(a, b, ..)]`
//!   (bit flags, unknown bits cleared; also generates `Type::A`, `Type::B` consts).

mod event;
mod fact;
mod input;
mod replicated;
mod shared;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

fn finish(result: syn::Result<proc_macro2::TokenStream>) -> TokenStream {
    result.unwrap_or_else(|e| e.to_compile_error()).into()
}

/// One replicated entity kind: `#[replicated(kind = "mob", id = 2,
/// priority = 2.0, global, x = field, y = field)]` on a struct with named
/// fields (Rust type → wire type: `bool`, `u8`, `u16`, `u32`, `i32`, `f32`,
/// `String` → short_str, `Vec<u8>` → bytes). The interest position is the
/// `x`/`y` fields unless overridden; a kind without a position must be
/// `global`.
#[proc_macro_derive(
    Replicated,
    attributes(replicated, quant, angle, field_type, priority, correct)
)]
pub fn derive_replicated(input: TokenStream) -> TokenStream {
    finish(replicated::expand(parse_macro_input!(input as DeriveInput)))
}

/// A compact binary input frame. Fields are encoded in order with the
/// shared value codec; `#[range(..)]` clamps, `#[buttons(..)]` masks,
/// non-finite floats reject.
#[proc_macro_derive(GameInput, attributes(quant, angle, field_type, range, buttons))]
pub fn derive_game_input(input: TokenStream) -> TokenStream {
    finish(input::expand(parse_macro_input!(input as DeriveInput)))
}

/// Zone events with stable ids: `#[game_event(id = N, name = "..")]` on a
/// struct, or on every variant of an enum (unit, named fields, or one value
/// such as a `GameRecord`).
#[proc_macro_derive(GameEvent, attributes(game_event, quant, angle, field_type, long))]
pub fn derive_game_event(input: TokenStream) -> TokenStream {
    finish(event::expand_event(parse_macro_input!(
        input as DeriveInput
    )))
}

/// A nested payload record (lists of rows, shared structs).
#[proc_macro_derive(GameRecord, attributes(quant, angle, field_type, long))]
pub fn derive_game_record(input: TokenStream) -> TokenStream {
    finish(event::expand_record(parse_macro_input!(
        input as DeriveInput
    )))
}

/// Bind a cell command input to its command:
/// `#[fact(command = RECORD_KILL, key(mob, generation))]`. Requires
/// `serde::Serialize`. Identity- and reward-looking field names are
/// rejected.
#[proc_macro_derive(FactCommand, attributes(fact))]
pub fn derive_fact_command(input: TokenStream) -> TokenStream {
    finish(fact::expand(parse_macro_input!(input as DeriveInput)))
}
