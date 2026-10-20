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
//! | [`TicketExtension`] | struct | `TicketExtension`: the extension claims a ticket may carry |
//! | [`macro@predict`] (attribute) | fn | `PredictStep` for a client-predicted step + its WebAssembly export |
//! | [`macro@game_pure`] (attribute) | fn | `GamePure` for a pure function compiled to WebAssembly + its export |
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

mod claims;
mod event;
mod fact;
mod input;
mod predict;
mod pure;
mod replicated;
mod shared;
mod trusted;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn};

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
/// `serde::Serialize`. Identity-, reward- and economy-looking field names
/// (`principal`, `user_id`, `xp`, `reward`, `amount`, `gold`, `price`,
/// `balance`, `payout`, …; `_`/`-`/case ignored, serde renames included)
/// are rejected.
#[proc_macro_derive(FactCommand, attributes(fact))]
pub fn derive_fact_command(input: TokenStream) -> TokenStream {
    finish(fact::expand(parse_macro_input!(input as DeriveInput)))
}

/// Declare a game's ticket extension claims (match id, team, party…):
/// a struct with named fields that also derives serde `Serialize` and
/// `Deserialize`. Generates `TicketExtension::CLAIMS` (serialized names).
/// Identity-, reward- and economy-looking names are rejected with the same
/// deny list as [`FactCommand`]; `rename_all` and `flatten` are refused.
#[proc_macro_derive(TicketExtension)]
pub fn derive_ticket_extension(input: TokenStream) -> TokenStream {
    finish(claims::expand(parse_macro_input!(input as DeriveInput)))
}

/// Declare a zone's client-predicted step:
/// `#[predict(ArenaStep, kind = "player")]` on
/// `fn(state: &State, input: &Input, context: &Context, dt: f32) -> State`.
///
/// Keeps the function and adds `struct ArenaStep` implementing
/// `distributed_game::prediction::PredictStep` (name it in
/// `GameSurface::predict::<ArenaStep>()`). On `wasm32` it also exports the
/// step as `dg_predict_<fn name>` over the binary ABI described in
/// `distributed_game::prediction`.
#[proc_macro_attribute]
pub fn predict(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as predict::Args);
    let function = parse_macro_input!(item as ItemFn);
    finish(predict::expand(args, function))
}

/// Declare a pure function the generated client calls through WebAssembly:
/// `#[game_pure(GridCellFn)]` on `fn(arg: &A) -> R` or `fn() -> R` (`A`, `R`:
/// `Wire` types, typically `#[derive(GameRecord)]` structs).
///
/// Keeps the function and adds `struct GridCellFn` implementing
/// `distributed_game::pure::GamePure` (name it in
/// `GameSurface::pure::<GridCellFn>()`). On `wasm32` it also exports the
/// function as `dg_pure_<fn name>` over the binary ABI described in
/// `distributed_game::pure`.
#[proc_macro_attribute]
pub fn game_pure(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as pure::Args);
    let function = parse_macro_input!(item as ItemFn);
    finish(pure::expand(args, function))
}
