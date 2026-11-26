//! Compile-fail suite for `distributed_macros`.
//!
//! Each fixture is expected to fail to compile with a pointed, spanned
//! diagnostic. The matching `.stderr` file pins the message so regressions in
//! diagnostic quality are caught. The fixtures live in three directories:
//!
//! - `tests/compile_fail/`: the macros in general;
//! - `tests/command_types_compile_fail/`: `CommandInput` / `CommandOutput`;
//! - `tests/command_input_defaults_compile_fail/`: `command_input_defaults!`.
//!
//! One `TestCases` holds all three globs, so trybuild builds its project once.
//!
//! Regenerate `.stderr` snapshots with:
//! `rustup component add rust-src` (keeps standard-library source spans aligned
//! with CI), then:
//! `TRYBUILD=overwrite cargo test -p distributed_macros`

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
    t.compile_fail("tests/command_types_compile_fail/*.rs");
    t.compile_fail("tests/command_input_defaults_compile_fail/*.rs");
}
