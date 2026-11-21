//! Compile-fail suite for `distributed_game_macros`.
//!
//! Each fixture in `tests/compile_fail/` must fail with a pointed, spanned
//! diagnostic; the `.stderr` snapshot pins it. Regenerate with
//! `TRYBUILD=overwrite cargo test -p distributed_game_macros`.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
