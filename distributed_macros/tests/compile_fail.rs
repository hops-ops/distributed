//! Compile-fail suite for `distributed_macros`.
//!
//! Each fixture in `tests/compile_fail/` is expected to fail to compile with a
//! pointed, spanned diagnostic. The matching `.stderr` file pins the message so
//! regressions in diagnostic quality are caught.
//!
//! Regenerate `.stderr` snapshots with:
//! `rustup component add rust-src` (keeps standard-library source spans aligned
//! with CI), then:
//! `TRYBUILD=overwrite cargo test -p distributed_macros`

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
