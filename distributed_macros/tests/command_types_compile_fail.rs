//! Compile-time diagnostics specific to CommandInput / CommandOutput.

#[test]
fn command_types_compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/command_types_compile_fail/*.rs");
}
