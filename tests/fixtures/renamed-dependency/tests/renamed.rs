#[test]
fn generated_paths_follow_a_renamed_distributed_dependency() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/fixtures/renamed_dependency.rs");
}
