//! Compile failures whose message is rustc's, not the macro's (a trait bound
//! or a type mismatch in generated code). Each must fail and name its cause;
//! the full rendering is not pinned. See `tests/support/rustc_fail.rs`.

#[path = "../../tests/support/rustc_fail.rs"]
mod rustc_fail;

#[test]
fn rustc_rejects_generated_code_for_the_named_cause() {
    rustc_fail::assert_fail_with(
        env!("CARGO_MANIFEST_DIR"),
        "tests/rustc_fail",
        r#"distributed = { path = "{workspace}" }
serde = { version = "1", features = ["derive"] }"#,
        &[
            // A generated input default only fits a `String` field.
            (
                "input_default_list",
                &[
                    "E0271",
                    "Value == String",
                    "InputField_ids as EffectInputFieldMarker",
                ],
            ),
            (
                "input_default_nullable",
                &[
                    "E0271",
                    "Value == String",
                    "InputField_id as EffectInputFieldMarker",
                ],
            ),
            (
                "input_default_wrong_type",
                &[
                    "E0271",
                    "Value == String",
                    "InputField_count as EffectInputFieldMarker",
                ],
            ),
            // Only methods that record on `self` become domain commands.
            (
                "sourced_helper_other_receiver",
                &["cannot find type `Unrelated` in module `domain_commands`"],
            ),
        ],
    );
}
