//! Compile failures reported through a trait bound (`#[diagnostic::on_unimplemented]`
//! text or a type mismatch) rather than the macro's own error. Each must fail
//! and name its cause; rustc's notes around it are not pinned. See
//! `tests/support/rustc_fail.rs` at the workspace root.

#[path = "../../tests/support/rustc_fail.rs"]
mod rustc_fail;

#[test]
fn rustc_rejects_generated_code_for_the_named_cause() {
    rustc_fail::assert_fail_with(
        env!("CARGO_MANIFEST_DIR"),
        "tests/rustc_fail",
        r#"distributed_game = { path = "{workspace}/distributed_game" }
serde = { version = "1", features = ["derive"] }"#,
        &[
            (
                "game_pure_unsupported_output",
                &["`Opaque` has no game payload encoding"],
            ),
            (
                "input_buttons_on_float",
                &["`#[buttons]` needs a u8, u16 or u32 field, not `f32`"],
            ),
            (
                "input_unsupported_field",
                &["`String` cannot be a game input field"],
            ),
            (
                "replicated_unsupported_type",
                &["`i8` cannot be a replicated entity field"],
            ),
            (
                "replicated_quant_on_integer",
                &["`#[quant]` / `#[angle]` need an `f32` field, not `u8`"],
            ),
            (
                "predict_returns_state",
                &["E0308", "expected `State`, found `f32`"],
            ),
        ],
    );
}
