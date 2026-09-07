//! Structural gate: competing projector authoring surfaces must stay gone.
//!
//! Checks that the old event-owning projection **proc-macro**, separately
//! authored `command_effects!` / `command_confirmations!`, command-side
//! `.project(...)` selectors, and the public `ProjectionReadModelWorkspace`
//! ORM authoring path stay removed.
//!
//! The declarative `projection!` macro (event→mutation mount) is the current
//! public authoring surface and is expected in `src/lib.rs`.

#![cfg(feature = "graphql")]

#[test]
fn declarative_projection_macro_is_public_authoring() {
    let lib = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"));
    assert!(
        lib.contains("macro_rules! projection"),
        "declarative projection! must be defined at crate root"
    );
    // Old gate rejected any `projection,` substring (false-positive on
    // compile_projection). Ensure the proc-macro path stays gone instead.
    assert!(
        !lib.lines().any(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("//")
                && (trimmed.contains("pub use distributed_macros::projection")
                    || trimmed.contains("projection,")
                        && trimmed.contains("distributed_macros")
                        && !trimmed.contains("compile_projection"))
        }),
        "event-owning projection proc-macro must not be re-exported"
    );
}

#[test]
fn mutation_macro_is_the_public_authoring_export() {
    let lib = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"));
    assert!(
        lib.contains("mutation,"),
        "mutation! must remain publicly re-exported"
    );
    // Reject live re-exports (comments may still mention the removed macros).
    assert!(
        !lib.contains("command_effects,")
            && !lib.contains("command_effects }")
            && !lib.lines().any(|line| {
                let trimmed = line.trim_start();
                !trimmed.starts_with("//") && trimmed.contains("command_effects")
            }),
        "command_effects! must not be re-exported at crate root"
    );
    assert!(
        !lib.contains("command_confirmations,")
            && !lib.contains("command_confirmations }")
            && !lib.lines().any(|line| {
                let trimmed = line.trim_start();
                !trimmed.starts_with("//") && trimmed.contains("command_confirmations")
            }),
        "command_confirmations! must not be re-exported at crate root"
    );
}

#[test]
fn handlers_source_has_no_command_side_project_selector() {
    let handlers = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/microsvc/service/handlers.rs"
    ));
    assert!(
        !handlers.contains("pub fn project<"),
        "command-side .project(...) selector must be removed from handlers.rs"
    );
}

#[test]
fn projection_read_model_workspace_is_gone() {
    let microsvc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/microsvc/mod.rs"));
    assert!(
        !microsvc.contains("ProjectionReadModelWorkspace"),
        "ProjectionReadModelWorkspace must not be re-exported from microsvc"
    );
    let projector_mod = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/microsvc/projector/mod.rs"
    ));
    assert!(
        !projector_mod.contains("graph_workspace"),
        "graph_workspace module must be deleted from projector"
    );
    assert!(
        !std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/microsvc/projector/graph_workspace.rs"
        ))
        .exists(),
        "graph_workspace.rs must not exist"
    );
}

#[test]
fn dead_effects_authoring_types_are_not_publicly_reexported() {
    let graphql_mod = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/graphql/mod.rs"));
    let command_mod = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/command/mod.rs"));
    for forbidden in [
        "CompiledCommandEffects",
        "CompiledConfirmationPlan",
        "__command_effects",
        "__command_confirmations",
        "__effect_upsert",
        "__effect_patch",
    ] {
        assert!(
            !graphql_mod.contains(forbidden) && !command_mod.contains(forbidden),
            "{forbidden} must not be re-exported from command:: or graphql::"
        );
    }
}

#[test]
fn typed_command_has_no_public_effects_or_confirmations_builder() {
    let typed = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/command/typed_command.rs"
    ));
    assert!(
        !typed.contains("pub fn effects("),
        "TypedCommand::effects must be removed"
    );
    assert!(
        !typed.contains("pub fn confirmations("),
        "TypedCommand::confirmations must be removed"
    );
}

#[test]
fn macros_crate_does_not_export_effects_or_projection_macros() {
    let macros_lib = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/distributed_macros/src/lib.rs"
    ));
    assert!(
        !macros_lib.contains("pub fn command_effects("),
        "command_effects! proc-macro must be removed"
    );
    assert!(
        !macros_lib.contains("pub fn command_confirmations("),
        "command_confirmations! proc-macro must be removed"
    );
    assert!(
        !macros_lib.contains("pub fn projection("),
        "projection! proc-macro must stay removed"
    );
    assert!(
        macros_lib.contains("mod command_input_defaults"),
        "input defaults module must use the honest name"
    );
}
