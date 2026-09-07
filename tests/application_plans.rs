//! Deployment plan compiler coverage for task 10.

use distributed::application::{
    compile_deployment_plan, Application, CommandDefinition, CommandSpec, CommandTypeField,
    CommandTypeSpec, DeploymentPlan, ModelFieldSpec, ModelSpec, Module, MountSelector,
    ProcessIntent, ProcessPreset, ProjectionSpec,
};
use distributed::command::CommandConsistency;

fn portable_command(id: &str, consistency: CommandConsistency) -> CommandSpec {
    let command = CommandSpec::try_new(
        id,
        id.replace('.', "_"),
        CommandTypeSpec {
            name: format!("{id}Input"),
            fields: vec![CommandTypeField {
                name: "title".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        },
        CommandTypeSpec {
            name: format!("{id}Output"),
            fields: vec![CommandTypeField {
                name: "id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        },
        consistency,
    )
    .expect("portable command");
    if matches!(consistency, CommandConsistency::Atomic) {
        command
            .with_direct_projection(
                "TodoView",
                serde_json::json!({"model": "TodoView", "kind": "direct"}),
            )
            .expect("atomic proof")
    } else {
        command
    }
}

fn definition(id: &str, consistency: CommandConsistency) -> CommandDefinition {
    CommandDefinition::contract(portable_command(id, consistency))
}

fn todo_model() -> ModelSpec {
    ModelSpec::try_new(
        "TodoView",
        "todos",
        [
            ModelFieldSpec {
                name: "todo_id".into(),
                scalar: "String".into(),
                nullable: false,
            },
            ModelFieldSpec {
                name: "title".into(),
                scalar: "String".into(),
                nullable: false,
            },
        ],
        ["todo_id"],
    )
    .expect("todo model")
}

fn plan_manifest() -> distributed::ApplicationManifest {
    let module = Module::new("todo")
        .command_definitions([
            definition("todo.create", CommandConsistency::Eventual),
            definition("todo.force", CommandConsistency::Atomic),
        ])
        .models([todo_model()])
        .projections([
            ProjectionSpec::try_new("project_todos", ["todo.created"], ["TodoView"]).unwrap(),
            ProjectionSpec::try_new("project_todos_direct", ["todo.forced"], ["TodoView"])
                .unwrap()
                .with_direct(true)
                .unwrap(),
        ])
        .build()
        .expect("module should build");
    Application::new("todo-app")
        .module(module)
        .build()
        .expect("application should build")
        .manifest()
        .clone()
}

#[test]
fn full_preset_lowers_to_ordinary_mounts_and_is_byte_deterministic() {
    let manifest = plan_manifest();
    let plan = compile_deployment_plan(
        "local-full",
        &manifest,
        [ProcessIntent::with_preset("full", &manifest, ProcessPreset::Full).unwrap()],
    )
    .expect("full plan");
    assert_eq!(plan.processes.len(), 1);
    let mounts = &plan.processes[0].mounts;
    assert!(mounts
        .iter()
        .any(|m| matches!(m, MountSelector::Command { id } if id == "todo.create")));
    assert!(mounts
        .iter()
        .any(|m| matches!(m, MountSelector::Command { id } if id == "todo.force")));
    assert!(mounts
        .iter()
        .any(|m| matches!(m, MountSelector::Projector { id } if id == "project_todos")));
    assert!(mounts
        .iter()
        .any(|m| matches!(m, MountSelector::Projector { id } if id == "project_todos_direct")));

    let first = plan.encode().unwrap();
    let second = compile_deployment_plan(
        "local-full",
        &manifest,
        [ProcessIntent::with_preset("full", &manifest, ProcessPreset::Full).unwrap()],
    )
    .unwrap()
    .encode()
    .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        DeploymentPlan::from_canonical_bytes(&first)
            .unwrap()
            .application_manifest_logical,
        manifest.fingerprints.logical
    );
}

#[test]
fn presets_and_mixed_selection_share_one_mount_algebra() {
    let manifest = plan_manifest();
    let writer = ProcessPreset::Writer.expand(&manifest).unwrap();
    let projector = ProcessPreset::Projector.expand(&manifest).unwrap();
    assert!(writer
        .iter()
        .all(|m| matches!(m, MountSelector::Command { .. })));
    assert!(projector
        .iter()
        .all(|m| matches!(m, MountSelector::Projector { .. })));

    let mixed = compile_deployment_plan(
        "mixed",
        &manifest,
        [ProcessIntent::new("api")
            .unwrap()
            .mounts([
                MountSelector::command("todo.create").unwrap(),
                MountSelector::projector("project_todos").unwrap(),
            ])
            .remote_commands(false)],
    )
    .expect("mixed plan");
    assert_eq!(mixed.processes[0].mounts.len(), 2);
    assert!(mixed
        .capabilities
        .iter()
        .any(|cap| cap.capability.as_str() == "event_store"));
}

#[test]
fn atomic_separation_fails_and_eventual_split_succeeds() {
    let manifest = plan_manifest();

    let eventual = compile_deployment_plan(
        "eventual-split",
        &manifest,
        [
            ProcessIntent::new("writer")
                .unwrap()
                .mounts([MountSelector::command("todo.create").unwrap()]),
            ProcessIntent::new("projectors")
                .unwrap()
                .mounts([MountSelector::projector("project_todos").unwrap()]),
        ],
    );
    assert!(eventual.is_ok(), "{eventual:?}");

    let atomic_split = compile_deployment_plan(
        "atomic-split",
        &manifest,
        [
            ProcessIntent::new("writer")
                .unwrap()
                .mounts([MountSelector::command("todo.force").unwrap()]),
            ProcessIntent::new("projectors")
                .unwrap()
                .mounts([MountSelector::projector("project_todos_direct").unwrap()]),
        ],
    );
    assert!(
        atomic_split.is_err(),
        "atomic command must not separate from direct projection"
    );
    let message = atomic_split.unwrap_err().to_string();
    assert!(
        message.contains("todo.force") && message.to_lowercase().contains("collocat"),
        "{message}"
    );

    let collocated = compile_deployment_plan(
        "atomic-local",
        &manifest,
        [ProcessIntent::new("writer").unwrap().mounts([
            MountSelector::command("todo.force").unwrap(),
            MountSelector::projector("project_todos_direct").unwrap(),
        ])],
    );
    assert!(collocated.is_ok(), "{collocated:?}");
}

#[test]
fn default_empty_process_list_expands_to_full_local() {
    let manifest = plan_manifest();
    let plan = compile_deployment_plan("default", &manifest, []).expect("default full");
    assert_eq!(plan.processes.len(), 1);
    assert_eq!(plan.processes[0].id, "full");
    assert!(!plan.processes[0].remote_commands);
}

#[test]
fn unknown_mount_and_duplicate_process_fail_closed() {
    let manifest = plan_manifest();
    let missing = compile_deployment_plan(
        "missing",
        &manifest,
        [ProcessIntent::new("p")
            .unwrap()
            .mounts([MountSelector::command("todo.missing").unwrap()])],
    );
    assert!(missing.is_err());

    let duplicate = compile_deployment_plan(
        "dup",
        &manifest,
        [
            ProcessIntent::with_preset("same", &manifest, ProcessPreset::Writer).unwrap(),
            ProcessIntent::with_preset("same", &manifest, ProcessPreset::Projector).unwrap(),
        ],
    );
    assert!(duplicate.is_err());
}

#[test]
fn capability_closure_is_explained_and_schema_has_one_owner() {
    let manifest = plan_manifest();
    let plan = compile_deployment_plan(
        "caps",
        &manifest,
        [ProcessIntent::with_preset("full", &manifest, ProcessPreset::Full).unwrap()],
    )
    .unwrap();
    assert!(plan.schema_lifecycle.required);
    assert_eq!(
        plan.schema_lifecycle.logical_owner.as_deref(),
        Some("todo-app")
    );
    assert!(plan.capabilities.iter().any(|cap| !cap.reasons.is_empty()));
    let described = plan.describe();
    assert_eq!(described["application"], "todo-app");
    assert!(!described["capabilities"].as_array().unwrap().is_empty());
}

#[test]
fn stale_manifest_predecessor_is_retained_exactly() {
    let manifest = plan_manifest();
    let plan = compile_deployment_plan(
        "pred",
        &manifest,
        [ProcessIntent::with_preset("full", &manifest, ProcessPreset::Full).unwrap()],
    )
    .unwrap();
    assert_eq!(
        plan.application_manifest_logical,
        manifest.fingerprints.logical
    );
    assert_eq!(
        plan.application_manifest_canonical,
        manifest.fingerprints.canonical
    );
}
