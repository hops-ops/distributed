use std::collections::BTreeMap;
use std::sync::Arc;

use distributed::application::{
    admit_command_session, command_roles_require_principal, Application, ApplicationExtension,
    CommandDefinition, CommandMount, CommandSpec, CommandTypeField, CommandTypeSpec,
    ContractCompiler, Module, ProjectionSpec, Runtime, RuntimeDialect, SurfaceSpec,
};
use distributed::command::{typed_command, CommandConsistency, Succeeded};
use distributed::graphql::{
    build_surface, col, prune_client_manifest, surface_for_application_contract, surface_for_role,
    ClientCommandPureReduce, ClientProjectionArm, ClientProjectionEventRef,
    ClientProjectionFallback, ClientProjectionMutationKind, ClientProjectionOperation,
    ClientProjectionPartition, ClientProjectionProgram, ClientSurfaceIdentity,
    CommandProjectionArmRef, CommandProjectionExtension, CommandProjectionPreviewOccurrence,
    DistributedClientSurfaceExport, RoleGrant, Surface, SurfaceOptions,
};
use distributed::projection::{
    PROJECTION_OPERATION_SEMANTICS_VERSION, PROJECTION_PROGRAM_IR_VERSION,
};
use distributed::{
    ApplicationManifest, CommandInput, CommandOutput, ReadModel, RelationalReadModel,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, ReadModel, Serialize)]
#[table("todos")]
struct TodoView {
    #[id("todo_id")]
    todo_id: String,
    title: String,
    status: String,
}

#[derive(Clone, Debug, Default, Deserialize, ReadModel, Serialize)]
#[table("chat_messages")]
struct ChatView {
    #[id("message_id")]
    message_id: String,
    body: String,
}

#[allow(dead_code)]
#[derive(Clone, Deserialize, CommandInput)]
struct ContractCommandInput {
    title: String,
}

#[derive(Clone, Serialize, CommandOutput)]
struct ContractCommandOutput {
    id: String,
}

fn typed_definition(id: &'static str, roles: &[&str]) -> CommandDefinition {
    let command = typed_command::<ContractCommandInput, Succeeded<ContractCommandOutput>>(id)
        .roles(roles.iter().copied());
    CommandDefinition::from_typed_command(command, None)
        .expect("typed contract should compile without an executable mount")
}

fn command_module() -> Module {
    Module::new("todo-contract")
        .command_definitions([
            typed_definition("todo.allowed", &["user"]),
            typed_definition("todo.forbidden", &["admin"]),
        ])
        .build()
        .expect("typed command module should compile")
}

fn command_catalog() -> Surface {
    full_surface()
        .with_module(&command_module())
        .expect("module contracts should bind before authorization")
}

fn grants_for(role: &str) -> BTreeMap<String, RoleGrant> {
    BTreeMap::from([(
        "TodoView".into(),
        RoleGrant::all_columns().rows(col("status").eq(role)),
    )])
}

fn command_names(surface: &Surface) -> Vec<String> {
    surface
        .commands()
        .iter()
        .map(|command| command.command_name.clone())
        .collect()
}

fn command(id: &str) -> CommandSpec {
    CommandSpec::try_new(
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
        CommandConsistency::Eventual,
    )
    .expect("test command should be portable")
}

fn definition(id: &str) -> CommandDefinition {
    CommandDefinition::contract(command(id))
}

fn application_with_commands(order: &[&str]) -> Application {
    let definitions = order.iter().map(|id| definition(id)).collect::<Vec<_>>();
    let module = Module::new("todo")
        .command_definitions(definitions)
        .build()
        .expect("test module should be valid");
    Application::new("todo-app")
        .module(module)
        .build()
        .expect("test application should be valid")
}

fn full_surface() -> Surface {
    build_surface(&[TodoView::schema().clone()], &SurfaceOptions::sqlite())
        .expect("non-empty Surface should compile")
}

fn selected_surface() -> Surface {
    let full = full_surface();
    let grants = BTreeMap::from([(
        "user".to_string(),
        BTreeMap::from([(
            "TodoView".to_string(),
            RoleGrant::all_columns()
                .with_aggregations()
                .rows(col("status").eq("open")),
        )]),
    )]);
    surface_for_application_contract(&full, "web", &["user".into()], &["user".into()], &grants)
        .expect("role/application-selected Surface should compile")
}

#[test]
fn explicit_module_commands_filter_allowed_and_forbidden_role_application_inventory() {
    let catalog = command_catalog();
    let user_grants = grants_for("user");
    let role = surface_for_role(&catalog, "user", &user_grants).unwrap();
    assert_eq!(command_names(&role), vec!["todo.allowed"]);

    let application = surface_for_application_contract(
        &catalog,
        "web",
        &["admin".into(), "user".into()],
        &["user".into()],
        &BTreeMap::from([("user".into(), user_grants)]),
    )
    .unwrap();
    assert_eq!(command_names(&application), vec!["todo.allowed"]);
    let spec = SurfaceSpec::from_surface("web", &application).unwrap();
    assert_eq!(spec.eligible_roles, ["admin", "user"]);
    assert_eq!(spec.schema_roles, ["user"]);
    assert_eq!(spec.commands.len(), 1);
    assert_eq!(spec.commands[0].id, "todo.allowed");
    assert_eq!(
        spec.contract["selection"],
        serde_json::json!({
            "kind": "application",
            "name": "web",
            "eligible_roles": ["admin", "user"],
            "schema_roles": ["user"],
        })
    );
    let client = ContractCompiler::from_surface("command-client", "web", Arc::new(application))
        .unwrap()
        .client_manifest()
        .unwrap();
    match client.surface {
        ClientSurfaceIdentity::Application {
            eligible_roles,
            schema_roles,
            ..
        } => {
            assert_eq!(eligible_roles, ["admin", "user"]);
            assert_eq!(schema_roles, ["user"]);
        }
        other => panic!("expected application client identity, got {other:?}"),
    }
}

#[test]
fn role_and_application_command_closure_rejects_missing_unauthorized_and_tampered_inventories() {
    let module = command_module();
    let catalog = command_catalog();
    let user_grants = grants_for("user");
    let selected = surface_for_application_contract(
        &catalog,
        "web",
        &["admin".into(), "user".into()],
        &["user".into()],
        &BTreeMap::from([("user".into(), user_grants.clone())]),
    )
    .unwrap();
    let valid = SurfaceSpec::from_surface("web", &selected).unwrap();
    Application::try_new("valid-command-closure", [module.clone()], [valid.clone()])
        .expect("authorized command closure should compile");

    let mut missing = valid.clone();
    missing.commands.clear();
    missing.contract["commands"] = serde_json::json!([]);
    missing.fingerprint =
        distributed::application::sha256_fingerprint(&missing.canonical_bytes().unwrap());
    let error = Application::try_new("missing-command", [module.clone()], [missing])
        .expect_err("missing selected command must fail closed");
    assert!(error
        .to_string()
        .contains("exact authorized command closure"));

    let mut tampered = valid.clone();
    tampered.commands[0].roles = vec!["admin".into()];
    tampered.contract["commands"][0]["roles"] = serde_json::json!(["admin"]);
    tampered.fingerprint =
        distributed::application::sha256_fingerprint(&tampered.canonical_bytes().unwrap());
    let error = Application::try_new("tampered-command", [module.clone()], [tampered])
        .expect_err("tampered selected command must fail closed");
    assert!(error.to_string().contains("not compatible"));

    let admin = surface_for_application_contract(
        &catalog,
        "admin-web",
        &["admin".into()],
        &["admin".into()],
        &BTreeMap::from([("admin".into(), grants_for("admin"))]),
    )
    .unwrap();
    let admin_spec = SurfaceSpec::from_surface("web", &admin).unwrap();
    let forbidden_command = admin_spec.commands[0].clone();
    let forbidden_contract = admin_spec.contract["commands"][0].clone();
    let mut unauthorized = valid;
    unauthorized.commands.push(forbidden_command.clone());
    unauthorized
        .commands
        .sort_by(|left, right| left.id.cmp(&right.id));
    unauthorized.contract["commands"]
        .as_array_mut()
        .expect("surface command contract array")
        .push(forbidden_contract.clone());
    unauthorized.fingerprint =
        distributed::application::sha256_fingerprint(&unauthorized.canonical_bytes().unwrap());
    let error = Application::try_new("unauthorized-command", [module], [unauthorized])
        .expect_err("unauthorized selected command must fail closed");
    assert!(error
        .to_string()
        .contains("exact authorized command closure"));

    let role_surface = surface_for_role(&catalog, "user", &user_grants).unwrap();
    let role_valid = SurfaceSpec::from_surface("user", &role_surface).unwrap();

    let mut role_unauthorized = role_valid.clone();
    role_unauthorized.commands.push(forbidden_command);
    role_unauthorized
        .commands
        .sort_by(|left, right| left.id.cmp(&right.id));
    role_unauthorized.contract["commands"]
        .as_array_mut()
        .expect("surface command contract array")
        .push(forbidden_contract);
    role_unauthorized.fingerprint =
        distributed::application::sha256_fingerprint(&role_unauthorized.canonical_bytes().unwrap());
    let error = Application::try_new(
        "unauthorized-role-command",
        [command_module()],
        [role_unauthorized],
    )
    .expect_err("unauthorized role command must fail closed");
    assert!(error
        .to_string()
        .contains("exact authorized command closure"));

    let mut role_tampered = role_valid.clone();
    role_tampered.commands[0].roles = vec!["admin".into()];
    role_tampered.contract["commands"][0]["roles"] = serde_json::json!(["admin"]);
    role_tampered.fingerprint =
        distributed::application::sha256_fingerprint(&role_tampered.canonical_bytes().unwrap());
    let error = Application::try_new("tampered-role-command", [command_module()], [role_tampered])
        .expect_err("tampered role command must fail closed");
    assert!(error.to_string().contains("not compatible"));

    let mut role_missing = role_valid;
    role_missing.commands.clear();
    role_missing.contract["commands"] = serde_json::json!([]);
    role_missing.fingerprint =
        distributed::application::sha256_fingerprint(&role_missing.canonical_bytes().unwrap());
    let error = Application::try_new("missing-role-command", [command_module()], [role_missing])
        .expect_err("role command closure must fail closed");
    assert!(error
        .to_string()
        .contains("exact authorized command closure"));
}

#[test]
fn module_identity_is_identical_across_full_and_split_selection() {
    let full = application_with_commands(&["todo.create", "todo.archive"]);
    let split = application_with_commands(&["todo.archive", "todo.create"]);

    assert_eq!(full.manifest().commands, split.manifest().commands);
    assert_eq!(
        full.manifest().canonical_bytes().unwrap(),
        split.manifest().canonical_bytes().unwrap()
    );
}

#[test]
fn application_manifest_is_byte_deterministic_and_contains_no_executable_data() {
    let spec = command("todo.create");
    let mount = CommandMount::from_request_handler(spec.clone(), |request| async move {
        Ok(distributed::microsvc::CommandResponse {
            status: 200,
            body: request.input,
        })
    });
    let definition = CommandDefinition::with_mount(spec, mount).unwrap();
    let module = Module::new("todo")
        .command_definition(definition)
        .build()
        .expect("mounted module should be valid");
    let application = Application::new("todo-app")
        .module(module)
        .extension(
            ApplicationExtension::try_new(
                "ui",
                1,
                serde_json::json!({
                    "default_view": "board",
                    "columns": ["title", "status"],
                    "literal_url": "https://domain.example/view",
                    "literal_path": "orders/today"
                }),
            )
            .unwrap(),
        )
        .build()
        .unwrap();

    let first = application.manifest().canonical_bytes().unwrap();
    let second = application.manifest().canonical_bytes().unwrap();
    assert_eq!(first, second);
    let value: serde_json::Value = serde_json::from_slice(&first).unwrap();
    assert!(value.get("schema_version").is_some());
    assert!(value["fingerprints"]["canonical"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    let extensions = value["extensions"].as_array().unwrap();
    let framework = extensions
        .iter()
        .find(|extension| extension["id"] == "distributed.framework")
        .unwrap();
    assert_eq!(framework["version"], 1);
    assert_eq!(framework["value"]["release"], env!("CARGO_PKG_VERSION"));
    let ui = extensions
        .iter()
        .find(|extension| extension["id"] == "ui")
        .unwrap();
    assert_eq!(ui["value"]["literal_url"], "https://domain.example/view");
    assert_eq!(ui["value"]["literal_path"], "orders/today");
    assert!(!String::from_utf8(first.clone())
        .unwrap()
        .contains("handler"));
    assert!(!String::from_utf8(first.clone())
        .unwrap()
        .contains("application_composition"));
    assert!(ApplicationManifest::from_canonical_bytes(&first).is_ok());

    let mut missing_version = value.clone();
    missing_version
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let missing_version = serde_json::to_vec(&missing_version).unwrap();
    assert!(ApplicationManifest::from_canonical_bytes(&missing_version).is_err());

    let mut missing_framework = application.manifest().clone();
    missing_framework
        .extensions
        .retain(|extension| extension.id != "distributed.framework");
    assert!(missing_framework
        .validate()
        .unwrap_err()
        .to_string()
        .contains("missing framework compatibility"));

    let mut skewed_framework = application.manifest().clone();
    skewed_framework
        .extensions
        .iter_mut()
        .find(|extension| extension.id == "distributed.framework")
        .unwrap()
        .value["release"] = serde_json::json!("0.0.0-skewed");
    assert!(skewed_framework
        .validate()
        .unwrap_err()
        .to_string()
        .contains("does not match compiling framework"));

    for field in [
        "tables",
        "services",
        "endpoints",
        "transport",
        "observability",
    ] {
        let mut legacy_owner = value.clone();
        legacy_owner[field] = serde_json::json!([]);
        let legacy_owner = serde_json::to_vec(&legacy_owner).unwrap();
        assert!(
            ApplicationManifest::from_canonical_bytes(&legacy_owner).is_err(),
            "legacy logical-manifest field `{field}` must not decode"
        );
    }
}

#[test]
fn manifest_provenance_separates_logical_and_artifact_identity() {
    let application = application_with_commands(&["todo.create"]);
    let first = application
        .manifest()
        .clone()
        .with_source_revision("git:one");
    let second = application
        .manifest()
        .clone()
        .with_source_revision("git:two");

    assert_eq!(
        first.logical_fingerprint().unwrap(),
        second.logical_fingerprint().unwrap()
    );
    assert_ne!(first.fingerprint().unwrap(), second.fingerprint().unwrap());
    let decoded =
        ApplicationManifest::from_canonical_bytes(&first.canonical_bytes().unwrap()).unwrap();
    assert_eq!(
        decoded.provenance.source_revision.as_deref(),
        Some("git:one")
    );
    assert!(String::from_utf8(first.canonical_bytes().unwrap())
        .unwrap()
        .contains("git:one"));
}

#[test]
fn contract_compiler_pins_manifest_sdl_and_client_to_one_surface() {
    let selected = selected_surface();
    let compiler =
        ContractCompiler::from_surface("contract-only", "web", Arc::new(selected.clone())).unwrap();
    let manifest = compiler.manifest().unwrap();
    let sdl = compiler.graphql_sdl().unwrap();
    let client = compiler.client_manifest().unwrap();

    assert!(sdl.contains("TodoView"));
    assert_eq!(manifest.surfaces.len(), 1);
    assert_eq!(manifest.surfaces[0].selection, "application:web");
    assert!(client
        .models
        .iter()
        .any(|model| model.typename == "TodoView"));
    assert!(ContractCompiler::new("contract-only")
        .with_surface("web", Arc::new(selected))
        .unwrap()
        .with_surface("other", Arc::new(full_surface()))
        .is_err());
    assert!(matches!(
        client.surface,
        ClientSurfaceIdentity::Application { ref name, .. } if name == "web"
    ));
}

#[test]
fn surface_contract_retains_policy_literals_and_rejects_stale_redundancy() {
    let selected = selected_surface();
    let spec = distributed::application::SurfaceSpec::from_surface("web", &selected).unwrap();
    let row_policy = &spec.models[0].row_policy;
    assert_eq!(row_policy["kind"], "predicate");
    assert!(row_policy.get("expression").is_some());
    assert_ne!(row_policy, &serde_json::json!("predicate"));

    let mut stale = spec.clone();
    stale.contract["models"][0]["table_name"] = serde_json::json!("other_table");
    stale.fingerprint =
        distributed::application::sha256_fingerprint(&stale.canonical_bytes().unwrap());
    let error = Application::try_new("stale", [], [stale]).unwrap_err();
    assert!(error.to_string().contains("surface contract material"));
}

#[test]
fn explicit_definition_mount_identity_and_missing_pairing_fail_closed() {
    let spec = command("todo.create");
    let other = command("todo.other");
    let mount = CommandMount::contract(other);
    let error = CommandDefinition::with_mount(spec, mount).unwrap_err();
    assert!(error
        .to_string()
        .contains("definition and executable mount"));

    let duplicate = Module::new("todo")
        .command_definitions([definition("todo.create"), definition("todo.create")])
        .build()
        .expect_err("duplicate definitions must fail closed");
    assert!(duplicate.to_string().contains("duplicate command identity"));
}

#[test]
fn no_linker_inventory_is_needed_for_explicit_application_selection() {
    let application = application_with_commands(&["todo.create"]);
    assert_eq!(application.manifest().module_ids(), ["todo"]);
}

#[test]
fn nested_fingerprints_and_projection_references_are_fail_closed() {
    let application = application_with_commands(&["todo.create"]);
    let bytes = application.manifest().canonical_bytes().unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["commands"][0]["fingerprint"] = serde_json::json!("");
    let malformed: ApplicationManifest = serde_json::from_value(value)
        .expect("the malformed value should still be structurally deserializable");
    assert!(malformed.clone().refresh_fingerprints().is_err());
    assert!(ApplicationManifest::from_canonical_bytes(
        &serde_json::to_vec(&serde_json::to_value(malformed).unwrap()).unwrap()
    )
    .is_err());

    let mut projection =
        ProjectionSpec::try_new("todo.list", std::iter::empty::<String>(), ["TodoView"]).unwrap();
    projection.dependencies.push("projection:missing".into());
    projection.dependencies.sort();
    projection.fingerprint =
        distributed::application::sha256_fingerprint(&projection.canonical_bytes().unwrap());
    let module = Module::new("todo")
        .surface(
            distributed::application::SurfaceSpec::from_surface("web", &selected_surface())
                .unwrap(),
        )
        .projection(projection)
        .build()
        .unwrap();
    let error = Application::try_new("projection-owner", [module], [])
        .expect_err("missing projection dependencies must fail closed");
    assert!(error.to_string().contains("missing"));
}

#[test]
fn module_commands_only_keeps_command_identity() {
    let projection =
        ProjectionSpec::try_new("project_todos", ["todo.created"], ["TodoView"]).unwrap();
    let module = Module::new("todo")
        .command_definitions([definition("todo.create")])
        .projection(projection)
        .build()
        .unwrap();
    let commands = module.commands_only().unwrap();
    let projectors = module.projectors_only().unwrap();
    assert_eq!(module.commands()[0].id, commands.commands()[0].id);
    assert!(projectors.commands().is_empty());
    assert!(commands.manifest().projections.is_empty());
    assert!(!module.manifest().projections.is_empty());
    assert_eq!(module.module().id(), "todo");
}

#[test]
fn runtime_from_url_selects_dialect_and_workers() {
    let module = Module::new("todo")
        .command_definitions([definition("todo.create")])
        .build()
        .unwrap();
    let sqlite = Runtime::from_database_url("sqlite:./app.db")
        .unwrap()
        .mount(module.clone());
    assert_eq!(sqlite.dialect(), RuntimeDialect::Sqlite);
    assert!(sqlite.starts_outbox());
    assert!(!sqlite.starts_projector_consumer());
    let postgres = Runtime::from_database_url("postgres://localhost/app").unwrap();
    assert_eq!(postgres.dialect(), RuntimeDialect::Postgres);
}

#[test]
fn runtime_rejects_atomic_without_seal() {
    let mut command = command("blob.move");
    command.consistency = CommandConsistency::Atomic;
    command.fingerprint = distributed::application::sha256_fingerprint(
        &command.canonical_bytes().expect("command bytes"),
    );
    let module = Module::new("blob")
        .command_definitions([CommandDefinition::contract(command)])
        .build()
        .unwrap();
    let runtime = Runtime::from_database_url("sqlite::memory:")
        .unwrap()
        .mount(module);
    let error = runtime.validate().expect_err("atomic without seal");
    assert!(error.to_string().contains("Atomic"));
}

#[test]
fn runtime_rejects_atomic_with_unrelated_direct_projection() {
    let mut command = command("blob.move");
    command.consistency = CommandConsistency::Atomic;
    command.projected_model = Some("BlobGames".into());
    command.fingerprint = distributed::application::sha256_fingerprint(
        &command.canonical_bytes().expect("command bytes"),
    );
    let blob = Module::new("blob")
        .command_definitions([CommandDefinition::contract(command)])
        .build()
        .unwrap();
    let projection = ProjectionSpec::try_new("project_todos", ["todo.created"], ["TodoView"])
        .unwrap()
        .with_direct(true)
        .unwrap();
    let todos = Module::new("todo").projection(projection).build().unwrap();
    let error = Runtime::from_database_url("sqlite::memory:")
        .unwrap()
        .mount(blob)
        .mount(todos)
        .validate()
        .expect_err("unrelated seal must not satisfy Atomic");
    assert!(
        error.to_string().contains("Atomic") || error.to_string().contains("direct"),
        "{error}"
    );
}

#[test]
fn runtime_from_database_url_rejects_unsupported_schemes() {
    let error =
        Runtime::from_database_url("mysql://localhost/app").expect_err("unsupported scheme");
    assert!(error.to_string().contains("unsupported"), "{error}");
}

#[test]
fn runtime_route_for_prefers_longest_wildcard() {
    let runtime = Runtime::from_database_url("sqlite::memory:")
        .unwrap()
        .dispatch_route("todo.*", "http://todo")
        .dispatch_route("todo.admin.*", "http://todo-admin");
    assert_eq!(
        runtime.route_for("todo.admin.delete"),
        Some("http://todo-admin")
    );
    assert_eq!(runtime.route_for("todo.create"), Some("http://todo"));
}

#[test]
fn runtime_rejects_direct_seal_without_atomic_commands() {
    let projection = ProjectionSpec::try_new("project_blob", ["blob.moved"], ["BlobGames"])
        .unwrap()
        .with_direct(true)
        .unwrap();
    let module = Module::new("blob").projection(projection).build().unwrap();
    let runtime = Runtime::from_database_url("sqlite::memory:")
        .unwrap()
        .mount(module);
    let error = runtime.validate().expect_err("seal without commands");
    assert!(error.to_string().contains("direct projection"));
}

#[test]
fn roles_are_admission_without_a_second_guard() {
    let roles = vec!["user".into(), "admin".into()];
    assert!(command_roles_require_principal(&roles));
    assert_eq!(
        admit_command_session(&roles, None, &["user"]).unwrap_err(),
        "unauthenticated"
    );
    assert!(admit_command_session(&roles, Some("alice"), &["user"]).is_ok());
    assert!(admit_command_session(&["anonymous".into()], None, &[]).is_ok());
    assert_eq!(
        admit_command_session(&roles, Some("   "), &["user"]).unwrap_err(),
        "unauthenticated"
    );
}

#[test]
fn contract_export_prunes_unselected_read_models() {
    let tables = [TodoView::schema().clone(), ChatView::schema().clone()];
    let full = build_surface(&tables, &SurfaceOptions::sqlite()).unwrap();
    let grants = BTreeMap::from([(
        "user".to_string(),
        BTreeMap::from([
            ("TodoView".to_string(), RoleGrant::all_columns()),
            ("ChatView".to_string(), RoleGrant::all_columns()),
        ]),
    )]);
    let selected =
        surface_for_application_contract(&full, "web", &["user".into()], &["user".into()], &grants)
            .unwrap();
    let export = DistributedClientSurfaceExport::from_contract("todo-app", selected).unwrap();
    let todos_only = prune_client_manifest(export.manifest().unwrap(), ["TodoView"]).unwrap();
    assert!(todos_only
        .models
        .iter()
        .any(|model| model.typename == "TodoView"));
    assert!(!todos_only
        .models
        .iter()
        .any(|model| model.typename == "ChatView"));
    let chat_only = prune_client_manifest(export.manifest().unwrap(), ["ChatView"]).unwrap();
    assert!(chat_only
        .models
        .iter()
        .any(|model| model.typename == "ChatView"));
    assert!(!chat_only
        .models
        .iter()
        .any(|model| model.typename == "TodoView"));
}

#[test]
fn prune_drops_unselected_command_optimism_and_causal_slots() {
    let tables = [TodoView::schema().clone(), ChatView::schema().clone()];
    let full = build_surface(&tables, &SurfaceOptions::sqlite()).unwrap();
    let grants = BTreeMap::from([(
        "user".to_string(),
        BTreeMap::from([
            ("TodoView".to_string(), RoleGrant::all_columns()),
            ("ChatView".to_string(), RoleGrant::all_columns()),
        ]),
    )]);
    let catalog = full.with_module(&command_module()).unwrap();
    let selected = surface_for_application_contract(
        &catalog,
        "web",
        &["user".into()],
        &["user".into()],
        &grants,
    )
    .unwrap();
    let export = DistributedClientSurfaceExport::from_contract("todo-app", selected).unwrap();
    let mut manifest = export.manifest().unwrap();
    attach_todo_and_chat_command_optimism(&mut manifest);

    let before = serde_json::to_string(&manifest).unwrap();
    assert!(
        before.contains("ChatView"),
        "setup must include ChatView optimism"
    );
    assert!(before.contains("TodoView"));

    let todos_only = prune_client_manifest(manifest, ["TodoView"]).unwrap();
    let after = serde_json::to_string(&todos_only).unwrap();
    assert!(
        !after.contains("ChatView"),
        "pruned manifest leaked ChatView: {after}"
    );
    assert!(todos_only
        .models
        .iter()
        .any(|model| model.typename == "TodoView"));
    for program in &todos_only.projection_programs {
        for arm in &program.arms {
            assert!(
                arm.operations
                    .iter()
                    .all(|operation| operation.model == "TodoView"),
                "unselected-model operations must be gone"
            );
        }
    }
    for command in &todos_only.commands {
        if let Some(projection) = &command.extensions.projection {
            assert!(projection
                .pure_reduces
                .iter()
                .all(|reduce| reduce.model == "TodoView"));
            assert!(projection
                .program_arms
                .iter()
                .all(|arm| arm.program_id == "project_todos"));
            assert!(projection
                .preview_occurrences
                .iter()
                .all(|occurrence| occurrence.event.name == "todo.completed"));
            for (index, occurrence) in projection.preview_occurrences.iter().enumerate() {
                assert_eq!(occurrence.ordinal as usize, index);
            }
        }
    }
}

fn attach_todo_and_chat_command_optimism(
    manifest: &mut distributed::graphql::DistributedClientManifest,
) {
    let todo_event = ClientProjectionEventRef {
        id: "todo.completed".into(),
        name: "todo.completed".into(),
        version: 1,
    };
    let chat_event = ClientProjectionEventRef {
        id: "chat.posted".into(),
        name: "chat.posted".into(),
        version: 1,
    };
    manifest
        .projectors
        .push(distributed::graphql::ClientProjector {
            version: 1,
            name: "project_mixed".into(),
            facts: vec!["todo.completed".into(), "chat.posted".into()],
            models: vec!["TodoView".into(), "ChatView".into()],
            dependencies: Vec::new(),
            causal_confirmation: false,
        });
    manifest.projection_programs.push(ClientProjectionProgram {
        version: 2,
        program_id: "project_todos".into(),
        name: "project_todos".into(),
        program_version: 1,
        ir_version: PROJECTION_PROGRAM_IR_VERSION,
        operation_semantics_version: PROJECTION_OPERATION_SEMANTICS_VERSION,
        arms: vec![ClientProjectionArm {
            arm: "complete".into(),
            event: todo_event.clone(),
            partition: ClientProjectionPartition::Unit,
            operations: vec![ClientProjectionOperation {
                operation: "upsert_todo".into(),
                ordinal: 0,
                kind: ClientProjectionMutationKind::Upsert,
                model: "TodoView".into(),
                key: Vec::new(),
                fields: Vec::new(),
                relationships: Vec::new(),
                invalidations: Vec::new(),
            }],
        }],
    });
    manifest.projection_programs.push(ClientProjectionProgram {
        version: 2,
        program_id: "project_chat".into(),
        name: "project_chat".into(),
        program_version: 1,
        ir_version: PROJECTION_PROGRAM_IR_VERSION,
        operation_semantics_version: PROJECTION_OPERATION_SEMANTICS_VERSION,
        arms: vec![ClientProjectionArm {
            arm: "post".into(),
            event: chat_event.clone(),
            partition: ClientProjectionPartition::Unit,
            operations: vec![ClientProjectionOperation {
                operation: "upsert_chat".into(),
                ordinal: 0,
                kind: ClientProjectionMutationKind::Upsert,
                model: "ChatView".into(),
                key: Vec::new(),
                fields: Vec::new(),
                relationships: Vec::new(),
                invalidations: Vec::new(),
            }],
        }],
    });
    let command = manifest
        .commands
        .first_mut()
        .expect("module commands should compile onto the surface");
    command.extensions.projection = Some(CommandProjectionExtension {
        version: 2,
        event_set: vec![chat_event.clone(), todo_event.clone()],
        program_arms: vec![
            CommandProjectionArmRef {
                event: chat_event.clone(),
                program_id: "project_chat".into(),
                arm: "post".into(),
            },
            CommandProjectionArmRef {
                event: todo_event.clone(),
                program_id: "project_todos".into(),
                arm: "complete".into(),
            },
        ],
        preview_occurrences: vec![
            CommandProjectionPreviewOccurrence {
                ordinal: 0,
                event: chat_event,
                values: Vec::new(),
            },
            CommandProjectionPreviewOccurrence {
                ordinal: 1,
                event: todo_event,
                values: Vec::new(),
            },
        ],
        pure_reduces: vec![
            ClientCommandPureReduce {
                fn_name: "todo.status".into(),
                client_module: "pures".into(),
                client_export: "todoStatus".into(),
                wasm_package: String::new(),
                wasm_export: String::new(),
                model: "TodoView".into(),
                key: Vec::new(),
                args: Vec::new(),
                assign: Vec::new(),
            },
            ClientCommandPureReduce {
                fn_name: "chat.echo".into(),
                client_module: "pures".into(),
                client_export: "chatEcho".into(),
                wasm_package: String::new(),
                wasm_export: String::new(),
                model: "ChatView".into(),
                key: Vec::new(),
                args: Vec::new(),
                assign: Vec::new(),
            },
        ],
        fallback: ClientProjectionFallback::Revalidate,
    });
}

#[test]
fn explicit_dispatch_route_map() {
    let runtime = Runtime::from_database_url("sqlite::memory:")
        .unwrap()
        .graphql()
        .dispatch_route("todo.*", "http://commands");
    assert_eq!(runtime.route_for("todo.create"), Some("http://commands"));
    assert!(runtime.starts_graphql());
    assert!(!runtime.starts_outbox());
}

#[test]
fn application_surface_must_expose_a_schema_role() {
    let mut surface =
        distributed::application::SurfaceSpec::from_surface("web", &selected_surface()).unwrap();
    surface.schema_roles.clear();
    surface.contract["selection"]["schema_roles"] = serde_json::json!([]);
    surface.fingerprint =
        distributed::application::sha256_fingerprint(&surface.canonical_bytes().unwrap());
    let error = Application::try_new("role-owner", [], [surface])
        .expect_err("application surfaces without schema roles must fail closed");
    assert!(error.to_string().contains("schema role"));
}
