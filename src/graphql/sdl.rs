//! Dep-free SDL text renderer for `distributed schema --format graphql`.
//!
//! Renders the dialect-independent core query surface from `&[TableSchema]`.
//! Artifact scope grows with the crate version (aggregates in phase 3,
//! Subscription root in phase 4). Renderer and engine ship together.

use std::collections::{BTreeMap, BTreeSet};

use crate::table::{TableKind, TableSchema};

use super::naming::{
    causal_protocol_type_names, comparison_exp_name, include_postgres_json_comparison_ops,
    is_valid_graphql_name, order_by_enum_values, reserved_type_names, COMMAND_STATUS_ROOT_FIELD,
    CUSTOM_SCALARS, DISTRIBUTED_COMMAND_STATE_TYPE, DISTRIBUTED_COMMAND_STATE_VALUES,
    DISTRIBUTED_COMMAND_STATUS_TYPE,
};

/// Options controlling which surface slices the renderer emits.
#[derive(Clone, Debug)]
pub struct SdlOptions {
    /// Emit `<table>_aggregate` roots and nested aggregate fields (phase 3).
    pub aggregates: bool,
    /// Emit Postgres `jsonb` comparison operators on `JSON_comparison_exp`.
    ///
    /// Must match the runtime engine dialect: **false for SQLite**, true for
    /// Postgres. Defaults to false (SQLite / dialect-independent artifact).
    /// See [`SdlOptions::sqlite`] / [`SdlOptions::postgres`].
    pub jsonb_operators: bool,
    /// Emit a Subscription root mirroring Query list fields (phase 4).
    pub subscriptions: bool,
}

impl Default for SdlOptions {
    fn default() -> Self {
        Self::sqlite()
    }
}

impl SdlOptions {
    /// SDL for SQLite-backed engines (no PG JSON comparison ops).
    pub fn sqlite() -> Self {
        Self {
            aggregates: true,
            jsonb_operators: include_postgres_json_comparison_ops(false),
            subscriptions: true,
        }
    }

    /// SDL for Postgres-backed engines (includes jsonb comparison ops).
    pub fn postgres() -> Self {
        Self {
            aggregates: true,
            jsonb_operators: include_postgres_json_comparison_ops(true),
            subscriptions: true,
        }
    }
}

/// Render GraphQL SDL for the given tables (ReadModel only; operational filtered).
///
/// Builds the shared [[surface]] IR first, then emits SDL only from that IR so
/// dialect ops and model set cannot diverge from `build_surface`.
pub fn graphql_sdl_for_tables(tables: &[TableSchema]) -> Result<String, String> {
    graphql_sdl_for_tables_with_options(tables, &SdlOptions::default())
}

pub fn graphql_sdl_for_tables_with_options(
    tables: &[TableSchema],
    options: &SdlOptions,
) -> Result<String, String> {
    let surface_opts = super::surface::SurfaceOptions {
        dialect: if options.jsonb_operators {
            super::surface::SurfaceDialect::Postgres
        } else {
            super::surface::SurfaceDialect::Sqlite
        },
        aggregates: options.aggregates,
        subscriptions: options.subscriptions,
        default_limit: 100,
        max_limit: 1000,
    };
    let surface = super::surface::build_surface(tables, &surface_opts)?;
    graphql_sdl_from_surface(&surface)
}

/// Emit GraphQL SDL from a pre-built surface IR (role-filtered or full catalog).
pub fn graphql_sdl_from_surface(surface: &super::surface::Surface) -> Result<String, String> {
    graphql_sdl_from_read_models(surface)
}

/// Production path for **role-filtered** SDL (gap A10).
///
/// ```text
/// build_surface → surface_for_role → graphql_sdl_from_surface
/// ```
///
/// Prefer this over filtering full SDL as text. `grants` maps model_name →
/// [`RoleGrant`](super::surface::RoleGrant) for the role.
pub fn graphql_sdl_for_role(
    tables: &[TableSchema],
    options: &SdlOptions,
    role: &str,
    grants: &std::collections::BTreeMap<String, super::surface::RoleGrant>,
) -> Result<String, String> {
    let surface_opts = super::surface::SurfaceOptions {
        dialect: if options.jsonb_operators {
            super::surface::SurfaceDialect::Postgres
        } else {
            super::surface::SurfaceDialect::Sqlite
        },
        aggregates: options.aggregates,
        subscriptions: options.subscriptions,
        default_limit: 100,
        max_limit: 1000,
    };
    let full = super::surface::build_surface(tables, &surface_opts)?;
    let role_surface = super::surface::surface_for_role(&full, role, grants)?;
    graphql_sdl_from_surface(&role_surface)
}

/// Internal renderer over an already IR-filtered set of read models.
fn graphql_sdl_from_read_models(surface: &super::surface::Surface) -> Result<String, String> {
    let has_causal_commands = !surface.commands.is_empty();
    // Type names and root field names are separate GraphQL namespaces (Hasura
    // reuses e.g. `players_aggregate` as both a root field and an object type).
    let mut type_names: BTreeSet<String> = BTreeSet::new();
    let mut query_fields: BTreeSet<String> = BTreeSet::new();
    let mut subscription_fields: BTreeSet<String> = BTreeSet::new();
    for reserved in reserved_type_names() {
        type_names.insert(reserved.to_string());
    }
    if has_causal_commands {
        for reserved in causal_protocol_type_names() {
            type_names.insert(reserved.to_string());
            if surface.commands.iter().any(|command| {
                command_shape_uses_type_name(&command.input, reserved)
                    || command_shape_uses_type_name(&command.output, reserved)
            }) {
                return Err(format!(
                    "generated name `{reserved}` collides with a causal protocol type"
                ));
            }
        }
    }
    for scalar in CUSTOM_SCALARS {
        if !is_valid_graphql_name(scalar) {
            return Err(format!("scalar `{scalar}` is not a valid GraphQL name"));
        }
    }

    for comparison_name in surface.comparison_ops.keys() {
        claim_name(&mut type_names, comparison_name)?;
    }
    for model in surface.models.values() {
        claim_name(&mut type_names, &model.object_name)?;
        claim_name(&mut type_names, &format!("{}_bool_exp", model.table_name))?;
        claim_name(&mut type_names, &format!("{}_order_by", model.table_name))?;
        if model.aggregations {
            claim_name(&mut type_names, &format!("{}_aggregate", model.table_name))?;
            claim_name(
                &mut type_names,
                &format!("{}_aggregate_fields", model.table_name),
            )?;
        }
        for column in &model.columns {
            if !is_valid_graphql_name(&column.name) {
                return Err(format!(
                    "model `{}` column `{}` is not a valid GraphQL name",
                    model.model_name, column.name
                ));
            }
        }
        for relationship in &model.relationships {
            if !is_valid_graphql_name(&relationship.name) {
                return Err(format!(
                    "model `{}` relationship `{}` is not a valid GraphQL name",
                    model.model_name, relationship.name
                ));
            }
        }
    }
    for root in &surface.query_fields {
        claim_name(&mut query_fields, &root.name)?;
    }
    if has_causal_commands {
        claim_name(&mut query_fields, COMMAND_STATUS_ROOT_FIELD)?;
    }
    for root in &surface.subscription_fields {
        claim_name(&mut subscription_fields, &root.name)?;
    }

    let mut out = String::new();

    // Custom scalars, alphabetically.
    for scalar in CUSTOM_SCALARS {
        out.push_str(&format!("scalar {scalar}\n"));
    }
    out.push('\n');

    // order_by enum.
    out.push_str("enum order_by {\n");
    for v in order_by_enum_values() {
        out.push_str(&format!("  {v}\n"));
    }
    out.push_str("}\n\n");

    // Comparison input types (shared per scalar that appears).
    let used_scalars: BTreeSet<&str> = surface
        .models
        .values()
        .flat_map(|model| model.columns.iter().map(|column| column.scalar.as_str()))
        .collect();
    for scalar in &used_scalars {
        let name = comparison_exp_name(scalar);
        let operators = surface
            .comparison_ops
            .get(&name)
            .ok_or_else(|| format!("Surface is missing comparison operator inventory `{name}`"))?;
        emit_comparison_exp(&mut out, scalar, operators);
    }

    for model in surface.models.values() {
        emit_object_type(&mut out, model, surface);
        emit_bool_exp(&mut out, model, surface);
        emit_order_by_input(&mut out, model);
        if model.aggregations {
            emit_aggregate_types(&mut out, model);
        }
    }

    emit_command_types(&mut out, &surface.commands, &surface.models)?;
    if has_causal_commands {
        emit_causal_command_protocol_types(&mut out);
    }

    // Roots are emitted from the Surface inventory, not reconstructed from
    // schemas. This is what keeps hidden/partial by-PK identity and per-model
    // aggregate grants aligned with runtime and client manifest output.
    out.push_str("type Query {\n");
    if surface.query_fields.is_empty() && !has_causal_commands {
        // async-graphql requires a non-empty Query object and the runtime uses
        // this same fail-closed sentinel for roles with no readable models.
        // It is intentionally not a client-manifest root.
        out.push_str("  _empty: Boolean!\n");
    } else {
        for field in &surface.query_fields {
            out.push_str(&surface_root_sdl(field));
            out.push('\n');
        }
        if has_causal_commands {
            out.push_str(&format!(
                "  {COMMAND_STATUS_ROOT_FIELD}(commandId: ID!): {DISTRIBUTED_COMMAND_STATUS_TYPE}!\n"
            ));
        }
    }
    out.push_str("}\n");

    if !surface.subscription_fields.is_empty() {
        out.push_str("\ntype Subscription {\n");
        for field in &surface.subscription_fields {
            out.push_str(&surface_root_sdl(field));
            out.push('\n');
        }
        out.push_str("}\n");
    }

    if !surface.commands.is_empty() {
        out.push_str("\ntype Mutation {\n");
        for command in &surface.commands {
            let input = command_arguments_sdl(&command.input);
            let output = match &command.output {
                super::surface::SurfaceCommandShape::None => {
                    return Err(format!(
                        "command `{}` cannot declare an empty output",
                        command.command_name
                    ));
                }
                super::surface::SurfaceCommandShape::Typed(definition) => &definition.name,
            };
            out.push_str(&format!("  {}{}: {}!\n", command.field_name, input, output));
        }
        out.push_str("}\n");
    }

    Ok(out)
}

fn emit_causal_command_protocol_types(out: &mut String) {
    out.push_str(&format!("enum {DISTRIBUTED_COMMAND_STATE_TYPE} {{\n"));
    for state in DISTRIBUTED_COMMAND_STATE_VALUES {
        out.push_str(&format!("  {state}\n"));
    }
    out.push_str("}\n\n");
    out.push_str(&format!("type {DISTRIBUTED_COMMAND_STATUS_TYPE} {{\n"));
    out.push_str(&format!("  state: {DISTRIBUTED_COMMAND_STATE_TYPE}!\n"));
    out.push_str("}\n\n");
}

fn command_shape_uses_type_name(shape: &super::surface::SurfaceCommandShape, name: &str) -> bool {
    match shape {
        super::surface::SurfaceCommandShape::None => false,
        super::surface::SurfaceCommandShape::Typed(definition) => {
            command_type_uses_name(definition, name)
        }
    }
}

fn command_type_uses_name(definition: &super::surface::SurfaceTypeDef, name: &str) -> bool {
    definition.name == name
        || definition.fields.iter().any(|field| {
            field.type_name == name
                || field
                    .nested
                    .as_deref()
                    .is_some_and(|nested| command_type_uses_name(nested, name))
        })
}

fn command_arguments_sdl(input: &super::surface::SurfaceCommandShape) -> String {
    let mut arguments = vec!["commandId: ID!".to_string()];
    match input {
        super::surface::SurfaceCommandShape::None => {}
        super::surface::SurfaceCommandShape::Typed(definition) => {
            arguments.push(format!("input: {}!", definition.name));
        }
    }
    if arguments.is_empty() {
        String::new()
    } else {
        format!("({})", arguments.join(", "))
    }
}

fn surface_root_sdl(root: &super::surface::RootField) -> String {
    let arguments = root
        .arguments
        .iter()
        .map(|argument| {
            let mut ty = if argument.list {
                format!("[{}!]", argument.type_name)
            } else {
                argument.type_name.clone()
            };
            if !argument.nullable {
                ty.push('!');
            }
            format!("{}: {ty}", argument.name)
        })
        .collect::<Vec<_>>();
    let arguments = if arguments.is_empty() {
        String::new()
    } else {
        format!("({})", arguments.join(", "))
    };
    let output = match root.kind {
        super::surface::RootKind::List => format!("[{}!]!", root.object),
        super::surface::RootKind::ByPk => root.object.clone(),
        super::surface::RootKind::Aggregate => root.name.clone(),
    };
    format!("  {}{}: {}", root.name, arguments, output)
}

fn emit_command_types(
    out: &mut String,
    commands: &[super::surface::SurfaceCommand],
    models: &BTreeMap<String, super::surface::SurfaceModel>,
) -> Result<(), String> {
    let mut inputs = BTreeMap::new();
    let mut outputs = BTreeMap::new();
    for command in commands {
        if let super::surface::SurfaceCommandShape::Typed(definition) = &command.input {
            collect_command_type(definition, &mut inputs)?;
        }
        if let super::surface::SurfaceCommandShape::Typed(definition) = &command.output {
            let reuses_visible_model = super::surface::projected_output_reuses_surface_model(
                &command.command_name,
                command.consistency,
                command.projected_model.as_ref(),
                definition,
                models,
            )?;
            if !reuses_visible_model {
                collect_command_type(definition, &mut outputs)?;
            }
        }
    }
    for definition in inputs.values() {
        emit_command_type(out, "input", definition);
    }
    for definition in outputs.values() {
        emit_command_type(out, "type", definition);
    }
    Ok(())
}

fn collect_command_type(
    definition: &super::surface::SurfaceTypeDef,
    types: &mut BTreeMap<String, super::surface::SurfaceTypeDef>,
) -> Result<(), String> {
    if let Some(existing) = types.get(&definition.name) {
        if existing != definition {
            return Err(format!(
                "command type `{}` has conflicting structural definitions",
                definition.name
            ));
        }
        return Ok(());
    }
    types.insert(definition.name.clone(), definition.clone());
    for field in &definition.fields {
        if let Some(nested) = &field.nested {
            collect_command_type(nested, types)?;
        }
    }
    Ok(())
}

fn emit_command_type(out: &mut String, keyword: &str, definition: &super::surface::SurfaceTypeDef) {
    out.push_str(&format!("{keyword} {} {{\n", definition.name));
    for field in &definition.fields {
        let mut ty = if field.list {
            if field.item_nullable {
                format!("[{}]", field.type_name)
            } else {
                format!("[{}!]", field.type_name)
            }
        } else {
            field.type_name.clone()
        };
        if !field.nullable {
            ty.push('!');
        }
        out.push_str(&format!("  {}: {}\n", field.name, ty));
    }
    out.push_str("}\n\n");
}

fn claim_name(names: &mut BTreeSet<String>, name: &str) -> Result<(), String> {
    if !is_valid_graphql_name(name) {
        return Err(format!(
            "generated name `{name}` is not a valid GraphQL name"
        ));
    }
    if !names.insert(name.to_string()) {
        return Err(format!(
            "generated name `{name}` collides with another type or field"
        ));
    }
    Ok(())
}

fn emit_comparison_exp(out: &mut String, scalar: &str, operators: &[String]) {
    let name = comparison_exp_name(scalar);
    out.push_str(&format!("input {name} {{\n"));
    for operator in operators {
        let operand = match operator.as_str() {
            "_in" | "_nin" => format!("[{scalar}!]"),
            "_is_null" => "Boolean".into(),
            "_like" | "_ilike" | "_icontains" | "_has_key" => "String".into(),
            _ => scalar.to_string(),
        };
        out.push_str(&format!("  {operator}: {operand}\n"));
    }
    out.push_str("}\n\n");
}

fn emit_object_type(
    out: &mut String,
    model: &super::surface::SurfaceModel,
    surface: &super::surface::Surface,
) {
    let name = &model.object_name;
    out.push_str(&format!("type {name} {{\n"));
    for column in &model.columns {
        let null = if column.nullable { "" } else { "!" };
        out.push_str(&format!("  {}: {}{}\n", column.name, column.scalar, null));
    }
    for relationship in &model.relationships {
        let Some(target) = surface.models.get(&relationship.target_model) else {
            continue;
        };
        if relationship.list {
            out.push_str(&format!(
                "  {}{}: [{}!]!\n",
                relationship.name,
                surface_arguments_sdl(&relationship.arguments),
                target.object_name
            ));
        } else {
            let null = if relationship.nullable { "" } else { "!" };
            out.push_str(&format!(
                "  {}: {}{}\n",
                relationship.name, target.object_name, null
            ));
        }
        if let Some(aggregate) = &relationship.aggregate {
            out.push_str(&format!(
                "  {}{}: {}\n",
                aggregate.name,
                surface_arguments_sdl(&aggregate.arguments),
                aggregate.type_name
            ));
        }
    }
    out.push_str("}\n\n");
}

fn emit_bool_exp(
    out: &mut String,
    model: &super::surface::SurfaceModel,
    surface: &super::surface::Surface,
) {
    let name = format!("{}_bool_exp", model.table_name);
    out.push_str(&format!("input {name} {{\n"));
    out.push_str(&format!("  _and: [{name}!]\n"));
    out.push_str(&format!("  _or: [{name}!]\n"));
    out.push_str(&format!("  _not: {name}\n"));
    for column in &model.columns {
        let cmp = comparison_exp_name(&column.scalar);
        out.push_str(&format!("  {}: {}\n", column.name, cmp));
    }
    for relationship in &model.relationships {
        let Some(target) = surface.models.get(&relationship.target_model) else {
            continue;
        };
        let target_bool = format!("{}_bool_exp", target.table_name);
        out.push_str(&format!("  {}: {}\n", relationship.name, target_bool));
    }
    out.push_str("}\n\n");
}

fn emit_order_by_input(out: &mut String, model: &super::surface::SurfaceModel) {
    let name = format!("{}_order_by", model.table_name);
    out.push_str(&format!("input {name} {{\n"));
    for column in &model.columns {
        out.push_str(&format!("  {}: order_by\n", column.name));
    }
    out.push_str("}\n\n");
}

fn emit_aggregate_types(out: &mut String, model: &super::surface::SurfaceModel) {
    let agg = format!("{}_aggregate", model.table_name);
    let fields = format!("{}_aggregate_fields", model.table_name);
    let obj = &model.object_name;
    out.push_str(&format!("type {agg} {{\n"));
    out.push_str(&format!("  aggregate: {fields}\n"));
    out.push_str(&format!("  nodes: [{obj}!]!\n"));
    out.push_str("}\n\n");

    out.push_str(&format!("type {fields} {{\n"));
    out.push_str("  count: Int!\n");
    out.push_str("}\n\n");
}

fn surface_arguments_sdl(arguments: &[super::surface::SurfaceArgument]) -> String {
    if arguments.is_empty() {
        return String::new();
    }
    let arguments = arguments
        .iter()
        .map(|argument| {
            let mut type_name = if argument.list {
                format!("[{}!]", argument.type_name)
            } else {
                argument.type_name.clone()
            };
            if !argument.nullable {
                type_name.push('!');
            }
            format!("{}: {type_name}", argument.name)
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("({arguments})")
}

/// Filter operational tables and render SDL for a read-model schema catalog.
pub fn graphql_sdl_from_schemas(
    schemas: impl IntoIterator<Item = TableSchema>,
) -> Result<String, String> {
    let tables: Vec<TableSchema> = schemas
        .into_iter()
        .filter(|t| matches!(t.kind, TableKind::ReadModel))
        .collect();
    graphql_sdl_for_tables(&tables)
}

#[cfg(test)]
mod causal_command_sdl_tests {
    use super::*;
    use crate::command::{CommandConsistency, CommandEffects};
    use crate::graphql::surface::{SurfaceCommandShape, SurfaceTypeDef};

    fn command_surface() -> crate::graphql::surface::Surface {
        use crate::graphql::surface::{Surface, SurfaceCommand, SurfaceDialect, SurfaceSelection};

        Surface {
            selection: SurfaceSelection::Role {
                name: "user".into(),
            },
            dialect: SurfaceDialect::Sqlite,
            aggregates: false,
            subscriptions: false,
            default_limit: 100,
            max_limit: 1000,
            catalog: BTreeMap::new(),
            models: BTreeMap::new(),
            query_fields: Vec::new(),
            subscription_fields: Vec::new(),
            comparison_ops: BTreeMap::new(),
            commands: vec![SurfaceCommand {
                command_name: "todo.complete".into(),
                field_name: "todo_complete".into(),
                roles: vec!["user".into()],
                input: SurfaceCommandShape::Typed(SurfaceTypeDef {
                    name: "CompleteTodoInput".into(),
                    fields: vec![crate::graphql::surface::SurfaceTypeField {
                        name: "id".into(),
                        type_name: "String".into(),
                        nullable: false,
                        list: false,
                        item_nullable: false,
                        nested: None,
                    }],
                }),
                output: SurfaceCommandShape::Typed(SurfaceTypeDef {
                    name: "CompleteTodoPayload".into(),
                    fields: vec![crate::graphql::surface::SurfaceTypeField {
                        name: "id".into(),
                        type_name: "String".into(),
                        nullable: false,
                        list: false,
                        item_nullable: false,
                        nested: None,
                    }],
                }),
                consistency: CommandConsistency::Succeeded,
                input_defaults: Vec::new(),
                effects: Some(CommandEffects::revalidate()),
                confirmations: Vec::new(),
                projected_model: None,
                direct_projection: None,
                projections: Default::default(),
                confirmation_unavailable: false,
            }],
            commands_attached: true,
            projectors: Vec::new(),
            projectors_attached: false,
            service_binding: None,
        }
    }

    #[test]
    fn causal_mutations_require_framework_command_id_before_input() {
        let typed = SurfaceCommandShape::Typed(SurfaceTypeDef {
            name: "CompleteTodoInput".into(),
            fields: Vec::new(),
        });
        assert_eq!(
            command_arguments_sdl(&typed),
            "(commandId: ID!, input: CompleteTodoInput!)"
        );
    }

    #[test]
    fn causal_surface_emits_status_root_and_lowercase_state_enum() {
        let sdl = graphql_sdl_from_surface(&command_surface()).unwrap();

        assert!(sdl.contains("commandStatus(commandId: ID!): DistributedCommandStatus!"));
        assert!(sdl.contains("type DistributedCommandStatus {\n  state: DistributedCommandState!"));
        for state in DISTRIBUTED_COMMAND_STATE_VALUES {
            assert!(
                sdl.contains(&format!("\n  {state}\n")),
                "missing status state `{state}`:\n{sdl}"
            );
        }
        assert!(!sdl.contains("_empty: Boolean!"));
    }

    #[test]
    fn causal_status_root_and_types_fail_closed_on_collisions() {
        use crate::graphql::surface::{RootField, RootKind};

        let mut root_collision = command_surface();
        root_collision.query_fields.push(RootField {
            name: COMMAND_STATUS_ROOT_FIELD.into(),
            kind: RootKind::List,
            object: "Unused".into(),
            model_name: "Unused".into(),
            arguments: Vec::new(),
            dependencies: Vec::new(),
            default_limit: None,
            max_limit: None,
        });
        let error = graphql_sdl_from_surface(&root_collision).unwrap_err();
        assert!(
            error.contains("commandStatus") && error.contains("collides"),
            "{error}"
        );

        let mut type_collision = command_surface();
        type_collision.commands[0].input = SurfaceCommandShape::Typed(SurfaceTypeDef {
            name: DISTRIBUTED_COMMAND_STATUS_TYPE.into(),
            fields: Vec::new(),
        });
        let error = graphql_sdl_from_surface(&type_collision).unwrap_err();
        assert!(
            error.contains(DISTRIBUTED_COMMAND_STATUS_TYPE)
                && error.contains("causal protocol type"),
            "{error}"
        );
    }
}
