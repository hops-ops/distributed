//! Per-role dynamic schema construction (async-graphql).
#![allow(clippy::items_after_test_module, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::dynamic::{
    Enum, Field, FieldFuture, InputObject, InputValue, Object, Scalar, Schema, SchemaError, TypeRef,
};
use async_graphql::Value;

use super::compile::{self, QueryPlan, RootKind};
use super::engine::{EngineInner, ExecutionAuthority};
use super::identity::VerifiedPrincipal;
use super::naming::{
    bool_exp_name, causal_protocol_type_names, comparison_exp_name, order_by_name,
    COMMAND_STATUS_ROOT_FIELD, CUSTOM_SCALARS, DISTRIBUTED_COMMAND_STATE_TYPE,
    DISTRIBUTED_COMMAND_STATE_VALUES, DISTRIBUTED_COMMAND_STATUS_TYPE,
};
use super::protocol::ProtocolResponseAccumulator;
use super::surface::{
    RootKind as SurfaceRootKind, Surface, SurfaceArgument, SurfaceCommandShape, SurfaceModel,
    SurfaceTypeDef, SurfaceTypeField,
};
use crate::microsvc::Session;

pub fn build_role_schema(
    role_surface: &Surface,
    max_depth: usize,
    max_complexity: usize,
    disable_introspection: bool,
) -> Result<Schema, String> {
    let has_causal_commands = !role_surface.commands.is_empty();
    if has_causal_commands {
        validate_causal_protocol_names(role_surface)?;
    }

    let mut query = Object::new("Query");
    let mut registered_objects: BTreeMap<String, Object> = BTreeMap::new();
    let mut registered_inputs: BTreeMap<String, InputObject> = BTreeMap::new();
    let mut scalars_needed = std::collections::BTreeSet::<String>::new();

    for scalar in CUSTOM_SCALARS {
        scalars_needed.insert((*scalar).into());
    }

    // order_by enum as string scalar alternative — use InputObject with enum-like strings
    // via TypeRef::named("order_by"). We'll register a scalar for simplicity in dynamic mode
    // and accept enum values as strings. For fuller fidelity use Enum type.
    let order_by_enum = async_graphql::dynamic::Enum::new("order_by")
        .item("asc")
        .item("asc_nulls_first")
        .item("asc_nulls_last")
        .item("desc")
        .item("desc_nulls_first")
        .item("desc_nulls_last");
    let command_state_enum = has_causal_commands.then(|| {
        let mut command_state = Enum::new(DISTRIBUTED_COMMAND_STATE_TYPE);
        for state in DISTRIBUTED_COMMAND_STATE_VALUES {
            command_state = command_state.item(*state);
        }
        command_state
    });
    if has_causal_commands {
        let status = Object::new(DISTRIBUTED_COMMAND_STATUS_TYPE).field(Field::new(
            "state",
            TypeRef::named_nn(DISTRIBUTED_COMMAND_STATE_TYPE),
            |ctx| FieldFuture::new(async move { schema_key_passthrough(&ctx, "state") }),
        ));
        registered_objects.insert(DISTRIBUTED_COMMAND_STATUS_TYPE.into(), status);
    }

    // Emit roots only for fields present on the role surface (IR inventory).
    for root in &role_surface.query_fields {
        let Some(model) = role_surface.models.get(&root.model_name) else {
            continue;
        };
        let model_name = root.model_name.clone();
        let obj_name = root.object.clone();

        ensure_object_type(
            &mut registered_objects,
            &root.model_name,
            role_surface,
            &mut scalars_needed,
        );
        ensure_bool_exp(
            &mut registered_inputs,
            &root.model_name,
            role_surface,
            &mut scalars_needed,
        );
        ensure_order_by_input(&mut registered_inputs, model);

        match root.kind {
            SurfaceRootKind::List => {
                let model_for_resolver = model_name.clone();
                let list_field = with_field_arguments(
                    Field::new(
                        root.name.clone(),
                        TypeRef::named_nn_list_nn(obj_name.clone()),
                        move |ctx| {
                            let model = model_for_resolver.clone();
                            FieldFuture::new(async move {
                                resolve_root(&ctx, &model, RootKind::List).await
                            })
                        },
                    ),
                    &root.arguments,
                );
                query = query.field(list_field);
            }
            SurfaceRootKind::ByPk => {
                let model_for_pk = model_name.clone();
                let mut pk_field = Field::new(
                    root.name.clone(),
                    TypeRef::named(obj_name.clone()),
                    move |ctx| {
                        let model = model_for_pk.clone();
                        FieldFuture::new(
                            async move { resolve_root(&ctx, &model, RootKind::ByPk).await },
                        )
                    },
                );
                pk_field = with_field_arguments(pk_field, &root.arguments);
                query = query.field(pk_field);
            }
            SurfaceRootKind::Aggregate => {
                let agg_type = root.name.clone();
                ensure_aggregate_type(&mut registered_objects, model);
                let model_for_agg = model_name.clone();
                let agg_field = with_field_arguments(
                    Field::new(root.name.clone(), TypeRef::named(agg_type), move |ctx| {
                        let model = model_for_agg.clone();
                        FieldFuture::new(async move {
                            resolve_root(&ctx, &model, RootKind::Aggregate).await
                        })
                    }),
                    &root.arguments,
                );
                query = query.field(agg_field);
            }
        }
    }

    // Comparison input types come from this exact role Surface instance.
    // Only register when IR lists ops — never emit empty input objects.
    for scalar in &scalars_needed {
        if matches!(
            scalar.as_str(),
            "String" | "Boolean" | "BigInt" | "Float" | "JSON" | "Timestamptz" | "Bytea"
        ) {
            let scalar_name = scalar.as_str();
            let name = comparison_exp_name(scalar_name);
            let ops: Vec<String> = role_surface
                .comparison_ops_for_scalar(scalar_name)
                .into_iter()
                .map(str::to_string)
                .collect();
            if ops.is_empty() {
                continue;
            }
            registered_inputs.entry(name.clone()).or_insert_with(|| {
                let mut input = InputObject::new(name);
                for op in &ops {
                    let ty = match op.as_str() {
                        "_is_null" => TypeRef::named(TypeRef::BOOLEAN),
                        "_in" | "_nin" => TypeRef::named_nn_list(scalar_name),
                        "_has_key" => TypeRef::named("String"),
                        _ => TypeRef::named(scalar_name),
                    };
                    input = input.field(InputValue::new(op.as_str(), ty));
                }
                input
            });
        }
    }

    // Empty-role Query must still define ≥1 field (async-graphql requirement).
    // Spec: empty role → FORBIDDEN with extensions.code (no query surface).
    if role_surface.query_fields.is_empty() && !has_causal_commands {
        query = query.field(Field::new(
            "_empty",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |_| {
                FieldFuture::new(async {
                    Err::<Option<Value>, _>(client_error("FORBIDDEN", "role has no GraphQL grants"))
                })
            },
        ));
    }
    if has_causal_commands {
        query = query.field(
            Field::new(
                COMMAND_STATUS_ROOT_FIELD,
                TypeRef::named_nn(DISTRIBUTED_COMMAND_STATUS_TYPE),
                |ctx| FieldFuture::new(async move { resolve_command_status(&ctx).await }),
            )
            .argument(InputValue::new("commandId", TypeRef::named_nn(TypeRef::ID))),
        );
    }

    // Mutation root from commands
    let mut mutation: Option<Object> = None;
    if !role_surface.commands.is_empty() {
        let mut mut_obj = Object::new("Mutation");
        for cmd in &role_surface.commands {
            let output_type = match &cmd.output {
                SurfaceCommandShape::None => {
                    return Err(format!(
                        "command `{}` cannot declare an empty output",
                        cmd.command_name
                    ));
                }
                SurfaceCommandShape::Typed(t) => {
                    ensure_command_output(&mut registered_objects, t);
                    t.name.as_str()
                }
            };
            let cmd_name = cmd.command_name.clone();
            let mut field = Field::new(
                cmd.field_name.clone(),
                TypeRef::named_nn(output_type),
                move |ctx| {
                    let cmd_name = cmd_name.clone();
                    FieldFuture::new(async move { resolve_command(&ctx, &cmd_name).await })
                },
            )
            .argument(InputValue::new("commandId", TypeRef::named_nn(TypeRef::ID)));
            match &cmd.input {
                SurfaceCommandShape::None => {}
                SurfaceCommandShape::Typed(tdef) => {
                    // Register nested input type if needed
                    ensure_command_input(&mut registered_inputs, tdef);
                    field = field.argument(InputValue::new(
                        "input",
                        TypeRef::named_nn(tdef.name.as_str()),
                    ));
                }
            }
            mut_obj = mut_obj.field(field);
        }
        mutation = Some(mut_obj);
    }

    // Subscription root: live queries refreshed off ChangeHub (commit-path).
    use async_graphql::dynamic::{Subscription, SubscriptionField, SubscriptionFieldFuture};
    let mut subscription = Subscription::new("Subscription");
    let mut has_subscription = false;
    for sub_root in &role_surface.subscription_fields {
        if !matches!(sub_root.kind, SurfaceRootKind::List) {
            continue;
        }
        let Some(_model) = role_surface.models.get(&sub_root.model_name) else {
            continue;
        };
        let model_for_sub = sub_root.model_name.clone();
        let obj_name = sub_root.object.clone();
        let field = with_subscription_arguments(
            SubscriptionField::new(
                sub_root.name.clone(),
                TypeRef::named_nn_list_nn(obj_name),
                move |ctx| {
                    let model = model_for_sub.clone();
                    // Extract owned data before the async block (stream is 'static).
                    let inner = ctx.data_opt::<Arc<EngineInner>>().cloned();
                    let session = ctx
                        .data_opt::<Session>()
                        .cloned()
                        .unwrap_or_else(Session::new);
                    let authority = ctx.data_opt::<ExecutionAuthority>().cloned();
                    let protocol = ctx.data_opt::<ProtocolResponseAccumulator>().cloned();
                    let selection = compile::selection_from_field(ctx.field());
                    SubscriptionFieldFuture::new(async move {
                        let inner = inner.ok_or_else(|| {
                            async_graphql::Error::new("GraphqlEngine not in request data")
                        })?;
                        let role = privilege_role_for_request(
                            authority.as_ref(),
                            &session,
                            &inner.anonymous_role,
                        );
                        let stream = super::subscribe::live_query_stream(
                            inner, session, role, model, selection, protocol,
                        )
                        .await
                        .map_err(async_graphql::Error::new)?;
                        Ok(stream)
                    })
                },
            ),
            &sub_root.arguments,
        );
        subscription = subscription.field(field);
        has_subscription = true;
    }

    let mut builder = if let Some(m) = mutation {
        if has_subscription {
            Schema::build(
                query.type_name(),
                Some(m.type_name()),
                Some(subscription.type_name()),
            )
            .register(query)
            .register(m)
            .register(subscription)
        } else {
            Schema::build(query.type_name(), Some(m.type_name()), None)
                .register(query)
                .register(m)
        }
    } else if has_subscription {
        Schema::build(query.type_name(), None, Some(subscription.type_name()))
            .register(query)
            .register(subscription)
    } else {
        Schema::build(query.type_name(), None, None).register(query)
    };

    builder = builder.register(order_by_enum);
    if let Some(command_state_enum) = command_state_enum {
        builder = builder.register(command_state_enum);
    }
    for name in CUSTOM_SCALARS {
        builder = builder.register(Scalar::new(*name));
    }
    for (_, obj) in registered_objects {
        builder = builder.register(obj);
    }
    for (_, input) in registered_inputs {
        builder = builder.register(input);
    }

    let mut builder = builder
        .limit_depth(max_depth)
        .limit_complexity(max_complexity);
    if disable_introspection {
        builder = builder.disable_introspection();
    }
    builder.finish().map_err(|e: SchemaError| e.to_string())
}

fn validate_causal_protocol_names(surface: &Surface) -> Result<(), String> {
    if surface
        .query_fields
        .iter()
        .any(|root| root.name == COMMAND_STATUS_ROOT_FIELD)
    {
        return Err(format!(
            "generated name `{COMMAND_STATUS_ROOT_FIELD}` collides with another type or field"
        ));
    }

    for protocol_name in causal_protocol_type_names() {
        let model_collision = surface.models.values().any(|model| {
            model.object_name == protocol_name
                || bool_exp_name(&model.schema) == protocol_name
                || order_by_name(&model.schema) == protocol_name
                || (model.aggregations
                    && (format!("{}_aggregate", model.table_name) == protocol_name
                        || format!("{}_aggregate_fields", model.table_name) == protocol_name))
        });
        let command_collision = surface.commands.iter().any(|command| {
            command_shape_uses_type_name(&command.input, protocol_name)
                || command_shape_uses_type_name(&command.output, protocol_name)
        });
        if surface.comparison_ops.contains_key(protocol_name)
            || model_collision
            || command_collision
        {
            return Err(format!(
                "generated name `{protocol_name}` collides with a causal protocol type"
            ));
        }
    }
    Ok(())
}

fn command_shape_uses_type_name(shape: &SurfaceCommandShape, name: &str) -> bool {
    match shape {
        SurfaceCommandShape::None => false,
        SurfaceCommandShape::Typed(definition) => command_type_uses_name(definition, name),
    }
}

fn command_type_uses_name(definition: &SurfaceTypeDef, name: &str) -> bool {
    definition.name == name
        || definition.fields.iter().any(|field| {
            field.type_name == name
                || field
                    .nested
                    .as_deref()
                    .is_some_and(|nested| command_type_uses_name(nested, name))
        })
}

fn ensure_object_type(
    objects: &mut BTreeMap<String, Object>,
    model_name: &str,
    surface: &Surface,
    scalars: &mut std::collections::BTreeSet<String>,
) {
    let model = &surface.models[model_name];
    let name = model.object_name.clone();
    if objects.contains_key(&name) {
        return;
    }
    // Break relationship cycles while nested object fields are registered.
    objects.insert(name.clone(), Object::new(name.clone()));
    let mut obj = Object::new(name.clone());
    for column in &model.columns {
        scalars.insert(column.scalar.clone());
        let ty = if column.nullable {
            TypeRef::named(column.scalar.as_str())
        } else {
            TypeRef::named_nn(column.scalar.as_str())
        };
        let key = column.name.clone();
        obj = obj.field(Field::new(column.name.as_str(), ty, move |ctx| {
            let key = key.clone();
            FieldFuture::new(async move { response_key_passthrough(&ctx, &key) })
        }));
    }
    for relationship in &model.relationships {
        let Some(target) = surface.models.get(&relationship.target_model) else {
            continue;
        };
        let target_obj = target.object_name.clone();
        ensure_object_type(objects, &relationship.target_model, surface, scalars);
        let key = relationship.name.clone();
        if relationship.list {
            let field = with_field_arguments(
                Field::new(
                    relationship.name.as_str(),
                    TypeRef::named_nn_list_nn(target_obj),
                    move |ctx| {
                        let key = key.clone();
                        FieldFuture::new(async move { response_key_passthrough(&ctx, &key) })
                    },
                ),
                &relationship.arguments,
            );
            obj = obj.field(field);
            if let Some(aggregate_plan) = &relationship.aggregate {
                ensure_aggregate_type(objects, target);
                let aggregate_name = aggregate_plan.name.clone();
                let aggregate_type = aggregate_plan.type_name.clone();
                let field_key = aggregate_name.clone();
                let aggregate = with_field_arguments(
                    Field::new(aggregate_name, TypeRef::named(aggregate_type), move |ctx| {
                        let key = field_key.clone();
                        FieldFuture::new(async move { response_key_passthrough(&ctx, &key) })
                    }),
                    &aggregate_plan.arguments,
                );
                obj = obj.field(aggregate);
            }
        } else {
            let ty = if relationship.nullable {
                TypeRef::named(target_obj)
            } else {
                TypeRef::named_nn(target_obj)
            };
            obj = obj.field(Field::new(relationship.name.as_str(), ty, move |ctx| {
                let key = key.clone();
                FieldFuture::new(async move { response_key_passthrough(&ctx, &key) })
            }));
        }
    }
    objects.insert(name, obj);
}

fn ensure_bool_exp(
    inputs: &mut BTreeMap<String, InputObject>,
    model_name: &str,
    surface: &Surface,
    scalars: &mut std::collections::BTreeSet<String>,
) {
    let model = &surface.models[model_name];
    let name = bool_exp_name(&model.schema);
    if inputs.contains_key(&name) {
        return;
    }
    // Insert placeholder first to break cycles.
    inputs.insert(name.clone(), InputObject::new(name.clone()));
    let mut input = InputObject::new(name.clone());
    // Optional list/recursive fields (Hasura-style); never required.
    input = input.field(InputValue::new(
        "_and",
        TypeRef::named_nn_list(name.as_str()),
    ));
    input = input.field(InputValue::new(
        "_or",
        TypeRef::named_nn_list(name.as_str()),
    ));
    input = input.field(InputValue::new("_not", TypeRef::named(name.as_str())));
    for column in &model.columns {
        scalars.insert(column.scalar.clone());
        let comparison = comparison_exp_name(&column.scalar);
        input = input.field(InputValue::new(
            column.name.as_str(),
            TypeRef::named(comparison),
        ));
    }
    for relationship in &model.relationships {
        let Some(target) = surface.models.get(&relationship.target_model) else {
            continue;
        };
        ensure_bool_exp(inputs, &relationship.target_model, surface, scalars);
        let target_bool = bool_exp_name(&target.schema);
        input = input.field(InputValue::new(
            relationship.name.as_str(),
            TypeRef::named(target_bool),
        ));
    }
    inputs.insert(name, input);
}

fn ensure_order_by_input(inputs: &mut BTreeMap<String, InputObject>, model: &SurfaceModel) {
    let name = order_by_name(&model.schema);
    if inputs.contains_key(&name) {
        return;
    }
    let mut input = InputObject::new(name.clone());
    for column in &model.columns {
        input = input.field(InputValue::new(
            column.name.as_str(),
            TypeRef::named("order_by"),
        ));
    }
    inputs.insert(name, input);
}

fn ensure_aggregate_type(objects: &mut BTreeMap<String, Object>, model: &SurfaceModel) {
    let agg = format!("{}_aggregate", model.table_name);
    if objects.contains_key(&agg) {
        return;
    }
    let fields_name = format!("{}_aggregate_fields", model.table_name);
    let mut fields_obj = Object::new(fields_name.clone());
    fields_obj = fields_obj.field(Field::new(
        "count",
        TypeRef::named_nn(TypeRef::INT),
        |ctx| FieldFuture::new(async move { response_key_passthrough(&ctx, "count") }),
    ));
    objects.insert(fields_name.clone(), fields_obj);

    let mut agg_obj = Object::new(agg.clone());
    agg_obj = agg_obj.field(Field::new(
        "aggregate",
        TypeRef::named(fields_name),
        |ctx| FieldFuture::new(async move { response_key_passthrough(&ctx, "aggregate") }),
    ));
    let obj = model.object_name.clone();
    agg_obj = agg_obj.field(Field::new("nodes", TypeRef::named_nn_list_nn(obj), |ctx| {
        FieldFuture::new(async move { response_key_passthrough(&ctx, "nodes") })
    }));
    objects.insert(agg, agg_obj);
}

fn surface_argument_type(argument: &SurfaceArgument) -> TypeRef {
    match (argument.list, argument.nullable) {
        (true, false) => TypeRef::named_nn_list_nn(argument.type_name.as_str()),
        (true, true) => TypeRef::named_nn_list(argument.type_name.as_str()),
        (false, false) => TypeRef::named_nn(argument.type_name.as_str()),
        (false, true) => TypeRef::named(argument.type_name.as_str()),
    }
}

fn with_field_arguments(mut field: Field, arguments: &[SurfaceArgument]) -> Field {
    for argument in arguments {
        field = field.argument(InputValue::new(
            argument.name.as_str(),
            surface_argument_type(argument),
        ));
    }
    field
}

fn with_subscription_arguments(
    mut field: async_graphql::dynamic::SubscriptionField,
    arguments: &[SurfaceArgument],
) -> async_graphql::dynamic::SubscriptionField {
    for argument in arguments {
        field = field.argument(InputValue::new(
            argument.name.as_str(),
            surface_argument_type(argument),
        ));
    }
    field
}

fn ensure_command_input(inputs: &mut BTreeMap<String, InputObject>, tdef: &SurfaceTypeDef) {
    if inputs.contains_key(&tdef.name) {
        return;
    }
    let mut input = InputObject::new(tdef.name.clone());
    for field in &tdef.fields {
        let ty = command_field_type(field);
        input = input.field(InputValue::new(field.name.as_str(), ty));
        if let Some(nested) = &field.nested {
            ensure_command_input(inputs, nested);
        }
    }
    inputs.insert(tdef.name.clone(), input);
}

/// Register a typed command-mutation payload so field selection works on results.
fn ensure_command_output(objects: &mut BTreeMap<String, Object>, tdef: &SurfaceTypeDef) {
    if objects.contains_key(&tdef.name) {
        return;
    }
    // Placeholder first so nested object cycles cannot re-enter forever.
    objects.insert(tdef.name.clone(), Object::new(tdef.name.clone()));
    let mut obj = Object::new(tdef.name.clone());
    for field in &tdef.fields {
        if let Some(nested) = &field.nested {
            ensure_command_output(objects, nested);
        }
        let ty = command_field_type(field);
        let key = field.name.clone();
        obj = obj.field(Field::new(field.name.as_str(), ty, move |ctx| {
            let key = key.clone();
            FieldFuture::new(async move { schema_key_passthrough(&ctx, &key) })
        }));
    }
    objects.insert(tdef.name.clone(), obj);
}

fn command_field_type(field: &SurfaceTypeField) -> TypeRef {
    match (field.list, field.nullable, field.item_nullable) {
        (true, false, false) => TypeRef::named_nn_list_nn(field.type_name.as_str()),
        (true, true, false) => TypeRef::named_nn_list(field.type_name.as_str()),
        (true, false, true) => TypeRef::named_list_nn(field.type_name.as_str()),
        (true, true, true) => TypeRef::named_list(field.type_name.as_str()),
        (false, false, _) => TypeRef::named_nn(field.type_name.as_str()),
        (false, true, _) => TypeRef::named(field.type_name.as_str()),
    }
}

fn response_key_passthrough(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    key: &str,
) -> Result<Option<Value>, async_graphql::Error> {
    // SQL projection objects are keyed by response name so two selections of
    // the same schema field can retain distinct arguments and sub-selections.
    // An unaliased field's response name is its schema name.
    let response_key = ctx.field().alias().unwrap_or(key);
    passthrough_key(ctx, response_key)
}

fn schema_key_passthrough(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    key: &str,
) -> Result<Option<Value>, async_graphql::Error> {
    // Command and status payloads are produced by framework/application code,
    // not the SQL compiler, and therefore remain keyed by schema field name.
    passthrough_key(ctx, key)
}

fn passthrough_key(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    key: &str,
) -> Result<Option<Value>, async_graphql::Error> {
    let Some(value) = ctx.parent_value.as_value() else {
        return Ok(None);
    };
    // Nested JSON may arrive as a string (SQLite json_group_array quirk).
    if let Value::String(s) = value {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
            if let Ok(v) = Value::from_json(parsed) {
                return Ok(lookup_key(&v, key));
            }
        }
    }
    Ok(lookup_key(value, key))
}

async fn execute_cell_by_key(
    inner: &EngineInner,
    model: &str,
    pk: &BTreeMap<String, String>,
    row_filter: Option<&super::filter::FilterExpr>,
    selection: &compile::SelectionNode,
) -> Result<Value, String> {
    let getter = inner
        .cell_getters
        .get(model)
        .ok_or_else(|| format!("cell-by-key getter not configured for `{model}`"))?;
    let Some(row) = getter.get_sealed_row(pk).await? else {
        return Ok(Value::Null);
    };
    let row_object = row
        .as_object()
        .ok_or_else(|| "cell sealed row must be a JSON object".to_string())?;
    for (key, expected) in pk {
        if row_object.get(key).and_then(serde_json::Value::as_str) != Some(expected) {
            return Err(format!(
                "cell sealed row primary key {key} does not match request"
            ));
        }
    }
    if let Some(filter) = row_filter {
        let schema = &inner
            .catalog
            .get(model)
            .ok_or_else(|| format!("unknown model `{model}`"))?
            .schema;
        if !compile::cell_row_matches(schema, filter, &row) {
            return Ok(Value::Null);
        }
    }
    let mut out = serde_json::Map::new();
    for child in &selection.children {
        if child.field_name == "__typename" {
            continue;
        }
        if let Some(value) = row.get(&child.field_name) {
            out.insert(child.response_key.clone(), value.clone());
        }
    }
    Value::from_json(serde_json::Value::Object(out)).map_err(|error| error.to_string())
}

fn lookup_key(value: &Value, key: &str) -> Option<Value> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k.as_str() == key {
                    return (!matches!(v, Value::Null)).then(|| v.clone());
                }
            }
            None
        }
        _ => None,
    }
}

async fn resolve_root(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    model: &str,
    kind: RootKind,
) -> Result<Option<Value>, async_graphql::Error> {
    let inner = ctx
        .data_opt::<Arc<EngineInner>>()
        .cloned()
        .ok_or_else(|| async_graphql::Error::new("GraphqlEngine not in request data"))?;
    let session = ctx
        .data_opt::<Session>()
        .cloned()
        .unwrap_or_else(Session::new);
    let authority = ctx.data_opt::<ExecutionAuthority>();
    let role = privilege_role_for_request(authority, &session, &inner.anonymous_role);

    let selection = compile::selection_from_field(ctx.field());
    let plan = compile::compile_query(&inner, &session, &role, model, kind, &selection)
        .map_err(|e| client_error("BAD_REQUEST", sanitize_compile_error(&e)))?;
    let value = match plan {
        QueryPlan::CellByKey {
            model,
            pk,
            row_filter,
        } => execute_cell_by_key(&inner, &model, &pk, row_filter.as_ref(), &selection)
            .await
            .map_err(|_| client_error("INTERNAL", "cell read dependency failed"))?,
        QueryPlan::Sql(plan) => {
            if let Some(protocol) = ctx.data_opt::<ProtocolResponseAccumulator>().cloned() {
                let role_surface = inner.role_surfaces.get(&role).cloned().ok_or_else(|| {
                    client_error("INTERNAL", "authorized GraphQL role surface is unavailable")
                })?;
                let executed = super::query_protocol::execute_query_with_protocol(
                    &inner,
                    role_surface,
                    protocol.clone(),
                    &plan,
                    None,
                )
                .await
                .map_err(|e| client_error_for_execute_err(&e))?;
                protocol
                    .record_query_metadata(executed.snapshot, None)
                    .map_err(|_| client_error("INTERNAL", "query evidence encoding failed"))?;
                executed.value
            } else {
                super::engine::execute_plan(&inner, &plan)
                    .await
                    .map_err(|e| client_error_for_execute_err(&e))?
            }
        }
    };
    // `None` (not `Some(Null)`) so nullable by_pk roots do not try to resolve
    // non-null child fields on a null parent.
    if matches!(value, Value::Null) {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

/// Privilege pack for SQL grants: surface execution authority, else anonymous.
fn privilege_role_for_request(
    authority: Option<&ExecutionAuthority>,
    session: &Session,
    anonymous_role: &str,
) -> String {
    if let Some(authority) = authority {
        return authority.privilege_role.clone();
    }
    // Fallback for schema unit tests that execute without engine request data.
    session
        .roles()
        .first()
        .map(|role| (*role).to_string())
        .unwrap_or_else(|| anonymous_role.to_string())
}

/// Closed set of engine-authored GraphQL `extensions.code` values (v1 freeze).
/// Async-graphql document validation may still emit uncoded errors.
#[allow(dead_code)] // public contract constant; asserted in unit tests
pub const ENGINE_ERROR_CODES: &[&str] = &[
    "BAD_REQUEST",
    "FORBIDDEN",
    "TIMEOUT",
    "INTERNAL",
    "UNAUTHORIZED", // command mutations
    "NOT_FOUND",    // command mutations
    "REJECTED",     // command mutations
    "COMMAND_ID_REUSE",
    "COMMAND_IN_PROGRESS",
    "COMMAND_EXPIRED",
];

/// Map executor error strings to stable client errors (`extensions.code`).
pub(crate) fn client_error_for_execute_err(e: &str) -> async_graphql::Error {
    if e.contains("timeout") {
        client_error("TIMEOUT", "statement timeout")
    } else {
        client_error("INTERNAL", "internal error")
    }
}

fn sanitize_compile_error(e: &str) -> String {
    // Stable short messages; never return raw SQL.
    if e.contains("max depth") {
        "max depth exceeded".into()
    } else if e.contains("too complex") || e.contains("query too complex") {
        "query too complex".into()
    } else if e.contains("max_in_list")
        || e.contains("_in list")
        || e.contains("max_bool_width")
        || e.contains("_and list")
        || e.contains("_or list")
    {
        "list too long".into()
    } else if e.contains("invalid GraphQL response key") {
        "invalid response key".into()
    } else if e.contains("unknown comparison")
        || e.contains("unknown where field")
        || e.contains("ungranted where")
        || e.contains("unknown order_by")
        || e.contains("ungranted order_by")
        || e.contains("ambiguous order_by")
    {
        "invalid filter".into()
    } else if e.contains("cell-by-key") {
        "unsupported on cell store".into()
    } else {
        "bad request".into()
    }
}

fn client_error(code: &str, message: impl Into<String>) -> async_graphql::Error {
    use async_graphql::ErrorExtensions;
    let code = code.to_string();
    async_graphql::Error::new(message.into()).extend_with(move |_, ext| {
        ext.set("code", code.as_str());
    })
}

/// Command-mutation errors also carry numeric HTTP `extensions.status`.
fn client_error_with_status(
    code: &str,
    status: u16,
    message: impl Into<String>,
) -> async_graphql::Error {
    use async_graphql::ErrorExtensions;
    let code = code.to_string();
    async_graphql::Error::new(message.into()).extend_with(move |_, ext| {
        ext.set("code", code.as_str());
        ext.set("status", status as i32);
    })
}

#[cfg(test)]
mod execute_err_mapping_tests {
    use super::{client_error_for_execute_err, sanitize_compile_error, ENGINE_ERROR_CODES};

    #[test]
    fn engine_error_codes_closed_set_v1() {
        // Freeze: expanding this set is a client-breaking change.
        assert_eq!(
            ENGINE_ERROR_CODES,
            &[
                "BAD_REQUEST",
                "FORBIDDEN",
                "TIMEOUT",
                "INTERNAL",
                "UNAUTHORIZED",
                "NOT_FOUND",
                "REJECTED",
                "COMMAND_ID_REUSE",
                "COMMAND_IN_PROGRESS",
                "COMMAND_EXPIRED",
            ]
        );
    }

    #[test]
    fn statement_timeout_maps_to_timeout_code() {
        let err = client_error_for_execute_err("statement timeout");
        assert_eq!(err.message, "statement timeout");
        let code = err
            .extensions
            .as_ref()
            .and_then(|ext| ext.get("code"))
            .map(|v| format!("{v:?}"));
        assert!(
            code.as_deref()
                .map(|c| c.contains("TIMEOUT"))
                .unwrap_or(false),
            "expected TIMEOUT extension, got {code:?}"
        );
    }

    #[test]
    fn other_errors_map_to_internal() {
        let err = client_error_for_execute_err("sqlite execute: boom");
        assert_eq!(err.message, "internal error");
        let code = err
            .extensions
            .as_ref()
            .and_then(|ext| ext.get("code"))
            .map(|v| format!("{v:?}"));
        assert!(
            code.as_deref()
                .map(|c| c.contains("INTERNAL"))
                .unwrap_or(false),
            "expected INTERNAL extension, got {code:?}"
        );
    }

    #[test]
    fn sanitize_compile_error_table() {
        assert_eq!(
            sanitize_compile_error("max depth exceeded"),
            "max depth exceeded"
        );
        assert_eq!(
            sanitize_compile_error("_and list length 999 exceeds max_bool_width 256"),
            "list too long"
        );
        assert_eq!(
            sanitize_compile_error("_in list length 500 exceeds max_in_list 100"),
            "list too long"
        );
        assert_eq!(
            sanitize_compile_error("invalid GraphQL response key `x y`"),
            "invalid response key"
        );
        assert_eq!(
            sanitize_compile_error("unknown comparison op `_wat`"),
            "invalid filter"
        );
        assert_eq!(
            sanitize_compile_error("unknown where field `nope`"),
            "invalid filter"
        );
        assert_eq!(
            sanitize_compile_error("ungranted order_by column `secret`"),
            "invalid filter"
        );
        assert_eq!(
            sanitize_compile_error(
                "ambiguous order_by entry: use one field per list entry to declare priority"
            ),
            "invalid filter"
        );
        assert_eq!(
            sanitize_compile_error("SELECT * FROM secret"),
            "bad request"
        );
        assert_eq!(
            sanitize_compile_error(
                "cell-by-key store does not support list queries (would fan out to N cells); declare a SQL index read model"
            ),
            "unsupported on cell store"
        );
    }
}

async fn resolve_command(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    command_name: &str,
) -> Result<Option<Value>, async_graphql::Error> {
    use crate::command_dispatch::SharedCommandHost;

    let session = ctx
        .data_opt::<Session>()
        .cloned()
        .unwrap_or_else(Session::new);
    let host = ctx.data_opt::<SharedCommandHost>();
    let Some(host) = host else {
        return Err(client_error(
            "INTERNAL",
            "command host not configured (use graphql_router_with_host or graphql_router_with_service)",
        ));
    };

    let input = ctx
        .args
        .get("input")
        .map(|v| v.deserialize::<serde_json::Value>())
        .transpose()
        .map_err(|e| client_error("BAD_REQUEST", format!("invalid command input: {e:?}")))?
        .unwrap_or(serde_json::json!({}));

    let protocol = ctx
        .data_opt::<ProtocolResponseAccumulator>()
        .cloned()
        .ok_or_else(|| {
            client_error(
                "INTERNAL",
                "causal command protocol is not configured for this endpoint",
            )
        })?;
    protocol
        .claim_dispatch()
        .map_err(|error| client_error("BAD_REQUEST", error.to_string()))?;
    let command_id = ctx
        .args
        .get("commandId")
        .ok_or_else(|| client_error("BAD_REQUEST", "missing commandId"))?
        .deserialize::<String>()
        .map_err(|_| client_error("BAD_REQUEST", "invalid commandId"))?;
    let principal = ctx
        .data_opt::<VerifiedPrincipal>()
        .cloned()
        .ok_or_else(|| {
            client_error_with_status(
                "UNAUTHORIZED",
                401,
                "durable commands require a verified OIDC bearer",
            )
        })?;
    let result = host
        .invoke(
            command_name,
            &command_id,
            input,
            session,
            principal,
            Some(protocol.clone()),
        )
        .await
        .map_err(|error| {
            client_error_with_status(error.code(), error.status_code(), error.client_message())
        })?;
    protocol
        .record_receipt(&result.receipt)
        .map_err(|_| client_error("INTERNAL", "causal receipt encoding failed"))?;
    Value::from_json(result.payload)
        .map(Some)
        .map_err(|e| client_error("INTERNAL", format!("response encode: {e}")))
}

async fn resolve_command_status(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
) -> Result<Option<Value>, async_graphql::Error> {
    use crate::command_dispatch::SharedCommandHost;
    use async_graphql::indexmap::IndexMap;

    let session = ctx
        .data_opt::<Session>()
        .cloned()
        .unwrap_or_else(Session::new);
    let principal = ctx
        .data_opt::<VerifiedPrincipal>()
        .cloned()
        .ok_or_else(|| {
            client_error_with_status(
                "UNAUTHORIZED",
                401,
                "durable command status requires a verified OIDC bearer",
            )
        })?;
    let protocol = ctx
        .data_opt::<ProtocolResponseAccumulator>()
        .cloned()
        .ok_or_else(|| {
            client_error(
                "INTERNAL",
                "causal command protocol is not configured for this endpoint",
            )
        })?;
    let host = ctx.data_opt::<SharedCommandHost>().ok_or_else(|| {
        client_error(
            "INTERNAL",
            "command host not configured (use graphql_router_with_host or graphql_router_with_service)",
        )
    })?;
    let command_id = ctx
        .args
        .get("commandId")
        .ok_or_else(|| client_error("BAD_REQUEST", "missing commandId"))?
        .deserialize::<String>()
        .map_err(|_| client_error("BAD_REQUEST", "invalid commandId"))?;

    let status = host
        .status(&command_id, &session, principal, Some(protocol.clone()))
        .await
        .map_err(|error| {
            client_error_with_status(error.code(), error.status_code(), error.client_message())
        })?;
    protocol
        .record_status(&status)
        .map_err(|_| client_error("INTERNAL", "causal status encoding failed"))?;
    let mut value = IndexMap::new();
    value.insert(
        async_graphql::Name::new("state"),
        Value::Enum(async_graphql::Name::new(status.state.as_str())),
    );
    Ok(Some(Value::Object(value)))
}

#[cfg(test)]
mod causal_command_schema_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::command::{CommandConsistency, CommandEffects};
    use crate::command_dispatch::{LocalCommandHost, SharedCommandHost};
    use crate::graphql::protocol::{
        DistributedEnvelopeV1, ProtocolResponseAccumulator, ProtocolTokenCodec,
        ProtocolTokenPurpose,
    };
    use crate::graphql::sdl::graphql_sdl_from_surface;
    use crate::graphql::surface::{
        RootField, RootKind, SurfaceCommand, SurfaceDialect, SurfaceSelection,
    };
    use crate::microsvc::Service;

    fn command_surface() -> Surface {
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
                    fields: vec![SurfaceTypeField {
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
                    fields: vec![SurfaceTypeField {
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

    fn runtime_sdl(surface: &Surface) -> String {
        build_role_schema(surface, 32, 1_000, false)
            .expect("role schema should build")
            .sdl()
    }

    fn protocol_accumulator() -> ProtocolResponseAccumulator {
        let codec = ProtocolTokenCodec::new([0x51; 32]);
        let cache_scope = codec
            .issue(
                ProtocolTokenPurpose::CacheScope,
                &("schema-unit-test", "status-test"),
            )
            .expect("test cache scope should encode");
        ProtocolResponseAccumulator::new(
            DistributedEnvelopeV1::new(
                "sha256:schema-unit-test",
                "sha256:authorization-unit-test",
                cache_scope,
                None,
            ),
            codec,
        )
    }

    #[test]
    fn causal_runtime_and_static_sdl_share_status_protocol() {
        let surface = command_surface();
        let static_sdl = graphql_sdl_from_surface(&surface).unwrap();
        let runtime_sdl = runtime_sdl(&surface);

        for expected in [
            "enum DistributedCommandState",
            "type DistributedCommandStatus",
            "state: DistributedCommandState!",
            "commandStatus(commandId: ID!): DistributedCommandStatus!",
        ] {
            assert!(
                static_sdl.contains(expected),
                "static SDL missing `{expected}`"
            );
            assert!(
                runtime_sdl.contains(expected),
                "runtime SDL missing `{expected}`:\n{runtime_sdl}"
            );
        }
        for state in DISTRIBUTED_COMMAND_STATE_VALUES {
            assert!(static_sdl.contains(&format!("\n  {state}\n")));
            assert!(
                runtime_sdl.contains(&format!("\t{state}\n"))
                    || runtime_sdl.contains(&format!("\n  {state}\n")),
                "runtime SDL missing lowercase enum value `{state}`:\n{runtime_sdl}"
            );
        }
        assert!(!static_sdl.contains("_empty: Boolean!"));
        assert!(!runtime_sdl.contains("_empty: Boolean!"));
    }

    #[test]
    fn causal_runtime_schema_fails_closed_on_root_and_type_collisions() {
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
        let error = build_role_schema(&root_collision, 32, 1_000, false).unwrap_err();
        assert!(
            error.contains(COMMAND_STATUS_ROOT_FIELD) && error.contains("collides"),
            "{error}"
        );

        let mut type_collision = command_surface();
        type_collision.commands[0].input = SurfaceCommandShape::Typed(SurfaceTypeDef {
            name: DISTRIBUTED_COMMAND_STATUS_TYPE.into(),
            fields: Vec::new(),
        });
        let error = build_role_schema(&type_collision, 32, 1_000, false).unwrap_err();
        assert!(
            error.contains(DISTRIBUTED_COMMAND_STATUS_TYPE)
                && error.contains("causal protocol type"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn command_status_requires_verified_principal() {
        let schema = build_role_schema(&command_surface(), 32, 1_000, false).unwrap();
        let response = schema
            .execute(format!(
                "{{ {COMMAND_STATUS_ROOT_FIELD}(commandId: \"{}\") {{ state }} }}",
                uuid::Uuid::now_v7()
            ))
            .await;

        assert_eq!(response.errors.len(), 1, "{response:?}");
        assert_eq!(
            response.errors[0].message,
            "durable command status requires a verified OIDC bearer"
        );
        let extensions = response.errors[0]
            .extensions
            .as_ref()
            .expect("auth error extensions");
        assert!(
            format!("{:?}", extensions.get("code")).contains("UNAUTHORIZED"),
            "{extensions:?}"
        );
        assert!(
            format!("{:?}", extensions.get("status")).contains("401"),
            "{extensions:?}"
        );
    }

    #[tokio::test]
    async fn authorized_unknown_status_returns_only_public_state() {
        let schema = build_role_schema(&command_surface(), 32, 1_000, false).unwrap();
        let request = async_graphql::Request::new(format!(
            "{{ {COMMAND_STATUS_ROOT_FIELD}(commandId: \"{}\") {{ s: state }} }}",
            uuid::Uuid::now_v7()
        ))
        .data(Arc::new(LocalCommandHost::new(Arc::new(
            Service::new().named("status-test"),
        ))) as SharedCommandHost)
        .data(VerifiedPrincipal::test_oidc(
            "https://issuer.example/",
            "status-test-subject",
            &["status-test-audience"],
        ))
        .data(protocol_accumulator());
        let response = schema.execute(request).await;

        assert!(response.errors.is_empty(), "{response:?}");
        assert_eq!(
            response.data.into_json().unwrap(),
            serde_json::json!({
                COMMAND_STATUS_ROOT_FIELD: {
                    "s": "unknown"
                }
            })
        );
    }
}
