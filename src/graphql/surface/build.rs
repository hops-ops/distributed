use super::*;

pub fn build_surface(tables: &[TableSchema], options: &SurfaceOptions) -> Result<Surface, String> {
    let mut catalog = BTreeMap::new();
    let mut all_table_ids = BTreeSet::new();
    for schema in tables {
        schema
            .validate()
            .map_err(|e| format!("schema `{}` invalid: {e}", schema.model_name))?;
        if schema.model_name.trim().is_empty() {
            return Err("table model id must not be empty".into());
        }
        if catalog
            .insert(schema.model_name.clone(), schema.clone())
            .is_some()
        {
            return Err(format!(
                "duplicate table model id `{}` in Surface catalog",
                schema.model_name
            ));
        }
        if !all_table_ids.insert(schema.table_name.clone()) {
            return Err(format!(
                "table id `{}` collides with another table in Surface catalog",
                schema.table_name
            ));
        }
    }
    let read_models: Vec<&TableSchema> = tables.iter().filter(|t| t.kind.is_read_model()).collect();

    let mut model_ids = BTreeSet::new();
    let mut table_ids = BTreeSet::new();
    let mut object_ids = BTreeSet::new();
    for schema in &read_models {
        if schema.model_name.trim().is_empty() {
            return Err("read model id must not be empty".into());
        }
        if !model_ids.insert(schema.model_name.clone()) {
            return Err(format!(
                "duplicate read model id `{}` in Surface inventory",
                schema.model_name
            ));
        }
        if !table_ids.insert(schema.table_name.clone()) {
            return Err(format!(
                "duplicate read-model table id `{}` in Surface inventory",
                schema.table_name
            ));
        }
        let object_id = object_type_name(schema).to_string();
        if !object_ids.insert(object_id.clone()) {
            return Err(format!(
                "duplicate GraphQL object id `{object_id}` in Surface inventory"
            ));
        }
        let mut relationship_ids = BTreeSet::new();
        for relationship in &schema.relationships {
            if relationship.field_name.trim().is_empty() {
                return Err(format!(
                    "model `{}` has a relationship with an empty field id",
                    schema.model_name
                ));
            }
            if !relationship_ids.insert(relationship.field_name.clone()) {
                return Err(format!(
                    "model `{}` declares duplicate relationship field `{}`",
                    schema.model_name, relationship.field_name
                ));
            }
            if matches!(relationship.kind, RelationshipKind::ManyToMany)
                && relationship.through.is_none()
            {
                return Err(format!(
                    "model `{}` relationship `{}` many-to-many must declare `through`",
                    schema.model_name, relationship.field_name
                ));
            }
        }
    }

    let by_model: BTreeMap<&str, &TableSchema> = read_models
        .iter()
        .map(|t| (t.model_name.as_str(), *t))
        .collect();
    // All tables (incl. operational / unexposed join tables) so m2m
    // relationship_emitted can resolve `through` for bool_exp + object fields.
    let by_table: BTreeMap<&str, &TableSchema> =
        tables.iter().map(|t| (t.table_name.as_str(), t)).collect();

    let postgres_json = include_postgres_json_comparison_ops(options.dialect.is_postgres());
    let mut used_scalars: BTreeSet<String> = BTreeSet::new();
    let mut models: BTreeMap<String, SurfaceModel> = BTreeMap::new();

    for schema in &read_models {
        let object_name = object_type_name(schema).to_string();
        if !is_valid_graphql_name(&object_name) {
            return Err(format!(
                "object type `{object_name}` is not a valid GraphQL name"
            ));
        }
        if !is_valid_graphql_name(root_list_field(schema)) {
            return Err(format!(
                "root field `{}` is not a valid GraphQL name",
                root_list_field(schema)
            ));
        }

        let mut columns = Vec::new();
        for col in visible_columns(schema) {
            if !is_valid_graphql_name(&col.column_name) {
                return Err(format!(
                    "model `{}` column `{}` is not a valid GraphQL name",
                    schema.model_name, col.column_name
                ));
            }
            let Some(scalar) = scalar_type_name(&col.column_type) else {
                return Err(format!(
                    "model `{}` column `{}` has unsupported type",
                    schema.model_name, col.column_name
                ));
            };
            used_scalars.insert(scalar.to_string());
            columns.push(ColumnField {
                name: col.column_name.clone(),
                scalar: scalar.to_string(),
                nullable: col.nullable,
            });
        }

        let mut relationships = Vec::new();
        for rel in &schema.relationships {
            if !is_valid_graphql_name(&rel.field_name) {
                return Err(format!(
                    "model `{}` relationship `{}` is not a valid GraphQL name",
                    schema.model_name, rel.field_name
                ));
            }
            if !relationship_emitted(schema, rel, &by_model, &by_table) {
                continue;
            }
            let target = by_model
                .get(rel.target_model.as_str())
                .expect("relationship_emitted");
            let list = matches!(
                rel.kind,
                RelationshipKind::HasMany | RelationshipKind::ManyToMany
            );
            // Belongs-to is always `Option<T>` in the read-model macro, and the
            // client materializer needs a nullable edge so optimistic parent
            // upserts remain complete before the join target is linked (e.g.
            // ChatMessages.author → AuthUsers). FK column nullability is a
            // storage constraint, not GraphQL join nullability.
            let nullable = matches!(rel.kind, RelationshipKind::BelongsTo);
            let (keys, mut dependencies) = relationship_keys(schema, rel, target, &by_table)?;
            dependencies.sort();
            dependencies.dedup();
            relationships.push(RelField {
                name: rel.field_name.clone(),
                target_model: rel.target_model.clone(),
                target_object: object_type_name(target).to_string(),
                kind: rel.kind.clone(),
                list,
                nullable,
                arguments: if list {
                    list_arguments(target)
                } else {
                    Vec::new()
                },
                keys,
                aggregate: (options.aggregates && list).then(|| SurfaceRelationshipAggregate {
                    name: format!("{}_aggregate", rel.field_name),
                    type_name: format!("{}_aggregate", target.table_name),
                    arguments: vec![SurfaceArgument {
                        name: "where".into(),
                        kind: SurfaceArgumentKind::Filter,
                        type_name: format!("{}_bool_exp", target.table_name),
                        nullable: true,
                        list: false,
                    }],
                    dependencies: dependencies.clone(),
                }),
                dependencies,
            });
        }

        models.insert(
            schema.model_name.clone(),
            SurfaceModel {
                model_name: schema.model_name.clone(),
                table_name: schema.table_name.clone(),
                object_name,
                columns,
                relationships,
                primary_key: schema.primary_key.columns.clone(),
                row_policy: SurfaceRowPolicy::Unrestricted,
                role_limit: None,
                aggregations: options.aggregates,
                schema: (*schema).clone(),
            },
        );
    }

    // Drop relationships whose target was not included (defensive).
    let model_keys: BTreeSet<String> = models.keys().cloned().collect();
    for model in models.values_mut() {
        model
            .relationships
            .retain(|r| model_keys.contains(&r.target_model));
    }

    sanitize_relationship_identity(&mut models);

    let aggregate_targets: BTreeMap<String, bool> = models
        .iter()
        .map(|(name, model)| (name.clone(), model.aggregations))
        .collect();
    for model in models.values_mut() {
        for relationship in &mut model.relationships {
            if !aggregate_targets
                .get(&relationship.target_model)
                .copied()
                .unwrap_or(false)
            {
                relationship.aggregate = None;
            } else if let Some(aggregate) = &mut relationship.aggregate {
                aggregate.dependencies = relationship.dependencies.clone();
            }
        }
    }

    let mut comparison_ops = BTreeMap::new();
    for scalar in &used_scalars {
        let ops = comparison_op_fields(scalar, postgres_json);
        comparison_ops.insert(
            comparison_exp_name(scalar),
            ops.into_iter().map(str::to_string).collect(),
        );
    }
    // Always reserve custom scalar names for naming collisions checks downstream.
    let _ = CUSTOM_SCALARS;

    let (query_fields, subscription_fields) = root_fields_for_models(
        &models,
        options.aggregates,
        options.subscriptions,
        options.default_limit,
        options.max_limit,
    );

    validate_root_ids(&query_fields, "query")?;
    validate_root_ids(&subscription_fields, "subscription")?;
    validate_generated_surface_names(&models, &comparison_ops)?;

    Ok(Surface {
        selection: SurfaceSelection::Catalog,
        dialect: options.dialect,
        aggregates: options.aggregates,
        subscriptions: options.subscriptions,
        default_limit: options.default_limit,
        max_limit: options.max_limit,
        catalog,
        models,
        query_fields,
        subscription_fields,
        comparison_ops,
        commands: Vec::new(),
        commands_attached: false,
        projectors: Vec::new(),
        projectors_attached: false,
        service_binding: None,
        leaderboards: Vec::new(),
    })
}
