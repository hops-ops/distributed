use super::*;

/// Convenience wrapper used heavily by unit tests; production uses
/// [`client_manifest_from_surface_with_execution`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn client_manifest_from_surface(
    service_id: &str,
    identity: ClientSurfaceIdentity,
    surface: &Surface,
) -> Result<DistributedClientManifest, ClientManifestError> {
    client_manifest_from_surface_with_execution(
        service_id,
        identity,
        surface,
        ClientExecutionLimits::default(),
    )
}

pub(super) fn client_manifest_from_surface_with_execution(
    service_id: &str,
    identity: ClientSurfaceIdentity,
    surface: &Surface,
    execution: ClientExecutionLimits,
) -> Result<DistributedClientManifest, ClientManifestError> {
    if service_id.trim().is_empty() {
        return Err(ClientManifestError("service_id must not be empty".into()));
    }
    let identity = identity.canonicalized()?;
    match (&surface.selection, &identity) {
        (SurfaceSelection::Catalog, _) => {
            return Err(ClientManifestError(
                "client manifests require an explicitly role- or application-selected Surface"
                    .into(),
            ));
        }
        (SurfaceSelection::Role { name: selected }, ClientSurfaceIdentity::Role { name })
            if selected == name => {}
        (
            SurfaceSelection::Application {
                name: selected_name,
                eligible_roles: selected_eligible_roles,
                schema_roles: selected_schema_roles,
            },
            ClientSurfaceIdentity::Application {
                name,
                eligible_roles,
                schema_roles,
            },
        ) if selected_name == name
            && selected_eligible_roles == eligible_roles
            && selected_schema_roles == schema_roles => {}
        _ => {
            return Err(ClientManifestError(
                "client Surface identity does not match its authorization selection provenance"
                    .into(),
            ));
        }
    }
    validate_surface_structure(surface)?;

    let live_models: BTreeSet<&str> = surface
        .subscription_fields
        .iter()
        .filter(|root| root.kind == RootKind::List)
        .map(|root| root.model_name.as_str())
        .collect();
    let mut models = Vec::new();
    for model in surface.models.values() {
        let mut fields: Vec<ClientField> = model
            .columns
            .iter()
            .map(|field| {
                let codec = scalar_codec(&field.scalar).ok_or_else(|| {
                    ClientManifestError(format!(
                        "model `{}` field `{}` uses unsupported scalar `{}`",
                        model.model_name, field.name, field.scalar
                    ))
                })?;
                Ok(ClientField {
                    name: field.name.clone(),
                    scalar: field.scalar.clone(),
                    codec: codec.into(),
                    nullable: field.nullable,
                })
            })
            .collect::<Result<_, ClientManifestError>>()?;
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        let field_by_name: BTreeMap<&str, &ClientField> = fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();
        let normalization = if model_has_client_normalized_identity(model) {
            ModelNormalization::Normalized {
                fields: model
                    .primary_key
                    .iter()
                    .map(|key| ClientKeyField {
                        name: key.clone(),
                        codec: field_by_name[key.as_str()].codec.clone(),
                    })
                    .collect(),
                encoding: KEY_ENCODING.into(),
            }
        } else {
            ModelNormalization::Embedded
        };

        let mut relationships = Vec::new();
        for relationship in &model.relationships {
            let Some(target) = surface.models.get(&relationship.target_model) else {
                return Err(ClientManifestError(format!(
                    "surface relationship `{}` targets absent model `{}`",
                    relationship.name, relationship.target_model
                )));
            };
            let target_field_by_name: BTreeMap<&str, &crate::graphql::surface::ColumnField> =
                target
                    .columns
                    .iter()
                    .map(|field| (field.name.as_str(), field))
                    .collect();
            let source_field_by_name: BTreeMap<&str, &crate::graphql::surface::ColumnField> = model
                .columns
                .iter()
                .map(|field| (field.name.as_str(), field))
                .collect();
            let stable_key_mapping = |local: &[String], remote: &[String]| {
                !local.is_empty()
                    && local.len() == remote.len()
                    && local.iter().all(|key| {
                        source_field_by_name
                            .get(key.as_str())
                            .is_some_and(|field| field.scalar != "BigInt")
                    })
                    && remote.iter().all(|key| {
                        target_field_by_name
                            .get(key.as_str())
                            .is_some_and(|field| field.scalar != "BigInt")
                    })
            };
            let key_mapping = match &relationship.keys {
                SurfaceRelationshipKeys::Direct { local, remote }
                    if stable_key_mapping(local, remote) =>
                {
                    RelationshipKeyMapping::Direct {
                        local: local.clone(),
                        remote: remote.clone(),
                    }
                }
                SurfaceRelationshipKeys::Through {
                    local,
                    remote,
                    table,
                    source_foreign_key,
                    target_foreign_key,
                } if stable_key_mapping(local, remote) => RelationshipKeyMapping::Through {
                    local: local.clone(),
                    remote: remote.clone(),
                    table: table.clone(),
                    source_foreign_key: source_foreign_key.clone(),
                    target_foreign_key: target_foreign_key.clone(),
                },
                SurfaceRelationshipKeys::ThroughOpaque {
                    local,
                    remote,
                    dependency,
                } if stable_key_mapping(local, remote) => RelationshipKeyMapping::ThroughOpaque {
                    local: local.clone(),
                    remote: remote.clone(),
                    dependency: dependency.clone(),
                },
                SurfaceRelationshipKeys::Embedded => RelationshipKeyMapping::Embedded,
                _ => RelationshipKeyMapping::Embedded,
            };
            let maintenance = match &key_mapping {
                RelationshipKeyMapping::Direct { .. } | RelationshipKeyMapping::Through { .. } => {
                    ClientRelationshipMaintenance::Local
                }
                RelationshipKeyMapping::ThroughOpaque { .. } | RelationshipKeyMapping::Embedded => {
                    ClientRelationshipMaintenance::Revalidate
                }
            };
            relationships.push(ClientRelationship {
                name: relationship.name.clone(),
                target_model: relationship.target_model.clone(),
                target_typename: relationship.target_object.clone(),
                kind: match relationship.kind {
                    RelationshipKind::HasMany => ClientRelationshipKind::HasMany,
                    RelationshipKind::BelongsTo => ClientRelationshipKind::BelongsTo,
                    RelationshipKind::ManyToMany => ClientRelationshipKind::ManyToMany,
                },
                list: relationship.list,
                nullable: relationship.nullable,
                arguments: relationship
                    .arguments
                    .iter()
                    .map(argument_manifest)
                    .collect(),
                key_mapping,
                maintenance,
                dependencies: sorted_unique(relationship.dependencies.clone()),
                filter: relationship.list.then(|| filter_semantics(surface, target)),
                order: relationship.list.then(|| order_semantics(target)),
                pagination: relationship
                    .list
                    .then(|| pagination_semantics(surface, target)),
                aggregate: relationship.aggregate.as_ref().map(|aggregate| {
                    ClientRelationshipAggregate {
                        name: aggregate.name.clone(),
                        arguments: aggregate.arguments.iter().map(argument_manifest).collect(),
                        semantics: aggregate_semantics(
                            target,
                            aggregate.type_name.clone(),
                            pagination_semantics(surface, target),
                        ),
                        dependencies: sorted_unique(aggregate.dependencies.clone()),
                    }
                }),
                live: relationship.list && live_models.contains(relationship.target_model.as_str()),
            });
        }
        relationships.sort_by(|a, b| a.name.cmp(&b.name));
        let filter_input = filter_input(surface, model)?;
        let causal_owner = model_has_visible_causal_owner(surface, &model.model_name);
        models.push(ClientModel {
            id: model.model_name.clone(),
            typename: model.object_name.clone(),
            source_table: model.table_name.clone(),
            dependencies: vec![model.table_name.clone()],
            normalization,
            fields,
            relationships,
            filter_input,
            row_policy: row_policy_manifest(&model.row_policy),
            // `_sourced_version` is only source metadata. These flags derive
            // from the role-visible Task 15 causal owner, never that column.
            record_revisions: causal_owner,
            tombstones: causal_owner,
        });
    }
    models.sort_by(|a, b| a.id.cmp(&b.id));

    let mut roots = Vec::new();
    for (operation, fields) in [
        (ClientRootOperation::Query, &surface.query_fields),
        (
            ClientRootOperation::Subscription,
            &surface.subscription_fields,
        ),
    ] {
        for root in fields {
            let model = surface.models.get(&root.model_name).ok_or_else(|| {
                ClientManifestError(format!(
                    "root `{}` references absent model `{}`",
                    root.name, root.model_name
                ))
            })?;
            let has_filter = root
                .arguments
                .iter()
                .any(|argument| argument.kind == SurfaceArgumentKind::Filter);
            let filter = has_filter.then(|| filter_semantics(surface, model));
            let has_order = root
                .arguments
                .iter()
                .any(|argument| argument.kind == SurfaceArgumentKind::Order);
            let order = has_order.then(|| order_semantics(model));
            let pagination =
                root.default_limit
                    .zip(root.max_limit)
                    .map(|(default_limit, max_limit)| ClientPaginationSemantics {
                        kind: "offset".into(),
                        default_limit,
                        max_limit,
                        coverage: "window".into(),
                    });
            let aggregate = matches!(root.kind, RootKind::Aggregate).then(|| {
                aggregate_semantics(
                    model,
                    aggregate_type_name(&model.schema),
                    pagination_semantics(surface, model),
                )
            });
            roots.push(ClientRoot {
                id: format!(
                    "{}:{}",
                    match operation {
                        ClientRootOperation::Query => "query",
                        ClientRootOperation::Subscription => "subscription",
                    },
                    root.name
                ),
                operation,
                name: root.name.clone(),
                kind: match root.kind {
                    RootKind::List => ClientRootKind::List,
                    RootKind::ByPk => ClientRootKind::ByPk,
                    RootKind::Aggregate => ClientRootKind::Aggregate,
                },
                model: root.model_name.clone(),
                arguments: root.arguments.iter().map(argument_manifest).collect(),
                filter,
                order,
                pagination,
                aggregate,
                dependencies: sorted_unique(root.dependencies.clone()),
                live: operation == ClientRootOperation::Subscription
                    || surface.subscription_fields.iter().any(|candidate| {
                        candidate.name == root.name && candidate.kind == root.kind
                    }),
            });
        }
    }
    roots.sort_by(|a, b| a.id.cmp(&b.id));

    let mut commands = Vec::new();
    for command in &surface.commands {
        let input = command_shape(&command.input)?;
        let output = command_shape(&command.output)?;
        let grants = sorted_unique(command.roles.clone());
        let operation = command_operation(&command.field_name, &input, &output);
        let operation_hash = hash_bytes(operation.as_bytes());
        let consistency = CommandConsistencyExtension {
            version: 1,
            kind: match command.consistency {
                CommandConsistency::Succeeded => "succeeded",
                CommandConsistency::Eventual => "eventual",
                CommandConsistency::Atomic => "atomic",
            }
            .into(),
        };
        let direct_projection = command_direct_projection_extension(command, surface)?;
        let input_defaults = (!command.input_defaults.is_empty())
            .then(|| {
                command
                    .input_defaults
                    .iter()
                    .map(|default| serde_json::to_value(default).map(canonical_json_value))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|defaults| CommandInputDefaultsExtension {
                        version: 1,
                        defaults,
                    })
            })
            .transpose()?;
        let mut trusted_presets = command_trusted_preset_descriptors(command, surface)?;
        let projection = command_projection_extension(command, surface, &trusted_presets)?;
        if let Some(projection) = &projection {
            for occurrence in &projection.preview_occurrences {
                for preview in &occurrence.values {
                    let ClientProjectionPreviewSource::TrustedPreset { name, codec } =
                        &preview.source
                    else {
                        continue;
                    };
                    match trusted_presets
                        .iter()
                        .find(|descriptor| descriptor.name == *name)
                    {
                        Some(descriptor) if descriptor.codec == *codec => {}
                        Some(descriptor) => {
                            return Err(ClientManifestError(format!(
                                "command `{}` trusted preset `{name}` is used with incompatible codecs `{}` and `{codec}`",
                                command.command_name,
                                descriptor.codec
                            )));
                        }
                        None => trusted_presets.push(ClientTrustedPresetDescriptor {
                            name: name.clone(),
                            codec: codec.clone(),
                        }),
                    }
                }
            }
            trusted_presets.sort();
        }
        commands.push(ClientCommand {
            version: 1,
            name: command.command_name.clone(),
            mutation_field: command.field_name.clone(),
            grants,
            input,
            output,
            operation,
            operation_hash,
            extensions: ClientCommandExtensionSlots {
                version: COMMAND_EXTENSION_SLOTS_VERSION,
                consistency,
                direct_projection,
                input_defaults,
                effects: None,
                confirmations: None,
                projection,
                trusted_presets,
            },
        });
    }
    commands.sort_by(|a, b| a.name.cmp(&b.name));

    let command_status = (!commands.is_empty()).then(|| {
        let operation = command_status_operation();
        ClientProtocolOperation {
            name: "Distributed_CommandStatus".into(),
            operation_hash: hash_bytes(operation.as_bytes()),
            operation,
        }
    });
    let protocol_operations = ClientProtocolOperations {
        version: PROTOCOL_OPERATIONS_VERSION,
        command_status,
    };

    let confirming_projectors: BTreeSet<&str> = surface
        .commands
        .iter()
        .flat_map(|command| {
            command
                .confirmations
                .iter()
                .map(|confirmation| confirmation.projector.as_str())
        })
        .collect();
    let mut projectors: Vec<ClientProjector> = surface
        .projectors
        .iter()
        .map(|projector| ClientProjector {
            version: PROJECTOR_ENTRY_VERSION,
            name: projector.name.clone(),
            facts: sorted_unique(projector.facts.clone()),
            models: sorted_unique(projector.models.clone()),
            dependencies: sorted_unique(projector.dependencies.clone()),
            causal_confirmation: confirming_projectors.contains(projector.name.as_str()),
        })
        .collect();
    projectors.sort_by(|a, b| a.name.cmp(&b.name));
    let (projection_programs, projection_bindings) = projection_manifest(surface)?;

    let scalar_codecs = supported_scalar_codecs();
    let record_evidence = query_footprint_has_record_evidence(surface);
    let leaderboards = client_leaderboards(surface);
    let live_leaderboards = leaderboards
        .iter()
        .any(|leaderboard| leaderboard.roots.iter().any(|root| root.live));
    let capabilities = ClientCapabilities {
        live_queries: !surface.subscription_fields.is_empty() || live_leaderboards,
        record_revisions: record_evidence,
        tombstones: record_evidence,
        causal_receipts: !commands.is_empty(),
        live_resume: query_footprint_supports_live_resume(surface),
        query_fallback: "revalidate".into(),
        // Every generated operation consumes one authoritative scope envelope,
        // including query-only surfaces.
        cache_scope: true,
        confirmed_persistence: false,
    };
    let protocol_fingerprint = protocol_fingerprint()?;

    #[derive(Serialize)]
    struct SchemaMaterial<'a> {
        manifest_version: u32,
        protocol_version: u32,
        service_id: &'a str,
        surface: &'a ClientSurfaceIdentity,
        execution: &'a ClientExecutionLimits,
        capabilities: &'a ClientCapabilities,
        scalar_codecs: &'a [ScalarCodec],
        models: &'a [ClientModel],
        roots: &'a [ClientRoot],
        commands: &'a [ClientCommand],
        protocol_operations: &'a ClientProtocolOperations,
        projectors: &'a [ClientProjector],
        projection_programs: &'a [ClientProjectionProgram],
        projection_bindings: &'a [ClientProjectionBinding],
        #[serde(skip_serializing_if = "<[ClientLeaderboard]>::is_empty")]
        leaderboards: &'a [ClientLeaderboard],
    }
    let schema_material = SchemaMaterial {
        manifest_version: DISTRIBUTED_CLIENT_MANIFEST_VERSION,
        protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
        service_id,
        surface: &identity,
        execution: &execution,
        capabilities: &capabilities,
        scalar_codecs: &scalar_codecs,
        models: &models,
        roots: &roots,
        commands: &commands,
        protocol_operations: &protocol_operations,
        projectors: &projectors,
        projection_programs: &projection_programs,
        projection_bindings: &projection_bindings,
        leaderboards: &leaderboards,
    };
    let schema_fingerprint = hash_json(&schema_material)?;

    let manifest = DistributedClientManifest {
        manifest_version: DISTRIBUTED_CLIENT_MANIFEST_VERSION,
        protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
        service_id: service_id.into(),
        surface: identity,
        schema_fingerprint,
        protocol_fingerprint,
        execution,
        capabilities,
        scalar_codecs,
        models,
        roots,
        commands,
        protocol_operations,
        projectors,
        projection_programs,
        projection_bindings,
        leaderboards,
    };
    // Validate the one exact scope-wide descriptor union while the complete
    // role-selected manifest is still available. The serialized manifest need
    // not duplicate this deterministic derivation.
    trusted_preset_descriptors(&manifest)?;
    Ok(manifest)
}

/// Leaderboard roots of the selected surface (same names and arguments as
/// the runtime schema and SDL).
fn client_leaderboards(surface: &Surface) -> Vec<ClientLeaderboard> {
    let mut out = surface
        .leaderboards()
        .iter()
        .map(|leaderboard| ClientLeaderboard {
            name: leaderboard.name().to_string(),
            entry_typename: crate::graphql::surface::LEADERBOARD_ENTRY_TYPE.to_string(),
            order: crate::graphql::surface::leaderboard_order_str(leaderboard.order()).to_string(),
            boards: match leaderboard.boards() {
                crate::graphql::surface::LeaderboardBoards::Single { .. } => "single",
                crate::graphql::surface::LeaderboardBoards::Family { .. } => "family",
            }
            .to_string(),
            default_first: leaderboard.default_first(),
            max_first: leaderboard.max_first(),
            max_radius: leaderboard.max_radius(),
            roots: leaderboard
                .roots(leaderboard.selected_grant())
                .into_iter()
                .map(|root| ClientLeaderboardRoot {
                    live: surface.subscriptions && root.operation.is_live(),
                    list: root.operation.is_list(),
                    operation: root.operation.as_str().to_string(),
                    arguments: root
                        .arguments
                        .iter()
                        .map(|argument| ClientLeaderboardArgument {
                            name: argument.name.to_string(),
                            type_name: argument.type_name.to_string(),
                            nullable: argument.nullable,
                        })
                        .collect(),
                    name: root.name,
                })
                .collect(),
        })
        .filter(|leaderboard| !leaderboard.roots.is_empty())
        .collect::<Vec<_>>();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}
