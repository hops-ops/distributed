use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestedProtocolClient {
    surface: ClientSurfaceIdentity,
    schema_hash: String,
}

fn requested_protocol_client(request: &Request) -> Result<Option<RequestedProtocolClient>, ()> {
    let Some(distributed) = request.extensions.get("distributed") else {
        return Ok(None);
    };
    let distributed = distributed.clone().into_json().map_err(|_| ())?;
    let distributed = distributed.as_object().ok_or(())?;
    let Some(client) = distributed.get("client") else {
        return Ok(None);
    };
    serde_json::from_value(client.clone())
        .map(Some)
        .map_err(|_| ())
}

/// Asserted engine roles for this principal (`x-roles` set only).
pub(crate) fn principal_asserted_roles(session: &Session) -> Vec<String> {
    session
        .roles()
        .into_iter()
        .map(|role| role.to_string())
        .collect()
}

fn principal_may_open_application(asserted: &[String], eligible: &[String]) -> bool {
    // Unauthenticated principals may open surfaces that list `anonymous`.
    if asserted.is_empty() {
        return eligible.iter().any(|role| role == "anonymous");
    }
    asserted.iter().any(|role| {
        eligible
            .binary_search_by(|candidate| candidate.as_str().cmp(role.as_str()))
            .is_ok()
    })
}

fn principal_has_role(asserted: &[String], role: &str) -> bool {
    asserted.iter().any(|existing| existing == role)
}

/// Resolve execution authority for one GraphQL request.
///
/// - Identity is the asserted **set** (`x-roles`).
/// - With a protocol **application** surface: open if any asserted role is
///   eligible; **privilege_role** is the surface privilege pack key.
/// - With a protocol **role** surface: named role must be asserted; privilege
///   is that role.
/// - Without protocol client: empty set → anonymous; single asserted role →
///   that role surface; multi-role without a named surface → reject (must open
///   an application surface).
pub(crate) fn resolve_execution_authority(
    inner: &EngineInner,
    session: &Session,
    request: &Request,
) -> Result<ExecutionAuthority, ()> {
    let asserted = principal_asserted_roles(session);
    let anonymous = inner.anonymous_role.as_str();

    let Some(requested) = requested_protocol_client(request)? else {
        return match asserted.len() {
            0 => {
                if !inner.schemas.contains_key(anonymous) {
                    return Err(());
                }
                Ok(ExecutionAuthority {
                    privilege_role: anonymous.to_string(),
                    asserted_roles: asserted,
                    surface: ClientSurfaceIdentity::role(anonymous),
                })
            }
            1 => {
                let only = asserted[0].clone();
                if !inner.schemas.contains_key(only.as_str()) {
                    return Err(());
                }
                Ok(ExecutionAuthority {
                    privilege_role: only.clone(),
                    asserted_roles: asserted,
                    surface: ClientSurfaceIdentity::role(only),
                })
            }
            _ => Err(()), // multi-role must name an application surface
        };
    };

    match requested.surface {
        ClientSurfaceIdentity::Role { name } => {
            // Anonymous role surface may be opened without asserted roles.
            let allowed = name == anonymous || principal_has_role(&asserted, &name);
            if !allowed {
                return Err(());
            }
            let Some(runtime) = &inner.protocol else {
                // Non-protocol engines still honor role membership for bare role surfaces.
                if !inner.schemas.contains_key(&name) {
                    return Err(());
                }
                return Ok(ExecutionAuthority {
                    privilege_role: name.clone(),
                    asserted_roles: asserted,
                    surface: ClientSurfaceIdentity::role(name),
                });
            };
            let info = runtime.roles.get(&name).ok_or(())?;
            if requested.schema_hash != info.surface.schema_fingerprint {
                return Err(());
            }
            Ok(ExecutionAuthority {
                privilege_role: name.clone(),
                asserted_roles: asserted,
                surface: ClientSurfaceIdentity::role(name),
            })
        }
        ClientSurfaceIdentity::Application {
            name,
            eligible_roles,
            schema_roles,
        } => {
            let runtime = inner.protocol.as_ref().ok_or(())?;
            let application = runtime.applications.get(&name).ok_or(())?;
            // Wire roles must equal the registered eligible set (canonical).
            if eligible_roles != application.eligible_roles
                || schema_roles != application.schema_roles
                || !principal_may_open_application(&asserted, &application.eligible_roles)
                || requested.schema_hash != application.surface.schema_fingerprint
            {
                return Err(());
            }
            Ok(ExecutionAuthority {
                privilege_role: application.privilege_key.clone(),
                asserted_roles: asserted,
                surface: ClientSurfaceIdentity::application_with_schema_roles(
                    name,
                    eligible_roles,
                    schema_roles,
                ),
            })
        }
    }
}

/// Protocol surface selection for envelope material (after authority is known).
pub(crate) fn select_protocol_surface<'a>(
    runtime: &'a ProtocolRuntime,
    authority: &ExecutionAuthority,
) -> Result<
    (
        ClientSurfaceIdentity,
        &'a ProtocolSurfaceInfo,
        &'a str,
        &'a [String],
    ),
    (),
> {
    match &authority.surface {
        ClientSurfaceIdentity::Role { name } => {
            let info = runtime.roles.get(name).ok_or(())?;
            Ok((
                ClientSurfaceIdentity::role(name.clone()),
                &info.surface,
                info.authorization_fingerprint.as_str(),
                info.claim_keys.as_slice(),
            ))
        }
        ClientSurfaceIdentity::Application {
            name,
            eligible_roles,
            schema_roles,
        } => {
            let application = runtime.applications.get(name).ok_or(())?;
            if eligible_roles != &application.eligible_roles
                || schema_roles != &application.schema_roles
            {
                return Err(());
            }
            Ok((
                ClientSurfaceIdentity::application_with_schema_roles(
                    name.clone(),
                    eligible_roles.clone(),
                    schema_roles.clone(),
                ),
                &application.surface,
                application.authorization_fingerprint.as_str(),
                application.claim_keys.as_slice(),
            ))
        }
    }
}

pub(crate) fn resolve_protocol_preset(
    session: &Session,
    descriptor: &ClientTrustedPresetDescriptor,
) -> Option<DistributedTrustedPreset> {
    use base64::Engine as _;

    // Match SQL row-policy claim lookup exactly: applications commonly
    // normalize HTTP header names to lowercase before constructing Session.
    let raw = session
        .get(&descriptor.name)
        .or_else(|| session.get(&descriptor.name.to_ascii_lowercase()))?;
    let value = match descriptor.codec.as_str() {
        "string" | "string_unvalidated_timestamp" => serde_json::Value::String(raw.to_string()),
        "base64" => {
            let decoded = base64::engine::general_purpose::STANDARD.decode(raw).ok()?;
            if base64::engine::general_purpose::STANDARD.encode(decoded) != raw {
                return None;
            }
            serde_json::Value::String(raw.to_string())
        }
        "boolean" => match raw {
            "true" => serde_json::Value::Bool(true),
            "false" => serde_json::Value::Bool(false),
            _ => return None,
        },
        "int32" => {
            let parsed = raw.parse::<i32>().ok()?;
            if parsed.to_string() != raw {
                return None;
            }
            serde_json::Value::Number(parsed.into())
        }
        "json_number_precision_limited" => {
            let parsed = raw.parse::<i64>().ok()?;
            if !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&parsed)
                || parsed.to_string() != raw
            {
                return None;
            }
            serde_json::Value::Number(parsed.into())
        }
        codec if crate::command::CommandUnsignedInteger::from_client_codec(codec).is_some() => {
            let unsigned = crate::command::CommandUnsignedInteger::from_client_codec(codec)?;
            let parsed = raw.parse::<u64>().ok()?;
            if parsed > unsigned.client_max_value() || parsed.to_string() != raw {
                return None;
            }
            serde_json::Value::Number(parsed.into())
        }
        "float64" => {
            let parsed = raw.parse::<f64>().ok()?;
            if !parsed.is_finite() {
                return None;
            }
            serde_json::Value::Number(serde_json::Number::from_f64(parsed)?)
        }
        "json" => serde_json::from_str(raw).ok()?,
        _ => return None,
    };
    Some(DistributedTrustedPreset {
        name: descriptor.name.clone(),
        codec: descriptor.codec.clone(),
        value,
    })
}

pub(crate) fn protocol_trusted_presets(
    manifest: &DistributedClientManifest,
) -> Result<Vec<ClientTrustedPresetDescriptor>, GraphqlBuildError> {
    trusted_preset_descriptors(manifest).map_err(|error| GraphqlBuildError(error.to_string()))
}

pub(crate) fn operation_fingerprint(document: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(document.as_bytes()))
}

/// Until the query executor owns an operation-wide database transaction, two
/// independent read roots cannot truthfully share one causal snapshot. Fail
/// closed instead of merging separately observed rows and duplicate index
/// vectors into an envelope that generated clients would treat as atomic.
pub(crate) fn has_multiple_protocol_query_roots(
    inner: &EngineInner,
    role: &str,
    request: &mut Request,
) -> bool {
    if inner.protocol.is_none() {
        return false;
    }
    let Some(surface) = inner.role_surfaces.get(role) else {
        return false;
    };
    let query_roots = surface
        .query_root_names()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if query_roots.is_empty() {
        return false;
    }

    let operation_name = request.operation_name.clone();
    let Ok(document) = request.parsed_query() else {
        return false;
    };
    let mut operations = document.operations.iter();
    let operation = if let Some(requested) = operation_name.as_deref() {
        operations
            .find(|(name, _)| name.map(|name| name.as_str()) == Some(requested))
            .map(|(_, operation)| operation)
    } else {
        let first = operations.next().map(|(_, operation)| operation);
        if operations.next().is_some() {
            None
        } else {
            first
        }
    };
    let Some(operation) = operation else {
        return false;
    };
    if operation.node.ty != async_graphql::parser::types::OperationType::Query {
        return false;
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "bounded recursive request analysis threads explicit cycle, budget, and result state"
    )]
    fn collect_root_keys<'a>(
        selection: &'a async_graphql::parser::types::SelectionSet,
        document: &'a async_graphql::parser::types::ExecutableDocument,
        query_roots: &BTreeSet<&str>,
        visiting: &mut BTreeSet<String>,
        completed: &mut BTreeSet<String>,
        remaining_selections: &mut usize,
        response_keys: &mut BTreeSet<String>,
        depth: usize,
    ) -> Result<(), ()> {
        if response_keys.len() > 1 {
            return Ok(());
        }
        if depth > REQUEST_ANALYSIS_MAX_DEPTH || selection.items.len() > *remaining_selections {
            return Err(());
        }
        *remaining_selections -= selection.items.len();
        for item in &selection.items {
            match &item.node {
                async_graphql::parser::types::Selection::Field(field) => {
                    if query_roots.contains(field.node.name.node.as_str()) {
                        response_keys.insert(field.node.response_key().node.to_string());
                    }
                }
                async_graphql::parser::types::Selection::InlineFragment(fragment) => {
                    collect_root_keys(
                        &fragment.node.selection_set.node,
                        document,
                        query_roots,
                        visiting,
                        completed,
                        remaining_selections,
                        response_keys,
                        depth + 1,
                    )?;
                }
                async_graphql::parser::types::Selection::FragmentSpread(spread) => {
                    let name = spread.node.fragment_name.node.to_string();
                    if completed.contains(&name) {
                        continue;
                    }
                    if !visiting.insert(name.clone()) {
                        return Err(());
                    }
                    let Some(fragment) = document.fragments.get(&spread.node.fragment_name.node)
                    else {
                        return Err(());
                    };
                    let result = collect_root_keys(
                        &fragment.node.selection_set.node,
                        document,
                        query_roots,
                        visiting,
                        completed,
                        remaining_selections,
                        response_keys,
                        depth + 1,
                    );
                    visiting.remove(&name);
                    result?;
                    completed.insert(name);
                }
            }
            if response_keys.len() > 1 {
                return Ok(());
            }
        }
        Ok(())
    }

    let mut response_keys = BTreeSet::new();
    let mut remaining_selections = REQUEST_ANALYSIS_MAX_SELECTIONS;
    let analysis = collect_root_keys(
        &operation.node.selection_set.node,
        document,
        &query_roots,
        &mut BTreeSet::new(),
        &mut BTreeSet::new(),
        &mut remaining_selections,
        &mut response_keys,
        0,
    );
    analysis.is_err() || response_keys.len() > 1
}

pub(crate) fn protocol_multi_root_error_response() -> Response {
    Response::from_errors(vec![ServerError::new(
        "causal GraphQL operations currently support one read root so data and revision evidence share one atomic snapshot; split this operation into separate requests",
        None,
    )])
}
