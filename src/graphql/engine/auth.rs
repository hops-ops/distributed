use super::*;

/// Leaderboard grants of `roles`, as fingerprint material, plus their member
/// claims. Empty when no leaderboard is granted, so existing fingerprints are
/// unchanged.
fn leaderboard_material(
    leaderboards: &[crate::graphql::surface::LeaderboardSurface],
    roles: &[&str],
    claim_keys: &mut BTreeSet<String>,
) -> Vec<serde_json::Value> {
    let mut material = Vec::new();
    for leaderboard in leaderboards {
        for role in roles {
            if let Some(selected) = leaderboard.for_role(role) {
                claim_keys.insert(selected.get_member_claim().to_string());
                material.push(serde_json::json!({
                    "role": role,
                    "leaderboard": selected.canonical_value(),
                }));
            }
        }
    }
    material
}

pub(crate) fn role_authorization_info(
    role: &str,
    permissions: &BTreeMap<(String, String), RoleModelPerm>,
    leaderboards: &[crate::graphql::surface::LeaderboardSurface],
) -> Result<(String, Vec<String>), GraphqlBuildError> {
    #[derive(Serialize)]
    struct PermissionMaterial<'a> {
        model: &'a str,
        all_columns: bool,
        columns: Vec<&'a str>,
        row_filter: Option<&'a FilterExpr>,
        limit: Option<u64>,
        aggregations: bool,
    }

    #[derive(Serialize)]
    struct RoleAuthorizationMaterial<'a> {
        domain: &'static str,
        version: u32,
        role: &'a str,
        permissions: Vec<PermissionMaterial<'a>>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        leaderboards: Vec<serde_json::Value>,
    }

    let mut claim_keys = BTreeSet::from([ROLE_KEY.to_string(), USER_ID_KEY.to_string()]);
    let mut role_permissions = Vec::new();
    for ((model, permission_role), entry) in permissions {
        if permission_role != role {
            continue;
        }
        if let Some(filter) = &entry.permission.row_filter {
            collect_filter_claim_keys(filter, &mut claim_keys);
        }
        role_permissions.push(PermissionMaterial {
            model,
            all_columns: entry.permission.all_columns,
            columns: entry
                .permission
                .columns
                .as_ref()
                .map(|columns| columns.iter().map(String::as_str).collect())
                .unwrap_or_default(),
            row_filter: entry.permission.row_filter.as_ref(),
            limit: entry.permission.limit,
            aggregations: entry.permission.aggregations,
        });
    }
    let leaderboards = leaderboard_material(leaderboards, &[role], &mut claim_keys);
    let canonical = serde_json::to_vec(&RoleAuthorizationMaterial {
        domain: "distributed.graphql.authorization-surface",
        version: 1,
        role,
        permissions: role_permissions,
        leaderboards,
    })
    .map_err(|_| {
        GraphqlBuildError(format!(
            "failed to encode GraphQL authorization surface for role `{role}`"
        ))
    })?;
    Ok((
        format!("sha256:{:x}", Sha256::digest(canonical)),
        claim_keys.into_iter().collect(),
    ))
}

fn collect_filter_claim_keys(filter: &FilterExpr, keys: &mut BTreeSet<String>) {
    fn collect_operand(operand: &Operand, keys: &mut BTreeSet<String>) {
        if let Operand::Claim(claim) = operand {
            keys.insert(claim.header.clone());
        }
    }

    match filter {
        FilterExpr::And(items) | FilterExpr::Or(items) => {
            for item in items {
                collect_filter_claim_keys(item, keys);
            }
        }
        FilterExpr::Not(item)
        | FilterExpr::Rel {
            predicate: item, ..
        } => {
            collect_filter_claim_keys(item, keys);
        }
        FilterExpr::Cmp { rhs, .. } => collect_operand(rhs, keys),
        FilterExpr::In { values, .. } => {
            for value in values {
                collect_operand(value, keys);
            }
        }
        FilterExpr::IsNull { .. } => {}
    }
}

pub(crate) fn identity_mode_label(mode: IdentityMode) -> &'static str {
    match mode {
        IdentityMode::TrustedProxy => "trusted_proxy",
        IdentityMode::OidcBearer => "oidc_bearer",
        IdentityMode::Hybrid => "hybrid",
        IdentityMode::DevHeaders => "dev_headers",
    }
}

/// Authorization fingerprint for a multi-privilege application pack.
pub(crate) fn role_authorization_info_for_roles(
    privilege_key: &str,
    schema_roles: &[String],
    permissions: &BTreeMap<(String, String), RoleModelPerm>,
    leaderboards: &[crate::graphql::surface::LeaderboardSurface],
) -> Result<(String, Vec<String>), GraphqlBuildError> {
    #[derive(Serialize)]
    struct MultiPrivilegeMaterial<'a> {
        domain: &'static str,
        version: u32,
        privilege_key: &'a str,
        schema_roles: &'a [String],
        models: Vec<&'a str>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        leaderboards: Vec<serde_json::Value>,
    }
    let mut claim_keys = BTreeSet::from([USER_ID_KEY.to_string()]);
    let mut models = BTreeSet::new();
    for ((model, permission_role), entry) in permissions {
        if !schema_roles.iter().any(|role| role == permission_role) {
            continue;
        }
        models.insert(model.as_str());
        if let Some(filter) = &entry.permission.row_filter {
            collect_filter_claim_keys(filter, &mut claim_keys);
        }
    }
    let models: Vec<&str> = models.into_iter().collect();
    let roles = schema_roles.iter().map(String::as_str).collect::<Vec<_>>();
    let leaderboards = leaderboard_material(leaderboards, &roles, &mut claim_keys);
    let canonical = serde_json::to_vec(&MultiPrivilegeMaterial {
        domain: "distributed.graphql.authorization-surface.multi",
        version: 1,
        privilege_key,
        schema_roles,
        models,
        leaderboards,
    })
    .map_err(|_| {
        GraphqlBuildError(format!(
            "failed to encode GraphQL authorization surface for privilege `{privilege_key}`"
        ))
    })?;
    Ok((
        format!("sha256:{:x}", Sha256::digest(canonical)),
        claim_keys.into_iter().collect(),
    ))
}

/// Insert intersected grants under a synthetic privilege key for multi-privilege apps.
pub(crate) fn insert_synthetic_privilege_permissions(
    privilege_key: &str,
    schema_roles: &[String],
    permissions: &mut BTreeMap<(String, String), RoleModelPerm>,
) {
    let models: BTreeSet<String> = permissions
        .keys()
        .filter(|(_, role)| schema_roles.iter().any(|r| r == role))
        .map(|(model, _)| model.clone())
        .collect();
    for model in models {
        let grants: Option<Vec<&ReadPermission>> = schema_roles
            .iter()
            .map(|role| {
                permissions
                    .get(&(model.clone(), role.clone()))
                    .map(|entry| &entry.permission)
            })
            .collect();
        let Some(grants) = grants else {
            continue;
        };
        let first = grants[0];
        let all_columns = grants.iter().all(|g| g.all_columns);
        let columns = if all_columns {
            None
        } else {
            let mut cols = first.columns.clone().unwrap_or_default();
            for g in grants.iter().skip(1) {
                if let Some(other) = &g.columns {
                    cols.retain(|c| other.contains(c));
                } else if !g.all_columns {
                    cols.clear();
                }
            }
            Some(cols)
        };
        let row_filter = if grants.iter().all(|g| g.row_filter == first.row_filter) {
            first.row_filter.clone()
        } else {
            // Differing row policies: no client-portable shared filter; server
            // still needs a denylist. Fail closed to first role's filter is
            // wrong — omit filter (unrestricted) only when all unrestricted;
            // otherwise keep first for SQL and rely on surface IR for client.
            first.row_filter.clone()
        };
        let limit = grants.iter().filter_map(|g| g.limit).min();
        let aggregations = grants.iter().all(|g| g.aggregations);
        permissions.insert(
            (model, privilege_key.to_string()),
            RoleModelPerm {
                permission: ReadPermission {
                    columns,
                    all_columns,
                    row_filter,
                    limit,
                    aggregations,
                },
            },
        );
    }
}
