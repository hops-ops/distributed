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
    permissions: &BTreeMap<String, BTreeMap<String, ReadPermission>>,
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
    for (model, permission) in permissions.get(role).into_iter().flatten() {
        if let Some(filter) = &permission.row_filter {
            filter.visit_claims(|claim| {
                claim_keys.insert(claim.to_string());
            });
        }
        role_permissions.push(PermissionMaterial {
            model,
            all_columns: permission.all_columns,
            columns: permission
                .columns
                .as_ref()
                .map(|columns| columns.iter().map(String::as_str).collect())
                .unwrap_or_default(),
            row_filter: permission.row_filter.as_ref(),
            limit: permission.limit,
            aggregations: permission.aggregations,
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

/// Authorization fingerprint for a multi-privilege application pack.
pub(crate) fn role_authorization_info_for_roles(
    privilege_key: &str,
    schema_roles: &[String],
    permissions: &BTreeMap<String, BTreeMap<String, ReadPermission>>,
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
    for (model, permission) in schema_roles
        .iter()
        .flat_map(|role| permissions.get(role).into_iter().flatten())
    {
        models.insert(model.as_str());
        if let Some(filter) = &permission.row_filter {
            filter.visit_claims(|claim| {
                claim_keys.insert(claim.to_string());
            });
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
