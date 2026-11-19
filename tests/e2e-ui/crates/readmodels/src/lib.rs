//! Query models and read authorization for the e2e-ui fixture.
//!
//! Projection programs live in `e2e-projections`; this crate owns only the
//! read side's shapes, relationships, and role grants.

pub mod models;

pub use models::{AuthUsers, BlobGames, ChatMessages, Todos};

pub fn distributed_manifest() -> distributed::ReadModelCatalog {
    use distributed::RelationalReadModel;

    // Schema-only catalog (physical tables). ApplicationManifest is the logical
    // application contract and is composed separately.
    distributed::ReadModelCatalog::new("e2e-ui")
        .table_schema(Todos::schema().clone())
        .table_schema(ChatMessages::schema().clone())
        .table_schema(BlobGames::schema().clone())
        .table_schema(AuthUsers::schema().clone())
}

/// Role grants compiled from the authorization attached to each read model.
pub fn application_grants() -> std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, distributed::graphql::ReadPermission>,
> {
    use distributed::graphql::ModelPermissions;
    use distributed::RelationalReadModel;

    fn add<M: RelationalReadModel>(
        grants: &mut std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, distributed::graphql::ReadPermission>,
        >,
        permissions: ModelPermissions<M>,
    ) {
        for (role, grant) in permissions.by_role() {
            grants
                .entry(role)
                .or_default()
                .insert(M::schema().model_name.clone(), grant);
        }
    }

    let mut grants = std::collections::BTreeMap::new();
    add(&mut grants, Todos::permissions());
    add(&mut grants, ChatMessages::permissions());
    add(&mut grants, BlobGames::permissions());
    add(&mut grants, AuthUsers::permissions());
    grants
}

#[cfg(test)]
mod tests {
    use super::*;
    use distributed::RelationalReadModel;

    #[test]
    fn referencing_models_own_one_way_auth_user_relationships() {
        let todo = Todos::schema();
        let blob = BlobGames::schema();
        let chat = ChatMessages::schema();
        assert!(blob
            .relationships
            .iter()
            .any(|relationship| relationship.field_name == "owner"
                && relationship.target_model == "AuthUsers"));
        assert!(chat
            .relationships
            .iter()
            .any(|relationship| relationship.field_name == "author"
                && relationship.target_model == "AuthUsers"));

        assert!(todo
            .relationships
            .iter()
            .any(|relationship| relationship.field_name == "owner"
                && relationship.target_model == "AuthUsers"));

        assert!(AuthUsers::schema().relationships.is_empty());

        let project = distributed_manifest();
        let surface = distributed::graphql::build_surface(
            &project.tables,
            &distributed::graphql::SurfaceOptions::sqlite(),
        )
        .unwrap();
        let sdl = distributed::graphql::graphql_sdl_from_surface(&surface).unwrap();
        let object = |name: &str| {
            let start = sdl.find(&format!("type {name} {{")).unwrap();
            let tail = &sdl[start..];
            &tail[..tail.find("\n}\n").unwrap()]
        };
        // belongs_to is always GraphQL-nullable (Option join), even when the FK
        // column is non-null — required for optimistic parent rows before the
        // join edge is linked. (Last field may be truncated before the closing
        // brace, so match without requiring a trailing newline.)
        for name in ["Todos", "BlobGames", "ChatMessages"] {
            let body = object(name);
            let join = if name == "ChatMessages" {
                "author"
            } else {
                "owner"
            };
            assert!(
                body.contains(&format!("\n  {join}: AuthUsers")),
                "{name} missing nullable AuthUsers join:\n{body}"
            );
            assert!(
                !body.contains(&format!("\n  {join}: AuthUsers!")),
                "{name} must not mark belongs_to non-null:\n{body}"
            );
        }
    }

    #[test]
    fn model_owned_permissions_compile_the_application_grants() {
        let grants = application_grants();
        let user = &grants["user"];
        let admin = &grants["admin"];
        let anonymous = &grants["anonymous"];

        assert_eq!(user.len(), 4);
        assert_eq!(admin.len(), 4);
        // Public surface: lobby chat + directory joins for author names.
        assert!(anonymous.contains_key("ChatMessages"));
        assert!(anonymous.contains_key("AuthUsers"));
        assert_eq!(anonymous.len(), 2);
        assert!(matches!(
            user["Todos"].row_policy,
            distributed::graphql::SurfaceRowPolicy::Predicate(_)
        ));
        assert!(matches!(
            user["BlobGames"].row_policy,
            distributed::graphql::SurfaceRowPolicy::Predicate(_)
        ));
        assert!(matches!(
            user["ChatMessages"].row_policy,
            distributed::graphql::SurfaceRowPolicy::Unrestricted
        ));
        assert!(matches!(
            user["AuthUsers"].row_policy,
            distributed::graphql::SurfaceRowPolicy::Unrestricted
        ));
    }
}
