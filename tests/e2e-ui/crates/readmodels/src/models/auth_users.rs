//! Auth users imported from Zitadel Action ingress.

use distributed::graphql::{read, AuthorizedReadModel, ModelPermissions};
use distributed::read_model::ReadModel;
use serde::{Deserialize, Serialize};

/// Imported IdP user. PK: `user_id` (= Zitadel subject / OIDC `sub` / session `x-user-id`).
///
/// Populated only from the Zitadel ingestor projector — never from commands.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("auth_users")]
pub struct AuthUsers {
    #[id("user_id")]
    pub user_id: String,
    pub email: String,
    pub display_name: String,
    /// `human` | `machine`
    pub user_kind: String,
    /// `pending` | `approved` | `rejected`
    pub approval_status: String,
    /// `active` | `deactivated`
    pub status: String,
    pub updated_at: String,
}

impl AuthorizedReadModel for AuthUsers {
    /// Read authorization attached to the imported identity query model.
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new()
            .grant("user", read().all_columns())
            .grant("admin", read().all_columns())
            // Public lobby joins author display names without a session.
            .grant("anonymous", read().all_columns())
    }
}
