//! Auth users: the Zitadel profile fact + its projection into `auth_users`.

use distributed::mutation_file;
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::Mutation;
use e2e_readmodels::AuthUsers;
use serde::{Deserialize, Serialize};

/// One Zitadel user's profile as the identity ingestor last saw it.
///
/// An external fact: the ingestor publishes it with
/// `publish_external`, identified by the provider delivery it came from, and
/// [`AUTH_USERS`] writes it to `auth_users` as it is (the row is the profile).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, distributed::DomainEvent)]
#[domain_event(name = "zitadel.user_profile", version = 1)]
pub struct ZitadelUserProfile {
    /// Zitadel subject (OIDC `sub`, session `x-user-id`).
    pub user_id: String,
    pub email: String,
    pub display_name: String,
    /// `human` | `machine`
    pub user_kind: String,
    /// `pending` | `approved` | `rejected`
    pub approval_status: String,
    /// `active` | `deactivated`
    pub status: String,
    /// When the ingestor saw it (ms since the epoch); not part of the profile.
    pub updated_at: String,
}

impl ZitadelUserProfile {
    /// Whether this profile differs from the `stored` row (or there is none).
    /// `updated_at` is when a profile was seen, not part of it.
    pub fn differs_from(&self, stored: Option<&AuthUsers>) -> bool {
        stored.is_none_or(|stored| {
            (
                &stored.user_id,
                &stored.email,
                &stored.display_name,
                &stored.user_kind,
                &stored.approval_status,
                &stored.status,
            ) != (
                &self.user_id,
                &self.email,
                &self.display_name,
                &self.user_kind,
                &self.approval_status,
                &self.status,
            )
        })
    }
}

/// Mutation: complete-row upsert of an auth user.
///
/// The constructor name matches the document operation: `mutation SaveAuthUser`.
#[allow(non_snake_case)]
pub fn SaveAuthUser() -> Mutation<()> {
    mutation_file!("src/mutations/save_auth_user.mutation.graphql")
}

distributed::projection! {
    pub const AUTH_USERS: ProjectionDescriptor<EventualOnly> = {
        name: "project_auth_users",
        version: 1,
        epoch: "e2e-ui-auth-users-v1",
        model: AuthUsers,
        on {
            events: [ZitadelUserProfile],
            mutation: SaveAuthUser,
            input: { user: body },
        },
    };
}

#[cfg(test)]
mod tests {
    use distributed::{DomainEventOccurrence, ExternalEventSource, RowValue, TableMutation};

    use super::*;

    fn ada() -> ZitadelUserProfile {
        ZitadelUserProfile {
            user_id: "user-99".into(),
            email: "ada@example.com".into(),
            display_name: "Ada".into(),
            user_kind: "human".into(),
            approval_status: "pending".into(),
            status: "deactivated".into(),
            updated_at: "1".into(),
        }
    }

    #[test]
    fn a_profile_upserts_its_row() {
        let occurrence = DomainEventOccurrence::capture_external(
            ExternalEventSource {
                producer: "zitadel.ingress.v1".into(),
                stream: "delivery-1".into(),
                position: 1,
                key: "user-99".into(),
            },
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            &ada(),
        )
        .unwrap();
        let lowered = AUTH_USERS
            .server_executor()
            .unwrap()
            .plan(&occurrence)
            .unwrap();
        let TableMutation::UpsertRow(row) = &lowered.write_plan.mutations[0] else {
            panic!("expected upsert");
        };
        for (column, value) in [
            ("user_id", "user-99"),
            ("display_name", "Ada"),
            ("status", "deactivated"),
            ("approval_status", "pending"),
        ] {
            assert_eq!(
                row.values.get(column),
                Some(&RowValue::String(value.into())),
                "{column}"
            );
        }
    }

    #[test]
    fn only_a_changed_profile_differs() {
        let profile = ada();
        let row = AuthUsers {
            user_id: profile.user_id.clone(),
            email: profile.email.clone(),
            display_name: profile.display_name.clone(),
            user_kind: profile.user_kind.clone(),
            approval_status: profile.approval_status.clone(),
            status: profile.status.clone(),
            updated_at: "0".into(),
        };
        assert!(profile.differs_from(None));
        assert!(!profile.differs_from(Some(&row)), "only updated_at differs");
        let renamed = AuthUsers {
            display_name: "Ada L.".into(),
            ..row.clone()
        };
        assert!(profile.differs_from(Some(&renamed)));
        let active = AuthUsers {
            status: "active".into(),
            ..row
        };
        assert!(profile.differs_from(Some(&active)));
    }
}
