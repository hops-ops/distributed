use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientSurfaceIdentity {
    Role {
        name: String,
    },
    /// `eligible_roles` is the canonical wire identity for principals who may
    /// open the application surface. `schema_roles` is the distinct role set
    /// used to derive the shared schema/command contract.
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

impl ClientSurfaceIdentity {
    pub fn role(name: impl Into<String>) -> Self {
        Self::Role { name: name.into() }
    }

    pub fn application(
        name: impl Into<String>,
        eligible_roles: impl IntoIterator<Item = impl Into<String>>,
        schema_roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::application_with_schema_roles(name, eligible_roles, schema_roles)
    }

    pub fn application_with_schema_roles(
        name: impl Into<String>,
        eligible_roles: impl IntoIterator<Item = impl Into<String>>,
        schema_roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut eligible_roles: Vec<String> = eligible_roles.into_iter().map(Into::into).collect();
        let mut schema_roles: Vec<String> = schema_roles.into_iter().map(Into::into).collect();
        eligible_roles.sort();
        eligible_roles.dedup();
        schema_roles.sort();
        schema_roles.dedup();
        Self::Application {
            name: name.into(),
            eligible_roles,
            schema_roles,
        }
    }

    pub(super) fn canonicalized(self) -> Result<Self, ClientManifestError> {
        match self {
            Self::Role { name } if name.trim().is_empty() => Err(ClientManifestError(
                "role surface name must not be empty".into(),
            )),
            Self::Role { name } => Ok(Self::Role { name }),
            Self::Application { name, .. } if name.trim().is_empty() => Err(ClientManifestError(
                "application surface name must not be empty".into(),
            )),
            Self::Application {
                name,
                mut eligible_roles,
                mut schema_roles,
            } => {
                if eligible_roles.iter().any(|role| role.trim().is_empty()) {
                    return Err(ClientManifestError(format!(
                        "application surface `{name}` contains an empty role id"
                    )));
                }
                if schema_roles.iter().any(|role| role.trim().is_empty()) {
                    return Err(ClientManifestError(format!(
                        "application surface `{name}` contains an empty schema role id"
                    )));
                }
                eligible_roles.sort();
                eligible_roles.dedup();
                schema_roles.sort();
                schema_roles.dedup();
                if eligible_roles.is_empty() {
                    return Err(ClientManifestError(format!(
                        "application surface `{name}` must declare at least one eligible role"
                    )));
                }
                if schema_roles.is_empty() {
                    return Err(ClientManifestError(format!(
                        "application surface `{name}` must declare at least one schema role"
                    )));
                }
                if schema_roles
                    .iter()
                    .any(|role| !eligible_roles.iter().any(|eligible| eligible == role))
                {
                    return Err(ClientManifestError(format!(
                        "application surface `{name}` schema roles must be a subset of eligible roles"
                    )));
                }
                Ok(Self::Application {
                    name,
                    eligible_roles,
                    schema_roles,
                })
            }
        }
    }
}
