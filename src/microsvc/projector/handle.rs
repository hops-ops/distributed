use std::fmt;
use std::str::FromStr;

const PROJECTION_REPAIR_HANDLE_VERSION: u8 = 1;
const PROJECTION_REPAIR_HANDLE_PREFIX: &str = "distributed-repair-v1:";
const DETERMINISTIC_FAILURE_ID_BYTES: usize = 64;

/// Transferable, non-sensitive operator handle for one durable projection
/// failure.
///
/// The handle deliberately contains only a format version and the globally
/// unique deterministic failure ID. It never serializes a canonical projection
/// partition, which may contain tenant identifiers. During repair, the owning
/// store resolves and validates the exact durable topology/partition scope and
/// the [`Service`](crate::microsvc::Service) verifies that topology is registered.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ProjectionRepairHandle {
    version: u8,
    failure_id: String,
}

impl ProjectionRepairHandle {
    pub(super) fn for_failure(failure_id: String) -> Self {
        debug_assert!(valid_deterministic_failure_id(&failure_id));
        Self {
            version: PROJECTION_REPAIR_HANDLE_VERSION,
            failure_id,
        }
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn failure_id(&self) -> &str {
        &self.failure_id
    }
}

/// A projection that a durable failure has stopped, as
/// [`Service::stopped_projections`](crate::microsvc::Service::stopped_projections)
/// reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoppedProjection {
    /// The stopped projector's topology name.
    pub projection: String,
    /// What [`Service::repair_projection`](crate::microsvc::Service::repair_projection)
    /// takes to let it resume from its failed input.
    pub repair: ProjectionRepairHandle,
}

impl fmt::Debug for ProjectionRepairHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionRepairHandle")
            .field("version", &self.version)
            .field("failure_id", &self.failure_id)
            .finish()
    }
}

impl fmt::Display for ProjectionRepairHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{PROJECTION_REPAIR_HANDLE_PREFIX}{}",
            self.failure_id
        )
    }
}

/// Failure parsing an operator-supplied [`ProjectionRepairHandle`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionRepairHandleParseError;

impl fmt::Display for ProjectionRepairHandleParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid projection repair handle")
    }
}

impl std::error::Error for ProjectionRepairHandleParseError {}

impl FromStr for ProjectionRepairHandle {
    type Err = ProjectionRepairHandleParseError;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let failure_id = token
            .strip_prefix(PROJECTION_REPAIR_HANDLE_PREFIX)
            .filter(|failure_id| valid_deterministic_failure_id(failure_id))
            .ok_or(ProjectionRepairHandleParseError)?;
        Ok(Self {
            version: PROJECTION_REPAIR_HANDLE_VERSION,
            failure_id: failure_id.to_string(),
        })
    }
}

impl serde::Serialize for ProjectionRepairHandle {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for ProjectionRepairHandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let token = String::deserialize(deserializer)?;
        token.parse().map_err(serde::de::Error::custom)
    }
}

fn valid_deterministic_failure_id(failure_id: &str) -> bool {
    failure_id.len() == DETERMINISTIC_FAILURE_ID_BYTES
        && failure_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
