//! Explained capability closure for a validated deployment plan.
//!
//! Capabilities are logical requirements with originating reasons. Provider
//! selection and environment binding are intentionally deferred to runtime
//! and environment policy (tasks 12 and 14).

use serde::{Deserialize, Serialize};

use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint};
use super::manifest::ApplicationManifest;
use super::mount::MountSelector;
use crate::command::CommandConsistency;

/// A named logical capability required by one or more mounts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    EventStore,
    LockManager,
    CommandLedger,
    TransactionalOutbox,
    Publisher,
    EventSubscription,
    InboxCheckpoint,
    ReadStore,
    ChangeFeed,
    IdentityMiddleware,
    HttpTransport,
    WebsocketTransport,
    Metrics,
    SchemaLifecycle,
    LocalCommandDispatch,
    RemoteCommandDispatch,
    DirectProjectionTransaction,
}

impl Capability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EventStore => "event_store",
            Self::LockManager => "lock_manager",
            Self::CommandLedger => "command_ledger",
            Self::TransactionalOutbox => "transactional_outbox",
            Self::Publisher => "publisher",
            Self::EventSubscription => "event_subscription",
            Self::InboxCheckpoint => "inbox_checkpoint",
            Self::ReadStore => "read_store",
            Self::ChangeFeed => "change_feed",
            Self::IdentityMiddleware => "identity_middleware",
            Self::HttpTransport => "http_transport",
            Self::WebsocketTransport => "websocket_transport",
            Self::Metrics => "metrics",
            Self::SchemaLifecycle => "schema_lifecycle",
            Self::LocalCommandDispatch => "local_command_dispatch",
            Self::RemoteCommandDispatch => "remote_command_dispatch",
            Self::DirectProjectionTransaction => "direct_projection_transaction",
        }
    }
}

/// Why a capability is required.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReason {
    pub capability: Capability,
    /// Originating process id when the requirement is process-local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    /// Originating mount kind when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_kind: Option<String>,
    /// Originating mount id when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_id: Option<String>,
    /// Human-readable but deterministic explanation.
    pub reason: String,
}

/// One required capability with all contributing reasons.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequirement {
    pub capability: Capability,
    pub reasons: Vec<CapabilityReason>,
}

/// Renderer-neutral schema/migration lifecycle requirement.
///
/// Describes *that* schema lifecycle is needed and which logical owner
/// produced the requirement. It does not choose Job, operator, or mode.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaLifecycleRequirement {
    pub required: bool,
    /// Single logical owner identity when schema lifecycle is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_owner: Option<String>,
    pub reasons: Vec<String>,
}

/// Derive explained capabilities for the mounts selected into one process.
pub fn derive_process_capabilities(
    manifest: &ApplicationManifest,
    process_id: &str,
    mounts: &[MountSelector],
    remote_commands: bool,
) -> ApplicationResult<(Vec<CapabilityRequirement>, SchemaLifecycleRequirement)> {
    let mut reasons = Vec::new();
    let mut schema_reasons = Vec::new();
    let mut schema_owner: Option<String> = None;

    let push = |reasons: &mut Vec<CapabilityReason>,
                capability: Capability,
                mount: Option<&MountSelector>,
                reason: String| {
        reasons.push(CapabilityReason {
            capability,
            process_id: Some(process_id.to_string()),
            mount_kind: mount.map(|mount| mount.kind_label().to_string()),
            mount_id: mount.map(|mount| mount.id().to_string()),
            reason,
        });
    };

    for mount in mounts {
        match mount {
            MountSelector::Command { id } => {
                let command = manifest
                    .commands
                    .iter()
                    .find(|command| command.id == *id)
                    .ok_or_else(|| ApplicationError::Missing {
                        kind: "command",
                        identity: id.clone(),
                    })?;
                if remote_commands {
                    push(
                        &mut reasons,
                        Capability::RemoteCommandDispatch,
                        Some(mount),
                        format!("command `{id}` is dispatched remotely"),
                    );
                } else {
                    push(
                        &mut reasons,
                        Capability::LocalCommandDispatch,
                        Some(mount),
                        format!("command `{id}` is executed by a local mount"),
                    );
                    push(
                        &mut reasons,
                        Capability::EventStore,
                        Some(mount),
                        format!("local command `{id}` requires an event store"),
                    );
                    push(
                        &mut reasons,
                        Capability::LockManager,
                        Some(mount),
                        format!("local command `{id}` requires aggregate locks"),
                    );
                    push(
                        &mut reasons,
                        Capability::CommandLedger,
                        Some(mount),
                        format!("local command `{id}` requires command ledger/dedup"),
                    );
                }
                if !command.emits.is_empty() {
                    push(
                        &mut reasons,
                        Capability::TransactionalOutbox,
                        Some(mount),
                        format!("command `{id}` emits facts and needs an outbox"),
                    );
                    push(
                        &mut reasons,
                        Capability::Publisher,
                        Some(mount),
                        format!("command `{id}` publishes outward facts"),
                    );
                }
                if matches!(command.consistency, CommandConsistency::Atomic) {
                    push(
                        &mut reasons,
                        Capability::DirectProjectionTransaction,
                        Some(mount),
                        format!("atomic command `{id}` requires collocated direct projection"),
                    );
                    push(
                        &mut reasons,
                        Capability::ReadStore,
                        Some(mount),
                        format!("atomic command `{id}` writes read models in-transaction"),
                    );
                }
                schema_reasons.push(format!("command `{id}` implies schema lifecycle"));
                if schema_owner.is_none() {
                    schema_owner = Some(manifest.name.clone());
                }
            }
            MountSelector::Projector { id } => {
                let projection = manifest
                    .projections
                    .iter()
                    .find(|projection| projection.id == *id)
                    .ok_or_else(|| ApplicationError::Missing {
                        kind: "projection",
                        identity: id.clone(),
                    })?;
                if projection.direct {
                    push(
                        &mut reasons,
                        Capability::DirectProjectionTransaction,
                        Some(mount),
                        format!("direct projector `{id}` seals in the writer transaction"),
                    );
                } else {
                    push(
                        &mut reasons,
                        Capability::EventSubscription,
                        Some(mount),
                        format!("eventual projector `{id}` subscribes to facts"),
                    );
                    push(
                        &mut reasons,
                        Capability::InboxCheckpoint,
                        Some(mount),
                        format!("eventual projector `{id}` checkpoints progress"),
                    );
                }
                push(
                    &mut reasons,
                    Capability::ReadStore,
                    Some(mount),
                    format!("projector `{id}` writes read models"),
                );
                push(
                    &mut reasons,
                    Capability::ChangeFeed,
                    Some(mount),
                    format!("projector `{id}` publishes change notifications"),
                );
                schema_reasons.push(format!("projector `{id}` implies schema lifecycle"));
                if schema_owner.is_none() {
                    schema_owner = Some(manifest.name.clone());
                }
            }
            MountSelector::Surface { id } => {
                push(
                    &mut reasons,
                    Capability::ReadStore,
                    Some(mount),
                    format!("surface `{id}` queries read models"),
                );
                push(
                    &mut reasons,
                    Capability::IdentityMiddleware,
                    Some(mount),
                    format!("surface `{id}` requires principal identity"),
                );
                push(
                    &mut reasons,
                    Capability::HttpTransport,
                    Some(mount),
                    format!("surface `{id}` is served over HTTP"),
                );
                // Live-capable surfaces always advertise change-feed need at plan
                // level; runtime may no-op when no @live fields are selected.
                push(
                    &mut reasons,
                    Capability::ChangeFeed,
                    Some(mount),
                    format!("surface `{id}` may expose live queries"),
                );
                push(
                    &mut reasons,
                    Capability::WebsocketTransport,
                    Some(mount),
                    format!("surface `{id}` may serve live subscriptions"),
                );
                if remote_commands {
                    push(
                        &mut reasons,
                        Capability::RemoteCommandDispatch,
                        Some(mount),
                        format!("surface `{id}` dispatches commands remotely"),
                    );
                } else if mounts
                    .iter()
                    .any(|m| matches!(m, MountSelector::Command { .. }))
                {
                    push(
                        &mut reasons,
                        Capability::LocalCommandDispatch,
                        Some(mount),
                        format!("surface `{id}` dispatches to local command mounts"),
                    );
                } else {
                    push(
                        &mut reasons,
                        Capability::RemoteCommandDispatch,
                        Some(mount),
                        format!(
                            "surface `{id}` has no local command mounts; remote dispatch required"
                        ),
                    );
                }
            }
            MountSelector::Extension { id } => {
                push(
                    &mut reasons,
                    Capability::Metrics,
                    Some(mount),
                    format!("extension `{id}` may require observability hooks"),
                );
            }
        }
    }

    // Always explain metrics when any process exists — readiness/observability.
    if !mounts.is_empty() {
        push(
            &mut reasons,
            Capability::Metrics,
            None,
            format!("process `{process_id}` exposes readiness and metrics"),
        );
    }

    reasons.sort();
    reasons.dedup();

    let mut by_capability = std::collections::BTreeMap::<Capability, Vec<CapabilityReason>>::new();
    for reason in reasons {
        by_capability
            .entry(reason.capability)
            .or_default()
            .push(reason);
    }
    let requirements = by_capability
        .into_iter()
        .map(|(capability, mut reasons)| {
            reasons.sort();
            CapabilityRequirement {
                capability,
                reasons,
            }
        })
        .collect::<Vec<_>>();

    schema_reasons.sort();
    schema_reasons.dedup();
    if schema_reasons.len() > 1 {
        // Single logical owner: the application name. Multiple process reasons
        // still share that owner.
        if schema_owner.as_deref() != Some(manifest.name.as_str()) && schema_owner.is_some() {
            return Err(ApplicationError::Collision {
                kind: "schema lifecycle owner",
                identity: schema_owner.clone().unwrap_or_default(),
                reason: format!("expected single logical owner `{}`", manifest.name),
            });
        }
    }
    let schema = SchemaLifecycleRequirement {
        required: !schema_reasons.is_empty(),
        logical_owner: if schema_reasons.is_empty() {
            None
        } else {
            Some(manifest.name.clone())
        },
        reasons: schema_reasons,
    };

    // Fingerprint stability helper (ensures reasons are canonical-serializable).
    let _ = sha256_fingerprint(&serde_json::to_vec(&canonical_json(
        &serde_json::to_value(&requirements)?,
    ))?);

    Ok((requirements, schema))
}
