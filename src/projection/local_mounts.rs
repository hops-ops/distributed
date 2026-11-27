//! Framework-owned local projection mount compilation.
//!
//! Applications declare program descriptors and read models. Physical
//! topology, partition codecs, catalog activation, and Surface packaging are
//! derived here.

#![allow(missing_docs)]

use crate::graphql::{SurfaceDirectProjection, SurfaceModeledProjection, SurfaceProjector};
use crate::projection::catalog::{
    ActiveProjectionBindings, ProjectionBindingActivation, ProjectionCatalog,
};
use crate::projection::lower::ProjectionDescriptor;
use crate::projection::placement::{
    ProjectionBinding, ProjectionBindingState, ProjectionExecutorRoute, ProjectionOutput,
    ProjectionOwner, ProjectionPhysicalTopology, ProjectionSourceBinding,
    PROJECTION_PARTITION_CODEC_VERSION,
};
use crate::projection_protocol::ProjectorTopologyId;
use sha2::{Digest, Sha256};

const LOCAL_TOPOLOGY_DIGEST_DOMAIN: &[u8] = b"distributed.local-projection-topology\0";

/// One eventual surface projector ready for GraphQL/service mounts.
#[derive(Clone)]
pub struct LocalEventualMount {
    pub owner: String,
    pub projector: SurfaceProjector,
}

/// One direct surface projection ready for GraphQL/service mounts.
#[derive(Clone)]
pub struct LocalDirectMount {
    pub owner: String,
    pub projection: SurfaceDirectProjection,
}

/// Compiled set of local projection mounts for one application.
#[derive(Clone, Default)]
pub struct LocalProjectionMounts {
    pub eventual: Vec<LocalEventualMount>,
    pub direct: Vec<LocalDirectMount>,
}

impl LocalProjectionMounts {
    /// Start mounting projection descriptors for `service_id`, consuming the
    /// stable domain event stream `domain_source` (e.g. `ordered-domain-events`).
    pub fn builder(
        service_id: impl Into<String>,
        domain_source: impl Into<String>,
    ) -> LocalProjectionMountsBuilder {
        LocalProjectionMountsBuilder::new(service_id, domain_source)
    }

    /// The eventual projector compiled from `descriptor`.
    ///
    /// # Panics
    ///
    /// When `descriptor` was not mounted with
    /// [`eventual`](LocalProjectionMountsBuilder::eventual): a composition
    /// mistake, reported at startup with the projection's name.
    pub fn projector<D>(&self, descriptor: ProjectionDescriptor<D>) -> SurfaceProjector {
        self.eventual
            .iter()
            .find(|mount| mount.owner == descriptor.name())
            .map(|mount| mount.projector.clone())
            .unwrap_or_else(|| {
                panic!(
                    "projection `{}` is not mounted eventually; add `.eventual(..)` for it to the LocalProjectionMountsBuilder",
                    descriptor.name()
                )
            })
    }

    /// Every mounted projection as a Surface owner, eventual then direct.
    pub fn all_owners(&self) -> Vec<crate::graphql::SurfaceProjectionOwner> {
        let mut owners = Vec::new();
        for mount in &self.eventual {
            owners.push(mount.projector.clone().into());
        }
        for mount in &self.direct {
            owners.push(mount.projection.clone().into());
        }
        owners
    }
}

type ModeledFactory = Box<
    dyn Fn(
            &ProjectionCatalog,
            &ActiveProjectionBindings,
            &ProjectionBinding,
        ) -> Result<SurfaceModeledProjection, String>
        + Send
        + Sync,
>;

struct Pending {
    owner: String,
    binding: ProjectionBinding,
    modeled_factory: ModeledFactory,
}

/// Builder that materializes a shared catalog and Surface mounts for local
/// hosting.
///
/// Each projection is mounted from its `projection!` descriptor alone: the
/// owner is the descriptor's `name` and the outputs its `model`, so no
/// identity is restated. Errors are kept until [`build`](Self::build).
pub struct LocalProjectionMountsBuilder {
    service_id: String,
    source: Result<ProjectionSourceBinding, String>,
    eventual: Vec<Pending>,
    direct: Vec<Pending>,
    errors: Vec<String>,
}

impl LocalProjectionMountsBuilder {
    /// `domain_source` is a stable domain event stream name (e.g. `ordered-domain-events`).
    pub fn new(service_id: impl Into<String>, domain_source: impl Into<String>) -> Self {
        let service_id = service_id.into();
        let source =
            ProjectionSourceBinding::try_new(format!("{service_id}-domain"), domain_source, 1)
                .map_err(|error| error.to_string());
        Self {
            service_id,
            source,
            eventual: Vec::new(),
            direct: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Mount `descriptor` as an eventual projection (ordered domain-event
    /// delivery).
    pub fn eventual<D>(mut self, descriptor: ProjectionDescriptor<D>) -> Self
    where
        D: Copy + 'static,
    {
        let pending = self.pending(descriptor, |source, owner, outputs, topology| {
            ProjectionBinding::materialize_eventual(
                descriptor.eventual(),
                source,
                owner,
                "distributed-projection-partition",
                PROJECTION_PARTITION_CODEC_VERSION,
                outputs,
                Vec::new(),
                Some(topology),
            )
            .map_err(|error| error.to_string())
        });
        match pending {
            Ok(pending) => self.eventual.push(pending),
            Err(error) => self.errors.push(error),
        }
        self
    }

    /// Mount `descriptor` as a direct projection, sealed in the command's own
    /// transaction.
    pub fn direct(
        mut self,
        descriptor: ProjectionDescriptor<crate::projection::lower::DirectCandidate>,
    ) -> Self {
        let pending = self.pending(descriptor, |source, owner, outputs, topology| {
            ProjectionBinding::materialize_direct(
                descriptor.direct(),
                source,
                owner,
                "distributed-projection-partition",
                PROJECTION_PARTITION_CODEC_VERSION,
                outputs,
                Vec::new(),
                Some(topology),
            )
            .map_err(|error| error.to_string())
        });
        match pending {
            Ok(pending) => self.direct.push(pending),
            Err(error) => self.errors.push(error),
        }
        self
    }

    fn pending<D: Copy + 'static>(
        &self,
        descriptor: ProjectionDescriptor<D>,
        materialize: impl FnOnce(
            ProjectionSourceBinding,
            ProjectionOwner,
            Vec<ProjectionOutput>,
            ProjectionPhysicalTopology,
        ) -> Result<ProjectionBinding, String>,
    ) -> Result<Pending, String> {
        let owner = descriptor.name().to_string();
        let source = self.source.clone()?;
        let outputs = descriptor
            .output_inventory()
            .map_err(|error| format!("projection `{owner}` outputs: {error}"))?
            .models
            .into_iter()
            .map(|output| {
                ProjectionOutput::try_new(output.model, output.storage, output.schema)
                    .map_err(|error| format!("projection `{owner}` output: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let binding = materialize(
            source,
            ProjectionOwner::try_new(owner.clone()).map_err(|e| e.to_string())?,
            outputs,
            physical_topology(&owner, stable_topology_digest(&owner)),
        )
        .map_err(|error| format!("projection `{owner}`: {error}"))?;
        Ok(Pending {
            owner,
            binding,
            modeled_factory: Box::new(move |catalog, active, binding| {
                SurfaceModeledProjection::try_from_descriptor(
                    descriptor,
                    catalog,
                    active,
                    binding.id(),
                )
            }),
        })
    }

    /// Compile every mounted projection into one active catalog, then
    /// package the Surface mounts.
    ///
    /// # Errors
    ///
    /// The first mount or catalog error, naming its projection.
    pub fn build(self) -> Result<LocalProjectionMounts, String> {
        if let Some(error) = self.errors.into_iter().next() {
            return Err(error);
        }
        let pending = || self.eventual.iter().chain(&self.direct);
        let catalog =
            ProjectionCatalog::try_new(pending().map(|entry| entry.binding.clone()).collect())
                .map_err(|e| e.to_string())?;
        let activations = pending()
            .map(|entry| activation(&entry.binding, &self.service_id))
            .collect::<Result<Vec<_>, _>>()?;
        let active = catalog.activate(activations).map_err(|e| e.to_string())?;

        let mut mounts = LocalProjectionMounts::default();
        for entry in &self.eventual {
            let modeled = (entry.modeled_factory)(&catalog, &active, &entry.binding)?;
            mounts.eventual.push(LocalEventualMount {
                owner: entry.owner.clone(),
                projector: SurfaceProjector::new(entry.owner.clone()).modeled(modeled),
            });
        }
        for entry in &self.direct {
            let modeled = (entry.modeled_factory)(&catalog, &active, &entry.binding)?;
            mounts.direct.push(LocalDirectMount {
                owner: entry.owner.clone(),
                projection: SurfaceDirectProjection::new(entry.owner.clone()).modeled(modeled),
            });
        }
        Ok(mounts)
    }
}

fn activation(
    binding: &ProjectionBinding,
    service_id: &str,
) -> Result<ProjectionBindingActivation, String> {
    Ok(ProjectionBindingActivation::new(
        binding.id(),
        binding.program_id(),
        ProjectionBindingState::Active,
        Some(ProjectionExecutorRoute::local(service_id).map_err(|e| e.to_string())?),
    ))
}

fn physical_topology(name: &str, digest: [u8; 32]) -> ProjectionPhysicalTopology {
    ProjectionPhysicalTopology::from_protocol(
        &ProjectorTopologyId::new(1, name, digest).expect("canonical local projection topology"),
    )
}

fn stable_topology_digest(owner: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(LOCAL_TOPOLOGY_DIGEST_DOMAIN);
    digest.update((owner.len() as u64).to_be_bytes());
    digest.update(owner.as_bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_topology_identity_is_stable_and_does_not_alias_projectors() {
        let workspace = "project_meta_workspaces";
        let changes = "project_meta_change_sets";

        let first = physical_topology(workspace, stable_topology_digest(workspace));
        let after_restart = physical_topology(workspace, stable_topology_digest(workspace));
        let subscriber = physical_topology(changes, stable_topology_digest(changes));

        assert_eq!(first, after_restart);
        assert_ne!(first.digest(), subscriber.digest());
    }
}
