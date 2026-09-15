//! Placement-independent application composition.
//!
//! The application module is deliberately feature-light. It contains the
//! portable contract values used by contract-only packages as well as the
//! erased executable mounts used at a heterogeneous runtime boundary. SQL,
//! HTTP, GraphQL execution, brokers, and deployment clients are not required
//! to construct these values.

mod capability;
mod command;
mod error;
mod identity;
mod manifest;
mod module;
mod mount;
mod plan;
mod projection_contract;
mod registration;
mod runtime;
mod runtime_host;
mod topology;

pub use capability::{
    Capability, CapabilityReason, CapabilityRequirement, SchemaLifecycleRequirement,
};
pub use command::{
    admit_command_session, command_roles_require_principal, CommandDefinition, CommandMount,
    CommandMountFuture, CommandMountHandler, CommandMountRegistrar, CommandSpec, CommandTypeField,
    CommandTypeSpec, EventSpec, TypeSpec,
};
pub(crate) use command::{
    CommandMountExecution, CommandMountExecutionError, CommandMountExecutionFuture,
    CommandMountExecutionResult, CommandMountInvocation,
};
pub use error::{ApplicationError, ApplicationResult};
pub use identity::{canonical_json, sha256_fingerprint, LogicalId};
pub use manifest::{
    ApplicationExtension, ApplicationManifest, FrameworkCompatibility, ManifestFingerprint,
    ManifestProvenance, APPLICATION_MANIFEST_SCHEMA_VERSION, FRAMEWORK_COMPATIBILITY_EXTENSION_ID,
    FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION, MAX_APPLICATION_MANIFEST_BYTES,
    MAX_MANIFEST_COLLECTION_ITEMS, MAX_MANIFEST_JSON_BYTES, MAX_MANIFEST_JSON_DEPTH,
    MAX_MANIFEST_STRING_BYTES,
};
pub use module::{
    ModelFieldSpec, ModelRelationshipSpec, ModelSpec, Module, ModuleBuilder, ModuleManifest,
    ProjectionSpec, SurfaceAggregateSpec, SurfaceArgumentSpec, SurfaceCommandSpec, SurfaceRootSpec,
    SurfaceSpec,
};
pub use mount::{MountSelector, ProcessPreset};
pub use plan::{
    compile_deployment_plan, DeploymentPlan, PlanFingerprint, ProcessIntent, ProcessPlan,
    DEPLOYMENT_PLAN_SCHEMA_VERSION, MAX_DEPLOYMENT_PLAN_BYTES,
};
pub(crate) use projection_contract::compact_projection_program_contract;
pub use projection_contract::expand_projection_program_contract;
pub use registration::{Application, ApplicationBuilder, ContractCompiler};
pub use runtime::{Runtime, RuntimeDialect};
pub use runtime_host::{bind_single_process, CapabilityProviders, RuntimeHost};
pub use topology::TopologyIntent;

/// Compile-time duplicate check emitted by the module macro for generated
/// command definition IDs. It compares the complete IDs, not a truncated or
/// hashed surrogate, so a hash collision cannot reject a valid module or hide
/// a duplicate declaration.
pub const fn assert_unique_command_ids(ids: &[&str]) {
    let mut index = 0;
    while index < ids.len() {
        let mut other = index + 1;
        while other < ids.len() {
            if same_const_str(ids[index], ids[other]) {
                panic!("duplicate command identity in module declaration");
            }
            other += 1;
        }
        index += 1;
    }
}

const fn same_const_str(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}
