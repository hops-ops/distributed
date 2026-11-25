//! Generic contract lifecycle records used by the CLI's build/dev lifecycle.
//!
//! This module records ownership, references, identities, and diagnostics. It
//! deliberately does not model the semantic payload of an application,
//! deployment, migration, or client artifact. Those payloads remain owned by
//! their respective compiler or framework modules.

mod artifact;
mod catalog;
mod diagnostic;

#[cfg(test)]
mod tests;

pub use artifact::{
    ArtifactIdentity, ArtifactPredecessor, ArtifactProvenance, ContractArtifactKind,
};
pub use catalog::{
    ClientDeclaration, ClientInventory, ContractCatalog, ContractEntry, ContractError,
    ContractScope, CONTRACT_CATALOG_SCHEMA_VERSION, MAX_CATALOG_GLOB_MATCHES,
};
pub use diagnostic::ContractDiagnosticCode;
