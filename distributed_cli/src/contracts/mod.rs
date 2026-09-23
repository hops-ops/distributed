//! Generic contract lifecycle records used by the CLI and its future checks.
//!
//! This module records ownership, references, identities, and diagnostics. It
//! deliberately does not model the semantic payload of an application,
//! deployment, migration, or client artifact. Those payloads remain owned by
//! their respective compiler or framework modules.

mod artifact;
mod catalog;
mod chain;
mod classification;
mod closeout;
mod diagnostic;
mod migrations;
mod program;
mod snapshots;
mod transaction;

#[cfg(test)]
mod tests;

pub use artifact::{
    ArtifactIdentity, ArtifactPredecessor, ArtifactProvenance, ContractArtifactKind,
    EnvironmentPolicyReference,
};
pub use catalog::{
    ClientDeclaration, ClientInventory, ContractCatalog, ContractEntry, ContractError,
    ContractScope, CLIENT_DECLARATION_SCHEMA_VERSION, CONTRACT_CATALOG_SCHEMA_VERSION,
    MAX_CATALOG_BYTES, MAX_CATALOG_DIRECTORIES, MAX_CATALOG_DIRECTORY_DEPTH,
    MAX_CATALOG_DIRECTORY_ENTRIES, MAX_CATALOG_ENTRIES, MAX_CATALOG_FILES,
    MAX_CATALOG_GLOB_MATCHES, MAX_CATALOG_JSON_DEPTH,
};
pub use chain::{check_predecessor_chain, ObservedPredecessor};
pub use classification::{
    classify_snapshot_diff, decisions_are_distinct, ClassifiedChange, LifecycleDecision,
};
pub use closeout::{classify_release_programs, close_local_contract_chain};
pub use diagnostic::{
    ContractCheckResult, ContractDiagnostic, ContractDiagnosticCode, SafeDiagnosticValue,
};
pub use migrations::{
    check_migration_history, check_migration_inventory, BaselineAvailability, MigrationDialect,
    MigrationEntry, MigrationFile, MigrationHistoryCheck, MigrationInventory, MAX_MIGRATIONS,
    MAX_MIGRATION_INVENTORY_BYTES, MAX_MIGRATION_JSON_DEPTH, MAX_MIGRATION_SQL_BYTES,
    MAX_MIGRATION_TOP_LEVEL_ENTRIES, MAX_MIGRATION_TOTAL_ENTRIES, MIGRATION_INVENTORY_PATH,
    MIGRATION_INVENTORY_SCHEMA_VERSION, MIGRATION_OWNER, MIGRATION_SCOPE,
};
pub use program::{
    ClientProgramArtifact, ClientProgramAsset, ClientProgramDescriptor, ClientProgramSurface,
    ProgramCompatibility, CLIENT_PROGRAM_DESCRIPTOR_VERSION, CLIENT_PROGRAM_POLICY_VERSION,
};
pub use snapshots::{
    diff_snapshots, snapshot_from_json, SemanticSnapshot, SnapshotChange, SnapshotDiff,
    SnapshotEntry, MAX_SNAPSHOT_DEPTH, MAX_SNAPSHOT_PATHS, MAX_SNAPSHOT_VALUE_BYTES,
};
pub use transaction::{
    contracts_accept, contracts_check, unknown_scope_diagnostic, ContractAcceptScope,
    ContractsAcceptReport, ContractsCheckReport,
};
