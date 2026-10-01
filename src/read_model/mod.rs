//! Read Models - storage-backed projections and read-optimized views.
//!
//! Relational models stage explicit row mutations:
//!
//! ```ignore
//! use distributed::read_model::{ReadModelWritePlanBuilder, ReadModelWritePlanCommitExt};
//!
//! let mut read_models = ReadModelWritePlanBuilder::new();
//! read_models.upsert(&player)?;
//! read_models.upsert_related(&player, "weapons", &weapon)?;
//! repo.read_models(read_models).commit(&mut aggregate).await?;
//! ```
//!
//! Persistent repositories expose the staging shape through
//! `ReadModelWritePlanCommitExt::read_models`, returning a future
//! from `commit`.
//!
//! Distributed projectors can commit a write plan directly against a read-model
//! adapter. Projection handlers should make those writes idempotent so bus
//! retries can safely replay the same message:
//!
//! ```ignore
//! let mut read_models = ReadModelWritePlanBuilder::new();
//! read_models.upsert(&view)?;
//! let outcome = read_models.commit(&read_store)?;
//! ```

mod capabilities;
pub mod change;
pub(crate) mod in_memory;
mod load;
mod scan;
mod plan;
mod workspace;

pub use change::ReadModelChange;

use serde::{de::DeserializeOwned, Serialize};

use crate::table::{RowKey, RowValues, TableSchema, TableStoreError};

/// Trait implemented by the derive macro for read-model identity metadata.
pub trait ReadModel: Serialize + DeserializeOwned + Clone + Send + Sync {
    /// The declared storage name for this read model type.
    const COLLECTION: &'static str;

    /// Returns the unique identifier for this read model instance.
    fn id(&self) -> &str;
}

/// A versioned wrapper around read model data for optimistic concurrency control.
#[derive(Debug, Clone, PartialEq)]
pub struct Versioned<T> {
    pub data: T,
    pub version: u64,
}

/// Opt-in trait for table-mapped relational read models.
pub trait RelationalReadModel: Clone + Send + Sync + Sized {
    /// The model's schema. Static because a model's schema is fixed at compile
    /// time; the derive macro backs this with a `LazyLock` so staging mutations
    /// never rebuilds or clones schema metadata.
    fn schema() -> &'static TableSchema;

    /// Stable declared read-model identity.
    ///
    /// This is `#[readmodel(name = "...")]` when present, otherwise the Rust
    /// type name stored on [`TableSchema::model_name`]. It is not the SQL
    /// table, process owner, or GraphQL field.
    fn read_model_id() -> &'static str {
        Self::schema().model_name.as_str()
    }

    fn primary_key(&self) -> Result<RowKey, TableStoreError>;
    fn to_row(&self) -> Result<RowValues, TableStoreError>;
    fn from_row(row: RowValues) -> Result<Self, TableStoreError>;
}

/// Relationship hydration hooks generated for table-mapped read models.
pub trait RelationalReadModelIncludes: RelationalReadModel {
    fn hydrate_include(
        &mut self,
        include: &str,
        rows: Vec<RowValues>,
    ) -> Result<(), TableStoreError>;

    fn include_rows(&self, include: &str) -> Result<Vec<RowValues>, TableStoreError>;

    /// Schema of the model targeted by the named relationship.
    fn include_target_schema(include: &str) -> Result<&'static TableSchema, TableStoreError>;
}

pub use capabilities::ReadModelQueryCapabilities;
pub use scan::{ReadModelScanCursor, ReadModelScanPage, ReadModelScanRequest};
pub use in_memory::InMemoryReadModelStore;
pub use load::{
    ReadModelIncludeRows, ReadModelLoadBuilder, ReadModelLoadGraph, ReadModelLoadRequest,
};
pub use plan::ReadModelWritePlanBuilder;
pub use workspace::{ReadModelWorkspace, ReadModelWorkspaceExt};
