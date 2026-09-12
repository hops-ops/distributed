//! Backend-agnostic read-model logic shared by the Postgres and SQLite repositories.
//!
//! Two layers live here:
//!
//! 1. **Pure helpers** (no `sqlx` types): write-plan validation, key/patch
//!    reconciliation, row-version arithmetic, relationship-include resolution.
//! 2. **The generic relational write path**: free functions over
//!    `DB: SqlxReadModelBackend` that build and run the upsert/patch/delete SQL via
//!    `QueryBuilder<DB>`. Only the value-binding (`Bool`/typed-`NULL`), the
//!    `rows_affected` accessor, and two label strings differ per dialect; the
//!    [`SqlxReadModelBackend`] trait carries exactly those — one trait, two
//!    one-line-per-method impls — so the SQL-building logic exists once rather than
//!    being mirrored (and risking silent drift) in `postgres_repo`/`sqlite_repo`.

mod backend;
mod load;
mod query;
mod schema_registry;
mod validation;
mod write_plan;

pub use backend::SqlxReadModelBackend;
pub(crate) use load::load_read_model_graph;
pub(crate) use query::{
    push_key_predicates, push_order_by_primary_key, relational_row_select, row_to_versioned_values,
};
pub(crate) use schema_registry::{
    remember_read_model_schemas, resolve_registered_read_model_schemas, IncludeSpec,
};
pub(crate) use validation::{
    column_by_name, initial_row_version, patch_values_preserving_key, quote_identifier,
    row_concurrency_conflict, row_values_from_key_and_patch, row_write_values,
    sql_read_model_capabilities, validate_row_expected_version, validate_sql_write_plan,
    validate_values_match_key, version_column,
};
pub(crate) use write_plan::{
    apply_read_model_write_plan_in_tx, begin_read_model_tx, commit_read_model_tx, row_version_in_tx,
};
