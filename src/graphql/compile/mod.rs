//! Selection set -> single SQL statement per root field (dialect-portable JSON tree).
//!
//! # v1 join / PK assumptions
//!
//! Relationship SQL:
//! - **HasMany**: FK lives on the child -> `child.fk = parent.pk`
//!   (`foreign_key` lists child columns in parent-PK order)
//! - **BelongsTo**: FK lives on the parent -> `child.pk = parent.fk`
//!   (`foreign_key` lists parent columns in target-PK order)
//! - **ManyToMany**: through-table holds the full primary key of each end
//!   (same-named columns, or `foreign_key` / `target_foreign_key` listing
//!   through columns in PK order). Join helpers AND those equalities.
//!
//! Join equality is centralized in [`join_predicate_direct`] /
//! [`join_predicate_m2m_pairs`].
//! Dialect SQL fragments live on [`DialectOps`].
#![allow(clippy::only_used_in_recursion, clippy::too_many_arguments)]

mod binds;
mod dialect;
mod error;
mod evidence;
mod filter;
mod projection;
mod relationship;
mod scan;

pub use binds::BindValue;
pub use dialect::SqlDialect;
pub use error::CompileError;
pub use projection::{
    compile_query, selection_from_field, QueryPlan, RootKind, SelectionNode, SqlPlan,
};

pub(crate) use evidence::{ExtractedQueryEvidence, QueryResponsePathSegment};
pub(crate) use projection::cell_row_matches;
