//! The query cost model: what one GraphQL request may ask the database for.
//!
//! Every knob has one unit, and the engine enforces each in one place. The
//! client manifest exports the same numbers (`execution`) so a client compiler
//! rejects the same operations at build time. `docs/graphql-query-cost.md`
//! is the user-facing description.
//!
//! | Knob | Unit | Enforced by |
//! |---|---|---|
//! | `max_depth` | nesting levels: fields in the document; relationships and `where` (boolean and relationship nesting) in the compiler | async-graphql `limit_depth`, the compiler |
//! | `max_fields` | selected fields in one document, every field once at every level | async-graphql `limit_complexity` |
//! | `max_rows` | model rows one root field's statement can produce (below) | the compiler, per root field |
//! | `default_limit`, `max_limit`, role `limit` | a list's page size: the client's `limit`, else the default, capped by the role and the engine | the compiler, per list |
//! | `max_in_list`, `max_bool_width` | entries in one `_in`/`_nin` list; entries in one `_and`/`_or` list | the compiler |
//! | `statement_timeout` | wall clock per root field | the database and the executor |
//!
//! **Rows.** A page of `limit` rows costs `limit × (1 + rows nested under one
//! of its rows)`; one object (a by-pk root, a belongs-to relationship) costs
//! `1 + rows nested under it`; columns and `aggregate { count }` cost nothing.
//! Each list counts its *resolved* limit, so for one chain of nested lists the
//! cost is about the product of their limits:
//! `orders(limit: 1000) { items(limit: 1000) { id } }` costs 1,001,000 rows.
//! `offset` is not part of the cost: skipped rows are scanned, not returned,
//! and the statement timeout bounds the scan.

/// Page size of a list whose request gives no `limit`.
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(crate) const DEFAULT_LIMIT: u64 = 100;
/// Largest page size any list may resolve to.
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(crate) const DEFAULT_MAX_LIMIT: u64 = 1_000;
/// Default nesting depth.
pub(crate) const DEFAULT_MAX_DEPTH: usize = 8;
/// Default selected fields per document.
pub(crate) const DEFAULT_MAX_FIELDS: usize = 500;
/// Default rows per root field. Admits a root list at the maximum page size
/// with one default-sized nested list (1000 × (1 + 100) = 101,000), or a
/// default-sized root list with up to 19 default-sized sibling lists; refuses
/// a second nested level at the default size (100 × (1 + 100 × 101) =
/// 1,010,100) and nested lists at the maximum size (1000 × 1001 = 1,001,000).
pub(crate) const DEFAULT_MAX_ROWS: u64 = 200_000;
/// Default entries in one `_in` / `_nin` list.
pub(crate) const DEFAULT_MAX_IN_LIST: usize = 1_000;
/// Default entries in one `_and` / `_or` list.
pub(crate) const DEFAULT_MAX_BOOL_WIDTH: usize = 256;
