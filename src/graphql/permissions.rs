//! Role-based **read** permissions for GraphQL models.
//!
//! # Mental model (deny-by-default)
//!
//! 1. A role that is **not** listed for a model cannot see that model at all.
//! 2. **Columns** — explicit allowlist, or all columns.
//! 3. **Rows** — optional predicate; when set, every access path (list, by_pk,
//!    relationships, aggregates) AND’s it into SQL `WHERE`.
//!
//! ```ignore
//! ModelPermissions::new()
//!     .grant(
//!         "user",
//!         read()
//!             .all_columns()
//!             .rows(col("owner_id").eq(claim("x-user-id"))),
//!     )
//!     .grant("admin", read().all_columns().aggregations())
//! ```
//!
//! Prefer this vocabulary over “filter” / bare “allow”: grants are roles,
//! columns are field allowlists, rows are row scope.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use super::filter::FilterExpr;

/// Per-role read access for one model.
#[derive(Clone, Debug)]
pub struct ReadPermission {
    /// Allowed column names. Empty means no columns (deny-by-default start).
    pub(crate) columns: Option<BTreeSet<String>>,
    pub(crate) all_columns: bool,
    /// When set, rows must match this predicate (compiled into every WHERE).
    pub(crate) row_filter: Option<FilterExpr>,
    pub(crate) limit: Option<u64>,
    pub(crate) aggregations: bool,
}

/// Start a deny-by-default read grant (no columns, no rows, no aggregations).
pub fn read() -> ReadPermission {
    ReadPermission {
        columns: Some(BTreeSet::new()),
        all_columns: false,
        row_filter: None,
        limit: None,
        aggregations: false,
    }
}

impl ReadPermission {
    /// Allow every column on the model for this role.
    pub fn all_columns(mut self) -> Self {
        self.all_columns = true;
        self.columns = None;
        self
    }

    /// Allow only these columns (deny-by-default for the rest).
    pub fn columns<I: IntoIterator<Item = impl Into<String>>>(mut self, i: I) -> Self {
        self.all_columns = false;
        self.columns = Some(i.into_iter().map(Into::into).collect());
        self
    }

    /// Restrict visible rows to those matching `predicate`.
    ///
    /// Compiled into the `WHERE` of list, by_pk, nested relationships, EXISTS
    /// filters, and aggregates — not an optional soft filter.
    pub fn rows(mut self, predicate: FilterExpr) -> Self {
        self.row_filter = Some(predicate);
        self
    }

    /// Cap the default page size for this role on this model (still clamped by
    /// engine max_limit).
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Enable aggregate root / nested aggregate fields for this role.
    pub fn aggregations(mut self) -> Self {
        self.aggregations = true;
        self
    }

    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    pub(crate) fn allows_column(&self, name: &str) -> bool {
        if self.all_columns {
            return true;
        }
        self.columns
            .as_ref()
            .is_some_and(|cols| cols.contains(name))
    }
}

/// Typed bag of `(role, ReadPermission)` pairs for one model.
pub struct ModelPermissions<M> {
    pub(crate) entries: Vec<(String, ReadPermission)>,
    _marker: PhantomData<M>,
}

impl<M> Default for ModelPermissions<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> ModelPermissions<M> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Grant `perm` to `role`. Roles never granted cannot query this model.
    pub fn grant(mut self, role: &str, perm: ReadPermission) -> Self {
        self.entries.push((role.to_string(), perm));
        self
    }

    /// The grants keyed by role, for selecting a role or application surface
    /// outside the engine (manifest export, generated clients).
    pub fn by_role(&self) -> BTreeMap<String, ReadPermission> {
        self.entries.iter().cloned().collect()
    }
}

/// A read model whose read permissions are declared next to it.
///
/// ```ignore
/// impl AuthorizedReadModel for Todos {
///     fn permissions() -> ModelPermissions<Self> {
///         ModelPermissions::new().grant("admin", read().all_columns())
///     }
/// }
/// ```
pub trait AuthorizedReadModel: crate::RelationalReadModelIncludes {
    /// Who may read this model, which columns and which rows.
    fn permissions() -> ModelPermissions<Self>;
}

/// The read models one application serves, each with its permissions.
///
/// The single list every consumer reads: schema bootstrap
/// ([`catalog`](Self::catalog)), the GraphQL engine
/// (`GraphqlEngineBuilder::read_models`), role grants for client export
/// ([`grants`](Self::grants)) and table names ([`table_names`](Self::table_names)).
#[derive(Clone)]
pub struct ReadModels {
    catalog: crate::ReadModelCatalog,
    models: Vec<RegisteredReadModel>,
}

#[derive(Clone)]
struct RegisteredReadModel {
    model_name: String,
    grants: Vec<(String, ReadPermission)>,
    #[cfg(feature = "graphql")]
    register: fn(super::GraphqlEngineBuilder) -> super::GraphqlEngineBuilder,
}

#[cfg(feature = "graphql")]
fn register_model<M: AuthorizedReadModel>(
    builder: super::GraphqlEngineBuilder,
) -> super::GraphqlEngineBuilder {
    builder.model::<M>(M::permissions())
}

impl ReadModels {
    /// An empty catalog named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            catalog: crate::ReadModelCatalog::new(name),
            models: Vec::new(),
        }
    }

    /// Serve `M` with its own permissions.
    ///
    /// # Panics
    ///
    /// When `M`'s schema is invalid or collides with a model already listed.
    pub fn model<M: AuthorizedReadModel>(mut self) -> Self {
        self.catalog = self.catalog.read_model::<M>();
        self.models.push(RegisteredReadModel {
            model_name: M::schema().model_name.clone(),
            grants: M::permissions().entries,
            #[cfg(feature = "graphql")]
            register: register_model::<M>,
        });
        self
    }

    /// The physical tables, for schema bootstrap and Surface building.
    pub fn catalog(&self) -> &crate::ReadModelCatalog {
        &self.catalog
    }

    /// Grants by role, then model name.
    pub fn grants(&self) -> BTreeMap<String, BTreeMap<String, ReadPermission>> {
        let mut grants = BTreeMap::<String, BTreeMap<String, ReadPermission>>::new();
        for model in &self.models {
            for (role, grant) in &model.grants {
                grants
                    .entry(role.clone())
                    .or_default()
                    .insert(model.model_name.clone(), grant.clone());
            }
        }
        grants
    }

    /// The table names, in catalog order.
    pub fn table_names(&self) -> Vec<String> {
        self.catalog
            .tables
            .iter()
            .map(|table| table.table_name.clone())
            .collect()
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn register(
        &self,
        mut builder: super::GraphqlEngineBuilder,
    ) -> super::GraphqlEngineBuilder {
        for model in &self.models {
            builder = (model.register)(builder);
        }
        builder
    }
}
