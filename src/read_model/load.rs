//! Explicit primary-key load requests and the untyped graphs adapters return.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use crate::repository::{ReadModelWritePlanStore, RelationalReadModelQueryStore};

use super::workspace::ReadModelWorkspace;
use super::{RelationalReadModel, RelationalReadModelIncludes, Versioned};
use crate::table::{RelationshipDef, RowKey, RowValues, TableSchema, TableStoreError};

/// A request an adapter can satisfy with a primary-key read plus explicit includes.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadModelLoadRequest {
    pub schema: TableSchema,
    pub key: RowKey,
    pub includes: Vec<String>,
}

/// Rows loaded for one requested relationship include.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadModelIncludeRows {
    pub relationship: RelationshipDef,
    pub target_schema: TableSchema,
    pub rows: Vec<Versioned<RowValues>>,
}

/// Untyped graph loaded by a relational read-model adapter.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadModelLoadGraph {
    pub root: Option<Versioned<RowValues>>,
    pub includes: BTreeMap<String, ReadModelIncludeRows>,
}

/// Builder for one explicit primary-key read-model load over the async store traits.
pub struct ReadModelLoadBuilder<'workspace, 'store, S, M>
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    pub(super) unit: &'workspace mut ReadModelWorkspace<'store, S>,
    pub(super) key: RowKey,
    pub(super) includes: Vec<String>,
    pub(super) _marker: PhantomData<M>,
}

impl<'workspace, 'store, S, M> ReadModelLoadBuilder<'workspace, 'store, S, M>
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore,
    M: RelationalReadModel + RelationalReadModelIncludes,
{
    pub fn include(mut self, relationship: impl Into<String>) -> Self {
        self.includes.push(relationship.into());
        self
    }

    pub async fn one(self) -> Result<Option<Versioned<M>>, TableStoreError> {
        let request = self
            .unit
            .writes
            .load_with::<M, _, _>(self.key, self.includes)?;
        let graph = self.unit.store.load_graph(request.clone()).await?;
        let Some(root) = graph.root else {
            return Ok(None);
        };

        let mut model = M::from_row(root.data.clone())?;
        for (include_name, include_rows) in &graph.includes {
            let rows = include_rows
                .rows
                .iter()
                .map(|row| row.data.clone())
                .collect::<Vec<_>>();
            model.hydrate_include(include_name, rows)?;
        }

        self.unit.track_graph::<M>(root.clone(), graph.includes)?;
        Ok(Some(Versioned {
            data: model,
            version: root.version,
        }))
    }
}
