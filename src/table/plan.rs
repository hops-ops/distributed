//! Write plans: the deterministic mutation batches table stores apply.

use super::{TableMutation, TableStoreError};

/// Deterministic unit-of-work output for relational table-store adapters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TableWritePlan {
    pub mutations: Vec<TableMutation>,
}

impl TableWritePlan {
    pub fn new(mutations: Vec<TableMutation>) -> Self {
        Self { mutations }
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn validate(&self) -> Result<(), TableStoreError> {
        self.mutations.iter().try_for_each(TableMutation::validate)
    }
}
