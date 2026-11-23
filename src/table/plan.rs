//! Write plans: the deterministic mutation batches table stores apply.

use super::mutation::{validate_delete_mutation, validate_patch_mutation, validate_row_mutation};
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
        for mutation in &self.mutations {
            match mutation {
                TableMutation::UpsertRow(mutation) => validate_row_mutation(mutation)?,
                TableMutation::PatchRow(mutation) => validate_patch_mutation(mutation)?,
                TableMutation::DeleteRow(mutation) => validate_delete_mutation(mutation)?,
            }
        }

        Ok(())
    }
}
