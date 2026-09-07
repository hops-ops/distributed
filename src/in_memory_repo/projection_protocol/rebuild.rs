use super::*;
use crate::projection::rebuild::{
    invalid, RebuildContext, SnapshotProjectionRebuildPlan, MAX_REBUILD_RECORDS,
};

fn inventory(
    protocol: &InMemoryProjectionProtocolState,
    context: &RebuildContext,
) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
    protocol.require_registered_topology(context.compiled.topology())?;
    let partition = context.partition()?;
    let mut records = Vec::new();
    for (scope, record) in &protocol.records {
        if scope.topology() == context.compiled.topology() {
            if scope.projection_partition() != &partition || record.change.epoch() != &context.epoch
            {
                return Err(invalid(
                    "snapshot rebuild requires the registered unit partition and epoch",
                ));
            }
            records.push(record.clone());
            if records.len() > MAX_REBUILD_RECORDS {
                return Err(invalid("snapshot rebuild exceeds 10000 records"));
            }
        }
    }
    Ok(records)
}

impl InMemoryRepository {
    pub(super) async fn snapshot_rebuild_records(
        &self,
        context: &RebuildContext,
    ) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
        let protocol = self
            .projection_protocol
            .read()
            .map_err(|_| invalid("projection rebuild inventory lock poisoned"))?;
        inventory(&protocol, context)
    }

    pub(super) async fn apply_snapshot_rebuild(
        &self,
        plan: SnapshotProjectionRebuildPlan,
    ) -> Result<usize, ProjectionProtocolError> {
        // Same lock order as ordinary commits. Stage both stores before publication.
        let mut rows = self
            .model_store
            .relational_rows
            .write()
            .map_err(|_| invalid("projection rebuild rows lock poisoned"))?;
        let mut protocol = self
            .projection_protocol
            .write()
            .map_err(|_| invalid("projection rebuild protocol lock poisoned"))?;
        plan.verify_inventory(&inventory(&protocol, &plan.context)?)?;
        let mut staged = protocol.clone();
        let mut staged_rows = rows.clone();
        let partition =
            PartitionKey::new(plan.context.compiled.topology(), &plan.context.partition()?);
        staged.ensure_partition(&partition, &plan.context.epoch)?;
        for owner in plan.context.compiled.ownership() {
            let registered = RegisteredModelKey {
                topology: partition.topology.clone(),
                model: owner.model.clone(),
            };
            if staged.registered_models.get(&registered) != Some(&owner.table)
                || staged.authoritative_table_owners.get(&owner.table) != Some(&registered)
            {
                return Err(invalid("snapshot rebuild does not own the target table"));
            }
            staged.ownership.insert(
                OwnershipKey {
                    partition: partition.clone(),
                    model: owner.model.clone(),
                },
                owner.table.clone(),
            );
        }
        for row in &plan.rows {
            let current = staged.records.get(&row.scope);
            row.verify_physical(current, staged_rows.contains_key(&row.mutation.lock_key()))?;
            let (kind, expectation) = row.transition(current);
            let (revision, tombstone) = staged.next_record(&row.scope, &expectation, kind, true)?;
            let change = staged.append_change(
                &partition,
                PendingChange {
                    kind: change_kind_for_mutation(kind),
                    causation_id: "distributed:snapshot-rebuild".into(),
                    observation_kind: None,
                    scope: Some(row.scope.clone()),
                    revision: Some(revision.clone()),
                    failure_id: None,
                },
            )?;
            let record = ProjectionRecordMetadata {
                revision,
                tombstone,
                change: change.cursor,
                source_snapshot: Some(row.source.clone()),
            };
            staged.ensure_live_record_identity_available(&record)?;
            staged.records.insert(row.scope.clone(), record);
        }
        apply_read_model_write_plan(plan.write_plan(), &mut staged_rows)?;
        staged.retain_change_suffix(&partition, self.projection_change_retention)?;
        *rows = staged_rows;
        *protocol = staged;
        Ok(plan.rows.len())
    }
}
