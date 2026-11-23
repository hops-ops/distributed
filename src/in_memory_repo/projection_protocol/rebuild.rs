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
        let partition =
            PartitionKey::new(plan.context.compiled.topology(), &plan.context.partition()?);
        // As in a commit: compute against stored state, then apply.
        let state = protocol.partitions.get(&partition);
        if let Some(state) = state {
            ensure_change_epoch(&state.change_epoch, &plan.context.epoch)?;
        }
        let mut head = state.map_or(0, |state| state.change_head);
        for owner in plan.context.compiled.ownership() {
            let registered = RegisteredModelKey {
                topology: partition.topology.clone(),
                model: owner.model.clone(),
            };
            if protocol.registered_models.get(&registered) != Some(&owner.table)
                || protocol.authoritative_table_owners.get(&owner.table) != Some(&registered)
            {
                return Err(invalid("snapshot rebuild does not own the target table"));
            }
        }
        let mut staged_records = HashMap::with_capacity(plan.rows.len());
        let mut changes = Vec::with_capacity(plan.rows.len());
        for row in &plan.rows {
            let current = staged_records
                .get(&row.scope)
                .or_else(|| protocol.records.get(&row.scope));
            row.verify_physical(current, rows.contains_key(&row.mutation.lock_key()))?;
            let (kind, expectation) = row.transition(current);
            let (revision, tombstone) = next_record(&row.scope, &expectation, kind, current, true)?;
            let change = next_change(
                &partition,
                &plan.context.epoch,
                &mut head,
                PendingChange {
                    kind: change_kind_for_mutation(kind),
                    causation_id: "distributed:snapshot-rebuild".into(),
                    observation_kind: None,
                    scope: Some(row.scope.clone()),
                    revision: Some(revision.clone()),
                    failure_id: None,
                    program_id: None,
                },
            )?;
            let record = ProjectionRecordMetadata {
                revision,
                tombstone,
                change: change.cursor.clone(),
                source_snapshot: Some(row.source.clone()),
            };
            ensure_live_record_identity_available(&protocol.records, &staged_records, &record)?;
            staged_records.insert(row.scope.clone(), record);
            changes.push(change);
        }
        // Only the rows the plan writes are staged.
        let touched_rows = plan
            .rows
            .iter()
            .map(|row| row.mutation.lock_key())
            .collect::<HashSet<_>>();
        let mut staged_rows = HashMap::with_capacity(touched_rows.len());
        for key in &touched_rows {
            if let Some(stored) = rows.get(key) {
                staged_rows.insert(key.clone(), stored.clone());
            }
        }
        apply_read_model_write_plan(plan.write_plan(), &mut staged_rows)?;
        let through = retained_through(state, head, self.projection_change_retention)?;

        // Nothing below can fail.
        for key in touched_rows {
            match staged_rows.remove(&key) {
                Some(stored) => {
                    rows.insert(key, stored);
                }
                None => {
                    rows.remove(&key);
                }
            }
        }
        let protocol = &mut *protocol;
        let state = protocol.partition_mut(&partition, &plan.context.epoch);
        state.append(changes);
        state.compact(through);
        protocol
            .ownership
            .extend(plan.context.compiled.ownership().iter().map(|owner| {
                (
                    OwnershipKey {
                        partition: partition.clone(),
                        model: owner.model.clone(),
                    },
                    owner.table.clone(),
                )
            }));
        protocol.records.extend(staged_records);
        Ok(plan.rows.len())
    }
}
