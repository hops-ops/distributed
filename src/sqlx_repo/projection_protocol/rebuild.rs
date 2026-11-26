use super::*;
use crate::projection::rebuild::{
    invalid, RebuildContext, SnapshotProjectionRebuildPlan, MAX_REBUILD_RECORDS,
};

impl<DB> SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    // Caller holds the normal writer partition lock throughout inventory/commit.
    async fn rebuild_inventory(
        tx: &mut Transaction<'_, DB>,
        context: &RebuildContext,
    ) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
        let topology = context.compiled.topology();
        let hash = topology.digest();
        let partition = context.partition()?;
        let mut query = QueryBuilder::<DB>::new(
            "SELECT model_name, canonical_key_bytes, canonical_key_hash, partition_hash, \
             incarnation, revision, tombstone, change_epoch, change_position, source_snapshot \
             FROM projection_records WHERE topology_hash = ",
        );
        query.push_bind(hash.as_slice());
        query.push(" LIMIT ");
        query.push_bind((MAX_REBUILD_RECORDS + 1) as i64);
        let keys = query
            .build()
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| protocol_storage_error::<DB>("read rebuild inventory", e))?;
        if keys.len() > MAX_REBUILD_RECORDS {
            return Err(invalid("snapshot rebuild exceeds 10000 records"));
        }
        let mut records = Vec::with_capacity(keys.len());
        for key in &keys {
            let model: String = key
                .try_get("model_name")
                .map_err(|e| protocol_storage_error::<DB>("decode rebuild model", e))?;
            let bytes: Vec<u8> = key
                .try_get("canonical_key_bytes")
                .map_err(|e| protocol_storage_error::<DB>("decode rebuild key", e))?;
            let stored_partition: Vec<u8> = key
                .try_get("partition_hash")
                .map_err(|e| protocol_storage_error::<DB>("decode rebuild partition", e))?;
            verify_digest(
                &stored_partition,
                partition.digest(),
                "snapshot rebuild partition",
            )?;
            let scope =
                ProjectionRecordScope::new(topology.clone(), partition.clone(), model, bytes)?;
            records.push(decode_record_row::<DB>(&key, &scope, &context.epoch)?.metadata);
        }
        Ok(records)
    }

    pub(super) async fn snapshot_rebuild_records(
        &self,
        context: &RebuildContext,
    ) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
        let mut tx = self
            .pool()
            .begin_with(DB::BEGIN_READ_THEN_WRITE)
            .await
            .map_err(|e| protocol_storage_error::<DB>("begin rebuild inventory", e))?;
        verify_registered_topology_in_tx(
            &mut tx,
            self.verified_registrations(),
            context.compiled.topology(),
        )
        .await?;
        lock_partition_in_tx(
            &mut tx,
            context.compiled.topology(),
            &context.partition()?,
            &context.epoch,
        )
        .await?;
        let records = Self::rebuild_inventory(&mut tx, context).await?;
        // No bootstrap, metadata changes, or maintenance locks survive capture.
        tx.rollback()
            .await
            .map_err(|e| protocol_storage_error::<DB>("close rebuild inventory", e))?;
        Ok(records)
    }

    pub(super) async fn apply_snapshot_rebuild(
        &self,
        plan: SnapshotProjectionRebuildPlan,
    ) -> Result<usize, ProjectionProtocolError> {
        let context = &plan.context;
        let topology = context.compiled.topology();
        let partition = context.partition()?;
        let write_plan = plan.write_plan();
        write_plan.validate()?;
        let mut tx = self
            .pool()
            .begin_with(DB::BEGIN_READ_THEN_WRITE)
            .await
            .map_err(|e| protocol_storage_error::<DB>("begin snapshot rebuild", e))?;
        verify_registered_topology_in_tx(&mut tx, self.verified_registrations(), topology).await?;
        let mut state = lock_partition_in_tx(&mut tx, topology, &partition, &context.epoch).await?;
        plan.verify_inventory(&Self::rebuild_inventory(&mut tx, context).await?)?;
        ensure_partition_ownership_in_tx(
            &mut tx,
            self.verified_registrations(),
            topology,
            &partition,
            context.compiled.ownership(),
        )
        .await?;
        let record_rows = record_rows_in_tx(
            &mut tx,
            topology,
            &partition,
            &plan.rows.iter().map(|row| &row.scope).collect::<Vec<_>>(),
        )
        .await?;
        let mut changes = Vec::with_capacity(plan.rows.len());
        let mut records = Vec::with_capacity(plan.rows.len());
        for row in &plan.rows {
            let current = stored_record::<DB>(&record_rows, &row.scope, &context.epoch)?;
            let metadata = current.as_ref().map(|r| &r.metadata);
            row.verify_physical(
                metadata,
                physical_row_exists_in_tx(&mut tx, &row.mutation).await?,
            )?;
            let (kind, expectation) = row.transition(metadata);
            let (revision, tombstone) =
                next_record(&row.scope, &expectation, kind, metadata, true)?;
            let change = allocate_change(
                &mut state,
                topology,
                &partition,
                change_kind_for_mutation(kind),
                "distributed:snapshot-rebuild".into(),
                None,
                Some(row.scope.clone()),
                Some(revision.clone()),
                None,
                None,
            )?;
            let record = ProjectionRecordMetadata {
                revision,
                tombstone,
                change: change.cursor.clone(),
                source_snapshot: Some(row.source.clone()),
            };
            changes.push(change);
            records.push(record);
        }
        insert_changes_in_tx(&mut tx, &changes).await?;
        upsert_records_in_tx(&mut tx, &records).await?;
        apply_read_model_write_plan_in_tx(&mut tx, write_plan).await?;
        update_partition_head_in_tx(
            &mut tx,
            topology,
            &partition,
            state.change_head,
            state.pending_retry_failure_id.as_deref(),
        )
        .await?;
        retain_projection_change_suffix_in_tx(
            &mut tx,
            topology,
            &partition,
            &state,
            self.projection_change_retention(),
        )
        .await?;
        let mut tables = context
            .compiled
            .ownership()
            .iter()
            .map(|o| o.table.clone())
            .collect::<BTreeSet<_>>();
        tables.insert(PROJECTION_CHANGE_NOTIFY_TABLE.to_string());
        if self.projection_notify_enabled() {
            DB::push_change_notify(&mut *tx, &tables).await?;
        }
        tx.commit()
            .await
            .map_err(|e| protocol_storage_error::<DB>("commit snapshot rebuild", e))?;
        self.publish_read_model_change(crate::read_model::ReadModelChange { tables });
        Ok(plan.rows.len())
    }
}
