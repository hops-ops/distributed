use super::*;

pub(super) fn decode_partition_row<DB>(
    row: &DB::Row,
    topology: &ProjectorTopologyId,
    partition: &crate::projection_protocol::PartitionId,
) -> Result<PartitionState, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_bytes: Vec<u8> = row
        .try_get("topology_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode projection topology bytes", error))?;
    let partition_bytes: Vec<u8> = row.try_get("partition_bytes").map_err(|error| {
        protocol_storage_error::<DB>("decode projection partition bytes", error)
    })?;
    verify_bytes(
        &topology_bytes,
        &topology.canonical_bytes(),
        "projector topology",
    )?;
    verify_bytes(
        &partition_bytes,
        partition.canonical_bytes(),
        "projection partition",
    )?;

    let active_generation = ProjectionGeneration::new(from_i64::<DB>(
        row.try_get("active_generation").map_err(|error| {
            protocol_storage_error::<DB>("decode projection active generation", error)
        })?,
        "projection active generation",
    )?)?;
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode projection change epoch", error))?;
    let change_epoch = ProjectionEpoch::new(change_epoch)?;
    let change_head = from_i64::<DB>(
        row.try_get("change_head").map_err(|error| {
            protocol_storage_error::<DB>("decode projection change head", error)
        })?,
        "projection change head",
    )?;
    let compacted_through = from_i64::<DB>(
        row.try_get("compacted_through").map_err(|error| {
            protocol_storage_error::<DB>("decode projection compaction watermark", error)
        })?,
        "projection compaction watermark",
    )?;
    if compacted_through > change_head {
        return Err(corrupt_storage(
            "projection compaction watermark exceeds change head",
        ));
    }
    let stopped_failure_id = row.try_get("stopped_failure_id").map_err(|error| {
        protocol_storage_error::<DB>("decode stopped projection failure", error)
    })?;
    let pending_retry_failure_id = row
        .try_get("pending_retry_failure_id")
        .map_err(|error| protocol_storage_error::<DB>("decode pending projection retry", error))?;
    Ok(PartitionState {
        active_generation,
        change_epoch,
        change_head,
        compacted_through,
        pending_retry_failure_id,
        stopped_failure_id,
    })
}

/// Lock a partition and return its stored state, creating it with a newly
/// minted change epoch when it does not exist yet. An existing partition
/// keeps the epoch it was created with.
pub(super) async fn lock_partition_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &crate::projection_protocol::PartitionId,
) -> Result<PartitionState, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let topology_bytes = topology.canonical_bytes();
    // Only a partition this statement creates takes the minted epoch. An
    // existing partition keeps the epoch it has and returns it.
    let minted_epoch = ProjectionEpoch::mint();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_partitions \
         (topology_bytes, topology_hash, partition_bytes, partition_hash, active_generation, change_epoch) \
         VALUES (",
    );
    builder.push_bind(topology_bytes.as_slice());
    builder.push(", ");
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition.canonical_bytes());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", 1, ");
    builder.push_bind(minted_epoch.as_str());
    builder.push(
        ") ON CONFLICT (topology_hash, partition_hash) DO UPDATE \
         SET topology_hash = excluded.topology_hash \
         RETURNING topology_bytes, partition_bytes, active_generation, change_epoch, \
         change_head, compacted_through, pending_retry_failure_id, stopped_failure_id",
    );
    let row = builder
        .build()
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("lock projection partition", error))?;
    let state = decode_partition_row::<DB>(&row, topology, partition)?;

    if generation_exists_in_tx::<DB>(tx, topology, partition, state.active_generation).await? {
        return Ok(state);
    }
    // Only a partition this statement just created has no generation yet.
    if state.active_generation != ProjectionGeneration::initial() {
        return Err(missing_generation(state.active_generation));
    }
    let mut generation = QueryBuilder::<DB>::new(
        "INSERT INTO projection_generations \
         (topology_hash, partition_hash, generation, retry_of_generation, retry_of_failure_id) \
         VALUES (",
    );
    generation.push_bind(topology_hash.as_slice());
    generation.push(", ");
    generation.push_bind(partition_hash.as_slice());
    generation.push(
        ", 1, NULL, NULL) ON CONFLICT (topology_hash, partition_hash, generation) DO NOTHING",
    );
    generation
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("ensure initial projection generation", error)
        })?;
    Ok(state)
}

pub(super) async fn lock_existing_partition_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &crate::projection_protocol::PartitionId,
) -> Result<Option<PartitionState>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "UPDATE projection_partitions SET topology_hash = topology_hash WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(
        " RETURNING topology_bytes, partition_bytes, active_generation, change_epoch, \
         change_head, compacted_through, pending_retry_failure_id, stopped_failure_id",
    );
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("lock projection partition", error))?;
    row.map(|row| decode_partition_row::<DB>(&row, topology, partition))
        .transpose()
}

#[allow(dead_code)]
pub(super) async fn load_partition<DB>(
    pool: &Pool<DB>,
    topology: &ProjectorTopologyId,
    partition: &crate::projection_protocol::PartitionId,
) -> Result<Option<PartitionState>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT topology_bytes, partition_bytes, active_generation, change_epoch, \
         change_head, compacted_through, pending_retry_failure_id, stopped_failure_id \
         FROM projection_partitions WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    let row = builder
        .build()
        .fetch_optional(pool)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection partition", error))?;
    row.map(|row| decode_partition_row::<DB>(&row, topology, partition))
        .transpose()
}

pub(super) async fn load_partition_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
) -> Result<Option<PartitionState>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    load_partition_in_connection(&mut **tx, topology, partition).await
}

pub(super) async fn load_partition_in_connection<DB>(
    connection: &mut DB::Connection,
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
) -> Result<Option<PartitionState>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT topology_bytes, partition_bytes, active_generation, change_epoch, \
         change_head, compacted_through, pending_retry_failure_id, stopped_failure_id \
         FROM projection_partitions WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    let row = builder
        .build()
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection partition", error))?;
    row.map(|row| decode_partition_row::<DB>(&row, topology, partition))
        .transpose()
}

/// Read the exact durable live boundary using a caller-owned SQL snapshot.
pub(crate) async fn read_projection_partition_snapshot_in_executor<DB>(
    connection: &mut DB::Connection,
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
) -> Result<ProjectionPartitionSnapshot, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let Some(state) = load_partition_in_connection(connection, topology, partition).await? else {
        return Ok(ProjectionPartitionSnapshot {
            epoch: None,
            head: None,
            compacted_through: 0,
        });
    };
    let head = (state.change_head != 0)
        .then(|| {
            ProjectionChangeCursor::new(
                topology.clone(),
                partition.clone(),
                state.change_epoch.clone(),
                state.change_head,
            )
        })
        .transpose()?;
    Ok(ProjectionPartitionSnapshot {
        epoch: Some(state.change_epoch),
        head,
        compacted_through: state.compacted_through,
    })
}

/// Load the runtime fence and its immutable pending-retry identity from one
/// database statement. PostgreSQL's default READ COMMITTED isolation gives
/// each statement its own snapshot, so independent partition/failure selects
/// could otherwise report a mixed repair state.
pub(super) async fn load_partition_runtime_state<DB>(
    pool: &Pool<DB>,
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
) -> Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT partition.topology_bytes, partition.partition_bytes, \
         partition.active_generation, partition.change_epoch, partition.change_head, \
         partition.compacted_through, partition.pending_retry_failure_id, \
         partition.stopped_failure_id, failure.failure_id AS retry_failure_id, \
         failure.source_bytes AS retry_source_bytes, failure.source_hash AS retry_source_hash, \
         failure.source_partition_bytes AS retry_source_partition_bytes, \
         failure.source_partition_hash AS retry_source_partition_hash, \
         failure.source_epoch AS retry_source_epoch, \
         failure.source_position AS retry_source_position, \
         failure.input_hash AS retry_input_hash, failure.message_id AS retry_message_id, \
         failure.causation_id AS retry_causation_id, failure.gap_free AS retry_gap_free, \
         failure.generation AS retry_failed_generation \
         FROM projection_partitions partition LEFT JOIN projection_failures failure \
         ON failure.topology_hash = partition.topology_hash \
         AND failure.partition_hash = partition.partition_hash \
         AND failure.failure_id = partition.pending_retry_failure_id \
         WHERE partition.topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition.partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    let Some(row) = builder
        .build()
        .fetch_optional(pool)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("load projection partition runtime state", error)
        })?
    else {
        return Ok(None);
    };

    let state = decode_partition_row::<DB>(&row, topology, partition)?;
    let pending_retry = match &state.pending_retry_failure_id {
        Some(expected_failure_id) => {
            if state.stopped_failure_id.is_some() {
                return Err(corrupt_storage(
                    "projection partition is both stopped and pending retry",
                ));
            }
            let failure_id = row
                .try_get::<Option<String>, _>("retry_failure_id")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry failure ID", error)
                })?
                .ok_or_else(|| {
                    corrupt_storage(format!(
                        "pending projection retry failure `{expected_failure_id}` is missing"
                    ))
                })?;
            if &failure_id != expected_failure_id {
                return Err(corrupt_storage(
                    "pending retry join returned the wrong failure",
                ));
            }
            let source_bytes = row
                .try_get::<Option<Vec<u8>>, _>("retry_source_bytes")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry source bytes", error)
                })?
                .ok_or_else(|| corrupt_storage("pending retry source bytes are missing"))?;
            let source_hash = row
                .try_get::<Option<Vec<u8>>, _>("retry_source_hash")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry source hash", error)
                })?
                .ok_or_else(|| corrupt_storage("pending retry source hash is missing"))?;
            let source_partition_bytes = row
                .try_get::<Option<Vec<u8>>, _>("retry_source_partition_bytes")
                .map_err(|error| {
                    protocol_storage_error::<DB>(
                        "decode pending retry source partition bytes",
                        error,
                    )
                })?
                .ok_or_else(|| {
                    corrupt_storage("pending retry source partition bytes are missing")
                })?;
            let source_partition_hash = row
                .try_get::<Option<Vec<u8>>, _>("retry_source_partition_hash")
                .map_err(|error| {
                    protocol_storage_error::<DB>(
                        "decode pending retry source partition hash",
                        error,
                    )
                })?
                .ok_or_else(|| corrupt_storage("pending retry source partition hash is missing"))?;
            let source =
                ProjectionSource::from_canonical_name_bytes(&source_bytes, source_partition_bytes)?;
            verify_digest(
                &source_hash,
                source.digest(),
                "pending retry projection source",
            )?;
            verify_digest(
                &source_partition_hash,
                source.partition_digest(),
                "pending retry projection source partition",
            )?;
            let source_epoch = row
                .try_get::<Option<String>, _>("retry_source_epoch")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry source epoch", error)
                })?
                .ok_or_else(|| corrupt_storage("pending retry source epoch is missing"))?;
            let source_position = from_i64::<DB>(
                row.try_get::<Option<i64>, _>("retry_source_position")
                    .map_err(|error| {
                        protocol_storage_error::<DB>("decode pending retry source position", error)
                    })?
                    .ok_or_else(|| corrupt_storage("pending retry source position is missing"))?,
                "pending retry source position",
            )?;
            let input_fingerprint = ProjectionInputFingerprint::from_digest(decode_digest(
                row.try_get::<Option<Vec<u8>>, _>("retry_input_hash")
                    .map_err(|error| {
                        protocol_storage_error::<DB>("decode pending retry input hash", error)
                    })?
                    .ok_or_else(|| corrupt_storage("pending retry input hash is missing"))?,
                "pending retry input",
            )?);
            let message_id = row
                .try_get::<Option<String>, _>("retry_message_id")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry message ID", error)
                })?
                .ok_or_else(|| corrupt_storage("pending retry message ID is missing"))?;
            let causation_id = row
                .try_get::<Option<String>, _>("retry_causation_id")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry causation ID", error)
                })?
                .ok_or_else(|| corrupt_storage("pending retry causation ID is missing"))?;
            let gap_free = match row
                .try_get::<Option<i64>, _>("retry_gap_free")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode pending retry gap-free flag", error)
                })?
                .ok_or_else(|| corrupt_storage("pending retry gap-free flag is missing"))?
            {
                0 => false,
                1 => true,
                value => {
                    return Err(corrupt_storage(format!(
                        "pending retry gap-free flag contains invalid value {value}"
                    )))
                }
            };
            let failed_generation = ProjectionGeneration::new(from_i64::<DB>(
                row.try_get::<Option<i64>, _>("retry_failed_generation")
                    .map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode pending retry failed generation",
                            error,
                        )
                    })?
                    .ok_or_else(|| corrupt_storage("pending retry failed generation is missing"))?,
                "pending retry failed generation",
            )?)?;
            if failed_generation.checked_next()? != state.active_generation {
                return Err(corrupt_storage(format!(
                    "pending retry failure generation {} does not precede active generation {}",
                    failed_generation.get(),
                    state.active_generation.get()
                )));
            }
            Some(ProjectionPendingRetry {
                failure_id,
                input: ProjectionInputCursor::new(
                    topology.clone(),
                    partition.clone(),
                    source,
                    ProjectionEpoch::new(source_epoch)?,
                    source_position,
                )?,
                input_fingerprint,
                message_id,
                causation_id,
                failed_generation,
                gap_free,
            })
        }
        None => None,
    };

    Ok(Some(ProjectionPartitionRuntimeState {
        active_generation: state.active_generation,
        stopped_failure_id: state.stopped_failure_id,
        pending_retry,
    }))
}

pub(super) async fn verify_generation_exists_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &crate::projection_protocol::PartitionId,
    generation: ProjectionGeneration,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    if generation_exists_in_tx::<DB>(tx, topology, partition, generation).await? {
        Ok(())
    } else {
        Err(missing_generation(generation))
    }
}

fn missing_generation(generation: ProjectionGeneration) -> ProjectionProtocolError {
    corrupt_storage(format!(
        "active projection generation {} is missing",
        generation.get()
    ))
}

async fn generation_exists_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &crate::projection_protocol::PartitionId,
    generation: ProjectionGeneration,
) -> Result<bool, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let generation_value = to_i64::<DB>(generation.get(), "projection generation")?;
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder =
        QueryBuilder::<DB>::new("SELECT 1 FROM projection_generations WHERE topology_hash = ");
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND generation = ");
    builder.push_bind(generation_value);
    builder.push(" LIMIT 1");
    Ok(builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("verify projection generation", error))?
        .is_some())
}
