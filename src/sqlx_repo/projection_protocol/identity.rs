use super::*;

pub(super) fn ensure_active_input(
    state: &PartitionState,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError> {
    if state.active_generation != input.generation {
        return Err(ProjectionProtocolError::GenerationFenced {
            expected: state.active_generation.get(),
            actual: input.generation.get(),
        });
    }
    if let Some(failure_id) = &state.stopped_failure_id {
        return Err(ProjectionProtocolError::PartitionStopped {
            failure_id: failure_id.clone(),
        });
    }
    Ok(())
}

pub(super) fn verify_stored_change(
    state: &PartitionState,
    change: &ProjectionChangeCursor,
) -> Result<(), ProjectionProtocolError> {
    if change.epoch() != &state.change_epoch || change.position() > state.change_head {
        return Err(corrupt_storage(
            "stored projection outcome change is outside its partition head",
        ));
    }
    Ok(())
}

pub(super) fn checkpoint_from_stored(
    cursor_scope: &ProjectionInputCursor,
    source_epoch: ProjectionEpoch,
    source_position: u64,
    change: ProjectionChangeCursor,
    gap_free: bool,
) -> Result<ProjectionCheckpoint, ProjectionProtocolError> {
    ProjectionCheckpoint::new(
        ProjectionInputCursor::new(
            cursor_scope.topology().clone(),
            cursor_scope.projection_partition().clone(),
            cursor_scope.source().clone(),
            source_epoch,
            source_position,
        )?,
        change,
        gap_free,
    )
    .map_err(ProjectionProtocolError::from)
}

pub(super) fn decode_stored_receipt<DB>(
    row: &DB::Row,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
) -> Result<StoredReceipt, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let source_bytes: Vec<u8> = row
        .try_get("source_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt source bytes", error))?;
    let source_hash: Vec<u8> = row
        .try_get("source_hash")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt source hash", error))?;
    let source_partition_bytes: Vec<u8> =
        row.try_get("source_partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode receipt source partition bytes", error)
        })?;
    let source_partition_hash: Vec<u8> = row.try_get("source_partition_hash").map_err(|error| {
        protocol_storage_error::<DB>("decode receipt source partition hash", error)
    })?;
    let source =
        ProjectionSource::from_canonical_name_bytes(&source_bytes, source_partition_bytes.clone())?;
    verify_digest(&source_hash, source.digest(), "projection receipt source")?;
    verify_digest(
        &source_partition_hash,
        source.partition_digest(),
        "projection receipt source partition",
    )?;
    let source_epoch: String = row
        .try_get("source_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt source epoch", error))?;
    let source_epoch = ProjectionEpoch::new(source_epoch)?;
    let source_position = from_i64::<DB>(
        row.try_get("source_position").map_err(|error| {
            protocol_storage_error::<DB>("decode receipt source position", error)
        })?,
        "receipt source position",
    )?;
    let input_hash = decode_digest(
        row.try_get("input_hash")
            .map_err(|error| protocol_storage_error::<DB>("decode receipt input hash", error))?,
        "projection input",
    )?;
    let message_id = row
        .try_get("message_id")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt message ID", error))?;
    let causation_id = row
        .try_get("causation_id")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt causation ID", error))?;
    let gap_free = match row
        .try_get::<i64, _>("gap_free")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt gap-free flag", error))?
    {
        0 => false,
        1 => true,
        value => {
            return Err(corrupt_storage(format!(
                "receipt gap-free flag contains invalid value {value}"
            )))
        }
    };
    let outcome_kind: String = row
        .try_get("outcome_kind")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt outcome", error))?;
    if outcome_kind != "applied" && outcome_kind != "failed" {
        return Err(corrupt_storage(format!(
            "unknown projection receipt outcome `{outcome_kind}`"
        )));
    }
    let failure_id: Option<String> = row
        .try_get("failure_id")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt failure ID", error))?;
    if (outcome_kind == "applied") != failure_id.is_none() {
        return Err(corrupt_storage(
            "projection receipt outcome/failure shape is inconsistent",
        ));
    }
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode receipt change epoch", error))?;
    let change_position = from_i64::<DB>(
        row.try_get("change_position").map_err(|error| {
            protocol_storage_error::<DB>("decode receipt change position", error)
        })?,
        "receipt change position",
    )?;
    let change = ProjectionChangeCursor::new(
        topology.clone(),
        partition.clone(),
        ProjectionEpoch::new(change_epoch)?,
        change_position,
    )?;
    Ok(StoredReceipt {
        source_bytes,
        source_hash,
        source_partition_bytes,
        source_partition_hash,
        source_epoch,
        source_position,
        input_fingerprint: ProjectionInputFingerprint::from_digest(input_hash),
        message_id,
        causation_id,
        gap_free,
        outcome_kind,
        change,
    })
}

pub(super) fn receipt_matches_input(
    receipt: &StoredReceipt,
    input: &TrustedProjectionInput,
) -> bool {
    let source = input.cursor.source();
    receipt.source_bytes == source.canonical_name_bytes()
        && receipt.source_hash == digest_bytes(source.digest())
        && receipt.source_partition_bytes == source.canonical_partition_bytes()
        && receipt.source_partition_hash == digest_bytes(source.partition_digest())
        && receipt.source_epoch == *input.cursor.epoch()
        && receipt.source_position == input.cursor.position()
        && receipt.input_fingerprint == input.fingerprint
        && receipt.message_id == input.message_id
        && receipt.causation_id == input.causation_id
        && receipt.gap_free == input.gap_free
}

pub(super) fn decode_stored_input_identity<DB>(
    row: &DB::Row,
) -> Result<StoredInputIdentity, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let partition_bytes: Vec<u8> = row.try_get("partition_bytes").map_err(|error| {
        protocol_storage_error::<DB>("decode input identity partition bytes", error)
    })?;
    let partition_hash: Vec<u8> = row.try_get("partition_hash").map_err(|error| {
        protocol_storage_error::<DB>("decode input identity partition hash", error)
    })?;
    let decoded_partition = ProjectionPartition::new(partition_bytes.clone())?;
    verify_digest(
        &partition_hash,
        decoded_partition.digest(),
        "projection input identity partition",
    )?;
    let source_bytes: Vec<u8> = row.try_get("source_bytes").map_err(|error| {
        protocol_storage_error::<DB>("decode input identity source bytes", error)
    })?;
    let source_hash: Vec<u8> = row.try_get("source_hash").map_err(|error| {
        protocol_storage_error::<DB>("decode input identity source hash", error)
    })?;
    let source_partition_bytes: Vec<u8> =
        row.try_get("source_partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode input identity source partition bytes", error)
        })?;
    let source_partition_hash: Vec<u8> = row.try_get("source_partition_hash").map_err(|error| {
        protocol_storage_error::<DB>("decode input identity source partition hash", error)
    })?;
    let source =
        ProjectionSource::from_canonical_name_bytes(&source_bytes, source_partition_bytes.clone())?;
    verify_digest(
        &source_hash,
        source.digest(),
        "projection input identity source",
    )?;
    verify_digest(
        &source_partition_hash,
        source.partition_digest(),
        "projection input identity source partition",
    )?;
    let source_epoch: String = row
        .try_get("source_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode input identity epoch", error))?;
    let source_position = from_i64::<DB>(
        row.try_get("source_position").map_err(|error| {
            protocol_storage_error::<DB>("decode input identity position", error)
        })?,
        "projection input identity position",
    )?;
    let input_hash = decode_digest(
        row.try_get("input_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode input identity fingerprint", error)
        })?,
        "projection input identity",
    )?;
    let gap_free = match row
        .try_get::<i64, _>("gap_free")
        .map_err(|error| protocol_storage_error::<DB>("decode input identity gap flag", error))?
    {
        0 => false,
        1 => true,
        value => {
            return Err(corrupt_storage(format!(
                "input identity gap-free flag contains invalid value {value}"
            )))
        }
    };
    Ok(StoredInputIdentity {
        partition_bytes,
        partition_hash,
        source_bytes,
        source_hash,
        source_partition_bytes,
        source_partition_hash,
        source_epoch: ProjectionEpoch::new(source_epoch)?,
        source_position,
        input_fingerprint: ProjectionInputFingerprint::from_digest(input_hash),
        message_id: row.try_get("message_id").map_err(|error| {
            protocol_storage_error::<DB>("decode input identity message ID", error)
        })?,
        causation_id: row.try_get("causation_id").map_err(|error| {
            protocol_storage_error::<DB>("decode input identity causation ID", error)
        })?,
        gap_free,
    })
}

pub(super) fn input_identity_cursor_matches(
    identity: &StoredInputIdentity,
    input: &TrustedProjectionInput,
) -> bool {
    input_identity_scope_matches(identity, input)
        && identity.source_position == input.cursor.position()
}

fn input_identity_scope_matches(
    identity: &StoredInputIdentity,
    input: &TrustedProjectionInput,
) -> bool {
    let source = input.cursor.source();
    identity.partition_bytes == input.cursor.projection_partition().canonical_bytes()
        && identity.partition_hash == digest_bytes(input.cursor.projection_partition().digest())
        && identity.source_bytes == source.canonical_name_bytes()
        && identity.source_hash == digest_bytes(source.digest())
        && identity.source_partition_bytes == source.canonical_partition_bytes()
        && identity.source_partition_hash == digest_bytes(source.partition_digest())
        && identity.source_epoch == *input.cursor.epoch()
}

pub(super) fn input_identity_matches(
    identity: &StoredInputIdentity,
    input: &TrustedProjectionInput,
) -> bool {
    input_identity_cursor_matches(identity, input)
        && identity.input_fingerprint == input.fingerprint
        && identity.message_id == input.message_id
        && identity.causation_id == input.causation_id
        && identity.gap_free == input.gap_free
}

pub(super) async fn input_identity_by_cursor_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<Option<StoredInputIdentity>, ProjectionProtocolError>
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
    let topology_hash = input.cursor.topology().digest();
    let partition_hash = input.cursor.projection_partition().digest();
    let source_hash = input.cursor.source().digest();
    let source_partition_hash = input.cursor.source().partition_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT partition.partition_bytes, identity.partition_hash, identity.source_bytes, \
         identity.source_hash, identity.source_partition_bytes, identity.source_partition_hash, \
         identity.source_epoch, identity.source_position, identity.input_hash, \
         identity.message_id, identity.causation_id, identity.gap_free \
         FROM (SELECT * FROM projection_input_identities UNION ALL \
         SELECT canonical.topology_hash, alias.partition_hash, canonical.source_bytes, \
         alias.source_hash, canonical.source_partition_bytes, alias.source_partition_hash, \
         alias.source_epoch, alias.source_position, canonical.input_hash, canonical.message_id, \
         canonical.causation_id, canonical.gap_free \
         FROM projection_input_delivery_aliases alias JOIN projection_input_identities canonical \
         ON canonical.topology_hash = alias.topology_hash AND canonical.message_id = alias.message_id) identity \
         JOIN projection_partitions partition \
         ON partition.topology_hash = identity.topology_hash \
         AND partition.partition_hash = identity.partition_hash \
         WHERE identity.topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND identity.partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND identity.source_hash = ");
    builder.push_bind(source_hash.as_slice());
    builder.push(" AND identity.source_partition_hash = ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(" AND identity.source_epoch = ");
    builder.push_bind(input.cursor.epoch().as_str());
    builder.push(" AND identity.source_position = ");
    builder.push_bind(to_i64::<DB>(
        input.cursor.position(),
        "projection input identity position",
    )?);
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("load projection input identity by cursor", error)
        })?;
    row.map(|row| decode_stored_input_identity::<DB>(&row))
        .transpose()
}

pub(super) async fn input_identity_by_message_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<Option<StoredInputIdentity>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = input.cursor.topology().digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT partition.partition_bytes, identity.partition_hash, identity.source_bytes, \
         identity.source_hash, identity.source_partition_bytes, identity.source_partition_hash, \
         identity.source_epoch, identity.source_position, identity.input_hash, \
         identity.message_id, identity.causation_id, identity.gap_free \
         FROM projection_input_identities identity JOIN projection_partitions partition \
         ON partition.topology_hash = identity.topology_hash \
         AND partition.partition_hash = identity.partition_hash \
         WHERE identity.topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND identity.message_id = ");
    builder.push_bind(input.message_id.as_str());
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("load projection input identity by message", error)
        })?;
    row.map(|row| decode_stored_input_identity::<DB>(&row))
        .transpose()
}

pub(super) async fn receipt_by_message_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<Option<StoredReceipt>, ProjectionProtocolError>
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
    let generation = to_i64::<DB>(input.generation.get(), "projection generation")?;
    let topology_hash = input.cursor.topology().digest();
    let partition_hash = input.cursor.projection_partition().digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT source_bytes, source_hash, source_partition_bytes, source_partition_hash, \
         source_epoch, source_position, input_hash, message_id, causation_id, outcome_kind, failure_id, \
         gap_free, change_epoch, change_position FROM projection_input_receipts WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND generation = ");
    builder.push_bind(generation);
    builder.push(" AND message_id = ");
    builder.push_bind(input.message_id.as_str());
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection message receipt", error))?;
    row.map(|row| {
        decode_stored_receipt::<DB>(
            &row,
            input.cursor.topology(),
            input.cursor.projection_partition(),
        )
    })
    .transpose()
}

pub(super) async fn receipt_by_cursor_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<Option<StoredReceipt>, ProjectionProtocolError>
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
    let generation = to_i64::<DB>(input.generation.get(), "projection generation")?;
    let position = to_i64::<DB>(input.cursor.position(), "projection input position")?;
    let topology_hash = input.cursor.topology().digest();
    let partition_hash = input.cursor.projection_partition().digest();
    let source_hash = input.cursor.source().digest();
    let source_partition_hash = input.cursor.source().partition_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT source_bytes, source_hash, source_partition_bytes, source_partition_hash, \
         source_epoch, source_position, input_hash, message_id, causation_id, outcome_kind, failure_id, \
         gap_free, change_epoch, change_position FROM projection_input_receipts WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND generation = ");
    builder.push_bind(generation);
    builder.push(" AND source_hash = ");
    builder.push_bind(source_hash.as_slice());
    builder.push(" AND source_partition_hash = ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(" AND source_epoch = ");
    builder.push_bind(input.cursor.epoch().as_str());
    builder.push(" AND source_position = ");
    builder.push_bind(position);
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection cursor receipt", error))?;
    row.map(|row| {
        decode_stored_receipt::<DB>(
            &row,
            input.cursor.topology(),
            input.cursor.projection_partition(),
        )
    })
    .transpose()
}

pub(super) async fn current_input_cursor_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<Option<StoredCursor>, ProjectionProtocolError>
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
    let generation = to_i64::<DB>(input.generation.get(), "projection generation")?;
    let topology_hash = input.cursor.topology().digest();
    let partition_hash = input.cursor.projection_partition().digest();
    let source_hash = input.cursor.source().digest();
    let source_partition_hash = input.cursor.source().partition_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT source_bytes, source_hash, source_partition_bytes, source_partition_hash, \
         source_epoch, source_position, input_hash, message_id, causation_id, \
         gap_free, change_epoch, change_position FROM projection_input_cursors WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND source_hash = ");
    builder.push_bind(source_hash.as_slice());
    builder.push(" AND source_partition_hash = ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(" AND generation = ");
    builder.push_bind(generation);
    let Some(row) = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection input cursor", error))?
    else {
        return Ok(None);
    };

    let source_bytes: Vec<u8> = row
        .try_get("source_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode cursor source bytes", error))?;
    let source_digest: Vec<u8> = row
        .try_get("source_hash")
        .map_err(|error| protocol_storage_error::<DB>("decode cursor source hash", error))?;
    let source_partition_bytes: Vec<u8> =
        row.try_get("source_partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode cursor source partition bytes", error)
        })?;
    let source_partition_digest: Vec<u8> =
        row.try_get("source_partition_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode cursor source partition hash", error)
        })?;
    verify_bytes(
        &source_bytes,
        &input.cursor.source().canonical_name_bytes(),
        "projection cursor source",
    )?;
    verify_digest(
        &source_digest,
        input.cursor.source().digest(),
        "projection cursor source",
    )?;
    verify_bytes(
        &source_partition_bytes,
        input.cursor.source().canonical_partition_bytes(),
        "projection cursor source partition",
    )?;
    verify_digest(
        &source_partition_digest,
        input.cursor.source().partition_digest(),
        "projection cursor source partition",
    )?;
    let source_epoch: String = row
        .try_get("source_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode cursor source epoch", error))?;
    let source_position = from_i64::<DB>(
        row.try_get("source_position").map_err(|error| {
            protocol_storage_error::<DB>("decode cursor source position", error)
        })?,
        "projection input position",
    )?;
    let input_hash = decode_digest(
        row.try_get("input_hash")
            .map_err(|error| protocol_storage_error::<DB>("decode cursor input hash", error))?,
        "projection input",
    )?;
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode cursor change epoch", error))?;
    let change_position = from_i64::<DB>(
        row.try_get("change_position").map_err(|error| {
            protocol_storage_error::<DB>("decode cursor change position", error)
        })?,
        "projection change position",
    )?;
    Ok(Some(StoredCursor {
        source_epoch: ProjectionEpoch::new(source_epoch)?,
        source_position,
        input_fingerprint: ProjectionInputFingerprint::from_digest(input_hash),
        message_id: row
            .try_get("message_id")
            .map_err(|error| protocol_storage_error::<DB>("decode cursor message ID", error))?,
        causation_id: row
            .try_get("causation_id")
            .map_err(|error| protocol_storage_error::<DB>("decode cursor causation ID", error))?,
        gap_free: match row
            .try_get::<i64, _>("gap_free")
            .map_err(|error| protocol_storage_error::<DB>("decode cursor gap-free flag", error))?
        {
            0 => false,
            1 => true,
            value => {
                return Err(corrupt_storage(format!(
                    "cursor gap-free flag contains invalid value {value}"
                )))
            }
        },
        change: ProjectionChangeCursor::new(
            input.cursor.topology().clone(),
            input.cursor.projection_partition().clone(),
            ProjectionEpoch::new(change_epoch)?,
            change_position,
        )?,
    }))
}

pub(super) fn stored_cursors_match(left: &StoredCursor, right: &StoredCursor) -> bool {
    left.source_epoch == right.source_epoch
        && left.source_position == right.source_position
        && left.input_fingerprint == right.input_fingerprint
        && left.message_id == right.message_id
        && left.causation_id == right.causation_id
        && left.gap_free == right.gap_free
        && left.change == right.change
}

pub(super) async fn verify_inherited_cursor_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
    current: &StoredCursor,
) -> Result<bool, ProjectionProtocolError>
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
    let topology_hash = input.cursor.topology().digest();
    let partition_hash = input.cursor.projection_partition().digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT retry_of_generation FROM projection_generations WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND generation = ");
    builder.push_bind(to_i64::<DB>(
        input.generation.get(),
        "projection generation",
    )?);
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("load projection repair generation lineage", error)
        })?
        .ok_or_else(|| {
            corrupt_storage(format!(
                "active projection generation {} is missing",
                input.generation.get()
            ))
        })?;
    let parent: Option<i64> = row.try_get("retry_of_generation").map_err(|error| {
        protocol_storage_error::<DB>("decode projection repair generation lineage", error)
    })?;
    let Some(parent) = parent else {
        return Ok(false);
    };
    let parent = ProjectionGeneration::new(from_i64::<DB>(
        parent,
        "projection repair parent generation",
    )?)?;
    let mut parent_input = input.clone();
    parent_input.generation = parent;
    let parent_cursor = current_input_cursor_in_tx(tx, &parent_input)
        .await?
        .ok_or_else(|| {
            corrupt_storage("projection repair generation contains a cursor absent from its parent")
        })?;
    if !stored_cursors_match(current, &parent_cursor) {
        return Err(corrupt_storage(
            "projection repair generation contains a cursor changed from its parent",
        ));
    }
    Ok(true)
}

pub(super) async fn validate_source_capability_in_tx_mode<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
    register_missing: bool,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = input.cursor.topology().digest();
    let partition_hash = input.cursor.projection_partition().digest();
    let source_hash = input.cursor.source().digest();
    let source_partition_hash = input.cursor.source().partition_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT source_bytes, source_hash, source_partition_bytes, source_partition_hash, gap_free \
         FROM projection_source_capabilities WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND source_hash = ");
    builder.push_bind(source_hash.as_slice());
    builder.push(" AND source_partition_hash = ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(" LIMIT 1");
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("load projection source capability", error)
        })?;
    let Some(row) = row else {
        if !register_missing {
            return Ok(());
        }
        let source = input.cursor.source();
        let source_bytes = source.canonical_name_bytes();
        let source_hash = source.digest();
        let source_partition_hash = source.partition_digest();
        let mut insert = QueryBuilder::<DB>::new(
            "INSERT INTO projection_source_capabilities \
             (topology_hash, partition_hash, source_bytes, source_hash, source_partition_bytes, \
             source_partition_hash, gap_free) VALUES (",
        );
        insert.push_bind(topology_hash.as_slice());
        insert.push(", ");
        insert.push_bind(partition_hash.as_slice());
        insert.push(", ");
        insert.push_bind(source_bytes.as_slice());
        insert.push(", ");
        insert.push_bind(source_hash.as_slice());
        insert.push(", ");
        insert.push_bind(source.canonical_partition_bytes());
        insert.push(", ");
        insert.push_bind(source_partition_hash.as_slice());
        insert.push(", ");
        insert.push_bind(i64::from(input.gap_free));
        insert.push(
            ") ON CONFLICT \
             (topology_hash, partition_hash, source_hash, source_partition_hash) DO NOTHING",
        );
        let result = insert.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("register projection source capability", error)
        })?;
        if DB::rows_affected(&result) != 1 {
            return Err(corrupt_storage(
                "projection source capability changed while its partition lock was held",
            ));
        }
        return Ok(());
    };
    let source_bytes: Vec<u8> = row
        .try_get("source_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode capability source bytes", error))?;
    let source_digest: Vec<u8> = row
        .try_get("source_hash")
        .map_err(|error| protocol_storage_error::<DB>("decode capability source hash", error))?;
    let source_partition_bytes: Vec<u8> =
        row.try_get("source_partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode capability source partition bytes", error)
        })?;
    let source_partition_digest: Vec<u8> =
        row.try_get("source_partition_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode capability source partition hash", error)
        })?;
    verify_bytes(
        &source_bytes,
        &input.cursor.source().canonical_name_bytes(),
        "projection capability source",
    )?;
    verify_digest(
        &source_digest,
        input.cursor.source().digest(),
        "projection capability source",
    )?;
    verify_bytes(
        &source_partition_bytes,
        input.cursor.source().canonical_partition_bytes(),
        "projection capability source partition",
    )?;
    verify_digest(
        &source_partition_digest,
        input.cursor.source().partition_digest(),
        "projection capability source partition",
    )?;
    let gap_free = match row.try_get::<i64, _>("gap_free").map_err(|error| {
        protocol_storage_error::<DB>("decode projection source capability", error)
    })? {
        0 => false,
        1 => true,
        value => {
            return Err(corrupt_storage(format!(
                "source capability gap-free flag contains invalid value {value}"
            )))
        }
    };
    if gap_free != input.gap_free {
        return Err(ProjectionProtocolError::InputCorruption);
    }
    Ok(())
}

pub(super) async fn validate_input_identity_in_tx_mode<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
    register_missing_source_capability: bool,
) -> Result<(), ProjectionProtocolError>
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
    validate_source_capability_in_tx_mode(tx, input, register_missing_source_capability).await?;
    let exact_identity = input_identity_by_cursor_in_tx(tx, input).await?;
    if let Some(identity) = &exact_identity {
        if !input_identity_cursor_matches(identity, input) {
            return Err(corrupt_storage(
                "projection input identity hash lookup resolved different canonical source bytes",
            ));
        }
        if !input_identity_matches(identity, input) {
            return Err(ProjectionProtocolError::InputCorruption);
        }
    }
    if let Some(identity) = input_identity_by_message_in_tx(tx, input).await? {
        if !input_identity_scope_matches(&identity, input) {
            return Err(ProjectionProtocolError::MessageIdReuse {
                message_id: input.message_id.clone(),
            });
        }
        if identity.input_fingerprint != input.fingerprint
            || identity.causation_id != input.causation_id
            || identity.gap_free != input.gap_free
        {
            return Err(ProjectionProtocolError::MessageIdReuse {
                message_id: input.message_id.clone(),
            });
        }
        if exact_identity.is_none() && identity.source_position == input.cursor.position() {
            return Err(corrupt_storage(
                "projection message identity exists without its exact cursor identity",
            ));
        }
    }
    Ok(())
}

pub(super) async fn validate_input_identity_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError>
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
    validate_input_identity_in_tx_mode(tx, input, true).await
}

pub(super) async fn validate_input_identity_read_only_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError>
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
    validate_input_identity_in_tx_mode(tx, input, false).await
}

pub(super) async fn classify_validated_input_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
    state: &PartitionState,
) -> Result<InputDisposition, ProjectionProtocolError>
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
    if let Some(receipt) = receipt_by_cursor_in_tx(tx, input).await? {
        verify_stored_change(state, &receipt.change)?;
        if receipt.source_bytes != input.cursor.source().canonical_name_bytes()
            || receipt.source_hash != digest_bytes(input.cursor.source().digest())
            || receipt.source_partition_bytes != input.cursor.source().canonical_partition_bytes()
            || receipt.source_partition_hash
                != digest_bytes(input.cursor.source().partition_digest())
        {
            return Err(corrupt_storage(
                "projection cursor receipt hash lookup resolved different canonical bytes",
            ));
        }
        if receipt.input_fingerprint != input.fingerprint
            || receipt.message_id != input.message_id
            || receipt.causation_id != input.causation_id
            || receipt.gap_free != input.gap_free
        {
            return Err(ProjectionProtocolError::InputCorruption);
        }
        if receipt.outcome_kind != "applied" {
            return Err(corrupt_storage(
                "failed cursor receipt exists without a stopped partition",
            ));
        }
        return Ok(InputDisposition::Duplicate(checkpoint_from_stored(
            &input.cursor,
            receipt.source_epoch,
            receipt.source_position,
            receipt.change,
            receipt.gap_free,
        )?));
    }

    let repeated_effect = if let Some(receipt) = receipt_by_message_in_tx(tx, input).await? {
        verify_stored_change(state, &receipt.change)?;
        let mut original_input = input.clone();
        original_input.cursor = ProjectionInputCursor::new(
            input.cursor.topology().clone(),
            input.cursor.projection_partition().clone(),
            input.cursor.source().clone(),
            input.cursor.epoch().clone(),
            receipt.source_position,
        )?;
        if !receipt_matches_input(&receipt, &original_input) {
            return Err(ProjectionProtocolError::MessageIdReuse {
                message_id: input.message_id.clone(),
            });
        }
        if receipt.outcome_kind != "applied" {
            return Err(corrupt_storage(
                "failed input receipt exists without a stopped partition",
            ));
        }
        true
    } else {
        false
    };

    let Some(previous) = current_input_cursor_in_tx(tx, input).await? else {
        return Ok(if repeated_effect {
            InputDisposition::Redelivery
        } else {
            InputDisposition::New
        });
    };
    verify_stored_change(state, &previous.change)?;
    if previous.gap_free != input.gap_free {
        return Err(ProjectionProtocolError::InputCorruption);
    }
    if previous.source_epoch != *input.cursor.epoch() {
        return Err(ProjectionProtocolError::IncomparableInput);
    }
    if input.cursor.position() < previous.source_position {
        return Ok(InputDisposition::Stale(checkpoint_from_stored(
            &input.cursor,
            previous.source_epoch,
            previous.source_position,
            previous.change,
            previous.gap_free,
        )?));
    }
    if input.cursor.position() == previous.source_position {
        if previous.input_fingerprint != input.fingerprint
            || previous.message_id != input.message_id
            || previous.causation_id != input.causation_id
            || previous.gap_free != input.gap_free
        {
            return Err(ProjectionProtocolError::InputCorruption);
        }
        if !repeated_effect && !verify_inherited_cursor_in_tx(tx, input, &previous).await? {
            return Err(corrupt_storage(
                "projection input cursor has no receipt and was not inherited by repair",
            ));
        }
        return Ok(InputDisposition::Duplicate(checkpoint_from_stored(
            &input.cursor,
            previous.source_epoch,
            previous.source_position,
            previous.change,
            previous.gap_free,
        )?));
    }
    if input.gap_free
        && input.cursor.position()
            != checked_next(previous.source_position, "gap-free projection input")?
    {
        return Err(ProjectionProtocolError::IncomparableInput);
    }
    Ok(if repeated_effect {
        InputDisposition::Redelivery
    } else {
        InputDisposition::New
    })
}
