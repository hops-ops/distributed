use super::*;

pub(super) fn protocol_storage_error<DB: SqlxRepoBackend>(
    operation: &str,
    error: sqlx::Error,
) -> ProjectionProtocolError {
    ProjectionProtocolError::Repository(repository_storage_error::<DB>(operation, error))
}

pub(super) fn corrupt_storage(message: impl Into<String>) -> ProjectionProtocolError {
    ProjectionProtocolError::InvalidBatch(format!(
        "corrupt projection protocol storage: {}",
        message.into()
    ))
}

pub(super) fn to_i64<DB: SqlxRepoBackend>(
    value: u64,
    field: &'static str,
) -> Result<i64, ProjectionProtocolError> {
    i64::try_from(value).map_err(|_| {
        ProjectionProtocolError::Repository(RepositoryError::Model(format!(
            "{} {field} value {value} exceeds signed bigint storage",
            DB::BACKEND
        )))
    })
}

pub(super) fn from_i64<DB: SqlxRepoBackend>(
    value: i64,
    field: &'static str,
) -> Result<u64, ProjectionProtocolError> {
    u64::try_from(value)
        .map_err(|_| corrupt_storage(format!("{} {field} value {value} is negative", DB::BACKEND)))
}

pub(super) fn digest_bytes(value: [u8; 32]) -> Vec<u8> {
    value.to_vec()
}

pub(super) fn decode_digest(
    value: Vec<u8>,
    field: &'static str,
) -> Result<[u8; 32], ProjectionProtocolError> {
    value.try_into().map_err(|value: Vec<u8>| {
        corrupt_storage(format!(
            "{field} digest contains {} bytes instead of 32",
            value.len()
        ))
    })
}

pub(super) fn verify_bytes(
    actual: &[u8],
    expected: &[u8],
    field: &'static str,
) -> Result<(), ProjectionProtocolError> {
    if actual == expected {
        Ok(())
    } else {
        Err(corrupt_storage(format!(
            "{field} bytes do not match their hash lookup"
        )))
    }
}

pub(super) fn verify_digest(
    actual: &[u8],
    expected: [u8; 32],
    field: &'static str,
) -> Result<(), ProjectionProtocolError> {
    verify_bytes(actual, &expected, field)
}

pub(super) async fn physical_row_exists_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    mutation: &TableMutation,
) -> Result<bool, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let (schema, key) = match mutation {
        TableMutation::UpsertRow(mutation) => (mutation.schema, &mutation.key),
        TableMutation::PatchRow(mutation) => (mutation.schema, &mutation.key),
        TableMutation::DeleteRow(mutation) => (mutation.schema, &mutation.key),
    };
    Ok(row_version_in_tx(tx, schema, key).await?.is_some())
}

pub(super) fn decode_change_kind(
    value: &str,
) -> Result<ProjectionChangeKind, ProjectionProtocolError> {
    ProjectionChangeKind::from_storage_str(value)
        .ok_or_else(|| corrupt_storage(format!("unknown projection change kind `{value}`")))
}

pub(super) fn decode_observation_kind(
    value: &str,
) -> Result<ProjectionObservationKind, ProjectionProtocolError> {
    ProjectionObservationKind::from_storage_str(value)
        .ok_or_else(|| corrupt_storage(format!("unknown projection observation kind `{value}`")))
}

pub(super) fn decode_program_id(
    value: Option<String>,
) -> Result<Option<crate::ProjectionProgramId>, ProjectionProtocolError> {
    value
        .map(|value| {
            crate::ProjectionProgramId::parse(&value).map_err(|error| {
                corrupt_storage(format!(
                    "invalid stored projection program identity: {error}"
                ))
            })
        })
        .transpose()
}
