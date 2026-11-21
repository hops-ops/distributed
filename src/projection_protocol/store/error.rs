use super::*;

#[derive(Debug)]
#[non_exhaustive]
pub enum ProjectionProtocolError {
    Validation(ProjectionProtocolValidationError),
    Repository(RepositoryError),
    Table(TableStoreError),
    /// The caller's batch or request is not acceptable.
    InvalidBatch(String),
    /// Stored protocol data contradicts itself. Never caused by a caller.
    StorageCorruption(String),
    ScopeMismatch {
        field: &'static str,
    },
    IncomparableInput,
    InputCorruption,
    MessageIdReuse {
        message_id: String,
    },
    GenerationFenced {
        expected: u64,
        actual: u64,
    },
    PartitionStopped {
        failure_id: String,
    },
    RecordMissing {
        model: String,
    },
    RecordAlreadyExists {
        model: String,
    },
    RecordRevisionConflict {
        model: String,
        expected_incarnation: u64,
        expected_revision: u64,
        actual_incarnation: u64,
        actual_revision: u64,
    },
    RecordTombstoned {
        model: String,
    },
    RecreateRequiresTombstone {
        model: String,
    },
    CausalWriteRequired {
        table: String,
    },
    PositionOverflow {
        domain: &'static str,
    },
}

impl fmt::Display for ProjectionProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(error) => write!(formatter, "invalid projection protocol value: {error}"),
            Self::Repository(error) => write!(formatter, "projection repository error: {error}"),
            Self::Table(error) => write!(formatter, "projection table error: {error}"),
            Self::InvalidBatch(message) => write!(formatter, "invalid projection batch: {message}"),
            Self::StorageCorruption(message) => write!(formatter, "corrupt projection protocol storage: {message}"),
            Self::ScopeMismatch { field } => write!(formatter, "{field} does not match the projection input scope"),
            Self::IncomparableInput => formatter.write_str("projection input is incomparable with the durable checkpoint"),
            Self::InputCorruption => formatter.write_str("the same projection input cursor was reused with different content"),
            Self::MessageIdReuse { message_id } => write!(formatter, "projection message ID `{message_id}` was reused for a different input"),
            Self::GenerationFenced { expected, actual } => write!(formatter, "projection generation {actual} is fenced; active generation is {expected}"),
            Self::PartitionStopped { failure_id } => write!(formatter, "projection partition is stopped by terminal failure `{failure_id}`"),
            Self::RecordMissing { model } => write!(formatter, "projection record `{model}` does not exist"),
            Self::RecordAlreadyExists { model } => write!(formatter, "projection record `{model}` already exists"),
            Self::RecordRevisionConflict { model, expected_incarnation, expected_revision, actual_incarnation, actual_revision } => write!(formatter, "projection record `{model}` expected revision ({expected_incarnation}, {expected_revision}) but found ({actual_incarnation}, {actual_revision})"),
            Self::RecordTombstoned { model } => write!(formatter, "projection record `{model}` is tombstoned; use explicit recreate"),
            Self::RecreateRequiresTombstone { model } => write!(formatter, "projection record `{model}` can only be recreated from its exact tombstone revision"),
            Self::CausalWriteRequired { table } => write!(formatter, "table `{table}` is causal-owned and requires the projection commit path"),
            Self::PositionOverflow { domain } => write!(formatter, "{domain} position overflow"),
        }
    }
}

impl std::error::Error for ProjectionProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Validation(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::Table(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ProjectionProtocolValidationError> for ProjectionProtocolError {
    fn from(error: ProjectionProtocolValidationError) -> Self {
        Self::Validation(error)
    }
}

impl From<RepositoryError> for ProjectionProtocolError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

impl From<TableStoreError> for ProjectionProtocolError {
    fn from(error: TableStoreError) -> Self {
        Self::Table(error)
    }
}
