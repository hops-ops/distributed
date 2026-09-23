//! SQL event-store operations independent of the connection runtime.
//!
//! SQLx and Durable Object SQL execute the same statements and decode the same
//! event/snapshot records. Only binding, row access, and transaction ownership
//! belong to the execution adapter. This module never opens a transaction: a
//! command's events, snapshots, ledger, and outbox must use the caller's one
//! transaction, not an independent transaction per participant.

use std::borrow::Cow;
use std::future::Future;
use std::time::{Duration, SystemTime};

pub(crate) mod ledger;
pub(crate) mod outbox;

use crate::entity::{Entity, EventRecord, BITCODE_PAYLOAD_CODEC};
use crate::snapshot::SnapshotRecord;

use super::{validate_snapshot_identity, PreparedEventAppend, RepositoryError, StreamIdentity};

#[derive(Clone, Debug)]
pub(crate) enum SqlBind<'a> {
    Text(String),
    Integer(i64),
    Bytes(Cow<'a, [u8]>),
    Metadata(String),
    Timestamp(SystemTime),
}

impl From<&str> for SqlBind<'_> {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}
impl From<i64> for SqlBind<'_> {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl<'a> From<&'a [u8]> for SqlBind<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Bytes(Cow::Borrowed(value))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SqlPart<'a> {
    Sql(String),
    Bind(SqlBind<'a>),
    LedgerNow,
    LedgerNowEpoch,
    LedgerDeadline(Duration),
    LedgerDeadlineIsLive(SystemTime),
    LedgerJson(String),
    TimestampCompare {
        column: &'static str,
        operator: &'static str,
        value: SystemTime,
    },
}

/// Structural statements: values are never interpolated into SQL text. The
/// executor renders placeholders for its dialect; it never splits SQL on `?`.
#[derive(Clone, Debug)]
pub(crate) struct Statement<'a>(pub Vec<SqlPart<'a>>);

impl<'a> Statement<'a> {
    pub fn push(&mut self, sql: impl Into<String>) {
        self.0.push(SqlPart::Sql(sql.into()));
    }

    pub fn push_bind(&mut self, value: impl Into<SqlBind<'a>>) {
        self.0.push(SqlPart::Bind(value.into()));
    }

    pub fn part(&mut self, part: SqlPart<'a>) {
        self.0.push(part);
    }
    pub fn new(sql: impl Into<String>) -> Self {
        Self(vec![SqlPart::Sql(sql.into())])
    }

    pub fn sql(mut self, sql: impl Into<String>) -> Self {
        self.0.push(SqlPart::Sql(sql.into()));
        self
    }

    pub fn bind(mut self, value: SqlBind<'a>) -> Self {
        self.0.push(SqlPart::Bind(value));
        self
    }

    fn identity(self, identity: &StreamIdentity) -> Self {
        self.bind(SqlBind::Text(identity.aggregate_type().into()))
            .sql(" AND aggregate_id = ")
            .bind(SqlBind::Text(identity.aggregate_id().into()))
    }
}

pub(crate) trait SqlRow: Send {
    fn text(&self, column: &'static str) -> Result<String, RepositoryError>;
    fn optional_text(&self, column: &'static str) -> Result<Option<String>, RepositoryError>;
    fn integer(&self, column: &'static str) -> Result<i64, RepositoryError>;
    fn optional_integer(&self, column: &'static str) -> Result<Option<i64>, RepositoryError>;
    fn bytes(&self, column: &'static str) -> Result<Vec<u8>, RepositoryError>;
    fn timestamp(&self, column: &'static str) -> Result<SystemTime, RepositoryError>;
    fn optional_timestamp(
        &self,
        column: &'static str,
    ) -> Result<Option<SystemTime>, RepositoryError>;
}

pub(crate) trait SqlExecutor: Send {
    type Row: SqlRow;
    const EVENT_SELECT: &'static str;
    const SNAPSHOT_SELECT: &'static str;
    const OUTBOX_SELECT: &'static str;
    const NOW: &'static str;
    const COMMAND_LEDGER_SELECT: &'static str;
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str;
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str;

    fn query(
        &mut self,
        statement: Statement<'_>,
    ) -> impl Future<Output = Result<Vec<Self::Row>, RepositoryError>> + Send;
    fn execute(
        &mut self,
        statement: Statement<'_>,
    ) -> impl Future<Output = Result<u64, RepositoryError>> + Send;
}

pub(crate) struct EventInsert<'a> {
    pub statement: Statement<'a>,
    pub candidates: Vec<(StreamIdentity, u64)>,
}

/// Prepare only new events, preserving the existing ten-column schema and
/// bound-parameter chunking. Conflict candidates are scoped to each statement:
/// earlier chunks in the same transaction must not skew conflict diagnostics.
pub(crate) fn event_inserts<'a>(
    appends: &[PreparedEventAppend<'a>],
    max_bind_params: usize,
) -> Result<Vec<EventInsert<'a>>, RepositoryError> {
    const COLUMNS: usize = 10;
    if max_bind_params < COLUMNS {
        return Err(RepositoryError::Model(
            "SQL executor cannot bind one event row".into(),
        ));
    }
    let mut inserts = Vec::new();
    let mut current: Option<EventInsert<'a>> = None;
    let mut count = 0;
    for append in appends {
        for event in append.events {
            let insert = current.get_or_insert_with(|| EventInsert {
                statement: Statement::new("INSERT INTO aggregate_events (aggregate_type, aggregate_id, sequence, event_name, event_version, payload, payload_codec, payload_codec_version, metadata, recorded_at) VALUES "),
                candidates: Vec::new(),
            });
            if count != 0 {
                insert.statement.0.push(SqlPart::Sql(", ".into()));
            }
            insert.statement.0.push(SqlPart::Sql("(".into()));
            let values = [
                SqlBind::Text(append.identity.aggregate_type().into()),
                SqlBind::Text(append.identity.aggregate_id().into()),
                SqlBind::Integer(signed(event.sequence, "sequence")?),
                SqlBind::Text(event.event_name.clone()),
                SqlBind::Integer(signed(event.event_version, "event version")?),
                SqlBind::Bytes(Cow::Borrowed(&event.payload)),
                SqlBind::Text(event.payload_codec.to_string()),
                SqlBind::Integer(i64::from(event.payload_codec_version)),
                SqlBind::Metadata(serde_json::to_string(&event.metadata).map_err(json_error)?),
                SqlBind::Timestamp(event.timestamp),
            ];
            for (index, value) in values.into_iter().enumerate() {
                if index != 0 {
                    insert.statement.0.push(SqlPart::Sql(", ".into()));
                }
                insert.statement.0.push(SqlPart::Bind(value));
            }
            insert.statement.0.push(SqlPart::Sql(")".into()));
            if !insert
                .candidates
                .iter()
                .any(|(id, _)| id == &append.identity)
            {
                insert
                    .candidates
                    .push((append.identity.clone(), append.expected_version));
            }
            count += 1;
            if count == max_bind_params / COLUMNS {
                inserts.push(current.take().expect("event insert initialized"));
                count = 0;
            }
        }
    }
    if let Some(insert) = current {
        inserts.push(insert);
    }
    Ok(inserts)
}

fn signed(value: u64, field: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::Model(format!("{field} exceeds SQL signed integer storage")))
}

fn unsigned(value: i64, field: &str) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::Model(format!("stored {field} is negative")))
}

fn codec_version(value: i64) -> Result<u16, RepositoryError> {
    u16::try_from(value)
        .map_err(|_| RepositoryError::Model("stored payload codec version is invalid".into()))
}

fn json_error(error: serde_json::Error) -> RepositoryError {
    RepositoryError::Model(format!("SQL metadata: {error}"))
}

pub(crate) fn event_from_row(row: &impl SqlRow) -> Result<EventRecord, RepositoryError> {
    let codec = row.text("payload_codec")?;
    let event = EventRecord {
        event_name: row.text("event_name")?,
        event_version: unsigned(row.integer("event_version")?, "event version")?,
        sequence: unsigned(row.integer("sequence")?, "event sequence")?,
        payload: row.bytes("payload")?,
        payload_codec: if codec == BITCODE_PAYLOAD_CODEC {
            Cow::Borrowed(BITCODE_PAYLOAD_CODEC)
        } else {
            Cow::Owned(codec)
        },
        payload_codec_version: codec_version(row.integer("payload_codec_version")?)?,
        metadata: serde_json::from_str(&row.text("metadata")?).map_err(json_error)?,
        timestamp: row.timestamp("recorded_at")?,
    };
    super::validation::validate_supported_event_codec(&event)?;
    Ok(event)
}

pub(crate) fn snapshot_from_row(row: &impl SqlRow) -> Result<SnapshotRecord, RepositoryError> {
    Ok(SnapshotRecord {
        aggregate_type: row.text("aggregate_type")?,
        aggregate_id: row.text("aggregate_id")?,
        version: unsigned(row.integer("version")?, "snapshot version")?,
        snapshot_version: unsigned(row.integer("snapshot_version")?, "snapshot payload version")?,
        payload: row.bytes("payload")?,
        payload_codec: row.text("payload_codec")?,
        payload_codec_version: codec_version(row.integer("payload_codec_version")?)?,
        metadata: serde_json::from_str(&row.text("metadata")?).map_err(json_error)?,
        recorded_at: row.timestamp("recorded_at")?,
    })
}

pub(crate) async fn stream_version(
    executor: &mut impl SqlExecutor,
    identity: &StreamIdentity,
) -> Result<u64, RepositoryError> {
    let rows = executor
        .query(
            Statement::new(
                "SELECT MAX(sequence) AS version FROM aggregate_events WHERE aggregate_type = ",
            )
            .identity(identity),
        )
        .await?;
    let row = rows
        .first()
        .ok_or_else(|| RepositoryError::Model("missing stream version row".into()))?;
    unsigned(
        row.optional_integer("version")?.unwrap_or(0),
        "event sequence",
    )
}

pub(crate) async fn load_stream<E: SqlExecutor>(
    executor: &mut E,
    identity: &StreamIdentity,
    after_version: Option<u64>,
) -> Result<Option<Entity>, RepositoryError> {
    let mut statement = Statement::new("SELECT ")
        .sql(E::EVENT_SELECT)
        .sql(" FROM aggregate_events WHERE aggregate_type = ")
        .identity(identity);
    if let Some(after) = after_version {
        statement = statement
            .sql(" AND sequence > ")
            .bind(SqlBind::Integer(signed(
                after,
                "snapshot tail lower bound",
            )?));
    }
    let rows = executor
        .query(statement.sql(" ORDER BY sequence ASC"))
        .await?;
    let events = rows
        .iter()
        .map(event_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut entity = Entity::with_id(identity.aggregate_id());
    match after_version {
        None if events.is_empty() => return Ok(None),
        None => entity.load_from_history(events),
        Some(after) => {
            // Keep the existing SQL snapshot-only restore contract. A future
            // snapshot above a non-empty durable stream is clamped, not trusted.
            let prefix = if events.is_empty() {
                match stream_version(executor, identity).await? {
                    0 => after,
                    version => after.min(version),
                }
            } else {
                after
            };
            entity.load_tail_from_history(events, prefix);
        }
    }
    Ok(Some(entity))
}

pub(crate) async fn load_snapshot<E: SqlExecutor>(
    executor: &mut E,
    identity: &StreamIdentity,
) -> Result<Option<SnapshotRecord>, RepositoryError> {
    let rows = executor
        .query(
            Statement::new("SELECT ")
                .sql(E::SNAPSHOT_SELECT)
                .sql(" FROM aggregate_snapshots WHERE aggregate_type = ")
                .identity(identity),
        )
        .await?;
    rows.first().map(snapshot_from_row).transpose()
}

/// The same upsert is used by standalone cache writes and command commits.
pub(crate) async fn save_snapshot<E: SqlExecutor>(
    executor: &mut E,
    identity: &StreamIdentity,
    record: &SnapshotRecord,
) -> Result<(), RepositoryError> {
    validate_snapshot_identity(identity, record)?;
    let values = [
        SqlBind::Text(identity.aggregate_type().into()),
        SqlBind::Text(identity.aggregate_id().into()),
        SqlBind::Integer(signed(record.version, "snapshot version")?),
        SqlBind::Integer(signed(record.snapshot_version, "snapshot payload version")?),
        SqlBind::Bytes(Cow::Borrowed(&record.payload)),
        SqlBind::Text(record.payload_codec.clone()),
        SqlBind::Integer(i64::from(record.payload_codec_version)),
        SqlBind::Metadata(serde_json::to_string(&record.metadata).map_err(json_error)?),
        SqlBind::Timestamp(record.recorded_at),
    ];
    let mut statement = Statement::new("INSERT INTO aggregate_snapshots (aggregate_type, aggregate_id, version, snapshot_version, payload, payload_codec, payload_codec_version, metadata, recorded_at) VALUES (");
    for (index, value) in values.into_iter().enumerate() {
        if index != 0 {
            statement = statement.sql(", ");
        }
        statement = statement.bind(value);
    }
    executor.execute(statement.sql(") ON CONFLICT(aggregate_type, aggregate_id) DO UPDATE SET version = excluded.version, snapshot_version = excluded.snapshot_version, payload = excluded.payload, payload_codec = excluded.payload_codec, payload_codec_version = excluded.payload_codec_version, metadata = excluded.metadata, recorded_at = excluded.recorded_at, updated_at = ").sql(E::NOW)).await?;
    Ok(())
}

pub(crate) async fn delete_snapshot(
    executor: &mut impl SqlExecutor,
    identity: &StreamIdentity,
) -> Result<bool, RepositoryError> {
    Ok(executor
        .execute(
            Statement::new("DELETE FROM aggregate_snapshots WHERE aggregate_type = ")
                .identity(identity),
        )
        .await?
        > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_plan_borrows_only_new_payloads_and_respects_bind_limit() {
        let mut entity = Entity::with_id("large-history");
        for _ in 0..1000 {
            entity.digest("created", &vec![7_u8; 4096]).unwrap();
        }
        entity.mark_committed();
        for _ in 0..3 {
            entity.digest("changed", &vec![9_u8; 4096]).unwrap();
        }
        let identity = StreamIdentity::new("Item", "large-history").unwrap();
        let append = PreparedEventAppend {
            identity: identity.clone(),
            expected_version: 1000,
            events: entity.new_events(),
        };
        let plans = event_inserts(&[append], 20).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0]
                .statement
                .0
                .iter()
                .filter(|part| matches!(part, SqlPart::Bind(_)))
                .count(),
            20
        );
        assert_eq!(
            plans[1]
                .statement
                .0
                .iter()
                .filter(|part| matches!(part, SqlPart::Bind(_)))
                .count(),
            10
        );
        let payloads = plans
            .iter()
            .flat_map(|plan| &plan.statement.0)
            .filter_map(|part| match part {
                SqlPart::Bind(SqlBind::Bytes(Cow::Borrowed(bytes))) => Some(*bytes),
                SqlPart::Bind(SqlBind::Bytes(Cow::Owned(_))) => {
                    panic!("event plan cloned a payload")
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            payloads.len(),
            3,
            "old history must not enter the insert plan"
        );
        for (payload, event) in payloads.iter().zip(entity.new_events()) {
            assert_eq!(payload.as_ptr(), event.payload.as_ptr());
        }
        for plan in plans {
            assert_eq!(plan.candidates, vec![(identity.clone(), 1000)]);
        }
    }

    #[test]
    fn event_plan_rejects_unrepresentable_sequence_and_too_few_binds() {
        let mut entity = Entity::with_id("one");
        entity.digest_empty("created").unwrap();
        let mut events = entity.new_events().to_vec();
        let identity = StreamIdentity::new("Item", "one").unwrap();
        let append = PreparedEventAppend {
            identity: identity.clone(),
            expected_version: 0,
            events: &events,
        };
        assert!(event_inserts(&[append], 9).is_err());
        events[0].sequence = u64::MAX;
        let append = PreparedEventAppend {
            identity,
            expected_version: 0,
            events: &events,
        };
        assert!(event_inserts(&[append], 900).is_err());
    }
}
