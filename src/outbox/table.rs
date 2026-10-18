use std::time::{SystemTime, UNIX_EPOCH};

use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::table::{
    ColumnType, ExpectedVersion, PrimaryKey, RowKey, RowValue, RowValues, RowWriteMode,
    TableColumn, TableIndex, TableKind, TableModel, TableMutation, TableRowMutation, TableSchema,
    TableStoreError, TableWritePlan,
};

pub const OUTBOX_MESSAGES_TABLE: &str = "outbox_messages";

/// Schema for the durable outbox delivery table.
pub fn outbox_message_schema() -> &'static TableSchema {
    static SCHEMA: std::sync::LazyLock<TableSchema> = std::sync::LazyLock::new(|| TableSchema {
        model_name: "OutboxMessage".into(),
        table_name: OUTBOX_MESSAGES_TABLE.into(),
        columns: vec![
            table_column("message_id", ColumnType::Text, false),
            table_column("event_type", ColumnType::Text, false),
            table_column("payload", ColumnType::Bytes, false),
            table_column("payload_codec", ColumnType::Text, false),
            table_column("payload_codec_version", ColumnType::UnsignedInteger, false),
            table_column("destination", ColumnType::Text, true),
            table_column("metadata", ColumnType::Json, false),
            table_column("status", ColumnType::Text, false),
            table_column("created_at", ColumnType::Timestamp, false),
            table_column("next_available_at", ColumnType::Timestamp, false),
            timestamp_column_with_default("updated_at", "CURRENT_TIMESTAMP"),
            table_column("claimed_by", ColumnType::Text, true),
            table_column("claimed_until", ColumnType::Timestamp, true),
            table_column("attempts", ColumnType::UnsignedInteger, false),
            table_column("last_error", ColumnType::Text, true),
            table_column("published_at", ColumnType::Timestamp, true),
            table_column("failed_at", ColumnType::Timestamp, true),
            table_column("source_aggregate_type", ColumnType::Text, true),
            table_column("source_aggregate_id", ColumnType::Text, true),
            table_column("source_sequence", ColumnType::UnsignedInteger, true),
            table_column("correlation_id", ColumnType::Text, true),
            table_column("causation_id", ColumnType::Text, true),
        ],
        primary_key: PrimaryKey::new(["message_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: vec![
            named_index(
                "outbox_messages_claimable_idx",
                ["status", "next_available_at", "claimed_until", "created_at"],
            ),
            named_index(
                "outbox_messages_source_idx",
                [
                    "source_aggregate_type",
                    "source_aggregate_id",
                    "source_sequence",
                ],
            ),
            named_index("outbox_messages_destination_idx", ["destination", "status"]),
        ],
        relationships: Vec::new(),
        kind: TableKind::Operational,
    });
    &SCHEMA
}

pub fn outbox_message_insert_plan(
    message: &OutboxMessage,
) -> Result<TableWritePlan, TableStoreError> {
    let mutation = TableMutation::UpsertRow(TableRowMutation {
        schema: outbox_message_schema(),
        key: outbox_message_key(message.id()),
        values: outbox_message_row_values(message)?,
        expected_version: ExpectedVersion::NotExists,
        mode: RowWriteMode::Insert,
    });
    Ok(TableWritePlan::new(vec![mutation]))
}

pub(crate) fn validate_outbox_message_table_write(
    message: &OutboxMessage,
) -> Result<(), TableStoreError> {
    let plan = outbox_message_insert_plan(message)?;
    plan.validate()
}

pub fn outbox_message_key(message_id: &str) -> RowKey {
    RowKey::new([("message_id", RowValue::String(message_id.to_string()))])
}

pub fn outbox_message_row_values(message: &OutboxMessage) -> Result<RowValues, TableStoreError> {
    let mut row = RowValues::new();
    row.insert("message_id", RowValue::String(message.id().to_string()));
    row.insert("event_type", RowValue::String(message.event_type.clone()));
    row.insert("payload", RowValue::Bytes(message.payload.clone()));
    row.insert(
        "payload_codec",
        RowValue::String(message.payload_codec.clone()),
    );
    row.insert(
        "payload_codec_version",
        RowValue::U64(message.payload_codec_version as u64),
    );
    row.insert("destination", optional_string(&message.destination));
    row.insert_serde("metadata", &message.metadata)?;
    row.insert("status", RowValue::String(message.status.as_str().into()));
    let created_at = system_time_epoch_secs(message.created_at)?;
    row.insert("created_at", RowValue::U64(created_at));
    let next_available_at = match message.deliver_at {
        Some(at) => system_time_epoch_secs(at)?.max(created_at),
        None => created_at,
    };
    row.insert("next_available_at", RowValue::U64(next_available_at));
    row.insert("claimed_by", optional_string(&message.worker_id));
    row.insert(
        "claimed_until",
        optional_time_epoch_secs(message.leased_until)?,
    );
    row.insert("attempts", RowValue::U64(message.attempts as u64));
    row.insert("last_error", optional_string(&message.last_error));
    row.insert("published_at", RowValue::Null);
    row.insert("failed_at", status_failed_at(message)?);
    row.insert(
        "source_aggregate_type",
        optional_string(&message.source_aggregate_type),
    );
    row.insert(
        "source_aggregate_id",
        optional_string(&message.source_aggregate_id),
    );
    row.insert("source_sequence", optional_u64(message.source_sequence));
    row.insert(
        "correlation_id",
        optional_str(message.metadata.get("correlation_id").map(String::as_str)),
    );
    row.insert(
        "causation_id",
        optional_str(message.metadata.get("causation_id").map(String::as_str)),
    );
    Ok(row)
}

impl TableModel for OutboxMessage {
    fn table_schema() -> &'static TableSchema {
        outbox_message_schema()
    }

    fn table_key(&self) -> Result<RowKey, TableStoreError> {
        Ok(outbox_message_key(self.id()))
    }

    fn to_table_row(&self) -> Result<RowValues, TableStoreError> {
        outbox_message_row_values(self)
    }
}

fn table_column(name: &str, column_type: ColumnType, nullable: bool) -> TableColumn {
    let mut column = TableColumn::new(name, name, column_type);
    column.nullable = nullable;
    column
}

fn timestamp_column_with_default(name: &str, default: &str) -> TableColumn {
    let mut column = table_column(name, ColumnType::Timestamp, false);
    column.has_default = true;
    column.default = Some(default.into());
    column
}

fn named_index(name: &str, columns: impl IntoIterator<Item = &'static str>) -> TableIndex {
    let mut index = TableIndex::new(columns);
    index.name = Some(name.into());
    index
}

fn optional_string(value: &Option<String>) -> RowValue {
    optional_str(value.as_deref())
}

fn optional_str(value: Option<&str>) -> RowValue {
    value
        .map(|value| RowValue::String(value.to_string()))
        .unwrap_or(RowValue::Null)
}

fn optional_u64(value: Option<u64>) -> RowValue {
    value.map(RowValue::U64).unwrap_or(RowValue::Null)
}

fn optional_time_epoch_secs(value: Option<SystemTime>) -> Result<RowValue, TableStoreError> {
    value
        .map(system_time_epoch_secs)
        .transpose()
        .map(optional_u64)
}

fn status_failed_at(message: &OutboxMessage) -> Result<RowValue, TableStoreError> {
    if message.status == OutboxMessageStatus::Failed {
        Ok(RowValue::U64(system_time_epoch_secs(crate::time::now())?))
    } else {
        Ok(RowValue::Null)
    }
}

fn system_time_epoch_secs(value: SystemTime) -> Result<u64, TableStoreError> {
    value
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|err| TableStoreError::Metadata(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_insert_plan_uses_table_row_mutation() {
        let message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();

        let plan = outbox_message_insert_plan(&message).unwrap();

        assert_eq!(plan.mutations.len(), 1);
        let TableMutation::UpsertRow(mutation) = &plan.mutations[0] else {
            panic!("outbox insert should lower to a table row mutation");
        };
        assert_eq!(mutation.schema.table_name, OUTBOX_MESSAGES_TABLE);
        assert_eq!(mutation.key, outbox_message_key("msg-1"));
        assert_eq!(mutation.expected_version, ExpectedVersion::NotExists);
        assert_eq!(mutation.mode, RowWriteMode::Insert);
        assert_eq!(
            mutation.values.get("event_type"),
            Some(&RowValue::String("Event".into()))
        );
    }
}
