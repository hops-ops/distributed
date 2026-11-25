use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};

pub const OUTBOX_MESSAGES_TABLE: &str = "outbox_messages";

/// Row contract for the durable outbox delivery table.
///
/// The migrations own this table's DDL. Its indexes are not listed here: they
/// differ per dialect and Postgres uses partial indexes, which `TableIndex`
/// cannot express, so rendering this schema must not create any.
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
            table_column("failed_at", ColumnType::Timestamp, true),
            table_column("source_aggregate_type", ColumnType::Text, true),
            table_column("source_aggregate_id", ColumnType::Text, true),
            table_column("source_sequence", ColumnType::UnsignedInteger, true),
            table_column("correlation_id", ColumnType::Text, true),
            table_column("causation_id", ColumnType::Text, true),
        ],
        primary_key: PrimaryKey::new(["message_id"]),
        version_column: None,
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::Operational,
    });
    &SCHEMA
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
