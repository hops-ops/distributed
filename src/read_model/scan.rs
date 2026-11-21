//! Bounded read-only equality/keyset selection. A row is not authorization.
use super::{RelationalReadModel, Versioned};
use crate::table::{ColumnType, RowValue, RowValues, TableSchema, TableStoreError};

/// Opaque continuation tied to exactly one declared schema and equality scope.
/// Pages are current reads, not a cross-page database snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadModelScanCursor {
    schema: TableSchema,
    equals: RowValues,
    after: String,
}
impl ReadModelScanCursor {
    pub fn after(&self) -> &str {
        &self.after
    }
}

#[derive(Clone, Debug)]
pub struct ReadModelScanRequest {
    pub(crate) schema: TableSchema,
    pub(crate) equals: RowValues,
    pub(crate) cursor: Option<ReadModelScanCursor>,
    pub(crate) limit: u16,
}
impl ReadModelScanRequest {
    /// Single nonnullable text PK, at most 16 scalar equality predicates and
    /// 1..=100 rows. No offsets, joins, includes, raw SQL or aggregate reads.
    pub fn new<M: RelationalReadModel>(
        equals: RowValues,
        limit: u16,
        cursor: Option<ReadModelScanCursor>,
    ) -> Result<Self, TableStoreError> {
        let request = Self {
            schema: M::schema().clone(),
            equals,
            cursor,
            limit,
        };
        request.validate()?;
        Ok(request)
    }
    pub(crate) fn validate(&self) -> Result<(), TableStoreError> {
        let invalid =
            |message: &str| TableStoreError::Metadata(format!("read-model keyset scan: {message}"));
        self.schema.validate()?;
        if !self.schema.kind.is_read_model() {
            return Err(invalid("operational tables are not read models"));
        }
        if !(1..=100).contains(&self.limit) || self.equals.len() > 16 {
            return Err(invalid(
                "limit must be 1..=100; at most 16 equality predicates",
            ));
        }
        if self.schema.primary_key.columns.len() != 1 {
            return Err(invalid("single text primary key required"));
        }
        let primary = self
            .schema
            .columns
            .iter()
            .find(|c| c.column_name == self.key_column())
            .ok_or_else(|| invalid("missing primary key"))?;
        if primary.column_type != ColumnType::Text || primary.nullable {
            return Err(invalid("nonnullable text primary key required"));
        }
        for (name, value) in self.equals.iter() {
            let column = self
                .schema
                .columns
                .iter()
                .find(|c| c.column_name == name)
                .ok_or_else(|| invalid("unknown filter column"))?;
            let valid = match (&column.column_type, value) {
                (_, RowValue::Null) => column.nullable,
                (ColumnType::Text, RowValue::String(value)) => {
                    value.len() <= 4096 && !value.contains('\0')
                }
                (ColumnType::Boolean, RowValue::Bool(_))
                | (ColumnType::Integer, RowValue::I64(_)) => true,
                (ColumnType::UnsignedInteger, RowValue::U64(value)) => *value <= i64::MAX as u64,
                (ColumnType::UnsignedInteger, RowValue::I64(value)) => *value >= 0,
                _ => false,
            };
            if !valid {
                return Err(invalid("unsupported or mismatched scalar filter"));
            }
        }
        if let Some(cursor) = &self.cursor {
            if cursor.schema != self.schema
                || cursor.equals != self.equals
                || cursor.after.len() > 4096
                || cursor.after.contains('\0')
            {
                return Err(invalid("cursor does not match schema/filter scope"));
            }
        }
        Ok(())
    }
    pub(crate) fn key_column(&self) -> &str {
        &self.schema.primary_key.columns[0]
    }
    pub(crate) fn after(&self) -> Option<&str> {
        self.cursor.as_ref().map(|c| c.after.as_str())
    }
    pub(crate) fn matches(&self, row: &RowValues) -> bool {
        self.equals
            .iter()
            .all(|(name, expected)| match (row.get(name), expected) {
                (Some(RowValue::I64(actual)), RowValue::U64(expected)) => {
                    *actual >= 0 && *actual as u64 == *expected
                }
                (Some(RowValue::U64(actual)), RowValue::I64(expected)) => {
                    *expected >= 0 && *actual == *expected as u64
                }
                (Some(actual), expected) => actual == expected,
                _ => false,
            })
    }
    pub(crate) fn finish(
        &self,
        mut rows: Vec<Versioned<RowValues>>,
    ) -> Result<ReadModelScanPage, TableStoreError> {
        let more = rows.len() > usize::from(self.limit);
        rows.truncate(usize::from(self.limit));
        let next = if more {
            let Some(RowValue::String(after)) =
                rows.last().and_then(|r| r.data.get(self.key_column()))
            else {
                return Err(TableStoreError::Metadata(
                    "scan returned invalid primary key".into(),
                ));
            };
            if after.len() > 4096 || after.contains('\0') {
                return Err(TableStoreError::Metadata(
                    "scan primary key exceeds cursor bound".into(),
                ));
            }
            Some(ReadModelScanCursor {
                schema: self.schema.clone(),
                equals: self.equals.clone(),
                after: after.clone(),
            })
        } else {
            None
        };
        Ok(ReadModelScanPage {
            rows,
            next,
            schema: self.schema.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadModelScanPage {
    pub rows: Vec<Versioned<RowValues>>,
    pub next: Option<ReadModelScanCursor>,
    schema: TableSchema,
}
impl ReadModelScanPage {
    pub fn typed<M: RelationalReadModel>(&self) -> Result<Vec<Versioned<M>>, TableStoreError> {
        if M::schema() != &self.schema {
            return Err(TableStoreError::Metadata(
                "scan page belongs to another model".into(),
            ));
        }
        self.rows
            .iter()
            .map(|row| {
                Ok(Versioned {
                    data: M::from_row(row.data.clone())?,
                    version: row.version,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{PrimaryKey, TableColumn, TableKind};
    fn request() -> ReadModelScanRequest {
        let schema = TableSchema {
            model_name: "Page".into(),
            table_name: "pages".into(),
            columns: vec![TableColumn::new("id", "id", ColumnType::Text)],
            primary_key: PrimaryKey::new(["id"]),
            relationships: vec![], indexes: vec![], foreign_keys: vec![], version_column: Some("_sourced_version".into()), kind: TableKind::ReadModel,
        };
        ReadModelScanRequest {
            schema,
            equals: RowValues::new(),
            cursor: None,
            limit: 10,
        }
    }
    #[test]
    fn malformed_schemas_and_cursor_scopes_fail_before_io() {
        let good = request();
        good.validate().unwrap();
        let mut bad = good.clone();
        bad.schema.kind = TableKind::Operational;
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.schema.primary_key.columns.push("extra".into());
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.schema.columns[0].column_type = ColumnType::Integer;
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.schema.columns[0].nullable = true;
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.cursor = Some(ReadModelScanCursor {
            schema: good.schema.clone(),
            equals: filter(),
            after: "key".into(),
        });
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.cursor = Some(ReadModelScanCursor {
            schema: good.schema.clone(),
            equals: RowValues::new(),
            after: "x".repeat(4097),
        });
        assert!(bad.validate().is_err());
    }
    fn filter() -> RowValues {
        let mut values = RowValues::new();
        values.insert("id", RowValue::String("key".into()));
        values
    }
}
