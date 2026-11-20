use std::collections::BTreeMap;

use super::{
    canonical_projection_topology_bytes, digest_projection_binding, ProjectionScopeCodec,
    ProjectionScopeCodecError,
};
use crate::projection_protocol::{
    ProjectionRecordScope, ProjectorTopologyId, RecordKey, RecordKeyField,
    ResolvedProjectionObligation,
};
use crate::table::{ColumnType, PrimaryKey, RowKey, RowValue, TableColumn, TableKind, TableSchema};

fn topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(3, "project_memberships", [7; 32]).unwrap()
}

#[test]
fn catalog_canonical_json_sorts_nested_object_keys_without_reordering_arrays() {
    let left = serde_json::json!({
        "z": {"b": 2, "a": 1},
        "a": [{"y": true, "x": false}, 3],
    });
    let right = serde_json::json!({
        "a": [{"x": false, "y": true}, 3],
        "z": {"a": 1, "b": 2},
    });

    assert_eq!(
        canonical_projection_topology_bytes(&left).unwrap(),
        canonical_projection_topology_bytes(&right).unwrap()
    );
}

#[test]
fn projection_binding_digest_is_domain_separated_and_length_bound() {
    let canonical = br#"{"binding":"todos"}"#;

    assert_eq!(
        digest_projection_binding(canonical),
        digest_projection_binding(canonical)
    );
    assert_ne!(
        digest_projection_binding(canonical),
        digest_projection_binding(br#"{"binding":"chat"}"#)
    );
}

fn schema(
    model: &str,
    table: &str,
    columns: Vec<TableColumn>,
    primary_key: &[&str],
) -> &'static TableSchema {
    Box::leak(Box::new(TableSchema {
        model_name: model.into(),
        table_name: table.into(),
        columns,
        primary_key: PrimaryKey::new(primary_key.iter().copied()),
        version_column: Some("_sourced_version".into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }))
}

fn key_column(field: &str, column: &str, column_type: ColumnType) -> TableColumn {
    TableColumn {
        primary_key: true,
        ..TableColumn::new(field, column, column_type)
    }
}

fn obligation(
    codec: &ProjectionScopeCodec,
    projector: &str,
    model: &str,
    partition: Option<serde_json::Value>,
    fields: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
) -> ResolvedProjectionObligation {
    let key = RecordKey {
        fields: fields
            .into_iter()
            .map(|(field, value)| RecordKeyField {
                field: field.into(),
                value,
            })
            .collect(),
    };
    let scope = codec
        .encode_resolved_obligation_scope(projector, model, &key, partition.as_ref())
        .unwrap_or_else(|_| {
            ProjectionRecordScope::new(
                codec.topology().clone(),
                codec.encode_partition(partition.as_ref()).unwrap(),
                model,
                b"invalid-test-key".to_vec(),
            )
            .unwrap()
        });
    ResolvedProjectionObligation {
        projector: projector.into(),
        model: model.into(),
        partition,
        key,
        scope,
    }
}

#[test]
fn obligation_and_row_keys_share_one_schema_ordered_composite_encoding() {
    let membership = schema(
        "Membership",
        "memberships",
        vec![
            key_column("tenantId", "tenant_id", ColumnType::Text),
            key_column("sequence", "member_sequence", ColumnType::UnsignedInteger),
            key_column("attributes", "attributes_json", ColumnType::Json),
        ],
        &["tenant_id", "member_sequence", "attributes_json"],
    );
    let codec =
        ProjectionScopeCodec::with_models(topology(), [("Membership", membership)]).unwrap();
    let command_partition =
        serde_json::from_str(r#"{"region":"west","tenant":{"id":"t-1","tier":2}}"#).unwrap();
    let projector_partition =
        serde_json::from_str(r#"{"tenant":{"tier":2,"id":"t-1"},"region":"west"}"#).unwrap();
    let command = obligation(
        &codec,
        "project_memberships",
        "Membership",
        Some(command_partition),
        [
            (
                "attributes",
                serde_json::json!({"z": [2, 1], "a": {"b": true}}),
            ),
            ("sequence", serde_json::json!(42_u64)),
            ("tenantId", serde_json::json!("t-1")),
        ],
    );
    let row = RowKey::new([
        ("tenant_id", RowValue::String("t-1".into())),
        ("member_sequence", RowValue::U64(42)),
        (
            "attributes_json",
            RowValue::Json(serde_json::json!({"a": {"b": true}, "z": [2, 1]})),
        ),
    ]);

    let obligation_scope = codec.encode_obligation_scope(&command).unwrap();
    let row_scope = codec
        .encode_row_scope(
            "project_memberships",
            "Membership",
            Some(&projector_partition),
            &row,
        )
        .unwrap();

    assert_eq!(obligation_scope, row_scope);
    assert_eq!(
        obligation_scope.canonical_key_bytes(),
        row_scope.canonical_key_bytes()
    );
    assert_eq!(obligation_scope.key_digest(), row_scope.key_digest());
}

#[test]
fn recursive_json_is_object_order_invariant_and_absent_is_not_null() {
    let codec = ProjectionScopeCodec::new(topology());
    let left = serde_json::from_str(r#"{"z":{"b":2,"a":1},"a":[{"y":true,"x":null}]}"#).unwrap();
    let right = serde_json::from_str(r#"{"a":[{"x":null,"y":true}],"z":{"a":1,"b":2}}"#).unwrap();

    assert_eq!(
        codec.encode_partition(Some(&left)).unwrap(),
        codec.encode_partition(Some(&right)).unwrap()
    );

    let absent = codec.encode_partition(None).unwrap();
    let explicit_null = codec
        .encode_partition(Some(&serde_json::Value::Null))
        .unwrap();
    assert_ne!(absent.canonical_bytes(), explicit_null.canonical_bytes());
    assert_ne!(absent.digest(), explicit_null.digest());
}

#[test]
fn integer_schema_controls_signedness_and_range() {
    let signed = schema(
        "SignedRecord",
        "signed_records",
        vec![key_column("id", "id", ColumnType::Integer)],
        &["id"],
    );
    let unsigned = schema(
        "UnsignedRecord",
        "unsigned_records",
        vec![key_column("id", "id", ColumnType::UnsignedInteger)],
        &["id"],
    );
    let codec = ProjectionScopeCodec::with_models(
        topology(),
        [("SignedRecord", signed), ("UnsignedRecord", unsigned)],
    )
    .unwrap();

    let signed_command = obligation(
        &codec,
        "project_memberships",
        "SignedRecord",
        None,
        [("id", serde_json::json!(-1))],
    );
    let signed_row = RowKey::new([("id", RowValue::I64(-1))]);
    assert_eq!(
        codec.encode_obligation_scope(&signed_command).unwrap(),
        codec
            .encode_row_scope("project_memberships", "SignedRecord", None, &signed_row)
            .unwrap()
    );

    let unsigned_command = obligation(
        &codec,
        "project_memberships",
        "UnsignedRecord",
        None,
        [("id", serde_json::json!(u64::MAX))],
    );
    let unsigned_row = RowKey::new([("id", RowValue::U64(u64::MAX))]);
    assert_eq!(
        codec.encode_obligation_scope(&unsigned_command).unwrap(),
        codec
            .encode_row_scope("project_memberships", "UnsignedRecord", None, &unsigned_row)
            .unwrap()
    );

    let negative_unsigned = obligation(
        &codec,
        "project_memberships",
        "UnsignedRecord",
        None,
        [("id", serde_json::json!(-1))],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&negative_unsigned),
        Err(ProjectionScopeCodecError::IntegerOutOfRange {
            expected: "unsigned 64-bit integer",
            ..
        })
    ));

    let too_large_signed = obligation(
        &codec,
        "project_memberships",
        "SignedRecord",
        None,
        [("id", serde_json::json!(u64::MAX))],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&too_large_signed),
        Err(ProjectionScopeCodecError::IntegerOutOfRange {
            expected: "signed 64-bit integer",
            ..
        })
    ));
    assert!(matches!(
        codec.encode_row_scope(
            "project_memberships",
            "SignedRecord",
            None,
            &RowKey::new([("id", RowValue::U64(1))]),
        ),
        Err(ProjectionScopeCodecError::WrongRowValueShape { .. })
    ));
    assert!(matches!(
        codec.encode_row_scope(
            "project_memberships",
            "UnsignedRecord",
            None,
            &RowKey::new([("id", RowValue::I64(1))]),
        ),
        Err(ProjectionScopeCodecError::WrongRowValueShape { .. })
    ));
}

#[test]
fn registration_and_topology_mismatches_fail_closed() {
    let record = schema(
        "Record",
        "records",
        vec![key_column("id", "record_id", ColumnType::Text)],
        &["record_id"],
    );
    let mismatched = schema(
        "Actual",
        "actual",
        vec![key_column("id", "id", ColumnType::Text)],
        &["id"],
    );
    let malformed = schema(
        "Malformed",
        "malformed",
        vec![TableColumn::new("id", "id", ColumnType::Text)],
        &["id"],
    );
    let mut codec = ProjectionScopeCodec::new(topology());

    assert_eq!(
        codec.register_model("", record).unwrap_err(),
        ProjectionScopeCodecError::BlankModelRegistration
    );
    assert!(matches!(
        codec.register_model("Declared", mismatched),
        Err(ProjectionScopeCodecError::ModelRegistrationMismatch { .. })
    ));
    assert!(matches!(
        codec.register_model("Malformed", malformed),
        Err(ProjectionScopeCodecError::InvalidModelRegistration { .. })
    ));
    codec.register_model("Record", record).unwrap();
    assert!(matches!(
        codec.register_model("Record", record),
        Err(ProjectionScopeCodecError::DuplicateModelRegistration { .. })
    ));

    let wrong_projector = obligation(
        &codec,
        "another_projector",
        "Record",
        None,
        [("id", serde_json::json!("r-1"))],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&wrong_projector),
        Err(ProjectionScopeCodecError::ProjectorMismatch { .. })
    ));
    let unknown_model = obligation(
        &codec,
        "project_memberships",
        "Unknown",
        None,
        [("id", serde_json::json!("r-1"))],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&unknown_model),
        Err(ProjectionScopeCodecError::UnknownModel { .. })
    ));
    assert!(matches!(
        codec.encode_row_scope(
            "another_projector",
            "Record",
            None,
            &RowKey::new([("record_id", RowValue::String("r-1".into()))]),
        ),
        Err(ProjectionScopeCodecError::ProjectorMismatch { .. })
    ));
}

#[test]
fn malformed_obligation_and_row_keys_fail_closed() {
    let record = schema(
        "Record",
        "records",
        vec![
            key_column("id", "record_id", ColumnType::Text),
            key_column("active", "is_active", ColumnType::Boolean),
        ],
        &["record_id", "is_active"],
    );
    let float_record = schema(
        "FloatRecord",
        "float_records",
        vec![key_column("id", "id", ColumnType::Float)],
        &["id"],
    );
    let bytes_record = schema(
        "BytesRecord",
        "bytes_records",
        vec![key_column("id", "id", ColumnType::Bytes)],
        &["id"],
    );
    let codec = ProjectionScopeCodec::with_models(
        topology(),
        [
            ("Record", record),
            ("FloatRecord", float_record),
            ("BytesRecord", bytes_record),
        ],
    )
    .unwrap();

    let missing = obligation(
        &codec,
        "project_memberships",
        "Record",
        None,
        [("id", serde_json::json!("r-1"))],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&missing),
        Err(ProjectionScopeCodecError::MissingKeyField { field, .. })
            if field == "active"
    ));

    let extra = obligation(
        &codec,
        "project_memberships",
        "Record",
        None,
        [
            ("id", serde_json::json!("r-1")),
            ("active", serde_json::json!(true)),
            ("other", serde_json::json!(1)),
        ],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&extra),
        Err(ProjectionScopeCodecError::ExtraKeyField { field, .. })
            if field == "other"
    ));

    let duplicate = obligation(
        &codec,
        "project_memberships",
        "Record",
        None,
        [
            ("id", serde_json::json!("r-1")),
            ("id", serde_json::json!("r-2")),
            ("active", serde_json::json!(true)),
        ],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&duplicate),
        Err(ProjectionScopeCodecError::DuplicateKeyField { field, .. })
            if field == "id"
    ));

    let null = obligation(
        &codec,
        "project_memberships",
        "Record",
        None,
        [
            ("id", serde_json::Value::Null),
            ("active", serde_json::json!(true)),
        ],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&null),
        Err(ProjectionScopeCodecError::NullPrimaryKey { field, .. })
            if field == "id"
    ));

    let wrong_json_shape = obligation(
        &codec,
        "project_memberships",
        "Record",
        None,
        [
            ("id", serde_json::json!("r-1")),
            ("active", serde_json::json!("true")),
        ],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&wrong_json_shape),
        Err(ProjectionScopeCodecError::WrongJsonShape { field, .. })
            if field == "active"
    ));

    assert!(matches!(
        codec.encode_row_scope(
            "project_memberships",
            "Record",
            None,
            &RowKey::new([
                ("record_id", RowValue::String("r-1".into())),
                ("is_active", RowValue::String("true".into())),
            ]),
        ),
        Err(ProjectionScopeCodecError::WrongRowValueShape { column, .. })
            if column == "is_active"
    ));
    assert!(matches!(
        codec.encode_row_scope(
            "project_memberships",
            "Record",
            None,
            &RowKey::new([
                ("record_id", RowValue::String("r-1".into())),
                ("is_active", RowValue::Bool(true)),
                ("other", RowValue::I64(1)),
            ]),
        ),
        Err(ProjectionScopeCodecError::ExtraKeyColumn { column, .. })
            if column == "other"
    ));
    assert!(matches!(
        codec.encode_row_scope(
            "project_memberships",
            "Record",
            None,
            &RowKey::new([("record_id", RowValue::String("r-1".into()))]),
        ),
        Err(ProjectionScopeCodecError::MissingKeyColumn { column, .. })
            if column == "is_active"
    ));
    assert!(matches!(
        codec.encode_row_scope(
            "project_memberships",
            "FloatRecord",
            None,
            &RowKey::new([("id", RowValue::F64(f64::NAN))]),
        ),
        Err(ProjectionScopeCodecError::NonFiniteFloat { .. })
    ));

    let noncanonical_base64 = obligation(
        &codec,
        "project_memberships",
        "BytesRecord",
        None,
        [("id", serde_json::json!("AB=="))],
    );
    assert!(matches!(
        codec.encode_obligation_scope(&noncanonical_base64),
        Err(ProjectionScopeCodecError::InvalidBytes { .. })
    ));
}

#[test]
fn codec_owns_registered_schema_independently_of_the_caller() {
    let mut original = TableSchema {
        model_name: "OwnedRecord".into(),
        table_name: "owned_records".into(),
        columns: vec![key_column("id", "record_id", ColumnType::Text)],
        primary_key: PrimaryKey::new(["record_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let codec =
        ProjectionScopeCodec::with_models(topology(), [("OwnedRecord", &original)]).unwrap();

    original.model_name = "MutatedAfterRegistration".into();
    original.table_name = "mutated_after_registration".into();
    original.primary_key = PrimaryKey::new(["not_the_registered_key"]);
    drop(original);

    let registered = codec.registered_schema("OwnedRecord").unwrap();
    assert_eq!(registered.model_name, "OwnedRecord");
    assert_eq!(registered.table_name, "owned_records");
    assert_eq!(registered.primary_key.columns, ["record_id"]);
    assert_eq!(
        codec
            .registered_schema_owned("OwnedRecord")
            .unwrap()
            .table_name,
        "owned_records"
    );
}

#[test]
fn graphql_json_columns_decode_lossless_composite_row_keys() {
    let composite = TableSchema {
        model_name: "CompositeRecord".into(),
        table_name: "composite_records".into(),
        columns: vec![
            key_column("signed", "signed_id", ColumnType::Integer),
            key_column("unsigned", "unsigned_id", ColumnType::UnsignedInteger),
            key_column("active", "is_active", ColumnType::Boolean),
            key_column("digest", "digest_bytes", ColumnType::Bytes),
            key_column("attributes", "attributes_json", ColumnType::Json),
        ],
        primary_key: PrimaryKey::new([
            "signed_id",
            "unsigned_id",
            "is_active",
            "digest_bytes",
            "attributes_json",
        ]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let codec =
        ProjectionScopeCodec::with_models(topology(), [("CompositeRecord", &composite)]).unwrap();
    let values = BTreeMap::from([
        (
            "signed_id".into(),
            serde_json::Value::String(i64::MIN.to_string()),
        ),
        (
            "unsigned_id".into(),
            serde_json::Value::String(u64::MAX.to_string()),
        ),
        ("is_active".into(), serde_json::json!(1)),
        ("digest_bytes".into(), serde_json::json!("AP8=")),
        (
            "attributes_json".into(),
            serde_json::json!({"z": [2, 1], "a": true}),
        ),
    ]);

    let decoded = codec
        .row_key_from_json_columns("CompositeRecord", &values)
        .unwrap();
    let expected = RowKey::new([
        ("signed_id", RowValue::I64(i64::MIN)),
        ("unsigned_id", RowValue::U64(u64::MAX)),
        ("is_active", RowValue::Bool(true)),
        ("digest_bytes", RowValue::Bytes(vec![0, 255])),
        (
            "attributes_json",
            RowValue::Json(serde_json::json!({"a": true, "z": [2, 1]})),
        ),
    ]);
    assert_eq!(decoded, expected);
    assert_eq!(
        codec
            .encode_unpartitioned_row_key("CompositeRecord", &decoded)
            .unwrap(),
        codec
            .encode_unpartitioned_row_key("CompositeRecord", &expected)
            .unwrap()
    );

    let mut missing = values.clone();
    missing.remove("digest_bytes");
    assert!(matches!(
        codec.row_key_from_json_columns("CompositeRecord", &missing),
        Err(ProjectionScopeCodecError::MissingKeyColumn { column, .. })
            if column == "digest_bytes"
    ));

    let mut extra = values.clone();
    extra.insert("other".into(), serde_json::json!(1));
    assert!(matches!(
        codec.row_key_from_json_columns("CompositeRecord", &extra),
        Err(ProjectionScopeCodecError::ExtraKeyColumn { column, .. })
            if column == "other"
    ));

    let mut noncanonical_integer = values.clone();
    noncanonical_integer.insert("signed_id".into(), serde_json::json!("01"));
    assert!(matches!(
        codec.row_key_from_json_columns("CompositeRecord", &noncanonical_integer),
        Err(ProjectionScopeCodecError::IntegerOutOfRange { .. })
    ));

    let mut noncanonical_bytes = values;
    noncanonical_bytes.insert("digest_bytes".into(), serde_json::json!("AB=="));
    assert!(matches!(
        codec.row_key_from_json_columns("CompositeRecord", &noncanonical_bytes),
        Err(ProjectionScopeCodecError::InvalidBytes { .. })
    ));
}
