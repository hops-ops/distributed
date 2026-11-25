use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use super::super::naming::is_valid_graphql_name;
use super::projection::validate_response_key;

pub(super) const QUERY_EVIDENCE_HIDDEN_PREFIX: &str = "0__distributed_evidence_pk_";
const MAX_QUERY_EVIDENCE_NODES: usize = 1_024;
const MAX_QUERY_EVIDENCE_KEY_FIELDS: usize = 64;
const MAX_QUERY_EVIDENCE_RECORDS: usize = 4_096;

#[derive(Clone, Debug)]
pub(crate) struct QueryEvidencePlan {
    root_response_key: String,
    root: QueryEvidenceNode,
}

#[derive(Clone, Debug)]
pub(super) enum QueryEvidenceNode {
    Object(QueryEvidenceObjectPlan),
    List(Box<QueryEvidenceNode>),
}

#[derive(Clone, Debug)]
pub(super) struct QueryEvidenceObjectPlan {
    pub(super) record: Option<QueryEvidenceRecordPlan>,
    pub(super) fields: Vec<QueryEvidenceFieldPlan>,
}

#[derive(Clone, Debug)]
pub(super) struct QueryEvidenceRecordPlan {
    pub(super) model: String,
    pub(super) key_fields: Vec<QueryEvidenceKeyPlan>,
}

#[derive(Clone, Debug)]
pub(super) struct QueryEvidenceKeyPlan {
    pub(super) hidden_key: String,
    pub(super) column: String,
}

#[derive(Clone, Debug)]
pub(super) struct QueryEvidenceFieldPlan {
    /// Key present in the compiler-owned SQL JSON object. Projection storage is
    /// response-keyed so repeated selections of one schema field remain
    /// distinct when their arguments or nested selections differ.
    pub(super) storage_key: String,
    /// Key emitted in the final GraphQL response and therefore used in causal
    /// record paths.
    pub(super) response_key: String,
    pub(super) node: Box<QueryEvidenceNode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QueryResponsePathSegment {
    Field(String),
    Index(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryRecordEvidence {
    pub(crate) model: String,
    pub(crate) key_columns: BTreeMap<String, JsonValue>,
    pub(crate) response_path: Vec<QueryResponsePathSegment>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExtractedQueryEvidence {
    pub(crate) records: Vec<QueryRecordEvidence>,
    /// False means the bounded collector saw more records than it can safely
    /// retain. Hidden fields were still removed, but callers must use their
    /// conservative causal fallback instead of partial evidence.
    pub(crate) complete: bool,
}

#[derive(Default)]
struct QueryEvidencePlanSize {
    nodes: usize,
    key_fields: usize,
}

#[derive(Default)]
struct QueryEvidenceExtraction {
    records: Vec<QueryRecordEvidence>,
    records_seen: usize,
    overflowed: bool,
    first_error: Option<String>,
}

impl QueryEvidencePlan {
    pub(super) fn new(root_response_key: String, root: QueryEvidenceNode) -> Result<Self, String> {
        validate_response_key(&root_response_key).map_err(|e| e.to_string())?;
        let mut size = QueryEvidencePlanSize::default();
        root.validate(&mut size)?;
        Ok(Self {
            root_response_key,
            root,
        })
    }

    pub(super) fn extract_and_strip(
        &self,
        value: &mut JsonValue,
    ) -> Result<ExtractedQueryEvidence, String> {
        let mut extraction = QueryEvidenceExtraction::default();
        let mut path = vec![QueryResponsePathSegment::Field(
            self.root_response_key.clone(),
        )];
        self.root.visit_and_strip(value, &mut path, &mut extraction);

        if let Some(error) = extraction.first_error {
            return Err(error);
        }
        if extraction.overflowed {
            extraction.records.clear();
        }
        Ok(ExtractedQueryEvidence {
            records: extraction.records,
            complete: !extraction.overflowed,
        })
    }
}

impl QueryEvidenceNode {
    fn validate(&self, size: &mut QueryEvidencePlanSize) -> Result<(), String> {
        size.nodes = size
            .nodes
            .checked_add(1)
            .ok_or_else(|| "query causal-evidence plan size overflowed".to_string())?;
        if size.nodes > MAX_QUERY_EVIDENCE_NODES {
            return Err(format!(
                "query causal-evidence plan exceeds {MAX_QUERY_EVIDENCE_NODES} nodes"
            ));
        }

        match self {
            Self::List(item) => item.validate(size),
            Self::Object(object) => {
                if let Some(record) = &object.record {
                    if record.model.trim().is_empty() || record.key_fields.is_empty() {
                        return Err(
                            "query causal-evidence record has no model or primary key".into()
                        );
                    }
                    size.key_fields = size
                        .key_fields
                        .checked_add(record.key_fields.len())
                        .ok_or_else(|| {
                            "query causal-evidence key-field count overflowed".to_string()
                        })?;
                    if size.key_fields > MAX_QUERY_EVIDENCE_KEY_FIELDS {
                        return Err(format!(
                            "query causal-evidence plan exceeds {MAX_QUERY_EVIDENCE_KEY_FIELDS} key fields"
                        ));
                    }
                    let mut hidden_keys = std::collections::BTreeSet::new();
                    let mut columns = std::collections::BTreeSet::new();
                    for key in &record.key_fields {
                        if !key.hidden_key.starts_with(QUERY_EVIDENCE_HIDDEN_PREFIX)
                            || is_valid_graphql_name(&key.hidden_key)
                            || key.column.trim().is_empty()
                            || !hidden_keys.insert(key.hidden_key.as_str())
                            || !columns.insert(key.column.as_str())
                        {
                            return Err(
                                "query causal-evidence record has an invalid or duplicate key field"
                                    .into(),
                            );
                        }
                    }
                }

                let mut response_keys = std::collections::BTreeSet::new();
                for field in &object.fields {
                    validate_response_key(&field.storage_key).map_err(|e| e.to_string())?;
                    validate_response_key(&field.response_key).map_err(|e| e.to_string())?;
                    if !response_keys.insert(field.response_key.as_str()) {
                        return Err(format!(
                            "query causal-evidence object repeats response key `{}`",
                            field.response_key
                        ));
                    }
                    field.node.validate(size)?;
                }
                Ok(())
            }
        }
    }

    fn visit_and_strip(
        &self,
        value: &mut JsonValue,
        path: &mut Vec<QueryResponsePathSegment>,
        extraction: &mut QueryEvidenceExtraction,
    ) {
        match self {
            Self::List(item) => match value {
                JsonValue::Null => {}
                JsonValue::Array(items) => {
                    for (index, value) in items.iter_mut().enumerate() {
                        path.push(QueryResponsePathSegment::Index(index));
                        item.visit_and_strip(value, path, extraction);
                        path.pop();
                    }
                }
                // Walk the expected item shape as a defensive cleanup even
                // when the database returned an impossible shape.
                other => {
                    extraction.record_error(format!(
                        "query causal-evidence expected a list at {}",
                        format_response_path(path)
                    ));
                    item.visit_and_strip(other, path, extraction);
                }
            },
            Self::Object(object) => match value {
                JsonValue::Null => {}
                JsonValue::Object(map) => object.visit_and_strip(map, path, extraction),
                JsonValue::Array(items) => {
                    extraction.record_error(format!(
                        "query causal-evidence expected an object at {}",
                        format_response_path(path)
                    ));
                    for (index, value) in items.iter_mut().enumerate() {
                        path.push(QueryResponsePathSegment::Index(index));
                        if let JsonValue::Object(map) = value {
                            object.visit_and_strip(map, path, extraction);
                        }
                        path.pop();
                    }
                }
                _ => extraction.record_error(format!(
                    "query causal-evidence expected an object at {}",
                    format_response_path(path)
                )),
            },
        }
    }
}

impl QueryEvidenceObjectPlan {
    fn visit_and_strip(
        &self,
        map: &mut serde_json::Map<String, JsonValue>,
        path: &mut Vec<QueryResponsePathSegment>,
        extraction: &mut QueryEvidenceExtraction,
    ) {
        if let Some(record) = &self.record {
            let mut key_columns = BTreeMap::new();
            let mut complete_key = true;
            for key in &record.key_fields {
                match map.remove(&key.hidden_key) {
                    Some(value) => {
                        key_columns.insert(key.column.clone(), value);
                    }
                    None => {
                        complete_key = false;
                        extraction.record_error(format!(
                            "query causal-evidence is missing key column `{}` for model `{}` at {}",
                            key.column,
                            record.model,
                            format_response_path(path)
                        ));
                    }
                }
            }
            if complete_key {
                extraction.records_seen += 1;
                if extraction.records_seen <= MAX_QUERY_EVIDENCE_RECORDS {
                    extraction.records.push(QueryRecordEvidence {
                        model: record.model.clone(),
                        key_columns,
                        response_path: path.clone(),
                    });
                } else {
                    extraction.overflowed = true;
                }
            }
        }

        // A newer compiler or malformed row must not leak an unrecognized
        // reserved alias. This only examines record/container objects selected
        // by the evidence tree; arbitrary user JSON values are never walked.
        let unexpected_hidden = map
            .keys()
            .filter(|key| key.starts_with(QUERY_EVIDENCE_HIDDEN_PREFIX))
            .cloned()
            .collect::<Vec<_>>();
        for key in unexpected_hidden {
            map.remove(&key);
            extraction.record_error(format!(
                "query causal-evidence contained unexpected hidden field `{key}` at {}",
                format_response_path(path)
            ));
        }

        for field in &self.fields {
            path.push(QueryResponsePathSegment::Field(field.response_key.clone()));
            match map.get_mut(&field.storage_key) {
                Some(value) => field.node.visit_and_strip(value, path, extraction),
                None => extraction.record_error(format!(
                    "query causal-evidence is missing storage field `{}` for response field `{}` at {}",
                    field.storage_key,
                    field.response_key,
                    format_response_path(path)
                )),
            }
            path.pop();
        }
    }
}

impl QueryEvidenceExtraction {
    fn record_error(&mut self, error: String) {
        if self.first_error.is_none() {
            self.first_error = Some(error);
        }
    }
}

fn format_response_path(path: &[QueryResponsePathSegment]) -> String {
    let mut rendered = String::from("$");
    for segment in path {
        match segment {
            QueryResponsePathSegment::Field(field) => {
                rendered.push('.');
                rendered.push_str(field);
            }
            QueryResponsePathSegment::Index(index) => {
                rendered.push('[');
                rendered.push_str(&index.to_string());
                rendered.push(']');
            }
        }
    }
    rendered
}

#[cfg(test)]
mod query_evidence_tests {
    use super::super::dialect::SqlDialect;
    use super::super::projection::{chunked_json_object, record_identity_projection};
    use super::*;
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};

    fn hidden(ordinal: usize) -> String {
        format!("{QUERY_EVIDENCE_HIDDEN_PREFIX}{ordinal}")
    }

    fn evidence_alias(response_key: &str, node: QueryEvidenceNode) -> QueryEvidenceFieldPlan {
        QueryEvidenceFieldPlan {
            storage_key: response_key.into(),
            response_key: response_key.into(),
            node: Box::new(node),
        }
    }

    fn object_node(
        model: Option<&str>,
        columns: &[&str],
        fields: Vec<QueryEvidenceFieldPlan>,
    ) -> QueryEvidenceNode {
        QueryEvidenceNode::Object(QueryEvidenceObjectPlan {
            record: model.map(|model| QueryEvidenceRecordPlan {
                model: model.into(),
                key_fields: columns
                    .iter()
                    .enumerate()
                    .map(|(ordinal, column)| QueryEvidenceKeyPlan {
                        hidden_key: hidden(ordinal),
                        column: (*column).into(),
                    })
                    .collect(),
            }),
            fields,
        })
    }

    fn list_node(item: QueryEvidenceNode) -> QueryEvidenceNode {
        QueryEvidenceNode::List(Box::new(item))
    }

    fn object(entries: impl IntoIterator<Item = (impl Into<String>, JsonValue)>) -> JsonValue {
        JsonValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    #[test]
    fn list_evidence_tracks_aliases_nested_relationships_and_composite_keys() {
        let comment = || object_node(Some("Comment"), &["tenant_id", "comment_id"], Vec::new());
        let plan = QueryEvidencePlan::new(
            "usersAlias".into(),
            list_node(object_node(
                Some("User"),
                &["user_id"],
                vec![
                    evidence_alias(
                        "authorAlias",
                        object_node(Some("Profile"), &["profile_id"], Vec::new()),
                    ),
                    evidence_alias("commentsAlias", list_node(comment())),
                    evidence_alias(
                        "commentsStatsAlias",
                        object_node(
                            None,
                            &[],
                            vec![evidence_alias("rowsAlias", list_node(comment()))],
                        ),
                    ),
                ],
            )),
        )
        .unwrap();

        let metadata = object([(hidden(0), serde_json::json!("user-owned-json"))]);
        let author = object([
            (hidden(0), serde_json::json!("profile-1")),
            ("displayName".into(), serde_json::json!("Pat")),
        ]);
        let first_comment = object([
            (hidden(0), serde_json::json!("tenant-1")),
            (hidden(1), serde_json::json!("9223372036854775807")),
            ("body".into(), serde_json::json!("first")),
        ]);
        let second_comment = object([
            (hidden(0), serde_json::json!("tenant-1")),
            (hidden(1), serde_json::json!("9223372036854775808")),
            ("body".into(), serde_json::json!("second")),
        ]);
        let aggregate_comment = object([
            (hidden(0), serde_json::json!("tenant-1")),
            (hidden(1), serde_json::json!("AP8=")),
            ("body".into(), serde_json::json!("aggregate")),
        ]);
        let mut value = JsonValue::Array(vec![object([
            (hidden(0), serde_json::json!("user-1")),
            ("name".into(), serde_json::json!("User One")),
            ("metadata".into(), metadata.clone()),
            ("authorAlias".into(), author),
            (
                "commentsAlias".into(),
                JsonValue::Array(vec![first_comment, second_comment]),
            ),
            (
                "commentsStatsAlias".into(),
                object([
                    ("aggregate", serde_json::json!({"count": 1})),
                    ("rowsAlias", JsonValue::Array(vec![aggregate_comment])),
                ]),
            ),
        ])]);

        let extracted = plan.extract_and_strip(&mut value).unwrap();
        assert!(extracted.complete);
        assert_eq!(extracted.records.len(), 5);
        assert_eq!(
            extracted.records[0],
            QueryRecordEvidence {
                model: "User".into(),
                key_columns: BTreeMap::from([("user_id".into(), serde_json::json!("user-1"))]),
                response_path: vec![
                    QueryResponsePathSegment::Field("usersAlias".into()),
                    QueryResponsePathSegment::Index(0),
                ],
            }
        );
        assert_eq!(
            extracted.records[2].response_path,
            vec![
                QueryResponsePathSegment::Field("usersAlias".into()),
                QueryResponsePathSegment::Index(0),
                QueryResponsePathSegment::Field("commentsAlias".into()),
                QueryResponsePathSegment::Index(0),
            ]
        );
        assert_eq!(
            extracted.records[2].key_columns,
            BTreeMap::from([
                (
                    "comment_id".into(),
                    serde_json::json!("9223372036854775807")
                ),
                ("tenant_id".into(), serde_json::json!("tenant-1")),
            ])
        );
        assert_eq!(
            extracted.records[4].response_path,
            vec![
                QueryResponsePathSegment::Field("usersAlias".into()),
                QueryResponsePathSegment::Index(0),
                QueryResponsePathSegment::Field("commentsStatsAlias".into()),
                QueryResponsePathSegment::Field("rowsAlias".into()),
                QueryResponsePathSegment::Index(0),
            ]
        );

        let expected = JsonValue::Array(vec![object([
            ("name", serde_json::json!("User One")),
            ("metadata", metadata),
            ("authorAlias", serde_json::json!({"displayName": "Pat"})),
            (
                "commentsAlias",
                serde_json::json!([
                    {"body": "first"},
                    {"body": "second"}
                ]),
            ),
            (
                "commentsStatsAlias",
                serde_json::json!({
                    "aggregate": {"count": 1},
                    "rowsAlias": [{"body": "aggregate"}]
                }),
            ),
        ])]);
        assert_eq!(value, expected);
        assert_eq!(
            value[0]["metadata"][hidden(0).as_str()],
            serde_json::json!("user-owned-json"),
            "plan-guided stripping must not recurse into arbitrary JSON scalars"
        );
    }

    #[test]
    fn by_pk_evidence_uses_the_root_alias_and_null_has_no_record() {
        let plan = QueryEvidencePlan::new(
            "itemAlias".into(),
            object_node(Some("Item"), &["item_id"], Vec::new()),
        )
        .unwrap();
        let mut value = object([
            (hidden(0), serde_json::json!("item-1")),
            ("label".into(), serde_json::json!("one")),
        ]);

        let extracted = plan.extract_and_strip(&mut value).unwrap();
        assert_eq!(
            extracted.records[0].response_path,
            vec![QueryResponsePathSegment::Field("itemAlias".into())]
        );
        assert_eq!(value, serde_json::json!({"label": "one"}));

        let mut absent = JsonValue::Null;
        let extracted = plan.extract_and_strip(&mut absent).unwrap();
        assert!(extracted.complete);
        assert!(extracted.records.is_empty());
    }

    #[test]
    fn hidden_aliases_cannot_collide_and_are_stripped_on_shape_errors() {
        assert!(!is_valid_graphql_name(&hidden(0)));
        let plan = QueryEvidencePlan::new(
            "items".into(),
            list_node(object_node(Some("Item"), &["id"], Vec::new())),
        )
        .unwrap();
        let unexpected = hidden(99);
        let mut value = JsonValue::Array(vec![
            object([
                (hidden(0), serde_json::json!("one")),
                (unexpected, serde_json::json!("private")),
            ]),
            object([("visible", serde_json::json!(true))]),
        ]);

        let error = plan.extract_and_strip(&mut value).unwrap_err();
        assert!(error.contains("unexpected hidden field"), "{error}");
        assert!(value[0].as_object().unwrap().is_empty());
        assert_eq!(value[1], serde_json::json!({"visible": true}));
        assert!(
            !serde_json::to_string(&value)
                .unwrap()
                .contains(QUERY_EVIDENCE_HIDDEN_PREFIX),
            "all record-level hidden aliases must be removed even after the first error"
        );
    }

    #[test]
    fn record_collection_bound_falls_back_without_disclosing_hidden_keys() {
        let plan = QueryEvidencePlan::new(
            "items".into(),
            list_node(object_node(Some("Item"), &["id"], Vec::new())),
        )
        .unwrap();
        let mut value = JsonValue::Array(
            (0..=MAX_QUERY_EVIDENCE_RECORDS)
                .map(|index| object([(hidden(0), serde_json::json!(index))]))
                .collect(),
        );

        let extracted = plan.extract_and_strip(&mut value).unwrap();
        assert!(!extracted.complete);
        assert!(extracted.records.is_empty());
        assert!(value
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value.as_object().unwrap().is_empty()));
    }

    #[test]
    fn evidence_plan_rejects_key_field_and_node_bounds() {
        let too_many_keys = (0..=MAX_QUERY_EVIDENCE_KEY_FIELDS)
            .map(|ordinal| format!("column_{ordinal}"))
            .collect::<Vec<_>>();
        let key_refs = too_many_keys.iter().map(String::as_str).collect::<Vec<_>>();
        let error = QueryEvidencePlan::new(
            "items".into(),
            object_node(Some("Wide"), &key_refs, Vec::new()),
        )
        .unwrap_err();
        assert!(error.contains("key fields"), "{error}");

        let mut deep = object_node(None, &[], Vec::new());
        for _ in 0..MAX_QUERY_EVIDENCE_NODES {
            deep = list_node(deep);
        }
        let error = QueryEvidencePlan::new("items".into(), deep).unwrap_err();
        assert!(error.contains("nodes"), "{error}");
    }

    #[test]
    fn compiler_injects_lossless_private_keys_even_when_not_selected() {
        let schema = TableSchema {
            model_name: "Composite".into(),
            table_name: "composites".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("sequence", "sequence_id", ColumnType::Text)
                },
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("digest", "digest_bytes", ColumnType::Bytes)
                },
                TableColumn::new("visible", "visible", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["sequence_id", "digest_bytes"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        };
        let mut bytes_paths = Vec::new();
        let (pairs, record) = record_identity_projection(
            SqlDialect::Sqlite,
            &schema,
            "t7",
            &mut bytes_paths,
            "childrenAlias",
        )
        .unwrap();

        assert_eq!(pairs.len(), 2);
        let record = record.expect("client-normalized identity evidence");
        assert_eq!(record.model, "Composite");
        assert_eq!(record.key_fields[0].column, "sequence_id");
        assert_eq!(pairs[0].0, hidden(0));
        assert_eq!(pairs[0].1, "t7.\"sequence_id\"");
        assert_eq!(pairs[1].0, hidden(1));
        assert!(pairs[1].1.contains("hex"));
        assert_eq!(bytes_paths, vec![format!("childrenAlias.{}", hidden(1))]);
        let sql = chunked_json_object(SqlDialect::Sqlite, &pairs);
        assert!(sql.contains(QUERY_EVIDENCE_HIDDEN_PREFIX), "{sql}");
        assert!(!sql.contains("'visible'"), "{sql}");

        let (postgres_pairs, _) =
            record_identity_projection(SqlDialect::Postgres, &schema, "t7", &mut Vec::new(), "")
                .unwrap();
        assert_eq!(postgres_pairs[0].1, "t7.\"sequence_id\"");
        assert!(
            postgres_pairs[1].1.contains("replace(encode(")
                && postgres_pairs[1].1.contains("E'\\n'"),
            "{}",
            postgres_pairs[1].1
        );
    }

    #[test]
    fn embedded_client_identity_omits_record_projection_but_not_row_data() {
        let schema = TableSchema {
            model_name: "BigIntRecord".into(),
            table_name: "bigint_records".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("sequence", "sequence_id", ColumnType::UnsignedInteger)
                },
                TableColumn::new("visible", "visible", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["sequence_id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        };

        // The compiler emits record evidence only for a model the selected
        // surface gives a client-addressable identity (`ModelScan`).
        let surface = crate::graphql::build_surface(
            std::slice::from_ref(&schema),
            &crate::graphql::SurfaceOptions::sqlite(),
        )
        .unwrap();
        assert!(
            !crate::graphql::surface::model_has_client_normalized_identity(
                &surface.models["BigIntRecord"]
            )
        );
    }
}
