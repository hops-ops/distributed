//! Rejections of the manifest's surface, capability, scalar and model
//! contract, one row per check, through [`ClientManifest::parse`].
//!
//! Every row starts from the valid test manifest, breaks exactly one rule and
//! names the diagnostic code the compiler must refuse it with.

use serde_json::{json, Value as JsonValue};

use crate::client_compiler::tests::{manifest, normalized_list_relationship, parse_manifest};
use crate::client_compiler::ClientSurfaceIdentity;

type Mutation = fn(&mut JsonValue);

struct Rejection {
    name: &'static str,
    mutate: Mutation,
    code: &'static str,
}

fn todo(value: &mut JsonValue) -> &mut JsonValue {
    &mut value["models"][0]
}

fn owner(value: &mut JsonValue) -> &mut JsonValue {
    &mut value["models"][0]["relationships"][0]
}

/// Replace the singular embedded `owner` with the has-many list fixture.
fn list_owner(value: &mut JsonValue) -> &mut JsonValue {
    *owner(value) = normalized_list_relationship();
    owner(value)
}

fn relationship_aggregate() -> JsonValue {
    json!({
        "name": "owner_aggregate",
        "arguments": [],
        "semantics": {
            "wrapper_typename": "todo_aggregate",
            "fields_typename": "todo_aggregate_fields",
            "nodes_pagination": {
                "kind": "offset",
                "default_limit": 25,
                "max_limit": 100,
                "coverage": "window"
            },
            "count": true,
            "nodes": true,
            "sum": [],
            "avg": [],
            "min": [],
            "max": []
        },
        "dependencies": ["todo_rows"]
    })
}

fn application_surface(eligible: JsonValue, schema: JsonValue) -> JsonValue {
    json!({
        "kind": "application",
        "name": "web",
        "eligible_roles": eligible,
        "schema_roles": schema
    })
}

fn remove_argument(relationship: &mut JsonValue, kind: &str) {
    relationship["arguments"]
        .as_array_mut()
        .expect("relationship arguments")
        .retain(|argument| argument["kind"] != kind);
}

fn cmp(column: &str, rhs: JsonValue) -> JsonValue {
    json!({"kind": "cmp", "value": {"column": column, "op": "eq", "rhs": rhs}})
}

fn predicate(expression: JsonValue) -> JsonValue {
    json!({"kind": "predicate", "expression": expression})
}

fn rejections() -> Vec<Rejection> {
    vec![
        // Surface identity.
        Rejection {
            name: "blank role surface name",
            mutate: |value| value["surface"]["name"] = json!("  "),
            code: "client.manifest.empty",
        },
        Rejection {
            name: "application surface without eligible roles",
            mutate: |value| value["surface"] = application_surface(json!([]), json!(["user"])),
            code: "client.manifest.surface_eligible_roles",
        },
        Rejection {
            name: "application surface without schema roles",
            mutate: |value| value["surface"] = application_surface(json!(["user"]), json!([])),
            code: "client.manifest.surface_schema_roles",
        },
        Rejection {
            name: "application schema role that is not eligible",
            mutate: |value| {
                value["surface"] = application_surface(json!(["user"]), json!(["admin"]))
            },
            code: "client.manifest.surface_schema_roles",
        },
        Rejection {
            name: "application surface repeating an eligible role",
            mutate: |value| {
                value["surface"] = application_surface(json!(["user", "user"]), json!(["user"]))
            },
            code: "client.manifest.duplicate_entry",
        },
        // Declared capabilities.
        Rejection {
            name: "query fallback other than revalidate",
            mutate: |value| value["capabilities"]["query_fallback"] = json!("network"),
            code: "client.manifest.query_fallback",
        },
        Rejection {
            name: "live resume without live queries",
            mutate: |value| value["capabilities"]["live_queries"] = json!(false),
            code: "client.manifest.live_resume",
        },
        Rejection {
            name: "tombstones without record revisions",
            mutate: |value| value["capabilities"]["record_revisions"] = json!(false),
            code: "client.manifest.tombstone_capability",
        },
        // Capabilities derived from the inventory.
        Rejection {
            name: "live queries denied while subscription roots exist",
            mutate: |value| {
                value["capabilities"]["live_queries"] = json!(false);
                value["capabilities"]["live_resume"] = json!(false);
            },
            code: "client.manifest.live_capability",
        },
        Rejection {
            name: "causal receipts without commands",
            mutate: |value| value["capabilities"]["causal_receipts"] = json!(true),
            code: "client.manifest.command_capability",
        },
        Rejection {
            name: "cache scope disabled",
            mutate: |value| value["capabilities"]["cache_scope"] = json!(false),
            code: "client.manifest.command_capability",
        },
        Rejection {
            name: "confirmed persistence requested",
            mutate: |value| value["capabilities"]["confirmed_persistence"] = json!(true),
            code: "client.manifest.persistence_capability",
        },
        // Scalar codec inventory.
        Rejection {
            name: "empty scalar codec inventory",
            mutate: |value| value["scalar_codecs"] = json!([]),
            code: "client.manifest.scalar_codecs",
        },
        Rejection {
            name: "blank scalar name",
            mutate: |value| value["scalar_codecs"][0]["scalar"] = json!(" "),
            code: "client.manifest.empty",
        },
        Rejection {
            name: "scalar with a codec this compiler does not implement",
            mutate: |value| value["scalar_codecs"][5]["codec"] = json!("int64"),
            code: "client.manifest.codec_unsupported",
        },
        Rejection {
            name: "scalar codec declared twice",
            mutate: |value| {
                let first = value["scalar_codecs"][0].clone();
                value["scalar_codecs"]
                    .as_array_mut()
                    .expect("scalar codecs")
                    .push(first);
            },
            code: "client.manifest.duplicate_scalar",
        },
        Rejection {
            name: "scalar codec inventory missing a scalar",
            mutate: |value| {
                value["scalar_codecs"]
                    .as_array_mut()
                    .expect("scalar codecs")
                    .retain(|entry| entry["scalar"] != "Timestamptz");
            },
            code: "client.manifest.scalar_codecs",
        },
        // Model shape.
        Rejection {
            name: "model dependency listed twice",
            mutate: |value| todo(value)["dependencies"] = json!(["todo_rows", "todo_rows"]),
            code: "client.manifest.duplicate_entry",
        },
        Rejection {
            name: "filter field operator listed twice",
            mutate: |value| {
                todo(value)["filter_input"]["fields"][0]["operators"] = json!(["_eq", "_eq"])
            },
            code: "client.manifest.duplicate_entry",
        },
        Rejection {
            name: "model id that is not a GraphQL name",
            mutate: |value| todo(value)["id"] = json!("1Todo"),
            code: "client.manifest.graphql_name",
        },
        Rejection {
            name: "model typename in the introspection namespace",
            mutate: |value| todo(value)["typename"] = json!("__todo"),
            code: "client.manifest.graphql_name",
        },
        Rejection {
            name: "model dependencies without its source table",
            mutate: |value| todo(value)["dependencies"] = json!(["other_rows"]),
            code: "client.manifest.model_dependency",
        },
        Rejection {
            name: "model tombstones without record revisions",
            mutate: |value| todo(value)["record_revisions"] = json!(false),
            code: "client.manifest.model_tombstones",
        },
        Rejection {
            name: "field codec that disagrees with its scalar",
            mutate: |value| todo(value)["fields"][2]["codec"] = json!("string"),
            code: "client.manifest.field_codec",
        },
        Rejection {
            name: "field scalar absent from the inventory",
            mutate: |value| todo(value)["fields"][2]["scalar"] = json!("Decimal"),
            code: "client.manifest.field_scalar",
        },
        Rejection {
            name: "field declared twice",
            mutate: |value| {
                let field = todo(value)["fields"][0].clone();
                todo(value)["fields"]
                    .as_array_mut()
                    .expect("fields")
                    .push(field);
            },
            code: "client.manifest.duplicate_field",
        },
        Rejection {
            name: "relationship sharing a field's name",
            mutate: |value| owner(value)["name"] = json!("title"),
            code: "client.manifest.duplicate_member",
        },
        // Normalized identity.
        Rejection {
            name: "unsupported identity encoding",
            mutate: |value| todo(value)["normalization"]["encoding"] = json!("tuple_v0"),
            code: "client.manifest.identity_encoding",
        },
        Rejection {
            name: "normalized model without identity fields",
            mutate: |value| todo(value)["normalization"]["fields"] = json!([]),
            code: "client.manifest.empty_identity",
        },
        Rejection {
            name: "identity field listed twice",
            mutate: |value| {
                todo(value)["normalization"]["fields"] = json!([
                    {"name": "id", "codec": "string"},
                    {"name": "id", "codec": "string"}
                ])
            },
            code: "client.manifest.duplicate_identity",
        },
        Rejection {
            name: "identity field that is not an authorized field",
            mutate: |value| todo(value)["normalization"]["fields"][1]["name"] = json!("secretId"),
            code: "client.manifest.identity_field",
        },
        Rejection {
            name: "nullable identity field",
            mutate: |value| {
                todo(value)["normalization"]["fields"][1] =
                    json!({"name": "title", "codec": "string"})
            },
            code: "client.manifest.identity_codec",
        },
        Rejection {
            name: "identity codec that disagrees with the field",
            mutate: |value| todo(value)["normalization"]["fields"][1]["codec"] = json!("int32"),
            code: "client.manifest.identity_codec",
        },
        // Relationship graph.
        Rejection {
            name: "relationship to an absent model",
            mutate: |value| owner(value)["target_model"] = json!("Ghost"),
            code: "client.manifest.filter_input_target",
        },
        Rejection {
            name: "relationship target typename that is not the model's",
            mutate: |value| owner(value)["target_typename"] = json!("ghost"),
            code: "client.manifest.relationship_typename",
        },
        Rejection {
            name: "relationship dependencies without the source table",
            mutate: |value| owner(value)["dependencies"] = json!(["other_rows"]),
            code: "client.manifest.dependency",
        },
        Rejection {
            name: "through relationship without its join table dependency",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["kind"] = json!("many_to_many");
                relationship["key_mapping"] = json!({
                    "kind": "through",
                    "local": ["id"],
                    "remote": ["id"],
                    "table": "todo_members",
                    "source_foreign_key": ["todo_id"],
                    "target_foreign_key": ["member_id"]
                });
            },
            code: "client.manifest.dependency",
        },
        Rejection {
            name: "belongs-to relationship declared as a list",
            mutate: |value| owner(value)["list"] = json!(true),
            code: "client.manifest.relationship_cardinality",
        },
        Rejection {
            name: "has-many relationship declared singular",
            mutate: |value| list_owner(value)["list"] = json!(false),
            code: "client.manifest.relationship_cardinality",
        },
        Rejection {
            name: "nullable list relationship",
            mutate: |value| list_owner(value)["nullable"] = json!(true),
            code: "client.manifest.relationship_nullability",
        },
        // Key mappings.
        Rejection {
            name: "embedded relationship claiming local maintenance",
            mutate: |value| owner(value)["maintenance"] = json!("local"),
            code: "client.manifest.relationship_maintenance",
        },
        Rejection {
            name: "direct mapping without local keys",
            mutate: |value| list_owner(value)["key_mapping"]["local"] = json!([]),
            code: "client.manifest.relationship_key_mapping",
        },
        Rejection {
            name: "direct mapping with unequal key counts",
            mutate: |value| list_owner(value)["key_mapping"]["remote"] = json!(["id"]),
            code: "client.manifest.relationship_key_mapping",
        },
        Rejection {
            name: "direct mapping from an absent local field",
            mutate: |value| list_owner(value)["key_mapping"]["local"][0] = json!("ghost"),
            code: "client.manifest.relationship_local_key",
        },
        Rejection {
            name: "direct mapping to an absent target field",
            mutate: |value| list_owner(value)["key_mapping"]["remote"][0] = json!("ghost"),
            code: "client.manifest.relationship_remote_key",
        },
        Rejection {
            name: "direct mapping between keys of different codecs",
            mutate: |value| list_owner(value)["key_mapping"]["remote"][1] = json!("priority"),
            code: "client.manifest.relationship_key_codec",
        },
        Rejection {
            name: "through mapping with a short source foreign key",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["kind"] = json!("many_to_many");
                relationship["dependencies"] = json!(["todo_members", "todo_rows"]);
                relationship["key_mapping"] = json!({
                    "kind": "through",
                    "local": ["tenantId", "id"],
                    "remote": ["tenantId", "id"],
                    "table": "todo_members",
                    "source_foreign_key": ["todo_id"],
                    "target_foreign_key": ["member_tenant", "member_id"]
                });
            },
            code: "client.manifest.relationship_keys",
        },
        Rejection {
            name: "through mapping with a short target foreign key",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["kind"] = json!("many_to_many");
                relationship["dependencies"] = json!(["todo_members", "todo_rows"]);
                relationship["key_mapping"] = json!({
                    "kind": "through",
                    "local": ["tenantId", "id"],
                    "remote": ["tenantId", "id"],
                    "table": "todo_members",
                    "source_foreign_key": ["todo_tenant", "todo_id"],
                    "target_foreign_key": []
                });
            },
            code: "client.manifest.relationship_keys",
        },
        Rejection {
            name: "through mapping with a blank foreign key column",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["kind"] = json!("many_to_many");
                relationship["dependencies"] = json!(["todo_members", "todo_rows"]);
                relationship["key_mapping"] = json!({
                    "kind": "through",
                    "local": ["id"],
                    "remote": ["id"],
                    "table": "todo_members",
                    "source_foreign_key": [" "],
                    "target_foreign_key": ["member_id"]
                });
            },
            code: "client.manifest.empty",
        },
        Rejection {
            name: "opaque mapping whose dependency is not declared",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["kind"] = json!("many_to_many");
                relationship["maintenance"] = json!("revalidate");
                relationship["key_mapping"] = json!({
                    "kind": "through_opaque",
                    "local": ["id"],
                    "remote": ["id"],
                    "dependency": "todo_members"
                });
            },
            code: "client.manifest.relationship_dependency",
        },
        Rejection {
            name: "opaque mapping with a blank dependency",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["kind"] = json!("many_to_many");
                relationship["maintenance"] = json!("revalidate");
                relationship["key_mapping"] = json!({
                    "kind": "through_opaque",
                    "local": ["id"],
                    "remote": ["id"],
                    "dependency": " "
                });
            },
            code: "client.manifest.empty",
        },
        // List semantics of a relationship.
        Rejection {
            name: "list relationship with paging arguments but no pagination",
            mutate: |value| list_owner(value)["pagination"] = JsonValue::Null,
            code: "client.manifest.relationship_arguments",
        },
        Rejection {
            name: "list relationship without filter semantics",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["filter"] = JsonValue::Null;
                remove_argument(relationship, "filter");
            },
            code: "client.manifest.relationship_filter",
        },
        Rejection {
            name: "list relationship without order semantics",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["order"] = JsonValue::Null;
                remove_argument(relationship, "order");
            },
            code: "client.manifest.relationship_order",
        },
        Rejection {
            name: "list relationship without pagination semantics",
            mutate: |value| {
                let relationship = list_owner(value);
                relationship["pagination"] = JsonValue::Null;
                remove_argument(relationship, "limit");
            },
            code: "client.manifest.relationship_pagination",
        },
        Rejection {
            name: "singular relationship with filter semantics",
            mutate: |value| {
                let list = normalized_list_relationship();
                let relationship = owner(value);
                relationship["filter"] = list["filter"].clone();
                relationship["arguments"] = json!([list["arguments"][0].clone()]);
            },
            code: "client.manifest.relationship_list_semantics",
        },
        Rejection {
            name: "singular relationship marked live",
            mutate: |value| owner(value)["live"] = json!(true),
            code: "client.manifest.relationship_live",
        },
        Rejection {
            name: "singular relationship with an aggregate",
            mutate: |value| owner(value)["aggregate"] = relationship_aggregate(),
            code: "client.manifest.relationship_aggregate",
        },
        Rejection {
            name: "relationship pagination that is not an offset window",
            mutate: |value| list_owner(value)["pagination"]["kind"] = json!("cursor"),
            code: "client.manifest.pagination",
        },
        Rejection {
            name: "relationship default limit above its maximum",
            mutate: |value| list_owner(value)["pagination"]["default_limit"] = json!(101),
            code: "client.manifest.pagination_limit",
        },
        Rejection {
            name: "relationship filter fields that are not the target's filter input",
            mutate: |value| list_owner(value)["filter"]["fields"][2]["operators"] = json!(["_eq"]),
            code: "client.manifest.filter_contract",
        },
        Rejection {
            name: "relationship filter relationships that are not the target's",
            mutate: |value| list_owner(value)["filter"]["relationships"] = json!([]),
            code: "client.manifest.filter_contract",
        },
        Rejection {
            name: "relationship filter dropping the target's row policy",
            mutate: |value| {
                list_owner(value)["filter"]["row_policy"] = json!({"kind": "server_only"})
            },
            code: "client.manifest.filter_row_policy",
        },
        Rejection {
            name: "relationship order missing a field",
            mutate: |value| {
                list_owner(value)["order"]["fields"] =
                    json!(["completed", "id", "priority", "tenantId"])
            },
            code: "client.manifest.order_fields",
        },
        Rejection {
            name: "relationship order missing a direction",
            mutate: |value| list_owner(value)["order"]["values"] = json!(["asc", "desc"]),
            code: "client.manifest.order_values",
        },
        // Relationship aggregates.
        Rejection {
            name: "aggregate without count",
            mutate: |value| {
                let mut aggregate = relationship_aggregate();
                aggregate["semantics"]["count"] = json!(false);
                list_owner(value)["aggregate"] = aggregate;
            },
            code: "client.manifest.aggregate_capability",
        },
        Rejection {
            name: "aggregate over an absent field",
            mutate: |value| {
                let mut aggregate = relationship_aggregate();
                aggregate["semantics"]["sum"] = json!(["ghost"]);
                list_owner(value)["aggregate"] = aggregate;
            },
            code: "client.manifest.aggregate_field",
        },
        Rejection {
            name: "aggregate missing a dependency of its relationship",
            mutate: |value| {
                let mut aggregate = relationship_aggregate();
                aggregate["dependencies"] = json!(["other_rows"]);
                list_owner(value)["aggregate"] = aggregate;
            },
            code: "client.manifest.dependency",
        },
        Rejection {
            name: "aggregate nodes paginated past their maximum",
            mutate: |value| {
                let mut aggregate = relationship_aggregate();
                aggregate["semantics"]["nodes_pagination"]["default_limit"] = json!(500);
                list_owner(value)["aggregate"] = aggregate;
            },
            code: "client.manifest.pagination_limit",
        },
        // Filter input.
        Rejection {
            name: "filter input missing a field",
            mutate: |value| {
                todo(value)["filter_input"]["fields"]
                    .as_array_mut()
                    .expect("filter fields")
                    .pop();
            },
            code: "client.manifest.filter_fields",
        },
        Rejection {
            name: "filter field without operators",
            mutate: |value| todo(value)["filter_input"]["fields"][0]["operators"] = json!([]),
            code: "client.manifest.filter_operators",
        },
        Rejection {
            name: "filter field with an unknown operator",
            mutate: |value| {
                todo(value)["filter_input"]["fields"][0]["operators"] = json!(["_regex"])
            },
            code: "client.manifest.filter_operator",
        },
        Rejection {
            name: "filter input missing a relationship",
            mutate: |value| todo(value)["filter_input"]["relationships"] = json!([]),
            code: "client.manifest.filter_input_relationships",
        },
        // Row policy.
        Rejection {
            name: "row policy over an absent column",
            mutate: |value| {
                todo(value)["row_policy"] = predicate(cmp(
                    "secret",
                    json!({"kind": "lit", "value": {"kind": "bool", "value": true}}),
                ))
            },
            code: "client.manifest.row_policy_column",
        },
        Rejection {
            name: "row policy through an absent relationship",
            mutate: |value| {
                todo(value)["row_policy"] = predicate(json!({
                    "kind": "rel",
                    "value": {
                        "field": "ghost",
                        "predicate": {"kind": "is_null", "value": {"column": "id", "is_null": false}}
                    }
                }))
            },
            code: "client.manifest.row_policy_relationship",
        },
        Rejection {
            name: "row policy with an integer JavaScript cannot represent",
            mutate: |value| {
                todo(value)["row_policy"] = predicate(cmp(
                    "priority",
                    json!({"kind": "lit", "value": {"kind": "i64", "value": -9_007_199_254_740_992_i64}}),
                ))
            },
            code: "client.manifest.row_policy_portability",
        },
        Rejection {
            name: "row policy with a JSON number JavaScript cannot represent",
            mutate: |value| {
                todo(value)["row_policy"] = predicate(json!({
                    "kind": "in",
                    "value": {
                        "column": "priority",
                        "negated": false,
                        "values": [{
                            "kind": "lit",
                            "value": {"kind": "json", "value": {"nested": [9_007_199_254_740_993_u64]}}
                        }]
                    }
                }))
            },
            code: "client.manifest.row_policy_portability",
        },
        Rejection {
            name: "row policy claim header with surrounding whitespace",
            mutate: |value| {
                todo(value)["row_policy"] = predicate(json!({
                    "kind": "not",
                    "value": cmp("tenantId", json!({"kind": "claim", "value": {"header": " x-tenant "}}))
                }))
            },
            code: "client.manifest.trusted_preset_name",
        },
    ]
}

#[test]
fn each_broken_model_contract_rule_is_refused_with_its_code() {
    let selector = ClientSurfaceIdentity::role("user");
    parse_manifest(manifest(), &selector).expect("the unmodified manifest is valid");
    for row in rejections() {
        let mut value = manifest();
        (row.mutate)(&mut value);
        crate::client_compiler::tests::refresh_schema_fingerprint(&mut value);
        let error = parse_manifest(value, &selector)
            .err()
            .unwrap_or_else(|| panic!("{}: manifest was accepted", row.name));
        assert_eq!(error.code, row.code, "{}: {}", row.name, error.message);
    }
}

/// A row policy combining every expression form, including a predicate on
/// a related model, is accepted when each column and relationship exists and
/// every literal is portable to JavaScript.
#[test]
fn a_compound_row_policy_over_existing_columns_is_accepted() {
    let policy = predicate(json!({
        "kind": "and",
        "value": [
            cmp("tenantId", json!({"kind": "claim", "value": {"header": "x-tenant-id"}})),
            {"kind": "or", "value": [
                {"kind": "not", "value": {"kind": "is_null", "value": {"column": "title", "is_null": true}}},
                {"kind": "in", "value": {
                    "column": "priority",
                    "negated": false,
                    "values": [
                        {"kind": "lit", "value": {"kind": "i64", "value": 9_007_199_254_740_991_i64}},
                        {"kind": "lit", "value": {"kind": "json", "value": {"b": [1.5, null], "a": true}}}
                    ]
                }}
            ]},
            {"kind": "rel", "value": {
                "field": "owner",
                "predicate": {"kind": "is_null", "value": {"column": "id", "is_null": false}}
            }}
        ]
    }));
    let mut value = manifest();
    todo(&mut value)["row_policy"] = policy.clone();
    for root in value["roots"].as_array_mut().expect("roots") {
        if !root["filter"].is_null() {
            root["filter"]["row_policy"] = policy.clone();
        }
    }
    crate::client_compiler::tests::refresh_schema_fingerprint(&mut value);

    let manifest = parse_manifest(value, &ClientSurfaceIdentity::role("user"))
        .expect("compound row policy is accepted");
    let parsed = serde_json::to_value(&manifest.models["Todo"].row_policy).expect("serialize");
    assert_eq!(parsed, policy);
}
