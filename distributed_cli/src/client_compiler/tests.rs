use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};

use super::manifest::{refresh_schema_fingerprint, ClientManifest};
use super::{compile_client, ClientCompileInput, ClientDocument, ClientSurfaceSelector};

fn fingerprint(label: &str) -> String {
    let digest = Sha256::digest(label.as_bytes());
    format!("sha256:{digest:x}")
}

fn scalar_codecs() -> JsonValue {
    json!([
        {"scalar": "BigInt", "codec": "json_number_precision_limited"},
        {"scalar": "Boolean", "codec": "boolean"},
        {"scalar": "Bytea", "codec": "base64"},
        {"scalar": "Float", "codec": "float64"},
        {"scalar": "ID", "codec": "string"},
        {"scalar": "Int", "codec": "int32"},
        {"scalar": "JSON", "codec": "json"},
        {"scalar": "String", "codec": "string"},
        {"scalar": "Timestamptz", "codec": "string_unvalidated_timestamp"}
    ])
}

fn model(id: &str, typename: &str) -> JsonValue {
    let filter_input_type = format!("{typename}_bool_exp");
    json!({
        "id": id,
        "typename": typename,
        "source_table": format!("{}_rows", id.to_lowercase()),
        "dependencies": [format!("{}_rows", id.to_lowercase())],
        "normalization": {
            "kind": "normalized",
            "fields": [
                {"name": "tenantId", "codec": "string"},
                {"name": "id", "codec": "string"}
            ],
            "encoding": "canonical_json_tuple_v1"
        },
        "fields": [
            {"name": "completed", "scalar": "Boolean", "codec": "boolean", "nullable": false},
            {"name": "id", "scalar": "ID", "codec": "string", "nullable": false},
            {"name": "priority", "scalar": "Int", "codec": "int32", "nullable": false},
            {"name": "tenantId", "scalar": "ID", "codec": "string", "nullable": false},
            {"name": "title", "scalar": "String", "codec": "string", "nullable": true}
        ],
        "relationships": [
            {
                "name": "owner",
                "target_model": id,
                "target_typename": typename,
                "kind": "belongs_to",
                "list": false,
                "nullable": true,
                "arguments": [],
                "key_mapping": {"kind": "embedded"},
                "maintenance": "revalidate",
                "dependencies": [format!("{}_rows", id.to_lowercase())],
                "live": false
            }
        ],
        "filter_input": {
            "type_name": filter_input_type,
            "fields": filter_semantics()["fields"].clone(),
            "relationships": [
                {"field": "owner", "target_type": filter_input_type}
            ]
        },
        "row_policy": {"kind": "unrestricted"},
        "record_revisions": true,
        "tombstones": true
    })
}

fn list_arguments() -> JsonValue {
    json!([
        {
            "name": "where",
            "kind": "filter",
            "type_name": "todo_bool_exp",
            "nullable": true,
            "list": false
        },
        {
            "name": "order_by",
            "kind": "order",
            "type_name": "todo_order_by",
            "nullable": true,
            "list": true
        },
        {
            "name": "limit",
            "kind": "limit",
            "type_name": "Int",
            "nullable": true,
            "list": false,
            "codec": "int32"
        },
        {
            "name": "offset",
            "kind": "offset",
            "type_name": "Int",
            "nullable": true,
            "list": false,
            "codec": "int32"
        }
    ])
}

fn filter_semantics() -> JsonValue {
    json!({
        "fields": [
            {"name": "completed", "operators": ["_eq"]},
            {"name": "id", "operators": ["_eq"]},
            {"name": "priority", "operators": ["_eq", "_in", "_nin"]},
            {"name": "tenantId", "operators": ["_eq"]},
            {"name": "title", "operators": ["_eq"]}
        ],
        "relationships": ["owner"],
        "row_policy": {"kind": "unrestricted"}
    })
}

fn order_semantics() -> JsonValue {
    json!({
        "fields": ["completed", "id", "priority", "tenantId", "title"],
        "values": [
            "asc",
            "asc_nulls_first",
            "asc_nulls_last",
            "desc",
            "desc_nulls_first",
            "desc_nulls_last"
        ]
    })
}

fn normalized_list_relationship() -> JsonValue {
    json!({
        "name": "owner",
        "target_model": "Todo",
        "target_typename": "todo",
        "kind": "has_many",
        "list": true,
        "nullable": false,
        "arguments": list_arguments(),
        "key_mapping": {
            "kind": "direct",
            "local": ["tenantId", "id"],
            "remote": ["tenantId", "id"]
        },
        "maintenance": "local",
        "dependencies": ["todo_rows"],
        "filter": filter_semantics(),
        "order": order_semantics(),
        "pagination": {
            "kind": "offset",
            "default_limit": 25,
            "max_limit": 100,
            "coverage": "window"
        },
        "aggregate": null,
        "live": true
    })
}

fn list_root(operation: &str) -> JsonValue {
    json!({
        "id": format!("{operation}:todos"),
        "operation": operation,
        "name": "todos",
        "kind": "list",
        "model": "Todo",
        "arguments": list_arguments(),
        "filter": filter_semantics(),
        "order": order_semantics(),
        "pagination": {
            "kind": "offset",
            "default_limit": 25,
            "max_limit": 100,
            "coverage": "window"
        },
        "aggregate": null,
        "dependencies": ["todo_rows"],
        "live": true
    })
}

fn by_pk_root() -> JsonValue {
    json!({
        "id": "query:todo",
        "operation": "query",
        "name": "todo",
        "kind": "by_pk",
        "model": "Todo",
        "arguments": [
            {
                "name": "tenantId",
                "kind": "primary_key",
                "type_name": "ID",
                "nullable": false,
                "list": false,
                "codec": "string"
            },
            {
                "name": "id",
                "kind": "primary_key",
                "type_name": "ID",
                "nullable": false,
                "list": false,
                "codec": "string"
            }
        ],
        "filter": null,
        "order": null,
        "pagination": null,
        "aggregate": null,
        "dependencies": ["todo_rows"],
        "live": false
    })
}

fn aggregate_root() -> JsonValue {
    json!({
        "id": "query:todos_aggregate",
        "operation": "query",
        "name": "todos_aggregate",
        "kind": "aggregate",
        "model": "Todo",
        "arguments": [{
            "name": "where",
            "kind": "filter",
            "type_name": "todo_bool_exp",
            "nullable": true,
            "list": false
        }],
        "filter": filter_semantics(),
        "order": null,
        "pagination": null,
        "aggregate": {
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
        "dependencies": ["todo_rows"],
        "live": false
    })
}

pub(super) fn manifest() -> JsonValue {
    let mut value = json!({
        "manifest_version": 2,
        "protocol_version": 1,
        "service_id": "todos-service",
        "surface": {"kind": "role", "name": "user"},
        "schema_fingerprint": fingerprint("schema"),
        "protocol_fingerprint": "sha256:00fb342f3acb4dc1c1716a43cc3001c748d5f6c500ff831690d820e9e43e2782",
        "execution": {
            "max_depth": 8,
            "max_complexity": 500,
            "max_bool_width": 256,
            "max_in_list": 1000,
            "complexity": {
                "version": 1,
                "scalar": 1,
                "belongs_to": 2,
                "has_many": 10,
                "m2m": 12,
                "aggregate": 8,
                "list_root": 3,
                "by_pk": 1,
                "list_fanout": 5
            }
        },
        "capabilities": {
            "live_queries": true,
            "record_revisions": true,
            "tombstones": true,
            "causal_receipts": false,
            "live_resume": true,
            "query_fallback": "revalidate",
            "cache_scope": true,
            "confirmed_persistence": false
        },
        "scalar_codecs": scalar_codecs(),
        "models": [model("Todo", "todo")],
        "roots": [
            list_root("query"),
            list_root("subscription"),
            by_pk_root(),
            aggregate_root()
        ],
        "commands": [],
        "protocol_operations": {"version": 1},
        "projectors": [],
        "projection_programs": [],
        "projection_bindings": []
    });
    refresh_schema_fingerprint(&mut value);
    value
}

fn install_create_projection(value: &mut JsonValue) {
    const PROGRAM_ID: &str =
        "pp1:sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const BINDING_ID: &str =
        "pb1:sha256:2222222222222222222222222222222222222222222222222222222222222222";
    let event = json!({
        "id": "event:todo.created:v1",
        "name": "todo.created",
        "version": 1
    });
    value["projection_programs"] = json!([{
        "version": 2,
        "program_id": PROGRAM_ID,
        "name": "project_todos",
        "program_version": 1,
        "ir_version": 1,
        "operation_semantics_version": 1,
        "arms": [{
            "arm": "todo-created",
            "event": event,
            "partition": {
                "kind": "expression",
                "expression": {
                    "kind": "slot",
                    "slot": "state.tenantId",
                    "value_type": {"type": "string"}
                }
            },
            "operations": [{
                "operation": "upsert-todo",
                "ordinal": 0,
                "kind": "upsert",
                "model": "Todo",
                "key": [
                    {
                        "ordinal": 0,
                        "name": "tenantId",
                        "expression": {
                            "kind": "slot",
                            "slot": "state.tenantId",
                            "value_type": {"type": "string"}
                        }
                    },
                    {
                        "ordinal": 1,
                        "name": "id",
                        "expression": {
                            "kind": "slot",
                            "slot": "state.id",
                            "value_type": {"type": "string"}
                        }
                    }
                ],
                "fields": [
                    {
                        "ordinal": 0,
                        "name": "completed",
                        "assignment": {
                            "kind": "set",
                            "expression": {
                                "kind": "slot",
                                "slot": "state.completed",
                                "value_type": {"type": "boolean"}
                            }
                        }
                    },
                    {
                        "ordinal": 1,
                        "name": "priority",
                        "assignment": {
                            "kind": "set",
                            "expression": {
                                "kind": "slot",
                                "slot": "state.priority",
                                "value_type": {"type": "i64"}
                            }
                        }
                    },
                    {
                        "ordinal": 2,
                        "name": "title",
                        "assignment": {
                            "kind": "set",
                            "expression": {
                                "kind": "slot",
                                "slot": "state.title",
                                "value_type": {"type": "string"}
                            }
                        }
                    }
                ],
                "relationships": [],
                "invalidations": []
            }]
        }]
    }]);
    value["projection_bindings"] = json!([{
        "version": 1,
        "binding_id": BINDING_ID,
        "program_id": PROGRAM_ID,
        "epoch": "todos-projection-v2",
        "state": "active",
        "placement": "eventual",
        "execution_class": "causal"
    }]);
}

fn custom_scalar_manifest() -> JsonValue {
    let mut value = manifest();
    value["models"][0]["fields"]
        .as_array_mut()
        .expect("model fields")
        .extend([
            json!({"name": "blob", "scalar": "Bytea", "codec": "base64", "nullable": true}),
            json!({"name": "payload", "scalar": "JSON", "codec": "json", "nullable": true}),
            json!({"name": "sequence", "scalar": "BigInt", "codec": "json_number_precision_limited", "nullable": false}),
            json!({"name": "updatedAt", "scalar": "Timestamptz", "codec": "string_unvalidated_timestamp", "nullable": true}),
        ]);
    value["models"][0]["filter_input"]["fields"]
        .as_array_mut()
        .expect("model filter input fields")
        .extend([
            json!({"name": "blob", "operators": ["_eq"]}),
            json!({"name": "payload", "operators": ["_eq"]}),
            json!({"name": "sequence", "operators": ["_eq", "_in"]}),
            json!({"name": "updatedAt", "operators": ["_eq"]}),
        ]);
    for root in value["roots"].as_array_mut().expect("manifest roots") {
        if let Some(fields) = root
            .get_mut("filter")
            .and_then(JsonValue::as_object_mut)
            .and_then(|filter| filter.get_mut("fields"))
            .and_then(JsonValue::as_array_mut)
        {
            fields.extend([
                json!({"name": "blob", "operators": ["_eq"]}),
                json!({"name": "payload", "operators": ["_eq"]}),
                json!({"name": "sequence", "operators": ["_eq", "_in"]}),
                json!({"name": "updatedAt", "operators": ["_eq"]}),
            ]);
        }
        if let Some(fields) = root
            .get_mut("order")
            .and_then(JsonValue::as_object_mut)
            .and_then(|order| order.get_mut("fields"))
            .and_then(JsonValue::as_array_mut)
        {
            fields.extend(["blob", "payload", "sequence", "updatedAt"].map(JsonValue::from));
        }
    }
    refresh_schema_fingerprint(&mut value);
    value
}

fn literal_scalar_manifest() -> JsonValue {
    let mut value = custom_scalar_manifest();
    value["models"][0]["fields"]
        .as_array_mut()
        .expect("model fields")
        .push(json!({
            "name": "ratio",
            "scalar": "Float",
            "codec": "float64",
            "nullable": false
        }));
    value["models"][0]["filter_input"]["fields"]
        .as_array_mut()
        .expect("model filter input fields")
        .push(json!({"name": "ratio", "operators": ["_eq"]}));
    for root in value["roots"].as_array_mut().expect("manifest roots") {
        if let Some(fields) = root
            .get_mut("filter")
            .and_then(JsonValue::as_object_mut)
            .and_then(|filter| filter.get_mut("fields"))
            .and_then(JsonValue::as_array_mut)
        {
            fields.push(json!({"name": "ratio", "operators": ["_eq"]}));
        }
        if let Some(fields) = root
            .get_mut("order")
            .and_then(JsonValue::as_object_mut)
            .and_then(|order| order.get_mut("fields"))
            .and_then(JsonValue::as_array_mut)
        {
            fields.push(json!("ratio"));
        }
    }
    refresh_schema_fingerprint(&mut value);
    value
}

fn projected_manifest() -> JsonValue {
    let mutation = "mutation Client_projectTodo($commandId: ID!, $input: ProjectTodoInput!) { projectTodo(commandId: $commandId, input: $input) { completed id priority tenantId title } }";
    let status =
        "query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }";
    let mut value = manifest();
    value["capabilities"]["causal_receipts"] = json!(true);
    value["capabilities"]["cache_scope"] = json!(true);
    value["commands"] = json!([{
        "version": 1,
        "name": "ProjectTodo",
        "mutation_field": "projectTodo",
        "grants": ["user"],
        "input": {
            "kind": "object",
            "definition": {
                "name": "ProjectTodoInput",
                "fields": [
                    {
                        "name": "id",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    },
                    {
                        "name": "tenantId",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    }
                ]
            }
        },
        "output": {
            "kind": "object",
            "definition": {
                "name": "todo",
                "fields": [
                    {
                        "name": "completed",
                        "type_name": "Boolean",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "boolean"
                    },
                    {
                        "name": "id",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    },
                    {
                        "name": "priority",
                        "type_name": "Int",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "int32"
                    },
                    {
                        "name": "tenantId",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    },
                    {
                        "name": "title",
                        "type_name": "String",
                        "nullable": true,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    }
                ]
            }
        },
        "operation": mutation,
        "operation_hash": fingerprint(mutation),
        "extensions": {
            "version": 2,
            "consistency": {"version": 1, "kind": "atomic"},
            "direct_projection": {
                "topology": {
                    "version": 1,
                    "name": "todos",
                    "digest": fingerprint("todos topology")
                },
                "model": "Todo",
                "partition": {"kind": "input", "path": ["tenantId"]},
                "change_epoch": "todos-v1"
            }
        }
    }]);
    value["projectors"] = json!([{
        "version": 1,
        "name": "todos",
        "facts": [],
        "models": ["Todo"],
        "dependencies": ["todo_rows"],
        "causal_confirmation": false
    }]);
    value["protocol_operations"] = json!({
        "version": 1,
        "command_status": {
            "name": "Distributed_CommandStatus",
            "operation": status,
            "operation_hash": fingerprint(status)
        }
    });
    refresh_schema_fingerprint(&mut value);
    value
}

fn generated_command_types_manifest() -> JsonValue {
    let import = "mutation Client_importTodos($commandId: ID!, $input: ImportTodosInput!) { importTodos(commandId: $commandId, input: $input) { result } }";
    let ping =
        "mutation Client_pingTodos($commandId: ID!) { pingTodos(commandId: $commandId) { ok } }";
    let mut value = projected_manifest();
    value["commands"][0]["name"] = json!("todo.project");
    value["commands"].as_array_mut().expect("commands").extend([
        json!({
            "version": 1,
            "name": "todo.import",
            "mutation_field": "importTodos",
            "grants": ["user"],
            "input": {
                "kind": "object",
                "definition": {
                    "name": "ImportTodosInput",
                    "fields": [{
                        "name": "source",
                        "type_name": "JSON",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "json"
                    }]
                }
            },
            "output": {
                "kind": "object",
                "definition": {
                    "name": "ImportTodosPayload",
                    "fields": [{
                        "name": "result",
                        "type_name": "JSON",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "json"
                    }]
                }
            },
            "operation": import,
            "operation_hash": fingerprint(import),
            "extensions": {
                "version": 2,
                "consistency": {"version": 1, "kind": "succeeded"}
            }
        }),
        json!({
            "version": 1,
            "name": "todo.ping",
            "mutation_field": "pingTodos",
            "grants": ["user"],
            "input": {"kind": "none"},
            "output": {
                "kind": "object",
                "definition": {
                    "name": "PingTodosPayload",
                    "fields": [{
                        "name": "ok",
                        "type_name": "Boolean",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "boolean"
                    }]
                }
            },
            "operation": ping,
            "operation_hash": fingerprint(ping),
            "extensions": {
                "version": 2,
                "consistency": {"version": 1, "kind": "succeeded"}
            }
        }),
    ]);
    install_create_projection(&mut value);
    value["commands"][0]["extensions"]["projection"] = json!({
        "version": 2,
        "event_set": [{
            "id": "event:todo.created:v1",
            "name": "todo.created",
            "version": 1
        }],
        "program_arms": [{
            "event": {
                "id": "event:todo.created:v1",
                "name": "todo.created",
                "version": 1
            },
            "program_id": "pp1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "arm": "todo-created"
        }],
        "preview_occurrences": [{
            "ordinal": 0,
            "event": {
                "id": "event:todo.created:v1",
                "name": "todo.created",
                "version": 1
            },
            "values": [
                {
                    "slot": "state.completed",
                    "source": {
                        "kind": "constant",
                        "value": {"type": "boolean", "value": false}
                    }
                },
                {
                    "slot": "state.id",
                    "source": {"kind": "input", "path": ["id"]}
                },
                {
                    "slot": "state.priority",
                    "source": {
                        "kind": "constant",
                        "value": {"type": "i64", "value": "0"}
                    }
                },
                {
                    "slot": "state.tenantId",
                    "source": {"kind": "input", "path": ["tenantId"]}
                },
                {
                    "slot": "state.title",
                    "source": {"kind": "null"}
                }
            ]
        }],
        "fallback": "revalidate"
    });
    refresh_schema_fingerprint(&mut value);
    value
}

fn embedded_model_invalidation_manifest() -> JsonValue {
    let mut value = generated_command_types_manifest();
    let command = &mut value["commands"][0];
    command["extensions"]["consistency"]["kind"] = json!("eventual");
    command["extensions"]
        .as_object_mut()
        .unwrap()
        .remove("direct_projection");
    command["output"]["definition"]["name"] = json!("ProjectTodoPayload");
    command["extensions"]["projection"]["preview_occurrences"][0]["values"] = json!([{
        "slot": "state.tenantId",
        "source": {"kind": "input", "path": ["tenantId"]}
    }]);
    value["models"][0]["normalization"] = json!({"kind": "embedded"});
    let operation = &mut value["projection_programs"][0]["arms"][0]["operations"][0];
    operation["kind"] = json!("invalidate_model");
    operation["key"] = json!([]);
    operation["fields"] = json!([]);
    operation["relationships"] = json!([]);
    operation["invalidations"] = json!([{"kind": "model", "model": "Todo"}]);
    refresh_schema_fingerprint(&mut value);
    value
}

#[test]
fn embedded_model_invalidation_parses_compiles_and_generates_unkeyed_recovery() {
    let value = embedded_model_invalidation_manifest();
    let manifest = ClientManifest::parse(value.clone(), &ClientSurfaceSelector::role("user"))
        .expect("embedded model invalidation must be valid client authority");
    let command = manifest
        .commands
        .iter()
        .find(|command| command.extensions.projection.is_some())
        .expect("modeled command");
    let compiled = super::projection_delta::compile_command_preview(command, &manifest)
        .expect("embedded invalidation preview must compile")
        .expect("modeled command projection");
    let compiled = serde_json::to_value(compiled).unwrap();
    let mutation = &compiled["preview"]["operations"][0]["mutation"];
    assert_eq!(mutation["op"], "invalidate_model");
    assert_eq!(mutation["model"], "Todo");
    assert!(mutation.get("scope").is_none());
    assert!(mutation.get("key").is_none());
    let recovery = &compiled["preview"]["recoveries"][0];
    assert_eq!(recovery["condition"], "always");
    assert_eq!(recovery["target"]["kind"], "model");
    assert_eq!(recovery["target"]["model"], "Todo");
    assert!(recovery["target"].get("key").is_none());
    assert_eq!(
        compiled["capabilities"]["arms"][0]["mutations"],
        json!([{"kind": "model", "model": "Todo"}])
    );

    let project = compile_client(ClientCompileInput::new(
        value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("generate client for embedded model invalidation");
    let commands = file(&project, "commands.ts");
    assert!(commands.contains("\"op\": \"invalidate_model\""));
    assert!(commands.contains("\"condition\": \"always\""));
    assert!(commands.contains("\"kind\": \"model\""));
    assert!(!commands.contains("\"kind\": \"record\""));
    assert!(!commands.contains("\"key\":"));
}

#[test]
fn embedded_model_rejects_every_identity_dependent_projection_mutation() {
    let template = generated_command_types_manifest();
    let key = template["projection_programs"][0]["arms"][0]["operations"][0]["key"].clone();
    for kind in [
        "insert",
        "upsert",
        "patch",
        "upsert_patch",
        "delete",
        "recreate",
        "insert_related",
        "upsert_related",
        "invalidate_relationship",
    ] {
        let mut value = embedded_model_invalidation_manifest();
        let operation = &mut value["projection_programs"][0]["arms"][0]["operations"][0];
        operation["kind"] = json!(kind);
        operation["key"] = key.clone();
        refresh_schema_fingerprint(&mut value);

        let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
            .expect_err("identity-dependent operations must reject embedded models");
        assert_eq!(error.code, "client.manifest.projection_model", "{kind}");
    }
}

#[test]
fn embedded_model_invalidation_rejects_record_and_relationship_authority() {
    let template = generated_command_types_manifest();
    for member in ["key", "fields"] {
        let mut value = embedded_model_invalidation_manifest();
        value["projection_programs"][0]["arms"][0]["operations"][0][member] =
            template["projection_programs"][0]["arms"][0]["operations"][0][member].clone();
        refresh_schema_fingerprint(&mut value);

        let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
            .expect_err("embedded model invalidation must remain unkeyed and fieldless");
        assert_eq!(
            error.code, "client.manifest.projection_invalidation",
            "{member}"
        );
    }

    let mut value = embedded_model_invalidation_manifest();
    value["projection_programs"][0]["arms"][0]["operations"][0]["relationships"] = json!([{
        "ordinal": 0,
        "kind": "invalidate",
        "source_model": "Todo",
        "relationship": "owner",
        "target_model": "Todo",
        "source_key": [],
        "target_key": []
    }]);
    refresh_schema_fingerprint(&mut value);
    let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect_err("embedded model invalidation cannot carry relationship effects");
    assert_eq!(error.code, "client.manifest.projection_invalidation");
}

#[test]
fn embedded_model_invalidation_requires_canonical_same_model_scope() {
    let mut value = embedded_model_invalidation_manifest();
    value["projection_programs"][0]["arms"][0]["operations"][0]["invalidations"] = json!([]);
    refresh_schema_fingerprint(&mut value);

    let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect_err("embedded model invalidation must retain its canonical model scope");
    assert_eq!(error.code, "client.manifest.projection_invalidation");
}

#[test]
fn embedded_model_invalidation_rejects_relationship_invalidation_scope() {
    let mut value = embedded_model_invalidation_manifest();
    value["projection_programs"][0]["arms"][0]["operations"][0]["invalidations"] = json!([{
        "kind": "relationship",
        "source_model": "Todo",
        "relationship": "owner",
        "target_model": "Todo"
    }]);
    refresh_schema_fingerprint(&mut value);

    let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect_err("embedded model invalidation cannot authorize relationship invalidation");
    assert_eq!(error.code, "client.manifest.projection_invalidation");
}

#[test]
fn embedded_model_invalidation_rejects_foreign_existing_model_scope() {
    let mut value = embedded_model_invalidation_manifest();
    value["models"]
        .as_array_mut()
        .expect("models")
        .push(model("Other", "other"));
    value["projection_programs"][0]["arms"][0]["operations"][0]["invalidations"] =
        json!([{"kind": "model", "model": "Other"}]);
    refresh_schema_fingerprint(&mut value);

    let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect_err("embedded model invalidation cannot authorize a foreign model");
    assert_eq!(error.code, "client.manifest.projection_invalidation");
}

#[test]
fn embedded_model_invalidation_preserves_duplicate_scope_rejection() {
    let mut value = embedded_model_invalidation_manifest();
    let invalidation =
        value["projection_programs"][0]["arms"][0]["operations"][0]["invalidations"][0].clone();
    value["projection_programs"][0]["arms"][0]["operations"][0]["invalidations"] =
        json!([invalidation.clone(), invalidation]);
    refresh_schema_fingerprint(&mut value);

    let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect_err("duplicate embedded model invalidation scope must remain invalid");
    assert_eq!(error.code, "client.manifest.projection_invalidation");
    assert!(
        error.message.contains("repeats an invalidation scope"),
        "{error}"
    );
}

#[test]
fn rejects_legacy_top_level_json_command_shapes() {
    for slot in ["input", "output"] {
        let mut value = projected_manifest();
        value["commands"][0][slot] = json!({"kind": "json", "codec": "json"});

        let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
            .expect_err("top-level JSON command shapes were removed in manifest v7");
        assert_eq!(error.code, "client.manifest.invalid");
        assert!(error.message.contains("unknown variant `json`"));
    }
}

fn input(source: &str) -> ClientCompileInput {
    ClientCompileInput::new(
        manifest(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            source,
        )],
    )
}

fn input_with_manifest(manifest: JsonValue, source: &str) -> ClientCompileInput {
    ClientCompileInput::new(
        manifest,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            source,
        )],
    )
}

const CUSTOM_SCALAR_QUERY: &str = r#"
  query ScalarInputs(
    $where: todo_bool_exp!
    $order: [todo_order_by!]
    $id: ID!
    $big: BigInt!
    $bytes: Bytea
    $json: JSON
    $timestamp: Timestamptz
  ) {
    todos(
      where: {
        _and: [
          $where,
          {
            id: {_eq: $id},
            sequence: {_eq: $big},
            blob: {_eq: $bytes},
            payload: {_eq: $json},
            updatedAt: {_eq: $timestamp}
          }
        ]
      },
      order_by: $order
    ) { id }
  }
"#;

fn custom_scalar_project() -> super::GeneratedClientProject {
    compile_client(input_with_manifest(
        custom_scalar_manifest(),
        CUSTOM_SCALAR_QUERY,
    ))
    .expect("compile recursive custom-scalar inputs")
}

fn row_policy_claim_manifest() -> JsonValue {
    let mut value = manifest();
    let policy = json!({
        "kind": "predicate",
        "expression": {
            "kind": "cmp",
            "value": {
                "column": "tenantId",
                "op": "eq",
                "rhs": {
                    "kind": "claim",
                    "value": {"header": "x-tenant-id"}
                }
            }
        }
    });
    value["models"][0]["row_policy"] = policy.clone();
    for root in value["roots"].as_array_mut().expect("manifest roots") {
        if !root["filter"].is_null() {
            root["filter"]["row_policy"] = policy.clone();
        }
    }
    refresh_schema_fingerprint(&mut value);
    value
}

fn file<'a>(project: &'a super::GeneratedClientProject, path: &str) -> &'a str {
    project
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("missing generated file {path}"))
        .contents
        .as_str()
}

fn operation_artifact(project: &super::GeneratedClientProject) -> JsonValue {
    let operation = &project.operations[0];
    let generated = file(project, &operation.module_path);
    let start = generated
        .rfind(" = {")
        .expect("generated operation artifact")
        + 3;
    let json = generated[start..]
        .strip_suffix(";\n")
        .expect("generated operation artifact terminator");
    serde_json::from_str(json).expect("generated operation artifact JSON")
}

fn object_member<'a>(selection: &'a JsonValue, field: &str) -> &'a JsonValue {
    selection["members"]
        .as_array()
        .expect("selection members")
        .iter()
        .find(|member| member["field"] == field)
        .unwrap_or_else(|| panic!("missing selection member {field}"))
}

fn selected_relationship_artifact(relationship: JsonValue) -> JsonValue {
    let mut value = manifest();
    value["models"][0]["relationships"][0] = relationship;
    refresh_schema_fingerprint(&mut value);
    let project = compile_client(input_with_manifest(
        value,
        "query RelationshipPlan { todos { owner { id } } }",
    ))
    .expect("compile relationship plan");
    operation_artifact(&project)
}

#[test]
fn compiles_aliases_composite_wire_identity_live_and_load() {
    let project = compile_client(input(
        r#"
          query Todos($limit: Int!) @load @live {
            rows: todos(limit: $limit) {
              _distributed_tenantId: title
              headline: title
            }
          }
        "#,
    ))
    .expect("compile");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);

    assert!(generated.contains("query Todos($limit: Int!)"));
    assert!(generated.contains("_distributed_tenantId_2: tenantId"));
    assert!(generated.contains("_distributed_id: id"));
    assert!(generated.contains("_distributed_typename: __typename"));
    assert!(generated.contains("subscription Todos_Live($limit: Int!)"));
    assert!(generated.contains("\"expose\": false"));
    assert!(generated.contains("readonly \"headline\": string | null"));
    assert!(!generated.contains("readonly \"_distributed_id\""));
    assert_eq!(
        operation.operation_hash,
        fingerprint(
            "query Todos($limit: Int!) {\n  rows: todos(limit: $limit) {\n    _distributed_tenantId: title\n    headline: title\n    _distributed_tenantId_2: tenantId\n    _distributed_id: id\n    _distributed_typename: __typename\n  }\n}\n"
        )
    );
    assert!(operation.live_operation_hash.is_some());
}

#[test]
fn operation_artifact_carries_normalized_source_provenance() {
    let project = compile_client(input("query SourceLocated { todos { id } }"))
        .expect("compile source-located operation");
    let artifact = operation_artifact(&project);

    assert_eq!(
        artifact["source"],
        json!({
            "path": "src/routes/todos/+page.graphql",
            "line": 1,
            "column": 1
        })
    );
}

#[test]
fn component_load_compiles_to_a_framework_neutral_island_inventory() {
    let project = compile_client(ClientCompileInput::new(
        manifest(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/lib/todos/TodoSummary.graphql",
            "query TodoSummary($limit: Int!) @load @live { todos(limit: $limit) { id } }",
        )],
    ))
    .expect("component load island compiles before adapter placement");

    assert!(project.files.iter().all(|file| file.path != "routes.ts"));
    assert_eq!(project.islands.len(), 1);
    let island = &project.islands[0];
    assert_eq!(island.version, 1);
    assert_eq!(island.operation, "TodoSummary");
    assert_eq!(island.source.path, "src/lib/todos/TodoSummary.graphql");
    assert!(island.directives.load);
    assert!(island.directives.live);
    assert!(island.live_coverage.finite);
    assert_eq!(island.variable_schema.variables[0].name, "limit");
    assert_eq!(island.variable_schema.variables[0].graphql_type, "Int!");

    let inventory: JsonValue =
        serde_json::from_str(file(&project, "islands.json")).expect("island inventory JSON");
    assert_eq!(inventory["version"], 1);
    assert_eq!(inventory["islands"][0]["id"], island.id);
    assert_eq!(
        inventory["islands"][0]["operationHash"],
        island.operation_hash
    );
    assert!(file(&project, "islands.ts").contains("DISTRIBUTED_ISLAND_OPERATIONS"));
    assert!(!file(&project, "islands.json").contains("svelte"));
    assert!(!file(&project, "islands.ts").contains("svelte"));

    assert!(file(&project, "islands.ts").contains(&island.id));
    assert!(file(&project, "islands.ts").contains("artifact: Operation_TodoSummary"));
}

#[test]
fn multiple_load_islands_may_share_one_future_boundary() {
    let project = compile_client(ClientCompileInput::new(
        manifest(),
        ClientSurfaceSelector::role("user"),
        vec![
            ClientDocument::new(
                "src/lib/todos/TodoCount.graphql",
                "query TodoCount @load { todos { id } }",
            ),
            ClientDocument::new(
                "src/lib/todos/TodoTitles.graphql",
                "query TodoTitles @load { todos { title } }",
            ),
        ],
    ))
    .expect("route ownership is no longer one-operation-per-route");

    assert_eq!(project.islands.len(), 2);
    assert_ne!(project.islands[0].id, project.islands[1].id);
}

#[test]
fn enforces_the_selected_services_exact_depth_and_complexity_limits() {
    let shallow_source = "query ExactDepth { todos { id } }";
    let mut exact_depth_manifest = manifest();
    exact_depth_manifest["execution"]["max_depth"] = json!(2);
    refresh_schema_fingerprint(&mut exact_depth_manifest);
    compile_client(input_with_manifest(exact_depth_manifest, shallow_source))
        .expect("root and leaf fields exactly at max_depth must compile");

    let mut shallow_rejected_manifest = manifest();
    shallow_rejected_manifest["execution"]["max_depth"] = json!(1);
    refresh_schema_fingerprint(&mut shallow_rejected_manifest);
    let error = compile_client(input_with_manifest(
        shallow_rejected_manifest,
        shallow_source,
    ))
    .expect_err("root plus leaf must count as operation depth two");
    assert_eq!(error.code, "client.operation.depth_limit");
    assert!(error.message.contains("operation depth 2"), "{error:?}");

    let mut depth_manifest = manifest();
    depth_manifest["execution"]["max_depth"] = json!(3);
    refresh_schema_fingerprint(&mut depth_manifest);
    let error = compile_client(input_with_manifest(
        depth_manifest,
        "query TooDeep { todos { owner { owner { id } } } }",
    ))
    .expect_err("depth must fail at build time");
    assert_eq!(error.code, "client.operation.depth_limit");
    assert!(error.message.contains("operation depth 4"), "{error:?}");

    let mut exact_deep_manifest = manifest();
    exact_deep_manifest["execution"]["max_depth"] = json!(4);
    refresh_schema_fingerprint(&mut exact_deep_manifest);
    compile_client(input_with_manifest(
        exact_deep_manifest,
        "query ExactDeep { todos { owner { owner { id } } } }",
    ))
    .expect("the exact recursive service depth boundary must compile");

    let source = "query ExactCost { todos { id } }";
    let mut accepted_manifest = manifest();
    accepted_manifest["execution"]["max_complexity"] = json!(18);
    refresh_schema_fingerprint(&mut accepted_manifest);
    compile_client(input_with_manifest(accepted_manifest, source))
        .expect("the exact service complexity boundary must compile");

    let mut rejected_manifest = manifest();
    rejected_manifest["execution"]["max_complexity"] = json!(17);
    refresh_schema_fingerprint(&mut rejected_manifest);
    let error = compile_client(input_with_manifest(rejected_manifest, source))
        .expect_err("complexity must fail at build time");
    assert_eq!(error.code, "client.operation.complexity_limit");
    assert!(
        error.message.contains("operation complexity 18"),
        "{error:?}"
    );
}

#[test]
fn compiles_recursive_normalized_relationships_and_nested_fragments() {
    let project = compile_client(input(
        r#"
          query NestedOwner {
            todos {
              title
              owner { ...OwnerFields }
            }
          }

          fragment OwnerFields on todo { id }
        "#,
    ))
    .expect("compile normalized relationship");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    let canonical = "query NestedOwner {\n  todos {\n    title\n    owner {\n      id\n      _distributed_tenantId: tenantId\n      _distributed_typename: __typename\n    }\n    _distributed_tenantId: tenantId\n    _distributed_id: id\n    _distributed_typename: __typename\n  }\n}\n";

    assert!(
        generated.contains(&serde_json::to_string(canonical).unwrap()),
        "generated:\n{generated}"
    );
    assert_eq!(operation.operation_hash, fingerprint(canonical));
    assert!(generated.contains("readonly \"owner\": {"));
    assert!(generated.contains("} | null;"));
    assert!(generated.contains("\"semantic\": \"relationship\""));
    assert!(generated.contains("\"maintenance\": \"revalidate\""));
    assert!(generated.contains("\"codec\": \"string\""));
    assert!(!generated.contains("fragment OwnerFields"));
}

#[test]
fn emits_exact_relationship_plans_and_injects_direct_through_and_opaque_keys() {
    let mut belongs_to = model("Todo", "todo")["relationships"][0].clone();
    belongs_to["key_mapping"] = json!({
        "kind": "direct",
        "local": ["title"],
        "remote": ["title"]
    });
    belongs_to["maintenance"] = json!("local");

    let mut has_many = normalized_list_relationship();
    has_many["key_mapping"] = json!({
        "kind": "direct",
        "local": ["title"],
        "remote": ["title"]
    });

    let mut many_to_many = normalized_list_relationship();
    many_to_many["kind"] = json!("many_to_many");
    many_to_many["key_mapping"] = json!({
        "kind": "through",
        "local": ["title"],
        "remote": ["title"],
        "table": "todo_members",
        "source_foreign_key": ["todo_id"],
        "target_foreign_key": ["member_id"]
    });
    many_to_many["dependencies"] = json!(["todo_members", "todo_rows"]);

    let mut opaque = normalized_list_relationship();
    opaque["kind"] = json!("many_to_many");
    opaque["key_mapping"] = json!({
        "kind": "through_opaque",
        "local": ["title"],
        "remote": ["title"],
        "dependency": "todo_members"
    });
    opaque["maintenance"] = json!("revalidate");
    opaque["dependencies"] = json!(["todo_members", "todo_rows"]);

    let cases = [
        (
            "belongs_to direct",
            belongs_to,
            json!({
                "field": "owner",
                "targetModel": "Todo",
                "kind": "belongs_to",
                "keyMapping": {
                    "kind": "direct",
                    "local": ["title"],
                    "remote": ["title"]
                },
                "maintenance": "local",
                "dependencies": ["todo_rows"]
            }),
            "one",
        ),
        (
            "has_many direct",
            has_many,
            json!({
                "field": "owner",
                "targetModel": "Todo",
                "kind": "has_many",
                "keyMapping": {
                    "kind": "direct",
                    "local": ["title"],
                    "remote": ["title"]
                },
                "maintenance": "local",
                "dependencies": ["todo_rows"]
            }),
            "many",
        ),
        (
            "many_to_many through",
            many_to_many,
            json!({
                "field": "owner",
                "targetModel": "Todo",
                "kind": "many_to_many",
                "keyMapping": {
                    "kind": "through",
                    "local": ["title"],
                    "remote": ["title"],
                    "table": "todo_members",
                    "sourceForeignKey": ["todo_id"],
                    "targetForeignKey": ["member_id"]
                },
                "maintenance": "local",
                "dependencies": ["todo_members", "todo_rows"]
            }),
            "many",
        ),
        (
            "many_to_many opaque",
            opaque,
            json!({
                "field": "owner",
                "targetModel": "Todo",
                "kind": "many_to_many",
                "keyMapping": {
                    "kind": "through_opaque",
                    "local": ["title"],
                    "remote": ["title"],
                    "dependency": "todo_members"
                },
                "maintenance": "revalidate",
                "dependencies": ["todo_members", "todo_rows"]
            }),
            "many",
        ),
    ];

    for (label, relationship, expected, cardinality) in cases {
        let artifact = selected_relationship_artifact(relationship);
        let root = &artifact["roots"][0];
        let branch = object_member(&root["selection"], "owner");
        assert_eq!(branch["relationship"], expected, "{label}");
        assert_eq!(branch["cardinality"], cardinality, "{label}");
        assert_eq!(
            root["filter"]["relationships"][0], expected,
            "{label} filter catalog"
        );

        let source_key = object_member(&root["selection"], "title");
        assert_eq!(source_key["expose"], false, "{label} source key");
        let target_key = object_member(&branch["selection"], "title");
        assert_eq!(target_key["expose"], false, "{label} target key");
    }
}

#[test]
fn embedded_relationship_plans_remain_revalidate_without_invented_keys() {
    let artifact =
        selected_relationship_artifact(model("Todo", "todo")["relationships"][0].clone());
    let root = &artifact["roots"][0];
    let branch = object_member(&root["selection"], "owner");
    let expected = json!({
        "field": "owner",
        "targetModel": "Todo",
        "kind": "belongs_to",
        "keyMapping": {"kind": "embedded"},
        "maintenance": "revalidate",
        "dependencies": ["todo_rows"]
    });
    assert_eq!(branch["relationship"], expected);
    assert_eq!(root["filter"]["relationships"][0], expected);
    assert!(
        root["selection"]["members"]
            .as_array()
            .expect("root members")
            .iter()
            .all(|member| member["field"] != "title"),
        "embedded mappings must not invent unavailable relationship keys"
    );
}

#[test]
fn relationship_filter_catalog_injects_its_referenced_source_keys() {
    let mut value = manifest();
    let mut relationship = normalized_list_relationship();
    relationship["key_mapping"] = json!({
        "kind": "direct",
        "local": ["title"],
        "remote": ["title"]
    });
    value["models"][0]["relationships"][0] = relationship;
    refresh_schema_fingerprint(&mut value);
    let project = compile_client(input_with_manifest(
        value,
        r#"query RelationshipFilter {
            todos(where: {owner: {title: {_eq: "owned"}}}) { id }
        }"#,
    ))
    .expect("compile relationship predicate");
    let artifact = operation_artifact(&project);
    let root = &artifact["roots"][0];
    assert_eq!(
        root["filter"]["relationships"][0]["keyMapping"],
        json!({
            "kind": "direct",
            "local": ["title"],
            "remote": ["title"]
        })
    );
    let source_key = object_member(&root["selection"], "title");
    assert_eq!(source_key["expose"], false);
}

#[test]
fn compiles_nested_list_arguments_coverage_and_variable_usage() {
    let mut value = manifest();
    value["models"][0]["relationships"][0] = normalized_list_relationship();
    refresh_schema_fingerprint(&mut value);
    let project = compile_client(ClientCompileInput::new(
        value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query NestedList($take: Int!) { todos { copies: owner(limit: $take, offset: 0) { headline: title } } }",
        )],
    ))
    .expect("compile normalized list relationship");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    let canonical = "query NestedList($take: Int!) {\n  todos {\n    copies: owner(limit: $take, offset: 0) {\n      headline: title\n      _distributed_tenantId: tenantId\n      _distributed_id: id\n      _distributed_typename: __typename\n    }\n    _distributed_tenantId: tenantId\n    _distributed_id: id\n    _distributed_typename: __typename\n  }\n}\n";

    assert_eq!(operation.operation_hash, fingerprint(canonical));
    assert!(generated.contains(&serde_json::to_string(canonical).unwrap()));
    assert!(generated.contains("\"cardinality\": \"many\""));
    assert!(generated.contains("\"maintenance\": \"local\""));
    assert!(generated.contains("\"offsetArgument\": \"offset\""));
    assert!(generated.contains("\"limitArgument\": \"limit\""));
    assert!(generated.contains("readonly \"copies\": readonly {"));
}

#[test]
fn compiles_embedded_objects_without_inventing_wire_identity() {
    let mut value = manifest();
    value["models"].as_array_mut().expect("models").push(json!({
        "id": "TodoDetails",
        "typename": "todo_details",
        "source_table": "todo_details_rows",
        "dependencies": ["todo_details_rows"],
        "normalization": {"kind": "embedded"},
        "fields": [{
            "name": "note",
            "scalar": "String",
            "codec": "string",
            "nullable": true
        }],
        "relationships": [],
        "filter_input": {
            "type_name": "todo_details_bool_exp",
            "fields": [
                {"name": "note", "operators": ["_eq"]}
            ],
            "relationships": []
        },
        "row_policy": {"kind": "unrestricted"},
        "record_revisions": false,
        "tombstones": false
    }));
    value["models"][0]["relationships"]
        .as_array_mut()
        .expect("relationships")
        .push(json!({
            "name": "details",
            "target_model": "TodoDetails",
            "target_typename": "todo_details",
            "kind": "belongs_to",
            "list": false,
            "nullable": true,
            "arguments": [],
            "key_mapping": {"kind": "embedded"},
            "maintenance": "revalidate",
            "dependencies": ["todo_details_rows", "todo_rows"],
            "live": false
        }));
    value["models"][0]["filter_input"]["relationships"]
        .as_array_mut()
        .expect("filter input relationships")
        .push(json!({
            "field": "details",
            "target_type": "todo_details_bool_exp"
        }));
    value["roots"][0]["filter"]["relationships"] = json!(["details", "owner"]);
    value["roots"][1]["filter"]["relationships"] = json!(["details", "owner"]);
    value["roots"][3]["filter"]["relationships"] = json!(["details", "owner"]);
    refresh_schema_fingerprint(&mut value);

    let project = compile_client(ClientCompileInput::new(
        value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Embedded { todos { details { note } } }",
        )],
    ))
    .expect("compile embedded relationship");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    let canonical = "query Embedded {\n  todos {\n    details {\n      note\n    }\n    _distributed_tenantId: tenantId\n    _distributed_id: id\n    _distributed_typename: __typename\n  }\n}\n";
    assert_eq!(operation.operation_hash, fingerprint(canonical));
    assert!(generated.contains(&serde_json::to_string(canonical).unwrap()));
    assert!(generated.contains("\"typename\": \"todo_details\""));
    assert!(generated.contains("\"kind\": \"embedded\""));
    assert!(generated.contains("readonly \"note\": string | null;"));
}

#[test]
fn compiles_aggregate_count_and_nodes_with_exact_typenames_and_window() {
    let project = compile_client(input(
        r#"
          query AggregateTodos($where: todo_bool_exp) {
            stats: todos_aggregate(where: $where) { ...AggregateParts }
          }

          fragment AggregateParts on todo_aggregate {
            summary: aggregate { ...CountFields }
            items: nodes { ...TodoFields }
          }
          fragment CountFields on todo_aggregate_fields { total: count }
          fragment TodoFields on todo { headline: title }
        "#,
    ))
    .expect("compile aggregate");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    let canonical = "query AggregateTodos($where: todo_bool_exp) {\n  stats: todos_aggregate(where: $where) {\n    summary: aggregate {\n      total: count\n    }\n    items: nodes {\n      headline: title\n      _distributed_tenantId: tenantId\n      _distributed_id: id\n      _distributed_typename: __typename\n      _distributed_completed: completed\n      _distributed_priority: priority\n    }\n  }\n}\n";

    assert!(
        generated.contains(&serde_json::to_string(canonical).unwrap()),
        "generated:\n{generated}"
    );
    assert_eq!(operation.operation_hash, fingerprint(canonical));
    assert!(generated.contains("\"typename\": \"todo_aggregate\""));
    assert!(generated.contains("\"typename\": \"todo_aggregate_fields\""));
    assert!(generated.contains("\"semantic\": \"aggregate_fields\""));
    assert!(generated.contains("\"semantic\": \"aggregate_nodes\""));
    assert!(generated.contains("\"defaultLimit\": 25"));
    assert!(generated.contains("\"maxLimit\": 100"));
    assert!(generated.contains("readonly \"stats\": {"));
    assert!(generated.contains("readonly \"summary\": {"));
    assert!(generated.contains("readonly \"total\": number;"));
    assert!(generated.contains("readonly \"items\": readonly {"));
    assert!(!generated.contains("fragment AggregateParts"));
}

#[test]
fn compiles_count_only_without_inventing_nodes_or_record_identity() {
    let project = compile_client(input(
        "query CountOnly { summary: todos_aggregate { stats: aggregate { total: count } } }",
    ))
    .expect("compile count-only aggregate");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    let canonical = "query CountOnly {\n  summary: todos_aggregate {\n    stats: aggregate {\n      total: count\n    }\n  }\n}\n";

    assert_eq!(operation.operation_hash, fingerprint(canonical));
    assert!(generated.contains(&serde_json::to_string(canonical).unwrap()));
    assert!(!generated.contains("\"semantic\": \"aggregate_nodes\""));
    assert!(!generated.contains("\"model\": \"Todo\""));
    assert!(generated.contains("readonly \"summary\": {"));
    assert!(generated.contains("readonly \"stats\": {"));
    assert!(generated.contains("readonly \"total\": number;"));
}

#[test]
fn compiles_nullable_relationship_aggregate_from_declared_typenames() {
    let mut value = manifest();
    let mut relationship = normalized_list_relationship();
    relationship["aggregate"] = json!({
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
    });
    value["models"][0]["relationships"][0] = relationship;
    refresh_schema_fingerprint(&mut value);
    let project = compile_client(ClientCompileInput::new(
        value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query RelationshipStats { todos { metrics: owner_aggregate { stats: aggregate { total: count } items: nodes { headline: title } } } }",
        )],
    ))
    .expect("compile relationship aggregate");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    let canonical = "query RelationshipStats {\n  todos {\n    metrics: owner_aggregate {\n      stats: aggregate {\n        total: count\n      }\n      items: nodes {\n        headline: title\n        _distributed_tenantId: tenantId\n        _distributed_id: id\n        _distributed_typename: __typename\n      }\n    }\n    _distributed_tenantId: tenantId\n    _distributed_id: id\n    _distributed_typename: __typename\n  }\n}\n";

    assert_eq!(operation.operation_hash, fingerprint(canonical));
    assert!(generated.contains(&serde_json::to_string(canonical).unwrap()));
    assert!(generated.contains("\"semantic\": \"aggregate\""));
    assert!(generated.contains("\"field\": \"owner_aggregate\""));
    assert!(generated.contains("\"nullable\": true"));
    assert!(generated.contains("readonly \"metrics\": {"));
    assert!(generated.contains("} | null;"));
}

#[test]
fn preserves_enum_wire_syntax_and_json_runtime_value() {
    let project = compile_client(input(
        r#"
          query Ordered {
            todos(order_by: [{priority: asc}]) { id }
          }
        "#,
    ))
    .expect("compile");
    let generated = file(&project, &project.operations[0].module_path);
    assert!(generated.contains("todos(order_by: [{priority: asc}])"));
    assert!(generated.contains("\"priority\": \"asc\""));
    assert!(!generated.contains("priority: \\\"asc\\\""));
}

#[test]
fn emits_executable_filter_order_and_pagination_plans_with_hidden_dependencies() {
    let project = compile_client(input(
        r#"
          query Planned($where: todo_bool_exp, $order: [todo_order_by!]) {
            todos(where: $where, order_by: $order, limit: 10) { id }
          }
        "#,
    ))
    .expect("compile executable index plan");
    let generated = file(&project, &project.operations[0].module_path);

    assert!(generated.contains("\"filter\": {"));
    assert!(generated.contains("\"input\": {"));
    assert!(generated.contains("\"name\": \"where\""));
    assert!(generated.contains("\"rowPolicy\": {"));
    assert!(generated.contains("\"kind\": \"unrestricted\""));
    assert!(generated.contains("\"field\": \"priority\""));
    assert!(generated.contains("\"codec\": \"int32\""));
    assert!(generated.contains("\"order\": {"));
    assert!(generated.contains("\"tieBreakers\": ["));
    assert!(generated.contains("\"pagination\": {"));
    assert!(generated.contains("\"insert\": \"local\""));
    assert!(generated.contains("\"delete\": \"local\""));
    assert!(generated.contains("\"reorder\": \"local\""));
    assert!(generated.contains("\"stableUpdate\": \"local\""));
    assert!(generated.contains("type Operation_Planned_Input_todo_bool_exp = {"));
    assert!(generated.contains(
        "readonly \"_and\"?: Operation_Planned_Input_todo_bool_exp | readonly Operation_Planned_Input_todo_bool_exp[] | null;"
    ));
    assert!(generated.contains("readonly \"_in\"?: number | readonly number[];"));
    assert!(generated.contains("type Operation_Planned_Input_todo_order_by_Direction ="));
    assert!(generated.contains("readonly \"order\"?: Operation_Planned_Input_todo_order_by | readonly Operation_Planned_Input_todo_order_by[] | null;"));
    assert!(generated.contains("\"variableCodec\": {"));
    assert!(generated.contains("\"version\": 2"));
    assert!(generated.contains("\"maxBoolWidth\": 256"));
    assert!(generated.contains("\"maxInList\": 1000"));
    let provenance: JsonValue =
        serde_json::from_str(file(&project, "manifest.json")).expect("compiler manifest JSON");
    assert_eq!(provenance["distributed_manifest_version"], 2);
    assert!(generated.contains(
        "\"target\": {\n              \"kind\": \"input\",\n              \"name\": \"todo_bool_exp\""
    ));
    assert!(!generated.contains("Readonly<Record<string, unknown>>"));

    // A whole-object filter/order variable may reference every authorized
    // field, so the compiler must inject their complete conservative envelope.
    assert!(generated.contains("_distributed_completed: completed"));
    assert!(generated.contains("_distributed_priority: priority"));
    assert!(generated.contains("_distributed_tenantId: tenantId"));
    assert!(generated.contains("_distributed_title: title"));
    let public_data = generated
        .split("export type Operation_Planned_Data =")
        .nth(1)
        .expect("generated public data type")
        .split("/** Exact canonical query bytes")
        .next()
        .expect("public data type boundary");
    assert!(!public_data.contains("readonly \"priority\""));
    assert!(!public_data.contains("readonly \"title\""));
}

#[test]
fn generated_variable_codec_types_recursive_inputs_and_custom_scalars() {
    let project = custom_scalar_project();
    let generated = file(&project, &project.operations[0].module_path);

    assert_eq!(
        generated,
        include_str!("../../tests/fixtures/generated-operation.ts"),
        "the checked-in TypeScript consumer fixture must remain byte-exact"
    );
    assert!(generated.contains("readonly \"id\": string | number;"));
    assert!(generated.contains("readonly \"big\": number;"));
    assert!(generated.contains("readonly \"bytes\"?: string | null;"));
    assert!(generated.contains("readonly \"json\"?: ReplicaValue | null;"));
    assert!(generated.contains("readonly \"timestamp\"?: string | null;"));
    assert!(generated.contains("readonly \"payload\"?: {"));
    assert!(generated.contains("readonly \"_eq\"?: ReplicaValue | null;"));
    assert!(generated.contains("readonly \"_in\"?: number | readonly number[];"));
    assert!(!generated.contains("Readonly<Record<string, unknown>>"));

    let artifact = operation_artifact(&project);
    assert_eq!(artifact["protocol"]["trustedPresets"], json!([]));
    assert_eq!(artifact["variableCodec"]["version"], 2);
    assert_eq!(artifact["variableCodec"]["defaults"], json!({}));
    assert_eq!(
        artifact["variableCodec"]["limits"],
        json!({
            "maxDepth": 8,
            "maxBoolWidth": 256,
            "maxInList": 1000
        })
    );
    assert_eq!(
        artifact["variableCodec"]["variables"]["where"]["filterBaseDepth"],
        1
    );
    assert_eq!(
        artifact["variableCodec"]["variables"]["id"],
        json!({
            "kind": "scalar",
            "scalar": "ID",
            "codec": "string",
            "nullable": false
        })
    );
    assert_eq!(
        artifact["variableCodec"]["variables"]["order"]["kind"],
        "list"
    );
    assert_eq!(
        artifact["variableCodec"]["variables"]["order"]["item"]["name"],
        "todo_order_by"
    );
    let filter_fields = artifact["variableCodec"]["inputs"]["todo_bool_exp"]["fields"]
        .as_array()
        .expect("compiled filter input fields");
    let sequence = filter_fields
        .iter()
        .find(|field| field["field"] == "sequence")
        .expect("BigInt filter field");
    assert_eq!(sequence["scalar"], "BigInt");
    assert_eq!(sequence["codec"], "json_number_precision_limited");
    assert_eq!(
        artifact["variableCodec"]["inputs"]["todo_bool_exp"]["relationships"][0]["target"]["kind"],
        "input"
    );
}

#[test]
fn generated_query_carries_row_policy_claim_contract_without_a_command_runtime() {
    let project = compile_client(input_with_manifest(
        row_policy_claim_manifest(),
        "query Todos { todos { id tenantId } }",
    ))
    .expect("compile client-visible row-policy claim");
    let artifact = operation_artifact(&project);
    assert_eq!(
        artifact["protocol"]["trustedPresets"],
        json!([{"name": "x-tenant-id", "codec": "string"}])
    );
    assert_eq!(
        artifact["roots"][0]["filter"]["rowPolicy"]["expression"]["value"]["rhs"]["value"]
            ["header"],
        "x-tenant-id"
    );
}

#[test]
fn model_filter_contract_resolves_belongs_to_and_has_many_cycles() {
    let mut recursive = manifest();
    let mut children = normalized_list_relationship();
    children["name"] = json!("children");
    children["filter"]["relationships"] = json!(["children", "owner"]);
    recursive["models"][0]["relationships"]
        .as_array_mut()
        .expect("relationships")
        .push(children);
    recursive["models"][0]["filter_input"]["relationships"]
        .as_array_mut()
        .expect("filter input relationships")
        .push(json!({
            "field": "children",
            "target_type": "todo_bool_exp"
        }));
    for root in recursive["roots"].as_array_mut().expect("roots") {
        if let Some(relationships) = root
            .get_mut("filter")
            .and_then(JsonValue::as_object_mut)
            .and_then(|filter| filter.get_mut("relationships"))
        {
            *relationships = json!(["children", "owner"]);
        }
    }
    refresh_schema_fingerprint(&mut recursive);
    let project = compile_client(input_with_manifest(
        recursive,
        "query RecursiveWhere($where: todo_bool_exp) { todos(where: $where) { id } }",
    ))
    .expect("compile explicit self-recursive filter contract");
    let generated = file(&project, &project.operations[0].module_path);

    assert_eq!(
        generated
            .matches("type Operation_RecursiveWhere_Input_todo_bool_exp = {")
            .count(),
        1,
        "recursive aliases must be declared once"
    );
    assert!(generated
        .contains("readonly \"children\"?: Operation_RecursiveWhere_Input_todo_bool_exp | null;"));
    assert!(generated
        .contains("readonly \"owner\"?: Operation_RecursiveWhere_Input_todo_bool_exp | null;"));
    let artifact = operation_artifact(&project);
    let relationships = artifact["variableCodec"]["inputs"]["todo_bool_exp"]["relationships"]
        .as_array()
        .expect("compiled filter input relationships");
    assert_eq!(relationships.len(), 2);
    for relationship in relationships {
        assert_eq!(
            relationship["target"],
            json!({"kind": "input", "name": "todo_bool_exp"})
        );
    }
}

#[test]
fn literal_index_plans_inject_only_referenced_fields_and_validate_shape() {
    let project = compile_client(input(
        "query LiteralPlan { todos(where: {priority: {_eq: 3}}, order_by: [{priority: desc}]) { id } }",
    ))
    .expect("compile literal index plan");
    let generated = file(&project, &project.operations[0].module_path);
    assert!(generated.contains("_distributed_priority: priority"));
    assert!(!generated.contains("_distributed_completed: completed"));
    assert!(!generated.contains("_distributed_title: title"));
    assert!(generated.contains("\"kind\": \"literal\""));
    assert!(generated.contains("\"priority\": {"));
    assert!(generated.contains("\"_eq\": 3"));

    let error = compile_client(input(
        "query BadFilterField { todos(where: {missing: {_eq: 3}}) { id } }",
    ))
    .expect_err("unknown filter field must fail at build time");
    assert_eq!(error.code, "client.filter.field_denied_or_unknown");

    let error = compile_client(input(
        "query BadFilterOperator { todos(where: {priority: {_gt: 3}}) { id } }",
    ))
    .expect_err("unselected filter operator must fail at build time");
    assert_eq!(error.code, "client.filter.operator_denied_or_unknown");

    let error = compile_client(input(
        "query BadFilterLiteral { todos(where: {priority: {_eq: \"bad\"}}) { id } }",
    ))
    .expect_err("filter literals must match the selected scalar codec");
    assert_eq!(error.code, "client.filter.literal_type");

    let error = compile_client(input(
        "query BadFilterList { todos(where: {priority: {_in: [1, \"bad\"]}}) { id } }",
    ))
    .expect_err("every filter list item must match the selected scalar codec");
    assert_eq!(error.code, "client.filter.literal_type");

    let error = compile_client(input(
        "query AmbiguousOrder { todos(order_by: [{priority: asc, id: desc}]) { id } }",
    ))
    .expect_err("ambiguous order priority must fail at build time");
    assert_eq!(error.code, "client.order.ambiguous");
}

#[test]
fn scalar_literals_use_the_same_canonical_domains_as_variables() {
    let project = compile_client(input_with_manifest(
        literal_scalar_manifest(),
        r#"
          query LiteralScalarCodecs {
            todos(
              where: {
                id: {_eq: -0}
                priority: {_eq: -0}
                blob: {_eq: "AQI="}
                payload: {_eq: {z: -0, a: 1, safe: 9007199254740991, decimal: 0.100000000000000005}}
                ratio: {_eq: -0}
                sequence: {_eq: -0}
              }
            ) { id }
          }
        "#,
    ))
    .expect("compile canonical scalar literals");
    let operation = operation_artifact(&project);
    let value = &operation["roots"][0]["filter"]["input"]["value"];
    assert_eq!(value["id"]["_eq"], "0");
    assert_eq!(value["priority"]["_eq"], 0);
    assert_eq!(value["blob"]["_eq"], "AQI=");
    assert_eq!(
        value["payload"]["_eq"],
        json!({"a": 1, "decimal": 0.1, "safe": 9_007_199_254_740_991_u64, "z": 0})
    );
    assert_eq!(value["ratio"]["_eq"], 0);
    assert_eq!(value["sequence"]["_eq"], 0);
    let document = operation["document"].as_str().expect("operation document");
    assert!(document.contains("id: {_eq: \"0\"}"));
    assert!(document.contains("decimal: 0.1"));
    assert!(!document.contains("0.100000000000000005"));

    let rounded_float = compile_client(input_with_manifest(
        literal_scalar_manifest(),
        "query RoundedFloat { todos(where: {ratio: {_eq: 9007199254740993}}) { id } }",
    ))
    .expect("Float literals canonicalize through the JavaScript/server f64 domain");
    let rounded_float = operation_artifact(&rounded_float);
    assert_eq!(
        rounded_float["roots"][0]["filter"]["input"]["value"]["ratio"]["_eq"].as_f64(),
        Some(9_007_199_254_740_992.0)
    );
    let rounded_document = rounded_float["document"]
        .as_str()
        .expect("rounded document");
    assert!(!rounded_document.contains("9007199254740993"));

    let mixed_json = compile_client(input_with_manifest(
        custom_scalar_manifest(),
        "query MixedJson($id: ID!) { todos(where: {id: {_eq: $id}, payload: {_eq: {a: 1}}}) { id } }",
    ))
    .expect("a JSON scalar literal remains a scalar beside an unrelated variable");
    let mixed_json = operation_artifact(&mixed_json);
    assert_eq!(
        mixed_json["roots"][0]["filter"]["input"]["fields"]["payload"]["value"]["_eq"]["a"],
        1
    );

    let by_pk = compile_client(input(
        "query LiteralIds { todo(id: -0, tenantId: 1) { id } }",
    ))
    .expect("GraphQL integer ID literals coerce to canonical strings");
    let by_pk = operation_artifact(&by_pk);
    assert_eq!(by_pk["roots"][0]["arguments"]["id"]["value"], "0");
    assert_eq!(by_pk["roots"][0]["arguments"]["tenantId"]["value"], "1");

    for (source, label) in [
        (
            "query UnsafeId { todos(where: {id: {_eq: 9007199254740992}}) { id } }",
            "unsafe integer ID",
        ),
        (
            "query WideInt { todos(where: {priority: {_eq: 2147483648}}) { id } }",
            "out-of-range Int",
        ),
        (
            "query FractionalBigInt { todos(where: {sequence: {_eq: 1.5}}) { id } }",
            "fractional BigInt",
        ),
        (
            "query UnsafeBigInt { todos(where: {sequence: {_eq: 9007199254740992}}) { id } }",
            "unsafe BigInt",
        ),
        (
            "query NonCanonicalBytea { todos(where: {blob: {_eq: \"AB==\"}}) { id } }",
            "non-canonical Bytea",
        ),
        (
            "query UnsafeJsonInteger { todos(where: {payload: {_eq: {n: 9007199254740993}}}) { id } }",
            "JSON integer that cannot round-trip through JavaScript",
        ),
        (
            "query UnsafeJsonStringifyInteger { todos(where: {payload: {_eq: {n: 36028797018963968}}}) { id } }",
            "JSON integer whose JavaScript decimal serialization changes",
        ),
    ] {
        let error = match compile_client(input_with_manifest(literal_scalar_manifest(), source)) {
            Ok(_) => panic!("{label} must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "client.filter.literal_type", "{label}");
    }

    let error = compile_client(input("query WideLimit { todos(limit: 2147483648) { id } }"))
        .expect_err("root Int literals use the same signed 32-bit domain as variables");
    assert_eq!(error.code, "client.argument.literal_type");
}

#[test]
fn canonicalizes_graphql_singletons_at_every_filter_and_order_list_position() {
    let project = compile_client(input(
        r#"
          query SingletonLists {
            todos(
              where: {
                _and: {priority: {_in: 3}}
                _or: {completed: {_eq: true}}
              }
              order_by: {priority: desc}
            ) { id }
          }
        "#,
    ))
    .expect("GraphQL singleton input coercion must compile");
    let generated = file(&project, &project.operations[0].module_path);
    assert!(generated.contains(
        "todos(order_by: [{priority: desc}], where: {_and: [{priority: {_in: [3]}}], _or: [{completed: {_eq: true}}]})"
    ));

    let artifact = operation_artifact(&project);
    let root = &artifact["roots"][0];
    let expected_filter = json!({
        "kind": "literal",
        "value": {
            "_and": [{"priority": {"_in": [3]}}],
            "_or": [{"completed": {"_eq": true}}]
        }
    });
    let expected_order = json!({
        "kind": "literal",
        "value": [{"priority": "desc"}]
    });
    assert_eq!(root["arguments"]["where"], expected_filter);
    assert_eq!(root["filter"]["input"], expected_filter);
    assert_eq!(root["arguments"]["order_by"], expected_order);
    assert_eq!(root["order"]["input"], expected_order);
}

#[test]
fn enforces_filter_depth_and_width_limits_after_graphql_singleton_coercion() {
    let mut depth_manifest = manifest();
    depth_manifest["execution"]["max_depth"] = json!(2);
    refresh_schema_fingerprint(&mut depth_manifest);
    compile_client(input_with_manifest(
        depth_manifest.clone(),
        r#"
          query ExactFilterDepth {
            todos(where: {
              _not: {
                _not: {
                  _and: null
                  _or: []
                  priority: {_in: [1, 2]}
                }
              }
            }) { id }
          }
        "#,
    ))
    .expect("semantic filter depth exactly at max_depth must compile");

    for source in [
        "query TooDeepNot { todos(where: {_not: {_not: {_not: null}}}) { id } }",
        "query TooDeepRelationship { todos(where: {owner: {owner: {owner: null}}}) { id } }",
    ] {
        let error = compile_client(input_with_manifest(depth_manifest.clone(), source))
            .expect_err("null still enters _not and relationship filter children");
        assert_eq!(error.code, "client.filter.depth_limit", "{source}");
    }

    let mut width_manifest = manifest();
    width_manifest["execution"]["max_bool_width"] = json!(1);
    width_manifest["execution"]["max_in_list"] = json!(1);
    refresh_schema_fingerprint(&mut width_manifest);
    compile_client(input_with_manifest(
        width_manifest.clone(),
        r#"
          query ExactFilterWidths {
            todos(where: {_and: {priority: {_in: 1}}}) { id }
          }
        "#,
    ))
    .expect("singleton boolean and IN inputs coerce to their exact width boundary");

    for (source, label) in [
        (
            "query WideBool { todos(where: {_and: [{priority: {_eq: 1}}, {priority: {_eq: 2}}]}) { id } }",
            "literal boolean list",
        ),
        (
            "query WideIn { todos(where: {priority: {_in: [1, 2]}}) { id } }",
            "literal IN list",
        ),
        (
            "query WideMixedBool($where: todo_bool_exp!) { todos(where: {_and: [$where, {priority: {_eq: 1}}]}) { id } }",
            "mixed boolean list",
        ),
        (
            "query WideMixedIn($priority: Int!) { todos(where: {priority: {_in: [$priority, 1]}}) { id } }",
            "mixed IN list",
        ),
    ] {
        let error = compile_client(input_with_manifest(width_manifest.clone(), source))
            .expect_err("filter width above the exact service boundary must fail");
        assert_eq!(error.code, "client.filter.width_limit", "{label}");
    }
}

#[test]
fn variable_codec_intersects_filter_depth_and_list_constraints() {
    let mut value = manifest();
    value["execution"]["max_bool_width"] = json!(7);
    value["execution"]["max_in_list"] = json!(11);
    refresh_schema_fingerprint(&mut value);
    let project = compile_client(input_with_manifest(
        value,
        r#"
          query VariableConstraints(
            $where: todo_bool_exp!
            $clauses: [todo_bool_exp!]
            $ids: [Int!]
          ) {
            todos(where: {
              _and: [
                $where
                {_not: $where}
                {_and: $clauses}
                {priority: {_in: $ids}}
              ]
            }) { id }
          }
        "#,
    ))
    .expect("compile variables reused at multiple filter positions");
    let artifact = operation_artifact(&project);
    let variables = &artifact["variableCodec"]["variables"];
    assert_eq!(variables["where"]["filterBaseDepth"], 2);
    assert_eq!(variables["clauses"]["maxItems"], 7);
    assert_eq!(variables["clauses"]["item"]["filterBaseDepth"], 2);
    assert_eq!(variables["ids"]["maxItems"], 11);
}

#[test]
fn relationship_selection_and_aggregate_filters_inherit_model_edge_depth() {
    let mut value = manifest();
    let mut relationship = normalized_list_relationship();
    relationship["aggregate"] = json!({
        "name": "owner_aggregate",
        "arguments": list_arguments(),
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
    });
    value["models"][0]["relationships"][0] = relationship;
    refresh_schema_fingerprint(&mut value);
    let project = compile_client(input_with_manifest(
        value,
        r#"
          query EdgeFilterDepth(
            $selectedWhere: todo_bool_exp
            $aggregateWhere: todo_bool_exp
          ) {
            todos {
              owner(where: $selectedWhere) { id }
              owner_aggregate(where: $aggregateWhere) {
                aggregate { count }
              }
            }
          }
        "#,
    ))
    .expect("compile relationship selection and aggregate filter variables");
    let artifact = operation_artifact(&project);
    for variable in ["selectedWhere", "aggregateWhere"] {
        assert_eq!(
            artifact["variableCodec"]["variables"][variable]["filterBaseDepth"], 1,
            "{variable}"
        );
    }
}

#[test]
fn variable_nullability_is_compatible_but_never_weaker() {
    compile_client(input(
        "query Strict($limit: Int!) { todos(limit: $limit) { id } }",
    ))
    .expect("non-null variable may satisfy nullable argument");

    let error = compile_client(input(
        "query Weak($tenantId: ID, $id: ID!) { todo(tenantId: $tenantId, id: $id) { title } }",
    ))
    .expect_err("nullable variable may not satisfy non-null argument");
    assert_eq!(error.code, "client.variable.type_mismatch");

    let error = compile_client(input(
        "query WeakItem($order: [todo_order_by]) { todos(order_by: $order) { id } }",
    ))
    .expect_err("nullable list item may not satisfy non-null item");
    assert_eq!(error.code, "client.variable.type_mismatch");
}

#[test]
fn compiles_nested_filter_and_order_variables_with_recursive_value_sources() {
    let project = compile_client(input(
        r#"
          query Nested($priority: Int!, $direction: order_by!) {
            todos(
              where: {priority: {_eq: $priority}}
              order_by: [{priority: $direction}]
            ) { id }
          }
        "#,
    ))
    .expect("compile nested variables");
    let generated = file(&project, &project.operations[0].module_path);
    assert!(generated.contains("where: {priority: {_eq: $priority}}"));
    assert!(generated.contains("order_by: [{priority: $direction}]"));
    assert!(generated.contains("\"kind\": \"object\""));
    assert!(generated.contains("\"kind\": \"list\""));
    assert!(generated.contains("\"name\": \"priority\""));
    assert!(generated.contains("\"name\": \"direction\""));
    assert!(generated.contains("_distributed_priority: priority"));
    assert!(!generated.contains("_distributed_completed: completed"));
    assert!(generated.contains(
        "readonly \"direction\": \"asc\" | \"asc_nulls_first\" | \"asc_nulls_last\" | \"desc\" | \"desc_nulls_first\" | \"desc_nulls_last\";"
    ));
    assert!(generated.contains("\"kind\": \"enum\""));
    assert!(!generated.contains("Readonly<Record<string, unknown>>"));

    let error = compile_client(input(
        "query WrongNested($priority: String!) { todos(where: {priority: {_eq: $priority}}) { id } }",
    ))
    .expect_err("nested variable type must match its scalar position");
    assert_eq!(error.code, "client.variable.type_mismatch");

    let error = compile_client(input(
        "query MissingNested { todos(where: {priority: {_eq: $missing}}) { id } }",
    ))
    .expect_err("nested variable must be defined");
    assert_eq!(error.code, "client.variable.undefined");

    let error = compile_client(input(
        "query WrongListItem($priority: String!) { todos(where: {priority: {_in: [$priority]}}) { id } }",
    ))
    .expect_err("nested list-item variables must match the non-null scalar item type");
    assert_eq!(error.code, "client.variable.type_mismatch");
}

#[test]
fn compiles_variable_defaults_into_query_types_identity_and_island_plan() {
    let project = compile_client(input(
        "query Defaulted($limit: Int! = 10) @load { todos(limit: $limit) { id } }",
    ))
    .expect("defaults are compiler-owned inputs");
    let operation = &project.operations[0];
    let generated = file(&project, &operation.module_path);
    assert!(generated.contains("readonly \"limit\"?: number;"));
    assert!(generated.contains("query Defaulted($limit: Int! = 10)"));

    let artifact = operation_artifact(&project);
    assert_eq!(artifact["variableCodec"]["version"], 2);
    assert_eq!(artifact["variableCodec"]["defaults"], json!({"limit": 10}));

    let islands: JsonValue =
        serde_json::from_str(file(&project, "islands.json")).expect("island inventory JSON");
    assert_eq!(
        islands["islands"][0]["variableSchema"]["variables"][0]["defaultValue"],
        10
    );
}

#[test]
fn application_surfaces_are_explicit_and_fingerprint_separate_chunks() {
    let document = ClientDocument::new(
        "src/routes/todos/+page.graphql",
        "query Todos { todos { id } }",
    );
    let mut common = manifest();
    common["surface"] = json!({
        "kind": "application",
        "name": "web-common",
        "eligible_roles": ["user"],
        "schema_roles": ["user"],
    });
    refresh_schema_fingerprint(&mut common);
    let common_fingerprint = common["schema_fingerprint"]
        .as_str()
        .expect("common fingerprint")
        .to_string();
    let common_project = compile_client(ClientCompileInput::new(
        common.clone(),
        ClientSurfaceSelector::application("web-common", ["user"], ["user"]),
        vec![document.clone()],
    ))
    .expect("compile exact application surface");
    assert_eq!(common_project.schema_fingerprint, common_fingerprint);
    assert!(
        file(&common_project, "sveltekit.ts").contains("key: \"surface:application:web-common\"")
    );

    let mut changed_contract = common.clone();
    changed_contract["surface"]["eligible_roles"] = json!(["admin", "user"]);
    refresh_schema_fingerprint(&mut changed_contract);
    let changed_project = compile_client(ClientCompileInput::new(
        changed_contract,
        ClientSurfaceSelector::application("web-common", ["admin", "user"], ["user"]),
        vec![document.clone()],
    ))
    .expect("compile changed contract for the same application surface");
    assert!(
        file(&changed_project, "sveltekit.ts").contains("key: \"surface:application:web-common\"")
    );

    let mismatch = compile_client(ClientCompileInput::new(
        common,
        ClientSurfaceSelector::role("user"),
        vec![document.clone()],
    ))
    .expect_err("a role selector cannot relabel an application surface");
    assert_eq!(mismatch.code, "client.manifest.surface_mismatch");

    let mut elevated = manifest();
    elevated["surface"] = json!({
        "kind": "application",
        "name": "web-admin",
        "eligible_roles": ["admin"],
        "schema_roles": ["admin"],
    });
    refresh_schema_fingerprint(&mut elevated);
    let elevated_project = compile_client(ClientCompileInput::new(
        elevated,
        ClientSurfaceSelector::application("web-admin", ["admin"], ["admin"]),
        vec![document],
    ))
    .expect("compile separate elevated application surface");
    assert_ne!(
        common_project.schema_fingerprint,
        elevated_project.schema_fingerprint
    );
}

#[test]
fn sveltekit_wrapper_exposes_framework_neutral_islands_without_routes() {
    let input = ClientCompileInput::new(
        manifest(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/features/todos.graphql",
            "query Todos @load { todos { id } }",
        )],
    );
    let project = compile_client(input).expect("adapter-owned island placement");
    assert!(project.islands[0].directives.load);
    assert!(project.files.iter().all(|file| file.path != "routes.ts"));
    let sveltekit = file(&project, "sveltekit.ts");
    assert!(
        sveltekit.contains(
            "export const Todos = defineDistributedSvelteKitOperation(DistributedOperation_0);"
        ),
        "{sveltekit}"
    );
    assert!(sveltekit.contains("export function provideDistributed("));
    assert!(sveltekit.contains("export function retainBoundary<TSession, TProps>("));
    assert!(sveltekit.contains("return retainDistributedSvelteKitBoundary(instance, context);"));
    assert!(sveltekit.contains("export function useCommands(): GeneratedCommands"));
    assert!(sveltekit.contains("export type GeneratedCommands = Readonly<Record<never, never>>;"));
    assert!(!sveltekit.contains("createGeneratedCommands"));
    assert!(
        !sveltekit.contains("const client ="),
        "generated modules must not retain a module-global client: {sveltekit}"
    );
}

#[test]
fn sveltekit_wrapper_names_cannot_shadow_generated_runtime_exports() {
    let error = compile_client(input(
        r#"
          query useCommands {
            todos { id }
          }
        "#,
    ))
    .expect_err("reserved generated SvelteKit export");
    assert_eq!(error.code, "client.operation.sveltekit_export_collision");
    assert!(error.message.contains("useCommands"));
}

#[test]
fn sveltekit_wrapper_names_fail_closed_on_typescript_keywords() {
    for name in ["default", "await"] {
        let error = compile_client(input(&format!("query {name} {{ todos {{ id }} }}")))
            .expect_err("TypeScript keyword must not become a generated value binding");
        assert_eq!(error.code, "client.operation.sveltekit_identifier");
        assert!(error.message.contains(name), "{error}");
        let source = error.source.expect("keyword error is source-located");
        assert_eq!(source.path, "src/routes/todos/+page.graphql");
        assert_eq!(source.line, 1);
    }

    let project = compile_client(input("query awaited { todos { id } }"))
        .expect("near-keyword operation remains valid");
    assert!(file(&project, "sveltekit.ts").contains("export const awaited ="));
}

#[test]
fn expands_root_named_and_inline_fragments_into_canonical_full_text() {
    let project = compile_client(input(
        r#"
          query Fragmented {
            ...Root
          }
          fragment Root on Query {
            todos {
              ...Fields
              ... { priority }
              ... on todo { headline: title }
              ...Fields
            }
          }
          fragment Fields on todo { id }
        "#,
    ))
    .expect("compile fragments");
    let expected = "query Fragmented {\n  todos {\n    id\n    priority\n    headline: title\n    _distributed_tenantId: tenantId\n    _distributed_typename: __typename\n  }\n}\n";

    assert_eq!(project.operations[0].operation_hash, fingerprint(expected));
    assert!(!file(&project, &project.operations[0].module_path).contains("fragment Root"));
}

#[test]
fn merges_identical_root_objects_and_preserves_first_encounter_order() {
    let project = compile_client(input(
        r#"
          query Merged {
            ...First
            ...Second
          }
          fragment First on Query {
            rows: todos(limit: 10, offset: 0) { title }
          }
          fragment Second on Query {
            rows: todos(offset: 0, limit: 10) { id title }
          }
        "#,
    ))
    .expect("merge compatible root selections");
    let expected = "query Merged {\n  rows: todos(limit: 10, offset: 0) {\n    title\n    id\n    _distributed_tenantId: tenantId\n    _distributed_typename: __typename\n  }\n}\n";

    assert_eq!(project.operations[0].operation_hash, fingerprint(expected));
}

#[test]
fn rejects_fragment_graph_errors_deterministically() {
    let cases = [
        (
            "query Missing { todos { ...Absent } }",
            "client.graphql.fragment_undefined",
            "Absent",
        ),
        (
            "query Cyclic { todos { ...A } } fragment A on todo { ...B } fragment B on todo { ...A }",
            "client.graphql.fragment_cycle",
            "A -> B -> A",
        ),
        (
            "query WrongType { todos { ...Fields } } fragment Fields on Todo { id }",
            "client.graphql.fragment_type",
            "current concrete type is `todo`",
        ),
        (
            "query WrongRoot { ...Root } fragment Root on query { todos { id } }",
            "client.graphql.fragment_type",
            "current concrete type is `Query`",
        ),
        (
            "query WrongInline { todos { ... on Todo { id } } }",
            "client.graphql.fragment_type",
            "current concrete type is `todo`",
        ),
    ];
    for (source, code, message) in cases {
        let error = compile_client(input(source)).expect_err(source);
        assert_eq!(error.code, code, "{source}: {error}");
        assert!(error.message.contains(message), "{source}: {error}");
        assert!(error.source.is_some());
    }

    let error = compile_client(input(
        r#"
          query Unused { todos { id } }
          fragment ZedFirst on todo { id }
          fragment AlphaLater on todo { title }
        "#,
    ))
    .expect_err("source-first unused fragment");
    assert_eq!(error.code, "client.graphql.fragment_unused");
    assert!(error.message.contains("ZedFirst"));
}

#[test]
fn prevalidates_fragment_graph_through_nested_fields_before_lowering() {
    let error = compile_client(input(
        r#"
          query NestedCycle {
            todos { ...Recursive }
          }
          fragment Recursive on todo {
            owner { ...Recursive }
          }
        "#,
    ))
    .expect_err("fragment cycles remain invalid through relationship fields");
    assert_eq!(error.code, "client.graphql.fragment_cycle");
    assert!(error.message.contains("Recursive -> Recursive"));
    assert_eq!(error.source.as_ref().map(|source| source.line), Some(6));

    let error = compile_client(input(
        "query NestedMissing { todos { owner { ...Missing } } }",
    ))
    .expect_err("undefined nested fragments precede unsupported relationship lowering");
    assert_eq!(error.code, "client.graphql.fragment_undefined");
    assert!(error.message.contains("Missing"));
}

#[test]
fn fragment_definition_order_does_not_change_generated_output() {
    let operation = "query Stable { todos { ...First ...Second } }\n";
    let left = compile_client(input(&format!(
        "{operation}fragment First on todo {{ id }}\nfragment Second on todo {{ title }}\n"
    )))
    .expect("first definition order");
    let right = compile_client(input(&format!(
        "{operation}fragment Second on todo {{ title }}\nfragment First on todo {{ id }}\n"
    )))
    .expect("second definition order");

    assert_eq!(left, right);
}

#[test]
fn rejects_directives_on_every_fragment_surface() {
    let cases = [
        (
            "query SpreadDirective { todos { ...Fields @skip(if: true) } } fragment Fields on todo { id }",
            "client.directive.conditional_unsupported",
        ),
        (
            "query DefinitionDirective { todos { ...Fields } } fragment Fields on todo @custom { id }",
            "client.directive.unsupported",
        ),
        (
            "query InlineDirective { todos { ... @custom { id } } }",
            "client.directive.unsupported",
        ),
    ];
    for (source, code) in cases {
        let error = compile_client(input(source)).expect_err(source);
        assert_eq!(error.code, code, "{source}: {error}");
    }
}

#[test]
fn rejects_response_key_conflicts_at_the_later_selection() {
    let source = r#"
      query Conflict {
        todos {
          same: id
          same: title
        }
      }
    "#;
    let error = compile_client(input(source)).expect_err("field-name conflict");
    assert_eq!(error.code, "client.selection.conflict");
    assert_eq!(error.source.as_ref().map(|source| source.line), Some(5));
    assert!(error.message.contains("first selection at 4:"));

    for source in [
        "query ShapeConflict { todos { same: id same: id { value } } }",
        "query ArgumentConflict { rows: todos(limit: 1) { id } rows: todos(limit: 2) { id } }",
    ] {
        let error = compile_client(input(source)).expect_err(source);
        assert_eq!(error.code, "client.selection.conflict", "{source}: {error}");
    }
}

#[test]
fn fragment_expansion_is_bounded_by_depth_and_total_work() {
    let mut deep = String::from("query Deep { todos { ...F0 } }\n");
    for index in 0..65 {
        deep.push_str(&format!(
            "fragment F{index} on todo {{ ...F{} }}\n",
            index + 1
        ));
    }
    deep.push_str("fragment F65 on todo { id }\n");
    let error = compile_client(input(&deep)).expect_err("depth bound");
    assert_eq!(error.code, "client.selection.depth");

    let spreads = "...Fields ".repeat(5_000);
    let expanded =
        format!("query Expanded {{ todos {{ {spreads} }} }} fragment Fields on todo {{ id }}");
    let error = compile_client(input(&expanded)).expect_err("expansion work bound");
    assert_eq!(error.code, "client.selection.expansion_bound");
}

#[test]
fn rejects_conditional_and_multi_root() {
    let cases = [
        (
            "query Conditional($yes: Boolean!) { todos { id @include(if: $yes) } }",
            "client.directive.conditional_unsupported",
        ),
        (
            "query Multi { todos { id } todo(tenantId: \"t\", id: \"1\") { id } }",
            "client.operation.single_root",
        ),
    ];
    for (source, code) in cases {
        let error = compile_client(input(source)).expect_err(source);
        assert_eq!(error.code, code, "{source}: {error}");
        assert!(error.source.is_some());
    }
}

#[test]
fn recursive_selection_shapes_fail_closed() {
    let cases = [
        (
            "query MissingObject { todos { owner } }",
            "client.selection.object_required",
        ),
        (
            "query NestedScalar { todos { title { id } } }",
            "client.selection.scalar_nested",
        ),
        (
            "query NestedUndefined { todos { owner(limit: $missing) { id } } }",
            "client.argument.denied_or_unknown",
        ),
        (
            "query Metric { todos_aggregate { aggregate { sum } } }",
            "client.selection.aggregate_metric_unsupported",
        ),
        (
            "query WrongType { todos_aggregate { ...Wrong } } fragment Wrong on todo { id }",
            "client.graphql.fragment_type",
        ),
    ];
    for (source, code) in cases {
        let error = compile_client(input(source)).expect_err(source);
        assert_eq!(error.code, code, "{source}: {error}");
        assert!(error.source.is_some());
    }
}

#[test]
fn rejects_unknown_scalar_and_codec_instead_of_emitting_unknown() {
    let mut invalid = manifest();
    invalid["scalar_codecs"]
        .as_array_mut()
        .expect("array")
        .push(json!({"scalar": "Money", "codec": "money"}));
    let error = compile_client(ClientCompileInput::new(
        invalid,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect_err("unknown codec");
    assert_eq!(error.code, "client.manifest.scalar_unsupported");
}

#[test]
fn rejects_well_formed_but_incompatible_protocol_fingerprint() {
    let mut invalid = manifest();
    invalid["protocol_fingerprint"] = json!(fingerprint("different protocol"));
    let error = compile_client(ClientCompileInput::new(
        invalid,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect_err("protocol drift");
    assert_eq!(error.code, "client.manifest.protocol_fingerprint");
    assert!(error.message.contains("matching distributed version"));
}

#[test]
fn live_companion_fails_closed_on_dependency_or_pagination_drift() {
    let mut invalid = manifest();
    let subscription = invalid["roots"]
        .as_array_mut()
        .expect("roots")
        .iter_mut()
        .find(|root| root["operation"] == "subscription")
        .expect("subscription");
    subscription["pagination"]["default_limit"] = json!(24);
    refresh_schema_fingerprint(&mut invalid);
    let error = compile_client(ClientCompileInput::new(
        invalid,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos @live { todos { id } }",
        )],
    ))
    .expect_err("live drift");
    assert_eq!(error.code, "client.live.root_mismatch");
}

#[test]
fn command_protocol_and_extensions_are_preserved_exactly() {
    let mutation = "mutation Client_createTodo($commandId: ID!, $input: CreateTodoInput!) { createTodo(commandId: $commandId, input: $input) { id } }";
    let status =
        "query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }";
    let mut value = manifest();
    value["capabilities"]["causal_receipts"] = json!(true);
    value["capabilities"]["cache_scope"] = json!(true);
    value["commands"] = json!([{
        "version": 1,
        "name": "todo.create",
        "mutation_field": "createTodo",
        "grants": ["user"],
        "input": {
            "kind": "object",
            "definition": {
                "name": "CreateTodoInput",
                "fields": [
                    {
                        "name": "id",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    },
                    {
                        "name": "tenantId",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    },
                    {
                        "name": "title",
                        "type_name": "String",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    }
                ]
            }
        },
        "output": {
            "kind": "object",
            "definition": {
                "name": "CreateTodoPayload",
                "fields": [{
                    "name": "id",
                    "type_name": "ID",
                    "nullable": false,
                    "list": false,
                    "item_nullable": false,
                    "codec": "string"
                }]
            }
        },
        "operation": mutation,
        "operation_hash": fingerprint(mutation),
        "extensions": {
            "version": 2,
            "consistency": {"version": 1, "kind": "eventual"},
            "input_defaults": {
                "version": 1,
                "defaults": [{"path": ["id"], "generator": "uuid_v7"}]
            },
            "projection": {
                "version": 2,
                "event_set": [{
                    "id": "event:todo.created:v1",
                    "name": "todo.created",
                    "version": 1
                }],
                "program_arms": [{
                    "event": {
                        "id": "event:todo.created:v1",
                        "name": "todo.created",
                        "version": 1
                    },
                    "program_id": "pp1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "arm": "todo-created"
                }],
                "preview_occurrences": [{
                    "ordinal": 0,
                    "event": {
                        "id": "event:todo.created:v1",
                        "name": "todo.created",
                        "version": 1
                    },
                    "values": [
                        {
                            "slot": "state.completed",
                            "source": {
                                "kind": "constant",
                                "value": {"type": "boolean", "value": false}
                            }
                        },
                        {
                            "slot": "state.id",
                            "source": {"kind": "generated_default", "path": ["id"]}
                        },
                        {
                            "slot": "state.priority",
                            "source": {
                                "kind": "constant",
                                "value": {"type": "i64", "value": "0"}
                            }
                        },
                        {
                            "slot": "state.tenantId",
                            "source": {"kind": "input", "path": ["tenantId"]}
                        },
                        {
                            "slot": "state.title",
                            "source": {"kind": "input", "path": ["title"]}
                        }
                    ]
                }],
                "fallback": "revalidate"
            }
        }
    }]);
    install_create_projection(&mut value);
    value["projectors"] = json!([{
        "version": 1,
        "name": "todos",
        "facts": ["TodoCreated"],
        "models": ["Todo"],
        "dependencies": ["todo_rows"],
        "causal_confirmation": true
    }]);
    value["protocol_operations"] = json!({
        "version": 1,
        "command_status": {
            "name": "Distributed_CommandStatus",
            "operation": status,
            "operation_hash": fingerprint(status)
        }
    });
    refresh_schema_fingerprint(&mut value);
    let mut partial_value = value.clone();
    partial_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"]
        [4]["source"] = json!({"kind": "unknown"});
    refresh_schema_fingerprint(&mut partial_value);
    let mut absent_value = value.clone();
    absent_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"]
        [4]["source"] = json!({"kind": "absent"});
    refresh_schema_fingerprint(&mut absent_value);
    let mut absent_object_value = absent_value.clone();
    absent_object_value["projection_programs"][0]["arms"][0]["operations"][0]["fields"][2]
        ["assignment"]["expression"] = json!({
        "kind": "object",
        "fields": [
            {
                "name": "missing",
                "value": {
                    "kind": "slot",
                    "slot": "state.title",
                    "value_type": {"type": "string"}
                }
            },
            {
                "name": "present",
                "value": {
                    "kind": "slot",
                    "slot": "state.tenantId",
                    "value_type": {"type": "string"}
                }
            }
        ]
    });
    refresh_schema_fingerprint(&mut absent_object_value);
    let mut null_value = value.clone();
    null_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"][4]
        ["source"] = json!({"kind": "null"});
    refresh_schema_fingerprint(&mut null_value);
    let mut unknown_key_value = value.clone();
    unknown_key_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][1]["source"] = json!({"kind": "unknown"});
    refresh_schema_fingerprint(&mut unknown_key_value);
    let mut null_key_value = value.clone();
    null_key_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"]
        [1]["source"] = json!({"kind": "null"});
    refresh_schema_fingerprint(&mut null_key_value);
    let mut nullable_input_key_value = value.clone();
    nullable_input_key_value["commands"][0]["input"]["definition"]["fields"][0]["nullable"] =
        json!(true);
    nullable_input_key_value["commands"][0]["extensions"]
        .as_object_mut()
        .unwrap()
        .remove("input_defaults");
    nullable_input_key_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][1]["source"] = json!({"kind": "input", "path": ["id"]});
    refresh_schema_fingerprint(&mut nullable_input_key_value);
    let mut json_list_key_value = value.clone();
    json_list_key_value["projection_programs"][0]["arms"][0]["operations"][0]["key"][1]
        ["expression"]["value_type"] = json!({"type": "json"});
    json_list_key_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][1]["source"] = json!({
        "kind": "constant",
        "value": {
            "type": "list",
            "value": [{"type": "string", "value": "todo-1"}]
        }
    });
    refresh_schema_fingerprint(&mut json_list_key_value);
    let mut json_object_key_value = value.clone();
    json_object_key_value["projection_programs"][0]["arms"][0]["operations"][0]["key"][1]
        ["expression"]["value_type"] = json!({"type": "json"});
    json_object_key_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][1]["source"] = json!({
        "kind": "constant",
        "value": {
            "type": "object",
            "value": [{
                "name": "id",
                "value": {"type": "string", "value": "todo-1"}
            }]
        }
    });
    refresh_schema_fingerprint(&mut json_object_key_value);
    let mut unknown_partition_value = value.clone();
    unknown_partition_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][3]["source"] = json!({"kind": "unknown"});
    refresh_schema_fingerprint(&mut unknown_partition_value);
    let mut delete_value = value.clone();
    delete_value["projection_programs"][0]["arms"][0]["operations"][0]["kind"] = json!("delete");
    delete_value["projection_programs"][0]["arms"][0]["operations"][0]["fields"] = json!([]);
    delete_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"] = json!([
        {
            "slot": "state.id",
            "source": {"kind": "generated_default", "path": ["id"]}
        },
        {
            "slot": "state.tenantId",
            "source": {"kind": "input", "path": ["tenantId"]}
        }
    ]);
    refresh_schema_fingerprint(&mut delete_value);
    let mut multi_model_value = value.clone();
    multi_model_value["models"]
        .as_array_mut()
        .unwrap()
        .push(model("User", "user"));
    let mut second_operation =
        multi_model_value["projection_programs"][0]["arms"][0]["operations"][0].clone();
    second_operation["operation"] = json!("upsert-user");
    second_operation["ordinal"] = json!(1);
    second_operation["model"] = json!("User");
    multi_model_value["projection_programs"][0]["arms"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .push(second_operation);
    refresh_schema_fingerprint(&mut multi_model_value);
    let capability_boundaries = [65_usize, 128].map(|operation_count| {
        let mut boundary = value.clone();
        let template = boundary["projection_programs"][0]["arms"][0]["operations"][0].clone();
        let mut models = boundary["models"].as_array().unwrap().clone();
        let mut operations = Vec::with_capacity(operation_count);
        for index in 0..operation_count {
            let model_id = if index == 0 {
                "Todo".to_owned()
            } else {
                let model_id = format!("ProjectionModel{index:03}");
                models.push(model(&model_id, &format!("ProjectionModel{index:03}")));
                model_id
            };
            let mut operation = template.clone();
            operation["operation"] = json!(format!("upsert-model-{index:03}"));
            operation["ordinal"] = json!(index);
            operation["model"] = json!(model_id);
            operations.push(operation);
        }
        boundary["models"] = JsonValue::Array(models);
        boundary["projection_programs"][0]["arms"][0]["operations"] = JsonValue::Array(operations);
        refresh_schema_fingerprint(&mut boundary);
        (operation_count, boundary)
    });
    let mut input_i64_value = value.clone();
    input_i64_value["commands"][0]["input"]["definition"]["fields"]
        .as_array_mut()
        .unwrap()
        .insert(
            1,
            json!({
                "name": "priority",
                "type_name": "Int",
                "nullable": false,
                "list": false,
                "item_nullable": false,
                "codec": "int32"
            }),
        );
    input_i64_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][2]["source"] = json!({"kind": "input", "path": ["priority"]});
    refresh_schema_fingerprint(&mut input_i64_value);
    let mut preset_i64_value = value.clone();
    preset_i64_value["commands"][0]["extensions"]["trusted_presets"] =
        json!([{"name": "priority", "codec": "int32"}]);
    preset_i64_value["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][2]["source"] =
        json!({"kind": "trusted_preset", "name": "priority", "codec": "int32"});
    refresh_schema_fingerprint(&mut preset_i64_value);
    let mut invalid_u64_source = input_i64_value.clone();
    invalid_u64_source["projection_programs"][0]["arms"][0]["operations"][0]["fields"][1]
        ["assignment"]["expression"]["value_type"] = json!({"type": "u64"});
    refresh_schema_fingerprint(&mut invalid_u64_source);
    let mut invalid_constant_source = value.clone();
    invalid_constant_source["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][2]["source"] =
        json!({"kind": "constant", "value": {"type": "string", "value": "zero"}});
    refresh_schema_fingerprint(&mut invalid_constant_source);
    let mut invalid_default_source = value.clone();
    invalid_default_source["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][2]["source"] = json!({"kind": "generated_default", "path": ["id"]});
    refresh_schema_fingerprint(&mut invalid_default_source);
    let mut invalid_list_source = input_i64_value.clone();
    invalid_list_source["commands"][0]["input"]["definition"]["fields"][1]["list"] = json!(true);
    refresh_schema_fingerprint(&mut invalid_list_source);
    let mut invalid_preset_source = value.clone();
    invalid_preset_source["commands"][0]["extensions"]["trusted_presets"] =
        json!([{"name": "priority", "codec": "json"}]);
    invalid_preset_source["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"][2]["source"] =
        json!({"kind": "trusted_preset", "name": "priority", "codec": "json"});
    refresh_schema_fingerprint(&mut invalid_preset_source);
    let mut conflicting_slot_type = value.clone();
    conflicting_slot_type["projection_programs"][0]["arms"][0]["operations"][0]["fields"][2]
        ["assignment"]["expression"] = json!({
        "kind": "slot",
        "slot": "state.id",
        "value_type": {"type": "boolean"}
    });
    refresh_schema_fingerprint(&mut conflicting_slot_type);
    let mut top_level_relationship_invalidation = value.clone();
    top_level_relationship_invalidation["projection_programs"][0]["arms"][0]["operations"][0]
        ["kind"] = json!("invalidate_relationship");
    refresh_schema_fingerprint(&mut top_level_relationship_invalidation);
    let mut no_preview_value = value.clone();
    no_preview_value["models"]
        .as_array_mut()
        .unwrap()
        .push(model("User", "user"));
    no_preview_value["commands"][0]["extensions"]["projection"]["preview_occurrences"] = json!([]);
    refresh_schema_fingerprint(&mut no_preview_value);
    let mut occurrence_boundary = value.clone();
    let occurrence = occurrence_boundary["commands"][0]["extensions"]["projection"]
        ["preview_occurrences"][0]
        .clone();
    occurrence_boundary["commands"][0]["extensions"]["projection"]["preview_occurrences"] =
        JsonValue::Array(
            (0..128)
                .map(|ordinal| {
                    let mut occurrence = occurrence.clone();
                    occurrence["ordinal"] = json!(ordinal);
                    occurrence
                })
                .collect(),
        );
    refresh_schema_fingerprint(&mut occurrence_boundary);
    let mut too_many_occurrences = occurrence_boundary.clone();
    let mut extra_occurrence = too_many_occurrences["commands"][0]["extensions"]["projection"]
        ["preview_occurrences"][0]
        .clone();
    extra_occurrence["ordinal"] = json!(128);
    too_many_occurrences["commands"][0]["extensions"]["projection"]["preview_occurrences"]
        .as_array_mut()
        .unwrap()
        .push(extra_occurrence);
    refresh_schema_fingerprint(&mut too_many_occurrences);
    let mut value_boundary = value.clone();
    let extra_slots = (0..123)
        .map(|index| format!("zz{index:03}"))
        .collect::<Vec<_>>();
    value_boundary["projection_programs"][0]["arms"][0]["partition"]["expression"] = json!({
        "kind": "list",
        "values": extra_slots
            .iter()
            .map(|slot| json!({
                "kind": "slot",
                "slot": slot,
                "value_type": {"type": "string"}
            }))
            .collect::<Vec<_>>()
    });
    value_boundary["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"]
        .as_array_mut()
        .unwrap()
        .extend(extra_slots.iter().map(|slot| {
            json!({
                "slot": slot,
                "source": {
                    "kind": "constant",
                    "value": {"type": "string", "value": slot}
                }
            })
        }));
    refresh_schema_fingerprint(&mut value_boundary);
    let mut too_many_values = value_boundary.clone();
    too_many_values["projection_programs"][0]["arms"][0]["partition"]["expression"]["values"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "kind": "slot",
            "slot": "zz123",
            "value_type": {"type": "string"}
        }));
    too_many_values["commands"][0]["extensions"]["projection"]["preview_occurrences"][0]["values"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "slot": "zz123",
            "source": {
                "kind": "constant",
                "value": {"type": "string", "value": "zz123"}
            }
        }));
    refresh_schema_fingerprint(&mut too_many_values);
    let project = compile_client(ClientCompileInput::new(
        value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile commands");
    let commands = file(&project, "commands.ts");
    let protocol = file(&project, "protocol.ts");
    let sveltekit = file(&project, "sveltekit.ts");
    assert!(commands.contains(mutation));
    assert!(commands.contains("createReplicaCommandRuntime"));
    assert!(commands.contains("prepareReplicaCommand"));
    assert!(commands.contains("ReplicaCommandArtifact,"));
    assert!(commands.contains("export type Command_createTodo_Input"));
    assert!(commands.contains("readonly \"id\"?: string;"));
    assert!(commands.contains("readonly \"tenantId\": string;"));
    assert!(commands.contains("\"mutationField\": \"createTodo\""));
    assert!(commands.contains("\"inputDefaults\""));
    assert!(commands.contains("\"projection\""));
    assert!(commands.contains("\"version\": 2"));
    assert!(commands.contains("\"deltaWireVersion\": 1"));
    assert!(commands.contains("\"projectionProgramVersion\": 2"));
    assert!(commands.contains("\"operationSemanticsVersion\": 1"));
    assert!(commands.contains("\"op\": \"upsert\""));
    assert!(commands.contains("\"requires\": \"current_cache_partition\""));
    assert!(commands.contains("\"programId\": \"pp1:sha256:1111111111111111111111111111111111111111111111111111111111111111\""));
    assert!(commands.contains("\"bindingId\": \"pb1:sha256:2222222222222222222222222222222222222222222222222222222222222222\""));
    assert!(!commands.contains("\"token\""));
    assert!(!commands.contains("\"effects\""));
    assert!(!commands.contains("\"confirmations\""));
    assert!(commands.contains("\"consistency\": \"eventual\""));
    assert!(commands.contains("\"revalidation\""));
    assert!(commands.contains("\"trustedPresets\": []"));
    assert!(commands.contains("export function prepareCommand_createTodo"));
    assert!(commands.contains("export const COMMAND_ARTIFACTS = [Command_createTodo] as const;"));
    assert!(commands.contains("export const COMMANDS = {"));
    assert!(commands.contains("\"todo.create\": Command_createTodo"));
    assert!(commands.contains("export function createCommands("));
    assert!(commands.contains("import { COMMAND_STATUS } from './protocol.js';"));
    assert!(!commands.contains("export const commands ="));
    assert!(!commands.contains("{ artifact: Command_createTodo"));
    assert!(!commands.contains("Command_todo.create"));
    assert!(!commands.contains("\"extensions\""));
    assert!(protocol.contains(status));
    assert!(protocol.contains(&fingerprint(status)));
    assert!(protocol.contains("import type { ReplicaCommandStatusArtifact }"));
    assert!(protocol.contains("export const COMMAND_STATUS: ReplicaCommandStatusArtifact"));
    assert!(protocol.contains("\"document\": \"query Distributed_CommandStatus"));
    assert!(protocol.contains("\"schemaHash\":"));
    assert!(protocol.contains("\"protocolHash\":"));
    assert!(protocol.contains("\"surface\": {"));
    assert!(protocol.contains("\"trustedPresets\": []"));
    assert!(protocol.contains("\ttrustedPresets: []"));
    assert!(sveltekit.contains("createCommands as createGeneratedCommands"));
    assert!(sveltekit.contains("type GeneratedCommands"));
    assert!(sveltekit.contains("export type { GeneratedCommands } from './commands.js';"));
    assert!(sveltekit.contains("createCommands: createGeneratedCommands"));
    assert!(!sveltekit.contains("Readonly<Record<never, never>>"));

    let partial = compile_client(ClientCompileInput::new(
        partial_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile partial projection preview");
    let partial_commands = file(&partial, "commands.ts");
    assert!(partial_commands.contains("\"op\": \"patch\""));
    assert!(!partial_commands.contains("\"op\": \"upsert\""));
    assert!(partial_commands.contains("\"condition\": \"if_record_missing\""));
    assert!(partial_commands.contains("\"kind\": \"record\""));
    assert!(partial_commands.contains("\"required\": false"));

    let absent = compile_client(ClientCompileInput::new(
        absent_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile absent projection field without clearing cached state");
    let absent_commands = file(&absent, "commands.ts");
    assert!(absent_commands.contains("\"op\": \"patch\""));
    assert!(!absent_commands.contains("\"unset\""));
    assert!(absent_commands.contains("\"field\": \"completed\""));
    assert!(absent_commands.contains("\"field\": \"priority\""));
    assert!(absent_commands.contains("\"condition\": \"if_record_missing\""));
    assert!(!absent_commands.contains("\"op\": \"upsert\""));

    let absent_object = compile_client(ClientCompileInput::new(
        absent_object_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile object-member absence as a whole-expression absence");
    let absent_object_commands = file(&absent_object, "commands.ts");
    assert!(absent_object_commands.contains("\"op\": \"patch\""));
    assert!(absent_object_commands.contains("\"condition\": \"if_record_missing\""));
    assert!(!absent_object_commands.contains("\"name\": \"missing\""));
    assert!(!absent_object_commands.contains("\"op\": \"upsert\""));

    let null = compile_client(ClientCompileInput::new(
        null_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile known null as a complete projection value");
    let null_commands = file(&null, "commands.ts");
    assert!(null_commands.contains("\"op\": \"upsert\""));
    assert!(null_commands.contains("\"kind\": \"null\""));
    assert!(!null_commands.contains("\"condition\": \"if_record_missing\""));

    for fallback in [
        unknown_key_value,
        null_key_value,
        nullable_input_key_value,
        json_list_key_value,
        json_object_key_value,
        unknown_partition_value,
    ] {
        let project = compile_client(ClientCompileInput::new(
            fallback,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
        .expect("compile conservative projection fallback");
        let fallback_commands = file(&project, "commands.ts");
        assert!(fallback_commands.contains("\"operations\": []"));
        assert!(fallback_commands.contains("\"kind\": \"model\""));
        assert!(!fallback_commands.contains("\"op\": \"upsert\""));
        assert!(!fallback_commands.contains("\"op\": \"patch\""));
        assert!(fallback_commands.contains("\"required\": true"));
    }

    let delete = compile_client(ClientCompileInput::new(
        delete_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile delete projection preview");
    let delete_commands = file(&delete, "commands.ts");
    assert!(delete_commands.contains("\"op\": \"delete\""));
    assert!(!delete_commands.contains("\"op\": \"upsert\""));
    assert!(!delete_commands.contains("\"op\": \"patch\""));

    let multi_model = compile_client(ClientCompileInput::new(
        multi_model_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile one projection arm spanning multiple models");
    let multi_model_commands = file(&multi_model, "commands.ts");
    assert_eq!(
        multi_model_commands.matches("\"op\": \"upsert\"").count(),
        2
    );
    assert!(multi_model_commands.contains("\"model\": \"Todo\""));
    assert!(multi_model_commands.contains("\"model\": \"User\""));

    for (operation_count, boundary) in capability_boundaries {
        let compiled = compile_client(ClientCompileInput::new(
            boundary,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
        .expect("expanded capabilities remain valid at the operation boundary");
        let commands = file(&compiled, "commands.ts");
        assert_eq!(
            commands.matches("\"kind\": \"record\"").count(),
            operation_count
        );
        assert_eq!(
            commands.matches("\"kind\": \"model\"").count(),
            operation_count
        );
    }

    for valid_i64 in [input_i64_value, preset_i64_value] {
        compile_client(ClientCompileInput::new(
            valid_i64,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
        .expect("signed Int/int32 sources prove an I64 preview slot");
    }

    for invalid_source in [
        invalid_u64_source,
        invalid_constant_source,
        invalid_default_source,
        invalid_list_source,
        invalid_preset_source,
    ] {
        let error = compile_client(ClientCompileInput::new(
            invalid_source,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
        .expect_err("tampered projection preview source type must fail closed");
        assert_eq!(error.code, "client.manifest.command_projection_source_type");
    }

    let error = compile_client(ClientCompileInput::new(
        conflicting_slot_type,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect_err("one opaque slot cannot claim conflicting value types");
    assert_eq!(error.code, "client.manifest.command_projection_slot_type");

    let error = compile_client(ClientCompileInput::new(
        top_level_relationship_invalidation,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect_err("top-level relationship invalidation lacks source-key provenance");
    assert_eq!(error.code, "client.manifest.projection_invalidation");

    let no_preview = compile_client(ClientCompileInput::new(
        no_preview_value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile static revalidation targets without optimistic occurrences");
    let no_preview_commands = file(&no_preview, "commands.ts");
    assert!(no_preview_commands.contains("\"models\": [\n      \"Todo\"\n"));
    assert!(!no_preview_commands.contains("\"User\""));
    assert!(!no_preview_commands.contains("\"user_rows\""));

    for boundary in [occurrence_boundary, value_boundary] {
        compile_client(ClientCompileInput::new(
            boundary,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
        .expect("projection preview accepts the frozen 128-entry boundary");
    }
    for overflow in [too_many_occurrences, too_many_values] {
        let error = compile_client(ClientCompileInput::new(
            overflow,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
        .expect_err("projection preview rejects 129 entries");
        assert!(matches!(
            error.code,
            "client.manifest.command_projection_inventory"
                | "client.manifest.command_projection_preview_inventory"
        ));
    }
}

#[test]
fn manifest_v2_parses_exact_projection_program_binding_and_preview_contract() {
    let mutation = "mutation Client_createTodo($commandId: ID!, $input: CreateTodoInput!) { createTodo(commandId: $commandId, input: $input) { id } }";
    let status =
        "query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }";
    let mut value = manifest();
    value["capabilities"]["causal_receipts"] = json!(true);
    value["commands"] = json!([{
        "version": 1,
        "name": "todo.create",
        "mutation_field": "createTodo",
        "grants": ["user"],
        "input": {
            "kind": "object",
            "definition": {
                "name": "CreateTodoInput",
                "fields": [
                    {"name": "id", "type_name": "ID", "nullable": false, "list": false, "item_nullable": false, "codec": "string"},
                    {"name": "tenantId", "type_name": "ID", "nullable": false, "list": false, "item_nullable": false, "codec": "string"},
                    {"name": "title", "type_name": "String", "nullable": false, "list": false, "item_nullable": false, "codec": "string"}
                ]
            }
        },
        "output": {
            "kind": "object",
            "definition": {
                "name": "CreateTodoPayload",
                "fields": [{"name": "id", "type_name": "ID", "nullable": false, "list": false, "item_nullable": false, "codec": "string"}]
            }
        },
        "operation": mutation,
        "operation_hash": fingerprint(mutation),
        "extensions": {
            "version": 2,
            "consistency": {"version": 1, "kind": "eventual"},
            "input_defaults": {
                "version": 1,
                "defaults": [{"path": ["id"], "generator": "uuid_v7"}]
            },
            "projection": {
                "version": 2,
                "event_set": [{"id": "event:todo.created:v1", "name": "todo.created", "version": 1}],
                "program_arms": [{
                    "event": {"id": "event:todo.created:v1", "name": "todo.created", "version": 1},
                    "program_id": "pp1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "arm": "todo-created"
                }],
                "preview_occurrences": [{
                    "ordinal": 0,
                    "event": {"id": "event:todo.created:v1", "name": "todo.created", "version": 1},
                    "values": [
                        {"slot": "state.completed", "source": {"kind": "constant", "value": {"type": "boolean", "value": false}}},
                        {"slot": "state.id", "source": {"kind": "generated_default", "path": ["id"]}},
                        {"slot": "state.priority", "source": {"kind": "constant", "value": {"type": "i64", "value": "0"}}},
                        {"slot": "state.tenantId", "source": {"kind": "input", "path": ["tenantId"]}},
                        {"slot": "state.title", "source": {"kind": "input", "path": ["title"]}}
                    ]
                }],
                "fallback": "revalidate"
            }
        }
    }]);
    value["protocol_operations"] = json!({
        "version": 1,
        "command_status": {
            "name": "Distributed_CommandStatus",
            "operation": status,
            "operation_hash": fingerprint(status)
        }
    });
    install_create_projection(&mut value);
    refresh_schema_fingerprint(&mut value);

    let parsed = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect("manifest-v2 projection contract");
    assert_eq!(parsed.projection_programs.len(), 1);
    assert_eq!(parsed.projection_programs[0].version, 2);
    assert!(matches!(
        parsed.projection_programs[0].arms[0].partition,
        super::manifest::ManifestProjectionPartition::Expression { .. }
    ));
    assert_eq!(parsed.projection_bindings.len(), 1);
    assert!(parsed.commands[0].extensions.effects.is_none());
    assert!(parsed.commands[0].extensions.confirmations.is_none());
    assert_eq!(
        parsed.commands[0]
            .extensions
            .projection
            .as_ref()
            .expect("projection extension")
            .preview_occurrences
            .len(),
        1
    );
}

#[test]
fn manifest_v2_has_no_legacy_projection_or_command_authority_decoder() {
    let mut stale_manifest = manifest();
    stale_manifest["manifest_version"] = json!(1);
    refresh_schema_fingerprint(&mut stale_manifest);
    let error = ClientManifest::parse(stale_manifest, &ClientSurfaceSelector::role("user"))
        .expect_err("manifest v1 must fail closed");
    assert_eq!(error.code, "client.manifest.version");

    let mut stale_command = projected_manifest();
    stale_command["commands"][0]["extensions"]["version"] = json!(1);
    refresh_schema_fingerprint(&mut stale_command);
    let error = ClientManifest::parse(stale_command, &ClientSurfaceSelector::role("user"))
        .expect_err("command extensions v1 must fail closed");
    assert_eq!(error.code, "client.manifest.command_extensions");

    let mut legacy_effects = projected_manifest();
    legacy_effects["commands"][0]["extensions"]["effects"] =
        json!({"version": 1, "operations": [], "fallback": "revalidate"});
    let error = ClientManifest::parse(legacy_effects, &ClientSurfaceSelector::role("user"))
        .expect_err("legacy effects must not deserialize");
    assert_eq!(error.code, "client.manifest.invalid");

    let mut unknown_projection_field = manifest();
    install_create_projection(&mut unknown_projection_field);
    unknown_projection_field["projection_programs"][0]["unexpected"] = json!(true);
    let error = ClientManifest::parse(
        unknown_projection_field,
        &ClientSurfaceSelector::role("user"),
    )
    .expect_err("unknown projection fields must fail closed");
    assert_eq!(error.code, "client.manifest.invalid");

    let mut unsupported_program = manifest();
    install_create_projection(&mut unsupported_program);
    unsupported_program["projection_programs"][0]["version"] = json!(3);
    refresh_schema_fingerprint(&mut unsupported_program);
    let error = ClientManifest::parse(unsupported_program, &ClientSurfaceSelector::role("user"))
        .expect_err("unknown projection program versions must fail closed");
    assert_eq!(error.code, "client.manifest.projection_program_version");

    let mut duplicate_operation = manifest();
    install_create_projection(&mut duplicate_operation);
    let mut duplicate =
        duplicate_operation["projection_programs"][0]["arms"][0]["operations"][0].clone();
    duplicate["ordinal"] = json!(1);
    duplicate_operation["projection_programs"][0]["arms"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    refresh_schema_fingerprint(&mut duplicate_operation);
    let error = ClientManifest::parse(duplicate_operation, &ClientSurfaceSelector::role("user"))
        .expect_err("projection operation IDs must be unique within a program");
    assert_eq!(error.code, "client.manifest.projection_operation_id");
}

#[test]
fn generated_command_typescript_covers_typed_json_fields_and_no_input_wrappers() {
    let project = compile_client(ClientCompileInput::new(
        generated_command_types_manifest(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile generated command type fixture");
    let commands = file(&project, "commands.ts");
    assert_eq!(
        commands,
        include_str!("../../tests/fixtures/generated-commands.ts")
    );
}

#[test]
fn generated_command_namespaces_reject_prefix_collisions() {
    let mut value = generated_command_types_manifest();
    value["commands"][0]["name"] = json!("todo");
    value["commands"][1]["name"] = json!("todo.complete");
    refresh_schema_fingerprint(&mut value);

    let error = compile_client(ClientCompileInput::new(
        value,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect_err("a command path cannot also be a namespace");
    assert_eq!(error.code, "client.command.namespace_collision");
    assert!(error.message.contains("`todo`"));
    assert!(error.message.contains("`todo.complete`"));
    assert!(error.message.contains("prefixes"));
}

#[test]
fn projected_command_requires_exact_role_safe_direct_projection() {
    let value = projected_manifest();
    let parsed = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect("valid projected direct target");
    assert!(parsed.projectors[0].facts.is_empty());
    assert!(!parsed.projectors[0].causal_confirmation);
    let direct = parsed.commands[0]
        .extensions
        .direct_projection
        .as_ref()
        .expect("projected command direct target");
    assert_eq!(direct.topology.version, 1);
    assert_eq!(direct.topology.name, "todos");
    assert_eq!(direct.model, "Todo");
    assert_eq!(direct.change_epoch, "todos-v1");
    assert!(matches!(
        direct.partition,
        Some(super::manifest::ManifestEffectExpression::Input { ref path })
            if path.as_slice() == ["tenantId"]
    ));

    let project = compile_client(input_with_manifest(
        projected_manifest(),
        "query Todos { todos { id } }",
    ))
    .expect("compile projected command");
    let commands = file(&project, "commands.ts");
    assert!(commands.contains("\"identityFields\": [\n      \"tenantId\",\n      \"id\"\n    ]"));
    assert!(!commands.contains("\"identity_fields\""));

    let mut absent = projected_manifest();
    absent["commands"][0]["extensions"]
        .as_object_mut()
        .expect("extensions")
        .remove("direct_projection");
    refresh_schema_fingerprint(&mut absent);
    let error = ClientManifest::parse(absent, &ClientSurfaceSelector::role("user"))
        .expect_err("projected direct target is mandatory");
    assert_eq!(error.code, "client.manifest.direct_projection_required");

    let mut non_projected = projected_manifest();
    non_projected["commands"][0]["extensions"]["consistency"]["kind"] = json!("succeeded");
    refresh_schema_fingerprint(&mut non_projected);
    let error = ClientManifest::parse(non_projected, &ClientSurfaceSelector::role("user"))
        .expect_err("succeeded commands cannot carry direct projection metadata");
    assert_eq!(error.code, "client.manifest.direct_projection_unexpected");

    let mut impossible_confirmation = projected_manifest();
    impossible_confirmation["projectors"][0]["causal_confirmation"] = json!(true);
    refresh_schema_fingerprint(&mut impossible_confirmation);
    let error = ClientManifest::parse(
        impossible_confirmation,
        &ClientSurfaceSelector::role("user"),
    )
    .expect_err("a direct-only owner cannot confirm asynchronous work");
    assert_eq!(error.code, "client.manifest.projector_inventory");

    let mut embedded = projected_manifest();
    embedded["models"][0]["normalization"] = json!({"kind": "embedded"});
    refresh_schema_fingerprint(&mut embedded);
    let error = ClientManifest::parse(embedded, &ClientSurfaceSelector::role("user"))
        .expect_err("direct projection requires a complete normalized identity");
    assert_eq!(error.code, "client.manifest.direct_projection_model");
}

#[test]
fn projected_command_rejects_tampered_topology_and_wrong_owner() {
    let mut tampered = projected_manifest();
    tampered["commands"][0]["extensions"]["direct_projection"]["topology"]["digest"] =
        json!(format!("sha256:{}", "AB".repeat(32)));
    refresh_schema_fingerprint(&mut tampered);
    let error = ClientManifest::parse(tampered, &ClientSurfaceSelector::role("user"))
        .expect_err("topology digest must be canonical lowercase SHA-256");
    assert_eq!(error.code, "client.manifest.hash");

    let mut wrong_owner = projected_manifest();
    wrong_owner["commands"][0]["extensions"]["direct_projection"]["topology"]["name"] =
        json!("other_projector");
    refresh_schema_fingerprint(&mut wrong_owner);
    let error = ClientManifest::parse(wrong_owner, &ClientSurfaceSelector::role("user"))
        .expect_err("direct target must use the visible model owner");
    assert_eq!(error.code, "client.manifest.direct_projection_owner");

    let mut trusted_preset = projected_manifest();
    trusted_preset["commands"][0]["extensions"]["direct_projection"]["partition"] =
        json!({"kind": "trusted_preset", "name": "current_tenant"});
    trusted_preset["commands"][0]["extensions"]["trusted_presets"] =
        json!([{"name": "current_tenant", "codec": "string"}]);
    refresh_schema_fingerprint(&mut trusted_preset);
    ClientManifest::parse(trusted_preset.clone(), &ClientSurfaceSelector::role("user"))
        .expect("scope-bound trusted preset may define a direct target");
    let project = compile_client(input_with_manifest(
        trusted_preset,
        "query Todos { todos { id } }",
    ))
    .expect("compile trusted-preset client surface");
    assert_eq!(
        operation_artifact(&project)["protocol"]["trustedPresets"],
        json!([{"name": "current_tenant", "codec": "string"}]),
        "every query artifact carries the exact surface-wide preset contract"
    );
}

#[test]
fn rejects_commands_without_causal_identity_or_normative_input_defaults() {
    let valid = "mutation Client_createTodo($commandId: ID!, $input: CreateTodoInput!) { createTodo(commandId: $commandId, input: $input) { id } }";
    let status = "query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }";
    let command = |operation: &str, generator: &str| {
        json!({
            "version": 1,
            "name": "CreateTodo",
            "mutation_field": "createTodo",
            "grants": ["user"],
            "input": {
                "kind": "object",
                "definition": {
                    "name": "CreateTodoInput",
                    "fields": [{
                        "name": "id",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    }]
                }
            },
            "output": {
                "kind": "object",
                "definition": {
                    "name": "CreateTodoPayload",
                    "fields": [{
                        "name": "id",
                        "type_name": "ID",
                        "nullable": false,
                        "list": false,
                        "item_nullable": false,
                        "codec": "string"
                    }]
                }
            },
            "operation": operation,
            "operation_hash": fingerprint(operation),
            "extensions": {
                "version": 2,
                "consistency": {"version": 1, "kind": "atomic"},
                "input_defaults": {
                    "version": 1,
                    "defaults": [{"path": ["id"], "generator": generator}]
                }
            }
        })
    };
    let compile = |entry: JsonValue| {
        let mut value = manifest();
        value["capabilities"]["causal_receipts"] = json!(true);
        value["capabilities"]["cache_scope"] = json!(true);
        value["commands"] = json!([entry]);
        value["protocol_operations"] = json!({
            "version": 1,
            "command_status": {
                "name": "Distributed_CommandStatus",
                "operation": status,
                "operation_hash": fingerprint(status)
            }
        });
        compile_client(ClientCompileInput::new(
            value,
            ClientSurfaceSelector::role("user"),
            vec![ClientDocument::new(
                "src/routes/todos/+page.graphql",
                "query Todos { todos { id } }",
            )],
        ))
    };

    let without_id =
        "mutation Client_createTodo($input: CreateTodoInput!) { createTodo(input: $input) { id } }";
    assert_eq!(
        compile(command(without_id, "uuid_v7"))
            .expect_err("missing causal identity")
            .code,
        "client.manifest.command_operation"
    );
    assert_eq!(
        compile(command(valid, "uuid_v4"))
            .expect_err("unsupported generator")
            .code,
        "client.manifest.input_default_generator"
    );
}

#[test]
fn output_is_identical_for_shuffled_documents() {
    let mut left_manifest = manifest();
    left_manifest["models"]
        .as_array_mut()
        .expect("models")
        .push(model("Unused", "unused"));
    refresh_schema_fingerprint(&mut left_manifest);
    let right_manifest = left_manifest.clone();
    let mut tampered_manifest = left_manifest.clone();
    for key in ["models", "roots", "scalar_codecs", "projectors", "commands"] {
        tampered_manifest[key]
            .as_array_mut()
            .expect("set-like array")
            .reverse();
    }
    let docs = vec![
        ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        ),
        ClientDocument::new(
            "src/routes/todo/+page.graphql",
            "query Todo { todo(tenantId: \"t\", id: \"1\") { title } }",
        ),
    ];
    let left = compile_client(ClientCompileInput::new(
        left_manifest,
        ClientSurfaceSelector::role("user"),
        docs.clone(),
    ))
    .expect("left");
    let right = compile_client(ClientCompileInput::new(
        right_manifest,
        ClientSurfaceSelector::role("user"),
        docs.into_iter().rev().collect(),
    ))
    .expect("right");
    assert_eq!(left, right);

    let error = compile_client(ClientCompileInput::new(
        tampered_manifest,
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect_err("manifest order is part of the emitter-owned fingerprint");
    assert_eq!(error.code, "client.manifest.schema_fingerprint");
}

#[test]
fn manifest_parser_accepts_embedded_by_pk_and_zero_sized_windows() {
    let mut value = manifest();
    value["models"][0]["normalization"] = json!({"kind": "embedded"});
    value["roots"]
        .as_array_mut()
        .expect("roots")
        .iter_mut()
        .find(|root| root["kind"] == "by_pk")
        .expect("by-pk root")["arguments"] = json!([]);
    for root in value["roots"].as_array_mut().expect("roots") {
        if root["kind"] == "list" {
            root["pagination"]["default_limit"] = json!(0);
            root["pagination"]["max_limit"] = json!(0);
        }
    }
    refresh_schema_fingerprint(&mut value);

    ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect("embedded models and zero-sized authorized windows are valid v6 contracts");
}

#[test]
fn manifest_parser_rejects_execution_limits_outside_javascript_integer_range() {
    for name in ["max_depth", "max_bool_width", "max_in_list"] {
        let mut value = manifest();
        value["execution"][name] = json!(9_007_199_254_740_992_u64);
        refresh_schema_fingerprint(&mut value);
        let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
            .expect_err("runtime codec limits must remain exact JavaScript integers");
        assert_eq!(error.code, "client.manifest.execution_js_integer", "{name}");
    }
}

#[test]
fn manifest_parser_rejects_list_roots_without_executable_query_plan_semantics() {
    let cases = [
        ("filter", &["filter"][..], "client.manifest.root_filter"),
        ("order", &["order"][..], "client.manifest.root_order"),
        (
            "pagination",
            &["limit", "offset"][..],
            "client.manifest.root_pagination",
        ),
    ];

    for (semantic, removed_argument_kinds, expected_code) in cases {
        let mut value = manifest();
        let root = value["roots"]
            .as_array_mut()
            .expect("roots")
            .iter_mut()
            .find(|root| root["operation"] == "query" && root["kind"] == "list")
            .expect("query list root");
        root[semantic] = JsonValue::Null;
        root["arguments"]
            .as_array_mut()
            .expect("list arguments")
            .retain(|argument| {
                let kind = argument["kind"].as_str().expect("argument kind");
                !removed_argument_kinds.contains(&kind)
            });
        refresh_schema_fingerprint(&mut value);

        let error = ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
            .expect_err("list roots require the complete executable query-plan contract");
        assert_eq!(error.code, expected_code, "missing {semantic}");
    }
}

#[test]
fn manifest_parser_rejects_filter_input_target_and_argument_drift() {
    let mut wrong_target = manifest();
    wrong_target["models"][0]["filter_input"]["relationships"][0]["target_type"] =
        json!("other_bool_exp");
    refresh_schema_fingerprint(&mut wrong_target);
    let error = ClientManifest::parse(wrong_target, &ClientSurfaceSelector::role("user"))
        .expect_err("relationship filter targets must resolve to the target model contract");
    assert_eq!(error.code, "client.manifest.filter_input_target_type");

    let mut wrong_argument = manifest();
    wrong_argument["roots"][0]["arguments"][0]["type_name"] = json!("other_bool_exp");
    refresh_schema_fingerprint(&mut wrong_argument);
    let error = ClientManifest::parse(wrong_argument, &ClientSurfaceSelector::role("user"))
        .expect_err("root filter arguments must name the model filter contract");
    assert_eq!(error.code, "client.manifest.filter_argument_type");
}

#[test]
fn manifest_parser_accepts_mixed_per_model_revision_evidence() {
    let mut value = manifest();
    let mut without_evidence = model("Unowned", "Unowned");
    without_evidence["record_revisions"] = json!(false);
    without_evidence["tombstones"] = json!(false);
    value["models"]
        .as_array_mut()
        .expect("models")
        .push(without_evidence);
    value["capabilities"]["record_revisions"] = json!(false);
    value["capabilities"]["tombstones"] = json!(false);
    refresh_schema_fingerprint(&mut value);

    ClientManifest::parse(value, &ClientSurfaceSelector::role("user"))
        .expect("global record evidence describes the selected query footprint, not any model");
}

#[test]
fn source_paths_fail_closed_before_they_can_inject_generated_typescript() {
    let error = compile_client(ClientCompileInput::new(
        manifest(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/injected*/\nexport const compromised = true;\n/**.graphql",
            "query Safe { todos { id } }",
        )],
    ))
    .expect_err("reject adversarial source path");
    assert_eq!(error.code, "client.documents.invalid_path");
    assert!(!error.message.contains("export const compromised"));
}

#[test]
fn generated_lazy_commands_defer_definitions_and_preserve_command_signatures() {
    let project = compile_client(ClientCompileInput::new(
        generated_command_types_manifest(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "todos.graphql",
            "query Todos { todos { id } }",
        )],
    ))
    .expect("compile lazy commands");
    let lazy = file(&project, "lazy-commands.ts");
    assert!(lazy.contains("import type { COMMANDS, GeneratedCommandRuntimeOptions }"));
    assert!(lazy.contains("import('./commands.js')"));
    assert!(lazy.contains("\"hasInput\": false"));
    assert!(lazy.contains("\"hasInput\": true"));
    let wrapper = file(&project, "sveltekit.ts");
    assert!(wrapper.contains("export function provideDistributedLazy("));
    assert!(wrapper.contains("createCommands: createLazyCommands"));
    assert!(wrapper.contains("createCommands: createGeneratedCommands"));
    assert_eq!(
        lazy.replace("'./commands.js'", "'./generated-commands.js'"),
        include_str!("../../tests/fixtures/generated-lazy-commands.ts")
    );
}
