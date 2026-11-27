//! Build-time checks of filter and order arguments in operation documents,
//! one row per rule, through the whole compiler.
//!
//! A filter or order argument reaches the validator in one of two forms: a
//! JSON literal when it holds no variable, or a tree of compiled arguments
//! when any part of it is a variable. Rows cover each rule in both forms.

use serde_json::{json, Value as JsonValue};

use crate::client_compiler::tests::{custom_scalar_manifest, refresh_schema_fingerprint};
use crate::client_compiler::{
    compile_client, ClientCompileError, ClientCompileInput, ClientDocument, ClientSurfaceIdentity,
};

/// The custom-scalar manifest with `_is_null` on `title` and `_has_key` on
/// the JSON `payload`, in the model's filter input and every root filter.
fn operator_manifest() -> JsonValue {
    let mut value = custom_scalar_manifest();
    let operators = [
        ("title", json!(["_eq", "_is_null"])),
        ("payload", json!(["_eq", "_has_key"])),
    ];
    let set = |fields: &mut JsonValue| {
        for field in fields.as_array_mut().expect("filter fields") {
            for (name, ops) in &operators {
                if field["name"] == *name {
                    field["operators"] = ops.clone();
                }
            }
        }
    };
    set(&mut value["models"][0]["filter_input"]["fields"]);
    for root in value["roots"].as_array_mut().expect("roots") {
        if !root["filter"].is_null() {
            set(&mut root["filter"]["fields"]);
        }
    }
    refresh_schema_fingerprint(&mut value);
    value
}

fn compile(source: &str) -> Result<(), ClientCompileError> {
    let input = ClientCompileInput::from_manifest_json(
        operator_manifest(),
        ClientSurfaceIdentity::role("user"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            source,
        )],
    )?;
    compile_client(input).map(|_| ())
}

struct Row {
    name: &'static str,
    source: &'static str,
    code: &'static str,
}

#[test]
fn filter_and_order_arguments_outside_the_manifest_are_refused() {
    let rows = [
        // Filters written as one literal.
        Row {
            name: "literal boolean list holding a non-object",
            source: "query Q { todos(where: {_and: [3]}) { id } }",
            code: "client.filter.object_required",
        },
        Row {
            name: "literal field compared to a bare value",
            source: "query Q { todos(where: {priority: 3}) { id } }",
            code: "client.filter.comparison_object",
        },
        Row {
            name: "literal _is_null with a number",
            source: "query Q { todos(where: {title: {_is_null: 3}}) { id } }",
            code: "client.filter.boolean_required",
        },
        Row {
            name: "literal filter on an unknown field of a related model",
            source: "query Q { todos(where: {owner: {secret: {_eq: 1}}}) { id } }",
            code: "client.filter.field_denied_or_unknown",
        },
        // Filters holding a variable somewhere.
        Row {
            name: "field compared to a bare value beside a variable",
            source: "query Q($id: ID!) { todos(where: {priority: 3, id: {_eq: $id}}) { id } }",
            code: "client.filter.comparison_object",
        },
        Row {
            name: "unselected operator beside a variable",
            source: "query Q($id: ID!) { todos(where: {priority: {_gt: 3}, id: {_eq: $id}}) { id } }",
            code: "client.filter.operator_denied_or_unknown",
        },
        Row {
            name: "IN list item of the wrong scalar beside a variable",
            source: "query Q($id: ID!) { todos(where: {priority: {_in: [1, \"x\"]}, id: {_eq: $id}}) { id } }",
            code: "client.filter.literal_type",
        },
        Row {
            name: "_is_null with a number beside a variable",
            source: "query Q($id: ID!) { todos(where: {title: {_is_null: 3}, id: {_eq: $id}}) { id } }",
            code: "client.filter.boolean_required",
        },
        Row {
            name: "_has_key with a number beside a variable",
            source: "query Q($id: ID!) { todos(where: {payload: {_has_key: 3}, id: {_eq: $id}}) { id } }",
            code: "client.filter.literal_type",
        },
        Row {
            name: "unselected operator inside a field holding a variable",
            source: "query Q($p: Int!) { todos(where: {priority: {_eq: $p, _gt: 1}}) { id } }",
            code: "client.filter.operator_denied_or_unknown",
        },
        Row {
            name: "IN item of the wrong scalar beside a variable item",
            source: "query Q($p: Int!) { todos(where: {priority: {_in: [$p, \"x\"]}}) { id } }",
            code: "client.filter.literal_type",
        },
        Row {
            name: "IN variable item of the wrong type",
            source: "query Q($p: String!) { todos(where: {priority: {_in: [$p, 1]}}) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "literal operand of the wrong scalar beside a variable operand",
            source: "query Q($p: Int!) { todos(where: {priority: {_in: [$p], _eq: \"x\"}}) { id } }",
            code: "client.filter.literal_type",
        },
        Row {
            name: "_is_null with a number beside a variable operand",
            source: "query Q($t: String) { todos(where: {title: {_eq: $t, _is_null: 3}}) { id } }",
            code: "client.filter.boolean_required",
        },
        Row {
            name: "_is_null variable that is not a boolean",
            source: "query Q($t: String) { todos(where: {title: {_is_null: $t}}) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "_has_key variable that is not a string",
            source: "query Q($k: Int) { todos(where: {payload: {_has_key: $k}}) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "_has_key with a number beside a variable operand",
            source: "query Q($j: JSON) { todos(where: {payload: {_eq: $j, _has_key: 3}}) { id } }",
            code: "client.filter.literal_type",
        },
        Row {
            name: "unknown field of a related model beside a variable",
            source: "query Q($id: ID!) { todos(where: {owner: {secret: {_eq: $id}}}) { id } }",
            code: "client.filter.field_denied_or_unknown",
        },
        Row {
            name: "boolean list item variable of the wrong input type",
            source: "query Q($w: String!) { todos(where: {_or: [$w]}) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "boolean list variable of the wrong input type",
            source: "query Q($w: [String!]) { todos(where: {_or: $w}) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "filter that is a list beside a variable",
            source: "query Q($id: ID!) { todos(where: [{id: {_eq: $id}}]) { id } }",
            code: "client.filter.object_required",
        },
        Row {
            name: "field compared to a variable instead of an operator object",
            source: "query Q($p: Int!) { todos(where: {priority: $p}) { id } }",
            code: "client.filter.comparison_object",
        },
        Row {
            name: "IN item that is a list holding a variable",
            source: "query Q($p: Int!) { todos(where: {priority: {_in: [[$p]]}}) { id } }",
            code: "client.filter.operand",
        },
        Row {
            name: "JSON operand that is an object holding a variable",
            source: "query Q($j: JSON) { todos(where: {payload: {_eq: {nested: $j}}}) { id } }",
            code: "client.filter.operand",
        },
        Row {
            name: "_has_key operand that is a list holding a variable",
            source: "query Q($k: String) { todos(where: {payload: {_has_key: [$k]}}) { id } }",
            code: "client.filter.operand",
        },
        // Order written as one literal.
        Row {
            name: "literal order entry that is not an object",
            source: "query Q { todos(order_by: [3]) { id } }",
            code: "client.order.object_required",
        },
        Row {
            name: "literal order on an unknown field",
            source: "query Q { todos(order_by: [{secret: asc}]) { id } }",
            code: "client.selection.dependency_denied",
        },
        Row {
            name: "literal order direction that is not an enum",
            source: "query Q { todos(order_by: [{priority: 1}]) { id } }",
            code: "client.order.direction",
        },
        Row {
            name: "literal order direction absent from the manifest",
            source: "query Q { todos(order_by: [{priority: sideways}]) { id } }",
            code: "client.order.direction",
        },
        // Order holding a variable somewhere.
        Row {
            name: "order entry that is a list holding a variable",
            source: "query Q($o: todo_order_by!) { todos(order_by: [[$o]]) { id } }",
            code: "client.order.object_required",
        },
        Row {
            name: "order entry variable of the wrong input type",
            source: "query Q($o: String!) { todos(order_by: [$o]) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "order entry with two fields beside a variable",
            source: "query Q($d: order_by!) { todos(order_by: [{priority: asc, id: $d}]) { id } }",
            code: "client.order.ambiguous",
        },
        Row {
            name: "order direction variable of the wrong type",
            source: "query Q($d: String!) { todos(order_by: [{priority: $d}]) { id } }",
            code: "client.variable.type_mismatch",
        },
        Row {
            name: "literal order direction that is not an enum beside a variable entry",
            source: "query Q($o: todo_order_by!) { todos(order_by: [$o, {priority: 1}]) { id } }",
            code: "client.order.direction",
        },
        Row {
            name: "literal order direction absent from the manifest beside a variable entry",
            source: "query Q($d: order_by!) { todos(order_by: [{id: $d}, {priority: sideways}]) { id } }",
            code: "client.order.direction",
        },
    ];
    for row in rows {
        let error = compile(row.source)
            .err()
            .unwrap_or_else(|| panic!("{}: operation was accepted", row.name));
        assert_eq!(error.code, row.code, "{}: {}", row.name, error.message);
    }
}

/// Every operator form the validator accepts, in both forms, compiles.
#[test]
fn filter_and_order_arguments_inside_the_manifest_compile() {
    for source in [
        "query Q { todos(where: {_and: null, title: {_is_null: true}, payload: {_has_key: \"a\"}}) { id } }",
        "query Q($id: ID!) { todos(where: {_and: null, id: {_eq: $id}, priority: {_in: [1, 2]}, title: {_is_null: false}, payload: {_has_key: \"a\"}}) { id } }",
        "query Q($p: Int!, $b: Boolean, $k: String) { todos(where: {priority: {_in: [1, 2], _eq: $p, _nin: [$p]}, title: {_is_null: $b}, payload: {_has_key: $k}}) { id } }",
        "query Q($t: String) { todos(where: {title: {_eq: $t, _is_null: null}, payload: {_eq: null, _has_key: \"a\"}}) { id } }",
        "query Q($w: todo_bool_exp!) { todos(where: {_or: [$w, {priority: {_eq: 1}}], _not: {owner: {id: {_eq: \"x\"}}}}) { id } }",
        "query Q($o: todo_order_by!, $d: order_by!) { todos(order_by: [$o, {priority: $d}, {id: desc}]) { id } }",
        "query Q { todos(where: null, order_by: null) { id } }",
    ] {
        compile(source).unwrap_or_else(|error| panic!("{source}: {error:?}"));
    }
}
