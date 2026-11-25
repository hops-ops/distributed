//! `@window`: a live head plus keyset pages of one list operation.
//!
//! The directive is sugar the compiler lowers before anything else reads the
//! document. For `order_by: [{created_at: desc}]` over a model keyed by
//! `message_id` it adds one nullable cursor variable per keyset column and
//! ANDs this predicate into `where`:
//!
//! ```graphql
//! _or: [
//!   {created_at: {_lt: $cursor_created_at}}
//!   {created_at: {_eq: $cursor_created_at}, message_id: {_gt: $cursor_message_id}}
//! ]
//! ```
//!
//! The keyset is the `order_by` columns followed by every primary-key column
//! not already listed, ascending, because the server always appends the
//! primary key ascending. With the cursor omitted every comparison has a
//! `null` operand and adds no predicate, so the same document is the head
//! page. Everything after this module (filter validation, variable codec,
//! coverage, live companion) sees an ordinary filtered list.

use super::*;

/// One keyset column, in window order.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompiledWindowKey {
    pub(crate) field: String,
    /// Where each row carries the column; filled once the selection compiles.
    pub(crate) response_key: String,
    pub(crate) variable: String,
    pub(crate) direction: &'static str,
    pub(crate) codec: String,
}

/// The runtime contract of a `@window` operation.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompiledWindow {
    pub(crate) limit_variable: String,
    /// The limit variable's default, else the root's default limit.
    pub(crate) page_size: u64,
    /// The server clamps `limit` to this; a page asks for at most this many.
    pub(crate) max_limit: u64,
    pub(crate) keys: Vec<CompiledWindowKey>,
}

/// Codecs whose order the server and the replica agree on.
const KEYSET_CODECS: &[&str] = &[
    "string",
    "int32",
    "float64",
    "json_number_precision_limited",
];

/// Rewrite the operation in place and return its window contract.
pub(super) fn apply_window(
    operation: &mut OperationDefinition,
    manifest: &ClientManifest,
    document: &ClientDocument,
    position: Pos,
) -> Result<CompiledWindow, ClientCompileError> {
    let error = |code, message: String| source_error(code, message, document, position);
    let [root] = operation.selection_set.node.items.as_mut_slice() else {
        return Err(error(
            "client.window.root",
            "`@window` requires exactly one inline list root field".into(),
        ));
    };
    let Selection::Field(field) = &mut root.node else {
        return Err(error(
            "client.window.root",
            "`@window` requires the list root field inline, not in a fragment".into(),
        ));
    };
    let field = &mut field.node;
    let root_name = field.name.node.to_string();
    let root_manifest = manifest
        .root(ClientRootOperation::Query, &root_name)
        .filter(|root| root.kind == ClientRootKind::List)
        .ok_or_else(|| {
            error(
                "client.window.root",
                format!("`@window` requires a list root; `{root_name}` is not one"),
            )
        })?;
    let pagination = root_manifest.pagination.as_ref().ok_or_else(|| {
        error(
            "client.window.root",
            format!("list root `{root_name}` declares no limit"),
        )
    })?;
    let model = manifest.models.get(&root_manifest.model).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.root_model",
            format!(
                "root `{root_name}` references absent model `{}`",
                root_manifest.model
            ),
        )
    })?;
    let argument_name = |kind| {
        root_manifest
            .arguments
            .iter()
            .find(|argument| argument.kind == kind)
            .map(|argument| argument.name.as_str())
    };
    let supplied = |kind| {
        argument_name(kind).and_then(|name| {
            field
                .arguments
                .iter()
                .position(|(argument, _)| argument.node == name)
        })
    };
    let (offset_index, limit_index, order_index, filter_index) = (
        supplied(ClientArgumentKind::Offset),
        supplied(ClientArgumentKind::Limit),
        supplied(ClientArgumentKind::Order),
        supplied(ClientArgumentKind::Filter),
    );

    if offset_index.is_some() {
        return Err(error(
            "client.window.offset",
            "`@window` pages by keyset cursor; remove `offset`".into(),
        ));
    }
    let limit_variable = limit_index
        .and_then(|index| match &field.arguments[index].1.node {
            Value::Variable(name) => Some(name.to_string()),
            _ => None,
        })
        .ok_or_else(|| {
            error(
                "client.window.limit",
                "`@window` requires `limit: $variable` so each page sets its size".into(),
            )
        })?;
    let page_size = operation
        .variable_definitions
        .iter()
        .find(|definition| definition.node.name.node == limit_variable.as_str())
        .and_then(|definition| definition.node.default_value.as_ref())
        .and_then(|default| match &default.node {
            async_graphql_value::ConstValue::Number(number) => number.as_u64(),
            _ => None,
        })
        .unwrap_or(pagination.default_limit);
    // A page anchored at the head's newest row asks for the rest of the head
    // plus one page: at most `2 * page_size - 1` rows.
    if page_size == 0 || page_size.saturating_mul(2) - 1 > pagination.max_limit {
        return Err(error(
            "client.window.page_size",
            format!(
                "`@window` page size {page_size} must be at least 1 and at most half of `{root_name}`'s max limit {}",
                pagination.max_limit
            ),
        ));
    }

    let order = order_index
        .map(|index| &field.arguments[index].1.node)
        .ok_or_else(|| {
            error(
                "client.window.order",
                "`@window` requires a literal `order_by`; it defines the keyset".into(),
            )
        })?;
    let mut keys = Vec::new();
    let order_entries = match order {
        Value::List(entries) => entries.as_slice(),
        _ => &[],
    };
    if order_entries.is_empty() {
        return Err(error(
            "client.window.order",
            "`@window` requires `order_by` as a literal list such as `[{created_at: desc}]`".into(),
        ));
    }
    for entry in order_entries {
        let (name, direction) = match entry {
            Value::Object(object) if object.len() == 1 => {
                let (name, direction) = object.iter().next().expect("one entry");
                let direction = match direction {
                    Value::Enum(direction) if direction.as_str() == "asc" => "asc",
                    Value::Enum(direction) if direction.as_str() == "desc" => "desc",
                    _ => {
                        return Err(error(
                            "client.window.order",
                            format!("`@window` orders `{name}` by literal `asc` or `desc` only"),
                        ))
                    }
                };
                (name.to_string(), direction)
            }
            _ => {
                return Err(error(
                    "client.window.order",
                    "each `@window` `order_by` entry names one column".into(),
                ))
            }
        };
        if keys.iter().any(|key: &(String, &str)| key.0 == name) {
            return Err(error(
                "client.window.order",
                format!("`order_by` lists `{name}` twice"),
            ));
        }
        keys.push((name, direction));
    }
    let identity = model.identity().ok_or_else(|| {
        error(
            "client.window.identity",
            format!("`@window` requires `{}` to have a primary key", model.id),
        )
    })?;
    for key in identity {
        if !keys.iter().any(|(name, _)| *name == key.name) {
            keys.push((key.name.clone(), "asc"));
        }
    }

    let mut compiled = Vec::with_capacity(keys.len());
    for (name, direction) in keys {
        let column = model.field(&name).ok_or_else(|| {
            error(
                "client.window.key",
                format!("`{name}` is not a column of `{}`", model.id),
            )
        })?;
        if column.nullable || !KEYSET_CODECS.contains(&column.codec.as_str()) {
            return Err(error(
                "client.window.key",
                format!(
                    "`@window` key `{name}` must be a non-null string or number column (it is {}{})",
                    column.codec,
                    if column.nullable { ", nullable" } else { "" }
                ),
            ));
        }
        let comparison = if direction == "desc" { "_lt" } else { "_gt" };
        let filterable = model
            .filter_input
            .fields
            .iter()
            .find(|filter| filter.name == name)
            .is_some_and(|filter| {
                filter.operators.iter().any(|op| op == "_eq")
                    && filter.operators.iter().any(|op| op == comparison)
            });
        if !filterable {
            return Err(error(
                "client.window.key",
                format!("`@window` key `{name}` must accept `_eq` and `{comparison}`"),
            ));
        }
        let variable = format!("cursor_{name}");
        if operation
            .variable_definitions
            .iter()
            .any(|definition| definition.node.name.node == variable.as_str())
        {
            return Err(error(
                "client.window.variable",
                format!("`${variable}` is reserved for the `@window` cursor"),
            ));
        }
        operation.variable_definitions.push(Positioned::new(
            async_graphql_parser::types::VariableDefinition {
                name: Positioned::new(Name::new(&variable), position),
                var_type: Positioned::new(
                    Type {
                        base: BaseType::Named(Name::new(&column.scalar)),
                        nullable: true,
                    },
                    position,
                ),
                directives: Vec::new(),
                default_value: None,
            },
            position,
        ));
        compiled.push(CompiledWindowKey {
            field: name,
            response_key: String::new(),
            variable,
            direction,
            codec: column.codec.clone(),
        });
    }

    let keyset = keyset_predicate(&compiled);
    let filter_name = argument_name(ClientArgumentKind::Filter).ok_or_else(|| {
        error(
            "client.window.filter",
            format!("list root `{root_name}` accepts no `where`"),
        )
    })?;
    match filter_index {
        Some(index) => {
            let existing = std::mem::replace(&mut field.arguments[index].1.node, Value::Null);
            field.arguments[index].1.node = object([("_and", Value::List(vec![existing, keyset]))]);
        }
        None => field.arguments.push((
            Positioned::new(Name::new(filter_name), position),
            Positioned::new(keyset, position),
        )),
    }

    Ok(CompiledWindow {
        limit_variable,
        page_size,
        max_limit: pagination.max_limit,
        keys: compiled,
    })
}

/// Rows after the cursor in window order: for each key, every earlier key
/// equal and this one strictly past.
fn keyset_predicate(keys: &[CompiledWindowKey]) -> Value {
    let comparison = |key: &CompiledWindowKey, op: &str| {
        object([(op, Value::Variable(Name::new(&key.variable)))])
    };
    let disjuncts = (0..keys.len())
        .map(|index| {
            let mut conjunct = async_graphql_value::indexmap::IndexMap::new();
            for earlier in &keys[..index] {
                conjunct.insert(Name::new(&earlier.field), comparison(earlier, "_eq"));
            }
            let key = &keys[index];
            let op = if key.direction == "desc" {
                "_lt"
            } else {
                "_gt"
            };
            conjunct.insert(Name::new(&key.field), comparison(key, op));
            Value::Object(conjunct)
        })
        .collect();
    object([("_or", Value::List(disjuncts))])
}

fn object<const N: usize>(entries: [(&str, Value); N]) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(name, value)| (Name::new(name), value))
            .collect(),
    )
}

/// Point each key at the exposed scalar that carries it in every row.
pub(super) fn bind_window_keys(
    window: &mut CompiledWindow,
    selection: &CompiledObject,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    for key in &mut window.keys {
        key.response_key = selection
            .members
            .iter()
            .find_map(|member| match member {
                CompiledMember::Scalar(scalar) if scalar.field == key.field && scalar.expose => {
                    Some(scalar.response_key.clone())
                }
                _ => None,
            })
            .ok_or_else(|| {
                source_error(
                    "client.window.key_unselected",
                    format!(
                        "select `{}`: the `@window` cursor is read from each row",
                        key.field
                    ),
                    document,
                    position,
                )
            })?;
    }
    Ok(())
}
