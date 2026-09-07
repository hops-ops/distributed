//! Whole GraphQL operation admission, independent of an executor or SQL pool.
//! The caller retains the original request/envelope; admission never dispatches
//! commands itself, rewrites command IDs or manufactures commit evidence.
use super::GraphqlCapabilities;
use async_graphql_parser::{
    parse_query,
    types::{ExecutableDocument, OperationDefinition, OperationType, Selection},
};
use serde_json::Value;

/// Maximum portable operation document size accepted before parsing.
pub const MAX_DOCUMENT_BYTES: usize = 256 * 1024;

/// Kind of the exact selected operation, obtained from the GraphQL parser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    /// Read query.
    Query,
    /// Built-in command status recovery query, owned by the command mount and
    /// excluded from query snapshot/coalescing eligibility.
    CommandStatus,
    /// Query combining ordinary reads and command status; requires both mounts
    /// and remains ineligible for query delivery reuse.
    MixedQuery,
    /// Command mutation; never eligible for an automatic retry.
    Mutation,
    /// Live subscription.
    Subscription,
}

/// Admission failure before invoking the selected executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationError {
    /// Malformed transport request/variables/extensions.
    InvalidRequest,
    /// Invalid executable GraphQL document.
    InvalidDocument,
    /// Multiple operations require an explicit operationName.
    AmbiguousOperation,
    /// The requested operationName does not exist in the document.
    UnknownOperation,
    /// The selected operation's surface is not mounted.
    NotMounted,
}
impl std::fmt::Display for OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidRequest => "invalid GraphQL request",
            Self::InvalidDocument => "invalid GraphQL document",
            Self::AmbiguousOperation => {
                "GraphQL operation name is required for multi-operation documents"
            }
            Self::UnknownOperation => "GraphQL operation name was not found",
            Self::NotMounted => "GraphQL operation surface is not mounted",
        })
    }
}
impl std::error::Error for OperationError {}
impl OperationError {
    /// GraphQL error envelope, without command receipts or response evidence.
    pub fn envelope(self) -> Value {
        serde_json::json!({ "errors": [{ "message": self.to_string(), "extensions": { "code": if self == Self::NotMounted { "OPERATION_NOT_MOUNTED" } else { "BAD_REQUEST" } } }] })
    }
}

/// Resolve the exact operation. Reject ambiguous/unknown selection even when
/// another operation in the same document would be allowed. This is not full
/// schema validation; that remains the executor's responsibility.
pub fn operation_kind(
    document: &str,
    operation_name: Option<&str>,
) -> Result<OperationKind, OperationError> {
    if document.len() > MAX_DOCUMENT_BYTES || operation_name.is_some_and(|name| name.len() > 256) {
        return Err(OperationError::InvalidRequest);
    }
    let document = parse_query(document).map_err(|_| OperationError::InvalidDocument)?;
    let mut operations = document.operations.iter();
    let operation = if let Some(name) = operation_name {
        operations
            .find(|(candidate, _)| candidate.is_some_and(|candidate| candidate.as_str() == name))
            .map(|(_, op)| op)
            .ok_or(OperationError::UnknownOperation)?
    } else {
        let (_, operation) = operations.next().ok_or(OperationError::InvalidDocument)?;
        if operations.next().is_some() {
            return Err(OperationError::AmbiguousOperation);
        }
        operation
    };
    Ok(match operation.node.ty {
        OperationType::Query => query_kind(&document, &operation.node),
        OperationType::Mutation => OperationKind::Mutation,
        OperationType::Subscription => OperationKind::Subscription,
    })
}

/// Check a selected operation against explicit command/query/live mounts.
pub fn admit_operation(
    document: &str,
    operation_name: Option<&str>,
    capabilities: GraphqlCapabilities,
) -> Result<OperationKind, OperationError> {
    let kind = operation_kind(document, operation_name)?;
    let allowed = match kind {
        OperationKind::Query => capabilities.queries,
        OperationKind::Mutation | OperationKind::CommandStatus => capabilities.commands,
        OperationKind::MixedQuery => capabilities.queries && capabilities.commands,
        OperationKind::Subscription => capabilities.live,
    };
    if allowed {
        Ok(kind)
    } else {
        Err(OperationError::NotMounted)
    }
}

/// Validate and admit one JSON GraphQL request, preserving all original values
/// and extensions for forwarding. Request byte/depth limits belong to the host.
pub fn admit_request(
    request: &Value,
    capabilities: GraphqlCapabilities,
) -> Result<OperationKind, OperationError> {
    let document = request
        .get("query")
        .and_then(Value::as_str)
        .ok_or(OperationError::InvalidRequest)?;
    let name = match request.get("operationName") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) => Some(name.as_str()),
        _ => return Err(OperationError::InvalidRequest),
    };
    for key in ["variables", "extensions"] {
        if request
            .get(key)
            .is_some_and(|value| !value.is_null() && !value.is_object())
        {
            return Err(OperationError::InvalidRequest);
        }
    }
    admit_operation(document, name, capabilities)
}

// Follow root fragments, not text/aliases. Every selected root must belong to
// status recovery; mixed read/status documents need the full query surface.
fn query_kind(document: &ExecutableDocument, operation: &OperationDefinition) -> OperationKind {
    let mut pending: Vec<_> = operation.selection_set.node.items.iter().collect();
    let mut fragments = std::collections::BTreeSet::new();
    let mut status = false;
    let mut query = false;
    let mut count = 0;
    while let Some(selection) = pending.pop() {
        count += 1;
        if count > 4096 {
            return OperationKind::MixedQuery;
        }
        match &selection.node {
            Selection::Field(field) => match field.node.name.node.as_str() {
                "commandStatus" => status = true,
                "__typename" => {}
                _ => query = true,
            },
            Selection::InlineFragment(fragment) => {
                pending.extend(fragment.node.selection_set.node.items.iter())
            }
            Selection::FragmentSpread(spread) => {
                let name = spread.node.fragment_name.node.as_str();
                if fragments.insert(name) {
                    let Some(fragment) = document.fragments.get(name) else {
                        return OperationKind::MixedQuery;
                    };
                    pending.extend(fragment.node.selection_set.node.items.iter());
                }
            }
        }
    }
    match (status, query) {
        (true, false) => OperationKind::CommandStatus,
        (true, true) => OperationKind::MixedQuery,
        _ => OperationKind::Query,
    }
}
