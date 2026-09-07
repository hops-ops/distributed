#![cfg(feature = "gateway-graphql")]
use distributed::gateway::{graphql::*, GraphqlCapabilities};
use serde_json::json;

#[test]
fn selected_operation_and_status_follow_the_parser() {
    let query = GraphqlCapabilities {
        queries: true,
        ..Default::default()
    };
    let command = GraphqlCapabilities {
        commands: true,
        ..Default::default()
    };
    let document =
        "query Allowed { message(text: \"mutation { write }\") } mutation Blocked { write }";
    assert_eq!(
        admit_operation(document, Some("Allowed"), query),
        Ok(OperationKind::Query)
    );
    assert_eq!(
        admit_operation(document, Some("Blocked"), query),
        Err(OperationError::NotMounted)
    );
    assert_eq!(
        admit_operation(document, None, query),
        Err(OperationError::AmbiguousOperation)
    );
    assert_eq!(
        admit_operation(document, Some("Missing"), query),
        Err(OperationError::UnknownOperation)
    );
    let status = "query Recover { ...Recovery } fragment Recovery on Query { renamed: commandStatus(commandId: \"same-id\") { state } }";
    assert_eq!(
        admit_operation(status, None, command),
        Ok(OperationKind::CommandStatus)
    );
    assert_eq!(
        admit_operation(status, None, query),
        Err(OperationError::NotMounted)
    );
    assert_eq!(
        admit_operation(
            "{ commandStatus(commandId: \"id\") { state } items { id } }",
            None,
            command
        ),
        Err(OperationError::NotMounted)
    );
    assert_eq!(
        admit_request(&json!({"query":"{ ok }", "variables":[1]}), query),
        Err(OperationError::InvalidRequest)
    );
    assert_eq!(
        admit_request(
            &json!({"query":"{ ok }", "variables":null, "extensions":{"opaque":"value"}}),
            query
        ),
        Ok(OperationKind::Query)
    );
    assert_eq!(
        operation_kind(&" ".repeat(MAX_DOCUMENT_BYTES + 1), None),
        Err(OperationError::InvalidRequest)
    );
}
