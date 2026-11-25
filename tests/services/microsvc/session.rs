//! Session integration tests — exercises session variables through dispatch.

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalDispatchError, HandlerError, Routes, Service, Session,
};
use distributed::InMemoryRepository;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

use crate::microsvc::commands::dispatch;

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct Identify {}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct Identity {
    user_id: String,
}

async fn identify(
    ctx: &CausalCommandContext<'_, ()>,
    _input: Identify,
) -> Result<PreparedCommand<Succeeded<Identity>>, HandlerError> {
    let user_id = ctx.user_id()?.to_string();
    PreparedCommand::prepare(Identity { user_id })
        .map_err(|error| HandlerError::Other(Box::new(error)))
}

fn service() -> Service {
    let repo = InMemoryRepository::new();
    Service::new().named("sessions").routes(
        Routes::for_stores(repo.clone(), repo)
            .typed_command(typed_command::<Identify, Succeeded<Identity>>(
                "session.identify",
            ))
            .handle(identify),
    )
}

#[tokio::test]
async fn handler_accesses_user_id() {
    let mut vars = HashMap::new();
    vars.insert(
        distributed::microsvc::USER_ID_KEY.to_string(),
        "user-42".to_string(),
    );
    let session = Session::from_map(vars);

    let result = dispatch(&service(), "session.identify", json!({}), session)
        .await
        .unwrap();
    assert_eq!(result, json!({ "user_id": "user-42" }));
}

#[tokio::test]
async fn missing_user_id_returns_unauthorized() {
    let result = dispatch(&service(), "session.identify", json!({}), Session::new()).await;
    assert!(
        matches!(
            result,
            Err(CausalDispatchError::Rejected {
                code: "UNAUTHORIZED",
                status: 401,
                ..
            })
        ),
        "{result:?}"
    );
}
