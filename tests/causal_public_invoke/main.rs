//! Public causal wait-path from outside `crate::microsvc`.
//!
//! Proves `Service::dispatch_causal_with_receipt` is callable from an
//! integration crate against an in-memory repository (no sqlx, no celld).

#![cfg(feature = "graphql")]

use distributed::command::{
    typed_command, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField, Succeeded,
};
use distributed::graphql::VerifiedPrincipal;
use distributed::microsvc::{Routes, Service, Session, USER_ID_KEY};
use distributed::{Aggregate, AggregateBuilder, Entity, InMemoryRepository, Snapshot};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Default, Snapshot)]
struct TodoHost {
    entity: Entity,
}

impl TodoHost {
    fn record(&mut self, id: String) -> distributed::SourcedResult {
        self.entity.set_id(id);
        self.entity.digest_empty("todo.recorded")
    }
}

impl Aggregate for TodoHost {
    type ReplayError = std::convert::Infallible;

    fn aggregate_type() -> &'static str {
        "causal-public-invoke-todo"
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &distributed::EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

#[derive(Deserialize)]
struct CompleteInput {
    id: String,
}

impl CommandInputType for CompleteInput {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CompleteInput",
            vec![CommandTypeField {
                name: "id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[derive(Serialize)]
struct CompletePayload {
    id: String,
}

impl CommandOutputType for CompletePayload {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CompletePayload",
            vec![CommandTypeField {
                name: "id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[tokio::test]
async fn public_causal_invoke_returns_receipt_without_sqlx_or_celld() {
    let routes = Routes::new()
        .with_repo(InMemoryRepository::new().aggregate::<TodoHost>())
        .typed_command(typed_command::<CompleteInput, Succeeded<CompletePayload>>(
            "todo.create",
        ))
        .create()
        .invoke(|aggregate, input, _owner| {
            aggregate.record(input.id.clone())?;
            Ok::<_, distributed::EventRecordError>(())
        })
        .succeeded(|aggregate| CompletePayload {
            id: aggregate.entity().id().to_string(),
        })
        .typed_command(typed_command::<CompleteInput, Succeeded<CompletePayload>>(
            "todo.complete",
        ))
        .load_by(|input: &CompleteInput| input.id.clone())
        .invoke(|aggregate, input, _owner| {
            aggregate.record(input.id.clone())?;
            Ok::<_, distributed::EventRecordError>(())
        })
        .succeeded(|aggregate| CompletePayload {
            id: aggregate.entity().id().to_string(),
        });

    let service = Service::new().named("causal-public-invoke").routes(routes);
    let mut session = Session::new();
    session.set(USER_ID_KEY, "alice");
    let principal = VerifiedPrincipal::test_oidc(
        "https://issuer.example/",
        "causal-public-subject",
        &["distributed-tests"],
    );
    let create_id = "0190a000-0000-7000-8000-000000000042";
    let complete_id = "0190a000-0000-7000-8000-000000000043";

    let created = service
        .dispatch_causal_with_receipt(
            "todo.create",
            &create_id,
            json!({ "id": "todo-1" }),
            session.clone(),
            principal.clone(),
        )
        .await
        .expect("create should commit through the public causal API");
    assert_eq!(created.payload(), &json!({ "id": "todo-1" }));
    assert_eq!(created.command_id(), create_id);
    assert_eq!(created.state(), "succeeded");
    assert!(!created.causation_id().is_empty());

    let completed = service
        .dispatch_causal_with_receipt(
            "todo.complete",
            &complete_id,
            json!({ "id": "todo-1" }),
            session,
            principal,
        )
        .await
        .expect("complete should load and commit through the public causal API");
    assert_eq!(completed.payload(), &json!({ "id": "todo-1" }));
    assert_eq!(completed.command_id(), complete_id);
    assert_eq!(completed.state(), "succeeded");
}
