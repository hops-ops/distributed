#![allow(unexpected_cfgs)]
#![allow(dead_code)]
#![allow(unused_imports)]

use distributed::command::Succeeded;
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::{Aggregate, CommandInput, CommandOutput, DomainEvent, Entity, EventRecord};
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct FixtureAggregate {
    entity: Entity,
}

impl Aggregate for FixtureAggregate {
    type ReplayError = String;

    fn aggregate_type() -> &'static str {
        "application-macro-fixture"
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

#[derive(Clone, Deserialize, CommandInput)]
pub struct CreateInput {
    id: String,
    title: String,
}

#[derive(Clone, Serialize, CommandOutput)]
pub struct CreateOutput {
    id: String,
}

#[derive(Clone, Serialize, DomainEvent)]
#[domain_event(name = "todo.created", version = 1)]
pub struct TodoCreated {
    id: String,
}

#[distributed::command(
    id = "todo.create",
    roles(user, admin),
    emits(TodoCreated),
    applies(distributed::event_preview! {
        TodoCreated => TodoCreated {
            id: input.id,
            ..unknown
        }
    }),
    default(title = uuid_v7),
    input = CreateInput,
    outcome = Succeeded<CreateOutput>
)]
pub async fn handle(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    _input: CreateInput,
) -> Result<distributed::command::PreparedCommand<Succeeded<CreateOutput>>, HandlerError> {
    unimplemented!()
}

distributed::module! {
    pub TODO_MODULE {
        id: "todo",
        commands: [HANDLE_DEFINITION],
        capabilities: ["events"],
    }
}

distributed::application! {
    pub TODO_APPLICATION {
        id: "todo-app",
        modules: [TODO_MODULE],
        capabilities: ["identity"],
    }
}

distributed::application! {
    pub IMPLICIT_APPLICATION {
        modules: [TODO_MODULE],
    }
}

#[test]
fn command_module_and_application_macros_share_one_portable_spec() {
    let spec = handle_spec().expect("generated command spec");
    assert_eq!(spec.id, "todo.create");
    assert_eq!(spec.roles, ["admin", "user"]);
    assert_eq!(spec.emits[0].name, "todo.created");
    assert!(spec
        .applies
        .as_array()
        .is_some_and(|values| !values.is_empty()));
    assert!(spec
        .defaults
        .as_array()
        .is_some_and(|values| !values.is_empty()));
    assert!(!spec.effects.is_null());
    assert!(!spec.fingerprint.is_empty());
    assert_eq!(
        spec.canonical_bytes().unwrap(),
        spec.canonical_bytes().unwrap()
    );

    assert_eq!(TODO_MODULE.manifest().commands[0], spec);
    assert_eq!(TODO_APPLICATION.manifest().modules[0].id, "todo");
    assert_eq!(TODO_APPLICATION.manifest().name, "todo-app");
    assert_eq!(
        TODO_APPLICATION.manifest().required_capabilities,
        ["events", "identity"]
    );
    assert_eq!(IMPLICIT_APPLICATION.manifest().name, "implicit_application");

    #[cfg(feature = "application-runtime")]
    {
        assert_eq!(HANDLE_MOUNT.spec().id, "todo.create");
        assert_eq!(HANDLE_MOUNT.spec().fingerprint, spec.fingerprint);
    }
}
