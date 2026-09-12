use framework as _;
use framework::{Aggregate, Entity, EventRecord};

#[derive(Default)]
struct RenamedAggregate {
    entity: Entity,
}

impl Aggregate for RenamedAggregate {
    type ReplayError = std::convert::Infallible;

    fn aggregate_type() -> &'static str {
        "renamed.aggregate"
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

#[derive(Clone, serde::Deserialize, framework::CommandInput)]
struct RenamedInput {
    id: String,
}

#[derive(Clone, serde::Serialize, framework::CommandOutput)]
struct RenamedOutput {
    id: String,
}

#[derive(Clone, serde::Serialize, framework::DomainEvent)]
#[domain_event(name = "renamed.created", version = 1)]
struct RenamedCreated {
    id: String,
}

#[framework::command(
    id = "renamed.create",
    roles(user),
    emits(RenamedCreated),
    input = RenamedInput,
    outcome = framework::command::Succeeded<RenamedOutput>
)]
async fn renamed_handler(
    _context: &framework::microsvc::CausalCommandContext<'_, RenamedAggregate>,
    _input: RenamedInput,
) -> Result<
    framework::command::PreparedCommand<framework::command::Succeeded<RenamedOutput>>,
    framework::microsvc::HandlerError,
> {
    unreachable!()
}

framework::module! {
    pub RENAMED_MODULE {
        id: "renamed-module",
        commands: [RENAMED_HANDLER_DEFINITION],
        projections: [],
        surfaces: [],
        capabilities: ["contract"],
    }
}

framework::application! {
    pub RENAMED_APPLICATION {
        id: "renamed-application",
        modules: [RENAMED_MODULE],
        surfaces: [],
        extensions: [],
    }
}

fn main() {
    assert_eq!(RENAMED_APPLICATION.manifest().name, "renamed-application");
    assert_eq!(RENAMED_MODULE.manifest().id, "renamed-module");
    assert_eq!(RENAMED_MODULE.manifest().commands[0].id, "renamed.create");
    assert_eq!(RENAMED_MODULE.manifest().events[0].name, "renamed.created");
}
