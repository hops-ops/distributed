use serde::{Deserialize, Serialize};

use distributed::{sourced, Entity, Snapshot};

#[derive(Default, Snapshot)]
pub struct CommandAggregate {
    pub entity: Entity,
    pub name: Option<String>,
    pub status: String,
}

#[sourced(entity, aggregate_type = "command_aggregate")]
impl CommandAggregate {
    #[event("service.command_recorded")]
    pub fn record_command(&mut self, command: String, id: String, name: Option<String>) {
        self.entity.set_id(&id);
        self.name = name;
        self.status = command;
    }
}

#[derive(Clone, Debug, Deserialize, distributed::command::CommandInput)]
pub struct CommandInput {
    pub id: String,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Serialize, distributed::command::CommandOutput)]
pub struct CommandOutput {
    pub command: String,
    pub id: String,
    pub model: String,
    pub name: Option<String>,
}
