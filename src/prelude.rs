//! What a typical aggregate, command or read-model file needs.
//!
//! ```
//! use distributed::prelude::*;
//! ```
//!
//! Each name here is a re-export; its own path is at the crate root or in its
//! module. `command`, `mutation` and `projection` bring both the macro and the
//! module of that name.

pub use crate::command::{
    typed_command, CommandInput, CommandOutput, PreparedCommand, Succeeded, TypedCommand,
};
pub use crate::domain_event::{DomainEvent, DomainEventContract, DomainState};
pub use crate::read_model::{ReadModel, RelationalReadModel};
pub use crate::table::JsonType;
pub use crate::{
    command, command_input_defaults, mutation, mutation_file, projection, sourced, Aggregate,
    AggregateBuilder, AggregateRepository, Entity, InMemoryRepository, Repository, RepositoryError,
    Snapshot, Snapshottable, StreamIdentity,
};
