//! Thin command path: load/create + domain method without a handler-context body.

use crate::aggregate::Aggregate;
use crate::microsvc::error::HandlerError;
use crate::microsvc::{AggregateCheckout, CausalCommandContext};

/// How a declared command gets its aggregate. `command!` (`load:`) and the
/// method form (`load =`) both resolve to this; the id is the command's shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandLoad {
    /// The aggregate must exist: `NotFound` otherwise.
    Required(String),
    /// Load it, or start a new one when the id has no stream yet.
    OrCreate(String),
    /// Start a new one. With an id, an existing stream rejects the command;
    /// without one the aggregate names itself and nothing is checked.
    Create(Option<String>),
}

/// Load (or start) the aggregate a declared command runs on.
pub async fn load_for_command<A>(
    ctx: &CausalCommandContext<'_, A>,
    load: CommandLoad,
) -> Result<AggregateCheckout<A>, HandlerError>
where
    A: Aggregate + Send + Sync + 'static,
{
    match load {
        CommandLoad::Required(id) => require_loaded(ctx.get(&id).await?, id),
        CommandLoad::OrCreate(id) => Ok(match ctx.get(&id).await? {
            Some(checkout) => checkout,
            None => ctx.create(),
        }),
        CommandLoad::Create(None) => Ok(ctx.create()),
        CommandLoad::Create(Some(id)) => match ctx.get(&id).await? {
            Some(_) => Err(HandlerError::Rejected(format!(
                "{} {id} already exists",
                A::aggregate_type()
            ))),
            None => Ok(ctx.create()),
        },
    }
}

/// Load an aggregate or return NotFound. Application code stays a domain call.
pub fn require_loaded<A>(loaded: Option<A>, id: impl Into<String>) -> Result<A, HandlerError> {
    loaded.ok_or_else(|| HandlerError::NotFound(id.into()))
}

/// Apply a domain transition and map domain errors to rejected commands.
pub fn invoke_transition<A, E>(
    aggregate: &mut A,
    transition: impl FnOnce(&mut A) -> Result<(), E>,
) -> Result<(), HandlerError>
where
    E: std::fmt::Display,
{
    transition(aggregate).map_err(|error| HandlerError::Rejected(error.to_string()))
}
