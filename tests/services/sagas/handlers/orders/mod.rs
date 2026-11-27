//! Order service handlers.

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError, TRANSPORT_ROLE};
use serde::Serialize;

use super::messages::*;
use crate::sagas::order::Order;

pub mod complete;
pub mod create;
