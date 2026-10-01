//! Versioned command dispatch boundary for local and remote execution.
//!
//! GraphQL mutation execution and process hosts depend on
//! [`CommandDispatcher`] rather than a concrete `Service`. Schema and client
//! compilation never construct a dispatcher.

mod envelope;
mod error;
#[cfg(feature = "graphql")]
mod host;
mod local;
mod remote;

pub use envelope::{
    CommandDispatchEnvelope, CommandDispatchReceipt, COMMAND_DISPATCH_ENVELOPE_VERSION,
};
pub use error::CommandDispatchError;
#[cfg(feature = "graphql")]
pub(crate) use host::{validate_principal_session, validate_principal_session_if_present};
#[cfg(feature = "graphql")]
pub use host::{
    CellRequestContext, CommandHost, HttpCommandHost, LocalCommandHost, SharedCommandHost,
    TrustedRequestMetadata,
};
pub use local::LocalCommandDispatcher;
pub use remote::{
    RemoteCommandDispatcher, RemoteDispatchConfig, RemoteTrustMode,
    APPROVED_REMOTE_DISPATCH_PROFILE,
};

use crate::microsvc::{CommandRequest, CommandResponse};
use async_trait::async_trait;
use std::sync::Arc;

/// Object-safe async command dispatcher shared by GraphQL and process hosts.
#[async_trait]
pub trait CommandDispatcher: Send + Sync {
    /// Dispatch one versioned command request and return the durable response.
    async fn dispatch(
        &self,
        request: &CommandRequest,
    ) -> Result<CommandResponse, CommandDispatchError>;

    /// Human-stable dispatcher kind for inspection/metrics.
    fn kind(&self) -> &'static str;
}

/// Shared handle used by GraphQL engines and runtimes.
pub type SharedCommandDispatcher = Arc<dyn CommandDispatcher>;
