//! The command host: the one boundary GraphQL, the schedule worker and cell
//! callers dispatch typed commands through, in process or over HTTP.

#[cfg(feature = "graphql")]
mod host;

#[cfg(feature = "graphql")]
pub(crate) use host::{validate_principal_session, validate_principal_session_if_present};
#[cfg(feature = "graphql")]
pub use host::{
    CellRequestContext, CommandHost, HttpCommandHost, LocalCommandHost, SharedCommandHost,
    TrustedRequestMetadata,
};
