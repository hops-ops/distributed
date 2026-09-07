//! Versioned framework metadata carried in GraphQL response extensions.
//!
//! Domain payloads remain ordinary GraphQL data. This module owns the one
//! `extensions.distributed` wire envelope plus opaque, keyed tokens for
//! authorization and projection scopes. Tokens are comparable capabilities,
//! never client-decodable identities and never bearer credentials.

mod accumulator;
mod projection_metadata;
#[cfg(test)]
mod tests;
mod token;
mod types;

pub(crate) use accumulator::issue_projection_obligation_token;
pub use accumulator::ProtocolResponseAccumulator;
pub(crate) use projection_metadata::{
    CommandProjectionLifecycleProofV1, CommandProjectionMetadataError, CommandProjectionMetadataV1,
    CommandProjectionObligationV1, MAX_COMMAND_PROJECTION_OBLIGATIONS,
};
pub(crate) use token::{
    OpaqueProtocolToken, ProtocolTokenCodec, ProtocolTokenError, ProtocolTokenPurpose,
    MAX_LIVE_RESUME_CURSORS,
};
pub(crate) use types::{
    DistributedCommandConsistency, DistributedCommandMetadata, DistributedCommandState,
    DistributedEnvelopeV1, DistributedIndexRevision, DistributedLiveCursor,
    DistributedLiveMetadata, DistributedProjectionDisposition, DistributedProjectionExpectation,
    DistributedProjectionObservation, DistributedQuerySnapshot, DistributedRecordRevision,
    DistributedTrustedPreset, RequestedLiveResume,
};
