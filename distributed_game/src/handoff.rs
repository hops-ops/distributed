//! Zone handoff types (transport-agnostic).
//!
//! A zone moves a player with [`Ctx::handoff`](crate::Ctx::handoff): the
//! host asks its transfer issuer (installed by the driver, e.g. the `net`
//! server signs an HS256 transfer ticket) for a ticket, removes the player
//! with [`LeaveReason::Handoff`](crate::LeaveReason::Handoff) after the hook
//! returns and sends `Handoff(target, ticket)`. The target admits the ticket
//! with [`ZoneHost::join_transfer`](crate::ZoneHost::join_transfer), which
//! calls [`Zone::on_handoff_join`](crate::Zone::on_handoff_join) with the
//! carried bytes (the encoding of the [`Wire`](crate::Wire) value the
//! source passed).
//!
//! **Custody rule: the client holds the carried state, and it is delivered
//! at most once.** The source removes the player as soon as the hook
//! returns; from then on the carried state exists only in the signed
//! ticket. The issuer's transfer record (reserve / confirm / release) is in
//! the issuing host's memory. So a client that drops the ticket, or
//! discards one on purpose and joins afresh, an expired ticket, or an
//! issuer restart before the target reserves it (`unknown transfer`) loses
//! the carried state, and the player is in neither zone until it joins
//! again. What holds is no duplication: the player is never in both zones
//! and the ticket is single-use. State that must not be lost moves through
//! a durable record (a fact from the hook driving a saga), never through
//! `carried`.

use crate::claims::TicketExtensions;
use crate::zone::TicketClaims;

/// Largest carried state accepted in a handoff (it travels inside the
/// signed ticket, which travels inside Hello).
pub const MAX_CARRIED_BYTES: usize = 16 * 1024;

/// What a zone asks the issuer to sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffRequest {
    /// Verified principal of the moving player.
    pub principal: String,
    /// The player's display name (from its admission ticket), carried over.
    pub name: Option<String>,
    /// The player's ticket extension claims, carried over.
    pub extensions: TicketExtensions,
    /// Zone the player leaves.
    pub source_zone: String,
    /// Zone the player goes to.
    pub target_zone: String,
    /// Game state carried along (a position hint, buffs...): an encoded
    /// [`Wire`](crate::Wire) value, in the client's custody (module docs).
    pub carried: Vec<u8>,
    /// Source stamp time of the departure (Unix ms): the facts the source
    /// emits while the player leaves carry it, and the target stamps
    /// everything after it ([`FactStamp`](crate::FactStamp) ordering).
    pub departed_at_ms: u64,
}

/// Verified transfer ticket contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transfer {
    /// Principal, target zone (`claims.zone`), expiry and single-use nonce.
    pub claims: TicketClaims,
    /// Zone the player left.
    pub source_zone: String,
    /// Host id that issued the ticket (holds the transfer record).
    pub issuer: String,
    /// Source zone ownership token at issue time.
    pub generation: u64,
    /// Carried state.
    pub carried: Vec<u8>,
    /// Source stamp time of the departure (Unix ms, see
    /// [`HandoffRequest::departed_at_ms`]).
    pub departed_at_ms: u64,
}

/// Why [`Ctx::handoff`](crate::Ctx::handoff) refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffError {
    /// No such player.
    UnknownPlayer,
    /// The host has no transfer issuer (no transport configured).
    NoIssuer,
    /// Carried state exceeds [`MAX_CARRIED_BYTES`].
    TooLarge,
    /// Target equals the current zone.
    SameZone,
    /// The issuer refused (e.g. target unknown).
    Refused,
}

/// Transfer ticket issuer: `None` refuses the handoff.
pub type TransferIssuer = dyn FnMut(&HandoffRequest) -> Option<String> + Send;
