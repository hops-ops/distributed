//! Zone handoff types (transport-agnostic).
//!
//! A zone moves a player with [`Ctx::handoff`](crate::Ctx::handoff): the
//! host asks its transfer issuer (installed by the driver, e.g. the `net`
//! server signs an HS256 transfer ticket) for a ticket, removes the player
//! with [`LeaveReason::Handoff`](crate::LeaveReason::Handoff) after the hook
//! returns and sends `Handoff(target, ticket)`. The target admits the ticket
//! with [`ZoneHost::join_transfer`](crate::ZoneHost::join_transfer), which
//! calls [`Zone::on_handoff_join`](crate::Zone::on_handoff_join) with the
//! carried bytes.

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
    /// Game state carried along (inventory, buffs, position hint...).
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
