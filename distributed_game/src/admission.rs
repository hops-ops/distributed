//! Admission, shared by every driver (the `net` server and celld zone
//! objects): what a Hello must prove before a connection may touch a zone,
//! and the per-connection gate every later client message passes.
//!
//! A driver reads the first frame within [`AdmissionConfig::hello_timeout`]
//! and calls [`admit`]: protocol version, then a reconnect token (checked
//! by the zone itself) or a ticket (signature or custom
//! [`TicketVerifier`](crate::ticket::TicketVerifier), expiry, a non-empty
//! principal and replay key, declared extension claims, the route zone).
//! The ticket's single use is recorded with [`Admitted::consume`] once the
//! driver's own checks passed (a transfer's reservation with its issuer,
//! for instance). Only an admitted Hello may activate or wake a zone.
//!
//! After the Welcome every client message but `Leave` goes through the
//! connection's [`InputGate`] (a token bucket): excess messages are
//! dropped (one `Error(RATE_LIMITED)` per second) and more than a burst
//! of drops within a second is kicked. Undecodable frames count too.
//!
//! Transport framing (WebSocket frames, Durable Object socket attachments),
//! transfer custody and zone placement stay with the drivers.

use std::time::Duration;

use crate::claims::{validate_extensions, ExtensionSchema};
use crate::handoff::Transfer;
use crate::protocol::{error, supports_protocol, Hello, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION};
use crate::ticket::{
    NonceCache, SharedTicketVerifier, TicketError, TicketKey, TicketVerifier, VerifiedTicket,
};
use crate::zone::TicketClaims;

/// Admission settings of a driver.
#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    /// HMAC key of transfer tickets and, unless `verifier` is set, of join
    /// tickets.
    pub key: TicketKey,
    /// Custom verifier for **join** tickets (e.g. the game's web-BFF JWTs);
    /// `None` = HS256 tickets signed with `key`. Expiry, route zone and
    /// single use are enforced either way.
    pub verifier: Option<SharedTicketVerifier>,
    /// Extension claims tickets may carry
    /// ([`ExtensionSchema::of::<E>()`](crate::ExtensionSchema::of) for a
    /// `#[derive(TicketExtension)]` type). `None` = a ticket with any `ext`
    /// claims is refused. Applies to join (any verifier) and transfer
    /// tickets.
    pub extensions: Option<ExtensionSchema>,
    /// Deadline for the first (Hello) frame.
    pub hello_timeout: Duration,
    /// Sustained client messages per second per connection.
    pub input_rate: f64,
    /// Client message burst size.
    pub input_burst: f64,
}

impl AdmissionConfig {
    /// Defaults with the given ticket key: 5 s Hello deadline, 120
    /// messages per second, bursts of 240.
    pub fn new(key: TicketKey) -> Self {
        Self {
            key,
            verifier: None,
            extensions: None,
            hello_timeout: Duration::from_secs(5),
            input_rate: 120.0,
            input_burst: 240.0,
        }
    }

    /// Admit join tickets with `verifier` instead of `key`.
    pub fn with_ticket_verifier(mut self, verifier: impl TicketVerifier) -> Self {
        self.verifier = Some(SharedTicketVerifier::new(verifier));
        self
    }

    /// Admit ticket extension claims of type `E`.
    pub fn with_ticket_extensions<E: crate::TicketExtension>(mut self) -> Self {
        self.extensions = Some(ExtensionSchema::of::<E>());
        self
    }
}

/// An admitted Hello.
#[derive(Debug, Clone, PartialEq)]
pub enum Admitted {
    /// A join ticket for this zone.
    Join(TicketClaims),
    /// A handoff transfer ticket arriving at its target zone.
    Arrival(Transfer),
    /// A refused handoff returning to its source zone: the ticket's zone
    /// and source are swapped, so `claims.zone` is this zone.
    Return(Transfer),
    /// A reconnect token (the zone checks it).
    Reconnect(String),
}

impl Admitted {
    /// The verified claims (`None` for a reconnect).
    pub fn claims(&self) -> Option<&TicketClaims> {
        match self {
            Admitted::Join(claims) => Some(claims),
            Admitted::Arrival(transfer) | Admitted::Return(transfer) => Some(&transfer.claims),
            Admitted::Reconnect(_) => None,
        }
    }

    /// Record the ticket's single use in `nonces` (a return to the source
    /// has its own replay key: the target may have recorded the nonce on
    /// the same host). A reconnect has nothing to record.
    pub fn consume(&self, nonces: &mut NonceCache, now_unix: u64) -> Result<(), Refusal> {
        let Some(claims) = self.claims() else {
            return Ok(());
        };
        let key = match self {
            Admitted::Return(_) => format!("return:{}", claims.replay_key()),
            _ => claims.replay_key().to_string(),
        };
        nonces
            .check_and_insert(&key, claims.expires_at, now_unix)
            .map_err(Refusal::ticket)
    }
}

/// Why a Hello was refused: the `Error` frame to answer with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// [`error`] code.
    pub code: u32,
    /// Message.
    pub message: String,
}

impl Refusal {
    /// A refusal with `code`.
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// `UNAUTHORIZED` for a ticket error.
    pub fn ticket(error: TicketError) -> Self {
        let code = match error {
            TicketError::NonceCacheFull => error::ZONE_UNAVAILABLE,
            _ => error::UNAUTHORIZED,
        };
        Self::new(code, format!("{error:?}"))
    }
}

/// Check a Hello for `zone_id` at `now_unix` (seconds). Pure: nothing is
/// recorded (see [`Admitted::consume`]).
pub fn admit(
    config: &AdmissionConfig,
    hello: &Hello,
    zone_id: &str,
    now_unix: u64,
) -> Result<Admitted, Refusal> {
    // Refuse other versions before the ticket is touched, so the client can
    // retry with an older one (§6.0).
    if !supports_protocol(hello.protocol_version) {
        return Err(Refusal::new(
            error::VERSION_MISMATCH,
            format!(
                "protocol {} unsupported ({MIN_PROTOCOL_VERSION}..={PROTOCOL_VERSION})",
                hello.protocol_version
            ),
        ));
    }
    if !hello.reconnect_token.is_empty() {
        return Ok(Admitted::Reconnect(hello.reconnect_token.clone()));
    }
    let other_zone = || Refusal::new(error::UNAUTHORIZED, "ticket is for another zone");
    match verify(config, &hello.ticket, zone_id, now_unix).map_err(Refusal::ticket)? {
        VerifiedTicket::Join(claims) if claims.zone == zone_id => Ok(Admitted::Join(claims)),
        VerifiedTicket::Join(_) => Err(other_zone()),
        VerifiedTicket::Transfer(transfer) if transfer.claims.zone == zone_id => {
            Ok(Admitted::Arrival(transfer))
        }
        VerifiedTicket::Transfer(mut transfer) if transfer.source_zone == zone_id => {
            let never_reached = std::mem::replace(&mut transfer.claims.zone, zone_id.to_string());
            transfer.source_zone = never_reached;
            Ok(Admitted::Return(transfer))
        }
        VerifiedTicket::Transfer(_) => Err(other_zone()),
    }
}

/// Transfer tickets always use `config.key`; join tickets go through the
/// configured verifier and are then checked for a principal, a replay key,
/// expiry and declared extensions.
fn verify(
    config: &AdmissionConfig,
    ticket: &str,
    zone_id: &str,
    now_unix: u64,
) -> Result<VerifiedTicket, TicketError> {
    let verified = match &config.verifier {
        None => config.key.verify_ticket(ticket, now_unix)?,
        Some(custom) => match config.key.verify_ticket(ticket, now_unix) {
            Ok(VerifiedTicket::Transfer(transfer)) => VerifiedTicket::Transfer(transfer),
            _ => VerifiedTicket::Join(custom.0.verify(ticket, zone_id, now_unix)?),
        },
    };
    let claims = match &verified {
        VerifiedTicket::Join(claims) => claims,
        VerifiedTicket::Transfer(transfer) => &transfer.claims,
    };
    if claims.principal.is_empty() {
        return Err(TicketError::MissingClaim("sub"));
    }
    if claims.replay_key().is_empty() {
        return Err(TicketError::MissingClaim("nonce"));
    }
    if claims.expires_at <= now_unix {
        return Err(TicketError::Expired);
    }
    validate_extensions(config.extensions.as_ref(), &claims.extensions)
        .map_err(|e| TicketError::Invalid(e.to_string()))?;
    Ok(verified)
}

/// What the gate does with one client message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// Handle it.
    Pass,
    /// Drop it; `warn`: answer `Error(RATE_LIMITED)` (at most once per
    /// second).
    Drop {
        /// Tell the client.
        warn: bool,
    },
    /// A flood (more than a burst dropped within a second): kick the
    /// connection (`Kick(RATE_LIMITED)`).
    Kick,
}

/// Per-connection token bucket over every post-Hello client message.
/// Times are any monotonic clock (`Duration` since an origin of the
/// driver's choosing).
#[derive(Debug, Clone)]
pub struct InputGate {
    tokens: f64,
    rate: f64,
    burst: f64,
    last: Duration,
    /// Start of the second the drops are counted in (and warned about
    /// once); `None` before the first drop.
    drop_window: Option<Duration>,
    dropped: u32,
}

impl InputGate {
    /// A full bucket at `now`.
    pub fn new(config: &AdmissionConfig, now: Duration) -> Self {
        Self {
            tokens: config.input_burst,
            rate: config.input_rate,
            burst: config.input_burst,
            last: now,
            drop_window: None,
            dropped: 0,
        }
    }

    /// Admit one message at `now`. More than a burst of drops within one
    /// second is a kick, however the drops interleave with refills (a
    /// flood is kicked at any delivery speed).
    pub fn take(&mut self, now: Duration) -> Gate {
        let elapsed = now.saturating_sub(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last = self.last.max(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Gate::Pass;
        }
        let warn = self
            .drop_window
            .is_none_or(|start| now.saturating_sub(start) >= Duration::from_secs(1));
        if warn {
            self.drop_window = Some(now);
            self.dropped = 0;
        }
        self.dropped += 1;
        if f64::from(self.dropped) > self.burst {
            return Gate::Kick;
        }
        Gate::Drop { warn }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Role;

    const ZONE: &str = "z1";

    fn config() -> AdmissionConfig {
        AdmissionConfig::new(TicketKey::new("admission-test-secret"))
    }

    fn claims(zone: &str, nonce: &str) -> TicketClaims {
        TicketClaims {
            principal: "ana".into(),
            zone: zone.into(),
            expires_at: 2_000,
            nonce: nonce.into(),
            role: Role::Player,
            name: None,
            extensions: Default::default(),
            wake: false,
        }
    }

    fn hello(ticket: String) -> Hello {
        Hello {
            protocol_version: PROTOCOL_VERSION,
            ticket,
            ..Hello::default()
        }
    }

    fn transfer(target: &str, source: &str) -> String {
        config().key.sign_transfer(&Transfer {
            claims: claims(target, "t1"),
            source_zone: source.into(),
            issuer: "host".into(),
            generation: 1,
            carried: vec![1],
            departed_at_ms: 0,
        })
    }

    #[test]
    fn admits_join_arrival_return_and_reconnect() {
        let c = config();
        let join = hello(c.key.sign(&claims(ZONE, "n1")));
        assert_eq!(
            admit(&c, &join, ZONE, 1_000),
            Ok(Admitted::Join(claims(ZONE, "n1")))
        );
        let arrival = admit(&c, &hello(transfer(ZONE, "z0")), ZONE, 1_000).unwrap();
        assert!(matches!(arrival, Admitted::Arrival(t) if t.source_zone == "z0"));
        let back = admit(&c, &hello(transfer("z2", ZONE)), ZONE, 1_000).unwrap();
        assert!(
            matches!(&back, Admitted::Return(t) if t.claims.zone == ZONE && t.source_zone == "z2")
        );
        let token = Hello {
            reconnect_token: "tok".into(),
            ..hello(String::new())
        };
        assert_eq!(
            admit(&c, &token, ZONE, 1_000),
            Ok(Admitted::Reconnect("tok".into()))
        );
    }

    #[test]
    fn refuses_bad_tickets_before_anything_is_recorded() {
        let c = config();
        let refused = |hello: Hello| admit(&c, &hello, ZONE, 1_000).unwrap_err().code;
        assert_eq!(refused(hello("garbage".into())), error::UNAUTHORIZED);
        let forged = TicketKey::new("another-key").sign(&claims(ZONE, "n1"));
        assert_eq!(refused(hello(forged)), error::UNAUTHORIZED);
        let expired = c.key.sign(&TicketClaims {
            expires_at: 1_000,
            ..claims(ZONE, "n1")
        });
        assert_eq!(refused(hello(expired)), error::UNAUTHORIZED);
        let elsewhere = c.key.sign(&claims("z9", "n1"));
        assert_eq!(refused(hello(elsewhere)), error::UNAUTHORIZED);
        assert_eq!(
            refused(hello(transfer("z8", "z9"))),
            error::UNAUTHORIZED,
            "a transfer for neither end"
        );
        let old = Hello {
            protocol_version: PROTOCOL_VERSION + 1,
            ..hello(c.key.sign(&claims(ZONE, "n1")))
        };
        assert_eq!(refused(old), error::VERSION_MISMATCH);
        let ext = c.key.sign(&TicketClaims {
            extensions: crate::TicketExtensions::from_json(serde_json::json!({"team": 1})).unwrap(),
            ..claims(ZONE, "n1")
        });
        assert_eq!(refused(hello(ext)), error::UNAUTHORIZED, "undeclared ext");
    }

    #[test]
    fn custom_verifiers_still_need_principal_nonce_and_expiry() {
        let c = config().with_ticket_verifier(|_: &str, zone: &str, _: u64| {
            Ok(TicketClaims {
                nonce: String::new(),
                ..claims(zone, "")
            })
        });
        let refusal = admit(&c, &hello("anything".into()), ZONE, 1_000).unwrap_err();
        assert_eq!(refusal.code, error::UNAUTHORIZED);
        assert!(refusal.message.contains("nonce"), "{refusal:?}");
    }

    #[test]
    fn tickets_are_single_use_and_returns_have_their_own_key() {
        let c = config();
        let mut nonces = NonceCache::default();
        let join = admit(&c, &hello(c.key.sign(&claims(ZONE, "t1"))), ZONE, 1_000).unwrap();
        join.consume(&mut nonces, 1_000).unwrap();
        assert_eq!(
            join.consume(&mut nonces, 1_000).unwrap_err().code,
            error::UNAUTHORIZED
        );
        let back = admit(&c, &hello(transfer("z2", ZONE)), ZONE, 1_000).unwrap();
        back.consume(&mut nonces, 1_000).unwrap();
        assert!(back.consume(&mut nonces, 1_000).is_err());
        Admitted::Reconnect("x".into())
            .consume(&mut nonces, 1_000)
            .unwrap();
    }

    #[test]
    fn the_gate_drops_warns_once_a_second_and_kicks_a_flood() {
        let c = AdmissionConfig {
            input_rate: 10.0,
            input_burst: 3.0,
            ..config()
        };
        let t = Duration::from_secs(100);
        let mut gate = InputGate::new(&c, t);
        for _ in 0..3 {
            assert_eq!(gate.take(t), Gate::Pass);
        }
        assert_eq!(gate.take(t), Gate::Drop { warn: true });
        assert_eq!(gate.take(t), Gate::Drop { warn: false });
        // A token every 100 ms.
        assert_eq!(gate.take(t + Duration::from_millis(100)), Gate::Pass);
        assert_eq!(
            gate.take(t + Duration::from_millis(100)),
            Gate::Drop { warn: false }
        );
        assert_eq!(
            gate.take(t + Duration::from_millis(1_100)),
            Gate::Pass,
            "refilled"
        );
        let later = t + Duration::from_millis(1_100);
        for _ in 0..2 {
            assert_eq!(gate.take(later), Gate::Pass);
        }
        assert_eq!(gate.take(later), Gate::Drop { warn: true });
        for _ in 0..2 {
            assert_eq!(gate.take(later), Gate::Drop { warn: false });
        }
        assert_eq!(gate.take(later), Gate::Kick, "more than a burst dropped");
    }

    #[test]
    fn a_flood_interleaved_with_refills_is_kicked() {
        let c = AdmissionConfig {
            input_rate: 10.0,
            input_burst: 3.0,
            ..config()
        };
        let t = Duration::from_secs(100);
        let mut gate = InputGate::new(&c, t);
        // A message every 20 ms: a token every 5th, which passes.
        let kicked_at =
            (0..50u64).position(|i| gate.take(t + Duration::from_millis(20 * i)) == Gate::Kick);
        assert_eq!(kicked_at, Some(7), "3 burst, drops 1-2, a pass, drops 3-4");
        // Just over the rate for long stretches is dropped, never kicked.
        let mut gate = InputGate::new(&c, t);
        for i in 0..1_000u64 {
            let got = gate.take(t + Duration::from_millis(90 * i));
            assert_ne!(got, Gate::Kick, "message {i}");
        }
    }
}
