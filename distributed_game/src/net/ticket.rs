//! HMAC-SHA256 signed admission tickets (JWT HS256 compact form).
//!
//! `base64url(header) "." base64url(claims) "." base64url(HMAC-SHA256(key,
//! base64url(header) "." base64url(claims)))`, unpadded base64url. The header
//! must be `{"alg":"HS256",...}`; claims are
//! `{"sub","zone","exp","nonce","role"?}` where `exp` is Unix seconds and
//! `role` is `"player"` (default) or `"observer"`. Any HS256 JWT library can
//! mint them. The principal is taken **only** from a verified `sub`.
//!
//! **Transfer tickets** (zone handoff) are the same format with
//! `"kind":"transfer"` plus `src` (source zone), `iss` (issuing host id),
//! `gen` (source ownership token), `left` (source departure stamp time,
//! Unix ms) and `carry` (base64url carried state).
//! [`TicketKey::verify`] rejects them ([`TicketError::WrongKind`]); use
//! [`TicketKey::verify_ticket`] to accept either kind.
//!
//! **Custom admission tickets.** Join tickets go through a
//! [`TicketVerifier`] (`ServerConfig::ticket_verifier`); the default is the
//! `TicketKey` itself. A game whose web backend already mints its own tickets
//! (different claim names, issuer/audience checks, no `zone`/`nonce`)
//! implements `TicketVerifier` (or passes a closure) and maps them onto
//! [`TicketClaims`]: `zone` is the route zone the client asked for, `nonce`
//! the replay key (e.g. the `jti`). The server still enforces
//! `claims.zone == route zone`, expiry and single use. Transfer tickets are
//! always minted and verified by the server's own `TicketKey`.
//! [`decode_hs256`] helps verifiers of other HS256 JWTs.

use std::collections::HashMap;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

use crate::handoff::Transfer;
use crate::protocol::Role;
use crate::zone::TicketClaims;

type HmacSha256 = Hmac<Sha256>;

/// Why a ticket was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TicketError {
    /// Not three base64url segments of JSON.
    Malformed,
    /// Header `alg` is not HS256.
    UnsupportedAlgorithm,
    /// Signature mismatch.
    BadSignature,
    /// Required claim missing or mistyped.
    MissingClaim(&'static str),
    /// `exp` in the past.
    Expired,
    /// Nonce already used.
    Replayed,
    /// A transfer ticket where a join ticket was expected (or vice versa).
    WrongKind,
    /// Signed for another zone than the one requested.
    WrongZone,
    /// Refused by a custom [`TicketVerifier`] (issuer, audience, ...).
    Invalid(String),
}

/// Verifies **join** tickets presented in Hello.
///
/// `zone` is the zone id of the route the client connected to and `now_unix`
/// the current Unix time in seconds. Return the verified claims (principal
/// only ever from the verified token); the server then checks
/// `claims.zone == zone`, `claims.expires_at > now_unix` and that
/// [`TicketClaims::replay_key`] was not used before.
///
/// Implemented by [`TicketKey`] (the built-in HS256 format) and by any
/// `Fn(&str, &str, u64) -> Result<TicketClaims, TicketError>` closure.
pub trait TicketVerifier: Send + Sync + 'static {
    /// Verify `token` for `zone` at `now_unix`.
    fn verify(&self, token: &str, zone: &str, now_unix: u64) -> Result<TicketClaims, TicketError>;
}

impl TicketVerifier for TicketKey {
    fn verify(&self, token: &str, zone: &str, now_unix: u64) -> Result<TicketClaims, TicketError> {
        let claims = TicketKey::verify(self, token, now_unix)?;
        if claims.zone != zone {
            return Err(TicketError::WrongZone);
        }
        Ok(claims)
    }
}

impl<F> TicketVerifier for F
where
    F: Fn(&str, &str, u64) -> Result<TicketClaims, TicketError> + Send + Sync + 'static,
{
    fn verify(&self, token: &str, zone: &str, now_unix: u64) -> Result<TicketClaims, TicketError> {
        self(token, zone, now_unix)
    }
}

/// A shared [`TicketVerifier`] (`ServerConfig::ticket_verifier`).
#[derive(Clone)]
pub struct SharedTicketVerifier(pub std::sync::Arc<dyn TicketVerifier>);

impl SharedTicketVerifier {
    /// Wrap a verifier.
    pub fn new(verifier: impl TicketVerifier) -> Self {
        Self(std::sync::Arc::new(verifier))
    }
}

impl std::fmt::Debug for SharedTicketVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedTicketVerifier(..)")
    }
}

/// Verify the signature of a compact HS256 JWT with `secret` and return its
/// claims object (no claim is interpreted, not even `exp`). For custom
/// [`TicketVerifier`]s over tickets minted elsewhere.
pub fn decode_hs256(token: &str, secret: &[u8]) -> Result<Value, TicketError> {
    TicketKey::new(secret).decode(token)
}

/// A verified ticket of either kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedTicket {
    /// Lobby-issued admission ticket.
    Join(TicketClaims),
    /// Server-issued handoff transfer ticket.
    Transfer(Transfer),
}

/// Shared HMAC key.
#[derive(Clone)]
pub struct TicketKey {
    secret: Vec<u8>,
}

impl std::fmt::Debug for TicketKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TicketKey([redacted])")
    }
}

impl TicketKey {
    /// Key from secret bytes.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    fn mac(&self) -> HmacSha256 {
        HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts any key length")
    }

    /// HMAC-SHA256 of `data`.
    pub fn hmac(&self, data: &[u8]) -> Vec<u8> {
        let mut mac = self.mac();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Mint a ticket (dev tooling and tests; production issuers can use any
    /// HS256 JWT library).
    pub fn sign(&self, claims: &TicketClaims) -> String {
        let body = json!({
            "sub": claims.principal,
            "zone": claims.zone,
            "exp": claims.expires_at,
            "nonce": claims.nonce,
            "role": match claims.role { Role::Player => "player", Role::Observer => "observer" },
        });
        self.sign_body(&with_name(body, claims))
    }

    /// Mint a transfer ticket for a handoff.
    pub fn sign_transfer(&self, transfer: &Transfer) -> String {
        let claims = &transfer.claims;
        let body = json!({
            "sub": claims.principal,
            "zone": claims.zone,
            "exp": claims.expires_at,
            "nonce": claims.nonce,
            "role": "player",
            "kind": "transfer",
            "src": transfer.source_zone,
            "iss": transfer.issuer,
            "gen": transfer.generation,
            "left": transfer.departed_at_ms,
            "carry": URL_SAFE_NO_PAD.encode(&transfer.carried),
        });
        self.sign_body(&with_name(body, claims))
    }

    /// Sign an arbitrary JSON claims object as an HS256 JWT (tests and dev
    /// tooling for custom [`TicketVerifier`] formats).
    pub fn sign_json(&self, claims: &Value) -> String {
        self.sign_body(claims)
    }

    fn sign_body(&self, body: &Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(body.to_string());
        let signing_input = format!("{header}.{payload}");
        let sig = URL_SAFE_NO_PAD.encode(self.hmac(signing_input.as_bytes()));
        format!("{signing_input}.{sig}")
    }

    /// Verify a join or transfer ticket (signature, expiry, claims).
    pub fn verify_ticket(
        &self,
        ticket: &str,
        now_unix: u64,
    ) -> Result<VerifiedTicket, TicketError> {
        let (claims, body) = self.verify_inner(ticket, now_unix)?;
        match body.get("kind").and_then(Value::as_str) {
            None | Some("join") => Ok(VerifiedTicket::Join(claims)),
            Some("transfer") => {
                let text = |name: &'static str| {
                    body.get(name)
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                        .map(str::to_string)
                        .ok_or(TicketError::MissingClaim(name))
                };
                let carried = URL_SAFE_NO_PAD
                    .decode(
                        body.get("carry")
                            .and_then(Value::as_str)
                            .ok_or(TicketError::MissingClaim("carry"))?,
                    )
                    .map_err(|_| TicketError::MissingClaim("carry"))?;
                if claims.role != Role::Player {
                    return Err(TicketError::MissingClaim("role"));
                }
                Ok(VerifiedTicket::Transfer(Transfer {
                    source_zone: text("src")?,
                    issuer: text("iss")?,
                    generation: body
                        .get("gen")
                        .and_then(Value::as_u64)
                        .ok_or(TicketError::MissingClaim("gen"))?,
                    departed_at_ms: body
                        .get("left")
                        .and_then(Value::as_u64)
                        .ok_or(TicketError::MissingClaim("left"))?,
                    carried,
                    claims,
                }))
            }
            Some(_) => Err(TicketError::WrongKind),
        }
    }

    /// Verify signature and expiry (`now_unix` seconds) of a **join**
    /// ticket. Nonce reuse is checked separately with a [`NonceCache`].
    pub fn verify(&self, ticket: &str, now_unix: u64) -> Result<TicketClaims, TicketError> {
        match self.verify_ticket(ticket, now_unix)? {
            VerifiedTicket::Join(claims) => Ok(claims),
            VerifiedTicket::Transfer(_) => Err(TicketError::WrongKind),
        }
    }

    /// Signature check only; returns the claims object.
    fn decode(&self, ticket: &str) -> Result<Value, TicketError> {
        let mut parts = ticket.split('.');
        let (Some(h), Some(p), Some(s), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(TicketError::Malformed);
        };
        let header: Value = decode_json(h)?;
        if header.get("alg").and_then(Value::as_str) != Some("HS256") {
            return Err(TicketError::UnsupportedAlgorithm);
        }
        let sig = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| TicketError::Malformed)?;
        let mut mac = self.mac();
        mac.update(h.as_bytes());
        mac.update(b".");
        mac.update(p.as_bytes());
        mac.verify_slice(&sig)
            .map_err(|_| TicketError::BadSignature)?;
        decode_json(p)
    }

    fn verify_inner(
        &self,
        ticket: &str,
        now_unix: u64,
    ) -> Result<(TicketClaims, Value), TicketError> {
        let claims = self.decode(ticket)?;
        let text = |name: &'static str| {
            claims
                .get(name)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
                .ok_or(TicketError::MissingClaim(name))
        };
        let expires_at = claims
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or(TicketError::MissingClaim("exp"))?;
        if expires_at <= now_unix {
            return Err(TicketError::Expired);
        }
        let role = match claims.get("role").and_then(Value::as_str) {
            None | Some("player") => Role::Player,
            Some("observer") => Role::Observer,
            Some(_) => return Err(TicketError::MissingClaim("role")),
        };
        Ok((
            TicketClaims {
                principal: text("sub")?,
                zone: text("zone")?,
                expires_at,
                nonce: text("nonce")?,
                role,
                name: claims
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string),
            },
            claims,
        ))
    }
}

fn with_name(mut body: Value, claims: &TicketClaims) -> Value {
    if let (Some(name), Some(object)) = (&claims.name, body.as_object_mut()) {
        object.insert("name".into(), Value::String(name.clone()));
    }
    body
}

fn decode_json(segment: &str) -> Result<Value, TicketError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| TicketError::Malformed)?;
    serde_json::from_slice(&bytes).map_err(|_| TicketError::Malformed)
}

/// Remembers used nonces until their ticket expires (single-use tickets).
#[derive(Debug, Default)]
pub struct NonceCache {
    seen: HashMap<String, u64>,
}

impl NonceCache {
    /// Record `nonce`; `Err(Replayed)` if already used and unexpired.
    pub fn check_and_insert(
        &mut self,
        nonce: &str,
        expires_at: u64,
        now_unix: u64,
    ) -> Result<(), TicketError> {
        if self.seen.len() > 1024 {
            self.seen.retain(|_, exp| *exp > now_unix);
        }
        match self.seen.get(nonce) {
            Some(exp) if *exp > now_unix => Err(TicketError::Replayed),
            _ => {
                self.seen.insert(nonce.to_string(), expires_at);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> TicketClaims {
        TicketClaims {
            principal: "alice".into(),
            zone: "z1".into(),
            expires_at: 2_000,
            nonce: "n1".into(),
            role: Role::Player,
            name: None,
        }
    }

    #[test]
    fn signs_and_verifies() {
        let key = TicketKey::new("secret");
        let ticket = key.sign(&claims());
        assert_eq!(key.verify(&ticket, 1_000).unwrap(), claims());
        assert_eq!(key.verify(&ticket, 2_000), Err(TicketError::Expired));
        assert_eq!(
            TicketKey::new("other").verify(&ticket, 1_000),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn tampered_principal_is_rejected() {
        let key = TicketKey::new("secret");
        let ticket = key.sign(&claims());
        let parts: Vec<_> = ticket.split('.').collect();
        let forged =
            URL_SAFE_NO_PAD.encode(r#"{"sub":"mallory","zone":"z1","exp":2000,"nonce":"n1"}"#);
        let forged = format!("{}.{}.{}", parts[0], forged, parts[2]);
        assert_eq!(key.verify(&forged, 1_000), Err(TicketError::BadSignature));
    }

    #[test]
    fn transfer_tickets_round_trip_and_are_not_join_tickets() {
        let key = TicketKey::new("secret");
        let transfer = Transfer {
            claims: claims(),
            source_zone: "z0".into(),
            issuer: "h1".into(),
            generation: 9,
            carried: vec![1, 2, 3],
            departed_at_ms: 1_700_000_000_123,
        };
        let ticket = key.sign_transfer(&transfer);
        assert_eq!(
            key.verify_ticket(&ticket, 1_000).unwrap(),
            VerifiedTicket::Transfer(transfer)
        );
        assert_eq!(key.verify(&ticket, 1_000), Err(TicketError::WrongKind));
        let join = key.sign(&claims());
        assert_eq!(
            key.verify_ticket(&join, 1_000).unwrap(),
            VerifiedTicket::Join(claims())
        );
    }

    #[test]
    fn names_round_trip_and_default_verifier_checks_zone() {
        let key = TicketKey::new("secret");
        let named = claims().with_name("Ana");
        let ticket = key.sign(&named);
        assert_eq!(
            TicketVerifier::verify(&key, &ticket, "z1", 1_000),
            Ok(named)
        );
        assert_eq!(
            TicketVerifier::verify(&key, &ticket, "z2", 1_000),
            Err(TicketError::WrongZone)
        );
        assert!(decode_hs256(&ticket, b"secret").is_ok());
        assert_eq!(
            decode_hs256(&ticket, b"other"),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn nonce_is_single_use() {
        let mut cache = NonceCache::default();
        assert!(cache.check_and_insert("n", 10, 1).is_ok());
        assert_eq!(
            cache.check_and_insert("n", 10, 2),
            Err(TicketError::Replayed)
        );
        assert!(cache.check_and_insert("n", 20, 11).is_ok());
    }
}
