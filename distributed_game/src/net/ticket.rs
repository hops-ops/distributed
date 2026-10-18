//! HMAC-SHA256 signed admission tickets (JWT HS256 compact form).
//!
//! `base64url(header) "." base64url(claims) "." base64url(HMAC-SHA256(key,
//! base64url(header) "." base64url(claims)))`, unpadded base64url. The header
//! must be `{"alg":"HS256",...}`; claims are
//! `{"sub","zone","exp","nonce","role"?}` where `exp` is Unix seconds and
//! `role` is `"player"` (default) or `"observer"`. Any HS256 JWT library can
//! mint them. The principal is taken **only** from a verified `sub`.

use std::collections::HashMap;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

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
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let body = json!({
            "sub": claims.principal,
            "zone": claims.zone,
            "exp": claims.expires_at,
            "nonce": claims.nonce,
            "role": match claims.role { Role::Player => "player", Role::Observer => "observer" },
        });
        let payload = URL_SAFE_NO_PAD.encode(body.to_string());
        let signing_input = format!("{header}.{payload}");
        let sig = URL_SAFE_NO_PAD.encode(self.hmac(signing_input.as_bytes()));
        format!("{signing_input}.{sig}")
    }

    /// Verify signature and expiry (`now_unix` seconds). Nonce reuse is
    /// checked separately with a [`NonceCache`].
    pub fn verify(&self, ticket: &str, now_unix: u64) -> Result<TicketClaims, TicketError> {
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
        let claims: Value = decode_json(p)?;
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
        Ok(TicketClaims {
            principal: text("sub")?,
            zone: text("zone")?,
            expires_at,
            nonce: text("nonce")?,
            role,
        })
    }
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
