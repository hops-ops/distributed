//! Guest principals and display names (dev ticket endpoint only).
//!
//! The principal is always server-generated (`guest-<hex>`); a caller can
//! only choose a display name, which zones look up by the verified principal
//! from the ticket. A returning browser presents the server-signed guest
//! token it got earlier to keep the same principal (so a reconnect after the
//! grace window is the same player), never a raw principal.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use distributed_game::net::TicketKey;

/// Longest display name (characters).
pub const MAX_NAME: usize = 16;

/// principal → display name, shared by the dev routes and the zones.
#[derive(Clone, Default)]
pub struct Guests {
    names: Arc<Mutex<HashMap<String, String>>>,
}

impl Guests {
    /// Display name registered for `principal`.
    pub fn name(&self, principal: &str) -> Option<String> {
        self.names.lock().expect("guests").get(principal).cloned()
    }

    /// Register (or rename) a guest.
    pub fn set_name(&self, principal: &str, name: String) {
        self.names
            .lock()
            .expect("guests")
            .insert(principal.to_string(), name);
    }

    /// Number of known guests.
    pub fn len(&self) -> usize {
        self.names.lock().expect("guests").len()
    }

    /// True when nobody registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Keep letters, digits, space, `-` and `_`; trim; at most [`MAX_NAME`] chars.
pub fn sanitize_name(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
        .take(MAX_NAME)
        .collect::<String>()
        .trim()
        .to_string()
}

/// A fresh server-generated principal.
pub fn new_principal() -> String {
    format!("guest-{}", random_hex(8))
}

/// `principal "." mac` — proves the server issued this principal.
pub fn guest_token(key: &TicketKey, principal: &str) -> String {
    format!("{principal}.{}", mac_hex(key, principal))
}

/// The principal inside a valid guest token.
pub fn verify_guest_token(key: &TicketKey, token: &str) -> Option<String> {
    let (principal, mac) = token.rsplit_once('.')?;
    (principal.starts_with("guest-") && mac == mac_hex(key, principal))
        .then(|| principal.to_string())
}

fn mac_hex(key: &TicketKey, principal: &str) -> String {
    key.hmac(format!("demo-guest|{principal}").as_bytes())[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// `len` random hex characters (OS-seeded SipHash keys; dev quality).
pub fn random_hex(len: usize) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut out = String::with_capacity(len + 16);
    while out.len() < len {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out.truncate(len);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_sanitized_and_tokens_verify() {
        assert_eq!(sanitize_name("  <b>Ana</b>!! "), "bAnab");
        assert_eq!(sanitize_name(&"x".repeat(40)).len(), MAX_NAME);
        let key = TicketKey::new("k");
        let principal = new_principal();
        let token = guest_token(&key, &principal);
        assert_eq!(verify_guest_token(&key, &token), Some(principal.clone()));
        assert_eq!(verify_guest_token(&TicketKey::new("other"), &token), None);
        assert_eq!(verify_guest_token(&key, "admin.deadbeef"), None);
        assert_ne!(new_principal(), principal);
    }
}
