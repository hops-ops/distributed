//! Issued-transfer records: make every transfer ticket single-use across
//! hosts, and let a failed handoff return to its source.
//!
//! The host that issues a transfer ticket (the source zone's host) keeps a
//! record per nonce:
//!
//! ```text
//! Pending ──reserve(target)──► Reserved ──confirm──► Consumed
//!    │                           │  └──release (target refused)──► Pending
//!    │                           └──(reservation expires)──► Consumed
//!    └──return(source)──► Consumed
//! ```
//!
//! The target reserves before admitting and confirms after; the source
//! accepts the same ticket back ("return") only while Pending. A reservation
//! that is never confirmed or released counts as consumed: if in doubt the
//! player is *not* let back into the source (never in two zones; worst
//! case it needs a fresh lobby ticket).

use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Pending,
    Reserved { until: u64 },
    Consumed,
}

/// Registry operation (also the admin wire name).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferOp {
    /// Target is about to admit.
    Reserve,
    /// Target admitted.
    Confirm,
    /// Target refused; the ticket may be used again (e.g. to return).
    Release,
    /// Source re-admits the player.
    Return,
}

impl TransferOp {
    /// Wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserve => "reserve",
            Self::Confirm => "confirm",
            Self::Release => "release",
            Self::Return => "return",
        }
    }

    /// Parse a wire name.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "reserve" => Self::Reserve,
            "confirm" => Self::Confirm,
            "release" => Self::Release,
            "return" => Self::Return,
            _ => return None,
        })
    }
}

/// Transfer records of the tickets this host issued.
#[derive(Debug, Default)]
pub struct TransferRegistry {
    entries: HashMap<String, (State, u64)>,
}

impl TransferRegistry {
    /// Record a newly issued ticket (`expires_at` Unix seconds).
    pub fn issue(&mut self, nonce: &str, expires_at: u64) {
        self.entries
            .insert(nonce.to_string(), (State::Pending, expires_at));
    }

    /// Apply `op` at `now` (Unix seconds); `reserve_for` bounds a reservation.
    pub fn apply(
        &mut self,
        op: TransferOp,
        nonce: &str,
        now: u64,
        reserve_for: u64,
    ) -> Result<(), &'static str> {
        if self.entries.len() > 4096 {
            self.entries.retain(|_, (_, exp)| *exp + reserve_for > now);
        }
        let Some((state, expires_at)) = self.entries.get_mut(nonce) else {
            return Err("unknown transfer");
        };
        if let State::Reserved { until } = *state {
            if until <= now {
                *state = State::Consumed;
            }
        }
        match (op, *state) {
            (TransferOp::Reserve, State::Pending) if *expires_at > now => {
                *state = State::Reserved {
                    until: now + reserve_for,
                };
                Ok(())
            }
            (TransferOp::Confirm, State::Reserved { .. }) => {
                *state = State::Consumed;
                Ok(())
            }
            (TransferOp::Release, State::Reserved { .. }) => {
                *state = State::Pending;
                Ok(())
            }
            (TransferOp::Return, State::Pending) if *expires_at > now => {
                *state = State::Consumed;
                Ok(())
            }
            (_, State::Reserved { .. }) => Err("transfer in progress"),
            (_, State::Consumed) => Err("transfer already used"),
            _ => Err("transfer expired"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_use_with_release_and_return() {
        let mut r = TransferRegistry::default();
        r.issue("n", 100);
        r.apply(TransferOp::Reserve, "n", 10, 5).unwrap();
        assert!(r.apply(TransferOp::Return, "n", 11, 5).is_err());
        r.apply(TransferOp::Release, "n", 11, 5).unwrap();
        r.apply(TransferOp::Return, "n", 12, 5).unwrap();
        assert!(r.apply(TransferOp::Reserve, "n", 13, 5).is_err());

        r.issue("m", 100);
        r.apply(TransferOp::Reserve, "m", 10, 5).unwrap();
        r.apply(TransferOp::Confirm, "m", 11, 5).unwrap();
        assert!(r.apply(TransferOp::Return, "m", 12, 5).is_err());

        // An unconfirmed reservation that lapses counts as used.
        r.issue("k", 100);
        r.apply(TransferOp::Reserve, "k", 10, 5).unwrap();
        assert_eq!(
            r.apply(TransferOp::Return, "k", 20, 5),
            Err("transfer already used")
        );
        r.issue("e", 15);
        assert_eq!(
            r.apply(TransferOp::Reserve, "e", 20, 5),
            Err("transfer expired")
        );
    }
}
