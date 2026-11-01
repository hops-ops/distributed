use serde::{Deserialize, Serialize};

use super::{FencingToken, LeaseError};

/// Resource-side fencing high-water mark.
///
/// Embed one in the state a lease protects (it is `Serialize`, so it can live
/// in an event-sourced aggregate and be rebuilt on replay). Call
/// [`admit`](Self::admit) with the token carried by each write: tokens older
/// than the newest admitted one are rejected, equal tokens (the same holder
/// writing again) and newer tokens (a new holder) are accepted.
///
/// Because the check and the write happen in the same cell/transaction, this
/// is the authoritative fence; [`LeaseStore::check_fence`](super::LeaseStore::check_fence)
/// is only advisory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    high_water: Option<FencingToken>,
}

impl Fence {
    /// An empty fence (admits any token).
    pub const fn new() -> Self {
        Self { high_water: None }
    }

    /// The newest token admitted so far.
    pub fn high_water(&self) -> Option<FencingToken> {
        self.high_water
    }

    /// Whether `token` would be admitted, without recording it.
    pub fn would_admit(&self, token: FencingToken) -> bool {
        self.high_water.is_none_or(|seen| token >= seen)
    }

    /// Admit `token` (raising the high-water mark) or reject it as stale.
    /// `key` only labels the error.
    pub fn admit(&mut self, key: &str, token: FencingToken) -> Result<(), LeaseError> {
        match self.high_water {
            Some(current) if token < current => Err(LeaseError::StaleFence {
                key: key.to_string(),
                presented: token,
                current,
            }),
            _ => {
                self.high_water = Some(token);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_rejects_only_older_tokens() {
        let mut fence = Fence::new();
        fence.admit("k", FencingToken::new(2)).unwrap();
        fence.admit("k", FencingToken::new(2)).unwrap();
        assert!(matches!(
            fence.admit("k", FencingToken::new(1)),
            Err(LeaseError::StaleFence { .. })
        ));
        fence.admit("k", FencingToken::new(5)).unwrap();
        assert_eq!(fence.high_water(), Some(FencingToken::new(5)));
        assert!(!fence.would_admit(FencingToken::new(4)));
    }
}
