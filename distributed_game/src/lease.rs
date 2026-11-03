//! Entity leases between zones: one zone lends an entity (a mob chasing a
//! player through a doorway) to the zone next door without ever having it
//! live in both, and without losing it to a dropped message or a crashed
//! or sleeping zone.
//!
//! The *home* zone keeps the entity [`Lent`] (hidden there, its slot
//! reserved); the zone it went to keeps it [`Held`] while the lease holds.
//! The holder beats every [`LeaseTimings::beat`] ticks; the home answers
//! each beat with a renewal, or a recall when it no longer lends that
//! transfer. A [`Held`] entity whose last renewal is older than
//! [`LeaseTimings::lease`] lapses (the holder drops it); a [`Lent`] one
//! that has not been heard from for [`LeaseTimings::grace`] comes back
//! home. Grace is longer than the lease, so the holder always lets go
//! first: a lost message, or either zone stopping, costs the entity at
//! most the grace, never the entity.
//!
//! This module only keeps the state and the clock; the messages are the
//! game's own ([`Ctx::send_zone`](crate::Ctx::send_zone), typed by
//! [`Zone::Message`](crate::Zone::Message)). A game defines one per step:
//!
//! | Message | From → to | Receiver |
//! |---------|-----------|----------|
//! | arrive (the entity's state) | home → holder | [`LeaseTimings::hold`]; refuse with a recall |
//! | beat | holder → home | [`Lent::heard`] and renew, or recall if not lent |
//! | renew | home → holder | [`Held::renew`] |
//! | recall | either way | drop the [`Held`] / bring the [`Lent`] back |
//! | returned / ended | holder → home | the entity is back (or gone): end the [`Lent`] |
//!
//! Match every message to its lease with [`Lent::is`] / [`Held::is`]
//! (sending zone and [`TransferId`]); anything else is stale and ignored.
//! The zone-to-zone transport already drops repeats of one message, but an
//! arrive can be resent after a restart: check [`Held::is`] before taking
//! it twice. A zone that hibernates keeps its lent entities' zone and
//! transfer and, on restore, lends them again from now
//! ([`LeaseTimings::lend`]): their visitor's next beat renews them, or they
//! come back after a fresh grace.

/// Names one lending of one entity: unique per lending zone across its
/// activations, and at most 2^53 − 1 (message codecs carry it exactly as a
/// JavaScript number).
pub type TransferId = u64;

/// The largest integer a JavaScript number carries exactly.
const SAFE_INT: u64 = (1 << 53) - 1;

/// Hands out [`TransferId`]s for one zone activation.
#[derive(Debug, Clone, Default)]
pub struct TransferIds {
    count: u64,
}

impl TransferIds {
    /// The next id, from the activation's
    /// [`boot_epoch`](crate::Ctx::boot_epoch) (so a restarted zone never
    /// reuses one its previous activation handed out).
    pub fn next(&mut self, boot_epoch: u64) -> TransferId {
        self.count += 1;
        ((boot_epoch << 20) ^ self.count) & SAFE_INT
    }
}

/// How often a holder beats and how long each side waits (ticks of each
/// zone's own clock).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTimings {
    beat: u64,
    lease: u64,
    grace: u64,
}

impl LeaseTimings {
    /// Beat every `beat` ticks; a holder keeps the entity `lease` ticks past
    /// its last renewal; the home takes it back `grace` ticks after the last
    /// beat it heard. `lease - beat` is the time a renewal has to arrive,
    /// and `grace - lease` the time a renewal may take on the way (plus any
    /// difference in the zones' tick rates).
    ///
    /// # Panics
    /// Unless `0 < beat < lease < grace` (a compile error in a `const`).
    pub const fn new(beat: u64, lease: u64, grace: u64) -> Self {
        assert!(beat > 0, "a lease needs a beat");
        assert!(lease > beat, "a lease must outlast a beat");
        assert!(grace > lease, "the home's grace must outlast the lease");
        Self { beat, lease, grace }
    }

    /// Ticks between beats.
    pub const fn beat(&self) -> u64 {
        self.beat
    }

    /// Ticks a holder keeps the entity past its last renewal.
    pub const fn lease(&self) -> u64 {
        self.lease
    }

    /// Ticks the home waits after the last beat before taking it back.
    pub const fn grace(&self) -> u64 {
        self.grace
    }

    /// The home side of lending an entity to zone `to` at `tick`.
    pub fn lend(&self, to: impl Into<String>, transfer: TransferId, tick: u64) -> Lent {
        Lent {
            to: to.into(),
            transfer,
            grace: self.grace,
            heard: tick,
        }
    }

    /// The holder's side of an entity zone `from` lent it at `tick`.
    pub fn hold(&self, from: impl Into<String>, transfer: TransferId, tick: u64) -> Held {
        Held {
            from: from.into(),
            transfer,
            beat: self.beat,
            lease: self.lease,
            until: tick + self.lease,
            next_beat: tick + self.beat,
        }
    }
}

/// An entity this zone lent out (home side).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lent {
    to: String,
    transfer: TransferId,
    grace: u64,
    heard: u64,
}

impl Lent {
    /// The zone holding it.
    pub fn to(&self) -> &str {
        &self.to
    }

    /// Its transfer.
    pub fn transfer(&self) -> TransferId {
        self.transfer
    }

    /// Whether a message from zone `from` about `transfer` is about this one.
    pub fn is(&self, from: &str, transfer: TransferId) -> bool {
        self.to == from && self.transfer == transfer
    }

    /// Its holder beat at `tick` (answer with a renewal).
    pub fn heard(&mut self, tick: u64) {
        self.heard = self.heard.max(tick);
    }

    /// Not heard from for longer than the grace: take it back.
    pub fn overdue(&self, tick: u64) -> bool {
        tick > self.heard + self.grace
    }
}

/// An entity another zone lent this one (holder side).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    from: String,
    transfer: TransferId,
    beat: u64,
    lease: u64,
    until: u64,
    next_beat: u64,
}

/// What a [`Held`] lease needs this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// Still held; nothing to send.
    Held,
    /// Still held; send its home a beat.
    Beat,
    /// Not renewed in time: drop it (its home may have taken it back).
    Lapsed,
}

impl Held {
    /// Its home zone.
    pub fn from(&self) -> &str {
        &self.from
    }

    /// Its transfer.
    pub fn transfer(&self) -> TransferId {
        self.transfer
    }

    /// Whether a message from zone `from` about `transfer` is about this one.
    pub fn is(&self, from: &str, transfer: TransferId) -> bool {
        self.from == from && self.transfer == transfer
    }

    /// Its home renewed it at `tick`.
    pub fn renew(&mut self, tick: u64) {
        self.until = self.until.max(tick + self.lease);
    }

    /// Once a tick: lapse it, or say a beat is due.
    pub fn poll(&mut self, tick: u64) -> Hold {
        if tick > self.until {
            Hold::Lapsed
        } else if tick >= self.next_beat {
            self.next_beat = tick + self.beat;
            Hold::Beat
        } else {
            Hold::Held
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: LeaseTimings = LeaseTimings::new(30, 60, 120);

    #[test]
    #[should_panic(expected = "grace must outlast")]
    fn grace_must_outlast_the_lease() {
        LeaseTimings::new(30, 60, 60);
    }

    #[test]
    #[should_panic(expected = "outlast a beat")]
    fn a_lease_must_outlast_a_beat() {
        LeaseTimings::new(30, 30, 120);
    }

    #[test]
    fn a_held_entity_beats_and_lapses_without_renewals() {
        let mut held = T.hold("pit.0.0", 7, 100);
        assert_eq!(held.poll(129), Hold::Held);
        assert_eq!(held.poll(130), Hold::Beat);
        assert_eq!(held.poll(131), Hold::Held);
        assert_eq!(held.poll(160), Hold::Beat);
        assert_eq!(held.poll(161), Hold::Lapsed);
    }

    #[test]
    fn renewals_keep_it_and_an_old_one_never_shortens_the_lease() {
        let mut held = T.hold("pit.0.0", 7, 0);
        held.renew(50);
        held.renew(10);
        assert_eq!(held.poll(110), Hold::Beat);
        assert_eq!(held.poll(111), Hold::Lapsed);
    }

    #[test]
    fn the_holder_always_lets_go_before_the_home_takes_it_back() {
        // The home hears the last beat at 200; its renewal takes `d` ticks
        // to arrive. With d below grace − lease the holder lapses first.
        for d in 0..(T.grace() - T.lease()) {
            let mut lent = T.lend("pit.1.0", 9, 0);
            let mut held = T.hold("pit.0.0", 9, 0);
            lent.heard(200);
            held.renew(200 + d);
            let lapsed = (200..).find(|&t| held.poll(t) == Hold::Lapsed).unwrap();
            let back = (200..).find(|&t| lent.overdue(t)).unwrap();
            assert!(lapsed < back, "d={d}: lapsed {lapsed}, back {back}");
        }
    }

    #[test]
    fn messages_match_by_zone_and_transfer() {
        let lent = T.lend("pit.1.0", 9, 0);
        assert!(lent.is("pit.1.0", 9));
        assert!(!lent.is("pit.1.0", 8));
        assert!(!lent.is("pit.0.1", 9));
        let held = T.hold("pit.0.0", 9, 0);
        assert!(held.is("pit.0.0", 9));
        assert!(!held.is("pit.1.0", 9));
    }

    #[test]
    fn transfer_ids_differ_across_activations_and_stay_safe_integers() {
        let mut a = TransferIds::default();
        let mut b = TransferIds::default();
        let epoch = u64::MAX - 3;
        let first = a.next(epoch);
        assert!(first <= SAFE_INT);
        assert_ne!(first, a.next(epoch));
        assert_ne!(first, b.next(epoch + 1));
    }
}
