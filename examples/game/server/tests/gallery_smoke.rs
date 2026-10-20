//! Shooting Gallery: deterministic lag-compensation checks on a
//! `ZoneHost` driven by a `ManualClock` (the end-to-end run under simulated
//! latency is `client/tests/gallery-headless.test.mjs`).

use std::sync::Arc;
use std::time::Duration;

use distributed_game::GameInput;
use distributed_game::{HostConfig, ManualClock, Role, TicketClaims, Viewer, ZoneHost};
use game_demos::gallery::{
    self, Gallery, GalleryConfig, ShotInput, FLAG_FIRE, FLAG_NO_COMP, MAX_REWIND_MS,
};

const DT: Duration = Duration::from_nanos(33_333_333);

struct Rig {
    clock: ManualClock,
    host: ZoneHost<Gallery>,
    client: distributed_game::ClientId,
    player: distributed_game::PlayerId,
    seq: u32,
}

impl Rig {
    fn new() -> Self {
        let clock = ManualClock::new();
        let mut config = HostConfig::default();
        config.rewind.max_rewind = Duration::from_millis(MAX_REWIND_MS);
        let mut host = ZoneHost::<Gallery>::new(
            "gallery-1",
            config,
            Arc::new(clock.clone()),
            GalleryConfig::default(),
        );
        let joined = host
            .join(
                &TicketClaims {
                    principal: "guest-test".into(),
                    zone: "gallery-1".into(),
                    expires_at: u64::MAX,
                    nonce: "n1".into(),
                    role: Role::Player,
                    name: None,
                },
                0,
                "",
            )
            .unwrap();
        let Viewer::Player(player) = joined.viewer else {
            panic!("player")
        };
        let mut rig = Self {
            clock,
            host,
            client: joined.client,
            player,
            seq: 0,
        };
        rig.ticks(60);
        rig
    }

    fn ticks(&mut self, n: u32) {
        for _ in 0..n {
            self.clock.advance(DT);
            self.host.advance();
        }
        self.host.drain_outbound();
    }

    /// Aim from station 0's eye at target `id` as it was `ago` ticks
    /// before the latest recorded tick; claim `delay_ms` of interpolation.
    fn shoot(&mut self, id: u32, ago: u64, delay_ms: u16, flags: u8) -> gallery::ShooterStats {
        let latest = self.host.rewind().latest().unwrap().tick();
        let seen = self
            .host
            .rewind()
            .at(latest - ago)
            .and_then(|f| f.get(id))
            .unwrap()
            .pos;
        let (x, y) = gallery::station_position(0);
        let d = [seen[0] - x, seen[1] - y, seen[2] - gallery::EYE];
        let len = (d[0] * d[0] + d[1] * d[1] + d[2] * d[2]).sqrt();
        self.seq += 1;
        let shot = ShotInput::new(
            FLAG_FIRE | flags,
            self.seq as u16,
            delay_ms,
            [d[0] / len, d[1] / len, d[2] / len],
        );
        self.host
            .push_input(self.client, self.seq, latest as u32, &shot.to_payload());
        self.ticks(1);
        self.host.zone().stats(self.player).unwrap()
    }
}

#[test]
fn shot_payload_round_trips() {
    let shot = ShotInput::new(FLAG_FIRE | FLAG_NO_COMP | 0x80, 513, 287, [0.6, 0.8, 0.0]);
    let back = ShotInput::parse(&shot.to_payload()).unwrap();
    assert_eq!(
        (back.flags, back.shot, back.delay_ms),
        (3, 513, 287),
        "unknown flags cleared"
    );
    assert!((back.dx - 0.6).abs() < 1e-4 && (back.dy - 0.8).abs() < 1e-4);
    assert_eq!(ShotInput::parse(&[1, 0, 0]), None, "truncated");
    assert_eq!(ShotInput::parse(&[1; 10]), None, "trailing bytes");
    let zero = ShotInput::new(FLAG_FIRE, 1, 0, [0.0, 0.0, 0.0]);
    assert_eq!(ShotInput::parse(&zero.to_payload()), None, "zero direction");
}

#[test]
fn rewound_shots_hit_what_the_shooter_saw() {
    let mut rig = Rig::new();
    // Fast rail target (id 5, 12 m/s) as rendered 300 ms ago: 9 ticks.
    let s = rig.shoot(5, 9, 300, 0);
    assert_eq!((s.shots, s.hits, s.clamped), (1, 1, 0), "compensated hit");
    // Same aim without compensation: the target moved ~3.6 m.
    let s = rig.shoot(5, 9, 300, FLAG_NO_COMP);
    assert_eq!((s.shots, s.hits), (2, 1), "uncompensated miss");
    // Uncompensated shots hit only when aimed at the present.
    let s = rig.shoot(5, 0, 0, FLAG_NO_COMP);
    assert_eq!(s.hits, 2);
    // Claiming 2 s of delay is clamped to MAX_REWIND_MS: aiming 2 s back
    // misses, aiming at the clamp (18 ticks = 600 ms) still hits.
    let s = rig.shoot(5, 18, 2000, 0);
    assert_eq!((s.shots, s.hits, s.clamped), (4, 3, 1));
    let s = rig.shoot(2, 18, 2000, 0);
    assert_eq!((s.shots, s.clamped), (5, 2));
}

#[test]
fn targets_move_deterministically() {
    let a = gallery::target_position(4, 1.0);
    let b = gallery::target_position(4, 1.0 + 1.0 / 30.0);
    let step = ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2)).sqrt();
    assert!(
        (step - gallery::target_speed(4) / 30.0).abs() < 1e-3,
        "{step}"
    );
    for i in 0..gallery::TARGETS as usize {
        for k in 0..300 {
            let p = gallery::target_position(i, f64::from(k) * 0.1);
            assert!(p[0].abs() < gallery::HALF && p[1].abs() < gallery::HALF);
        }
    }
}
