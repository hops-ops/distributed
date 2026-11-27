//! Heap allocations on the hot loop, counted (not timed) by a counting
//! global allocator: a zone of `n` replicated entities ticking with `m`
//! clients, rewind recording and lag-compensated queries.
//!
//! The counter is per thread, so tests running in parallel do not see each
//! other's allocations and every count is deterministic.
//!
//! `cargo test -p distributed_game --test alloc -- --ignored --nocapture`
//! prints the per-site table (`alloc_report`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use distributed_game::{
    schema, Collider, Colliders, Ctx, Entities, Entity, EntityId, EntitySchema, Historic,
    HitQuery, HostConfig, ManualClock, PlayerId, Ray, RewindFrame, Role, TicketClaims, Zone,
    ZoneHost,
};

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
}

// SAFETY: forwards every call to `System` unchanged; only counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

/// What the zone does each tick.
#[derive(Debug, Clone, Copy, Default)]
struct Load {
    /// Entities.
    n: u32,
    /// Project them into the replicated world.
    replicate: bool,
    /// Record them for lag compensation.
    rewind: bool,
    /// Raycasts and rewind hit tests per tick.
    queries: u32,
    /// Every entity moves every tick (else nothing changes).
    moving: bool,
}

struct Mob {
    id: EntityId,
    x: f32,
    y: f32,
    hp: u8,
}

impl Entity for Mob {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, distributed_game::Replicated)]
#[replicated(kind = "mob", id = 1, of = Mob)]
struct MobView {
    #[from]
    x: f32,
    #[from]
    y: f32,
    #[from]
    hp: u8,
}

struct Swarm {
    load: Load,
    mobs: Vec<Mob>,
    hits: u32,
}

impl Zone for Swarm {
    type Config = Load;
    type Input = u8;
    type Timer = ();
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        schema![MobView]
    }

    fn entities() -> Entities<Self> {
        Entities::new().each::<MobView, [Mob]>(|z: &Swarm| {
            if z.load.replicate {
                &z.mobs[..]
            } else {
                &[]
            }
        })
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, load: Load) -> Self {
        let mut colliders = Colliders::new();
        for i in 0..16 {
            colliders.push(Collider::cylinder(
                [(i % 4) as f32 * 25.0 + 12.0, (i / 4) as f32 * 25.0 + 12.0],
                1.0,
                3.0,
            ));
        }
        ctx.set_colliders(colliders);
        let side = (load.n as f32).sqrt().ceil().max(1.0) as u32;
        let mobs = (0..load.n)
            .map(|i| Mob {
                id: EntityId(i + 1),
                x: (i % side) as f32 * 100.0 / side as f32,
                y: (i / side) as f32 * 100.0 / side as f32,
                hp: 100,
            })
            .collect();
        Self {
            load,
            mobs: if load.replicate || load.rewind {
                mobs
            } else {
                Vec::new()
            },
            hits: 0,
        }
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<u8> {
        payload.first().copied()
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        if self.load.moving {
            for mob in &mut self.mobs {
                mob.x = (mob.x + 0.5) % 100.0;
            }
        }
        let now = ctx.rewind().latest().map(|f| f.time());
        for q in 0..self.load.queries {
            let y = (q % 10) as f32 * 10.0 + 0.5;
            let ray = Ray::new([-1.0, y, 0.0], [1.0, 0.0, 0.0], 200.0);
            self.hits += u32::from(ctx.raycast(&ray).is_some());
            if let Some(time) = now {
                let shot = HitQuery::ray([-1.0, y, 0.0], [1.0, 0.0, 0.0], 200.0);
                let hit = ctx.rewind().hit_test(time, &shot, |id| id % 2 == 0);
                self.hits += u32::from(hit.is_some());
            }
        }
    }

    fn record_rewind(&self, frame: &mut RewindFrame) {
        if self.load.rewind {
            for mob in &self.mobs {
                frame.push(Historic::sphere(mob.id.get(), [mob.x, mob.y, 0.0], 0.5));
            }
        }
    }

    fn has_pending_work(&self) -> bool {
        true
    }
}

/// A host running `load` with `clients` players that see everything.
fn host(load: Load, clients: u32) -> (ZoneHost<Swarm>, ManualClock) {
    let clock = ManualClock::new();
    let config = HostConfig {
        tick_rate: 20,
        patch_rate: 20,
        byte_budget: usize::MAX,
        // Every client sees every entity.
        default_view_radius: 500.0,
        ..HostConfig::default()
    };
    let mut host = ZoneHost::<Swarm>::new("swarm", config, Arc::new(clock.clone()), load);
    for c in 0..clients {
        let claims = TicketClaims::new(
            format!("p{c}"),
            "swarm",
            u64::MAX,
            format!("n{c}"),
            Role::Player,
        );
        host.join(&claims, 0, "").expect("joins");
    }
    (host, clock)
}

/// Mean allocations per tick over `ticks` ticks after a warm-up (every
/// client knows every entity; buffers reached their size).
fn per_tick(load: Load, clients: u32, ticks: u32) -> u64 {
    let (mut host, clock) = host(load, clients);
    let step = Duration::from_millis(50);
    let mut tick = || {
        clock.advance(step);
        let report = host.advance();
        assert_eq!(report.ticks, 1, "one tick per advance");
        host.drain_outbound();
        drop(report);
    };
    for _ in 0..40 {
        tick();
    }
    let before = allocations();
    for _ in 0..ticks {
        tick();
    }
    (allocations() - before) / u64::from(ticks)
}

const SIZES: [u32; 3] = [50, 200, 1000];

#[test]
#[ignore = "report: --ignored --nocapture"]
fn alloc_report() {
    let clients = 8;
    let ticks = 20;
    let base = |n| Load {
        n,
        ..Load::default()
    };
    println!("allocations per tick ({clients} clients, {ticks} ticks after warm-up)");
    println!(
        "{:>6} {:>9} {:>11} {:>11} {:>8} {:>10} {:>11} {:>11}",
        "n", "baseline", "replicate", "repl+move", "rewind", "queries16", "all idle", "all moving"
    );
    for n in SIZES {
        let baseline = per_tick(base(n), clients, ticks);
        let site = |load: Load| per_tick(load, clients, ticks).saturating_sub(baseline);
        let replicate = site(Load {
            replicate: true,
            ..base(n)
        });
        let replicate_moving = site(Load {
            replicate: true,
            moving: true,
            ..base(n)
        });
        let rewind = site(Load {
            rewind: true,
            ..base(n)
        });
        let queries = site(Load {
            rewind: true,
            queries: 16,
            ..base(n)
        })
        .saturating_sub(rewind);
        let all = Load {
            n,
            replicate: true,
            rewind: true,
            queries: 16,
            moving: false,
        };
        let idle = per_tick(all, clients, ticks);
        let moving = per_tick(
            Load {
                moving: true,
                ..all
            },
            clients,
            ticks,
        );
        println!(
            "{n:>6} {baseline:>9} {replicate:>11} {replicate_moving:>11} {rewind:>8} {queries:>10} {idle:>11} {moving:>11}"
        );
    }
}

/// The hot loop of an idle world (replication to 8 clients, rewind
/// recording, 16 raycasts and 16 rewind hit tests per tick) allocates the
/// same, small number of times per tick at any entity count; a moving one
/// adds a bounded number per changed entity (its new payload and update
/// record), nothing per unchanged one.
#[test]
fn hot_loop_allocates_only_on_change() {
    let load = |n, moving| Load {
        n,
        replicate: true,
        rewind: true,
        queries: 16,
        moving,
    };
    let idle: Vec<u64> = SIZES.iter().map(|&n| per_tick(load(n, false), 8, 10)).collect();
    assert!(
        idle.iter().all(|&a| a == idle[0]),
        "idle allocations per tick grow with the entity count: {idle:?} for {SIZES:?}"
    );
    assert!(idle[0] <= 24, "idle allocations per tick: {}", idle[0]);
    for (&n, &idle) in SIZES.iter().zip(&idle) {
        let moving = per_tick(load(n, true), 8, 10);
        assert!(
            moving - idle <= 5 * u64::from(n),
            "{n} moving entities: {moving} allocations per tick, idle {idle}"
        );
    }
}
