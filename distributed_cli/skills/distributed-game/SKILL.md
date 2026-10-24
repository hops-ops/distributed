---
name: distributed-game
description: Build authoritative game zones with distributed_game - a plain Rust Zone for the simulation, replicated entities with interest management, durable facts settled into cells, and zones that watch world cells as pushed read models. Use when writing or scaling a game zone, its facts, its replication, or its world cells (native host or celld).
---

# Game zones with distributed_game

A zone is a plain Rust struct that owns one authoritative simulation
(movement, physics, combat ticks). The framework hosts it, replicates it,
settles its outcomes and keeps it informed about the world. Game code holds
no storage, transport, retry or celld code. `distributed_game/README.md` is
the full reference; this skill is the shortest path to the common cases.

## The three rules

1. **Ticks are never stored.** Only meaningful outcomes leave the zone, as
   *facts* (`ctx.emit`), and the tick never waits for them.
2. **Identity is bound by the host.** A fact's principal comes from the
   player's verified ticket. Inputs never carry owner identity, XP amounts
   or rewards chosen by a client.
3. **Read models are for display.** Watched world state flavours the zone
   (weather, a boss tier, an event); rewards still go through the fact →
   command path and are validated by the cell's handler.

## Facts: outcomes into cells

```rust
#[derive(Serialize, Deserialize, FactCommand)]
#[fact(command = RECORD_KILL)]           // the same constant the cell mounts
pub struct RecordKillInput { pub mob: u32 }

// in a zone hook: never blocks, returns the key
ctx.emit(player, RecordKillInput { mob: id })?;   // principal bound from the ticket
ctx.emit_zone(RoomAwakeInput { .. })?;            // zone/system fact, no principal
```

Route facts once, where the host is built (celld: `CelldZone::fact_routes`):

```rust
sink.route(FactRoute::by_principal::<RecordKillInput>("crawler"))     // crawler/{principal}
    .route(FactRoute::by_input_field::<RoomAwakeInput>("borough", "borough"))
```

You get:
- **Exactly once.** The key (zone + boot epoch + parts) becomes the command
  id, and the cell's ledger turns a retry into a replay.
- **Order per principal.** Facts of one player commit in emission order;
  different players overlap (`max_concurrent_commits`).
- **Backpressure, not loss.** Past `capacity` unsettled facts, `emit`
  returns `Backpressure` and the game decides what to do.
- **Batches by default on celld.** A lane's waiting facts for one cell go
  as one `POST …/__batch` (`ZONE_FACT_BATCH`, default 16), and the zone
  outbox clears them with one delete. A batch stops at its first retryable
  failure and resumes from there, so order and exactly-once hold. A custom
  sink opts in by overriding `FactSink::max_batch` and `commit_batch`, or
  `FactHost::max_fact_batch` and `dispatch_fact_batch`. Otherwise it gets
  one fact at a time.

Results come back on a later tick in `on_fact_settled`.

## Replication: what each client sees

```rust
// one #[replicated] per view struct (Mob Pit: examples/game/server/rooms/src/zone.rs)
#[replicated(kind = "mob", id = 2, of = Mob)]               // interest-managed
#[replicated(kind = "pillar", id = 3, global, of = Pillar)] // everyone, always
#[replicated(kind = "hero", id = 4, every_patch, of = Hero)] // never rate-thinned
```

- Clients see entities within their view radius, under a per-Delta byte
  budget (4 KiB) ranked by priority. Deferred entities gain priority until
  they are sent.
- Far entities update less often (`HostConfig::interest.update_rates`):
  - within 0.5 of the radius: every patch;
  - up to 0.8: every 2nd patch;
  - beyond that: every 4th.
  
  Creates and removes are never held. `UpdateRates::every_patch()` turns
  the rates off.
- A client watching the zone next door tells it where the player is
  (`watchZones(...).setFocus(at)`, zone-local coordinates). The
  neighbour's `Zone::on_observer_focus` then centres that observer's view
  on the player, so interest continues across the border.
- Each changed entity is encoded once per patch and shared by every client
  that was up to date. A dense room costs selection plus copying, not one
  encode per client.
- The wire format and the JS SDK's prediction are unchanged by these
  knobs.

## The world around: watched cells (CQRS)

World cells (hood, borough, city, content rollout, dev settings) are the
write model. Each zone keeps a pushed read model of the cells it declares,
and each view decodes into a plain struct that becomes a normal zone
message:

```rust
impl CelldZone for MobPit {
    fn watch(zone: &str) -> Watches<PitMessage> {
        Watches::new()
            .cell(HOOD_KIND, &hood_of(zone), |read: HoodRead| read.state.map(PitMessage::hood))
            .cell(CITY_KIND, CITY_SHARD, |read: CityRead| read.state.map(PitMessage::city))
    }
}
```

A cell opts in with one field and two calls:

```rust
pub struct HoodCell { cell: AggregateCell<Hood>, env: Env, watchers: CellWatchers }
// fetch: self.watchers.serve(&self.env, &self.cell, req, read_hood, sessions).await
// alarm: self.watchers.alarm(&self.env, &self.cell, read_hood).await
```

How it behaves:
- **Reads.** A zone reads each watched cell on wake. The read leaves an
  in-memory lease on the cell; nothing is written.
- **Pushes.** The cell pushes `{topic, version, view}` after a commit or
  alarm that changes the view. A heartbeat that only bumps the version
  pushes nothing.
- **Ordering.** The zone applies only newer versions.
- **Repair.** The zone re-reads every 45 ± 15 s, which renews its lease
  and repairs a lost push.
- **Sleeping zones.** A sleeping zone is never woken by a push.
- **Scale.** Cost grows with view changes × watchers, not with zones ×
  seconds.
- **Other reads.** `refresh`/`refresh_every` remain for async reads that
  are not cell views.

## Checking your work

- `cargo test -p distributed_game --all-features` runs the native host, the
  facts, replication and interest management with in-memory sinks.
- `mobpit-load` (in `examples/game/server/examples`) measures:
  - `--steps 32,64` for one room;
  - `--steps 64x1,128x1` for many rooms;
  - `--no-attack` for movement only;
  - `--var KEY=VALUE` for a worker var on every run.
- Compare work, not wall clock. Each step prints a `work` line from the
  rooms' `WorkCounters` (`GET /zones/{zone}/info` → `work`) and the
  watched cells' `__watchers` counts: records encoded and sent per patch,
  updates held by rates, bytes per frame, fact commands, cell requests and
  outbox deletes per room-second, world reads and pushes per room-minute.
  Counts do not depend on machine load; timings do.
- A/B one switch with interleaved runs of the same build:
  `--ab VAR=VAL --pairs 3` runs A,B then B,A … (fresh celld each) and
  prints the median of each side and of the per-pair change. Switches:
  `ZONE_FACT_BATCH=1` (fact batches off), `ZONE_UPDATE_RATES=0` (update
  rates off), `ZONE_WORLD_PUSH=0` (watched cells polled every second
  instead of pushed). Read timings only as these interleaved medians.
