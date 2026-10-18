# distributed_game

Authoritative multiplayer zone runtime for the `distributed` CQRS/ES
framework. A game implements [`Zone`]; [`ZoneHost`] runs it with a fixed
timestep, activity tiers, sequenced inputs, timers, seeded RNG,
non-blocking durable facts and interest-managed binary replication. The
optional `net` feature serves many zones over WebSockets.

Physics is out of scope: bring your own and step it in `on_tick`.

## Features

| Feature | Adds | Deps |
|---|---|---|
| *(default)* | Zone/host runtime, facts queue, protocol, schema, replication | `serde_json` |
| `runtime` | `spawn_fact_worker` on Tokio | `tokio` (rt, time, sync) |
| `cell` | `cell::CellFactSink` → `AggregateCell::dispatch_idempotent`, `stamp_event_record` | `distributed`, `sha2` |
| `metrics` | `metrics::GameMetrics` per-zone Prometheus text | – |
| `net` | `net::GameServer` (axum WebSocket), HMAC tickets, `GET /metrics` (implies `metrics`), handoff, admin peer routes, `WorldCoordinator` | `axum`, `hmac`, `sha2`, `base64`, `tokio` |
| `bots` | `bots` load-test client library + `game-bots` binary | `net` + `tokio-tungstenite` |
| `lease` | `world::FencedLeaseOwnership` over `distributed::lease` | `distributed` |
| `sqlite` | `world::SqliteZoneStateStore` (hibernation snapshots) | `rusqlite` (bundled) |

## Minimal zone

```rust
use std::sync::Arc;
use std::time::Duration;
use distributed_game::*;

struct Room { x: f32 }

impl Zone for Room {
    type Config = ();
    type Input = i8;          // -1, 0, 1
    type Timer = ();

    fn schema() -> EntitySchema {
        EntitySchema::new().kind(
            KindDef::new(1, "dot")
                .field("x", FieldType::QuantF32 { min: -50.0, max: 50.0, bits: 12 }),
        )
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _: ()) -> Self { Room { x: 0.0 } }

    fn decode_input(&self, _p: PlayerId, bytes: &[u8]) -> Option<i8> {
        bytes.first().map(|b| *b as i8)
    }

    fn sanitize_input(&self, _p: PlayerId, dx: i8) -> Option<i8> {
        (-1..=1).contains(&dx).then_some(dx)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let players: Vec<_> = ctx.players().map(|p| p.id).collect();
        for p in players {
            for input in ctx.take_inputs(p, ConsumePolicy::Drain) {
                self.x += f32::from(input.input) * 5.0 * ctx.dt_secs();
            }
        }
    }

    fn replicate(&self, out: &mut WorldView) {
        out.push(EntityState::new(1, 1, self.x, 0.0, vec![FieldValue::F(self.x)]));
    }
}

let clock = ManualClock::new();
let mut host = ZoneHost::<Room>::new("room-1", HostConfig::default(), Arc::new(clock.clone()), ());
let claims = TicketClaims {
    principal: "alice".into(), zone: "room-1".into(),
    expires_at: u64::MAX, nonce: "n".into(), role: Role::Player,
};
let joined = host.join(&claims, 0, "").unwrap();
host.push_input(joined.client, 1, 0, &[1]);
clock.advance(Duration::from_millis(100));
host.advance();
for out in host.drain_outbound() { /* send Outbound::Frame bytes to the client */ }
```

Serving it (`net`):

```rust,ignore
let server = GameServer::<Room>::new(ServerConfig::new(TicketKey::new(secret)), |zone_id| {
    zone_id.starts_with("room-").then_some(())
});
server.serve(tokio::net::TcpListener::bind("0.0.0.0:2567").await?, shutdown_signal).await?;
// or merge app routes (lobby, health, dev tools) with the same graceful shutdown:
// server.serve_with(listener, axum::Router::new().route("/healthz", get(ok)), signal).await?;
```

## Durable facts

Zones never await storage. Emit a fact during a tick:

```rust,ignore
ctx.emit_fact_for(player, Fact::new(format!("kill:{zone}:{tick}:{enemy}"), "hero.credit_xp", json!({"amount": 25})))?;
```

The host binds the principal from the player's **verified ticket** and
stamps zone/tick/content version. A `FactSink` commits facts on a worker
with bounded retries/backoff; `Zone::on_fact_settled` receives the outcome
on a later tick. `cell::CellFactSink` dispatches into an `AggregateCell`
with a deterministic command id per fact key, so retries replay instead of
double-crediting.

## Netcode and operations

- **Lag compensation** (`rewind`): implement `Zone::record_rewind` to push
  hit volumes (sphere / vertical capsule + numeric fields) each tick; the
  host keeps a ring buffer sized by `HostConfig::rewind.max_rewind`
  (default 500 ms). In a hook,
  `ctx.rewind().hit_test_shot(ShotTiming { client_tick, interp_delay }, &HitQuery::ray(o, d, range), |id| id != me)`
  evaluates at the shooter's view time, clamped to `max_rewind` and the
  stored history. The interpolation delay travels in the game's input
  payload (no wire change).
- **Record/replay** (`replay`): drive a host through `Recorder` (same
  methods as `ZoneHost`), save the compact `Recording` (`DGR1`), and
  `replay::<Z>(&recording, config)` re-runs it on a `ManualClock`,
  checking a per-tick state hash (`Zone::state_hash`, or the replicated
  `WorldView`). A mismatch returns a `Divergence` naming the tick and the
  stimuli applied since the last matching tick. Fact settlements are
  replayed on the tick they were delivered. World entry points (`join_transfer`,
  `reconnect_with_version`, `set_content_version`, `request_freeze` /
  `cancel_freeze`, `reload_stale_players`, `deliver_child_event`,
  `restore`) are recorded too; zone-issued handoffs replay against a stub
  issuer that always signs.
- **Metrics** (`metrics`, served by `GameServer` at `GET /metrics`):
  tick duration histogram, ticks/dropped ticks, tier, players, observers,
  entities, inputs by outcome (incl. `rate_limited` and `frozen` barrier drops),
  Delta bytes per frame, facts pending/settled.
  Only the first `ServerConfig::metrics_zone_labels` (default 64) live
  zones get a `zone` label; the rest aggregate into `zone="_other"`.
  `AdvanceReport` carries the raw numbers for custom drivers.
- **Bots** (`bots`): `run_bots(BotConfig, |i| behaviour)` connects N
  WebSocket clients with signed tickets, sends inputs (random walk,
  circle, or your own `Behaviour`), measures RTT and Delta bytes/s:
  `cargo run -p distributed_game --features bots --bin game-bots -- --url ws://127.0.0.1:2567 --zone room-1 --bots 50`.
- **Network simulation** (`NetSim`, dev only): one-way latency + uniform
  jitter + loss, per direction. WebSockets are reliable and ordered, so
  loss is simulated as a TCP retransmission delay (`retransmit`, default
  200 ms, up to 3×) with head-of-line blocking, never as a dropped or
  reordered message. `config.net_sim = NetSim::from_env()` reads
  `DG_NET_LATENCY_MS`, `DG_NET_JITTER_MS`, `DG_NET_LOSS`,
  `DG_NET_RETRANSMIT_MS`. `GameServer::set_net_sim(Some(sim) | None)`
  changes it at runtime for every connection (order is preserved).

## World structure

`world` (no feature needed for the traits and in-memory impls):

| Piece | Purpose |
|---|---|
| `ZoneDirectory` (`StaticDirectory`, `HashRing`) | zone → owning `HostAddr`. `HashRing` uses virtual nodes; adding/removing one of N hosts moves ~1/N of the zones. A Hello for a zone owned elsewhere gets `Redirect(zone, url)`. |
| `ZoneOwnership` (`LocalOwnership`, `FencedLeaseOwnership<S: LeaseStore>`) | every activation acquires a fenced claim (`host#activation`) and renews it; a failed renewal disposes the zone **without** saving. Two hosts never run the same zone. |
| `ZoneStateStore` (`MemoryZoneStateStore`, `SqliteZoneStateStore`) | hibernation snapshots, written under the claim's fencing token (stale writers get `StateError::Stale`). |
| `ZoneTopology` (`DottedTopology` default: `eldoria.port.docks` → `eldoria.port` → `eldoria`) | parent links for tier-up events. |

**Activate on demand / hibernate.** `GameServer` creates a zone on first
join (or tier-up event). With `ServerConfig::hibernate_after = Some(d)` a
zone that stays Dormant for `d` (no clients, no in-flight facts, not
frozen) is hibernated: unregistered atomically (later joins start a new
activation, which waits for this one's claim), `Zone::hibernate()` is
stored, `on_dispose` runs, the claim is released. The next activation
calls `Zone::on_restore`.

*Facts on dispose:* a zone only becomes Dormant with zero unsettled
facts, so hibernation never strands a settlement. Facts emitted during
disposal (e.g. in `on_dispose`) are still committed: the fact worker
drains its queue after the zone drops it, and the zone task waits up to
`settle_timeout` for it. We chose "settle, then dispose" over a durable
fact outbox because the zone-side queue is already bounded and
idempotent; a crash-durable outbox belongs to the sink (e.g. the cell
adapter commits the fact atomically with its own outbox row).

**Handoff.** In a hook: `ctx.handoff(player, "realm.west", carried_bytes)?`.
The host signs a transfer ticket (principal, source, target, issuer host,
ownership generation, carried state, nonce, 30 s expiry), removes the
player (`LeaveReason::Handoff`) and sends `Handoff(target, ticket)`. The
target reserves the ticket with the issuing host (locally or via
`/admin/transfer`), admits it with `Zone::on_handoff_join(player, from,
carried)` and confirms; a refused transfer is released and the client may
present the same ticket back to the source (PROTOCOL.md §8.1). The
realtime path never waits on storage. When the player's **durable**
record must move too (home zone, inventory cell), emit a fact from the
handoff hook and let a `distributed::saga` drive it, off the tick:

```rust,ignore
// zone hook: non-blocking
ctx.emit_fact_for(player, Fact::new(format!("move:{nonce}"), "player.relocate",
    json!({"from": from, "to": to})))?;
// fact sink → SagaRunner::<_, Relocate>::start(nonce, data):
//   step "detach" → source cell (compensate: "reattach")
//   step "attach" → target cell (compensate: "detach_again")
// timeouts compensate; the game keeps playing on carried state meanwhile.
```

**Tier-up events.** `ctx.emit_upward("boss_defeated", payload,
ChildEvent::TO_ROOT)`; the server routes it to the parent (activating it,
on its owning host via `/admin/child-event`), whose host delivers
`Zone::on_child_event` on a later tick and forwards it further up while
`levels` remain.

**Content barrier.** `net::WorldCoordinator::rollout(version, notes, mode,
apply)` over `GameServer`s and `RemoteHost`s (admin HTTP, shared secret):
freeze all zones (`Freeze{version}`; `Zone::freeze_ready` may delay),
wait for every ack within the timeout, `apply(version)`, then commit
(`Resume{version, notes}`; facts/`ctx.stamp()` carry the new version).
A missing ack or failed `apply` aborts: everyone resumes on the old
version. Inputs while frozen are dropped and counted
(`ZoneHost::inputs_dropped_frozen`). `HostConfig::stale_policy =
StalePolicy::SafeRoom { roam }` lets old clients keep playing after a
`BarrierMode::Rolling` commit until they reach a gate zone
(`Zone::is_gate`), where they get `Kick(RELOAD)` and reconnect with their
token after reloading; `BarrierMode::Hard` reloads every stale client at
once.

Admin routes (enabled by `ServerConfig::admin_secret`, header
`x-game-admin-secret`): `POST /admin/transfer`, `/admin/child-event`,
`/admin/barrier/{prepare,commit,abort}` — see `net::world`.

## Protocol

See [`PROTOCOL.md`](PROTOCOL.md) — binary, little-endian, varints, one
message per WebSocket frame, schema shipped in Welcome so generic clients
need no codegen. Golden fixtures for other implementations are in
`tests/fixtures/`.

## Tests

```sh
cargo test -p distributed_game --all-features
UPDATE_GOLDEN=1 cargo test -p distributed_game --test golden   # accept wire changes
```

## Extension seams (later phases)

- Metrics: `AdvanceReport`, `replication::DeltaStats`, `PushOutcome`.
- Rewind/lag compensation: `SequencedInput::client_tick`, deterministic
  `ZoneRng` and `ManualClock`.
