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
| `net` | `net::GameServer` (axum WebSocket), HMAC tickets | `axum`, `hmac`, `sha2`, `base64`, `tokio` |

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

- Hibernation / directory: `Tier::Dormant`, `ZoneHost::dispose`, zone
  factory in `GameServer`.
- Handoff saga: `ZoneHost::handoff` + `LeaveReason::Handoff` + Handoff message.
- Content barrier: `ZoneHost::freeze` / `resume`.
- Metrics: `AdvanceReport`, `replication::DeltaStats`, `PushOutcome`.
- Rewind/lag compensation: `SequencedInput::client_tick`, deterministic
  `ZoneRng` and `ManualClock`.
