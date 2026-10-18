# distributed_game demos

Small playable demos of the authoritative game runtime
([`distributed_game`](../../distributed_game/README.md)) and its browser SDK
([`@hops-ops/distributed/game`](../../js/README.md#game-zones)), each focused
on one mechanic. **Dev only**: the demo server mints tickets for anonymous
guests and lets any page change the network simulation.

| Demo | Run | Mechanic it demonstrates |
|---|---|---|
| **Arena** | `./run.sh arena` | Fixed 30 Hz tick / 20 Hz patches, binary deltas, interest management (view ring, drones pop in/out), client prediction + reconciliation (shared Rust/TS movement step, bit-exact), interpolation of remote entities, live latency/jitter/loss |
| **Endless Grid** | `./run.sh grid` | Seeded procedural world (terrain/biomes/props generated bit-identically in Rust and TS, nothing static replicated), 32 m cells as zones, observer connections to the 8 neighbours, handoff across borders **and hosts** (2 processes, hash ring, Redirect + transfer tickets), hibernation to SQLite and restore |
| **Boss Ripple** | `./run.sh ripple` | Zone hierarchy realm › city › borough › hood (`DottedTopology`), per-tier tick rates (2/5/10/30 Hz), boss defeats propagating up as tier-up events (each hop on another host), parent broadcasts reaching players through observer connections |

`./run.sh smoke` runs every demo's smoke tests; `./run.sh list` lists demos.

Requirements: Rust (stable), Node ≥ 22.18 (runs `.ts` test imports natively).

## Run

```sh
examples/game/run.sh arena
# → open http://localhost:5173/arena/   (?name=Ana&zone=arena-2 optional)
```

The script builds the JS SDK if `js/dist` is stale, runs `npm ci` for the
client, builds `game-demos`, starts it on `127.0.0.1:2567` with `--dev`, then
runs Vite (which proxies `/zones` (WebSocket), `/dev` and `/metrics` to the
server). Ports: `GAME_DEMOS_PORT`, `GAME_DEMOS_CLIENT_PORT`. Open a second tab
to see another player interpolated. Server metrics: `/metrics`.

Arena controls: WASD / arrows / left stick to move (camera-relative, drag to
orbit), Space / Shift / gamepad A to dash. **P** toggles client prediction
(off = your capsule is drawn like everyone else's, one RTT + 100 ms behind
your keys), **G** the wireframe ghost at the latest server position. The
NetSim sliders change the server's simulated one-way latency (applied in
both directions), jitter and loss (a TCP-like retransmit delay) for every
connection.

## Endless Grid

```sh
examples/game/run.sh grid
# → http://localhost:5173/grid/   (?zone=grid.4.-2&name=Ana optional)
```

Two `game-demos grid` processes: host **A** on `GAME_DEMOS_PORT` (Vite
proxies to it) and **B** on the next port, each listing the other with
`--peer`, sharing one ticket secret (the admin secret for host-to-host calls
is derived from it) and keeping hibernation snapshots under one
`--state-dir` (`grid-A.sqlite3`, `grid-B.sqlite3`).

- **World.** Cell `(x, y)` is zone `grid.<x>.<y>` (canonical integers,
  |x|,|y| ≤ 10⁶; the server uses `FlatTopology`, the dots are not a
  hierarchy). Its terrain height, biome and props are a pure function of
  `(seed, x, y, CONTENT_VERSION)` in `server/src/grid/world.rs` and its twin
  `client/src/grid/world.ts` (u32 hashing + binary32 value noise; checked
  bit-for-bit by `fixtures/grid-world.json`). Props sit inside 8 m
  sub-cells 2 m from their edges, so lines every 8 m are free trails and no
  prop reaches across a border. Seed: `DG_GRID_SEED`.
- **Zones.** A cell simulates walkers (predicted with the shared step,
  colliding with the cell's props) and 2–4 critters. Past a border by
  0.5 m the walker is handed off with its position in the neighbour's
  frame (`ctx.handoff`); the SDK follows the Handoff — through a Redirect
  when the neighbour lives on the other host (`HashRing`, 64 vnodes).
- **Interest across borders.** The client observes the 8 neighbours and
  re-syncs on every authority change; entities are drawn at
  `cell × 32 + local` around a floating origin.
- **Hibernation.** Nobody inside or observing → Idle → Dormant after 2 s →
  hibernated after 4 s more (`--hibernate-after-ms`): critters and markers
  (E) go to SQLite and come back on the next activation; its hello says
  `restored` and the page toasts. The minimap (`GET /dev/grid/map`, which
  asks the peer for its cells) shows active / hibernated / cold cells per
  host; borders are tinted by owning host.

Controls: WASD / stick, Space/Shift sprint, E drops a marker.

## Boss Ripple

```sh
examples/game/run.sh ripple
# → http://localhost:5173/ripple/   (?hood=realm.city.borough-2.hood-3 optional)
```

`realm` › `realm.city` › `realm.city.borough-{1,2}` ›
`….hood-{1,2,3}`. Placement alternates by depth over the two hosts (hoods
and the city on A, boroughs and the realm on B), so every hop is a
`/admin/child-event` call between processes. Tick rates per tier come from
`GameServer::with_host_config`: realm 2 Hz (keeps an idle background
clock), city 5, borough 10, hood 30. Upper tiers refuse players; they are
observed.

1. A hood's boss falls (E near it, or "Smite") → `boss_defeated` to the root.
2. The borough tallies its hoods; all down → `borough_cleared` (unlocked).
3. The city's weather follows the remaining bosses (storm → rain →
   clearing → clear) and is broadcast to its observers — every player
   observes the city, so the sky over their hood changes; both boroughs
   cleared → `city_liberated`.
4. The realm counts everything and broadcasts a banner.

Bosses respawn after 60 s (`boss_respawned` re-locks the borough). Every
tier's state is its hibernation snapshot. The page shows the tree with
each tier's host, rate and clock, animates each hop, and lets you click a
hood to fight there.

## Architecture

```
 browser (Vite dev server :5173)                        game-demos <demo> --dev (:2567)
 ┌──────────────────────────────────┐                  ┌─────────────────────────────────────┐
 │ src/<demo>/main.ts  Babylon scene │                  │ common::run / spawn                 │
 │   └ src/<demo>/client.ts          │  POST /dev/ticket │   ├ dev::routes  /dev/ticket        │
 │       GameClient (SDK)            │ ───────────────► │   │   guest principal (server-made) │
 │       ├ GameConnection  ws ───────┼─ /zones/{zone} ─►│   │   /dev/netsim → set_net_sim     │
 │       ├ EntityReplica (interp.)   │ ◄─ Welcome/Delta │   └ GameServer::serve_with          │
 │       └ Predictor ── movement.ts ═╪═ parity fixture ═╪══► ZoneHost<Arena> 30 Hz           │
 │ src/shared/ hud · input · scene   │                  │        └ arena::movement (twin)     │
 │            dev-api · events       │  GET /metrics    │   GET /metrics (Prometheus)         │
 └──────────────────────────────────┘                  └─────────────────────────────────────┘
```

Prediction is exact because (1) every input is one command frame of exactly
one tick, consumed one per tick with a small credit bank
(`arena::MAX_CREDITS`), (2) the step uses binary32 arithmetic only
(`Math.fround` in TS) without trig, and (3) the server snaps its state to the
wire quantization after each step, so the client rebases on exactly what the
server holds. `fixtures/arena-movement.json` (written by Rust) is replayed by
the TS step in `client/tests/movement-parity.test.mjs`.

## Layout

```
examples/game/
  run.sh                     one-command dev + smoke runner
  fixtures/                  cross-language golden data (Rust writes, TS checks)
  server/                    Rust crate `game_demos`, its own cargo workspace
    src/main.rs              `game-demos <demo>` subcommands
    src/common/              shared: Demo trait, run/spawn, dev routes, guests, JSON events
    src/cluster.rs           multi-host demos: --host-id/--peer, directory, ownership, snapshots
    src/arena/               Arena zone + movement.rs (twin of client movement.ts)
    src/grid/                Endless Grid zone, world.rs (twin of world.ts), dev map routes
    src/ripple/              Boss Ripple tiers and placement
    tests/                   arena_smoke.rs, movement_fixture.rs, grid_smoke.rs,
                             grid_fixture.rs, ripple_smoke.rs (+ support/ for two hosts)
  client/                    Vite + TypeScript + Babylon.js 9.30 (multi-page)
    index.html, <demo>/index.html
    src/shared/              hud.ts, input.ts, scene.ts, dev-api.ts, events.ts, style.css
    src/arena/               client.ts (headless), main.ts (Babylon), movement.ts
    src/grid/, src/ripple/   client.ts (headless), main.ts (Babylon); grid/world.ts
    tests/                   movement-parity, arena-headless, grid-world-parity,
                             grid-headless, ripple-headless (+ helpers/demo-server.mjs,
                             helpers/cluster.mjs for multi-host demos)
```

The server crate is excluded from the root workspace (`Cargo.toml`
`exclude`), depends on `../../../distributed_game` by path and keeps its own
`Cargo.lock`; the client depends on the SDK via `file:../../../js` (the built
`dist`).

## Adding a demo

1. **Server** — `server/src/<demo>/mod.rs`: the zone plus a unit struct
   implementing `common::Demo` (`NAME`, `DEFAULT_ZONE`, `hosts_zone`,
   `server(config, guests)`); add `pub mod <demo>;` to `lib.rs` and one
   variant to `DemoCommand` in `main.rs`. You get `/dev/ticket`,
   `/dev/netsim`, `/metrics`, `--port/--dev/--secret`, graceful shutdown.
   - Display names: `guests.name(&ctx.player(p)?.principal)` in `on_join`.
   - Low-rate events: `common::send_json_event` / `broadcast_json_event`.
   - Signed quantities that must rest at zero: `FieldType::quant_centered`.
2. **Smoke test** — `server/tests/<demo>_smoke.rs`: `common::spawn::<YourDemo>`
   on `127.0.0.1:0` with `DemoOptions::dev(secret)` and
   `distributed_game::bots::run_bots` (custom `encoder` for your input).
3. **Client** — `client/<demo>/index.html` + `src/<demo>/main.ts`, add the
   name to `DEMOS` in `vite.config.ts` and a link in `index.html`. Keep
   networking in `src/<demo>/client.ts` (no Babylon) so Node can test it.
   Reuse `DevApi` + `zoneUrl` (tickets), `Hud` (status/RTT/jitter/tick/
   delta B/s/entities + NetSim sliders + `row`/`toggle`), `InputSampler`,
   `FixedRate`, `cameraRelative`, `toAxis`, `createStage`/`toWorld`/
   `material`/`createRing`/`LabelLayer`, `onJsonEvent`.
4. **Headless test** — `client/tests/<demo>-headless.test.mjs` with
   `startDemo('<demo>')` from `tests/helpers/demo-server.mjs`.
5. Register the demo in `run.sh` (`DEMOS=(…)`) and in the table above.

A **multi-host** demo also implements `cluster::ClusterDemo` (`directory`
over the hosts, `cluster_server`, optional dev routes), uses
`DemoCommand::X(ClusterArgs)` → `run_cluster::<X>`, is listed in
`CLUSTER_DEMOS` in `run.sh`, and tests with `tests/support` (Rust) /
`startCluster` (Node).

## Security notes (why this is dev only)

- `--dev` (or `DG_DEMO_DEV=1`) mounts `/dev/*`; without it only `/zones` and
  `/metrics` exist. `run.sh` binds `127.0.0.1` and always sets it.
- `/dev/ticket` never accepts an identity: the principal is generated by the
  server (`guest-<hex>`); the caller may pass a display name (sanitized,
  ≤ 16 chars) and a server-signed guest token from an earlier response to
  stay the same guest. Unknown fields (e.g. `principal`) are rejected.
- `/dev/netsim` changes the simulation for every connection on the server.
- The ticket secret is random per process unless `DISTRIBUTED_GAME_SECRET`
  is set (needed to point `game-bots` at a running demo).
