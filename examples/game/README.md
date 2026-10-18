# distributed_game demos

Small playable demos of the authoritative game runtime
([`distributed_game`](../../distributed_game/README.md)) and its browser SDK
([`@hops-ops/distributed/game`](../../js/README.md#game-zones)), each focused
on one mechanic. **Dev only**: the demo server mints tickets for anonymous
guests and lets any page change the network simulation.

| Demo | Run | Mechanic it demonstrates |
|---|---|---|
| **Arena** | `./run.sh arena` | Fixed 30 Hz tick / 20 Hz patches, binary deltas, interest management (view ring, drones pop in/out), client prediction + reconciliation (shared Rust/TS movement step, bit-exact), interpolation of remote entities, live latency/jitter/loss |
| **Shooting Gallery** | `./run.sh gallery` | Lag compensation: hitscan shots resolved with `ctx.rewind().hit_test_shot` at the shooter's view time (`client_tick` + interpolation delay in the payload), `max_rewind` clamp, per-shot compensation on/off, predicted vs confirmed hits |
| **The Freeze** | `./run.sh freeze` | World-version barrier (`WorldCoordinator::rollout`) across **two server processes** as an in-world "admin update": freeze → apply → resume with change notes, inputs dropped while frozen, Hard reload vs safe-room gating (`StalePolicy::SafeRoom`, `Zone::is_gate`) of old clients, aborted rollout (`freeze_ready` → `NotAcked`), facts/events stamped with the content version |

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

Shooting Gallery controls: click a target (or Space / gamepad A through the
crosshair), drag to look. **C** toggles lag compensation (sent with every
shot), **G** wireframe ghosts at the newest received target state. Set the
NetSim latency to 150–250 ms: with compensation the confirmed hits match the
predicted ones (yellow marker → green); without it most turn red. The
client's interpolation delay adapts to `rtt / 2 + 2 patches + 2 × jitter`
(the SDK clock estimates the server's *present*, so a fixed 100 ms would ask
for states not yet received); each shot carries `delay = tickTime(client_tick)
- renderTime`, so the server rewinds to exactly what was on screen. Beyond
`gallery::MAX_REWIND_MS` (600 ms of view age, ≈ 250 ms one way) shots are
clamped and reported as such.

The Freeze: the server prints an **admin secret** (`./run.sh freeze` shows
it; set `DG_DEMO_ADMIN_SECRET` to choose one) — open
`/freeze/?admin=<secret>` (add `&old=1` in a second tab for an old client).
`game-demos freeze` hosts the Town Hub (safe-room gate) and Whisper Meadow
and starts a second process of itself for Ember Fields (`beta`, shared
ticket + admin secrets, exits with its parent); portals hand players off
between them (`Redirect` + `/admin/transfer` across processes). Content is
data-driven per version (`freeze::content`: v1 lava pools that burn, v2
"lava is now ice", v3 back to lava; slime speed/size/color, crystals, gem
value) and served by `GET /dev/freeze/content/{version}`; the client derives
the scene from *its own* pack.

- **Roll out update (Hard)**: every zone on both hosts freezes (SYSTEM
  overlay, inputs dropped and counted — client side, plus any in flight on
  the server), the "content CDN" stages the pack (`DG_FREEZE_STAGE_MS`,
  default 2.5 s), every zone resumes on the next version with the notes,
  and every client reloads (`Kick(RELOAD)` → `reloadContent` → reconnect
  with the token).
- **Roll out (Safe room)**: rolling commit. Normal clients patch live
  (`client.setContentVersion`); an old client (**O** / `&old=1`) keeps its
  old pack in the field — the server already runs the new rules — until it
  walks through a portal into the Town Hub, where it gets `RELOAD`.
  (An old client that is *already* in the hub when the update lands is
  refused by the field it walks into next — `VERSION_MISMATCH` — returns to
  the hub with its transfer ticket and is reloaded there.)
- **Ritual in Ember Fields**: that zone's `freeze_ready()` is false; the
  next rollout times out (`NotAcked: beta/freeze-ember`) and every zone
  resumes on the old version with "update aborted".

Each zone switch emits a `freeze.content_applied` fact and a stamped event;
gems emit `freeze.gem_collected` facts — the admin panel lists them with
their stamp. Display names live on the primary host: a player joining Ember
Fields directly (`&zone=freeze-ember`) shows as a guest there, names carry
over portals.

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
    src/arena/               Arena zone + movement.rs (twin of client movement.ts)
    src/gallery/             Shooting Gallery zone (targets, rewind hit tests); tests/gallery_smoke.rs
    src/freeze/              The Freeze zone, content packs, two-host world + admin routes; tests/freeze_smoke.rs
    tests/                   arena_smoke.rs (bots, dev routes), movement_fixture.rs
  client/                    Vite + TypeScript + Babylon.js 9.30 (multi-page)
    index.html, <demo>/index.html
    src/shared/              hud.ts, input.ts, scene.ts, dev-api.ts, events.ts, style.css
    src/arena/               client.ts (headless), main.ts (Babylon), movement.ts
    src/gallery/             client.ts (shots, adaptive delay), main.ts; tests/gallery-headless
    src/freeze/              client.ts (content packs, live patch / reload, admin API), main.ts; tests/freeze-headless
    tests/                   movement-parity, arena-headless (+ helpers/demo-server.mjs)
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
