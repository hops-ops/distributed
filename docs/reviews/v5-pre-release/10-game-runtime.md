## Assessment — realtime game runtime slice (`distributed_game`, `examples/game`, `tests/celld`, `tests/bomberman`, two docs)

The simulation core is in good shape: `ZoneHost` is synchronous and transport-agnostic, keeps players/clients/observers in `BTreeMap`s, uses a seeded RNG and sim-time timers, and has bounds-checked wire decoders. Durable state goes through idempotent commands into aggregates (facts, `ZoneLease`, `ZoneState`), so the unidirectional principle is honoured. The weak layer is everything around the core: there are two full drivers (`net/mod.rs` and `celld_zone/object.rs`) that each re-implement admission, tick loop, netsim, hibernation and fact plumbing, with different security and durability behaviour.

Nothing here was compiled or run (per the brief); every finding is from reading the cited lines.

The three things most worth changing:
1. **Ownership and activation safety on the native server** — unauthenticated activation (finding 1), fencing-token reuse after a crash-restart (2), and no self-fencing when lease renewal fails (3).
2. **One driver contract instead of two-and-a-half** — a single admission function and a single stimulus enum shared by `net`, `celld_zone` and `replay` (4, 5, 13).
3. **Pre-release cleanup** — drop protocol v1 negotiation, `CellFactSink`, the hand-rolled HTTP peer client and the duplicate `DelayLine` (10, 11, 12, 14).

Core-crate contamination is minimal: only doc comments mentioning bomberman/zones, `BindingKind::Zones` in the gateway config, and the game-flavoured `src/leaderboard` (2.5k lines, another reviewer's slice).

**Not read, or only skimmed:**
- `src/`: `celld_zone/cells.rs`, `celld_zone/plan.rs`, `world/{watch,zone_status,zone_activity,content_rollout,allocation,directory,topology,barrier}.rs`, `surface.rs`, `schema.rs` (only `MAX_FIELDS`), `authoring.rs` (partial), `pure.rs`, `prediction.rs`, `bots.rs`, `stamp.rs`, `claims.rs`, `work.rs`, `metrics.rs` body, `net/inbox.rs`, `net/world.rs` body, `net/ticket.rs` (only `NonceCache`), `replay.rs` (header, outline and `replay_with` start only), `collide.rs` shape maths (lines 136–660, 870–990).
- `object.rs`: read 455–975, 1064–1292, 1407–1530, 1795–2234; the rest outlined only.
- Tests: only function lists for `golden`, `replication`, `netcode`, `authoring`; other test bodies unread.
- `examples/game`: only `rooms/src/zone.rs` 1196–1330, the `zone/cross.rs` header and outlines; client TS, sveltekit, `tiers`, `crawler`, `service`, `cells` unread.
- `tests/celld/*` workers unread; `tests/bomberman` only the `main.rs` header and `tick_saga.rs`.
- `docs/coordination-primitives.md`: headings only.

---

### 1. Unauthenticated Hello activates zones and writes durable state before the ticket is verified
- Category: E   Severity: high   Confidence: CONFIRMED
- Where: `distributed_game/src/net/mod.rs:1689` (`route_zone`) runs before `:1719` (`verify_hello_ticket`); `activate_local` `:831-866`; `run_zone` `:1082-1137`; default `hibernate_after: None` `:215`
- What: Any client that sends a well-formed Hello with a supported protocol version triggers `activate_local`. That inserts into `zones`, calls the facts factory (spawning a fact worker, `:853`), spawns `run_zone`, acquires an ownership claim, loads the state store and allocation store, and runs the game's factory. The ticket is only checked afterwards.
- Why it matters: With `CellZoneOwnership`, each garbage Hello for a new zone id creates a `ZoneLease` aggregate plus `zone.activated`/`zone.released` events and read-model rows for an attacker-chosen id. For ids the factory accepts (the doc example is `starts_with("arena-")`), the zone starts Hot and, with `hibernate_after: None`, is never removed. That is unbounded memory, tasks and lease renewals from unauthenticated traffic.
- Change: Verify signature, expiry and zone match first; use `directory.locate` alone for the Redirect path; only then activate, and consume the nonce after. Add a `max_zones` cap. (M)

### 2. Fencing token and `boot_epoch` can repeat after a crash-restart, so new facts are dropped as replays
- Category: E   Severity: high   Confidence: CONFIRMED (code path; not executed)
- Where: `net/mod.rs:432,844,1083` (holder is `"{host_id}#{activation}"`, a per-process counter from 1); `:1145` (`boot_epoch = claim.token`); `world/zone_lease.rs:285-293`, `src/lease/in_memory.rs:99`, `src/lease/sqlx_store.rs:196` (same holder re-acquiring gets the same token)
- What: A host that crashes without releasing and restarts inside the TTL (15 s) builds the same holder string for any zone that gets the same activation ordinal, for example the first zone activated after boot. Durable ownership then returns the old claim and token.
- Why it matters: `host.rs:98-103` says `boot_epoch` must differ between activations. With the same epoch and tick/player ids restarting from zero, keys like `{cmd}:{zone}:{epoch}:t{tick}:{n}` and lifecycle keys collide with the previous run. The command ledger treats them as replays, so kills, XP and join facts vanish with a "committed (replayed)" receipt.
- Change: Put a per-process nonce (start micros or a UUID) in the holder: `"{host_id}#{process_nonce}#{activation}"`. Add a conformance test: a restarted holder gets a larger token. (S)

### 3. A zone keeps simulating and committing facts after its lease may have expired
- Category: E   Severity: high   Confidence: CONFIRMED
- Where: `net/mod.rs:1049-1068` (`Err(OwnershipError::Backend(_)) => continue`, forever); `:1262-1267` and `:1343-1347` (after `Exit::Lost`, `dispose()` runs `on_leave` hooks and the fact worker still drains the queue)
- What: Renewal that fails with backend errors retries indefinitely with no local deadline; `ZoneClaim.expires_at` is never consulted. The zone ticks through a partition while another host can acquire it. Even on a clean `Lost`, queued and dispose-time facts are committed, and facts carry no fencing check (the stamp's `activation` is informational).
- Why it matters: This is a split-brain window bounded only by partition length. Snapshots are fenced, facts are not, so two activations of a zone can both credit kills.
- Change: Track the last successful renewal and self-fence (freeze, then drop without dispose-time facts) at `ttl − margin`. Put a timeout on the renew call. On `Lost`, close the fact queue without draining, or have the sink pass the token so the cell can reject stale activations. (M)

### 4. Admission is implemented twice with different rules (native vs celld)
- Category: A/C (principle 7)   Severity: high   Confidence: CONFIRMED
- Where: `net/mod.rs:1659-1838` plus `verify_hello_ticket` `:1954-1985`; `celld_zone/object.rs:1795-1992`
- What: Both do protocol check, ticket verify, zone match, nonce, join/transfer/reconnect. They differ in these ways:
  - **Ticket checks:** native runs `validate_extensions`, the custom `ticket_verifier`, and empty-principal/nonce checks; celld calls only `key.verify_ticket` (no `validate_extensions` or `ticket_verifier` hit under `src/celld_zone/`).
  - **Transfers:** native uses Reserve/Confirm/Release/Return with the issuer's registry; celld persists a nonce in object storage and has no return-to-source path.
  - **Rate limiting:** native has a per-connection token bucket (`:1622`); celld has none (`object.rs:1136-1160` feeds every Input to the host).
  - **Hello deadline:** native enforces `hello_timeout`; celld accepts any upgrade (`:1102-1104`), keeps it indefinitely, and answers unauthenticated Pings (`:1166`).
- Why it matters: A game configured with ticket extensions gets them enforced on one backend and silently ignored on the other. Every admission fix has to be made twice.
- Change: Extract one transport-agnostic `admit(hello, now, &mut NonceStore, &TransferLedger, &AdmissionConfig) -> AdmitDecision` plus one `InputGate` (the bucket) into the crate core; both drivers call them. Breaking: `ServerConfig` ticket fields move into a shared `AdmissionConfig`. (L)

### 5. Un-rate-limited message types and an unbounded command drain can starve the tick
- Category: E/D   Severity: medium   Confidence: CONFIRMED
- Where: `net/mod.rs:1902-1935` (only `Input` goes through `bucket.take()`; `Focus`, `Content`, `Ping`, repeated `Hello` and malformed frames are unlimited); `:1274-1278` (`while let Ok(cmd) = rx.try_recv()` with no budget); `:843` (one 1024-slot command channel shared by all connections)
- What: A client can flood `Focus`/`Content` (each `Content` carries a string up to the 64 KiB frame limit). Each becomes a `ZoneCmd` on the shared channel, and the zone task drains until empty before the next `advance()`.
- Why it matters: A few flooding connections delay ticks and other players' inputs behind FIFO backpressure. `on_observer_focus` is a game hook, so the per-command cost is game-defined.
- Change: Run every post-Hello client message through the bucket and kick on sustained decode errors. Cap the drain per iteration (N commands or until the next tick is due). Coalesce `Focus` per connection. (S)

### 6. `Heightfield::raycast` can hang a zone on a long or infinite ray
- Category: E   Severity: medium   Confidence: CONFIRMED
- Where: `distributed_game/src/collide.rs:426-458`; `Ray::new` `:88-101` allows `max_dist = INFINITY`
- What: The march advances `t` by `cell/2` until `t >= max_dist`, with no clipping to the heightfield's bounds. Off-grid `under()` is false, so the loop continues. With `f32::INFINITY` or `f32::MAX` the loop never ends once `t + step == t`; with a large finite range it does millions of `height_at` calls.
- Why it matters: `Ray::new(o, d, f32::MAX)` is an idiomatic "no limit" ray. A hang is worse than a panic: the zone task spins and the lease keeps renewing.
- Change: Clip the ray to the heightfield AABB (xy extent, `[min_h, top]`) before marching and march only that interval. (S)

### 7. Lag compensation clones and interpolates every entity per query and allocates per entity per tick
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `rewind.rs:391-402` (`at_time` builds a fresh `Vec<Historic>`, each with a cloned `fields: Vec<f32>`); callers `:441`, `:461`, `:500`; `host.rs:770-775` (`Ctx::raycast` at "now" also goes through `at_time`); `host.rs:2159-2166` (new `RewindFrame` Vec per tick)
- What: Each shot, raycast or overlap costs O(N) clones and allocations with no spatial filter. Recording allocates a new frame, plus one Vec per entity with fields, every tick.
- Why it matters: M shooters × N entities per tick with heap churn on the hottest gameplay path. A bouncing laser calls it once per leg.
- Change: Iterate frames by reference and lerp position on the fly, materialising `Historic` only for the winning hit. Recycle the popped frame's Vec in `record`. Store fields in a small inline array or one flat `Vec<f32>` per frame. (M)

### 8. Static `Colliders` have no broadphase
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `collide.rs:728-760` (`raycast`), `:768-800` (`ground_at`), `:803-818` (`push_out`: three passes over all shapes)
- What: Every query scans every shape; movers call `push_out` and `ground_at` each tick.
- Why it matters: Cost is O(movers × shapes × 3) per tick. Geometry is static (set once in `on_create`) and `interest::SpatialGrid` already exists in the crate.
- Change: Build a uniform grid of shape indices in `set_colliders` / `Colliders::push` and query candidates from it. API unchanged. (M)

### 9. Each patch allocates at least three times per entity even when nothing changed
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `replication.rs:297-318` (per entity: `Writer::new()`, `offsets` Vec, `data` Vec, then compared with the previous payload); `replication.rs:64` (`fields: Vec<FieldValue>` per entity per patch); `host.rs:2181-2186` (fresh `WorldView` each patch); `interest.rs:120` (grid `HashMap<_, Vec<u32>>` rebuilt each patch)
- What: The encode-once/shared-revision design is good, but the "unchanged" check happens after building the full dynamic `FieldValue` vector and encoding into fresh buffers. `build_delta` also takes `&EncodedWorld` and locks an internal `Mutex` (`:249,454`) for what is single-threaded scratch.
- Why it matters: An idle world of 1,000 entities at 20 Hz does 60k+ allocations per second per zone.
- Change: Have `Replicated` encode straight into a per-patch arena (`Vec<u8>` plus an offsets table) and compare against the previous payload slice, allocating an `Arc<Payload>` only on change. Reuse `WorldView` and grid buffers across patches. Pass the update cache as `&mut` instead of a `Mutex`. Breaking: `EntityState.fields` / `FieldValue` leave the hot path. (M–L)

### 10. Wire protocol carries v1/v2 negotiation before first release
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `protocol/messages.rs:10-18` (`PROTOCOL_VERSION = 2`, `MIN_PROTOCOL_VERSION = 1`), `:498`, `:613-617`; `host.rs:979-980,1248-1251,1301,1511` (`next_protocol`, `speaking()`); `tests/golden/main.rs:639` `welcome_decodes_only_in_the_version_offered`
- What: The host keeps one-shot "protocol of the next admission" state so a v1 client gets a v1 Welcome. `feature::ALL` advertises a single `FOCUS` bit.
- Why it matters: Nothing is released, so this is compat for clients that do not exist. `speaking()` is a stateful one-call modifier that is easy to misuse.
- Change: One protocol version. Delete `MIN_PROTOCOL_VERSION`, `speaking`, `next_protocol`, the v1 Welcome branches and fixtures, and the `features` field until a second feature exists. (S)

### 11. `CellFactSink` is superseded and unused outside its own test
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `cell.rs:36-67,237-270`; rg over `*.rs` and `*.md` (not `js/`, which is TypeScript) finds it only in `lib.rs` docs, `README.md`, `cell.rs` and `tests/cell_sink/main.rs`
- What: This is a second in-process fact sink beside `CommandFactSink<LocalCommandHost>` (`command_sink.rs:428`). "Retryable" is classified three ways:
  - `cell.rs:228-235`: only InProgress/Internal.
  - `command_sink.rs:383-399`: also 5xx/408/429.
  - `cell.rs:180-182`: by status code and error code string.
- Why it matters: Parallel types, and retry semantics differ by sink.
- Change: Delete `CellFactSink` and `tests/cell_sink`. Keep `command_id_for`, `trusted_session` and `run_fact_batch`, and use one `is_retryable` classification. (S)

### 12. Host-to-host calls use a hand-rolled plaintext HTTP/1.1 client
- Category: E/F   Severity: medium   Confidence: CONFIRMED
- Where: `net/peer.rs:22-105` (raw `TcpStream`, `strip_prefix("http://")` only, `format!`-built request with the admin secret header, unbounded `read_to_end`, hand-written `dechunk`)
- What: Transfer reserve/confirm, child events, zone messages and barrier phases between hosts go over this client.
- Why it matters: The admin secret travels in clear text and cannot use TLS. There is no response size cap. The crate already depends on `reqwest` (feature `celld`), and core has `HttpCommandHost` and an internal-secret scheme.
- Change: Use `reqwest` with the core `InternalHttpSecret` header convention. Delete `post_inner` and `dechunk`. (S)

### 13. `Recorder` is a hand-mirrored wrapper of `ZoneHost`, is not used by either driver, and has drifted
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `replay.rs:1060-1408` (about 25 pass-through methods); rg finds `Recorder` only in `tests/world_host` and `tests/netcode`; no `deliver_zone`, `observer_focus` or `content_patched` in `replay.rs`; `put_config`/`get_config` `:566-680` hand-serialise `HostConfig`
- What: Record/replay only works if a caller drives the wrapper instead of the host. `net::run_zone` and `ZoneObject` call `ZoneHost` directly, so production sessions cannot be recorded. Zones that use zone messages (the mobpit example does) are not captured and will diverge on replay. Three parallel stimulus vocabularies exist: `replay::Event`, `net::ZoneCmd` (`net/mod.rs:232-290`) and `object.rs` `InItem`.
- Why it matters: This is the wrapper-type pattern the owner rejects, and 1.8k lines of the most valuable debugging tool are unreachable in production.
- Change: Define one `Stimulus` enum and `ZoneHost::apply(Stimulus) -> Outcome`. Make recording an optional tap inside the host (`with_recording(sink)`). `ZoneCmd` and `replay::Event` collapse onto it; `Recorder` is deleted. (L)

### 14. Dev netsim is implemented twice and lives in both production paths
- Category: C   Severity: low-medium   Confidence: CONFIRMED
- Where: `netsim.rs:146-205` (`DelayLine`, f64 ms; used by `object.rs:696-697`) and a private copy at `net/mod.rs:1569-1613` (`Instant`-based)
- What: Two order-preserving delay lines. `ZoneObject` keeps per-socket `out_lines`/`in_lines` maps and routes every frame through `push` even with no simulation.
- Change: Use `netsim::DelayLine` in `net` (convert `Instant` to ms) and delete the copy. Give `DelayLine` a bound. (S)

### 15. Native and celld give different fact durability
- Category: A (principle 7)   Severity: medium   Confidence: CONFIRMED
- Where: `facts.rs:351-356,412-433` (in-memory `VecDeque`); `net/mod.rs:1343-1347` (drain bounded by a 2 s `settle_timeout`, then the worker is detached); `object.rs:459-534` (`DurableFacts`: journal written to object storage after the tick and deleted on settle); `command_sink.rs:25-29` ("No local durable fallback")
- What: On celld, emitted facts survive a crash via the outbox. On the native server, unsettled facts are lost on crash and on a slow shutdown. The API and docs call both "durable facts".
- Why it matters: The same game code credits XP at-least-once on one backend and at-most-once on the other.
- Change: Make the journal a host-level seam (`FactJournal` trait: append after tick, delete on settle, list on wake). Native gets a sqlite/file implementation; celld keeps storage. Otherwise document native as at-most-once and rename accordingly. (M)

### 16. The example re-implements a fact backlog the host already has privately
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `examples/game/server/rooms/src/zone.rs:1292-1330` (`flush_backlog`: retry queue on `Backpressure`, `pending` map keyed by fact key); `host.rs:296,356-370` (`lifecycle_backlog` / `flush_lifecycle`, the same pattern, private)
- What: `Ctx::emit*` returns `EmitError::Backpressure` and every game must keep its own queue plus key→context map to correlate `on_fact_settled`. Hand-written `encode_carried`/`decode_carried` (`zone.rs:1098-1130`) also sit beside the `Wire` derive.
- Change: Add `Ctx::emit_queued(player, input)` backed by one host backlog (lifecycle facts use it too); `Backpressure` stops being a routine caller concern. Make `Ctx::handoff` carried state a `Wire` type. (M)

### 17. Handoff custody of carried state is the client, and the issuer registry is in memory
- Category: E   Severity: medium   Confidence: CONFIRMED (design)
- Where: `host.rs:881-938,2052-2056` (player removed from the source as soon as the hook returns; state rides in the signed ticket sent to the client); `net/transfer.rs:66-123` (`TransferRegistry` is a `HashMap` in the issuing process); `net/mod.rs:1734-1746`
- What: The no-duplication guarantee holds. But a client that drops the ticket, or an issuer restart ("unknown transfer"), loses the carried state: the player exists in neither zone. A client can also deliberately discard a ticket carrying bad state and rejoin fresh.
- Change: Either document handoff as at-most-once and client-droppable and tell games to carry only non-authoritative state, or park the departure in the source zone until Confirm or TTL (the example's `Parked` at `zone.rs:971-980` already does this in game code). (M)

### 18. Lease renewal is an event-sourced heartbeat
- Category: D   Severity: low-medium   Confidence: PLAUSIBLE (stream growth inferred, not measured)
- Where: `world/zone_lease.rs:132-135,319-344` (`zone_lease.renewed` event per renew); `net/mod.rs:216-217` (renew every 5 s)
- What: Each running zone appends about 17k `renewed` events per day to its `ZoneLease` stream and reschedules its expiry alarm each time.
- Change: Renew at about TTL/3 with a longer TTL (for example 60 s / 20 s), and confirm cell compaction drops events behind the snapshot. (S)

### 19. `commit_barrier` / `abort_barrier` ignore the barrier id
- Category: E/F   Severity: low   Confidence: CONFIRMED
- Where: `net/mod.rs:641-647` (`_barrier: u64`), `:667`
- What: A delayed commit or abort from an older barrier applies to whatever barrier is current. `world.barrier` holds the id but is never compared.
- Change: Check the id against `world.barrier` and no-op on mismatch, or remove the parameter. (S)

### 20. Ownership contention is reported as NOT_FOUND, and a crash-restart refuses joins for up to the TTL
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `net/mod.rs:1084-1088` (acquire failure → `error::NOT_FOUND`); `activation_timeout` 5 s vs `ownership_ttl` 15 s (`:216-218`)
- What: "Zone is owned by X" surfaces to clients as not found. After an unclean restart (once finding 2 is fixed, the holder always differs) joins fail for up to 15 s although only 5 s is waited.
- Change: Return `ZONE_UNAVAILABLE` and make `activation_timeout >= ownership_ttl` by default. (S)

### 21. `NonceCache` does an O(n) sweep per join once past 1,024 live nonces and is unbounded
- Category: D   Severity: low   Confidence: CONFIRMED
- Where: `net/ticket.rs:382-384` (`retain` on every insert while `len > 1024`), under the global `inner.nonces` mutex (`net/mod.rs:1762`)
- Change: Sweep on a time interval or use an expiry-ordered queue; cap the size. (S)

### 22. Test fixtures: about ten bespoke test zones, one duplicated inside `src`
- Category: B   Severity: low   Confidence: CONFIRMED (structure; bodies mostly unread)
- Where:
  - `impl Zone for` in `tests/{entities,zone_runtime/arena,authoring,inbox,netcode,world/realm,celld_zone_lease,zone_message}`.
  - `Dots` defined twice: `src/metrics.rs:641` and `src/work.rs:125`.
  - No test found for a restarted holder's token (finding 2), renew-failure self-fencing (3), admission parity between drivers (4), or a heightfield ray with a huge range (6).
- What is valuable and should stay: `tests/golden` (cross-language byte fixtures), `tests/netcode` `recorded_session_replays_identically` and `mutated_input_diverges_at_the_consuming_tick`, and `ownership_conformance` (`world/ownership.rs:274`).
- Change: One shared `tests/support` zone; an admission conformance suite run against both drivers; add the four missing tests. (M)

### 23. Giant files: jobs and seams
- Category: F   Severity: medium   Confidence: CONFIRMED
- `celld_zone/object.rs` (2580 lines) holds eight jobs: `CelldObjectHost` fact transport (253-457), durable outbox (459-534), JS clock/random (545-600), netsim plumbing (716-833), the DO entry points (1069-1282), world-watch reconcile (1527-1795), admission (1795-1992), and tick/persist/hibernate/alarm (2043-2440). Seams: `object_host.rs`, `outbox.rs`, `platform.rs`, `watch_sync.rs`, `lifecycle.rs`; admission goes to the shared module from finding 4.
- `net/mod.rs` (2063 lines): server config and builder (100-802), activation and ownership (804-1068), `run_zone` (1071-1405), command handling (1407-1536), connection (1538-2038). Seams: `server.rs`, `activation.rs`, `zone_task.rs`, `connection.rs`.
- `host.rs` (2351 lines): `Ctx` (414-956) is a separate job from `ZoneHost`. Admission (1253-1554) has near-duplicate `PlayerJoined` blocks at 1366-1386 and 1419-1441. Seams: `ctx.rs`, `membership.rs`, `advance.rs`.
- `replay.rs` (1792 lines): binary codec (442-1045) vs recorder vs replayer; shrinks to codec plus replayer after finding 13.
- `collide.rs` (1.4k lines) is half a kinematic physics library (`push_out`, `ground_at`, ramps, heightfield, `walkable`) in a crate whose docs say "Physics is out of scope" (`lib.rs:40`). The example's character controller (`examples/game/server/sim/src/mobpit.rs:273-470`) is the other half every 3D game would copy. Decide: move `collide` and a controller into an optional `distributed_game_physics` crate, or own it.

### 24. Small duplications and leftovers
- Category: C   Severity: low   Confidence: CONFIRMED
- `encode_segment` exists twice (`celld.rs:27`, `celld_zone/object.rs:234`); the shard/segment validity predicate three times (`celld.rs:21-25`, `command_sink.rs:281-285`, `FactCall` docs).
- `default_token` (`host.rs:2335-2351`) builds session tokens from `RandomState`, which is not a CSPRNG contract and is a fixed key on wasm32-unknown-unknown; both drivers override it. `ZoneHost::new` should take the token source as a required argument. Same for `unix_micros()` when `boot_epoch == 0` (`host.rs:1000-1002`).
- Two game models coexist without guidance: `tests/bomberman` (every tick is aggregates plus a saga) and `distributed_game` (in-memory zone plus facts). `src/microsvc/cell_host/store.rs:48,186` doc comments describe the generic parent-shard cell in bomberman terms.
- `docs/external-facts.md` is 78 lines with no headings or examples and is not game-specific; restructure it or fold it into the core docs.

### Look-alike modules: verdicts
| Game module | Core counterpart | Verdict |
|---|---|---|
| `lease.rs` (entity lending between zones) | `src/lease` | Different concept, tick-based and message-driven. Keep, but rename (`lend.rs` / `EntityLoan`) to stop the name collision. |
| `world/ownership.rs` + `world/zone_lease.rs` | `src/lease`, `schedule` | Reuses core correctly. |
| `facts.rs` (queue/feed/pump) | `src/outbox` | Justified: the tick must never await. Not durable on native (finding 15). |
| `timers.rs` | `src/schedule` | Justified: sim-time and deterministic. Fine. |
| `clock.rs` | `src/clock.rs` | Justified: monotonic `Duration` vs wall `SystemTime`, and the crate works without `distributed`. The same names (`Clock`, `ManualClock`, `SystemClock`) in both crates will confuse; consider `ZoneClock`. |
| `inbox.rs` | bus | Justified: direct to a running zone, at-most-once. |
| `metrics.rs` | `src/metrics` | Rationale is documented at the top of the file (instance-scoped, zone label cap). Acceptable, though a hand-written Prometheus renderer is a second one in the repo. |
| `command_sink.rs` / `cell.rs` | commands | Reuses core. `cell.rs` `CellFactSink` is redundant (finding 11). |

### Things that are good and should not be changed
- `ZoneHost` being synchronous, transport-agnostic and clock-injected, with `BTreeMap` state, a seeded `ZoneRng`, sim-time timers and per-tick state hashes. This is what makes replay and both drivers possible.
- Facts as idempotent commands with host-bound principals and restart-safe keys (`fact_key` plus deterministic `command_id_for`); zones never write durable state directly.
- `ZoneLease` / `ZoneState` as event-sourced cells with fencing tokens and schedule-declared expiry, and `FencedLeaseOwnership` over the core `LeaseStore`.
- The replication design: encode once per patch, shared revisions, a priority accumulator that never starves, hysteresis, deterministic candidate ordering. Optimise its allocations (finding 9), not its shape.
- Bounds-checked wire decoding (`Reader::raw`, count ≤ remaining before `with_capacity`, 1 MiB blob cap) and the golden byte fixtures shared with the JS client.
