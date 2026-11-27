# v5 pre-release review: consolidated summary

Reviewed 2026-11-18 on branch `v5` (d9e9bd20). Thirteen reviewers each read one slice; their full
reports are files `01`–`13` in this directory, with `path:line` citations for every finding.

**How to read this.** Nothing was compiled or run; every finding comes from reading code. Items
marked **[checked]** are ones the consolidating reviewer re-opened and confirmed in the source. The
rest are as reported by the slice reviewer (each marks its own findings CONFIRMED or PLAUSIBLE).
Each report opens with an exact list of what was not read.

## 1. Fix regardless of the breaking-change plan

### Authorization and identity
- **[checked] Multi-role application surface runs SQL with the first role's row filter.**
  `src/graphql/engine/auth.rs:219-227` returns `first.row_filter` in both branches. Report 04 #1.
- **[checked] Identity fails open.** A `GraphqlEngine` without `.identity(..)` defaults to
  `DevHeaders` (`engine/builder.rs:40`). `IdentityConfig::hybrid()` has no gateway secret, so a
  request with no bearer is trusted from its headers (`identity/resolve.rs:351-357`). `?x-user-id=`
  and `?x-roles=` are merged into headers on every WebSocket upgrade in every mode
  (`graphql/http.rs:546-547`), and the gateway strips the headers but forwards the query string.
  Reports 05 #1–3, 07 #1.
- Command-delta row policy is a fourth interpreter of `FilterExpr` with wrong NULL and numeric
  semantics, so a command can push a row into a replica that the query would not return.
  Report 05 #4.
- Gateway: Zones binding forwards the caller's bearer and cookies; worker `Handler`/`Assets`
  bindings get unstripped client headers; `extensions.gatewayDelivery` can be forged by a client.
  Report 07 #13–15.
- JWKS refresh has no failure backoff or stale-if-error, so an IdP outage turns into serial 5–10 s
  timeouts and 401s. Report 05 #5.
- Origin WebSocket subscriptions outlive token expiry and have no keepalive or caps. Report 05 #6.

### Data loss and correctness
- **[checked] `projection!` rebuilds and revalidates the whole program on every event and leaks a
  string per event type each time** (`src/lib.rs:231-238`, `src/mutation/descriptor.rs:85,155`).
  Reports 03 #1, 08 #3.
- **[checked] The default failure policy drops messages.** Kafka `dead_letter` commits past the
  record; RabbitMQ queues have no dead-letter exchange; SQL deletes the row. Report 07 #2.
- **[checked] Outbox retry has no backoff** and counts every failure toward the ceiling, so a broker
  outage sends the oldest rows terminal in seconds; there is no requeue API. Report 01 #5.
- Bus consumers return `Ok(())` when idle by default; Kafka and RabbitMQ have no long-running mode,
  and the supervisor treats the clean exit as a host bug. Report 07 #3.
- `#[event]` records the event before a fallible recorder body runs. Report 08 #1.
- `mutation_file!` does not register its input with Cargo, so editing only the `.graphql` file
  leaves the old program running. Report 08 #4.
- `QueuedRepository` **[checked: holds the stream lock after `get_stream` returns]** self-deadlocks
  when combined with snapshots on a stale snapshot, and releases locks it does not own.
  Report 01 #1–3.
- Plain `repo.commit(&mut agg)` with captured domain events persists replay events and publishes
  nothing. Report 01 #4.
- Legacy JSON handlers are a second write path with no roles, ledger or canonical input, and they
  are the only thing bus, Knative and saga commands can reach. `AggregateCell::dispatch` runs typed
  commands with none of those checks and exists only for tests. Report 06 #2–3.
- `#[command(id = ..)]` wraps the user's handler in `#[cfg(feature = "application-runtime")]`
  evaluated in the user's crate, with the lint suppressed. Reports 08 #6, 07 #9.
- Backends disagree: tombstoned-record observation (memory accepts, SQL rejects), tail load of a
  missing stream, `_like`/`_ilike` case rules. Reports 03 #7, 01 #7, 04 #7.

### Game runtime
- **[checked] An unauthenticated Hello activates a zone before the ticket is verified**
  (`distributed_game/src/net/mod.rs:1689` vs `:1719`). Report 10 #1.
- Fencing token and `boot_epoch` can repeat after a crash-restart, so new facts are dropped as
  replays; a zone keeps simulating and committing facts after lease renewal fails. Report 10 #2–3.
- Admission is implemented twice (native vs celld) with different ticket checks and no rate limit
  on celld. Report 10 #4.

### TypeScript client
- **[checked] A dropped live WebSocket is never reopened** (`impl-fetch-live.ts:347-362`); one
  socket per live query. Report 11 #2.
- **[checked] Every cache transaction `structuredClone`s the whole cache**, then re-extracts and
  re-materializes it several more times. Report 11 #1.
- **[checked] `gc()` has no caller**; per-operation and per-command maps never shrink.
  Report 11 #3–5.

### Tests that do not run
- **[checked] Five Postgres projection-protocol tests return early on an env var nothing sets**, so
  they pass without executing, including the regression test for #297. Report 02 #1.
- The same early-return pattern covers every broker and Postgres integration test; several targets
  are never run with their service in any workflow. GraphQL/RBAC is proven on SQLite only (about
  165 tests vs 4 on Postgres). Report 12 #1–2.

### Tooling
- `distributed dev` orphans `cargo run` / `npm run dev` on anything but Ctrl-C; build cancel kills
  only the direct child; a transient file error ends the session. Report 09 #11–12.
- The reference app contains a dual write (`auth_users` upserted directly and also projected).
  Report 13 #9.

## 2. Structural themes for the breaking-change pass

### One contract, several hand-maintained copies
The client wire contract exists as Rust runtime types, a second Rust copy in the CLI (the CLI does
not depend on `distributed`), and hand-written TypeScript types and validators. Preview
canonicalisation, trusted presets, complexity estimation and `GameSurface` are also twinned, and
some have already drifted. Reports 09 #1–6, 05 #8–9, 11 #6–9.
**Change:** the CLI depends on `distributed` (default features off) and `distributed_game`; delete
the mirrors; emit Rust-generated fixtures for the response envelope and a much larger query-plan
corpus for the TS side.

### Two (or more) of everything
| Concept | Parallel implementations | Report |
|---|---|---|
| Projection program IR | `MutationProgram` and `ProjectionProgram`, bridged by a per-field rewrite; 4 "kind" enums | 03 #2, #10 |
| Read permissions | `ReadPermission` and `RoleGrant`, two intersections, two validators | 04 #3 |
| Row-policy evaluation | SQL compiler, cell evaluator, delta evaluator, TS replica | 05 #4 |
| Commit assembly | `AggregateRepository::commit`, `CommitBuilder`, `AggregateCommit`, causal workspace | 01 #4 |
| Command declaration | 5–7 forms; the reference app uses the verbose one for all 11 commands | 08 #5, 06, 13 #2 |
| Typed command pipeline | 3 copies in `routes.rs` plus a parallel helper set for cells | 06 #1 |
| Command dispatch | `CommandHost` and an inert `CommandDispatcher`/remote envelope | 06 #6 |
| Immediate publish / timers / leases | mailbox vs born-InFlight; `deliver_at` vs `Schedule`; `lock/` vs `lease/` | 01 #11, #13, #14 |
| Gateway adapters | native and worker live/delivery state machines copied | 07 #10 |
| Game drivers | `net/` and `celld_zone/`, plus an unused `Recorder` wrapper | 10 #4, #13 |
| SQL dialect seam | `Statement/SqlExecutor` and `SqlxRepoBackend + QueryBuilder` | 02 #10 |

### Backend parity is asserted, not enforced
The projection commit state machine is written twice (memory and SQL) and has diverged; bus adapters
differ on ordering, redelivery, grouping and poison handling; conformance suites cover a fraction of
either. Reports 03 #6, 02 #2–3, 07 #4, 12 #3–4, #14.
**Change:** move decisions above the store/transport traits as pure functions; one conformance macro
per contract (the repository one in `tests/persistent_repository_conformance` is the model),
instantiated for every backend, with exclusions as named capabilities.

### Pre-release compatibility and unused surface
- `QueuedRepository` + `lock/` (about 2.3k lines; the service path already bypasses it). 01 #3
- Roughly a third of `src/mutation/` has no production caller. 03 #4
- Confirmations/obligations, the `CommandEffects` IR, half of `command_dispatch/`. 06 #4–6
- Application deployment plan / capability / runtime-host layer (about 1.6k lines). 07 #8
- About 2.7k lines of `distributed_cli/contracts/`; `contracts check` does not check drift; root
  `build.rs` twins it. 09 #7–8
- 13 migrations for an unreleased schema; the SQL bus runs a legacy upgrade under
  `ACCESS EXCLUSIVE` on every `listen`. 02 #11, 07 #5
- `aggregate!`, `#[digest]`, `#[enqueue]`, `emitter`, `impl_aggregate!`, `graphql_models!`,
  `module!`/`application!`, bus `capabilities.rs`, game protocol v1 negotiation, `CellFactSink`.
  08 #7, 07 #18, 10 #10–11
- Feature flags: `runtime` gates nothing; `application-runtime`; `leaderboard-background`; the
  17-times-copied "is tokio present" cfg list that omits `redis`. 01 #10, 07 #22–23

### Layering
- Typed command execution compiles only with the `graphql` feature; `ScheduleWorker` and projection
  rebuild are gated on it too. 06 #16, 01 #9, 03 #13
- Core imports `microsvc`, cell-host and Durable Object details. 01 #9
- Leaderboard is a second projection system inside the core crate. 07 #21
- Lifecycle write gate is an env var read from disk on every dispatch. 06 #12
- The CLI and "framework-neutral" compiler are hard-wired to SvelteKit, npm and the e2e app. 09 #13

### Authoring experience
Adding one command to the reference app touches about 12 hand-edited sites. The module `routes()`
bounds wall, `projection_owners()`, `MODULE_IDS`, the hand-built manifest and the sqlite/postgres
host fork restate what the framework knows. e2e-celld copies the whole service layer (about 3.3k of
3.6k lines). The app-owned `distributed.config.js` is 300 lines of validation. Report 13 #1–8,
#12–13, #17. Framework gaps surfaced by workarounds in the apps: no client pagination primitive, no
`now()` input default, no per-row pending state, no routes without an aggregate, no integration
event → `projection!` path. Report 13 #9–10, #18–21.

### Performance
- About 28 SQL statements per projected event; ownership-fence `FOR UPDATE` serialises read-model
  commits per table; unindexed causation lookup; per-row snapshot loop. 02 #4–7
- In-memory store clones all protocol state per commit. 03 #8
- Live queries: one full query per subscriber per table-level change, no sharing. 05 #7
- Query cost model ignores `limit`. 04 #4
- Contract fingerprint recomputed per dispatch; post-commit ledger re-read; ledger never compacted.
  06 #7, #8, #11
- Postgres bus: one contended row, about 7 statements per publish. 07 #16
- Game: per-entity allocations per patch, no collision broadphase, O(N) clone per lag-comp query.
  10 #7–9

### Tests
Overall verdict from the test reviewer: the suite is mostly behavioural; the implementation-detail
tests are a small nameable set.
- Source-grep tests: `tests/legacy_authoring_absence.rs`, two in `e2e_ui_celld_nats_profile`. 12 #7
- Golden SHA-256 hashes of whole SDL/manifest artefacts. 04 #17
- Expansion-string `contains` tests in the macro crates (about 140 assertions); 18 rustc-pinned
  trybuild snapshots. 08 #17–18
- Tests of derives, constants and unreachable states (named lists in 01 #24, 03 #20, 06 #20,
  09 #19, 12 #9, 13 #27).
- Copy-paste where a conformance suite belongs: 804 shared lines between `sqlite_transport` and
  `postgres_transport`; three near-identical projection-store test files (about 9k lines).
  12 #4, 03 #19, 02 #3
- 87 test binaries, 51 empty under default features; about 12 Todo-like fixture aggregates.
  12 #13, #16

### Docs
`docs/application-composition.md` describes an API that does not exist and a role matrix the README
rejects. The README is 2,845 lines mixing pitch, runbook and reference, with several stale
statements. Reports 13 #23–25, 07 #24.

## 3. Suggested order
1. Section 1 fixes, with a failing test first for each.
2. Make skipped tests fail (`require_service`), run the full matrix in CI, port the authz suite to
   Postgres.
3. Deletions (they shrink everything that follows).
4. Unifications: one program IR, one permission type, one commit path, one command form, one typed
   pipeline; CLI depends on the runtime crate.
5. Lift backend logic above the store and bus traits; conformance macros.
6. Performance work on the reduced code.
7. Authoring-surface and docs pass against the reference app.

## 4. What the reviewers said not to change
Each report ends with a short list. Recurring items: `CommitBatch` as the single atomic unit; the
command ledger state machine and its conformance suite; canonical, domain-separated identities with
frozen fixtures; the one-compiler/`DialectOps` GraphQL design with bind-only values; live queries as
hash-gated re-execution through the one-shot path; the bus runner layering; `ZoneHost` being
synchronous and deterministic; three-valued query-plan evaluation in the replica; the repository
conformance macro; domain crates as plain structs.

## Owner decisions (2026-11-18)
- v5 is a breaking release: prefer the clean structural change over a compatible patch wherever a
  finding offers both, and do wire-level renames now.
- The demos (e2e-ui and e2e-celld with SvelteKit, and the game demos) must work at the end. They may
  be rewritten to fit the new APIs; they are the acceptance gate, not a constraint on the design.
- The owner likes the e2e-ui demo experience and the principles as they are. Read the "Authoring
  experience" theme and report 13 as optional polish: internal changes should preserve what e2e-ui
  authors write and what its users see, and must not replace the forms it uses
  (`portable_command!`, `projection!`, permissions beside the model, the page code).
- Rename `portable_command!` to `command!`. This requires removing the fn-form `#[command(id = ..)]`
  attribute first (report 08 #5–6 already recommends it): both are exported from
  `distributed_macros` and two macros cannot share the name `command`. The method form
  `#[command("agg.verb", ..)]` is consumed by `#[sourced]`, not a macro of its own, so it can stay.
- `QueuedRepository` is kept. Fix its defects (report 01 #1-2); the recommendation to delete it and
  `lock/` (report 01 #3, and its entry under "unused surface" below) is withdrawn.
- gitkb/metaforge (`twig-metaforge`) is pinned to `v5` and must still work after the refactors.
- Assumed, not confirmed: no deployed databases need an upgrade path, so migrations can be squashed
  and legacy upgrade code deleted.

## Housekeeping
GitKB in this repo was stuck in an interrupted 0.1→1.0 upgrade during the review
(`.kb/.migrate-*` present, every `git-kb` command erroring), so no task documents were created.
