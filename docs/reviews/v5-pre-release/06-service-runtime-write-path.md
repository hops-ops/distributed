## Assessment

The typed command path is sound at its core: one backend transaction commits events, outbox, read-model plans and the fenced ledger completion, and authoring is thin (examples use only `#[command]`). The problem is what surrounds it. The same pipeline is written three times, and a second unledgered, role-less "legacy JSON" write path is still the only thing bus, Knative and saga commands can reach. Several half-removed mechanisms (confirmations/obligations, the `CommandEffects` IR, the remote dispatcher, direct-projection terminals) are still threaded through hot-path code.

The three changes most worth making:
1. Collapse the three typed pipelines in `routes.rs` (and the parallel helper set in `cell_host/causal.rs`) into one, and split `routes.rs`.
2. Make it one write path: delete `RegisteredHandler::Legacy`, `routes!` and `invoke_cell`, and let bus and saga commands reach typed, ledgered commands through an explicit trusted-ingress principal.
3. Delete the vestigial contract machinery and the dead half of `command_dispatch/`; compute the contract fingerprint once at bind time.

Not read or only skimmed (all paths under `/Users/patrickleet/dev/hops-ops/hops/distributed/src/`):
- **Not read:** `microsvc/projector/*`, `microsvc/cell_host/{store,sql_store,sql_executor,celld_outbox,wire,internal_auth,command}.rs`, `microsvc/{context,session,error,descriptor}.rs`, `command/{projections,direct_projection,projection_proof,effect_wire,types,tests}.rs`, `command_ledger/{ids,error}.rs`, `saga/{fan_out_tests,error}.rs`, the proc-macro expansion code in `distributed_macros`.
- **Outline or partial only:** `microsvc/service/tests.rs` and `command_ledger/tests.rs` (fn outline plus about six test bodies), `command_ledger/record.rs`, `microsvc/dependencies.rs`, `microsvc/cell_host/causal.rs`, `microsvc/causal.rs` tests (line 849 onward), `command/input.rs` after line 330, `microsvc/grpc.rs` wait-path middle section.
- **Docs:** the four docs were read early; only the disagreements listed in finding 22 were verified against code.
- **Line numbers:** those in `routes.rs`, `runtime.rs` and `handlers.rs` come from full reads; the load-bearing ones were re-opened at the end, and a few neighbouring ranges may be off by a few lines.

## Findings

### 1. The typed command pipeline is implemented three times
- Category: C/F   Severity: high   Confidence: CONFIRMED
- Where: `src/microsvc/service/routes.rs:1669-1917` (`dispatch_cell_causal`), `:1933-2343` (`dispatch`), `:2542-2598` (`invoke_cell`); helper twins at `src/microsvc/service/causal.rs:694-950` and `src/microsvc/cell_host/causal.rs:228-438`
- What: `dispatch_cell_causal` and `dispatch` are ~250-line near-copies of the same sequence, from admit and canonicalize through reserve, handler, `commit_causal_batch` and the ledger re-read. Each has about 15 hand-rolled `return abandon_causal_attempt(...)` blocks. The cell copy has its own `CellDispatchError`/`CellDispatchResult` and its own `handler_error_code`, `internal_ledger_error`, `replay_result`, `replay_rejection`, `commit_rejection`, `load_committed_result`, `abandon_attempt`, `recover_commit_error` and `resolve_ambiguous_lookup`, each mirroring a `service/causal.rs` function.
- Why it matters: every fix to the write path must be made twice or three times, and the copies have already diverged (findings 2 and 9). It also contradicts the owner's "no parallel types" preference.
- Change: one `execute_typed_command(ctx, reservation_source, terminal_policy)` returning one result/error type; cell and GraphQL differ only in how the result is rendered to wire and which terminal states are allowed. Delete `CellDispatchError`/`CellDispatchResult` in favour of `CausalDispatchError`/`CausalDispatchResult` plus a wire encoder (breaking for celld callers). Wrap the attempt in a guard so abandonment is one code path. L.

### 2. `AggregateCell::dispatch` / `invoke_cell` runs typed commands with no roles, no ledger and no canonical input
- Category: A/E   Severity: high   Confidence: CONFIRMED
- Where: `src/microsvc/service/routes.rs:2542-2598`; public entry `src/microsvc/cell_host/cell.rs:209-226`
- What: `invoke_cell` skips the role check (`admit_command_session`/`ensure_causal_grant`), the ledger reservation and `canonicalize_command_input` (it uses plain `serde_json::from_value`). It stamps a random `Uuid::now_v7` causation and commits via plain `TransactionalCommit::commit_batch`. `rg` finds callers only in `src/microsvc/cell_host/tests.rs`.
- Why it matters: it is a public API that runs the same handler without the authorization and idempotency every other typed ingress enforces, and it exists only for tests.
- Change: delete `AggregateCell::dispatch` and `invoke_cell`; move the tests to `dispatch_idempotent`. Breaking, S.

### 3. A second write path (legacy JSON handlers) is the only thing bus, Knative and saga commands can reach
- Category: A   Severity: high   Confidence: CONFIRMED
- Where: `src/microsvc/service/routes.rs:159-166` (`RegisteredHandler::Legacy`), `:1557-1561`; `src/microsvc/mod.rs:209` (`routes!`); `src/microsvc/http.rs` and `src/microsvc/grpc.rs:210` (fallback `service.dispatch`); `src/microsvc/message_router.rs:25-30`; `src/saga/definition.rs:62-63`
- What: `Service::dispatch`, `dispatch_request` and `dispatch_message` only run `Legacy` handlers. A typed command reached there returns `Unauthorized("typed causal commands require a verified GraphQL bearer envelope")`. Legacy handlers have no role declaration, no ledger and no input canonicalization, and they commit through the queued/locking repo rather than the causal workspace. Saga participant commands arrive over the bus, so they can only be legacy handlers; `SagaCommand::json` is documented as "what `microsvc` handlers decode with `ctx.input`". The saga's deterministic command id is never offered to the command ledger, so participants hand-roll dedupe (`docs/coordination-primitives.md:226-231`).
- Why it matters: two authoring models with different guarantees for the same concept. Principle 1's command step has an unaudited side door, and the framework's own coordination primitive cannot use the ledger. No file under `examples/` uses the legacy builder or `routes!` (0 hits; examples use `#[command]`). `routes!` matches in 24 test files and the legacy `.command("..")` builder appears on 12 lines under `tests/`.
- Change:
  - Remove `RegisteredHandler::Legacy`, `Routes::command(name).handle`, `routes!` command arms and the fire-and-forget HTTP/gRPC body shape.
  - Give trusted ingress (bus consumer, Knative, schedule worker) an explicit `VerifiedPrincipal::from_trusted_transport` and route commands through `CommandHost::invoke`, with the message id (saga command id) as the ledger command id.
  - Event handlers and projectors stay as they are.
  - Breaking, L.

### 4. Confirmations/obligations machinery is vestigial but still on the hot path
- Category: F   Severity: high   Confidence: CONFIRMED
- Where: `src/command/typed_command.rs:131-238` (`resolve_projection_obligations*`, with `#[allow(dead_code)]` at 131); `src/microsvc/service/routes.rs:2122`, `:2220`, `:2701-2748`; `src/microsvc/service/causal.rs:1135-1232`; `src/command_ledger/state.rs:83-156`; `src/command_ledger/record.rs:574`; `src/command_ledger/reservation.rs:464`
- What: `TypedCommandContract.confirmations` is only ever non-empty in `src/command/tests.rs:530,590` (repo-wide `rg`; `typed_command()` sets `Vec::new()` and there is no builder). Yet dispatch still resolves obligations, checks "cannot mix modeled projection metadata with legacy confirmations", stores `projection_obligations` in the ledger and evaluates a separate legacy evidence branch on status. Validators in `state.rs` and tests such as `completion_rejects_malformed_projection_obligations` guard data production never writes.
- Why it matters: roughly a thousand lines of dead protocol in the most security-sensitive code, and readers must hold two projection-evidence models to follow dispatch.
- Change: delete `confirmations`, `CommandProjectionConfirmation`, `resolve_projection_obligations*`, `complete_with_obligations`, ledger `projection_obligations`, `CausalCommandProjectionObligation`, the legacy branch of `evaluate_pending_projection_evidence`, and the GraphQL `sanitize_/validate_command_confirmations`. Breaking for ledger row format, M-L.

### 5. `CommandEffects` IR is dead: every contract is `CommandEffects::revalidate()`
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `src/command/effects.rs:20-26` (`EffectExpression::InvalidConstant`, `#[allow(dead_code)]`, "no longer constructed after macro removal"); `src/command/typed_command.rs:491-519`; `src/command/effect_wire.rs` (341 lines, not read)
- What: `CommandEffect::*` variants and `CommandEffects::new` are constructed only in tests (`rg`). The contract still carries `effects`, fingerprints it, and validates `InvalidConstant` in `TypedServiceCommandBinding::from_contracts`.
- Why it matters: vestigial field plus wire format; `tests/legacy_authoring_absence.rs` asserts the macro is gone but the runtime half remains.
- Change: remove `effects` from the contract and delete `effects.rs`/`effect_wire.rs` (check the JS client for consumers of the wire shape first). Breaking, M.

### 6. Half of `command_dispatch/` is unused scaffolding; two "dispatch" abstractions coexist
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `src/command_dispatch/remote.rs`, `envelope.rs`, `local.rs`, `host.rs:695-721`; `src/application/runtime_host.rs:1-110`
- What:
  - `CommandDispatcher` → `LocalCommandDispatcher::dispatch` calls `service.dispatch_request`, which returns 401 for every typed command.
  - `RemoteCommandDispatcher`, `RemoteDispatchConfig`, `RemoteTrustMode`, `APPROVED_REMOTE_DISPATCH_PROFILE`, `CommandDispatchEnvelope` and `CommandDispatchReceipt` have no users outside the module and `lib.rs` re-exports.
  - The only `RemoteCommandTransport` impl is `LoopbackRemoteTransport` (`#[allow(dead_code)]`).
  - Envelope fields `command_fingerprint`, `idempotency_key`, `causation_id` and `deadline_unix_ms` are always `None`; `timeout_ms` is never read.
  - `SharedCommandDispatcher` is stored but never called by `runtime_host.rs` ("skeleton (task 12)").
  - The real path is `CommandHost` (`LocalCommandHost`, `HttpCommandHost`), plus `impl CommandHost for LocalCommandDispatcher`, which builds a `LocalCommandHost` per call.
- Why it matters: public API that looks like the remote story but is inert; "which trait do I implement" has two answers.
- Change: keep `CommandHost` and its two impls only; delete `CommandDispatcher`, the remote dispatcher, envelope and receipt; `graphql/http.rs:832` takes a `LocalCommandHost` directly. Breaking, M.

### 7. Contract fingerprint is recomputed on every dispatch and per handler on every status poll
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/command/typed_command.rs:64-73`; callers `src/microsvc/service/routes.rs:1704`, `:1971`, `:2450`, `:2392`; status loops `routes.rs:2829-2867` and `src/microsvc/service/runtime.rs:962-984`
- What: `fingerprint_bytes()` calls `canonical_value()` each time. That clones roles, effects, defaults and projections, sorts with a serializing comparator, builds JSON, serializes and hashes. It runs once per command. On a status poll it runs once per causal handler in every route bundle, each followed by its own ledger lookup.
- Why it matters: avoidable allocation and SHA-256 on the hottest path, and status polling is O(routes × handlers) DB lookups for a command id that belongs to exactly one command.
- Change: compute the fingerprint once in `register_typed_handler`/bind and store `[u8; 32]` on the registered handler. For status, look the row up once by `(service, principal partition, command id)` and match its stored contract hash to a handler. S-M.

### 8. Nothing compacts the command ledger
- Category: E/D   Severity: medium   Confidence: CONFIRMED
- Where: `src/command_ledger/traits.rs:63-68` (`#[allow(dead_code)] fn compact_expired_commands`)
- What: every backend implements it, but the only non-delegating call in the repo is the conformance test at `src/command_ledger/tests.rs:1186`. Rows live for 30 days by policy, then forever.
- Why it matters: one row per command ever issued, unbounded, on the table every command reads and writes.
- Change: run compaction from the existing framework worker (next to the outbox drain) with a bounded batch, or drop the method and document operator-owned retention. S-M.

### 9. Attempts leak as `InProgress` on `?` after reservation
- Category: E   Severity: medium   Confidence: CONFIRMED
- Where: `src/microsvc/service/routes.rs:1873-1877`, `:1884`, `:2299`
- What: after the reservation is acquired, these `.map_err(...)?` returns exit without `abandon_causal_attempt`, unlike the ~15 sibling sites.
- Why it matters: the row stays `InProgress` until the 30-second lease expires, so a client retry with the same command id gets `InProgress` instead of an immediate re-run.
- Change: falls out of finding 1 (one abandon-on-error guard). S.

### 10. Version conflicts on typed commands surface as a 500; the `locks` argument is unused on this path
- Category: E/F   Severity: medium   Confidence: CONFIRMED (code path) / PLAUSIBLE (client-visible impact)
- Where: `src/microsvc/service/routes.rs:2306-2341`; `src/microsvc/service/causal.rs:900-950`; `src/queued_repo/repository.rs:303-312`
- What: causal loads "deliberately bypass this wrapper's queue locks". A `ConcurrentWrite` from `commit_causal_batch` goes to `recover_causal_commit_error`, which marks the row retryable and returns `CausalDispatchError::Internal(detail)`. There is no in-process reload-and-retry, although `SagaRunner` does exactly that (`src/saga/runner.rs:108-118`). `Routes::for_aggregate(repo, locks, read_models)` still requires a lock manager that typed commands never use.
- Why it matters: two concurrent commands on one aggregate give one success and one INTERNAL. Whether the JS client retries that transparently is what I did not check. The `locks` parameter is dead configuration for apps with only typed commands.
- Change: retry handler and commit a bounded number of times on `ConcurrentWrite` inside the same attempt (the handler is a pure function of loaded state), and surface a distinct retryable code otherwise. Drop `locks` from `for_aggregate` once finding 3 removes legacy handlers. M.

### 11. Per-command JSON round-trips, clones and a post-commit ledger re-read
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/command/input.rs:68`, `:116`; `src/microsvc/service/routes.rs:2014`, `:1736`, `:2251`, `:2319-2329`, `:1896-1904`; `src/microsvc/service/runtime.rs:636-642`, `:719-722`, `:1045-1076`, `:1237`
- What, per command:
  - input is canonicalized (BTreeMap per object), serialized for the digest, cloned to decode, then serialized again for the `Message` payload;
  - outbox causation is stamped three times;
  - after a successful commit the ledger row is re-read and its payload compared to the value just written (an extra DB round trip to detect a condition the transaction already guarantees);
  - `seal_wait_path_dispatch` calls `typed_command_contracts()`, which clones every contract in the service;
  - name lookup happens twice (service index, then route bundle).
- Why it matters: this is the command hot path; the re-read alone adds a query per command.
- Change: serialize canonical input once and reuse the bytes for digest and message; stamp causation only in `CausalCommitBatch::build`; build the result from the in-memory completion and drop the re-read; index contracts by name at registration. M.

### 12. Lifecycle write gate is an ambient env-var flag that does blocking file I/O per dispatch
- Category: A/D   Severity: medium   Confidence: CONFIRMED
- Where: `src/microsvc/mod.rs:153-158`; `src/microsvc/lifecycle.rs:78-103`; call sites `src/microsvc/service/runtime.rs:363`, `:748`, `:887`, `:1051`, `:1142` plus `src/graphql/http.rs:384,410,441` and `src/graphql/engine/request.rs:38,125`
- What: when `DISTRIBUTED_LIFECYCLE_DIR` is set, each check does `symlink_metadata`, opens and reads `dev.json` (up to 1 MiB), parses it and reads three more env vars. This is synchronous, inside async dispatch, and happens more than once per mutation (GraphQL layer, then `Service`).
- Why it matters: process behaviour changes through an environment variable no composition code mentions (Principle 6, and README 07's "you do not set a runtime role flag"). It also blocks the executor.
- Change: resolve membership once into an explicit `Service::with_lifecycle(LifecycleGate)` (file watcher or cached atomic) and check it at one layer. M.

### 13. `HttpCommandHost` reports every failure as a rejection and every status as unknown
- Category: E   Severity: medium   Confidence: CONFIRMED
- Where: `src/command_dispatch/host.rs:665-676`, `:680-689`
- What: any `status >= 400`, including 5xx, becomes `CausalDispatchError::Rejected { code: "REJECTED", .. }`. `status()` validates the session and returns `CausalCommandPublicStatus::unknown(command_id)` unconditionally.
- Why it matters: behaviour differs silently between local and HTTP hosts (Principle 7). A transient remote failure looks like a business rejection, and status polling can never resolve for remotely hosted commands.
- Change: map 5xx to `Internal`/retryable, carry the remote error code through the wire body, and implement status against the remote (or remove `status` from the trait and serve it from the ledger owner only). M.

### 14. Dead terminals and dead placeholders in the commit builder
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `src/microsvc/service/handlers.rs:458-477`, `:496-521`, `:523-541`; `src/command/outcomes.rs` (`finalize_after_commit`); `src/microsvc/service/routes.rs:2319`
- What: the `atomic` impls for `DirectReadModelProjection<M>` and `ProjectionDescriptor<DirectCandidate>` are unreachable, because no builder method produces those type parameters. `DirectReadModelProjection`/`direct_read_model` are public with zero users repo-wide. `NoDirectProjection::atomic` is documented as "legacy / registry path ... remains for compatibility". `finalize_after_commit` returns `(K, Value)` and both callers bind `_committed`, so the typed outcome value is never consumed after commit.
- Change: delete the two unreachable impls and `direct_read_model`; keep one `atomic` terminal; make `finalize_after_commit` return just the `Value`. Breaking for the public exports, S.

### 15. `routes.rs` (2992 lines) does about seven jobs
- Category: F/C   Severity: medium   Confidence: CONFIRMED
- Where: `src/microsvc/service/routes.rs`; also `src/microsvc/service/runtime.rs:754-759`, `:784-789`, `:803-808`, `:893-898`, `:1005-1010`, `:414-468`
- What: the file contains:
  - handler spec types;
  - erased traits;
  - bus publisher and outbox wiring (the 7-feature `cfg(any(...))` list appears three times at 478-588 and again in `src/microsvc/runtime.rs:95-104`);
  - four builders;
  - registration;
  - the three pipelines;
  - external reservations;
  - `ErasedRoutes for Routes`, with six copy-pasted "find causal handler by name else BadRequest `is not a typed causal command`" blocks (2761-2953).
  
  Further duplication: `ThinEventualHandler` and `ThinSucceededHandler` differ in one expression (590-1043); `CommandMountRegistrar for Routes` (1604-1655) duplicates `Service::register_command_mount_inner`; the route-index lookup block is repeated five times in `runtime.rs`.
- Change: split into `spec.rs`, `builder.rs`, `registry.rs`, `execute.rs` (the single pipeline) and `outbox_wiring.rs`. Add one `fn causal_handler(&self, name) -> Result<&dyn ErasedCausalHandler, _>`, one thin handler generic over the terminal, and one `cfg` alias for the 7-feature list (or drop the gate, since `test` is already in it). M.

### 16. Typed command execution is compiled only with the `graphql` feature
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `src/microsvc/service/causal.rs` (every item individually `#[cfg(feature = "graphql")]`); `src/microsvc/causal.rs:1` (`#![cfg_attr(not(feature = "graphql"), allow(dead_code))]`); `src/microsvc/wait_path.rs`
- What: the ledgered pipeline, HTTP/gRPC wait path and `CommandHost` exist only when the GraphQL engine is enabled. Without it a service has only legacy handlers.
- Why it matters: the write path's safety properties depend on a read-API feature flag, and per-item cfg makes the file hard to audit.
- Change: make typed dispatch unconditional (it does not need async-graphql; the protocol accumulator can be an optional parameter) and gate only the GraphQL transport. M.

### 17. Two files named `causal.rs` with unrelated responsibilities; `Atomic` means two things
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `src/microsvc/causal.rs` (workspace/unit-of-work staging) vs `src/microsvc/service/causal.rs` (dispatch results, status, ledger recovery); `src/microsvc/service/causal.rs:1343-1356`
- What: there is no logic overlap between the two files, only a shared name. `collapse_projection_evidence` returns `CausalCommandPublicState::Atomic` when all Eventual obligations are observed, the same name as the same-transaction terminal state. `causal_handler_error_code` (694-703) restates `CausalDispatchError::code` (61-68).
- Change: rename to `workspace.rs` and `dispatch_result.rs`/`status.rs`; name the collapsed public state for what it is (e.g. `Projected`). Breaking wire name, S.

### 18. `CausalRepository` and `get`/`load` duplicate the context API
- Category: C/F   Severity: low   Confidence: CONFIRMED
- Where: `src/microsvc/service/handlers.rs` (`CausalRepository` delegating `get/create/commit/publish_events/publish/read_models/readmodel/outbox`; `get` and `load` identical at 610-626); `src/microsvc/dependencies.rs:372-470` (`RepoDependencies`, `ReadModelStoreDependencies`, `RepoReadModelDependencies`)
- What: two facades for the same operations in handlers, and three dependency wrapper structs each re-implementing `HasRepo`/`CausalRouteDependencies`.
- Change: keep the context methods and drop `CausalRepository` and one of `get`/`load`; consider one `Dependencies<R, S = ()>`. Breaking, S-M.

### 19. Panics and `eprintln!` on runtime and registration paths
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `src/microsvc/service/routes.rs:881`, `:910`, `:972`, `:1033`, `:1276-1353`, `:2299` (`unreachable!`); `src/microsvc/service/runtime.rs` (`validate_projector_registrations`); `src/microsvc/workers.rs:65-73`; `src/microsvc/http.rs`, `grpc.rs:200,221`, `knative_ingress.rs:88`
- What:
  - `.expect("invoke must be chained...")` and `.expect("load_by")` fire at dispatch time for states the type-state builder already rules out.
  - `register_modeled_projection` panics on misconfiguration, although `try_with_graphql` shows a fallible style exists.
  - Server faults are logged with `eprintln!` (7 sites in `src/microsvc`).
  - `spawn_service_consumer_loop` rebuilds the whole service every 100 ms forever on error, with no backoff.
  - `spawn_outbox_publish_loop` discards the drain handle.
- Change: encode the thin-builder state in types so the `Option`s disappear; return `Result` from registration; use the crate's logging/metrics; add exponential backoff. S-M.

### 20. Tests asserting removed behaviour or internals, and heavy duplicated setup
- Category: B   Severity: medium   Confidence: CONFIRMED (outline plus sampled bodies)
- Where: `src/microsvc/service/tests.rs`, `src/command_ledger/tests.rs`, `tests/legacy_authoring_absence.rs`
- What:
  - `causal_dispatch_replay_contains_resolved_projection_obligation` (`service/tests.rs:2367`) now asserts `replay.projection_obligations.is_empty()`: the name says the opposite, and it tests a removed feature.
  - `durable_success_states_use_only_the_succeeded_vocabulary` (`command_ledger/tests.rs:2207`) asserts `parse("accepted")` fails, i.e. the absence of an old name.
  - `cell_ledger_external_binding_round_trips_and_legacy_rows_stay_unbound` (`:1668`) pins a compat fallback for rows "written before external dispatch existed", pre-release.
  - `completion_rejects_*projection_obligation*` and `replay_rejects_*projection_obligation*` (`:1383-1530`) test the vestigial path from finding 4.
  - `in_memory_adapter_reclaims_and_replays_with_a_stable_causation` (`:2018`), `concurrent_in_memory_reservations_have_one_winner_and_one_cause` (`:2082`) and `sqlite_adapter_enforces_attempt_fence_and_replays` (`:2109`) repeat the generic conformance functions at 878, 694 and 1051 for one backend each.
  - `tests/legacy_authoring_absence.rs` is an `include_str!` source-text gate that cannot detect the legacy things still present (findings 3-6).
  - Roughly ten tests reach through `Service::lookup_causal_command`, which is `#[allow(dead_code)]` in production (`runtime.rs:992`) and kept alive only by them.
  - `service/tests.rs` has 1,100 lines of setup before the first test: four near-identical `*_program/_resolve/_lower/_inventory` quads (195-466) and a 300-line `AmbiguousCommitRepository` (809-1120) hand-delegating about 25 trait methods to inject one commit fault.
- Change: delete the removed-behaviour tests with finding 4; fold the per-backend duplicates into `run_command_ledger_adapter_conformance`; replace `lookup_causal_command` assertions with the public status API; add a fault-injection hook on `InMemoryRepository` instead of the wrapper; parameterize the projection program fixture. M.

### 21. Missing behavioural tests
- Category: B   Severity: medium   Confidence: PLAUSIBLE (searched by test name in my slice only, not `tests/`)
- What I found no test for:
  - two concurrent typed commands on the same aggregate (finding 10);
  - an error after reservation leaving the row re-runnable immediately (finding 9);
  - `HttpCommandHost` 5xx mapping and status (finding 13);
  - ledger compaction running in a live service (finding 8);
  - a saga participant implemented as a typed command — `tests/sagas/microsvc_saga.rs:60-63` uses `routes!` legacy handlers.
- Change: add these as backend-generic scenarios next to the ledger conformance suite. M.

### 22. Docs and comments that disagree with code
- Category: F   Severity: low   Confidence: CONFIRMED
- Where and what:
  - README principle 07 ("You do not set a runtime role flag") vs `DISTRIBUTED_LIFECYCLE_DIR` (finding 12) and the `graphql` feature deciding whether typed commands exist (finding 16).
  - `docs/coordination-primitives.md:226-231` tells participants to dedupe on the saga command id themselves; nothing connects that id to the command ledger (finding 3).
  - `src/microsvc/runtime.rs:19-23` says a crash hands the row "to a separately operated polling worker", while `:39-42` says the in-process worker polls and a separate drainer is optional.
  - `src/microsvc/service/handlers.rs:160-164`, `:335`, `:465`, `:510` still describe the removed `project(PROJECTION)` and `.projected`.
  - `src/command_dispatch/*` and `src/application/runtime_host.rs` comments cite "task 12"/"task 20".
- Change: fix alongside the corresponding findings. S.

Answers to slice questions not covered above:
- **Sagas** are built from the same primitives. `Saga<D>` is an ordinary event-sourced aggregate whose transitions return outbox messages (command plus `deliver_at` timer), committed in one batch (`src/saga/runner.rs:120-136`), with deterministic command ids, a processed-id set and a version-fence retry. The only structural problem is finding 3.
- **`command/` vs `command_dispatch/`**: the boundary is real (contract types vs transport hosts).
- **Declaration surfaces**: there are seven — `#[command(id=..)]` on a fn, `#[command(..)]` inside `#[sourced]`, `portable_command!` (including the `handle:` escape hatch), the thin builder `.load_by/.invoke/.eventual`, `typed_command(..).handle`, legacy `.command(name).handle`, and `routes!`. Examples use only the first two; the last two are the vestigial ones. `.load_by(` appears four times in the whole repo (two in `src`, one in `tests`, one in the CLI/macros crates), so the thin builder is probably just the macro's expansion target and could be made non-public — I did not read the expansion to confirm.

## Things that are good and should not be changed
- Single-transaction `CausalCommitBatch` (events, outbox, read-model plans, snapshots, schedules, fenced ledger completion and optional direct projection) with the reservation as a separate short transaction and an attempt fence (`src/sqlx_repo/repo/commit.rs:242-253`).
- The ledger state machine lives once in `command_ledger/record.rs` and is shared by all backends, verified by one generic conformance suite (`run_command_ledger_adapter_conformance`) run against in-memory, SQLite and Postgres.
- Canonical input hashing plus contract fingerprint for detecting command-id reuse, and principal-partitioned ledger keys.
- Capability-restricted `CausalCommandContext` with an owned `AggregateCheckout`: handlers stage work and the framework commits, which keeps handlers thin.
- The saga design: a pure `SagaDefinition`, an event-sourced state machine, timers as delayed outbox rows, and timeout treated as "maybe applied, so compensate".
