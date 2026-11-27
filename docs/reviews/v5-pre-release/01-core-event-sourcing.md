## Assessment

The core is in better shape than its size suggests: `CommitBatch` is one atomic unit on every backend, validation is shared, snapshots are a cache with a schema gate, and the outbox claim/settle model is sound. The weak part is the layer above it, where several generations of API coexist (queued/locking repo, four commit assemblers, two immediate-publish modes, two timer mechanisms).

Nothing here was executed (the brief forbids builds), so "CONFIRMED" means confirmed by reading the cited lines.

Three things most worth changing:
1. Delete `QueuedRepository` and with it `lock/`; it self-deadlocks on a stale snapshot, leaks locks, and the service path already bypasses it (findings 1-3).
2. Collapse the four commit paths into one, with the domain-event guard inside the shared validation (finding 4).
3. Give outbox retries a backoff and classify failures; today a broker outage sends the oldest rows terminal in seconds (finding 5).

Not read:
- **Non-test code:** `src/entity/upcaster.rs` (signatures only), `src/emitter/local_event.rs`, `src/lease/in_memory.rs`, `src/lease/sqlx_store.rs` 200+, `src/lock/postgres_lock.rs` and `src/lock/sqlite_lock.rs` (constants only), `src/repository/sqlite_codec.rs`, `src/repository/sql/ledger.rs` 60+, `src/repository/error.rs` 120+, `src/repository/sql.rs` and `src/repository/sql/outbox.rs` tails beyond what is cited, `src/outbox_worker/testing.rs`.
- **In-module test bodies:** I listed every test fn name in the slice but read bodies only for `entity/entity.rs` 706-760, `entity/event_record.rs` 274-330, `outbox/table.rs` tests, `repository/sql.rs` 386-440, one test in `outbox_worker/store/tests.rs`, the head of the `in_memory_repo/repository.rs` tests, and `snapshot/repository.rs` and `queued_repo` tests. The test-quality question is therefore answered from names plus samples.
- **Out of slice:** `in_memory_repo/projection_protocol/`.

All paths below are under `/Users/patrickleet/dev/hops-ops/hops/distributed/`.

## Findings

### 1. `.queued()` + `.with_snapshots()` self-deadlocks on a stale snapshot
- Category: E   Severity: high   Confidence: CONFIRMED (by reading; not executed)
- Where: `src/snapshot/repository.rs:339` and `:357`; `src/queued_repo/repository.rs:142-154`, `:169-183`; `src/lock/in_memory.rs:9-13`; `src/lock/sqlx_common.rs:57-65`
- What: Snapshot `get` calls `(reads.tail)`, which on a queued repo takes and holds the per-stream lock. On `SnapshotHydrationError::Cache` (for example after a `SNAPSHOT_VERSION` bump) it calls `(reads.full)`, which is `QueuedRepository::get_stream` and awaits the same lock again. `InMemoryLock` is non-reentrant and ownerless, and the SQLx lock's in-process gate is the same type with `max_wait` defaulting to `None`.
- Why it matters: The first `get()` of any aggregate whose snapshot schema changed hangs forever. That is exactly the scenario the cache-miss fallback exists for. The only queued+snapshot integration test is the happy path (`tests/snapshots/main.rs:290`); the stale case is tested only through causal reads (`causal_snapshots_use_only_tail_through_queued_wrappers`, `src/snapshot/repository.rs:455`).
- Change: Remove `QueuedRepository` (finding 3). If it stays, the lock must be taken once by the caller and returned as a guard, not by each read primitive. Size S if deleted, M otherwise.

### 2. Queue locks leak and are released by non-owners
- Category: E   Severity: high   Confidence: CONFIRMED
- Where: `src/queued_repo/repository.rs:146-153`, `:267-295`; `src/lock/in_memory.rs:76-88`; `README.md:1229-1231`
- What: `get_stream` keeps the lock held after returning, including for `Ok(None)`. The README example returns `Err(NotFound)` without `abort`, which leaks the in-memory lock permanently (no TTL). `commit_batch` calls `ensure_lock(...).unlock()` for every committed stream whether or not this caller acquired it, and `unlock_core` has no owner check.
- Why it matters: A commit of a fresh or `peek`ed aggregate releases another task's lock, so the serialization guarantee is not real. Any early return leaks.
- Change: Same as finding 1. If per-stream serialization is wanted, make it an RAII guard returned by a single `checkout` call. Size M.

### 3. `QueuedRepository` is a decorator that must forward every backend trait, and the service path already bypasses it
- Category: A/C/F   Severity: high   Confidence: CONFIRMED
- Where: `src/queued_repo/repository.rs` (854 lines; forwarding at `:315`, `:360-543`, `:839`; "legacy" comments at `:310`, `:782-788`; blanket `impl<T> Queueable for T` at `:765`); `src/microsvc/service/defaults.rs:47`
- What: About 450 lines forward `CommandLedgerStore`, the `Causal*` traits, `ProjectionProtocolStore`, `SnapshotStore`, the read-model stores, `InboxStore`, and `microsvc::BackendHost`/`CausalHostProjections`. Core therefore imports `crate::microsvc` and `crate::projection_protocol`. The causal (service) path deliberately skips the locks (`:199-207`, `:303-312`) and the comments call the locking path legacy, yet `.queued()` is the documented default. `GetWithOpts`, `GetAllWithOpts`, `ReadOpts`, `UnlockableRepository` and `Queueable` exist only for it.
- Why it matters: This is the "wrapper type carrying one optimization" the owner does not want, and each new backend capability needs another forwarding block. Optimistic concurrency already fences writers (`tests/queued_repo/main.rs:255` asserts exactly that).
- Change (breaking): Delete `queued_repo/`, `lock/` (about 1,400 lines; its only consumer per `src/lock/mod.rs:3-4`) and the five helper traits. Move `RetryClass` out of `lock/error.rs` into the repository error module. Size M.

### 4. Four hand-assembled commit paths with different guarantees
- Category: A/C/E   Severity: high   Confidence: CONFIRMED
- Where: `src/aggregate/repository.rs:316-366`; `src/commit_builder/mod.rs:53-57`, `:79-87`, `:170-188`; `src/outbox/commit.rs:233-311`; `src/microsvc/causal.rs:639`, `:838`; `src/repository/validation.rs:29-52`
- What: `AggregateRepository::commit*`, `CommitBuilder::commit_all`, `AggregateCommit::commit` and the causal workspace each build a `CommitBatch`. They differ in three ways:
  - **Domain-event guard:** only the last two call `domain_event_commit_guard` / `pending_domain_events_for_commit` / `mark_domain_events_committed`. Plain `repo.commit(&mut agg)` with captured domain events persists the replay events, publishes nothing, and leaves the occurrences pending. `AggregateCommit` instead errors with "add `publish_events()`" (`outbox/commit.rs:307-311`).
  - **Outbox source stamping:** `CommitBuilder` overwrites unconditionally; `AggregateCommit` stamps only when all three fields are `None` (`:238-245`).
  - **Snapshots and immediate publish:** `CommitBuilder` hard-codes `snapshots: Vec::new()`, so the snapshot policy is silently skipped, and it never triggers immediate publish.
- Why it matters: The same aggregate gets different durability and publication semantics depending on which builder the handler used. The silent drop of domain events breaks principle 1.
- Change (breaking): Keep one commit builder (the `AggregateCommit` shape) and make `repo.commit(&mut a)` its zero-option form. Delete `commit_builder/`. Move the poison/pending-occurrence check into `validate_commit_batch` so no path can skip it. Size M-L.

### 5. Outbox retry has no backoff and rows go terminal within seconds
- Category: E   Severity: high   Confidence: CONFIRMED
- Where: `src/outbox_worker/store/api.rs:200-215`; `src/repository/sql/outbox.rs:231-246`; `src/outbox/message.rs:571-582`; `src/outbox_worker/drain.rs:188-196`
- What: `record_failure` releases the row until `claim.attempt >= max_attempts`, then marks it `Failed`, regardless of error kind. Release makes the row immediately claimable (`next_available_at = now`). The drain loop sleeps `Duration::ZERO` whenever `claimed >= batch_size`. I found no API to requeue failed rows (rg for `requeue`/`retry_failed` in outbox, outbox_worker and repository returned nothing).
- Why it matters: With a broker down and a backlog of at least 32 rows, the oldest rows burn every attempt in a tight loop and become terminal while hot-spinning the database. The schedule worker has the opposite policy: exponential backoff and no ceiling (`src/schedule.rs:601-607`).
- Change: On release set `next_available_at = now + backoff(attempt)`. Do not count transient or unknown transport outcomes toward the ceiling. Add an explicit requeue for failed rows. Use one retry policy type for outbox and schedules. Size M.

### 6. Direct read-model writes from command commits
- Category: A   Severity: medium   Confidence: CONFIRMED
- Where: `src/outbox/commit.rs:368`; `src/commit_builder/mod.rs:250-263`; `src/repository/traits.rs:37`; `src/repository/error.rs:38`
- What: Handlers can attach `TableWritePlan`s to the aggregate commit. The code's own error doc calls this "a raw/legacy repository batch".
- Why it matters: It is a second write path into read models that bypasses domain event → projection (principles 1 and 2), and the browser replica cannot reproduce those rows (principle 5).
- Change (breaking): Remove `read_model_plans` from the public commit surface and keep it crate-private for the projector. Port the tests and bomberman handlers that use it. Size M.

### 7. SQL and in-memory disagree on tail load of a missing stream
- Category: A/E   Severity: medium   Confidence: CONFIRMED
- Where: `src/in_memory_repo/repository.rs:635-648`; `src/repository/sql.rs:309-321`; `src/snapshot/repository.rs:339-342`; `src/repository/traits.rs:124-127`
- What: In-memory returns `Ok(None)` for a missing stream and clamps the prefix to the true version. SQL `load_stream` with `Some(after)` and zero rows returns `Some(entity)` at version `after` ("Keep the existing SQL snapshot-only restore contract"). The snapshot code expects `None` and the trait doc promises clamping.
- Why it matters: On SQL a lingering or planted snapshot for a non-existent stream hydrates a live aggregate; in memory it does not (principle 7).
- Change: Make SQL read the stream version and return `None`/clamp. Add the case to the persistent conformance suite. Size S.

### 8. Snapshots make `get_with`, `peek`, `get_all` and `peek_all` slower
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/aggregate/repository.rs:279-308`, `:376-387`; `src/snapshot/repository.rs:257-305`; policy struct at `src/aggregate/repository.rs:40-106`
- What: Only `get` reads snapshot then tail. The other four load the full stream and then also fetch the snapshot(s).
- Why it matters: This is strictly more I/O than with snapshots off, which contradicts "optimizations are transparent". `SnapshotPolicy` also carries six function pointers to avoid trait bounds.
- Change: Route every read through the one snapshot-then-tail loader (batched with `get_snapshots` plus one tail query for `get_all`). Size M.

### 9. Core depends on microsvc, GraphQL, Durable Objects and game vocabulary
- Category: A   Severity: medium   Confidence: CONFIRMED
- Where: `src/schedule.rs:50`, `:369-370`, `:437-448`, `:550`; `src/schedule.rs:52-53` with `src/schedule/worker.rs:54-56`; `src/repository/traits.rs:44`; `src/in_memory_repo/repository.rs:171-237`; `src/repository/migrations.rs:16-22`; `src/lease/mod.rs:3-5`, `:53-57`; `src/entity/entity.rs:355`
- What:
  - `schedule.rs` uses `microsvc::Session`, `cell_host::CellCommandIdentity` and `CausalCommandContext`, and has a JS-safe-integer limit for Durable Object SQL.
  - `ScheduleWorker` is gated on the `graphql` feature because it takes `graphql::identity::VerifiedPrincipal`.
  - `CommitBatch` depends on `schedule::ScheduleWrite`, so repository ↔ microsvc is a cycle.
  - `InMemoryRepository` exposes `clone_*/replace_*` hooks for DO persistence.
  - `migrations.rs` hard-codes cell migration versions `1|2|4|8|11`.
  - `lease` docs talk about "boss controller" and "encounter settlement".
- Why it matters: A layering inversion. A plain HTTP service cannot run schedules without GraphQL, and the reference backend carries an adapter's persistence API.
- Change: Split schedule into a core data type plus store trait (no session or cell types) and a worker in microsvc gated on a runtime feature. Move `VerifiedPrincipal` out of `graphql`. Move the DO hooks and the migration subset into `cell_host`. Scrub the game wording. Size M.

### 10. The "is tokio present" cfg list is copied 17 times and is wrong
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `src/lib.rs:6-9`, `:399-408`; `src/outbox/commit.rs:62-90`; `src/outbox_worker/mod.rs`; `src/microsvc/service/routes.rs:543-570`; `Cargo.toml:60-63`
- What: `any(feature="http","grpc","postgres","sqlite","nats","rabbitmq","kafka", test)` appears 17 times in `src`. It omits `redis` and `leaderboard-background`, which also enable `dep:tokio`. There is also a crate-level `allow(dead_code)` when no graphql/sqlite/postgres feature is on, and 16 `cfg_attr(not(feature = "graphql"), allow(dead_code))` sites.
- Why it matters: A redis-only build silently gets the fallback publish path. The dead-code allows hide genuinely dead items.
- Change: Add one internal `runtime-tokio` feature implied by every tokio-using feature and gate on that. Fix the layering in finding 9 so the graphql allows can go. Size S-M.

### 11. Two immediate-publish mechanisms, and three docs that contradict each other
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `src/outbox/commit.rs:31-94`, `:227-230`, `:252-266`; `src/outbox_worker/publish_hook.rs` (header); `src/microsvc/service/routes.rs:543-570`; `README.md:1390-1394`; `src/outbox/mod.rs:6`
- What: The mailbox path (rows stay pending, drain is hinted) and the "born InFlight" pre-claim plus `OutboxPublishHook` path both exist. `Service::with_bus` always installs the mailbox when a tokio feature is on, so the hook is reachable only in no-feature builds and tests. The README documents born-InFlight, the `commit` doc says rows stay pending, and `outbox/mod.rs` references a nonexistent `OutboxCommit`.
- Change: Keep the mailbox path only. Delete `publish_hook.rs`, `BusOutboxPublishHook` and the pre-claim branch. Fix the README. Size S-M.

### 12. `outbox/table.rs` is a second outbox row encoding used only for throwaway validation
- Category: C/D   Severity: medium   Confidence: CONFIRMED
- Where: `src/outbox/table.rs:92`, `:118`, `:200-205`; `src/repository/validation.rs:70`; `src/repository/sql/outbox.rs:27-88`
- What: Every commit builds a `TableWritePlan` per outbox message (cloning the payload and serializing metadata to JSON) only to validate it, then discards it. The real insert is a hand-written 19-column statement that serializes metadata again. The two encodings differ: the table version truncates timestamps to whole seconds and always writes `published_at` as Null.
- Why it matters: A per-message payload clone on the command hot path, and two schemas to keep in sync.
- Change: Validate fields directly (lengths, ids), keep only the schema constant, and delete the plan/row helpers along with `outbox_insert_plan_uses_table_row_mutation`. Size S.

### 13. `lock/` and `lease/` are two TTL-lease implementations
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `src/lock/sqlx_common.rs:330-550` (`LockDialect`, `LeaseQueries`, `aggregate_locks`); `src/lease/sqlx_store.rs:1-200` (`LeaseDialect`, `coordination_leases`); `src/lease/mod.rs:27-35`
- What: Both are database-clock TTL rows with an owner token and parallel dialect traits. `lease` adds fencing and renewal; `lock` adds an in-process gate and deletes its row on release.
- Change: Falls out of finding 3 — delete `lock/` and keep `lease/` as the only coordination primitive. Size S after finding 3.

### 14. Two durable timer mechanisms
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `src/outbox/message.rs:396-416`; `src/saga/state.rs:709`; `src/schedule.rs`; `README.md:1377-1383`
- What: `OutboxMessage::deliver_at/deliver_after` (only production user is the saga) and `Schedule` + `aggregate_schedules` + `ScheduleWorker`. The README documents only the first.
- Change: Choose `Schedule` (it has generations and idempotent command ids), port the saga timeouts to it, and drop `deliver_at`. Size M.

### 15. `Aggregate::aggregate_type` defaults to `type_name::<Self>()`
- Category: E/F   Severity: medium   Confidence: CONFIRMED
- Where: `src/aggregate/aggregate.rs:12-17`
- What: The default is the Rust type path, kept "so existing examples keep working".
- Why it matters: Renaming or moving a struct silently changes its stream identity and orphans its history. It is also a compat fallback.
- Change (breaking): Make it a required associated const and have the macros always emit it. Size S.

### 16. Vestigial items on `Entity`, and the replay loop written twice
- Category: F/C   Severity: low-medium   Confidence: CONFIRMED
- Where: `src/entity/entity.rs:76`, `:102-135`, `:438-478`, `:496`, `:531-542`; `src/aggregate/aggregate.rs:125-138`; `src/snapshot/repository.rs:162-175`
- What:
  - `Entity.timestamp` is written but read only by Debug/Clone and a serde test.
  - `Entity::rehydrate` and `ReplayGuard` are used only by their own unit test; real replay uses `set_replaying` in two duplicated loops.
  - `digest` and `digest_v` have duplicate bodies.
  - The manual `Clone` impl equals the derive.
  - `InMemoryRepository` reimplements `SnapshotStore` (`src/in_memory_repo/repository.rs:930-992`) identically to `InMemorySnapshotStore` (`src/snapshot/in_memory.rs:38-95`) instead of delegating to its own `snapshot_store` field.
- Change: Delete the dead items, keep one `replay_into` with one guard, and delegate. Size S.

### 17. Dead or near-dead public API
- Category: F   Severity: low-medium   Confidence: CONFIRMED (rg over the whole repo excluding `target`)
- Where and what:
  - `InboxOutcome` — no user outside `src/repository/{inbox,mod}.rs` and `src/lib.rs`.
  - `InboxReceipt.processed_at` — no reader; documented as "forward compatibility" (`src/repository/inbox.rs:36-39`).
  - `AggregateRepository::commit_entities` (`src/aggregate/repository.rs:357`) and `repo_mut()` — no callers.
  - `impl_aggregate!` — no invocations outside its own recursion; `README.md:1295` only mentions it.
  - `PayloadCodec` trait — one implementation, and `EventRecord::decode` hard-codes bitcode (`src/entity/event_record.rs:207-225`).
  - `EventRecord::new`, `new_versioned` and `with_metadata` are three copies of one constructor (`:151-203`).
  - `emitter/` (`EntityEmitter`, `event-emitter-rs`) — used only by the enqueue macros and their tests (`tests/enqueue`, `tests/sourced_enqueue`, `tests/sourced_snapshot`). It is an in-process string callback bus beside the outbox.
- Change: Delete them. For `emitter`, either delete the feature or justify it against principle 1. Size S each.

### 18. Legacy "sourced" names and stale comments
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `src/outbox_worker/outbox_dispatch.rs:43` (`x-sourced-` prefix vs `x-distributed-*` in `src/schedule.rs:63-72`); `src/lib.rs:342` (`SourcedResult`); `src/repository/identity.rs:7-9`; `src/queued_repo/repository.rs:148`, `:272-275`; `src/outbox_worker/drain.rs:5`
- What: Two metadata prefixes are on the wire; comments refer to a removed sync API and an "old in-memory OutboxWorker". State-transition errors on `OutboxMessage` are typed `EventRecordError`.
- Change: Use one prefix (a wire break, so do it now), rename `SourcedResult`, and give outbox its own error. Size S.

### 19. `OutboxMessage` constructor and field sprawl; `Published` state is unreachable in stores
- Category: F   Severity: low-medium   Confidence: CONFIRMED for the constructor sprawl, PLAUSIBLE for `Published`
- Where: `src/outbox/message.rs:169-274`, `:373`, `:427`, `:559-569`; `src/outbox_worker/store/api.rs:159-164`; `src/repository/sql/outbox.rs:226`
- What: All fields are public, plus `#[non_exhaustive]`, plus getters. There are seven constructors plus public `new()`/`initialize`. Stores delete rows on complete, so `OutboxMessageStatus::Published`, `complete()`, `is_published()` and the `published_at` column look vestigial. They are still referenced at `src/microsvc/runtime.rs:179`, `src/microsvc/cell_host/wire.rs:272` and in several tests, which I did not trace.
- Change: Make fields private with two constructors (typed and raw) and builder setters. Confirm the two microsvc uses and then drop `Published`. Size M.

### 20. Per-commit overhead on the hot path
- Category: D   Severity: low-medium   Confidence: CONFIRMED
- Where: `src/repository/sql.rs:168-179`; `src/outbox_worker/outbox_dispatch.rs:225`, `:238`, `:298-315`; `src/repository/sql/outbox.rs:176-205`; `src/outbox_worker/store/in_memory.rs:96-99`
- What:
  - `SqlBind::Text(String)` allocates four strings per event row, although `PreparedEventAppend`'s doc claims nothing is cloned (only payloads are borrowed).
  - `backlog_stats()` runs COUNT/MIN after every dispatch when metrics are on ("deliberately unthrottled"), so once per command commit.
  - SQLite claim does one UPDATE plus one SELECT per id.
  - In-memory claim sorts every outbox id on each call.
- Change: Make `SqlBind::Text` a `Cow<str>`, throttle backlog gauges to the drain tick, and batch the SQLite claim with `RETURNING`. Size S-M.

### 21. Domain-event validation re-serializes on every call
- Category: D   Severity: low   Confidence: CONFIRMED
- Where: `src/domain_event/occurrence.rs:165`, `:382-389`, `:417-421`, `:425-482`; `src/domain_event/canonical.rs:21-28`
- What: `capture` builds the occurrence, then calls `canonical_bytes()`, which calls `validate()`. That re-parses the body to a `serde_json::Value`, re-canonicalizes it, and recomputes the SHA-256 id; the whole envelope is then serialized via `to_value` and the bytes are discarded. `from_canonical_bytes` validates twice (`:417` and again inside `:418`).
- Why it matters: Roughly three body encodings and two hashes per captured event, on bodies up to 1 MiB. Correct, but wasteful on the command path.
- Change: Check only the size at capture (the body was just produced canonically) and keep full validation for the parse path, once. Size S.

### 22. `eprintln!` logging and silent no-ops in library code
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `src/schedule/worker.rs:262`; `src/outbox_worker/drain.rs:177`, `:197`; `src/in_memory_repo/repository.rs:270-275`, `:880-887`
- What: Worker errors go to stderr although `tracing` is an optional dependency used elsewhere. In-memory `purge_inbox_older_than` is a no-op and `inbox_contains` maps lock poison to `false` (principle 7).
- Change: Route through one error hook or `tracing`. Give in-memory receipts a timestamp so purge works, and return the poison error. Size S.

### 23. Publish order is not preserved across failures
- Category: E   Severity: low   Confidence: PLAUSIBLE
- Where: `src/outbox_worker/outbox_dispatch.rs:184-191`, `:371-398`
- What: With concurrency 1 a failed row is released while later rows of the same aggregate in the same batch still publish. The doc promises "strictly in claim order".
- To check: Whether consumers rely on per-aggregate order or only on the sequence in the occurrence. If order matters, stop the batch at the first failure per aggregate. Size S.

### 24. In-module tests: mostly behavioural, with a cluster that cannot fail or duplicates the conformance suite
- Category: B   Severity: medium   Confidence: CONFIRMED for bodies read, PLAUSIBLE where judged from names
- Where and what:
  - **Good, keep:** `src/domain_event/mod.rs` and `derived_tests.rs` (for example `replay_suppresses_outward_capture_without_poisoning_the_entity`, `canonical_fixture_round_trips_and_fingerprints_exact_wire_bytes`), `src/outbox/commit.rs` (`captured_occurrences_require_an_explicit_publication_leg`), and `src/outbox_worker/outbox_dispatch.rs` and `drain.rs`.
  - **Cannot fail / restate derives (read):** `entity/entity.rs` `new`, `clone`, `debug`, `serialize_deserialize`; `entity/event_record.rs` `new`, `clone`, `debug`, `payload_bytes`, `with_metadata_constructor`; `in_memory_repo/repository.rs` `new` (asserts a private map is empty); `entity::rehydrate` tests a function nothing else calls.
  - **Same category (names only):** the six `snapshot/in_memory.rs` tests.
  - **Implementation detail (read):** `outbox/table.rs::outbox_insert_plan_uses_table_row_mutation`; `repository/sql.rs::event_plan_borrows_only_new_payloads_and_respects_bind_limit` (matches on `Cow::Borrowed` inside statement parts; acceptable as a performance guard, but it is the only test of the statement builder). `repository/sql/outbox.rs::insert_plans_bound_rows_and_borrow_payloads` looks similar by name.
  - **Misleading name (read):** `outbox_worker/store/tests.rs::already_published_message_is_not_completed_again` actually asserts `NotFound` because the row was deleted.
  - **Duplicated setup (names):** the 16 in-memory tests in `outbox_worker/store/tests.rs` (`claim_*`, `competing_workers_only_claim_message_once`, `stale_*`, `complete_many_*`) cover the contract that `tests/persistent_repository_conformance/outbox.rs` runs against SQL. This is one contract with two suites. The 15 `commit_builder/mod.rs` tests disappear with finding 4.
  - **Missing behavioural tests:** queued+snapshots with a stale snapshot (finding 1); plain `repo.commit` with captured domain events (finding 4); outbox failure with a full backlog (finding 5); tail load of a missing stream with a lingering snapshot on SQL (finding 7); `get_all` under a snapshot policy (finding 8).
- Change: Delete the derive tests. Run the outbox store tests as a conformance suite generic over `OutboxStore`, including in-memory. Add the five missing cases. Size M.

### 25. `lib.rs` is a pile of re-exports
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `src/lib.rs:49`, `:111-141`, `:175-316`, `:352`, `:447-491`; `src/aggregate/repository.rs:24-30`; `src/commit_builder/mod.rs:257`
- What:
  - About 35 `pub use` blocks re-export hundreds of names at the root, while the modules are also `pub mod`, so everything has two public paths.
  - A 140-line `projection!` macro and `graphql_models!` live in the crate root.
  - `pub use microsvc::cell_host` sits at the root.
  - There are two unrelated `aggregate` methods: the blanket `AggregateBuilder::aggregate::<A>()` on every `T`, and `ReadModelWritePlanCommitExt::aggregate(&mut A)`.
- Why it matters: The public surface cannot be held in one's head, and every internal rename is a breaking change twice.
- Change (breaking): Make modules private by default and export a curated root plus a `prelude`. Move the macros into their modules. Remove the blanket impls on `T`. Size M.

## Answers to the slice questions
- **Repository contract:** The trait family in `src/repository/traits.rs` is coherent, and `AggregateRepository::with_snapshots` keeps one type. The overlap sits above it: `QueuedRepository`, `CommitBuilder`, `AggregateCommit`, and the crate-private `Causal*` parallels.
- **Core awareness of adapters, GraphQL or the game:** Yes; see finding 9.
- **Outbox atomicity:** Append, outbox rows, schedules, snapshots and inbox receipts commit in one transaction on every backend, so there is no dual-write window. Publish is after-commit and at-least-once. Polling and batching are bounded (drain 32 rows/1 s, hint mailbox 256, schedules 32/250 ms). The problem is retry policy (finding 5).
- **`lock/` vs `lease/`:** They overlap (finding 13).
- **`lib.rs`:** A pile (findings 10 and 25).
- **Tests:** Mostly behaviour, with the exceptions in finding 24.

## Things that are good and should not be changed
- `CommitBatch` as the single atomic unit, with `validate_commit_batch` as the shared preamble for all backends (`src/repository/validation.rs`), and the structural `Statement`/`SqlExecutor` shared by SQLx and cell SQL (`src/repository/sql.rs`).
- Snapshot-as-cache semantics: the schema-version gate, cache miss falling back to full replay, and the policy living on the same `AggregateRepository` type (`src/snapshot/repository.rs:50-93`).
- The outbox claim lease with attempt-scoped `OutboxClaimRef`, one `publish_and_settle` path shared by drain and immediate dispatch, and delete-on-complete.
- Domain-event occurrences: canonical bytes, content-independent retry-stable ids, poison-on-capture-failure, and replay suppression (`src/domain_event/`).
- `lease/` fencing tokens (`Fence::admit`) on the database clock, and schedule generation / command-id idempotency.
