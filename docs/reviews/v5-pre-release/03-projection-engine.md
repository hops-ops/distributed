## Assessment

The slice works and is carefully fail-closed, but it does not have one canonical IR. `MutationProgram` (authoring) is rewritten into `ProjectionProgram` (execution), which is resolved, lowered to `TableWritePlan`, then re-associated and re-normalised in the executor. The browser consumes yet another artifact (`ProjectionDelta` / command-projection metadata, owned by `src/graphql/projection_delta`). The protocol store is one trait, but the commit algorithm is implemented twice (memory, sqlx) with copy-pasted pure logic and at least one behavioural divergence.

The three things most worth changing:
1. **Build the program once.** The `projection!` descriptor recompiles the whole program on every event and leaks memory each time.
2. **Collapse `mutation/` and `projection/` program types into one IR.** About a third of `mutation/` is test-only scaffolding.
3. **Lift the commit state machine above the store trait.** Then replace the two 3k-line store test files with one conformance suite, following the pattern `projection/source_snapshot_tests.rs` already uses.

Not read, or only outlined:
- **Outline only:** `projection_protocol/codec/*`, `store/query.rs`, `store/identity.rs`, `store/replay.rs`, `store/commit.rs`, `projection_protocol.rs` (lines 1–110 read), `projection_protocol/workspace.rs` (struct only), `projection/provenance.rs`, `projection/error.rs`, `mutation/error.rs`.
- **Partly read:** `projection/placement.rs` (outline only), `projection/catalog.rs` (589–735 only), `projection/expression.rs` (outline only), `projection/executor.rs` tests, `projection/rebuild.rs` 1–195.
- **In-memory store:** `state.rs`, `read_helpers.rs`, `direct_projection.rs`, `util.rs` outline only; `store_impl.rs` only `commit_projection`; `state_impl.rs` only `next_record` / `validate_physical_record`.
- **Tests:** in-memory `tests.rs` and sqlx `tests.rs` by test names plus one paired body; `projection/tests.rs`, `mutation/tests.rs`, `relationship_fixture.rs`, `store/tests.rs`, `codec/tests.rs` by names only (except the mutation golden test).
- **sqlx (comparison only):** `commit_projection`, `next_record`, `failure_matches_batch`.

## Findings

### 1. `projection!` recompiles the whole program on every event and leaks memory each time
- Category: D/E   Severity: high   Confidence: CONFIRMED
- Where: `src/lib.rs:231-238` (`__resolve` calls `__handlers()?`), `src/lib.rs:196-230`, `src/mutation/descriptor.rs:85` and `:155` (`Box::leak`), `src/projection/lower.rs:529-535`, callers `src/microsvc/projector/runtime.rs:211`, `src/microsvc/causal.rs:782`, `src/projection/rebuild.rs:422`
- What: `ProjectionServerExecutorDescriptor::plan` calls the macro-generated `__resolve`, which calls `__handlers()` per occurrence. That re-runs `$mutation()` (the `mutation_file!` expansion builds a `MutationProgram` via `try_new`), and then for every event type in every arm it:
  - clones the program and builds body bindings from `M::schema()`;
  - leaks a string: `let mount_id = Box::leak(descriptor.name.replace('.', "-").into_boxed_str());`
  - rewrites the mutation into a `ProjectionArm` and re-validates it.
- Why it matters: the e2e `TODOS` projection (8 event types, `tests/e2e-ui/crates/projections/src/todos.rs:32-57`) leaks 8 strings and performs 8 rewrites plus full validation per projected event. A long-running projector's memory grows without bound. A 100k-event rebuild does this 100k times.
- Change: have `ProjectionDescriptor` hold a `OnceLock`/`LazyLock` compiled program (program, id, selector index), built once at registration. `resolve` takes `&CompiledProgram`. Make `arm_id` an owned `String` or `&'static str` from `stringify!`, and delete both `Box::leak` calls. Breaking: `ProjectionResolver` fn-pointer type and `descriptor_from_factories` signature. Size M.

### 2. Two parallel program IRs, joined by a per-field rewrite
- Category: A/C   Severity: high   Confidence: CONFIRMED
- Where: `src/mutation/program.rs:276-426` vs `src/projection/program.rs:375-493`; `src/mutation/expression.rs:51-73` vs `src/projection/expression.rs:359`; `src/mutation/program.rs:558-616`; `src/mutation/bind.rs:146-229`
- What: the two operation structs carry the same fields and near-identical `try_new` validation. The kind enums are 9 vs 8 variants. Expression enums differ only in `InputPath` vs `BodyPath`/`Envelope`. Static-ambiguity checks, ordinal validators, id digest/parse and canonical JSON are all duplicated. They are bridged by `rewrite_to_projection`, driven by an `InputPathBinder` with three fallback lookup strategies.
- Why it matters: Principle 5 says one program. Today the mutation program is what authors write and hash (`mp1:`), while the projection program is what executes and is pinned in the catalog (`ProjectionProgramId`). Validation rules already differ: the projection side requires complete-row key fields to use the exact key expression (`program.rs:447-467`); the mutation side does not, so a valid mutation program can fail only at rewrite.
- Change: one IR. A program is `{partition, arms: [{selector, input bindings, operations}]}` with a single expression type that has an `Input(path)` leaf resolved through the arm's bindings at evaluation time. Delete `rewrite_to_projection*`, `InputPathBinder`, and one of the two program modules. Breaking: public `Mutation*` / `Projection*` type surface and the two fixture files. Size L.

### 3. The binder detects a body path by serialising the expression to JSON
- Category: A/D   Severity: medium   Confidence: CONFIRMED
- Where: `src/mutation/bind.rs:215-229`
- What: `body_path_segments` calls `serde_json::to_value(expression)` and inspects `value["kind"] == "body_path"`, with the comment "ProjectionExpression does not expose body path publicly beyond as_ref which is crate-private". `mutation` is in the same crate, so `ProjectionExpressionRef` is reachable.
- Why it matters: this runs once per field per event type per event (via finding 1) and silently returns `None` if the serde tag is ever renamed.
- Change: goes away with finding 2. Otherwise match on `expression.as_ref()`. Size S.

### 4. About a third of `mutation/` has no production caller
- Category: F/C   Severity: high   Confidence: CONFIRMED (rg across `src`, `tests`, `examples`, `distributed_cli`, `distributed_game`, `distributed_macros`, `js`)
- Where and what:
  - `src/mutation/server.rs`: `MutationServerInterpreter` is used only by `preview.rs`; `rewrite_program_with_binder` has no users; `simple_body_bindings` is tests only.
  - `src/mutation/preview.rs`: `compose_event_preview`, `causal_scopes`, `zero_binding_preview` are tests only; `reconcile_with_actual`, `rewrite_binding_ops`, `ComposedPreviewLayer`, `MutationCausalScope` have no users.
  - `src/mutation/handler.rs`: `MutationHandlerCatalog` / `MutationHandlerRegistration` / `MutationHandlerPlacement` are tests only; `CustomMutationHandler`, `bindings_from_expressions`, `portable_binding` have no users.
  - `src/mutation/cache.rs`: `lower_mutation_cache` is reached only through `preview.rs` and tests; `lower_projection_ops_cache` has no users; `is_cache_writable` and `presence_label` ("Placeholder for shared golden-vector evaluation") are tests only.
  - `src/mutation/capabilities.rs`: `ReadModelMutationCapabilities` is tests only; no macro emits it.
  - Also unused: `MutationOperation::state_upsert` (`program.rs:434-500`), `MutationReturning`, `identity_body_path_binder`, `required_input_paths`, `assert_mutation_backed_program`, and `MutationExpression::resolve` (`expression.rs:259-342`).
  - Keep-alive stubs: `fn _projection_program_id_type` (`handler.rs:386`), `fn _use_mutation_expression` (`bind.rs:283`).
  - Empty branches left in `MutationOperation::try_new` (`program.rs:372-382`).
- Why it matters: this is a second writer-conflict catalog (`handler.rs:229-292`) and a second placement enum, shadowing the real ones in `projection/catalog.rs:669` and `placement.rs:40`. It is a second interpreter surface re-exported from the crate root (`src/lib.rs:127-141`).
- Change: delete `server.rs`, `preview.rs`, `handler.rs`, `cache.rs`, `capabilities.rs` and the listed functions with their tests. The cache/preview pair needs the owner's call (finding 5). Size M, mostly deletion.

### 5. `lower_mutation_cache` has a hand-written TypeScript mirror, and neither side is used by a runtime
- Category: A/C   Severity: medium   Confidence: CONFIRMED (JS usage checked only in `js/src` and `tests/e2e-ui/ui/src`)
- Where: `src/mutation/cache.rs:81-215`, `js/src/replica/mutation-cache.ts:4` ("Mirrors `distributed::mutation::cache::lower_mutation_cache`"), export at `js/src/replica/index.ts:251`, test `js/tests/mutation-cache-lowering.test.mjs:22`
- What: Rust and TS each implement the visibility-to-effect lowering. The shared fixture is only the input program (`tests/fixtures/mutation-program-v1.json`); expected effects are asserted separately on each side. The replica's real path is `projection-delta/` plus `command-projection-metadata-v1.json`.
- Why it matters: this is exactly the "TypeScript twin" Principle 5 forbids, and it is not load-bearing.
- Change: delete both. If role-safe cache lowering is wanted, emit it server-side into the client manifest so TS only interprets data. For the TS reviewer, the Rust definitions the replica must mirror today are: the `ProjectionProgram` wire form (`projection/program.rs`), `ProjectionExpression`/`ProjectionValue` (`projection/expression.rs`), `ProjectionScalarTransform`, key/partition canonical bytes (`projection/canonical.rs`), and whatever `graphql/projection_delta` derives from them. Shared fixtures exist for program bytes and delta (`tests/fixtures/projection-program-v1.json`, `projection-delta-v1.json`). I found no shared fixture for resolved plans (input occurrence → expected `ResolvedProjectionPlan` bytes), which is the fixture that would prove one set of semantics. Size S.

### 6. The projection commit state machine is implemented twice, including an 85-line byte-identical function
- Category: C/A   Severity: high   Confidence: CONFIRMED
- Where: `src/in_memory_repo/projection_protocol/store_impl.rs:105-435` vs `src/sqlx_repo/projection_protocol/store_impl.rs:333-647`; `next_record` at `src/in_memory_repo/projection_protocol/state_impl.rs:558-644` vs `src/sqlx_repo/projection_protocol/writes.rs:298-383`; `failure_matches_batch` wrapper at `writes.rs:1449`; `InputDisposition` declared in `in_memory_repo/.../state.rs:290`, `sqlx_repo/.../types.rs:65` and `store/identity.rs:183`
- What: both stores hand-code the same sequence: classify input, handle pending retry and redelivery, compute next revision per mutation, validate the snapshot write, allocate a change, resolve observation targets, emit a fallback checkpoint change, write receipt/identity/inbox, apply retention. Only `change_kind_for_mutation`, `checked_next`, `validate_snapshot_write` and `failure_matches_batch` are shared (`store/backend_helpers.rs`, 242 lines). `record_projection_failure`, `repair_projection` and the rebuild commit are duplicated in the same style.
- Why it matters: Principle 7. Every protocol rule must be changed in two places, and they have already drifted (finding 7).
- Change: move the algorithm above the trait as one `fn plan_commit(batch, &impl CommitState) -> CommitEffects`, where the store supplies a small transactional read/write primitive set (load record, load physical-exists, load observation, classify input, apply effects). `next_record`, physical-row validation and observation resolution live once in `projection_protocol/store/`. Breaking: `ProjectionProtocolStore` shrinks to primitives. Size L.

### 7. Backends disagree on observing a tombstoned existing record
- Category: E   Severity: medium   Confidence: CONFIRMED (by reading both; not executed)
- Where: `src/sqlx_repo/projection_protocol/store_impl.rs:506-510` vs `src/in_memory_repo/projection_protocol/store_impl.rs:288-303`
- What: for `ProjectionObservationTarget::ExistingRecord`, sqlx returns `RecordTombstoned` when `metadata.tombstone && metadata.source_snapshot.is_none()`. The in-memory path checks only the revision and accepts the observation. `rg RecordTombstoned src/in_memory_repo` finds no hit in the observation path.
- Why it matters: a command confirmation against a deleted row succeeds on the memory backend and fails on SQLite/Postgres, so tests on memory cannot catch it.
- Change: falls out of finding 6. Until then, add the check to memory and a conformance scenario. Size S.

### 8. In-memory store clones the entire protocol state and inbox on every commit
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/in_memory_repo/projection_protocol/store_impl.rs:174-175` (commit), `:532-533` (failure), `:44`, `:1218`; `rebuild.rs` `protocol.clone()` + `rows.clone()`
- What: `let mut staged_protocol = protocol.clone(); let mut staged_inbox = inbox.clone();` copies every record, change, input, receipt and observation of every partition per projected event, to get rollback.
- Why it matters: cost per event is O(total state), so a test or dev run is O(n²). The in-memory repo backs most of the test suite and local dev.
- Change: stage a delta (touched keys only, as already done for rows at `:192-197`) and merge on success. Size M; simpler after finding 6.

### 9. Per-event resolve/lower does avoidable serialisation and whole-plan clones
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/projection/plan.rs:414` (`program.id()?`), `plan.rs:441`, `src/projection/lower.rs:1117`, `plan.rs:631` / `src/projection/canonical.rs:13-21`, `src/projection/lower.rs:1523-1538`, `src/projection/executor.rs:211-221`, `executor.rs:124`
- What, per event after finding 1 is fixed:
  - the program id is recomputed (serialise whole program to `Value`, sort, re-serialise, SHA-256);
  - the occurrence is cloned into the plan, then the whole plan is cloned again in `finish_lowering`;
  - each key and partition goes through `to_value` → sort → `to_vec`;
  - numbers are stored as decimal strings in `ProjectionValue` and parsed back per field when lowering to `RowValue`;
  - the executor re-derives which logical mutation each physical mutation came from with an O(P×L) key-equality scan that recomputes `resolved_row_key` per pair;
  - `workspace.clone()` per stage.
- Change: cache the id in the compiled program. Have `LoweredProjectionPlan` own the resolved plan (lower by value) and hold the occurrence by `Arc` or reference. Have lowering emit `(logical index, TableMutation)` pairs so the executor does not re-match. Size M.

### 10. Too many intermediate representations between declaration and store
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `src/projection/executor.rs:27-36` (`LifecycleIntent`), `executor.rs:11` (`ProjectionMutationKind as LogicalMutationKind`), `src/projection_protocol/store/identity.rs:236`, `src/projection/program.rs:173`, `src/mutation/program.rs:218`; `executor.rs:611-670`; `src/projection/program.rs:79` vs `src/projection_protocol.rs:365`; `src/projection/plan.rs:88` vs `src/projection_protocol.rs:96`
- What:
  - Kind is represented four times: `MutationKind` (9), `projection::ProjectionMutationKind` (8), `LifecycleIntent` (6), `projection_protocol::ProjectionMutationKind` (3). Two different enums share one name, hence the import alias.
  - There are two unrelated `ProjectionPartition` types and two `ResolvedProjectionKey` types.
  - Lowering picks a `RowWriteMode`/`ExpectedVersion`, then the executor's `normalize_create/save/patch/delete` overwrites that choice from the snapshot. Rebuild overwrites it a third time (`rebuild.rs:430-438`).
- Change: lower straight from `ResolvedProjectionMutation` plus snapshot to `ProjectionRecordMutation` in one step, dropping the intermediate `TableWritePlan` and the normalise pass. Rename the protocol enum (e.g. `RecordTransition { Upsert, Delete, Recreate }`) and the protocol partition (e.g. `PartitionId`). Size M–L.

### 11. Store trait has test-only methods and fail-closed defaults that `QueuedRepository` falls through
- Category: E/F   Severity: high (the default trap) / medium (dead surface)   Confidence: CONFIRMED for missing forwarding and caller counts; PLAUSIBLE for production impact
- Where: `src/projection_protocol/store/trait.rs:99-128` (defaults), `src/queued_repo/repository.rs:360-545`, caller `src/microsvc/projector/context.rs:109`, `src/projection_protocol/store/mod.rs:9-13`
- What:
  - The trait has 22 methods. `projection_execution_snapshot_batch` and `projection_graph_snapshot` default to `Err("adapter does not support …")`. `QueuedRepository` forwards 18 methods but not those two, so a projector on a `QueuedRepository` gets the error on every stage that needs a snapshot read. I did not confirm that any service mounts a projector on one.
  - No non-test caller (besides `queued_repo` delegation) for: `projection_checkpoint`, `projection_query_snapshot_batch`, `projection_graph_snapshot`, `projection_live_record_batch`, `projection_observation`, `compact_projection_changes`, `projection_failure`.
  - The module carries a blanket `#![allow(dead_code)]` with the comment "the store trait + query vocabulary is wider than the production call graph".
- Why it matters: defaults "for older adapters" are a pre-release compat shim. They turn a compile error into a runtime failure on one backend wrapper. The graph-snapshot path alone is roughly 300 lines across `in_memory_repo/.../read_helpers.rs:24-175` and `sqlx_repo/.../reads.rs:1101-1305`, plus `workspace.rs:277` and `backend_helpers.rs:54-165`.
- Change: remove every default body so each implementor must implement or forward. Delete the methods with no production caller, with their request/response types and tests. Drop the blanket allow. Size M.

### 12. Source-snapshot fencing is threaded through every layer as flags
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `src/projection/program.rs:646-649` and `:708-743`; `src/projection/plan.rs:372-373`, `:390-400`; `src/projection/executor.rs:42`, `:137-156`, `:468-515`; `next_record(…, source_snapshot: bool)` in both stores; `src/sqlx_repo/projection_protocol/store_impl.rs:506`; `src/projection_protocol/source_snapshot.rs:14-23`
- What: the policy is two bools on the program, a bool on the plan, an `Option` on each execution mutation, a parallel `stage_source_snapshot` beside `stage_for_snapshot`, and a bool parameter that relaxes delete-of-tombstone in `next_record`. `SourceSnapshotVersion` has an `external: bool` that reuses `aggregate_type`/`aggregate_id` for external producers by writing `format!("external:{:x}", …)` into `aggregate_id`. The illegal state `external_source_snapshots && !source_snapshots` is representable.
- Why it matters: this is the "special cases threaded with flags" pattern. The delete-tombstone relaxation lives inside the generic revision function.
- Change: one enum on the program, `SourceOrdering { Position, AggregateSnapshot, ExternalSnapshot }`, matching the macro's `source:` keyword. `SourceSnapshotVersion` becomes an enum over aggregate/external identities. Model staging as a strategy chosen once from the enum, not per-mutation `Option` checks. The store then sees an explicit transition (idempotent tombstone refresh), not a bool. Size M.

### 13. Rebuild is not orthogonal: snapshot-only, unit-partition-only, and depends on GraphQL surface types
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `src/projection/rebuild.rs:4` (`use crate::graphql::SurfaceProjector`), `:279-305`, `src/projection/mod.rs:25-26` (`#[cfg(feature = "graphql")] pub mod rebuild`), `src/projection_protocol/store/trait.rs:18-41`, `rebuild.rs:276` (`#[allow(private_bounds)]`), `src/projection/local_mounts.rs:9`
- What: `SnapshotProjectionRebuild::begin` takes a `SurfaceProjector`, requires exactly one modeled binding, a local route and `source_snapshots()`. The two trait methods exist only under the `graphql` feature and default to "does not support". The public API is bounded by a `pub(crate)` trait. Delta/patch projections have no rebuild path.
- Why it matters: core projection maintenance depends on the API layer and on a feature flag. "Rebuild" covers only one fencing policy.
- Change: `begin` takes `(descriptor/executor, compiled topology, epoch)`, with the surface layer adapting. Remove the cfg and the defaults. Either name the type for what it is (snapshot reconciliation) or generalise it. Size M.

### 14. Rebuild coverage check is O(streams × history)
- Category: D   Severity: low   Confidence: CONFIRMED
- Where: `src/projection/rebuild.rs:491-511`
- What: for each relevant stream that has a coverage witness, the code filters the whole `history` slice again. Limits are 10,000 streams and 100,000 events (`:12-13`).
- Change: group event ids by stream in the first pass (the loop at `:382` already builds `sequences` per stream). Size S.

### 15. IR surface that authoring cannot reach
- Category: F   Severity: medium   Confidence: CONFIRMED for macro output; PLAUSIBLE that nothing else needs these
- Where: `distributed_macros/src/mutation.rs:729,792,839,907`; `src/mutation/program.rs:218-237`; `src/projection/lower.rs:1025-1032`, `:1045-1103`; `src/lib.rs:225`
- What:
  - The mutation macro emits only `Upsert`, `Insert`, `Delete`, `Patch`.
  - `projection!` always lowers with `lower_single_model`, which rejects `InsertRelated`/`UpsertRelated`.
  - `projection!` hard-codes `ProjectionPartition::Unit`. Expression partitions are reachable only through the `program:` escape hatch, and in-repo use is tests only (`tests/typed_commands/main.rs:413`, `tests/domain_event_projection_server.rs:179`).
  - `Invalidate`, `Recreate`, `UpsertPatch`, `MutationReturning`, relationship effects and `lower_related_mutation` are used only by tests and `tests/domain_event_projection_server.rs`.
- Change: decide per feature whether it ships. Where it does not, remove the variant, its validation, lowering, executor arms and tests. Size M.

### 16. `lower.rs` holds four jobs; half of it is the superseded authoring path
- Category: F   Severity: medium   Confidence: CONFIRMED (usage via rg)
- Where: `src/projection/lower.rs`
- What and seams:
  - (a) Derive-facing metadata traits and const asserts, `:30-245`. `assert_state_upsert_compatible` and `assert_explicit_field_compatible` are referenced only in this file.
  - (b) `ProjectionDescriptor` / executor descriptor plus a process-global `PLACEMENT_SELECTED_DIRECT` registry, `:247-591`.
  - (c) The pre-mutation authoring builders, `:674-980`, `:1182-1336`, `:1617-1660`: `state_upsert_operation`, `model_operation`, `related_operation`, `ProjectionAuthoringField`, `state_selector`, `deletion_selector`, `event_selector`, `resolve_typed`, `projection_program`, `output_relationship`. Their only users are `tests/domain_event_projection_server.rs`, plus `state_selector` in two tests. `ProjectionPlanTemplate` / `ProjectionEventSet` have the same users plus `command/`.
  - (d) The actual resolved-to-row lowering, `:982-1119`, `:1338-1609`.
- Also: the registry at `:543-564` is a global `RwLock<BTreeMap>` keyed by model name, silently replaces entries "so tests can rebuild services", and swallows a poisoned lock. This is hidden global state against Principle 6; the owning microsvc reviewer should confirm.
- Change: delete (c) with its test file. Split into `metadata.rs`, `descriptor.rs`, `lower.rs`. Move the registry into the Service. Size M.

### 17. Other oversized files: where the seams are
- Category: F   Severity: low   Confidence: CONFIRMED (structure)
- What:
  - `projection/catalog.rs` (2,125 lines) is about 830 lines of code and 1,290 of in-file tests. Move tests to `catalog/tests.rs`. `ProjectionCompatibilityPins` / `compatibility_pins` (`:303-395`) are referenced only inside this file.
  - `projection/placement.rs` (1,523) mixes wire-schema value types (`:40-575`), binding identity/materialisation (`:577-1167`) and the direct-eligibility proof (`:1357-1460`). Split into `binding.rs`, `schema.rs`, `direct.rs`.
  - `projection/executor.rs` (1,682) is about 795 lines of code and 885 of tests. `prepare_graph_projection` (`:253-279`) is test-only by its own attribute and should go.
  - `projection_protocol.rs` (1,649) is about 1,040 lines of identity/cursor newtypes with hand-written serde, plus 610 of tests. Move the newtypes to `projection_protocol/identity.rs`.
  - Small helpers are copied: `hex_nibble` ×4 (`program.rs:987`, `placement.rs:1485`, `mutation/program.rs:963`, `replay.rs:419`), `program_id_serde` ×2 (`catalog.rs:810`, `placement.rs:1501`), `body_kind_rank` ×2 (`placement.rs:1493`, `provenance.rs:328`), `sort_value`/`canonical_json_bytes` ×4 (`projection/canonical.rs`, `mutation/canonical.rs`, `projection_protocol/codec/catalog.rs:24`, `domain_event/canonical.rs:21`), `ProjectionValue → JSON` ×4 (`lower.rs:1559`, `executor.rs:353`, `mutation/expression.rs:480`, `graphql/projection_delta/runtime.rs:1493`).
- Change: one `canonical` module, one `hex` helper, one `ProjectionValue::to_json`. Size S–M.

### 18. `MutationExpression::resolve` is a second evaluator with different semantics
- Category: E/C   Severity: low (test-only today)   Confidence: CONFIRMED
- Where: `src/mutation/expression.rs:259-342`, `:480-488`
- What: it evaluates `Transform` as an error ("transform evaluation requires projection rewrite"), ignores the declared `value_type` (`let _ = value_type;`), and its JSON conversion turns I64/U64/F64 into JSON strings, where `lower.rs:1565-1588` emits numbers.
- Why it matters: a public method on the "canonical" IR gives different answers from the real evaluator (`projection/expression.rs:618`).
- Change: delete it (part of finding 4). Size S.

### 19. Store tests are duplicated per backend instead of one conformance suite
- Category: B/C   Severity: high   Confidence: CONFIRMED (names for all; body for one pair)
- Where: `src/in_memory_repo/projection_protocol/tests.rs` (3,787 lines, 34 tests), `src/sqlx_repo/projection_protocol/tests.rs` (3,236, 23 tests), `src/sqlx_repo/projection_protocol/postgres_tests.rs` (1,876, 5 tests), `src/projection_protocol/store/scenario_tests.rs` (631, 6 shared scenarios)
- What: a shared `ProjectionProtocolScenario` trait exists and both backends call its 6 scenarios. The rest is copy-paste. `query_snapshot_never_mixes_row_revision_checkpoint_or_resume_head` (memory `:924`) and `sqlite_query_snapshot_never_mixes_…` (sqlx `:671`) are line-for-line identical except the loop bound (64 vs 32). Other pairs by name:
  - `four_position_in_memory_…` / `four_position_sqlite_…` / `four_position_postgres_projection_commit_failure_matrix_rolls_back_every_side_effect`
  - `coherent_execution_and_graph_snapshots_use_fk_references_and_unique_scope_budgets` / `sqlite_coherent_…` / `postgres_coherent_…`
  - `physical_rows_must_match_projection_record_metadata` / `sqlite_record_metadata_is_fenced_against_physical_row_drift`
  - `table_failure_rolls_back_rows_protocol_and_inbox` / `sqlite_row_failure_rolls_back_protocol_receipt_inbox_and_domain_row`
  - `lengthening_retention_never_restores_a_compacted_prefix` / `sqlite_retention_prunes_exact_prefix_and_never_restores_it`
  - `registration_is_global_and_rejects_unowned_rows_atomically` / `sqlite_registration_rejects_legacy_rows_and_cross_topology_table_owners`
- Coverage holes by name: memory-only `concurrent_same_input_applies_once`, `observations_are_immutable_and_staged_records_reuse_row_changes`, `repair_generation_retries_only_the_exact_failed_input`; sqlite-only `sqlite_non_gap_repair_requires_failed_cursor_before_later_input`. The divergence in finding 7 has no scenario.
- Postgres tests return early when `DISTRIBUTED_TEST_POSTGRES_URL` is unset (`postgres_tests.rs:32,56,645,756,1017`), so they report as passed without running.
- Change: move every backend-agnostic scenario into `scenario_tests.rs`, generic over the store, as `source_snapshot_tests.rs:246` (`async fn matrix(store: &impl ProjectionProtocolStore)`) already does. Instantiate per backend with a macro. Keep per-backend files only for backend-specific behaviour (restart durability, SQL snapshot isolation, cancelled `begin`). Make Postgres `#[ignore]` or fail loudly when not configured. Expect roughly 5k fewer test lines. Size L.

### 20. Low-value or unable-to-fail tests in the mutation module
- Category: B   Severity: medium   Confidence: CONFIRMED for the first bullet; the rest PLAUSIBLE from names and the code they cover
- Where: `src/mutation/tests.rs`, `src/mutation/relationship_fixture.rs`, `tests/legacy_authoring_absence.rs`
- What:
  - `golden_fixture_round_trip_matches_canonical_program` (`tests.rs:595-615`) writes the fixture into the source tree if it is missing and then compares the program to itself. It also compares parsed JSON, not bytes; the projection twin `canonical_program_matches_frozen_vector` (`projection/tests.rs:476`) compares bytes.
  - `presence_labels_cover_lattice`, `read_model_capabilities_are_internal_metadata` and `preview_composition_zero_binding_and_multi_owner` (asserts `!zero_binding_preview().has_optimism()` on `Default`) test helpers that exist only for tests.
  - `portable_handler_catalog_rejects_duplicate_owner_event_epoch` and all five tests in `relationship_fixture.rs` exercise the unused `MutationHandlerCatalog`, preview and cache code.
  - `mutation_program_contains_no_event_selector_fields` asserts absence of struct fields via JSON.
  - `tests/legacy_authoring_absence.rs` (outside my slice) greps `src/lib.rs` text for substrings.
- Missing behavioural tests: no test that `projection!` resolve does not rebuild or leak per event (finding 1); no Rust/TS shared vector for occurrence → resolved plan (finding 5).
- Change: delete with finding 4. Make the golden test byte-exact and never self-writing. Size S.

### 21. `DirectCandidate` is described as compiler-generated but is typed by the author
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `src/projection/lower.rs:247-259`, `:344-349`; `src/lib.rs:179` (`$desc_ty:ty`)
- What: the docs say "The macro emits this marker only for the exact full-state upsert shorthand". In `projection!` the author writes the descriptor type by hand, so the marker is an opt-in and `.direct()` revalidation does the real work.
- Change: drop the type parameter and expose `descriptor.direct()` returning `Result`, or have the macro compute it. Fix the docs either way. Size S.

### 22. Stringly-typed errors and stale wording in the public mount API
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `src/projection/local_mounts.rs:114-248` (every method returns `Result<_, String>`, `#![allow(missing_docs)]`); `src/projection/lower.rs:413`, `:422`, `:1615` ("task-7"); `src/mutation/mod.rs:8-9` ("The old event-owning projection proc-macro authoring path is gone"); `src/projection/mod.rs:15-17` ("empty seams intentionally expose no … contract before their owning tasks define one" above modules that are fully populated); `src/mutation/cache.rs` header (a sentence that stops mid-phrase: "During the this module rewrites…")
- Change: add a typed `ProjectionMountError` and scrub task-history comments. Size S.

## Things that are good and should not be changed

- Canonical, domain-separated, length-prefixed digests for program/key/partition identity, with frozen byte fixtures (`projection/canonical.rs`, `tests/fixtures/projection-program-v1.json`). Keep the design; just deduplicate the code.
- The executor's separation of store-free preflight (`prepare_portable_projection`) from snapshot-driven lifecycle selection (`PreparedProjectionExecution::stage`), with exact scope-matched (not positional) snapshot validation and atomic staging into the workspace.
- `validate_snapshot_write` as a last-line check at the commit boundary, shared by both stores (`projection_protocol/source_snapshot.rs:105`). The fencing semantics are sound; only the flag plumbing needs reshaping.
- `projection/source_snapshot_tests.rs`: one `matrix(store)` / `rebuild_matrix(store)` run against memory, SQLite and Postgres. This is the template for the conformance suite in finding 19.
- Catalog/activation separation (`ProjectionCatalog` vs `ActiveProjectionBindings`): program identity independent of epoch/route, with writer-overlap and same-epoch-takeover rejected at construction. It is explicit topology composition as Principle 6 intends.
