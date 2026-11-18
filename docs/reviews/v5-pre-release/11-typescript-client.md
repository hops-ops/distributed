## Assessment: TypeScript client (`js/`)

Health is mixed. The causal protocol layer is careful and well tested: fences on every async boundary, three-valued filter/order evaluation that falls back to revalidation, bounded parsers, clean layering (core never imports a binding), and almost no `any` or `ts-ignore`. The engine underneath it is still the "executable spike", though. Every cache transaction deep-clones and re-sorts the whole cache several times, nothing in the package ever calls `gc()`, and a dropped WebSocket is never reopened automatically. Structurally, the big modules were split mechanically ("body-split") rather than along real seams.

The three things most worth changing:
1. **Cache engine cost model.** Replace the O(whole cache) × ~6 per-transaction work with structural sharing and an undo log, and wire eviction (findings 1, 3, 4).
2. **Live transport.** Add automatic reconnect with resume cursors, and multiplex subscriptions over one socket (finding 2).
3. **Rust↔TS contract.** There is no golden protocol-envelope fixture, the query-plan corpus has 4 cases, a dead TS twin of a Rust function is exported publicly, and the same state format is parsed by three hand-written parsers (findings 6–9).

Not read, or only skimmed:
- **`impl.ts`:** lines 1760–1975 and 2294–3060 (membership fences, shared-index disposition, record-evidence resolution) were not read.
- **`command-runtime/create.ts`:** lines 1–430 and 650–1190 were only outlined.
- **`projection-delta/validate.ts` and `commands/validate.ts`:** outline plus spot reads only.
- **`replica/identity/codec.ts`:** first 110 lines only.
- **Not opened:** `normalize.ts`, `materialize.ts`, `selection.ts`, `revalidation.ts`, `persistence/outbox.ts`, `sveltekit/lifecycle.ts`, `boundary-lifecycle.ts`, `boundary-variables.ts`, `server-replica.ts`, `leaderboard.ts`, `sveltekit/game/*`, and all of `game/*` beyond imports and `codec.ts` head.
- **`sveltekit/vite.ts`:** outline plus lines 1655–1806 only; `islands/boundaries.ts` outline only.
- **Tests:** sampled, not read end to end (`replica-protocol`, `replica-command-runtime`, `sveltekit-vite`, `cache-engine-conformance`, the corpus and bridge tests). Type-tests were sized only.

Race correctness inside `#writeProtocolResult` was not audited beyond reading lines 1133–1732 once. Nothing was executed.

### 1. Every cache transaction is O(entire cache) several times over
- Category: D   Severity: high   Confidence: CONFIRMED
- Where: `js/src/internal/cache-engine/engine.ts:1603-1632` (`#transaction`), `:1659-1672` (`#backup`), `:973-986` and `:1001-1004` (`#reconcileDerivedIndexes` → `extract()`), `:357-416` (`extract`), `:701-742` (`#materialize`), `:536-548` (`#reader`), `:1634-1643` (`#flushWatchers`)
- What:
  - `#transaction` starts every outermost transaction with `structuredClone` of all records, indexes and layers. This covers each live frame, HTTP result, optimistic layer create/replace/accept/confirm/reject, and `markIndexStale`.
  - Each transaction then calls `#reconcileDerivedIndexes`, which runs `this.extract()` (a full deep clone that sorts every record key and field name with `localeCompare`) plus two full `#materialize()` copies.
  - Layer operations add two more `#materialize()` calls (before/after).
  - Each affected watcher re-runs `#select`, which calls `#reader` and therefore `#materialize()` again, then `deepEqual`s the whole result.
  - The code says so itself at `:540-543`: "V1 executable-spike tradeoff: materializing the visible graph is O(records + indexes + overlay operations) per selector."
  - On top of that, the reconciler in `impl.ts:2229-2268` copies every record again (`index-maintenance/engine.ts:298-324`) and instantiates all plans against all indexes.
- Why it matters: Replica apply and live delivery are the hot path for a realtime framework. Cost per delta grows linearly with session cache size, with a large constant, and the cache is never evicted (finding 3).
- Change: Keep the `CacheEngine` interface and replace the internals. Use persistent/structurally-shared maps (rollback becomes keeping the old root), maintain the visible graph incrementally per touched key, and hand the reconciler a changed-keys view instead of `extract()`. Drop `localeCompare` for code-unit compare. Size L. Add a scaling test (10k records, 50 watchers, 100 deltas).

### 2. A dropped live WebSocket is never reopened; one socket per live query
- Category: E/D   Severity: high   Confidence: CONFIRMED
- Where: `js/src/replica/distributed-replica/impl-fetch-live.ts:347-362` (error handler), `:363-366` (complete), `:507-548` (`failLive`, the only path with a retry timer); `js/src/websocket.ts:123` (`new WebSocketImpl` per `subscribe`), `:124` (`operationId = '1'`), `:201-209` (onerror/onclose); `js/src/replica/graphql-transport.ts:237-245`; test `terminal live errors close their stream and allow a later HTTP retry` at `js/tests/replica-protocol.test.mjs:2762`
- What:
  - A transport error deletes the live entry, sets `state.live = 'error'` and stops. No timer, no HTTP fallback.
  - The following `complete` is ignored because the entry is gone, and the transport wrapper has already closed.
  - The stream reopens only if some unrelated result ingestion calls `resumeLiveWatches()` or the app calls `watch.refresh()`. The test codifies the manual refresh.
  - Backoff exists only for server failure frames. The game client has its own reconnect with backoff (`game/connection.ts:236-288`); the main client has none.
  - `subscribe()` opens a dedicated socket with a hard-coded operation id, so N live queries means N sockets and N `connection_init` handshakes.
  - No `online` or `visibilitychange` handling exists in any binding (rg found none).
- Why it matters: After a server restart, network blip or laptop sleep, live queries silently stop updating until something else happens to write.
- Change: Make the live transport a single multiplexed connection that reconnects with jittered backoff and re-subscribes with `resumeCursors`. Treat transport error like `failLive` (inactive sentinel plus retry timer) and do an HTTP refresh on reconnect. Share the backoff/scheduler helper with `game/connection.ts`. Breaking: `subscribe()` signature and connection ownership. Size M.

### 3. Nothing evicts: `gc()` has no caller and causal maps only clear on auth purge
- Category: D/E   Severity: high   Confidence: CONFIRMED
- Where: `js/src/replica/distributed-replica/impl.ts:2017-2029` (`gc`), `:217-219` (`#recordClocks`, `#recordKeysByScope`, `#projectedRecordFences`), `:212`, `:215-216`; clears only at `impl-protocol.ts:195-210` and `impl-hydration-orchestrate.ts:526-539`
- What:
  - `rg "\.gc\(\)"` across `js/src`, `tests/e2e-ui/ui/src` and `examples` finds only the definition. No binding, timer or watch teardown calls it.
  - `gc()` returns the collected keys, but the replica does not use them to drop `#recordClocks` or `#recordKeysByScope` entries.
  - `#queryStates`, `#operationProtocols` and `#operationGenerations` gain an entry per (operation, variables) and are never deleted when a watch is destroyed.
  - Only `#anonymousRecordClocks` is bounded (`MAX_ANONYMOUS_RECORD_CLOCKS`).
- Why it matters: A long-lived SPA session with paging or search grows without bound, and finding 1 makes every delta pay for that growth.
- Change: Tie retention to watches. On last-watch destroy (after a grace period), drop the operation's protocol state and run an incremental collect that also prunes record clocks for collected keys. Remove public `retainRecord`/`releaseRecord`/`gc` unless a binding uses them. Size M.

### 4. `replica.read()` registers plans that are only released by `dehydrate()`
- Category: E/D   Severity: medium   Confidence: CONFIRMED
- Where: `impl.ts:872-884` (`read` → `#rememberRenderedOperation(..., 'read')`), `:2189-2190`, `:2199-2211` (`#forgetRenderedWatch` skips disposal when `#readOperationKeys.has(key)`), `impl-hydration-orchestrate.ts:223-237` (only place `readOperationKeys` is cleared); `js/src/react/query.ts:66-72`
- What: `read()` adds the key to `#readOperationKeys` and registers an index-maintenance plan. That set is cleared only by `finishDehydration`, which is an SSR path. React's `getSnapshot` calls `replica.read()` before `subscribe`, so every (operation, variables) a React app renders keeps its `#renderedOperations` entry and plan after unmount.
- Why it matters: It is a leak, and every leaked plan is re-evaluated on every reconcile.
- Change: Make `read()` side-effect free (no registration). Have the SSR collector register explicitly via a scoped handle the server disposes. Size S–M.

### 5. Command runtime keeps one canonical delta string per command forever
- Category: E   Severity: medium   Confidence: CONFIRMED
- Where: `js/src/replica/command-runtime/create.ts:482-483`, `:638-644`
- What: `projectionReplays.set(commandId, canonical)` and `projectionRevalidations.set(...)` have no `.delete` or `.clear` anywhere (rg confirms), including in dispose.
- Why it matters: Memory grows per eventual-consistency command for the lifetime of the runtime.
- Change: Delete both entries when the layer retires or rejects, where `pending` and `unmanagedLayers` are already cleaned. Size S.

### 6. No shared golden for the response protocol envelope
- Category: C/B   Severity: high   Confidence: CONFIRMED
- Where: `js/src/protocol.ts` (1038 lines; limits at `:252-261`), Rust `src/graphql/protocol/types.rs`, `src/graphql/query_protocol.rs:559` (`MAX_PROTOCOL_EVIDENCE_ITEMS = 4_096`), `src/graphql/compile/evidence.rs:11`; `js/src/replica/distributed-replica/constants.ts:5-6`
- What:
  - `extensions.distributed` (snapshot, live, command, trustedPresets) is hand-parsed in TS against shapes Rust serde defines.
  - Shared fixtures exist for the projection delta, command projection metadata, mutation program, operation artifact and query-plan corpus. None exists for the envelope: no JSON fixture in `tests/fixtures` or `distributed_cli/tests/fixtures` contains `authorizationGeneration`.
  - Both sides build envelopes by hand in tests (`js/tests/replica-rust-artifact-bridge.test.mjs:49-80`, `src/graphql/protocol/tests.rs`).
  - Limits are duplicated as literals in three files. A comment at `constants.ts:5` reads "Matches protocol.ts MAX_EVIDENCE_ITEMS without making it public API".
- Why it matters: This is the hottest Rust↔TS seam, and a renamed or added field on either side will pass both test suites.
- Change: Have a Rust test emit `tests/fixtures/protocol-envelope-v1.json` (query snapshot, live snapshot and delta frames, failure frame, each command state). Parse it in a TS test and feed it to `writeResult`. Emit the limits into that fixture or the generated artifact. Size M.

### 7. `lowerMutationCache` is a public, unused TS twin of Rust `lower_mutation_cache`
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `js/src/replica/mutation-cache.ts:1-168` (header: "Mirrors `distributed::mutation::cache::lower_mutation_cache`"), exported at `js/src/replica/index.ts:250-263`
- What: Whole-repo rg (excluding `node_modules`, `dist`, `target`) finds no caller in `js/src`, the CLI renderers, e2e-ui or examples. The only consumer is `js/tests/mutation-cache-lowering.test.mjs`.
- Why it matters: It is exactly the "TypeScript twin" principle 5 forbids, shipped as public API with no runtime use.
- Change: Delete the module, its exports and its test. Rust keeps the fixture. Size S.

### 8. The dehydrated-state format has three independent parsers
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `js/src/replica/distributed-replica/hydration.ts:88-616`; `js/src/replica/persistence/state.ts:349-1094`; cache engine `parseSnapshot` via `restore` (`engine.ts:418-419`)
- What:
  - Hydration and persistence each parse the same `ReplicaDehydratedState`, with their own `parseAuthoritativeScope`, `parseRecordClock`, operation-state parser and primitive helpers.
  - The two `parseAuthoritativeScope` copies already differ: one returns `undefined`, the other throws; one uses `hydrationOpaque`, the other `nonEmptyString`.
  - `persistence/state.ts:515-521` canonicalises by constructing a whole cache engine, restoring and extracting.
  - Decimal comparison exists three times with three algorithms: `protocol.ts:1019` (length then `<`), `clocks.ts:197` (`localeCompare`), `state.ts:1090` (`BigInt`).
  - `modelFromRecordKey` is duplicated (`clocks.ts:39`, `state.ts:921`).
- Why it matters: About 1,700 lines must be kept in lockstep by hand, and they already disagree on error behaviour.
- Change: One `parseDehydratedState()` owned by the replica core. Persistence becomes policy-filter-then-store over the parsed value. One decimal comparator in `protocol.ts`. Size M.

### 9. Filter/order/pagination evaluator is checked against Rust by only 4 cases
- Category: B/A   Severity: medium   Confidence: CONFIRMED
- Where: `tests/fixtures/client-query-plan-corpus.json` (4 cases, 5 records, columns `id`/`priority`/`completed`, all non-null); consumers `js/tests/replica-query-plan-corpus.test.mjs:43` and `tests/graphql_compile/main.rs:156` (SQLite only); evaluator `js/src/replica/query-plan/filter.ts` (987 lines), `order.ts`, `pagination.ts`
- What: The shared corpus is the right mechanism. It does not cover string ordering (`compareUtf8` against `COLLATE "C"`/`BINARY`), nullable columns and nulls-first/last, tie-breakers, `_in`/`_nin` with NULL, empty `_and`/`_or`, row policies, trusted presets, relationship filters, float/int32 edges, or Postgres at all. Those are tested only in `replica-query-plan.test.mjs` (1307 lines) against expectations written from the TS side.
- Why it matters: This is the largest unavoidable re-implementation of server semantics, and its only cross-language proof is trivial.
- Change: Grow the corpus to cover each operator × codec × nullability × direction, plus row-policy cases and "expected: unknown" cases. Run it on SQLite and Postgres. Retire TS-only tests that then duplicate it. Size M.

### 10. String `_eq`/`_neq`/`_in` on non-identical strings is "unknown", so string-filtered lists are never maintained locally
- Category: A/D   Severity: medium   Confidence: CONFIRMED (code); PLAUSIBLE (user-visible impact)
- Where: `js/src/replica/query-plan/filter.ts:507-518`, `:589-598`; `js/src/replica/index-maintenance/engine.ts:623-624`; orphaned doc comment at `filter.ts:660-664`; Rust `src/graphql/compile/projection.rs:1227-1248`
- What:
  - `{status: {_eq: "open"}}` against a row with `status: "done"` returns `collation_not_portable`, and `maintainEntityIndex` then marks the index stale.
  - ORDER BY is treated differently: the server emits binary collation and TS orders by UTF-8 bytes.
  - So the commonest filter shape cannot optimistically remove or exclude a row. An optimistic status change always forces a network revalidation of the list.
  - The comment at `:660-664` describes the binary-collation contract but is attached to no function, which looks like a split leftover.
- Why it matters: This undercuts the README's "automatic client side optimism" for the canonical todo-by-status case, and the two halves of the query plan follow different rules.
- Change: Extend the binary-collation contract to equality and IN. Have the SQL compiler emit the same `COLLATE` for textual `=`/`IN`, or declare text read-model columns binary. Then make string `_eq`/`_neq`/`_in` decidable in TS and add corpus cases. I did not measure how often this path fires in the demo apps. Size S (TS) + S (Rust).

### 11. `impl.ts` split is cosmetic: 370 lines of "Host" accessor bags over one mutable object
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `js/src/replica/distributed-replica/impl.ts:262-570` (five `#xHost()` factories of getters/setters onto `#private` fields), `:2092-2227` (about 25 one-line `fooOn(this.#host())` forwarders), `:3061-3120` (underscore "package-internal" methods for `ReplicaWatchState`); `#writeProtocolResult` at `:1133-1732` is one 600-line method
- What: The `impl-*.ts` siblings are free functions that read and write the class's private maps through host objects such as `ProtocolHost` (about 40 members, including `setProtocolGeneration`, `setNextIndexRevision`, `clearIndexMaintenance`). State ownership is unchanged; it is the same object with indirection. The same pattern appears elsewhere:
  - `commands/implementation.ts:1` says "Re-export surface for command implementation modules (body-split)".
  - There are three barrel layers per module (`replica/identity.ts` → `identity/index.ts` → `identity/keys.ts`; `index-maintenance.ts` → `index.ts` → `implementation.ts` → `engine.ts`).
  - Former privates are now `export function` (`index-maintenance/engine.ts` exports about 40 helpers such as `write`, `stale`, `reason`, `plan`), and tests import them.
- Why it matters: The file count went up and comprehensibility did not. Any function can still mutate any map.
- Change: Split by owned state. Candidates: `RecordClockLedger` (clocks, scopes, anonymous clocks, projected fences), `OperationProtocolTable` (operation protocols, generations, cursors), `LiveCoordinator` (lives, in-flight), `OptimisticReceipts`. Each owns its maps behind a small API, and `#writeProtocolResult` becomes plan (pure, returns a decision struct) then commit. Delete host bags and intermediate barrels; un-export helpers. Size L.

### 12. Immutable compiler artifacts are fully re-validated on every call
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `js/src/replica/identity/codec.ts:30-43`; `js/src/replica/operation-binding.ts:21-55`; `impl.ts:1025-1028`; `js/src/replica/commands/prepare.ts:97` and `commands/validate.ts:97`; `js/src/react/query.ts:148-152`
- What:
  - `canonicalizeOperationVariables` runs `validateReplicaOperationBinding` and `validateVariableCodec`, rebuilding the registry Maps, with no memoisation. No `WeakMap` exists in these modules.
  - `read`, `watch` and `writeResult` additionally call `#bindArtifact`, which validates the binding again.
  - `useDistributedQuery` does all of this on every React render.
  - Every command `invoke` re-runs `validateCommandProjectionArtifact` (part of the 2.6k-line validator) even though the inventory was validated at registration.
- Why it matters: Per-render and per-command CPU spent on frozen generated constants.
- Change: Validate once per artifact identity (a `WeakMap<artifact, Validated>` or validate at inventory/registration) and pass the validated handle inward. Size S.

### 13. Client assembly (auth fence, transport, hydration, pending receipts) lives in `sveltekit/`; React gets only a hook
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `js/src/sveltekit/replica.ts:298-552`, `:899-976` (`PendingReceiptStore`, `wrapCommandTree`), `:1029-1128` (`createAuthorizationFence`, `validatedHydrationAuthority`, `sameReplicaScope`); `js/src/react/query.ts` (178 lines), `react/context.ts`
- What:
  - Apart from the query store, nothing in `createDistributedSvelteKit` is Svelte-specific. It handles session → authorization fence → transport → replica → hydrate-before-command-registration ordering → command wrapping → reload registration.
  - React exports only `DistributedProvider` and `useDistributedQuery` over a bare replica, so a React app must hand-build the auth fence and hydration ordering.
  - No CLI output, example or e2e uses `@hops-ops/distributed/react`. rg finds only the js tests, type-test and pack-smoke.
- Why it matters: The core replica is framework-agnostic, but the composition root is not. React is a query-hook stub, not at parity.
- Change: Extract a framework-neutral `createDistributedClient({session, url, hydration, authority, createCommands})` into core. SvelteKit adds stores and boundaries; React adds the hook plus a pending-receipts hook. Otherwise cut `./react` from v5. Size M.

### 14. `sveltekit/vite.ts` does seven jobs, and `game/vite` depends on it and so on `svelte/compiler`
- Category: F/A   Severity: medium   Confidence: CONFIRMED (structure, import chain); load failure without svelte not executed
- Where: `js/src/sveltekit/vite.ts`; `js/src/game/vite.ts:8-16`; `js/src/sveltekit/islands/boundaries.ts` (imports `parse` from `svelte/compiler`); `js/package.json` (svelte is an optional peer)
- What: `vite.ts` holds:
  - the GraphQL dev proxy (`:78`);
  - the SvelteKit plugin and aliases (`:380-760`, `:1018`);
  - the dev-reload lifecycle HTTP middleware (`:802-970`);
  - CLI child-process orchestration (`:973-1016`, `:2059-2160`);
  - transactional staging, tree diffing and commit (`:1475-1650`, `:1808-2057`);
  - a cross-process PID file lock (`:2254-2437`);
  - boundary-module TS codegen (`:1725-1806`);
  - game generation (`:2516-2765`).

  `game/vite.ts` is documented as "any Vite app (no SvelteKit required)" yet statically imports `../sveltekit/vite.js`, which statically imports `./islands/boundaries.js`, which imports `svelte/compiler`.
- Why it matters: 2.8k lines cannot be held in one head, and a non-Svelte Vite app importing `@hops-ops/distributed/game/vite` needs `svelte` installed to load the module.
- Change: Split into `vite/compiler-process.ts` (spawn, lock, staging), `vite/lifecycle-middleware.ts`, `sveltekit/vite.ts` (aliases, boundaries) and `game/vite.ts` (depends only on `vite/*`). Size M.

### 15. Two generators own one file: the TS planner appends to the CLI-generated `sveltekit.ts`
- Category: A/C   Severity: low-medium   Confidence: CONFIRMED
- Where: `js/src/sveltekit/vite.ts:1795-1806` (`exposeBoundaryModule`), `:1729-1793` (`boundaryModuleSource`)
- What: After `distributed client` writes the entrypoint, vite.ts checks its "GENERATED by distributed client" marker and appends an `export { … } from './boundaries.js'` line, then writes `boundaries.ts` itself. `rg boundaries distributed_cli/src` finds nothing, so the CLI is unaware. Client codegen itself is not duplicated: vite.ts shells out to the CLI. Boundary analysis needs the Svelte import graph, so doing it in TS is legitimate.
- Why it matters: The generated tree is a product of two tools, and CLI `--check` output differs from the committed tree.
- Change: Have the CLI always emit the re-export, plus an empty `boundaries.ts` stub the planner overwrites. Otherwise make the planner own a separate entry module. Size S.

### 16. `wasm-pack` and `graphql` are runtime dependencies of the browser package
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `js/package.json` (`dependencies`); `distributed_cli/src/js_framework.rs:185-186`, `:381-386`; `js/src/document.ts:2`, `:14-16`
- What: `wasm-pack` (which downloads a binary on install) is a hard dependency because the CLI resolves `node_modules/wasm-pack/run.js` from the framework package; no TS file imports it. `graphql`'s `print` is statically imported into the core request and websocket path only to support `DocumentNode` inputs, while generated artifacts already carry string documents.
- Why it matters: Every consumer installs a Rust toolchain helper, and the GraphQL printer lands in every bundle.
- Change: Make `wasm-pack` an optional peer or have the CLI resolve it from the app. Make `GqlDocument` a `string` and drop `graphql` and `@graphql-typed-document-node/core`. Size S.

### 17. Apollo spike vestiges
- Category: B/F   Severity: low   Confidence: CONFIRMED
- Where: tests `Apollo 4 public APIs pass basic layers/batching/extract but expose the causal ordering gap` (`js/tests/cache-engine-conformance.test.mjs:1099`) and `built declarations contain no Apollo/vendor types` (`:1146`); `js/scripts/measure-cache-engines.mjs`; devDependency `@apollo/client`; `createCacheEngine` factory with one implementation (`internal/cache-engine/create.ts:6-8`), class `PurposeBuiltCacheEngine`, "selected by the executable spike" at `engine.ts:80`
- What: The first test asserts Apollo's behaviour, not this package's. The second guards against a dependency that exists only for that test. The script benchmarks bundle size against Apollo.
- Change: Delete both tests, the script, the devDependency and the factory indirection; rename the class. Size S.

### 18. Persistence: orphaned blobs are never pruned; a failed open is cached forever; no cross-tab handling
- Category: E   Severity: medium   Confidence: CONFIRMED (code); no in-repo consumer exercises it
- Where: `js/src/replica/persistence/state.ts:896-907`, `:887` (`storedAt` parsed, never used for expiry); `js/src/replica/persistence/idb.ts:204-211`, `:249-282`, `:52-69`
- What:
  - The key includes `schemaHash` and `authorizationGeneration`, so every schema or auth-generation change leaves the previous whole-cache blob in IndexedDB with no sweep.
  - `#databasePromise ??= openDatabase(...)` caches a rejected promise (for example `onblocked`) for the adapter's lifetime.
  - There is no `onversionchange` → `close()`, so a second tab's upgrade blocks.
  - `save` rewrites the entire state as one record; tabs are last-writer-wins.
  - Corruption handling is good: parse failure deletes the entry and returns `undefined`.
  - No binding, e2e or example wires `createReplicaIndexedDbPersistence` or `commandOutbox`; only the js README and tests reference them.
- Change: Either cut persistence and outbox from v5 (about 1,800 source lines plus 950 test lines with no consumer) or finish it: sweep by `storedAt` and identity prefix on open, reset the promise on failure, handle `versionchange`, and wire it into the client factory from finding 13. Size S (cut) / M (finish).

### 19. Diagnostics do a full cache extract per write
- Category: D   Severity: low   Confidence: CONFIRMED
- Where: `js/src/replica/distributed-replica/impl.ts:1680-1699`; `impl-diagnostics.ts:35-38`
- What: When a diagnostics sink is present, each normalised result calls `engine.extract()` and then `cache.indexes.find` per index key, and `syncDiagnostics` extracts again. This is opt-in only. The event log itself is bounded (`diagnostics/event-log.ts:42-43`, `:170-173`).
- Change: Emit counts from the write summary and make the snapshot lazy (pull on inspect). Size S.

### 20. Small duplicated helpers despite `src/lib/`
- Category: C   Severity: low   Confidence: CONFIRMED
- Where: canonical JSON ×3 (`internal/cache-engine/helpers.ts:648`, `replica/identity/clone.ts:55`, `replica/index-maintenance/engine.ts:1106`); `stableJson` ×2 (`sveltekit/boundary-variables.ts:389`, `sveltekit/islands/boundaries.ts:1149`); `compareUtf8` ×2 (`query-plan/order.ts:247`, `projection-delta/validate.ts:2228`); `compareCodeUnits` redefined at `command-runtime/create.ts:334`; `isWithin`/`isMissing`/`portable` in both `vite.ts` and `boundaries.ts`; `exactRecord` ×3; `recordKeyMatchesModel` ×2
- What: The index-maintenance canonicaliser filters `undefined`, and the cache-key one does not. `sameValue(index.metadata.arguments, planValue.arguments)` at `index-maintenance/engine.ts:512` therefore compares with different rules from those that built the index key.
- Change: Move one of each into `src/lib/` and delete the rest. Size S.

### 21. Tests reaching into internals
- Category: B   Severity: low   Confidence: CONFIRMED
- Where:
  - `js/tests/unsigned-command-output.test.mjs` (imports `dist/replica/command-runtime/lib/output.js`)
  - `js/tests/replica-index-maintenance.test.mjs:5-6` (imports `relationshipMembership` and `replaceOptimisticLayerOn` from implementation files)
  - `js/tests/replica-protocol.test.mjs:3758`, `:3785` (imports the private `symbols.js`)
  - `js/tests/replica-command-runtime.test.mjs:6` (imports `createCacheEngine`)
- What: These pin the body-split file layout. The suite is otherwise behaviour-first through `dist/replica/index.js`, and `tests/fixtures/adapter-conformance.mjs` is a real shared conformance harness for React and SvelteKit.
- Change: Re-express through `createDistributedReplica` and the command runtime, or move to the shared corpus. Size S.

### 22. Important behaviour with no test
- Category: B   Severity: medium   Confidence: CONFIRMED (absence by rg/sampling)
- What:
  - automatic live reconnect (finding 2; the existing test asserts the manual path);
  - cache or replica scaling (no test with more than a handful of records);
  - memory retention after watch destroy (findings 3–5);
  - `game/vite` importable without svelte (finding 14);
  - multi-tab persistence;
  - a Rust-produced protocol envelope ingested by the TS replica (finding 6).

### Smaller answers to the slice questions
- **Type safety:** clean. 6 `any` (all `GameClient<any>` generics), 13 `as unknown as`, no `ts-ignore`/`ts-expect-error`.
- **`game/`:** a separate binary protocol with its own socket, codec and reconnect logic. It shares nothing with `websocket.ts`. The codec is tested against Rust goldens (`distributed_game/tests/fixtures/`), which is the pattern finding 6 asks for elsewhere.
- **Packaging:** `dist/` is gitignored and not committed (`git ls-files dist` = 0). The exports map is exhaustive and ESM-only, `sideEffects: false`, and node-only code is isolated behind `./sveltekit/vite` and `./game/vite`.

## Things that are good and should not be changed
- **Three-valued query-plan evaluation** (`match`/`no_match`/`unknown` → mark stale and revalidate) with a typed reason code. Extend what is decidable (findings 9, 10) but keep fail-closed.
- **Server-authoritative replacement of the optimistic guess.** The client previews from the compiler-derived artifact, then swaps the layer for the server's actual projection delta, with capability checks (`command-runtime/create.ts:580-644`). The TS interpreter stays small (`projection-delta/resolve.ts`, `commands/resolve.ts`), and domain "pures" run as the Rust code compiled to WASM (`projection-delta/wasm-pure.ts`).
- **Generation and request-revision fences on every async boundary** (`impl-fetch-live.ts:142-152`, `:167-205`, `:270-318`), and transactional cache writes with rollback semantics. Keep the semantics; change the cost.
- **Shared Rust↔TS fixtures where they exist** (projection-delta, command-projection-metadata, mutation-program, runtime-bridge operation, game goldens), type-checking the CLI's generated fixtures against package types (`tsconfig.generated-tests.json`), and the adapter conformance fixture.
- **Core→binding layering.** `replica/` and `internal/` import nothing from `sveltekit/`, `react/` or `game/`, and `ReplicaWatch` is a framework-neutral primitive that both bindings wrap thinly.
