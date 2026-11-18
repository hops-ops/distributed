## Assessment: CLI and client code generator (`distributed_cli/`, root `build.rs`, `Makefile`, `distributed.contracts.json`)

The slice works, but it is roughly twice the size it needs to be. The CLI does not depend on the `distributed` crate (`distributed_cli/Cargo.toml:17-30`), so it re-declares the runtime's wire types and re-implements its semantics in a second Rust copy. About 18.5k non-test lines of `client_compiler/` exist largely for that reason. Version skew cannot justify it: `cli.rs:2824` already requires the manifest's framework release to equal the CLI's `CARGO_PKG_VERSION`.

The three changes most worth making:
1. **Make the CLI depend on `distributed` (default features off) and `distributed_game`.** Deserialize `DistributedClientManifest` and `GameSurface` directly, share one merge/lowering implementation, and delete the mirrors and dead wire code (findings 1–6).
2. **Shrink `contracts/` and the lifecycle "file adapter" to what is wired.** About 2.7k non-test lines of `contracts/` have no production caller, and `contracts check` does not check what the README says (findings 7–10).
3. **Fix `dev`/`build` process handling and move SvelteKit/npm/e2e-app specifics behind one adapter** (findings 11–15).

How this was read: I read `client_compiler/{mod, manifest/parse, projection_delta/preview (non-test, about 60%), projection_delta/mod}` and about 45% of `cli.rs` myself, and compared them against `src/graphql/{projection_delta, client_manifest, complexity}`. Three helper agents read `contracts/`, `lifecycle/`, `generate/`, the game/skills/render code and the tests; I re-opened the cited lines for the findings marked CONFIRMED and mark the rest PLAUSIBLE. No cargo or npm was run.

Not read, or outline only:
- `client_compiler/graphql/{operation, objects, fragments, variables, types, arguments, query_plan}.rs` (outline only) and `graphql/validation.rs` beyond lines 1-110
- `client_compiler/manifest/{model_validation (lines 1-45 and 1340-1500 only), projections, projectors, roots, leaderboards, util}.rs`
- `client_compiler/command_manifest/{effects, confirmations, shape, support, protocol}.rs` bodies
- `client_compiler/projection_delta/wire.rs` beyond line 60, and `islands.rs`
- `client_compiler/tests.rs` about 55% (helper), `tests/cli_client.rs` 520-795, `tests/cli_skills_init.rs`, `tests/bats/lifecycle.bats`
- `skills/*/SKILL.md` end to end, most fixture files, `distributed_cli/README.md`

All paths below are under `/Users/patrickleet/dev/hops-ops/hops/distributed/`.

---

### 1. The CLI is a second implementation of the runtime's client-manifest contract
- Category: C/A   Severity: high   Confidence: CONFIRMED
- Where: `distributed_cli/Cargo.toml:17-30` (no `distributed` dependency); `distributed_cli/src/client_compiler/manifest/types.rs:9-1149` versus `src/graphql/client_manifest/types.rs:129` (and the rest of that file); `client_compiler/manifest/parse.rs:13-53`, `:255-281`; `client_compiler/manifest/mod.rs:20-29`; `src/lib.rs:40`
- What: `client_compiler/manifest/types.rs` re-declares the runtime's `Client*` types as about 110 `Manifest*` serde types.
  - Both sides use `deny_unknown_fields`.
  - The CLI recomputes `schema_fingerprint` from its own mirror (`parse.rs:255`).
  - The CLI hard-codes the protocol fingerprint as a literal (`mod.rs:22-23`), which the runtime computes in `src/graphql/client_manifest/codec.rs:258`.
  - Seven version constants are copied (`mod.rs:20-29`; `cli.rs:47-48` adds a third copy of the manifest version).
  - `pub mod graphql` is not feature-gated (`src/lib.rs:40`), and `src/graphql/client_manifest/mod.rs:5` calls itself "intentionally pool-free".
- Why it matters: this is Principle 5 inside Rust. Every manifest change is a hand-synchronised edit in two crates. Byte-exact fixtures catch drift only where one exists.
- Change: `distributed_cli` depends on `distributed` with `default-features = false`. `compile_client` takes `DistributedClientManifest`. Delete `manifest/types.rs`, the fingerprint recomputation, and the copied constants. Breaking: the `compile_client` input type. Size L.

### 2. Preview canonicalisation and lowering are near-verbatim twins of the runtime's
- Category: C   Severity: high   Confidence: CONFIRMED
- Where: `client_compiler/projection_delta/preview.rs:1781-1988` versus `src/graphql/projection_delta/canonical.rs:8-305`; `preview.rs:1037-1202` versus `src/graphql/projection_delta/lower.rs:572-784`; `preview.rs:835-842` and `:1090-1094` versus `lower.rs:775-784`
- What: `canonicalize_operations`, `canonicalize_recoveries`, `merge_operation`, `merge_mutation` and `merge_refs` are copied arm for arm, with identical error strings ("patch fields must belong to the upsert replacement mask").
  - They differ already: the runtime re-sorts by `canonical_order` (`canonical.rs:21`, `:65`); the CLI relies on the `BTreeMap` order of `PreviewOperationScope`.
  - `lower_record` repeats the runtime's decision table (complete write → Upsert; otherwise Patch plus `IfRecordMissing` recovery; otherwise `Always` recovery) over symbolic `Knowledge` instead of concrete values.
  - The "complete write" kind set is written three times.
  - The runtime already has a preview source (`ProjectionMutationSource::Preview`, `lower.rs:302`), but it is `#[cfg(test)]`.
- Why it matters: optimistic preview and server delta must agree on merge and recovery semantics, or clients flicker and revalidate. No shared vector pins the two.
- Change: make the canonical merge generic over the value type (`DeltaValue` versus `PreviewExpression`) in the runtime crate and call it from the CLI. Longer term, lower previews through the runtime's `Preview` source. Size M for canonicalisation, L for lowering.

### 3. Trusted-preset derivation, command-operation text and complexity estimation are also twins
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where:
  - `client_compiler/manifest/model_validation.rs:1348-1487` versus `src/graphql/client_manifest/validation.rs:3-150`
  - `client_compiler/command_manifest/shape.rs:239-258` versus `src/graphql/client_manifest/commands.rs:221-242`
  - `client_compiler/graphql/validation.rs:3-102` versus `src/graphql/complexity.rs:17-112`
- What: the first two pairs are copied line for line; the CLI regenerates the command mutation text only to byte-compare it with the manifest (`command_manifest/validation.rs:122-129`).
  - The complexity estimator has diverged. The runtime floors an empty selection at `w.scalar` (`complexity.rs:53-55`); the CLI folds to 0.
  - The CLI uses the manifest's weights; the runtime calls `default_weights()` (`complexity.rs:27`).
- Why it matters: an operation can pass the build-time limit and be rejected by the server, or the reverse.
- Change: with finding 1 done, call the runtime functions. Export one `estimate_complexity(weights, shape)` used by both. Size M.

### 4. `projection_delta/wire.rs` is about 800 lines of dead mirror code
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `client_compiler/projection_delta/wire.rs:1-7`, `:18`; `projection_delta/mod.rs:11`
- What: the file opens with `allow(dead_code, reason = "...runtime consumption lands in Task 16")`. A search of `distributed_cli/src` shows the only thing used outside the file is the constant `PROJECTION_DELTA_WIRE_VERSION`. The rest mirrors `src/graphql/projection_delta/types.rs` (the wire types, validation and canonical bytes), exercised only by nine in-file tests.
- Why it matters: a third copy of the wire contract with no caller.
- Change: delete the file and take the constant from the runtime. Size S.

### 5. Legacy "effects/confirmations" command authority is unreachable
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `client_compiler/command_manifest/validation.rs:131-137` then `:149-178`; `client_compiler/manifest/types.rs:520-546`; `manifest/commands.rs:69-74`; `command_manifest/effects.rs:154` (`validate_effect`), `confirmations.rs:17`; `src/graphql/client_manifest/types.rs:518-545`
- What: `extensions.version != 2` returns an error, and the next block is `if extensions.version == 1 { … }`.
  - Both fields deserialize through `reject_legacy_command_authority`, which always errors, so they are never `Some`.
  - Canonicalisation and tests still exist for them (`command_manifest_tests.rs:261`, `tests.rs:3998`, `:1225`).
  - The runtime types carry the same rejecting fields.
- Why it matters: this guards a format that never shipped. `deny_unknown_fields` already rejects the keys.
- Change: delete the fields on both sides, the dead block, `validate_effect`, `validate_confirmations`, and their tests. `validate_defaults`, `validate_trusted_preset_inventory` and `validate_projector_inventory` are live and stay. Size M.

### 6. `game_client.rs` mirrors `distributed_game`'s `GameSurface`
- Category: C   Severity: high   Confidence: CONFIRMED (constants and the no-op at `:483` re-opened; the type-by-type comparison is the helper's)
- Where: `distributed_cli/src/game_client.rs:31-34`, `:60-229`, `:483`; `distributed_game/src/surface.rs:40`
- What: 13 private serde types duplicate the surface schema. `GAME_SURFACE_SCHEMA_VERSION = 3` and `PREDICT_ABI_VERSION = 1` are second copies. `protocol_version` is read and discarded: `let _ = (surface.schema_version, surface.protocol_version);`.
- Why it matters: same drift problem as finding 1. Game concepts are compiled into the CLI unconditionally anyway.
- Change: keep the game commands in the one CLI (matches the single-CLI preference), but depend on `distributed_game`, deserialize `GameSurface`, and move structural validation to one `GameSurface::validate()`. Size M.

### 7. `contracts check` does not check drift, and most of `contracts/` has no production caller
- Category: A/F   Severity: high   Confidence: CONFIRMED
- Where: `cli.rs:1347-1355`; `distributed.contracts.json:4-16`; `contracts/migrations.rs` (1,513 lines), `snapshots.rs`, `classification.rs`, `closeout.rs`, `program.rs`, `chain.rs`; `lib.rs:36-55`; `Makefile:57-58`
- What: the CLI calls `contracts_check(&catalog, &root, std::iter::empty())`. The check validates catalog structure and that declared paths exist; nothing compares an identity to a baseline.
  - The repo catalog has one entry, whose identity is the literal `"ref:migrations"`.
  - `check_migration_history`, `check_migration_inventory`, `classify_snapshot_diff`, `close_local_contract_chain` and `classify_release_programs` appear only in `contracts/` and the `lib.rs` re-export.
  - Helper-reported, not re-opened: diagnostics tell users to pass `--base <revision>`, which `ContractsCheckArgs` does not have; the README says check "exits non-zero on drift"; the check gates CI jobs.
- Why it matters: the gate proves only that `migrations/inventory.json` exists, while carrying compatibility machinery (history immutability, program compatibility classes, five `schema_version` constants that have only ever been 1).
- Caveat: the repo has tags up to v4.x. If migration immutability against a released baseline is wanted, wire `--base` rather than deleting the history half.
- Change: either dispatch per kind so `check` runs the migration check, or delete the subcommand, the Makefile target and the unwired modules. Keep a catalog of `{kind, owner, sources, generator, outputs, predecessor}` for the lifecycle graph. Breaking: the `distributed_cli::contracts` public surface and catalog schema. Size L, mostly deletion.

### 8. Root `build.rs` and `contracts/migrations.rs` are twins, and the one that runs is `build.rs`
- Category: C   Severity: high   Confidence: CONFIRMED (constants on both sides re-opened; function-level pairs are the helper's)
- Where: `build.rs:9-18` versus `contracts/migrations.rs:27-45`; helper-listed pairs include `validate_json_nesting` (`build.rs:338` / `migrations.rs:817`), `collect_sql_files` (`:418` / `:1158`), `read_bounded_file` (`:383` / `:1378`), `sha256_hex` (`:571` / `:1422`)
- What: eight identical limit constants and about eight identical functions, roughly 450 of `build.rs`'s 576 lines. The CLI copy is the unreachable one (finding 7). The helper found they have diverged: the CLI adds a `validate_json_value` pass that `build.rs` lacks. There is no third copy in `src/`.
- Why it matters: hostile-input limits (JSON depth 24, 4,096 directories, path redaction) applied to the repo's own checked-in inventory, twice.
- Change: slim `build.rs` to parse, check consecutive versions, verify sha256 and emit; delete the CLI copy with finding 7. Size M.

### 9. `contracts accept` ignores its scope and its rollback is best-effort
- Category: E   Severity: medium   Confidence: CONFIRMED for the rollback lines; PLAUSIBLE for the rest (helper)
- Where: `contracts/transaction.rs:167-178`; helper cites `:108-191`, `:224-236`
- What: per file it runs `create_dir_all(...)?`, which returns after earlier files were already replaced. Rollback errors are discarded (`let _ = rollback(root, &prior);`). The staging name is `path.with_extension("distributed-accept-staging")`, so `a.json` and `a.ts` collide. Helper-reported: `scope` is only echoed in the report, and nothing in the repo produces the staged JSON it consumes.
- Why it matters: a command that writes arbitrary paths under root with a label for a scope and no caller.
- Change: delete `accept` until something produces its input. If kept, resolve the scope to the catalog entry's `outputs` and write all temp files before any rename. Size S.

### 10. The lifecycle "file adapter" path exists only for old fixtures
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `cli.rs:156-164`, `:190-198` ("Deprecated alias", hidden `--root`/`--catalog`/`--config`), `:786-901`, `:984-1003`, `:1302-1328`; `lifecycle/build.rs:180` ("retained for the compatibility file-adapter API"); `lib.rs:58-60`
- What: `resolve_lifecycle_project` has two modes, `Discovered` and `Files`. The `Files` mode needs `distributed.lifecycle.json`, which does not exist at the repo root. The helper found it referenced only by `cli.rs` and `tests/cli_lifecycle.rs`. That gives two option types (`LifecycleBuildOptions`/`LifecycleDevOptions` versus the project-plan types) and two entry points.
- Why it matters: `cli_lifecycle.rs`, the behavioural test suite for build/dev, exercises only the legacy adapter; the discovered path is touched only by the bats file.
- Change: delete the adapter and hidden flags; port `cli_lifecycle.rs` to build a project plan and call `run_lifecycle_project_build/dev`. Size M.

### 11. `dev` orphans its children on anything but Ctrl-C
- Category: E   Severity: high   Confidence: CONFIRMED (static read; not run)
- Where: `distributed_cli/Cargo.toml:22` (`ctrlc = "3"`, no `termination` feature); `cli.rs:1270`; `lifecycle/dev.rs:1183`, `:1283` (`command.process_group(0)`); the only `Drop` impls in `lifecycle/` are `BuildLock` (`build.rs:1253`) and `DevStateStore` (`dev.rs:824`)
- What: the handler catches SIGINT only. Children run in their own process groups, so the terminal's signal does not reach them, and there is no `Drop` cleanup for the child set.
- Why it matters: closing the terminal, `kill`, a container stop, or a panic in the supervisor leaves `cargo run` and `npm run dev` holding ports 8791/5180.
- Change: enable `ctrlc`'s `termination` feature and add a `Drop` that signals each group. Size S.

### 12. Build cancellation kills only the direct child; a transient file error ends the dev session
- Category: E   Severity: high   Confidence: CONFIRMED for the code paths; the editor-race trigger is PLAUSIBLE
- Where: `lifecycle/build.rs:677-689` (executor spawned without a process group); `lifecycle/dev.rs:335`, `:381-387`
- What: the executor child is `distributed describe`, which itself blocks in `cargo run`; cancel kills the leader and leaves cargo/rustc running on the same harness target dir. Separately, the watch loop does `let next = lifecycle_input_snapshot(...)?;`, so any error from the directory walk returns from the loop and shuts everything down.
- Why it matters: `dev` cancels on every superseding edit, so "cancel" saves nothing and the next build queues behind the orphan. A file removed between `read_dir` and `open` (editor swap file, `git checkout`) ends the session.
- Change: spawn executors with `process_group(0)` and reuse the dev group-kill helpers; in the watch loop treat snapshot errors as "not quiet yet" and retry. Size S/M.

### 13. The generic lifecycle and compiler are hard-wired to SvelteKit, npm and the repo's e2e app
- Category: A   Severity: medium   Confidence: CONFIRMED
- Where: `lifecycle/project.rs:271`, `:355`, `:364`, `:1007`, `:1012`, `:1065-1068`; `cli.rs:1139-1183`, `:1228-1243`; `client_compiler/render/project.rs:78-79`, `:323-385`; `render/commands.rs:229`, `:237`; `client_compiler/mod.rs:1`, `:137-141`
- What:
  - The lifecycle executor is `"distributed.sveltekit-clients"` at `node_modules/@hops-ops/distributed/dist/sveltekit/lifecycle-compiler.js`.
  - The package manager is `npm` (`ci`/`install`/`run`) only.
  - Every UI process gets `E2E_API_ORIGIN` and an `AUTH_SECRET` default; the helper found `E2E_API_ORIGIN` read only under `tests/e2e-*`.
  - Ports default to 8791 and 5180.
  - The compiler's module doc says "Pure, framework-neutral… Svelte, Vite, and router concepts must never enter this type", yet `render_project` always emits `sveltekit.ts` and fails compilation on SvelteKit naming collisions, and `pures.ts` imports through `$lib/…`.
- Why it matters: a layering inversion. `js/src/react` exists, but a React or pnpm project gets SvelteKit output and npm behaviour, and a test fixture's env contract ships in the product.
- Change: one UI adapter fed by `[workspace.metadata.distributed.ui]` (command, env, package manager); drop `E2E_API_ORIGIN`/`AUTH_*` (the e2e app's `.env` is already loaded by `cli.rs:903`); make `sveltekit.ts` and the `$lib` alias a `--target` render pass. Breaking: generated file set. Size M.

### 14. The manifest harness resolves dependencies separately from the user's workspace
- Category: E   Severity: medium   Confidence: CONFIRMED structurally (`manifest_harness.rs:87-113` re-opened); consequences PLAUSIBLE
- Where: `manifest_harness.rs:75-125`, `:185-196`; `cli.rs:2884-2911`
- What: it writes a temporary crate under `<target>/distributed-manifest-harness/<pkg>/<mode>` with its own `[workspace]`, takes an unbounded `lock_exclusive`, and runs `cargo run --quiet`. Helper-reported: the user's `Cargo.lock` and `[patch]` are not applied; schema modes need a local `distributed` checkout found by walking ancestors; the extern name is derived from the package name, so a custom `[lib] name` breaks.
- Why it matters: the manifest can be produced from different dependency versions than the app runs, and each Rust save compiles the service in two target dirs.
- Change: copy the workspace lockfile into the harness, take the lib target name and `distributed` path from `cargo metadata`, add a lock timeout. Better: have the runtime binary emit the manifest. Size M.

### 15. `describe` writes WASM into a watched input directory; generations are never pruned
- Category: A/E   Severity: medium   Confidence: CONFIRMED for the write site and the absence of pruning; loop consequences PLAUSIBLE
- Where: `cli.rs:1750-1760`; `wasm_pures.rs:92-99`, `:594-621` (helper); `rg remove_dir_all distributed_cli/src/lifecycle` returns nothing
- What: when `DISTRIBUTED_LIFECYCLE_ROOT` is set, `describe` calls `build_declared_wasm_pures`, which the helper traced to `ui/src/lib`, a declared input of the clients node. So a read-style command changes behaviour on ambient env vars and mutates inputs mid-build. Separately, every rebuild adds a directory under `generations/` and nothing removes old ones.
- Why it matters: this bypasses the staged, atomic generation model the lifecycle is built around, and a dev session grows the generation store by a full client tree per save.
- Change: make WASM a catalog node with staged outputs; prune to active plus the last N generations under the build lock. Size M.

### 16. `cli.rs` holds business logic, two identical command enums, and never-shipped compatibility
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `cli.rs:58-90` versus `:117-149`; `:712-732` versus `:763-783`; `:761-762`; `:318-327`, `:603-605`, `:2631-2647`; `:2152-2155`, `:2193-2228`, `:3133`; `:1091-1097`; `:1773-2367`
- What:
  - `DistributedCommands` and `ServiceCommands` are the same 14 variants with two identical dispatchers and a test (`:3234`) to keep them in sync. The doc on `run` says hosts call it "without mounting the aggregate contracts surface", but it mounts `Contracts`.
  - About 600 lines of generated-directory ownership logic and about 230 lines of skills wiring live in the clap file.
  - Compatibility for things that never shipped: `--kind` "Compatibility alias", `--http`/`--knative` "Compatibility shortcut", a single-variant `Framework` enum, and `compiler_manifest_version` `Some(1 | 2)` with a v1 `routes.ts` cleanup branch.
  - Error phase is classified by substring: `error.contains("manifest harness failed for package")`.
- Command surface: `build`, `dev`, `contracts {check, accept}`, `scaffold` (alias `create`), `describe`, `client`, `client-manifest`, `game-manifest`, `game-client`, `game-wasm`, `schema`, `skills {init, list}`, hidden `__probe`, `__js-watch`.
- Overlap and vestige: `describe`/`client-manifest`/`game-manifest`/`schema` are four modes of one harness with repeated flag sets. `contracts accept` is vestigial (finding 9). `client-manifest` output gets only a shallow third validator (`cli.rs:2834-2882`).
- Change: one command enum with a `command_prefix` parameter; move client-output writing to its own module; delete the compatibility flags and the v1 branch; give the harness error a typed variant. Size M.

### 17. Two generated-directory writers with different guarantees
- Category: C/E   Severity: medium   Confidence: CONFIRMED
- Where: `cli.rs:1986-2018`, `:2060-2115`, `:2255-2297` (GraphQL client) versus `:2469-2547` (game client)
- What: the GraphQL writer refuses symlinked path components and tracks ownership through `manifest.json`, then rewrites every file unconditionally (`:2014`). The game writer has no symlink or path checks and judges ownership by a `// @generated` first line, but skips unchanged files so watchers do not reload (`:2537-2542`). The two `--check` loops are near-identical.
- Why it matters: each generator has the protection the other lacks; the GraphQL writer triggers dev-server reloads on every run.
- Change: one `GeneratedDir { write, check }` with symlink refusal and skip-unchanged, used by both. Size M.

### 18. TypeScript emission is scattered `format!`, with twin helpers across the two generators
- Category: C   Severity: medium   Confidence: PLAUSIBLE (helper's read; I re-opened only `render/commands.rs:226-240`)
- Where: `game_client.rs:262-355`, `:901-918`, `:1951`; `client_compiler/render/common.rs:3-16`; `render/commands.rs:240`; `render/leaderboards.rs:47`; `render/typescript.rs:185-201`
- What: no emitter abstraction in either generator.
  - String literals: a hand escaper plus a weaker `key()` in the game generator, versus `serde_json::to_string` in three error-handling styles in `render/`.
  - `commands.rs:240` falls back to `"\"\""` on a serialization "failure" that cannot occur for `&str`.
  - Two `pascal` functions, four codec→TS tables, tabs in some files and two spaces in others.
- Determinism: output is deterministic through `BTreeMap`/explicit sorts. The helper flagged two sites that pretty-print a `serde_json::Value` without the canonicaliser used elsewhere (`render/commands.rs:339-342`, `game_client.rs:412-415`); these would change bytes if a consumer's build enables serde_json `preserve_order`.
- Change: one small `ts` module (literal, property, identifier casing, indented block) and one codec→TS table; canonicalise the two JSON sites. Size M.

### 19. Low-value tests in `client_compiler` and `contracts`
- Category: B   Severity: medium   Confidence: CONFIRMED for the named tests I opened (`tests.rs:3077`, the six `preview.rs` tests); the rest are the helpers' reads
- Where and what:
  - `client_compiler/tests.rs:3077` `command_protocol_and_extensions_are_preserved_exactly`: about 825 lines, about 25 manifest variants, around 100 `contains("\"version\": 2")`-style substring checks on pretty-printed output. These strings occur in many places, so the test can pass for the wrong reason.
  - `preview.rs:2253`, `:2270`, `:2386`, `:2416`, `:2590`, `:2605`: these exercise `Knowledge::Unset`/`CacheUnowned`, variants marked `allow(dead_code)` and "reserved" at `preview.rs:23-33`, which production never constructs.
  - `command_manifest_tests.rs:261`, `tests.rs:3998`, `:1225`: legacy authority that serde can never deliver (finding 5).
  - `tests.rs:2055-2063`, `runtime_bridge_tests.rs:106-112`: `contains` checks already implied by the byte-exact golden beside them.
  - `tests.rs:4481`: the lazy-commands golden is compared after a `.replace(...)`, so it is not real output.
  - `contracts/transaction.rs:304` `check_report_is_no_write` builds `ok: true` and asserts it; `contracts/tests.rs:1254` asserts `canonical_bytes() == canonical_bytes()`; `contracts/tests.rs:1272` compares fixture files to literals without running production code.
  - `tests/cli_scaffold.rs`: six tests repeat `contains` checks already in `generate/mod.rs` through a subprocess; `bus_variants_render_gitops_bus_config` pins `HOPS_BUS`, which nothing reads (finding 21).
  - Setup duplication: 40 call sites hand-build `ClientCompileInput` next to existing helpers; the `todo.create` manifest is written out at least twice; `cli_client.rs:11-274` carries another 264-line hand-written manifest.
  - Goldens: nine GraphQL golden files have no regeneration path; `UPDATE_GOLDEN` exists only in `tests/cli_game_client.rs:57`.
- Change: split the megatest per scenario and assert on parsed artifact JSON; delete the tests of unreachable states; one `assert_golden` helper honouring `UPDATE_GOLDEN`. Size M.

### 20. Generated output with no behavioural test
- Category: B   Severity: medium   Confidence: PLAUSIBLE (helper searched; I did not repeat the search)
- Where: `client_compiler/render/commands.rs:164-258` (`pures.ts`); `render/project.rs:99-170`, `:323-525`
- What: GraphQL `pures.ts` has no assertion anywhere in the compiler tests, `distributed_cli/tests`, `js/tests` or root `tests/`. It interpolates manifest strings raw into import statements (`commands.rs:229-232`, re-opened). `sveltekit.ts`, `islands.ts`, `protocol.ts` and `index.ts` are only substring-checked, and `generated-commands.ts` type-checks against a hand-written `protocol.d.ts` stub rather than the generated `protocol.ts`.
- What is covered well: the game chain runs Rust derives → pinned TS → `tsc` → a byte-for-byte codec round trip in Node. Operation and command JSON artifacts are executed by `js/tests/replica-*`. Real-Rust end-to-end exists but is `#[ignore]` by default (`tests/cli_manifest.rs:124`, `tests/cli_game_client.rs:175`).
- Change: generate one whole project (commands, pures, live, leaderboards) into a pinned directory included in the JS type-check; validate `client_module`/`client_export`/`wasm_package` as path/identifier grammar. Size M.

### 21. Scaffold flags that do nothing, and org-specific templates
- Category: F/A   Severity: medium   Confidence: CONFIRMED for `HOPS_BUS`, the in-memory default and the org strings; the rest PLAUSIBLE (helper)
- Where: `generate/gitops.rs:120`, `:127`, `:330`; `generate/service_crate.rs:134-139`; `generate/github.rs:112`, `:154`, `:230`; `cli.rs:2737-2769`
- What:
  - `--bus` emits a `HOPS_BUS` env var that appears only in the generator, one test and a skill doc.
  - Helper-reported: without `--query-api`, `--store postgres` still generates `service::in_memory()` and only adds a cargo feature.
  - Templates are `format!` raw strings pinned to `unbounded-tech/workflows-gitops/...@v1`, `workflow-vnext-tag@v1.21.3`, image `ghcr.io/hops-ops/<name>` and label `hops.ops.com.ai/service`.
  - `--github` runs `gh repo create --private`.
  - No Upbound packages are referenced.
- Why it matters: dead configuration, and one organisation's deployment conventions inside the framework CLI.
- Change: wire or drop `--store`/`--bus`; move gitops/github generation to the host `hops` CLI, which already mounts these commands, or behind one explicit preset. Size M.

### 22. Duplicated plumbing across lifecycle, harness and WASM modules
- Category: C   Severity: low   Confidence: PLAUSIBLE (helper's line list; not re-opened)
- Where: `cargo metadata` wrappers at `lifecycle/project.rs:417`, `wasm_pures.rs:260`, `manifest_harness.rs:285`; bounded directory walkers at `lifecycle/build.rs:927`, `js_framework.rs:609`, `wasm_pures.rs:374`, `:455`; atomic write at `dev.rs:656`, `build.rs:1075`, `js_framework.rs:360`; `toml_string`/`is_rust_ident` in both `generate/names.rs` and `manifest_harness.rs`; two WASM stamp formats (`game_wasm.rs:47-57` versus `wasm_pures.rs:69-76`)
- What: three sets of metadata structs, four walkers with duplicated limits, and two staleness rules for "compile a Rust function for the browser" (one hashes the module, one checks file existence).
- Change: one file-utility module and one cargo-metadata module loaded once per invocation; one stamp type. Size M.

### 23. Generated `predict.ts`/`input.ts` may reference undeclared record constants
- Category: E   Severity: medium   Confidence: PLAUSIBLE (helper's read; not executed and not re-opened)
- Where: `game_client.rs:921-936`, `:1083-1129`, `:1579-1586`, `:723-727`
- What: a record-typed field lowers to a bare `{NAME}_TYPE` identifier. Those constants are declared only in `events.ts` (unexported) and `pures.ts`. `predict.ts` builds its context layout from record fields but imports neither, and `check_predict` only checks that the context record exists. The pinned sample has a scalar-only context, so nothing would catch it.
- Change: reject non-scalar context/input fields in validation, or emit the type constants as `pures_ts` does; add a nested-record context to the authoring fixture. Size S.

### 24. `skills.rs` is reasonable but its content partly targets the framework repo
- Category: F   Severity: low   Confidence: PLAUSIBLE (helper)
- Where: `skills.rs:36-83`, `:127-131`; `cli.rs:1432-1570`; `skills/distributed-usage/SKILL.md:177-178`, `:286`, `:314-322`
- What: it embeds five `SKILL.md` files with `include_str!`. `skills init` writes them under `.distributed/skills/`, links them into hard-coded `.claude/skills/` and `.agents/skills/`, and adds a marked block to `AGENTS.md`.
  - No stale commands or flags were found against clap.
  - `distributed-usage` tells an agent in a user project to copy `tests/e2e-ui/` and run `cd tests/e2e-ui && make`, which do not exist there.
  - The game skill does not mention `game-manifest`/`game-client`/`game-wasm`.
- Verdict: it belongs. The skills version with the command surface; the cost is the hard-coded harness directories.
- Change: fix the content; move the wiring logic out of `cli.rs`. Size S.

---

## Things that are good and should not be changed
- `compile_client` is a pure function (manifest plus documents in, sorted files out) with stable, source-located error codes. Keep that boundary when switching its input type.
- Deterministic output by construction: `BTreeMap`/`BTreeSet` and explicit sorts throughout, with provenance written last so an interrupted run never advertises a complete generation (`cli.rs:2005-2008`).
- The exact release handshake between manifest and CLI (`cli.rs:2824`). It is what makes sharing types with the runtime safe.
- The game generator's test chain (Rust derives → pinned TS → `tsc` → Node codec round trip, with `UPDATE_GOLDEN`). It is the model the GraphQL goldens should follow.
- The staged-generation build with an flock-guarded activation, and `tests/cli_lifecycle.rs`'s real-binary, descendant-leak-checking style. Keep the tests; point them at the project path instead of the legacy adapter.
