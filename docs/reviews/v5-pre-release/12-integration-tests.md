## Assessment

The suite is mostly behavioural and stronger than the owner's worry suggests. The pure implementation-detail tests are a small, nameable set (roughly 40 functions plus two source-grep files). The larger problems are structural:

1. **Service-backed tests skip silently, and CI never supplies the service for several of them.** A number of Postgres and broker tests are green without ever executing.
2. **Principle 7 is not actually tested.** GraphQL/RBAC is proven on SQLite only; the transport "conformance" suite mostly runs against fakes; in-memory, Redis and Knative buses run none of the shared bus scenarios.
3. **Copy-paste where a conformance suite should be.** `sqlite_transport` and `postgres_transport` share 804 identical lines; `sqlite_repository` and `postgres_repository` share 437; three broker suites hand-copy the same four failure-path tests.

Beyond those: pre-release legacy/upgrade tests, and 87 separately linked test binaries with almost no shared fixtures.

**What I read**
- **Fully:** `tests/support/*.rs`, `tests/legacy_authoring_absence.rs`, `tests/persistent_repository_conformance/mod.rs`, the three `*_repository_conformance/main.rs`, `tests/transport_in_memory/main.rs`, `tests/graphql_postgres/main.rs`, `tests/graphql_oidc_keycloak/main.rs`, `tests/named_read_models.rs`, the Makefile, and workflows `rust.yml`, `test-all-features`, `integration-{postgres,kafka,nats,rabbitmq}`, `on-pr-quality`.
- **Partially (outline plus 100–400 lines each):** `graphql_query_protocol/main.rs`, `typed_commands/main.rs`, `distributed_read_model/main.rs`, `application_composition.rs`, `sqlite_transport`, `postgres_transport` (mostly by diff against sqlite), `kafka_transport`, `nats_transport`, `rabbitmq_transport`, `transport_conformance/mod.rs`, `sqlite_repository`, `aggregate_command`, `sagas/orchestration.rs`, `e2e_ui_celld_nats_profile`, `core_command_contract.rs`, `upcasting`, `sourced_upcasting`, `sourced/main.rs`, `graphql_sdl`, `graphql_engine`, `graphql_harden/softskip.rs`, `sql_lock_manager`, `coordination_lease`, `schedule_worker`, and workflows `integration-gateway`, `integration-graphql`, `integration-observability`, `integration-e2e-ui` (job outline only).
- **Test names and grep only:** `causal_wait_path`, `graphql_sqlite`, `graphql_harden/{authz,dos,inject,errors,transport,residual,dialect}`, `graphql_identity`, `gateway_*.rs`, `edge_query_delivery*.rs`, `domain_event_projection_server.rs`, `read_model_*`, `replay_event_version.rs`, `application_plans.rs`, `microsvc/*`, `saga_transfer`, `snapshots`, `sourced_snapshot`, `enqueue`, `sourced_enqueue`, `todos`, `event_store`, `repository_api`, `queued_repo`, `replay_property`, `snapshot_sqlite_hardening`, `durable_enqueue_sqlite`, `scheduled_outbox`, `knative_cloudevents`, `redis_leaderboard`, `graphql_leaderboard`, `graphql_causal_transport`, `graphql_subscriptions_sqlite`, `graphql_commands`, `graphql_http`, `graphql_compile`, `graphql_literal_text`, `graphql_unique_key_live`, `metrics_exposition`, `otel_export`.
- **Not read:** `persistent_repository_conformance/{scenario,outbox,inbox,read_models}.rs` bodies, `graphql_oidc_common/mod.rs` body, `graphql_oidc_{authentik,zitadel}` (diffed only), `tests/gateway-{auth,native,portable,worker,postgres}` sources, `tests/fixtures/*` crate sources, `compose.yaml` (image lines only), and workflows `integration-celld`, `integration-game-demos`, `integration-js`, `integration-wasm32-domains`, `integration-distributed-cli`, `quality.yaml`, `on-v-tag-publish`.
- **No timing data:** nothing was built or run, so every CI-time statement is structural.

## Findings

### 1. Service-gated tests pass by returning early, and CI never sets the service for several targets
- Category: B/E   Severity: high   Confidence: CONFIRMED
- Where: `tests/support/env.rs:7`, `tests/support/postgres.rs:19-23`, `tests/persistent_repository_conformance/mod.rs:31-33` (the pattern in all 22 cases), `.github/workflows/integration-postgres.yaml:40-48`, `.github/workflows/test-all-features.yaml:8-11`
- What: Every broker/DB test does `let Some(x) = ...from_env() else { return };` and reports PASS. The all-features job states that service tests self-skip. The Postgres job then lists targets by hand: `postgres_repository`, `postgres_repository_conformance`, `postgres_transport`, `distributed_read_model`, `sql_lock_manager`, `graphql_literal_text`, `schedule_worker`. No workflow runs these with their service present:
  - `graphql_postgres` and `graphql_query_protocol_postgres` (not named in any workflow).
  - The Postgres halves of `coordination_lease` (`main.rs:309-360`) and `scheduled_outbox` (`main.rs:188`).
  - Every broker cell of the read-model matrix (`distributed_read_model/main.rs:1001-1130` NATS/Rabbit/Kafka; `:1225-1264` Postgres×broker). The Postgres job runs that target with `--features postgres` only, and the broker jobs run only `--test <broker>_transport`.
- Why it matters: A silently skipped test cannot fail. The only place the full matrix executes is a developer running `make test-local`.
- Change: Replace the skip helpers with one `require_service(var)` that panics when `DISTRIBUTED_REQUIRE_SERVICES=1` (set in every integration job and in `make test-local`) and otherwise marks the test ignored rather than passed. Add one CI job that brings up `compose.yaml` and runs `cargo test --workspace --all-features` with that flag, replacing the hand-kept `--test` lists. Size M.

### 2. GraphQL, RBAC and query-protocol behaviour is proven on SQLite only
- Category: B (gap) / A (Principle 7)   Severity: high   Confidence: CONFIRMED
- Where: `tests/graphql_postgres/main.rs:20-63`, `tests/graphql_query_protocol_postgres/main.rs:164`, `tests/graphql_harden/main.rs` (SQLite-gated), `tests/support/graphql.rs:50-72`
- What: About 165 GraphQL integration tests run through a SQLite pool. Postgres gets four:
  - one list-query smoke test (never run with a database in CI);
  - one query-envelope test (never run in CI);
  - one literal-text test (run in CI);
  - one `#[ignore]` standby-routing test (run by `tests/gateway-postgres/run.py`).
  
  Tenant row filters, column allowlists, nested relationship denial, strict-where, depth/in-list limits and all 22 non-gateway live-subscription tests in `graphql_query_protocol` have no Postgres run in `tests/`. `graphql_harden/dialect.rs` shows the two dialects genuinely differ (JSON comparison operators).
- Why it matters: Postgres is the production backend and RBAC is a headline principle. The suite cannot detect a Postgres-only authorization or SQL-compilation bug.
- Change: Make the GraphQL fixtures generic over a `GraphqlPool` factory (SQLite in-memory, Postgres schema from `support/postgres.rs`) and instantiate `graphql_harden::{authz,softskip,residual,inject,dos}` and `graphql_query_protocol` for both, as the repository conformance macro already does. Delete `graphql_postgres` and `graphql_query_protocol_postgres`. Size L.

### 3. Transport "conformance" runs against fakes; real buses run 0–3 shared scenarios and hand-copy the rest
- Category: B/C/A (Principle 7)   Severity: high   Confidence: CONFIRMED
- Where: `tests/transport_conformance/mod.rs:226-515` (13 `source_*`/`dispatcher_*` scenarios using `FakeSource`/`FakePublisher`), `:787-880` (3 generic `bus_*` scenarios), `tests/transport_in_memory/main.rs` (whole file)
- What:
  - `transport_in_memory` does not test `InMemoryBus`; it runs the 13 fake-based scenarios, which are really unit tests of `run_source` and `OutboxDispatcher`. No real adapter runs them.
  - The three generic bus scenarios run on NATS, Postgres and SQLite (all 3), Kafka (2; `kafka_transport/main.rs:146` hand-rolls the listen case), RabbitMQ (1; `rabbitmq_transport/main.rs:168` and `:233` hand-roll the other two).
  - `InMemoryBus`, Redis streams (`src/bus/redis_streams.rs`) and the Knative bus run none of them.
  - Four failure-path tests are hand-written per broker under identical names in NATS, Kafka and RabbitMQ: `publish_then_consume_round_trips_through_*`, `message_id_and_metadata_survive_the_round_trip`, `retryable_failure_is_redelivered_then_succeeds`, `undecodable_payload_dead_letters_without_blocking`. SQLite has its own variants.
  - A real divergence is enshrined in a test: Kafka's `permanent_failure_skips_record_without_redelivery` (`kafka_transport/main.rs:287-292`, "no native dead-letter destination: commits past the record") versus `permanent_failure_routes_to_dead_letter_destination` on NATS and RabbitMQ.
- Why it matters: The suite cannot detect a backend that redelivers, orders or dead-letters differently. The one known difference (Kafka drops poison messages) is documented only in a test comment.
- Change: Move the 13 fake scenarios into `src/bus` unit tests. Make `transport_conformance` one macro `bus_conformance_tests!(factory)` covering point-to-point, fan-out, named group, metadata round-trip, retryable redelivery, permanent failure and undecodable payload. Instantiate it for all eight buses. Express the Kafka dead-letter difference as a declared capability the suite branches on, or give Kafka a DLQ topic. Size L.

### 4. `sqlite_transport` and `postgres_transport` are a copy-paste pair; so are `sqlite_repository` and `postgres_repository`
- Category: C   Severity: high   Confidence: CONFIRMED
- Where: `tests/sqlite_transport/main.rs:413-1092` vs `tests/postgres_transport/main.rs:473-1323`; `tests/sqlite_repository/main.rs` vs `tests/postgres_repository/main.rs`
- What: Line diff shows 804 of 1092 SQLite transport lines are identical in the Postgres file. Eleven tests have the same name and body shape, including `bus_listen_dead_letters_corrupt_queue_row_not_silently`, `stable_log_retry_keeps_the_original_cursor_and_rejects_conflicts`, `log_rewind_requires_an_explicit_fenced_reset` and `expired_queue_claim_cannot_be_settled_by_stale_worker`. The repository pair shares 437 of 756 lines and seven same-named tests, sitting outside the 22-case macro that already exists.
- Why it matters: Roughly 1,200 duplicated lines. A scenario added to one backend is not added to the other; SQLite has `retryable_event_failure_does_not_advance_offset` and Postgres does not.
- Change: Add `tests/sql_bus_conformance` generic over a small test trait for the SQL bus (construct, raw-execute, placeholder style), mirroring `src/bus/sql_bus_common.rs`. Fold the repository pair's shared tests into `persistent_repository_conformance`. Leave only truly dialect-specific cases per backend. Size M.

### 5. Both transport suites assert through private table and index names
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `tests/sqlite_transport/main.rs:437` (`SELECT count(*) FROM bus_queue`), `:488` (`SELECT last_seq FROM bus_offset WHERE consumer = 'projections'`), `:618` (`DROP INDEX bus_log_message_id_unique_idx`), `:633` (raw `INSERT INTO bus_log (...)`)
- What: 60 raw `sqlx::query` calls in the SQLite file and 56 in the Postgres file read or mutate bus tables directly.
- Why it matters: Any schema rename breaks dozens of tests with no behaviour change. Assertions like "offset equals max(seq)" restate the implementation.
- Change: Corrupt-row injection legitimately needs raw SQL; keep that behind one helper per dialect. Replace state assertions with observable behaviour: a second subscriber in the same group receives nothing; a re-publish with the same id is not redelivered. Size M, naturally done with finding 4.

### 6. Tests guard upgrade paths from schemas that were never released
- Category: B/F   Severity: medium   Confidence: CONFIRMED (tests); PLAUSIBLE (that the `src` code is removable — outside my slice)
- Where: `tests/sqlite_transport/main.rs:614, 671`; `tests/postgres_transport/main.rs:768, 831`; `tests/sqlite_repository/main.rs:22-52, 169-232`; `tests/postgres_repository/main.rs:25, 191`; `tests/upcasting/main.rs:78, 85`; `tests/core_command_contract.rs:54`
- What:
  - `legacy_equivalent_stable_id_duplicates_retain_the_minimum_cursor` and `legacy_conflicting_stable_id_duplicates_fail_preflight_without_mutation` (both backends) drop the unique index to "simulate legacy schema without uniqueness".
  - `projected_command_ledger_rows_upgrade_to_atomic_*` builds a migrator from migrations 0001–0003 only, inserts a `'projected'` row, migrates, and asserts `MAX(version) == 13`. That number breaks on every new migration.
  - `old_events_without_event_version_deserialize_as_v1` and `old_events_without_metadata_deserialize_with_empty_metadata` test deserialising pre-versioning records.
  - `neutral_declarations_preserve_the_pre_extraction_artifact` compares against a JSON fixture "captured from d188010d before changing the command implementation" — a one-off refactor guard.
- Why it matters: Nothing is released, so these protect compatibility with states no user has. They imply 13 migrations plus preflight dedup code that could be squashed.
- Change (breaking): Squash migrations to one baseline per dialect, delete the legacy preflight paths and these seven tests. Either delete `core-command-contract-v1.json` or rename the test to state that it is the v1 wire contract. Size M.

### 7. Two test files grep the repository's own source and docs
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `tests/legacy_authoring_absence.rs:13-157`; `tests/e2e_ui_celld_nats_profile/main.rs:17-70`
- What:
  - All seven tests in `legacy_authoring_absence.rs` `include_str!` files under `src/` and assert that substrings are absent (`"pub fn project<"`, `"CompiledCommandEffects"`, `"graph_workspace"`) or present (`"macro_rules! projection"`, `"mutation,"`, `"mod command_input_defaults"`).
  - `default_e2e_ui_host_stays_one_process` and `optional_profile_is_named_and_not_the_playground` assert that `host.rs` contains the comment text "single backend process", that a README contains "optional" and "make run", that a compose file contains `nats:2-alpine`, and that a worker source contains the sentence "projectors are not methods on this class".
  - The same pattern exists outside my slice at `tests/celld/main.rs:59,124,155` and `src/microsvc/cell_host/tests.rs:701`.
- Why it matters: The compiler already enforces absence of a deleted symbol. These fail on comment edits and formatting. The file's own comment at line 20 records a prior false positive ("Old gate rejected any `projection,` substring").
- Change: Delete `legacy_authoring_absence.rs` and the two fixture-grep tests; keep the `live` module in the profile target. If an API-absence guarantee is wanted, use one trybuild compile-fail case. Size S.

### 8. Two aggregate authoring forms mean four duplicated test targets
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `tests/upcasting/aggregate.rs:21-38` (`#[digest]` + `aggregate!`) vs `tests/sourced_upcasting/aggregate.rs:19-33` (`#[sourced]` + `#[event]`); pairs `upcasting`/`sourced_upcasting`, `enqueue`/`sourced_enqueue`, `snapshots`/`sourced_snapshot`, `todos`/`sourced`; `tests/replay_event_version.rs` (`exact_version_replays_with_both_macros`, `snapshot_prefix_tail_checks_versions_for_both_macros`)
- What: The same behaviours (hydrate v1 to v3 through chained upcasters, replay does not re-enqueue, snapshot round-trip) are tested once per form, often under the same test name. Fixture files share 97 of 163 lines (upcasting) and 61 of 123 (enqueue). `#[digest(` appears in only two files in the repo, both test fixtures; `#[sourced` appears 76 times across 54 files.
- Why it matters: Roughly 1,500 lines of tests keep a second authoring surface alive that nothing else uses.
- Change (breaking): Pick `#[sourced]`, remove `#[digest]`/`aggregate!` as public authoring, and merge the four pairs into one `aggregate_authoring` target. Whether `aggregate!` is needed internally is the macro reviewer's call. Size M.

### 9. Tests that cannot fail or assert nothing meaningful
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where and what:
  - `tests/graphql_oidc_keycloak/main.rs:17` `e0_skips_when_not_gated` (and its authentik and zitadel copies): body is an `eprintln!`.
  - `tests/graphql_sdl/main.rs:234` `capture_sdl_to_scratch`: returns unless `GROK_SCRATCH_SDL` is set; it is a developer dump tool.
  - `tests/graphql_engine/main.rs:162` `unknown_column_in_permission_via_grant_ok`: grants all columns on a valid schema and builds; no unknown column is involved.
  - `tests/graphql_engine/main.rs:190` `pure_sql_compile_helper`: asserts the SDL contains `"type Item"`; no SQL is compiled.
  - `tests/graphql_harden/softskip.rs:21` `default_engine_has_strict_where_on`: a getter check; `strict_unknown_where_key_errors` proves the behaviour.
  - `tests/sourced/main.rs:96` `enum_variants_exist_and_compile`, `:253` `partial_equality_on_enum`, `:271` `no_upcasters_by_default`.
  - `tests/sourced_enqueue/main.rs:96` `typed_event_enum_exists`, `:150` `custom_emitter_typed_enum`, `:159` `notifier_has_no_upcasters`.
  - `tests/sourced_upcasting/main.rs:10-31` `v1_has_no_upcasters`, `v2_has_one_upcaster`, `v3_has_two_upcasters`, with twins in `tests/upcasting`; the hydrate tests already prove the chain.
  - `tests/application_composition.rs:799` `no_linker_inventory_is_needed_for_explicit_application_selection`: asserts `module_ids() == ["todo"]`.
- Change: Delete them. Size S.

### 10. `sagas/orchestration.rs` tests its own fixture domain, not the framework
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `tests/sagas/orchestration.rs:23-455` (5 tests), `tests/sagas/order/*.rs` (about 490 lines of fixture aggregates)
- What: The test body is the orchestrator. It calls `inventory.reserve`, `saga.inventory_reserved()`, `payment.capture()` step by step and asserts fixture enum states. The only framework calls are `repo.get` and `repo.commit` on `InMemoryRepository`. The module doc describes it as an example.
- Why it matters: It can only fail if the fixture is wrong. There are four separate saga fixtures: `sagas/order`, `saga_transfer`, `persistent_repository_conformance/checkout_saga.rs`, and `distributed_read_model/checkout_saga_service`.
- Change: Delete `orchestration.rs`. Keep `microsvc_saga.rs` (`saga_orchestrated`, `saga_distributed`) and `saga_transfer` (timeouts, compensation, restart on SQLite), which drive `Service`, routes and the outbox. Move the order saga to `examples/` if it has documentation value. Size S.

### 11. `typed_commands` hand-assembles projection programs instead of using the public macro
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `tests/typed_commands/main.rs:403-1280`
- What: About 870 lines build `ProjectionProgram` values through `compile_projection`, `MutationEventBinding::try_new` and `descriptor_from_factories` (25 call sites). `projection! { ... }` is invoked in two places in my slice, both in `tests/sourced/flat_preview.rs`. Sixteen assertions match error text by substring (for example `:1326` `"TypeId mismatch"`, `:1350` `"structural fingerprint mismatch"`).
- Why it matters: The suite proves the intermediate representation works when built by hand. It does not prove that what a user writes with `projection!` yields a correct client manifest. Any change to the internal builder API breaks 870 lines of fixture.
- Change: Rewrite the eight fixture programs with `projection!`/`mutation!`; keep hand-built programs only for shapes the macro cannot express, moved into `src` unit tests. Replace substring error matches with a typed error kind. Size M.

### 12. GraphQL tests hand-write DDL beside `#[derive(ReadModel)]`
- Category: B/A (Principle 4)   Severity: medium   Confidence: CONFIRMED
- Where: `tests/support/graphql.rs:14-24` (model) with `:57-63` (hand-written `CREATE TABLE orders`); `tests/graphql_sqlite/main.rs` (19 `CREATE TABLE`); `tests/graphql_harden/dos.rs` (8); about 51 across GraphQL targets, versus 16 uses of `bootstrap_table_schema_for_dev`
- What: Rows are inserted with raw SQL into tables whose shape is retyped by hand, bypassing the declared read-model schema and the projection path.
- Why it matters: The schema generated from the model is never what GraphQL is tested against, so a model/DDL mismatch such as nullability or JSON storage type goes unnoticed. It is also why porting to Postgres (finding 2) is expensive.
- Change: Add one fixture helper, `seed::<M: ReadModel>(repo, rows)`, that bootstraps from the model registry and writes through a write plan. Size M, prerequisite for finding 2.

### 13. Almost no shared fixtures: each target rebuilds its own aggregate, read model, session and pool
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `tests/support/` (410 lines in six files: ids, env, outbox, sqlite, postgres, graphql)
- What:
  - About 50 aggregate definitions across 40 files.
  - Four near-identical `Todo` aggregates (`todos/aggregate.rs:5`, `sourced/aggregate.rs:4`, `snapshots/aggregate.rs:4`, `sourced_snapshot/aggregates.rs:9`), six `TodoV1..V3`, plus `DomainTodo` and `TodoHost`: 12 Todo-like aggregates. There are also separate `Counter` aggregates in `sqlite_repository`, `postgres_repository` and `microsvc`.
  - 96 `derive(ReadModel)` in 38 files.
  - 62 `sqlite::memory:` pool constructions in 33 files.
  - 30 local `*session*` helper functions.
  - `support/graphql.rs` is included by one target (`graphql_harden`).
  - Sharing is via `#[path = "../support/x.rs"]`, each file carrying `#![allow(dead_code)]`.
- Why it matters: The cost is not line count; fixtures drift. The four `Todo`s already differ in field visibility and methods.
- Change: Create a `tests/common` library crate (dev-dependency, `publish = false`) exporting one Todo aggregate, one Order/Parent/Child read-model set, session builders, `TempDb`, `PostgresTestSchema`, and the conformance macros. This also removes the `#[path]`/`dead_code` idiom. Size M.

### 14. Five different ways to run a scenario on several backends, and their case lists diverge
- Category: C/A (Principle 7)   Severity: medium   Confidence: CONFIRMED
- Where:
  - `tests/persistent_repository_conformance/mod.rs:27` — macro, one target per backend.
  - `tests/coordination_lease/main.rs:26-135` — generic `scenario_*` functions with hand-listed wrappers at `:137-360`.
  - `tests/schedule_worker/main.rs:312` — local `scenarios!` macro.
  - `tests/distributed_read_model/main.rs:865` — generic `run_matrix_cell`.
  - The transport and repository pairs — plain copy-paste.
- What: In `coordination_lease`, the in-memory module runs `invalid_inputs_rejected` and `concurrent_takeover_after_expiry_has_one_winner` (`:161, :201`); the SQLite and Postgres modules do not, and each has its own hand-copy of expiry takeover (`:277, :346`). In `schedule_worker`, SQLite and Postgres run five scenarios while memory runs one different test (`:574`).
- Change: Standardise on the repository macro pattern, which is the good one, for leases, locks, schedules and buses. Every backend then runs the identical list, and exclusions become explicit named capabilities. Size M.

### 15. Cross-backend work that should be kept and promoted
- Category: B (positive)   Severity: n/a   Confidence: CONFIRMED
- Where: `tests/persistent_repository_conformance/mod.rs:27-230`; `tests/graphql_oidc_common/mod.rs:141`; `tests/distributed_read_model/main.rs:865-1273`
- What:
  - 22 repository scenarios are each written once and run on in-memory, SQLite and Postgres through three 10–23 line entry files, including `aggregate_conflict_rolls_back_outbox`, `publish_failure_after_commit_retains_outbox_row_until_delivered` and `consumer_inbox_records_dedupes_and_fences_with_real_effects`.
  - The OIDC E1–E8 scenario is written once in `run_e1_through_e8`; per-provider files only mint tokens. Their only duplication is about 90 lines of `mint_client_credentials`/`decode_sub`/`urlencoding` shared by keycloak and authentik. CI correctly asserts the gate is set before running (`integration-graphql.yaml:69, 111, 151`).
  - The read-model persistence×bus matrix is one generic function instantiated per cell.
- Change: None, apart from moving the token helpers into the common module (S).

### 16. 87 test binaries; 51 are empty under default features; most jobs compile cold
- Category: D (CI)   Severity: medium   Confidence: CONFIRMED (structure); PLAUSIBLE (time saved — not measured)
- Where: `tests/` (70 `*/main.rs` plus 17 `*.rs`); `.github/workflows/on-pr-quality.yaml:39-43`; `test-all-features.yaml:34`; `integration-{kafka,nats,rabbitmq}.yaml`
- What:
  - 51 of 87 targets carry `#![cfg(feature = ...)]`, so the default-feature `cargo test` in the quality job compiles and links 51 empty binaries.
  - The all-features job links all 87 against a build that includes rdkafka, with no `rust-cache`. Kafka, NATS and RabbitMQ jobs also have no cache; Postgres, GraphQL, e2e-ui and celld do.
  - A PR runs about 14 workflows that each compile the crate with a different feature set.
  - `rust.yml` (coverage on main) repeats the all-features run under llvm-cov with no services, so coverage excludes every skipped test.
- Change: Consolidate into about eight integration binaries (`core`, `repository`, `transport`, `graphql`, `gateway`, `read_model`, `microsvc`, `observability`) with modules per current directory. Add `Swatinem/rust-cache` to the four uncached jobs. Merge the Postgres, NATS, RabbitMQ and Kafka jobs into the single compose-backed all-features job from finding 1, and collect coverage there. Size M.

### 17. Gateway tests do not gate main or releases; one fixture crate is never run
- Category: E   Severity: medium   Confidence: CONFIRMED
- Where: `.github/workflows/integration-gateway.yaml:3-28`; `on-push-main-version-and-tag.yaml:12-62`; `on-pr-quality.yaml:75`; `tests/fixtures/renamed-dependency-trybuild/`
- What:
  - The gateway workflow triggers on `pull_request` with path filters and manual dispatch only. It is not called from the main pipeline and is not in `version-and-tag`'s `needs`. It is the only place that runs the `#[ignore]`d `worker_*` tests, `tests/gateway-{portable,native,worker,auth}`, `gateway-postgres/run.py`, and `application_composition`/`application_plans`/`gateway_mounts` with their real feature set.
  - `integration-game-demos` is called on PRs but not on main; it is the only job that sets `REDIS_URL` for `redis_leaderboard`.
  - `renamed-dependency-trybuild` is a standalone crate with its own `[workspace]` and tests, referenced by no workflow, script or Makefile target.
- Change: Make the gateway workflow `workflow_call`, call it from both entry pipelines, and add it and `game-demos` to the release `needs`. Wire the trybuild fixture into the macros job or delete it. Size S.

### 18. Wall-clock assertions against real brokers and short TTLs
- Category: B (flakiness)   Severity: low   Confidence: CONFIRMED
- Where: `tests/nats_transport/main.rs:763, 828` (`latency < 250ms` against live JetStream), `:919` (`m2_policy + 1000ms < m1_projection`); `tests/queued_repo/main.rs:72` (50 ms sleep then "must still be blocked"); `tests/sql_lock_manager/main.rs:116, 151, 165`; `tests/coordination_lease/main.rs:283, 352` (200 ms TTL, 350 ms sleep)
- What: About 38 `sleep(` calls across 18 files. There are no fixed bound ports (servers bind `127.0.0.1:0`), no serial requirements, and the polling loops I inspected have deadlines (`schedule_worker/main.rs:296-309`).
- Why it matters: The two NATS 250 ms bounds are the likeliest flakes on a loaded runner. The negative-window sleeps can pass falsely but not fail falsely.
- Change: For in-process cases use `tokio::time::pause`/`advance`, or the store clock that `schedule_worker`'s memory test already uses. For NATS, assert relative ordering (fresh message handled before the poison's nth retry) instead of an absolute bound. Size S.

### 19. Test Postgres schemas are never dropped; local and CI service versions differ
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `tests/support/postgres.rs:13-101` (no `Drop`; `distributed_read_model/main.rs:1201` comments "The schema has no Drop"); `compose.yaml:68` (`nats:2.15-alpine`) vs `integration-nats.yaml` (`nats:2.10-alpine`) vs `e2e_ui_celld_nats_profile/main.rs:56` (`nats:2-alpine`); `compose.yaml:21` (`postgres:18`) vs CI (`postgres:18-alpine`)
- What: Each Postgres test leaves an `sr_<prefix>_<nanos>_<n>` schema behind; with `KEEP_COMPOSE=1` they accumulate indefinitely. CI and local run different NATS versions.
- Change: Drop the schema at the end of the test via an async `cleanup()` or a per-run database. Have CI start services from `compose.yaml` (also fixes finding 1). Size S.

### 20. Principle-level behaviours with no end-to-end test in `tests/`
- Category: B (gap)   Severity: medium   Confidence: PLAUSIBLE (searched by test name and grep; bodies of unread targets not checked)
- What I looked for and did not find:
  - **Projection rebuild through the public service API** (drop a read model, replay from the event store, same rows and same GraphQL answer). Rebuild is tested only at store level in `src/projection/source_snapshot_tests.rs:719-1012` and for Redis in `redis_leaderboard/main.rs:68`.
  - **RBAC on the live/subscription path on Postgres**, and RBAC on every query path on Postgres generally (finding 2). On SQLite, `row_filtered_surface_never_exposes_partition_wide_live_activity` (`graphql_query_protocol/main.rs:1223`) covers the live path.
  - **Idempotent command retry on Postgres** (same command id twice yields one effect and the same receipt). SQLite and in-memory are covered by `causal_wait_path` (`cell_wait_path_replays_once_after_internal_failure`, `celld_host_reclaims_an_ambiguous_receipt_with_the_same_causation`). Postgres has only a lib test that CI runs by name.
  - **Out-of-order or duplicate event delivery to a projector across real buses.** Only `src` has `source_snapshots_postgres_reordering_and_restart`; no bus-level conformance scenario delivers e2 before e1.
  - **Server/browser mutation-program parity** is covered by shared golden fixtures consumed by both `src` tests and `js/tests` (`tests/fixtures/mutation-program-v1.json`, `projection-delta-v1.json`, `client-query-plan-corpus.json`). That is sound, but nothing regenerates the fixtures and fails on drift from the Rust compiler output, as far as I could see.
- What is covered: crash between event append and outbox publish (`publish_failure_after_commit_retains_outbox_row_until_delivered`, `expired_outbox_lease_is_reclaimed_by_second_worker`, both on all three repositories; `schedule_worker::crash_between_commit_and_settle_does_not_fire_twice` on SQLite and Postgres).
- Change: Add `projection_rebuild` and `command_retry` scenarios to the repository conformance macro, and a "duplicate plus reordered delivery" scenario to the bus conformance macro from finding 3. Size M.

### 21. Manifest and contract tests are legitimate wire-contract tests but brittle
- Category: B   Severity: low   Confidence: CONFIRMED (sampled, not exhaustive)
- Where: `tests/application_composition.rs:600-660`; `tests/typed_commands/main.rs:1683-1730`; `tests/aggregate_command/main.rs:404-472`
- What:
  - `application_manifest_is_byte_deterministic_and_contains_no_executable_data` asserts the bytes do not contain the substrings `"handler"` or `"application_composition"`; it would fail if a user named a column "handler".
  - Many tests index `manifest.commands[0]` and `projection_programs[0].arms[0].operations[0]`.
  - The `generated_*_equal_the_hand_written_*` tests compare the macro's output with a hand-written registration. That is a sound equivalence oracle; keep it.
- Change: Replace the substring-absence checks with structural ones (no key outside the schema). Find manifest entries by name, not index. Size S.

## Things that are good and should not be changed

- **`repository_conformance_tests!`** (`tests/persistent_repository_conformance/mod.rs`): one case list, three backends, real rollback, lease and inbox scenarios. Extend this pattern to buses, locks, leases and GraphQL; do not replace it.
- **The persistence×bus matrix** (`run_matrix_cell` in `tests/distributed_read_model/main.rs`) and the shared OIDC scenario `run_e1_through_e8`. Both are correct; they need CI to actually execute them.
- **Golden wire vectors in `tests/fixtures/*.json` consumed by both Rust and `js/tests`.** This is the right way to enforce "no TypeScript twin" (Principle 5).
- **`graphql_harden`** is organised by threat category, drives the shipped `GraphqlEngine::execute`, and checks that errors do not leak SQL (`assert_no_sql_leak`). **`graphql_query_protocol`** asserts the client-visible envelope, including that hidden aliases and omitted primary keys never leak.
- **Failure-injection tests on real stores:** `schedule_worker` (crash between commit and settle, concurrent workers, backoff), `saga_transfer` (timeouts, late-reply tombstones, restart on SQLite), `replay_property` (proptest that reload and snapshot hydration equal in-memory state), `snapshot_sqlite_hardening`.
