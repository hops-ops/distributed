# Testing

**One behaviour, one owner.** Each behaviour is asserted in detail by exactly
one layer of the suite. Other layers may pass through it; they do not assert
it again. Before you add a test, name the behaviour, find the layer that owns
it (the table below), and add a case there.

## Where does my test go

| You are testing | It goes in | Example in this repo |
|---|---|---|
| Pure logic: parsing, validation, state transitions, codecs, planners | a unit test beside the code (`#[cfg(test)]` module or `*tests.rs` under `src/`) | `src/bus/wire_envelope.rs` (`encode_then_decode_round_trips_every_field`); `src/graphql/projection_delta/canonical.rs` |
| A contract with several implementations: repository, bus, lock, lease, schedule | a case in the shared conformance suite in `tests/common`, which every backend instantiates | `tests/common/src/repository/`, instantiated in `tests/repositories/sqlite_repository_conformance/mod.rs` and its in-memory and Postgres siblings |
| The same, for the projection store | a scenario in `src/projection_protocol/store/scenario_tests.rs` | instantiated in `src/in_memory_repo/projection_protocol/tests.rs` and `src/sqlx_repo/projection_protocol/{tests,postgres_tests}.rs` |
| A GraphQL behaviour that must hold on SQLite and Postgres | a case in an `on_each_dialect!` list | `tests/graphql/graphql_harden/mod.rs` (`authz::claim_row_filter_isolates_tenants`) |
| Something only one backend has | that backend's module in the area binary | `tests/repositories/postgres_repository/mod.rs` (`backend_termination_mid_commit_rolls_back_and_nothing_persists`) |
| Two or more components wired together: command to outbox to bus to projection, GraphQL over HTTP, a transport into the command ledger | a scenario module in the area binary, `tests/<area>/` | `tests/transports/transport_typed_commands`; `tests/graphql/graphql_http`; `tests/services/saga_transfer` |
| What only a deployed application shows: login through a real identity provider, the browser, lifecycle tooling | `tests/e2e-ui` (Rust suite and Playwright), the demos | `tests/e2e-ui/e2e/todos.user.spec.ts`; `tests/e2e-ui/crates/suite/tests/behavioral.rs`; `distributed_cli/tests/bats/lifecycle.bats` |
| What a derive or attribute macro generates | behavioural tests on real derived types, in a crate that uses the macro | `tests/core/sourced_snapshot`; `tests/read_models/read_model_metadata` |
| A macro must reject an input with a clear error | a compile-fail fixture with a pinned `.stderr` | `distributed_macros/tests/compile_fail/` |
| Macro argument parsing | a unit test in the macro crate | `distributed_macros/src/entry_tests.rs` |
| A wire format or schema that must not drift | a golden test against a fixture in `tests/fixtures/` | `tests/core/core_command_contract.rs` with `tests/fixtures/core-command-contract-v1.json` |
| What the CLI writes | a unit test of the generator | `distributed_cli/src/client_compiler/tests.rs`; `distributed_cli/src/atlas.rs` (`mod tests`) |
| What the CLI does when run: arguments, exit codes, files on disk | an integration test that runs the built binary | `distributed_cli/tests/cli_skills_init.rs` |
| JS client logic | `js/tests/*.test.mjs`; a contract with several implementations is a conformance file there | `js/tests/cache-engine-conformance.test.mjs`; `js/tests/island-adapter-conformance.test.mjs` |

Add a test at a second layer only for a different trust boundary (a test that
calls the public API from outside the crate, or the HTTP edge) or for a
failure the owner cannot produce. Say which in the test's doc comment.

## The rules

1. **One owner.** A behaviour has one layer that asserts it in detail.
   Others pass through it.
2. **Which layer owns what.**
   - Pure logic with no I/O: unit tests beside the code. Edge cases and error
     cases live here.
   - A contract with more than one implementation: one conformance suite,
     instantiated for every implementation with the same case list. A
     difference between backends is a declared capability, never a missing
     case.
   - Behaviour that needs two or more components wired together: a scenario
     in the area binary, one happy path plus the failures that only exist at
     that seam.
   - What only a deployed application can show: e2e-ui and the demos. Few
     tests, each a user-visible journey.
   - Macro output: behavioural tests on real derived types plus compile-fail
     fixtures. Argument parsing is pure logic and is unit-tested in the macro
     crate. Do not assert on expanded token text.
3. **Before adding a test**, find the owner and add a case there. A second
   layer needs one of the two reasons under the table, named in the test's
   doc comment.
4. **Shape.** Inputs that differ only in data are one table-driven test with
   a named case per row: a slice of a small struct with a `name` field, the
   name in every assertion message, no new dependency. A test name states the
   behaviour and the expected result. Fixtures for integration tests come from
   `tests/common` (the Todo aggregate is `tests/common/src/todo.rs`; do not
   copy it).
5. **Honesty.** A test that reports PASS has run. See
   [Live tests and services](#live-tests-and-services). No sleeps for
   ordering; wait on the event. No retries: `.config/nextest.toml` sets
   `retries = 0`, and a flaky test is a broken test.
6. **Protected tests.** Never removed for overlap alone: security negatives
   (authorization, spoofing, injection, resource limits) and any test whose
   doc comment names a trust boundary; wire-format, golden-file and
   frozen-fingerprint tests; fail-closed protocol cases; ordering, fencing and
   idempotency tests; tests of macro-generated code; a backend's instance of a
   conformance suite; a regression test that an incident or a commit message
   names. Line coverage cannot see what a macro expands into the test crate,
   so two such tests can share every covered line and still test different
   things.

## The layers

Counts are approximate (attributes and macro instances counted with `rg`,
2026-11-26).

### Unit tests (`src/**`)

`#[cfg(test)]` modules and `*tests.rs` files beside the code: about 1,250 in
the root crate, 250 in `distributed_cli`, 125 in `distributed_game`, a few
dozen in the macro crates and `distributed_leaderboard`.
`cargo test -p distributed --lib` runs the root crate's.

A unit test may use a real database or broker when it needs crate-private
items; it reads the address through `crate::test_env::require_service`, the
same guard as the integration tests (`tests/common/src/env.rs`). Examples:
`src/sqlx_repo/statement_count_tests.rs`, `src/projection/source_snapshot_tests.rs`.
Unit tests may use `common` only for helpers that name no `distributed` type:
`common` is built against the non-test build of the crate, so its domain types
do not unify with `crate::` types.

### Conformance suites

One case list per contract, expanded once per implementation. Every
expansion skips (returns early) only when its constructor returns `None`,
which goes through `require_service`.

| Contract | Suite | Expander | Instances |
|---|---|---|---|
| Repository | `tests/common/src/repository/` | `repository_conformance_tests!` | `tests/repositories/{in_memory,sqlite,postgres}_repository_conformance` (each also through `QueuedRepository`) |
| Lock | `tests/common/src/lock.rs` | `lock_conformance_tests!` | `tests/repositories/sql_lock_manager` |
| Lease | `tests/common/src/lease.rs` | `lease_conformance_tests!` | `tests/repositories/coordination_lease` |
| Schedule worker | `tests/common/src/schedule.rs` | `schedule_conformance_tests!` | `tests/repositories/schedule_worker` |
| Bus | `tests/common/src/transport/bus.rs` | `bus_conformance_tests!` | `tests/transports/*/mod.rs`: in-memory, SQLite, Postgres, NATS, Kafka, RabbitMQ, Redis Streams, Knative |
| SQL bus | `tests/transports/sql_bus_conformance/` | `sql_bus_conformance_tests!` | `tests/transports/{sqlite,postgres}_transport` |
| GraphQL dialect | the invoking module's scenarios | `on_each_dialect!` (`tests/common/src/graphql.rs`) | `tests/graphql/{graphql_harden,graphql_query_protocol,graphql_compile}` |
| Projection store | `src/projection_protocol/store/scenario_tests.rs` | `projection_protocol_scenario_tests!` | `src/in_memory_repo/projection_protocol/tests.rs`, `src/sqlx_repo/projection_protocol/tests.rs`, `src/sqlx_repo/projection_protocol/postgres_tests.rs` |

The repository, lock, lease and schedule macros expand through
`conformance_cases!` (`tests/common/src/lib.rs`), where a capability
(`sql_faults: true`, `controlled_clock: false`, ...) is how a backend leaves
cases out: it must be stated for every backend, and claiming one the backend
does not implement does not compile. A new backend contract uses
`conformance_cases!`.

Two suites live outside `tests/common`, for reasons:

- **The SQL bus suite** is in the `transports` binary because only that binary
  uses it.
- **The projection store suite** is in `src/` because it cannot be written
  against the public API. It starts with `use super::*` and its backend
  trait, `ProjectionProtocolScenario`, is `pub(crate)` and requires
  `ProjectionProtocolStore`, which is also `pub(crate)`. Its scenarios build
  `TrustedProjectionInput`, `ProjectionCommitBatch`, `ProjectionFailureBatch`,
  `RecordTransition`, `ProjectionRecordExpectation` and other `pub(crate)`
  items of `src/projection_protocol/store`. Moving it would mean widening
  that visibility, which is not done for tests.

`on_each_dialect!` is its own mechanism on purpose: it generates `sqlite::`
and `postgres::` modules from a nested case list whose scenarios take a
`Db`, and no case is dialect-gated. Its Postgres constructor goes through
`require_service` like every other suite.

### Area binaries (`tests/<area>/`)

The root crate's integration tests are eight binaries, one per area: `core`,
`gateway`, `graphql`, `observability`, `read_models`, `repositories`,
`services` and `transports` (`tests/<area>/main.rs`). Each former test
target is a module, so `cargo test --test <area> <module>::` runs one of them.
They hold the conformance instances above, backend-specific modules, and
scenarios across components. `tests/services` holds the demo-style scenarios
(`todos`, `bomberman`, `sagas`, `saga_transfer`, `microsvc`).

`distributed_cli/tests`, `distributed_game/tests` and
`distributed_leaderboard/tests` are the same layer for those crates.

### Full stack

Outside the workspace's `cargo test`: `tests/e2e-ui` (its own workspace: the
Rust suite in `tests/e2e-ui/crates/suite`, Playwright specs in
`tests/e2e-ui/e2e`, UI node tests in `tests/e2e-ui/ui/tests`),
`tests/e2e-celld`, `examples/game/server`, and the bats lifecycle suite. They
are kept for wiring, not line coverage.

### Macros and compile-fail

Behavioural tests of generated code are in the area binaries, because
generated code names `distributed::…` paths and only works from another
crate. Compile-fail tests are trybuild fixtures (`distributed_macros/tests/`,
`distributed_game_macros/tests/compile_fail/`) and `rustc_fail` fixtures
sharing the harness `tests/support/rustc_fail.rs`. Regenerate trybuild
snapshots with `TRYBUILD=overwrite cargo test -p distributed_macros`.

### JavaScript

The same rules. `js/tests/*.test.mjs` is the unit and conformance layer of
`@hops-ops/distributed` (`cd js && npm test`); a contract with several
implementations has a `*-conformance.test.mjs` file. Playwright in
`tests/e2e-ui` is the full-stack layer.

### Fixtures

- Crates: `tests/gateway-{portable,native,worker,postgres}`,
  `tests/fixtures/renamed-dependency`, `tests/fixtures/wasm32-native-domain`.
- A JS fixture: `tests/gateway-auth`.
- Golden wire-format data: the rest of `tests/fixtures/` (`*-v1.json`,
  `*.graphql`, `migrations/`). A golden file changes only with a deliberate
  format change.

## Live tests and services

`make test-local` starts Postgres, RabbitMQ, Kafka, NATS and Redis. A test
that needs one of them reads its address through `require_service`
(`tests/common/src/env.rs`): without the variable it prints `SKIPPED` and
returns, unless `DISTRIBUTED_REQUIRE_SERVICES` names the variable, in which
case it fails. `make test-local` names all of them, so in that run (and in
CI's services job) such a test cannot pass without running. No other early
return.

A **live test** needs something `make test-local` cannot start (celld, an
identity provider, an OTel collector, a browser). It is marked
`#[ignore = "live: needs <what>"]`, so a local or services-job run reports
it as ignored, never as passed, and it is run by the job that provides the
service, with `-- --ignored`. A live test that reads its address does so
with `common::env::require_live`, which panics when the variable is unset:
under `--ignored` a missing service is a failure. The live tests and the
jobs that own them:

| Tests | Needs | Run by |
|---|---|---|
| `tests/services/celld.rs` (4) | a celld host at `CELLD_URL` | `make -C tests/e2e-ui test-celld-http` (`integration-celld.yaml`, job `live`) |
| `tests/services/e2e_ui_celld_nats_profile` (1) | `CELLD_URL`, `NATS_URL` | `make -C tests/e2e-ui test-celld-nats` (same job) |
| `graphql_oidc_{zitadel,keycloak,authentik}::e1_through_e8_live` (1 each) | a bootstrapped identity provider (`OIDC_ISSUER`) | `integration-graphql.yaml`, one job per provider |
| `otel_export::dispatch_span_reaches_collector_with_propagated_parent` | an OTel Collector | `integration-observability.yaml` |
| `distributed_game` `celld_facts`, `celld_zone_lease`, `celld_zone_state` (1 each), `world` (2 of 21), `celld_zone` (5) | a built test Worker and celld (`fleet` also docker and the aws CLI) | `integration-celld.yaml`, job `storage` |
| the other ignored tests: `graphql_unique_key_live`, the gateway worker, load and Postgres-replica tests, the ignored `distributed_cli` tests | a browser, Node, wasm32, a Postgres replica | their own jobs (`integration-e2e-ui.yaml`, `integration-gateway.yaml`, `tests/gateway-postgres/run.py`, `integration-distributed-cli.yaml`) |

Two more ignored tests are measurements run by hand, not live tests:
`distributed_game` `alloc_report` and `replication::tests::dense_room_patch_build`.

The celld, OIDC and OTel jobs run their live tests through
`scripts/expect-tests-passed.sh <count>`,
which fails unless exactly that many passed: under `--ignored` a filter that
matches nothing is green with `0 passed`. A new live test in a job raises the
job's count. Never write a test that returns early when its service is
missing.

### First-class OIDC (Zitadel, Keycloak, Authentik)

GraphQL identity is built into the engine (`OidcBearer`: JWKS, iss/aud/exp,
claim → role/session). Live against three local IdPs — not mocks only:

| Provider | Compose + bootstrap (writes the env file) | Live test, after `set -a && source <env file> && set +a` |
|---|---|---|
| **[Zitadel](../../tests/graphql/graphql_oidc_zitadel/)** (reference) | `./scripts/oidc-zitadel-up.sh` (`graphql-oidc.env`) | `cargo test --test graphql --features graphql,sqlite,metrics graphql_oidc_zitadel:: -- --ignored` |
| **[Keycloak](../../tests/graphql/graphql_oidc_keycloak/)** | `./scripts/oidc-keycloak-up.sh` (`graphql-oidc-keycloak.env`) | `cargo test --test graphql --features graphql,sqlite,metrics graphql_oidc_keycloak:: -- --ignored` |
| **[Authentik](../../tests/graphql/graphql_oidc_authentik/)** | `./scripts/oidc-authentik-up.sh` (`graphql-oidc-authentik.env`) | `cargo test --test graphql --features graphql,sqlite,metrics graphql_oidc_authentik:: -- --ignored` |

Shared **E1–E8** in [`tests/common/src/oidc.rs`](../../tests/common/src/oidc.rs).
Each provider's test is live (ignored by default) and fails without
`OIDC_ISSUER`. Offline (mock JWKS, no Docker):
`cargo test --test graphql --features graphql,sqlite graphql_identity::`.
CI runs the three providers in `.github/workflows/integration-graphql.yaml`.

e2e-ui boots **Zitadel** for the browser path; the three stacks prove the
same `OidcBearer` edge is not vendor-locked.

## Running Tests

```bash
make test                   # = make test-local: compose up, the whole workspace
                            #   with --all-features against real services
                            #   (cargo nextest run, then cargo test --doc)
make test-cargo             # the same commands, no compose (bring your own services)
make test-cli-lifecycle     # builds the CLI and runs distributed_cli/tests/bats/lifecycle.bats
make compose-up             # start postgres, rabbitmq, kafka, nats, redis (compose.yaml)
make compose-down
cargo test                  # default feature set
cargo test --all-features   # all features; service tests skip without their env vars,
                            #   live tests are ignored
```

By layer:

```bash
cargo test -p distributed --lib                          # root crate unit tests
cargo test --all-features --test repositories            # one area binary
cargo test --all-features --test graphql graphql_harden::   # one module of it
cargo test -p distributed_cli -- --include-ignored       # CLI (needs wasm32-unknown-unknown and Node)
cargo test -p distributed_macros                         # macro unit and compile-fail tests
cd js && npm test                                        # JS unit and conformance
cd tests/e2e-ui && make test                             # full stack, offline
```

`make test-local` is what CI's services job runs
(`.github/workflows/integration-services.yaml`). It recreates the per-test
Postgres databases, exports every service URL, and sets
`DISTRIBUTED_REQUIRE_SERVICES`, so a test that cannot reach its service fails
instead of skipping (see [`tests/common/src/env.rs`](../../tests/common/src/env.rs)).
Set `KEEP_COMPOSE=1` to leave the services running afterwards.

Both targets run the tests with [cargo-nextest](https://nexte.st)
(`cargo install cargo-nextest --locked`), one process per test, then the
doctests with `cargo test --doc`, which nextest does not run.
`.config/nextest.toml` sets no retries and a slow-test timeout (reported at
60 s, killed at 5 min). Its `ci` profile (`NEXTEST_PROFILE=ci make test-local`,
what CI runs) does not stop at the first failure and writes
`target/nextest/ci/junit.xml`, which the services job uploads when it fails.

One process per test means an in-process lock serializes nothing, and a name
made from the clock and a per-process counter repeats across processes.
Build every generated test name (schema, temp file, subject, queue, stream,
id) with `common::ids` (`token`, `unique`, `unique_id`), which mixes in the
process id; `distributed_game` has the same rule in
`distributed_game/tests/support/unique.rs`. Tests that cannot get a namespace
of their own run in a nextest test group with `max-threads = 1`:

| Group | Tests | Why |
|---|---|---|
| `compile-fail` | the trybuild and `rustc_fail` binaries of `distributed_macros` and `distributed_game_macros` | they build scratch crates in shared directories under `target/tests` |
| `lifecycle-project` | the five ignored `distributed_cli::cli_lifecycle` tests that compile the lifecycle fixture project | one shared Cargo target directory |
| `celld-dev` | `distributed_game::celld_zone` (live, so only under `--run-ignored`; CI runs it with `cargo test`, where its in-process lock serializes) | `celld dev` instances race for the Worker bundler |
| `postgres-projection-views` | `postgres_projection_protocol_runtime_conforms`, `postgres_redelivery_identity_is_atomic_across_partition_and_cursor_races` | NOTIFY channels are database-wide; both use the table name `postgres_projection_views` |

### Transport Integration Tests

The transport adapters have integration tests against real brokers or a local
SQLite database. Broker tests are feature-gated and **skip when their env var is
unset** (unless `DISTRIBUTED_REQUIRE_SERVICES` names it); SQLite uses a
temporary database file and needs no Docker service.

```bash
make compose-up   # postgres, rabbitmq, kafka, nats, redis (see compose.yaml)

DATABASE_URL=postgres://sourced:sourced@localhost:5432/distributed \
  cargo test --test transports --features postgres postgres_transport::
cargo test --test transports --features sqlite sqlite_transport::
NATS_URL=nats://localhost:4222 \
  cargo test --test transports --features nats nats_transport::
AMQP_URL=amqp://guest:guest@localhost:5672/%2f \
  cargo test --test transports --features rabbitmq rabbitmq_transport::
KAFKA_BROKERS=127.0.0.1:9092 \
  cargo test --test transports --features kafka kafka_transport::
REDIS_URL=redis://localhost:6379 \
  cargo test --test transports --features redis-streams redis_streams::
```

In CI every broker test runs in the one services job above; the other
`.github/workflows/integration-*.yaml` jobs cover the gateway, GraphQL, CLI,
observability, e2e-ui, JS, game demos, celld and wasm32 domains.

## Coverage Reporting

`make coverage-local` gives one line coverage number for the workspace's
production code. It starts the compose services, recreates the test databases
and exports the service variables as `make test-local` does (one shared
definition in the `Makefile`), runs the same nextest suite under
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov), and prints the
lines covered per crate and in total:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov cargo-nextest --locked
make coverage-local        # KEEP_COMPOSE=1 keeps the services up afterwards
```

It honours `CARGO_TEST_ARGS` (default `--workspace --all-features`) and
writes `target/coverage/lcov.info` (for an editor or another tool) and
`target/coverage/summary.json`. Tests ignored by default (the live tests) do
not run. It builds in `target/llvm-cov-target`, cargo-llvm-cov's default and
the directory `make test-footprints` uses: their dependency builds are the
same, so each reuses the other's. Every run first removes old profiles and
the workspace crates' artifacts (`cargo llvm-cov clean --workspace`), so the
workspace crates rebuild each time and no earlier run leaks into the number.
Do not run the two at once.

What the number counts:

- Test files are excluded: everything
  [`classify.py`](../../scripts/test-footprints/classify.py) calls test or
  example code (`tests/`, `benches/`, `examples/`, `tests.rs`,
  `*_test(s).rs`, `test_*.rs`, modules declared under `#[cfg(test)]`).
  [`ignore_regex.py`](../../scripts/coverage/ignore_regex.py) turns that
  classification into the `--ignore-filename-regex` both reports use, and
  [`summary.py`](../../scripts/coverage/summary.py) fails if the report still
  holds a file the classifier calls test code. `tests/common`,
  `examples/quickstart` and `tests/fixtures/renamed-dependency` are workspace
  members made only of such files, so they are not reported.
- Inline `#[cfg(test)] mod tests { ... }` blocks cannot be excluded by file
  name: they count as production lines, so this number sits slightly above
  true production coverage. The footprint tool (next section) subtracts them
  and is the exact figure.
- Doctests are not included (stable cargo-llvm-cov cannot instrument them).
- `distributed_macros` and `distributed_game_macros` are proc-macro crates,
  which run inside rustc: their figure is their expansion while the workspace
  builds plus their own unit tests.

CI runs `make coverage-local` in
[`coverage.yaml`](../../.github/workflows/coverage.yaml) on every push to
`v5` and `main` (not on pull requests: the instrumented build roughly doubles
the services job). The job summary shows the per-crate table and the TOTAL,
and `lcov.info` is uploaded as the `workspace-coverage-lcov` artifact.
Release tagging does not wait on it. The pull request `quality` job reports
no coverage.

Scripts tested by `python3 -m unittest discover -s scripts/coverage`.

## Per-test coverage footprints

`make test-footprints` records which production lines each test executes. Use
it before deleting or merging tests (no covered line may be lost), to find
tests whose lines another test already covers, and to find uncovered code.
It is a local tool, not part of CI. The scripts are in
[`scripts/test-footprints/`](../../scripts/test-footprints/).

```bash
make compose-up                    # the services make test-local uses
make test-footprints               # every test, one instrumented process each
                                   #   -> target/test-footprints/
cp -R target/test-footprints target/test-footprints-base   # keep a baseline
# ... change tests ...
make test-footprints
make test-footprints-diff BASE=target/test-footprints-base
```

It needs `cargo-nextest`, `cargo-llvm-cov` and `rustup component add
llvm-tools-preview`, and on macOS a bash outside the system paths
(`brew install bash`; see `scripts/test-footprints/runner.sh` for why).
The target recreates the test databases and exports the service variables as
`make test-local` does, builds instrumented into `target/llvm-cov-target`
(the normal `target/` build stays warm), and runs nextest's `footprints`
profile (`.config/nextest.toml`), which keeps the test groups. A failing test
does not stop the run; it is listed in `summary.txt`. The make target fails
only when the tooling does.

Each run replaces `target/test-footprints/`, so copy a baseline aside before
the next run. `ARGS="..."` passes arguments to nextest; such a partial run
must name its own output directory with `OUT=`, and never writes over the
full run:

```bash
make test-footprints ARGS="-p distributed_macros" OUT=target/footprints-macros
make test-footprints ARGS="-p distributed_cli --run-ignored all" OUT=target/footprints-cli-all
```

`--run-ignored all` runs the live tests of the selected packages too, so
select packages whose ignored tests can run here (see
[Live tests and services](#live-tests-and-services)).

Outputs, in the output directory:

| File | Contents |
|---|---|
| `summary.txt` | commit, tests listed / run / failed / without coverage, production line coverage by crate and module, the unit/integration layer split, minimal cover sizes, overlap counts, failed tests, wall time |
| `per_test.csv` | `binary`, `test`, `group`, `layer`, `production_lines`, `unique_lines` (lines no other test reaches), `single_containing_test` (the smallest test whose lines include all of this one's), `in_minimal_cover`, `seconds`, `footprint_hash`, `status` (`passed`, `failed`, or `no-coverage` when the test left no profile, for example when it was killed) |
| `lines/` | one file per test, `<binary>__<test>.txt` (`/` written `%2F`), its production lines as sorted `path:line` |
| `identical_groups.txt` | tests with the same non-empty footprint, one group per line: the line count, then each test (`<binary> <test>`), tab-separated |
| `cross_binary_pairs.csv` | `test`, `containing_test`: a test whose lines all lie inside one test of another binary |
| `covered_lines.txt` | every covered production line, `path:line` |
| `uncovered_by_file.csv` | `file`, `lines`, `covered`, most uncovered first |
| `uncovered_ranges.txt` | `path:start-end` for each run of uncovered production lines |
| `sources.tar.gz` | the production sources measured, which the diff uses to follow lines that moved |

Production lines exclude test files (`tests/`, `benches/`, `tests.rs`,
`*_test(s).rs`, `test_*.rs`), modules declared under a test-only cfg
(`#[cfg(test)] mod x;`), and items under one (`#[cfg(test)] mod tests { .. }`).
Examples are reported apart. The rules are
[`classify.py`](../../scripts/test-footprints/classify.py), tested by
`python3 -m unittest discover -s scripts/test-footprints`.

`make test-footprints-diff BASE=<dir> [OUT=<dir>]` prints the production
lines covered in `BASE` and not now, with the tests that covered each before,
the tests added and removed, and the change in total coverage. It exits
non-zero when a covered line was lost. Lines that moved because a file was
edited are compared at their new number; lines whose text changed are listed
apart and do not fail the diff.

Two runs of one commit do not cover exactly the same lines: timeouts, slow
consumers and lease renewals run only on a slow or loaded machine. Lines seen
to flap are listed with their text in
`scripts/test-footprints/timing-dependent-lines.txt`; the diff reports them
but does not fail on them. An entry matches only while the baseline holds
that text at that line, so when the code moves the line is reported again and
the entry needs updating. More runs keep finding a few more such lines; a
lost line none of whose covering tests was removed is marked in the output as
possibly timing-dependent, and is added to the list only after two runs of
one commit disagree on it. A test that fails in either run loses the lines only
it reaches: the diff names such tests in a warning, and the comparison is only
meaningful between two runs without failures.

What a footprint cannot show: what a test asserts (two tests with the same
lines can check different things), code a macro expands into the test crate,
and third-party code. Proc-macro crates run at compile time, so their
footprints understate them. Per-test seconds include process start and
instrumentation and are only comparable with each other.
