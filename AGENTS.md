# Working in this repository

How to build, test and check changes here. What the framework is and why it
is shaped this way is in [README.md](README.md); the reference is in
[docs/guides/](docs/guides/).

This project tracks its work in GitKB; see `.kb/AGENTS.md` for that workflow.

## Layout

| Path | What it is |
|---|---|
| `src/` | The `distributed` crate. `src/lib.rs` includes `README.md` as crate docs, so README Rust blocks are doctests. |
| `distributed_macros/` | Proc macros, re-exported by `distributed`. Generated code names `distributed::…` paths, so it only works from another crate. |
| `distributed_cli/` | The `distributed` CLI (`build`, `dev`, `scaffold`, `describe`, …) and its library. |
| `distributed_game/`, `distributed_game_macros/` | The multiplayer zone runtime. |
| `distributed_leaderboard/` | Leaderboards as a projection target. |
| `tests/common/` | Shared test support, including the bus conformance suite. |
| `tests/{core,gateway,graphql,observability,read_models,repositories,services,transports}/` | The eight integration test binaries of the `distributed` crate (`cargo test --test <name>`). |
| `examples/quickstart/` | The no-Docker quickstart crate (workspace member). |
| `js/` | `@hops-ops/distributed`, the JS/TS client. |
| `tests/e2e-ui/` | The full-stack playground and template. Its own Cargo workspace and `Cargo.lock`. |
| `tests/e2e-celld/` | The same application on celld cells. |
| `examples/game/` | Game demos and the Mob Pit app. Its own workspace. |
| `migrations/` | SQLite and Postgres migrations, applied by the stores. |
| `docs/` | Guides (`docs/guides/`), contracts (`docs/*.md`), design notes (`docs/design/`), review snapshots (`docs/reviews/`). |
| `scripts/` | CI and local helpers: OIDC stacks, celld, the wasm32 check, the docs path check. |

## Rust

From the repository root:

```bash
cargo check --workspace --all-features
cargo test -p distributed --lib                  # unit tests, no services
cargo test -p distributed --doc --all-features   # doctests, including README.md
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

Tests that need a broker or Postgres return early when their URL is unset,
unless `DISTRIBUTED_REQUIRE_SERVICES` names it, in which case they fail.

### Root Makefile

| Target | What it does |
|---|---|
| `make test` (alias of `make test-local`) | Starts `compose.yaml`, recreates the test databases, and runs `cargo test --workspace --all-features --all-targets` and `--doc` against real services with `DISTRIBUTED_REQUIRE_SERVICES` set. The compose stack is torn down afterwards; `KEEP_COMPOSE=1` keeps it. This is what CI's services job runs. |
| `make test-cargo` | The same cargo commands without starting compose; set the service URLs yourself. |
| `make test-cli-lifecycle` | Builds the CLI and runs the bats lifecycle suite (`distributed_cli/tests/bats/lifecycle.bats`). |
| `make compose-up` / `make compose-down` | Start or stop the services alone. |

`compose.yaml` runs Postgres, RabbitMQ, Kafka, NATS and Redis on their
default ports. The service URLs are the variables at the top of the
`Makefile` (`DATABASE_URL`, `AMQP_URL`, `KAFKA_BROKERS`, `NATS_URL`,
`REDIS_URL`); export them to run one test binary by hand after
`make compose-up`, for example `cargo test --all-features --test transports`.

## JavaScript client

```bash
cd js
npm ci
npm run quality   # typecheck, generated-file check, tests, pack smoke test, publint
```

## The e2e-ui application

```bash
cd tests/e2e-ui
make test          # offline: build through Distributed, domain + suite tests, UI build and checks
make ci-offline    # what CI runs offline (pass DISTRIBUTED=<path to a built CLI> to skip the cargo run)
make up            # Docker: Postgres + Zitadel + OIDC bootstrap, writes e2e-ui.env
make run           # build and supervise API + UI on http://localhost:8791
make test-browser-install && make test-browser   # Playwright, against a running `make run`
make down          # stop the Docker stack
```

`make run` runs this checkout's CLI through Cargo, so the crate, CLI and JS
package versions always match. Open `http://localhost:8791` (not
`127.0.0.1`): the login cookie is set for `localhost`. Demo logins: `alice`,
`bob`, `admin`, password `Password1!`.

The celld variant: `make up && make up-celld-nats` in `tests/e2e-ui`, then
`make run` in `tests/e2e-celld`. See [tests/e2e-celld/README.md](tests/e2e-celld/README.md).

`tests/e2e-ui/Cargo.lock` is tracked: the application is its own workspace,
and its lockfile pins what CI and every checkout resolve. Commit lockfile
changes with the change that caused them.

## The no-Docker quickstart

```bash
cargo run -p distributed-quickstart
```

See the README's quick start for the curl calls. It is a workspace member, so
`cargo test --workspace` builds it.

## Docs

```bash
python3 scripts/check-doc-paths.py
```

It fails when README.md, AGENTS.md or a file under `docs/` (except
`docs/reviews/`) links to a missing file or heading, or names a backticked
repo path that does not exist. CI runs it in the Each-Feature Check job
(`.github/workflows/test-all-features.yaml`). README Rust snippets are
doctests; mark a fragment that cannot compile on its own `rust,ignore`.

## CI

`.github/workflows/on-pr-quality.yaml` runs on every pull request. It calls
the shared quality workflow (build, test, clippy, fmt), then one workflow
per area: `test-all-features.yaml` (each feature alone, plus the docs path
check), `integration-services.yaml` (`make test-local`), and
`integration-{gateway,distributed-cli,observability,graphql,e2e-ui,js,game-demos,celld,wasm32-domains}.yaml`.
Run the matching local command above before pushing a change in that area.
