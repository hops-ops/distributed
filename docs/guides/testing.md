# Testing

### Tests in this repo

| Suite | Focus |
|---|---|
| `tests/graphql/` | Engine, HTTP, SDL, dialects, harden (authz/DoS/inject), causal transport |
| `tests/graphql/graphql_identity` | Always-on OIDC/JWT matrix (mock JWKS; no Docker) |
| `tests/graphql/graphql_oidc_{zitadel,keycloak,authentik}` | **Live** multi-IdP e2e (compose + real JWKS; gated) |
| `tests/core/typed_commands` | Eventual / Atomic / Succeeded command registration |
| `tests/e2e-ui` | Multi-crate product template + SvelteKit + Zitadel UI login + Playwright |
| `js/tests` | Replica, command runtime, adapters |
| `examples/graphiql.rs` | Seeded local playground |

```bash
cargo test --test graphql --features "graphql,sqlite" graphql_engine::
cargo test --test graphql --features "graphql,sqlite" graphql_identity::
cargo test --test graphql --features "graphql,sqlite" graphql_harden::
cd js && npm run quality
# Live IdPs (optional):
#   ./scripts/oidc-zitadel-up.sh && set -a && source graphql-oidc.env && set +a
#   cargo test --test graphql --features graphql,sqlite graphql_oidc_zitadel::
# Full UI matrix: cd tests/e2e-ui && make test
```

### First-class OIDC (Zitadel, Keycloak, Authentik)

GraphQL identity is built into the engine (`OidcBearer`: JWKS, iss/aud/exp,
claim → role/session). Live against three local IdPs — not mocks only:

| Provider | Compose + bootstrap | Live test | Gate |
|---|---|---|---|
| **[Zitadel](../../tests/graphql/graphql_oidc_zitadel/)** (reference) | `./scripts/oidc-zitadel-up.sh` | `cargo test --test graphql --features graphql,sqlite graphql_oidc_zitadel::` | `ZITADEL_E2E=1` |
| **[Keycloak](../../tests/graphql/graphql_oidc_keycloak/)** | `./scripts/oidc-keycloak-up.sh` | `cargo test --test graphql --features graphql,sqlite graphql_oidc_keycloak::` | `KEYCLOAK_E2E=1` |
| **[Authentik](../../tests/graphql/graphql_oidc_authentik/)** | `./scripts/oidc-authentik-up.sh` | `cargo test --test graphql --features graphql,sqlite graphql_oidc_authentik::` | `AUTHENTIK_E2E=1` |

Shared **E1–E8** in [`tests/common/src/oidc.rs`](../../tests/common/src/oidc.rs).
Gated binaries skip cleanly when unset. Offline:
`cargo test --test graphql --features graphql,sqlite graphql_identity::`.

e2e-ui boots **Zitadel** for the browser path; the three stacks prove the
same `OidcBearer` edge is not vendor-locked.

## Running Tests

```bash
make test                   # = make test-local: compose up, the whole workspace
                            #   with --all-features against real services
make test-cargo             # the same cargo commands, no compose (bring your own services)
make test-cli-lifecycle     # builds the CLI and runs distributed_cli/tests/bats/lifecycle.bats
make compose-up             # start postgres, rabbitmq, kafka, nats, redis (compose.yaml)
make compose-down
cargo test                  # default feature set
cargo test --all-features   # all features; service tests skip without their env vars
```

`make test-local` is what CI's services job runs
(`.github/workflows/integration-services.yaml`). It recreates the per-test
Postgres databases, exports every service URL, and sets
`DISTRIBUTED_REQUIRE_SERVICES`, so a test that cannot reach its service fails
instead of skipping (see [`tests/common/src/env.rs`](../../tests/common/src/env.rs)).
Set `KEEP_COMPOSE=1` to leave the services running afterwards.

The integration tests are eight binaries, one per area: `core`, `gateway`,
`graphql`, `observability`, `read_models`, `repositories`, `services` and
`transports` (`tests/<area>/main.rs`). Each former test target is a module, so
`cargo test --test <area> <module>::` runs one of them.

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

This project uses [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov):

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov

cargo llvm-cov --all-features --summary-only
cargo llvm-cov --all-features --lcov --output-path lcov.info
```

On push to `main`, CI (`.github/workflows/rust.yml`) publishes `lcov.info` as a
workflow artifact and attempts an optional Codecov upload.
