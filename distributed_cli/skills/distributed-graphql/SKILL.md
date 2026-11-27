---
name: distributed-graphql
description: Expose generated GraphQL queries and typed causal commands over Distributed services. Use when adding a GraphQL API, roles, model exposure, subscriptions, command mutations, or generated clients.
---

# GraphQL query service

Enable the framework features and mount one endpoint:

```toml
distributed = { path = "...", features = ["graphql", "sqlite"] } # or postgres
```

## Service layout (`src/query/`)

```
src/query/
  mod.rs          # build_engine(repository, &service, protocol_key)
  roles.rs        # role name constants
  <model>.rs      # one file per exposed model: permissions()
```

Typed command declarations live beside their executable handlers. There is no
second GraphQL command registry.

### Add a model exposure

1. Ensure the read model is in `read_model_catalog()`.
2. Add `src/query/<table>.rs`:

```rust
use distributed::graphql::{read, col, claim, ModelPermissions};
use crate::read_models::OrderView;

pub type Model = OrderView;

pub fn permissions() -> ModelPermissions<OrderView> {
    ModelPermissions::new()
        .grant("user", read()
            .all_columns()
            .rows(col("customer_id").eq(claim("x-user-id"))))
        .grant("anonymous", read().columns(["order_id", "status"]))
}
```

3. Register it in `src/query/mod.rs` with
   `.model::<OrderView>(order_view::permissions())`.

### Roles

Declare the vocabulary with `.roles(&["user", "anonymous"])`. Deny-by-default:
a role sees nothing until granted via `.model(...)` / `.grant_all(role)` /
`.permission(...)`.

## Wire the service

```rust
let service = build_service(repository.clone());
let engine = query::build_engine(
    &repository,
    &service,
    protocol_token_key,
)?;
let service = service.try_with_graphql(engine)?;
// POST /graphql  — queries / mutations
// GET  /graphql  — GraphiQL IDE when `.graphiql(true)`
```

`POST /{command}` stays off. For an intentional, independently authenticated
ingress, expose that one command with `with_http_command(command, edge)`: the
edge checks the request and returns the dispatch (command ID, input, session,
principal). `with_http_command_routes()` opens every command to trusted-proxy
headers. Behind a gateway, `command_gateway_routes(binding)` keeps every
command path on the service. GraphQL mutations need none of these.

Build the executable `Service` first and pass that exact instance to
`GraphqlEngineBuilder::service`. For `Atomic<M>`, pass the repository handle
itself as the GraphQL pool source; a separately cloned raw pool cannot prove the
same transactional storage identity. Configure a stable, nonzero 32-byte
protocol key on every replica serving the same endpoint.

### GraphiQL (local visual explorer)

```bash
# Framework playground (seeded orders, no service needed):
cargo run --example graphiql --features "graphql,sqlite"
# → http://127.0.0.1:4000/graphql

# Or run your service and open GET /graphql
GRAPHIQL=0 cargo run   # disable IDE in production
```

Default GraphiQL headers: `x-roles: user`, `x-user-id: demo`. Edit in the IDE
Headers panel. See `README § GraphQL query service`.

### Identity (public GraphQL)

A public service **always** wires `public_oidc_identity_from_env()` → **`OidcBearer`** +
`require_auth=true` (D6). Set `OIDC_ISSUER` + `OIDC_AUDIENCE` (or `OIDC_CLIENT_ID`).
If unset, placeholder issuer still uses OidcBearer (401 without Bearer) — **never**
ambient `DevHeaders` on a public endpoint.

| Mode | Use |
|---|---|
| `OidcBearer` | Public API — validate `Authorization: Bearer` JWT (access token) |
| `Gateway` | Behind an authenticating gateway — `IdentityConfig::gateway(header, secret)`; headers are trusted only with the shared secret |
| `DevHeaders` | Local GraphiQL / unit tests only (explicit opt-in) |

There is no default mode: `build()` fails without `.identity(..)`.

Wire via `.identity(distributed::graphql::public_oidc_identity_from_env())`.
Never trust raw client `x-user-id` / `x-roles` on a public edge.

Pass `change_stream(repo.read_model_changes())` for live subscriptions.

### Typed causal commands

Declare each GraphQL mutation on the executable route:

```rust
let routes = Routes::new()
    .with_repo(repository.aggregate::<Order>())
    .command_transition::<
        domain_commands::Create,
        CreateOrderInput,
        Eventual<CreateOrderPayload>,
    >("order.create")
    .roles(["user"])
    .handle(create_order);
```

Handlers accept `CausalCommandContext`, stage aggregate/outbox work on that
context, and return `PreparedCommand<Succeeded<_>>`, `PreparedCommand<Eventual<_>>`,
or `PreparedCommand<Atomic<M>>`. Never commit outside the framework-owned
causal boundary. Projector obligations derive from the transition event set +
portable/modeled handlers (`mutation!`), not separately authored command
confirmations/effects.

### Command consistency modes (ship contract)

**Same portable mutation IR.** Different **response proof** — do not collapse them.

| Contract | Meaning | Mutation response | Client seal |
|----------|---------|-------------------|-------------|
| `Succeeded<T>` | Tx succeeded; no projection promise | Payload only | Revalidate / live |
| `Eventual<T>` | Events committed; Eventual projectors apply later | Payload + **projection-delta** + `expects` | Automatic event→mutation preview, then wait obligations |
| `Atomic<M>` | Exact row in **same** command tx | **Typed row `M`** + direct **`records[]`** (no eventual modeled metadata, empty `expects`) | Same automatic preview; **`confirmDirectProjection(row, records)`** before await settles |

Handler for Atomic — this *is* returning atomic read-model updates:

```rust
let row = save_*(...).from_state(...)?;
repo.readmodel(row).publish_events().commit(agg)?.atomic()
```

Rules:

1. Use `Atomic<M>` only when the exact row is staged in-handler
   (`readmodel(row).…commit()?.atomic()`). Server will not attach causal
   projection-delta metadata to same-tx commands (by design).
2. Use `command_transition::<domain_commands::Transition, _, _>` so the generated
   transition supplies the exact event set and values the compiler can prove.
   The role-visible projector arm is the only event→mutation mapping.
3. Direct and Eventual placements may both export that portable preview program
   (`is_preview_eligible`); only Eventual creates causal wait obligations
   (`is_causally_eligible`).
4. Do not board-sim Atomic UI — the returned row is authoritative.
5. Otherwise `Succeeded<T>`; never invent a projected row.

Surface IR: SDL is built via `build_surface` → `graphql_sdl_from_surface` (shared inventory
for dialect-honest comparison ops, role grants, typed commands, and generated
client manifests).

## SDL artifact (CI gate)

```bash
distributed schema --format graphql --out schema.graphql
git diff --exit-code schema.graphql
```

`--dialect` is ignored with `--format graphql` (dialect-independent core surface).

## Client / replica drift (dogfood)

If unit tests pass but the live UI 500s with schema/surface errors: the API
pod is still compiling or clients/`js/dist` are stale. Rebuild and wait —
don't redesign roles. Bare `__typename` (role fingerprint) ≠ application
surface clients.

## Reference

- Framework docs: `README § GraphQL query service` in the Distributed repo
- Distributed GitKB: `specs/query-layer/v1/client-replica` and
  `specs/query-layer/v1/causal-command-protocol` (normative)
