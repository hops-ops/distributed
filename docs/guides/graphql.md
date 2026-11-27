# GraphQL query service

Auto-generated GraphQL over relational read models — Hasura-style filtering,
ordering, pagination, relationships, role-based column allowlists and row
filters, live subscriptions after projection commits, and typed command
mutations derived from the executable `Service` (`Eventual<T>` commands with
eventual projectors, and `Atomic<T>` commands that return their committed row).

This is the public query/command edge for full-stack apps. The companion
TypeScript package [`@hops-ops/distributed`](../../js/) (see
[`js/README.md`](../../js/README.md)) supplies transport, a normalized causal replica,
command runtime, diagnostics, and a SvelteKit adapter. End-to-end template:
[`tests/e2e-ui/`](../../tests/e2e-ui/).
Example playground: `cargo run --example graphiql --features "graphql,sqlite"`.

## Enable

```toml
# Query engine + SQLite dialect (local / tests)
distributed = { version = "0.1", features = ["graphql", "sqlite"] }

# Production-shaped: GraphQL + Postgres repository/bus
distributed = { version = "0.1", features = ["graphql", "postgres"] }
```

`graphql` implies `http` (Axum router, including `/graphql/ws`). The SDL
helpers (`distributed::graphql::graphql_sdl_*`, `distributed::graphql::naming`)
and the `Surface` / client-manifest types compile without the feature, so
`distributed schema --format graphql` works in tooling crates.

## Literal text search

Use `_icontains` for a case-insensitive substring search on a String column:

```graphql
query SearchTodos($q: String! = "") @load @live {
  todos(where: { title: { _icontains: $q } }, limit: 20) {
    todo_id
    title
  }
}
```

A colocated binding can pass `q: searchParam('q', 'String')` directly. Callers do not
construct `%patterns%`: `%`, `_`, backslash, quotes and the escape character
are literal text. The query engine binds an escaped pattern on both PostgreSQL
and SQLite. An empty string matches every non-null string; use a bounded query
and show results only after a submitted search when building a search form.

The pattern operators mean the same thing on PostgreSQL and SQLite:

| Operator | Wildcards | Case |
|---|---|---|
| `_like` | `%` any run, `_` one character, `\` escapes the next character | exact |
| `_ilike` | same | ASCII letters fold; every other character is exact |
| `_icontains` | none: the operand is literal text | same as `_ilike` |

Folding is ASCII-only on both backends, so `é` does not match `É` under
`_ilike` or `_icontains`. A pattern that ends in a lone `\` is a
`BAD_REQUEST`. The browser replica does not evaluate these operators locally
and relies on the authorized server result. `_contains` is the PostgreSQL
JSON containment operator.

Text comparison is by UTF-8 bytes on both backends, for `_eq`, `_neq`, `_in`,
`_nin`, `_gt`/`_gte`/`_lt`/`_lte` and `order_by` alike: `"Open"` is not
`"open"`, and `"Zeta"` sorts before `"alpha"`. The browser replica compares
the same way, so a list filtered by a text column is maintained locally.
Generated PostgreSQL text columns are declared `COLLATE "C"` (SQLite text is
`BINARY`), so queries add no per-query collation and ordinary indexes on those
columns serve range filters and `order_by`. Read-model bootstrap converts
existing text columns of declared tables to `COLLATE "C"`; `citext` and
nondeterministic collations are not supported.

A comparison with `null` is neither true nor false, as in SQL, and a row is
returned or authorized only when its filter is true. A `where`, a row policy,
the command-delta path and the browser replica all follow that rule, and
[`tests/fixtures/client-query-plan-corpus.json`](../../tests/fixtures/client-query-plan-corpus.json)
holds PostgreSQL, SQLite, `FilterExpr::evaluate` and the replica to the same
answers.

Query cost limits are described in [GraphQL query cost](../graphql-query-cost.md).

## Scope

| In | Out |
|---|---|
| `SELECT`-only query surface from `TableSchema` / read models | Table mutations / write-to-projection via GraphQL |
| Nested `has_many` / `belongs_to` / `many_to_many`, including composite keys | Include-loader `include()` of m2m or composite direct joins |
| Role column allowlists + row filters (`claim(...)`) | Full IdP product UI (login pages live in your app / Auth.js) |
| **First-class OIDC Bearer validation** (JWKS, iss/aud/exp, claim → session) | Assuming raw HTTP microsvc routes authenticate without a proxy or GraphQL edge |
| SQLite + Postgres dialects | Cross-service federation / remote schemas |
| Typed causal command mutations (`Service` → GraphQL) | Raw JSON GraphQL command registries |
| Live list subscriptions via commit-path invalidation | Querying outbox / event-store operational tables |

## Mount on a service

Commands are declared once, with `#[command]` inside the `#[sourced]` impl or
with `command!` (see [Commands](commands.md#command-contracts)), and mounted on
the module's `Routes`. The GraphQL command fields are generated from that
`Service` inventory; nothing is registered on the engine separately
([`create.rs`](../../tests/e2e-ui/crates/todo-domain/src/commands/create.rs)):

```rust,ignore
command! {
    name: "todo.create",
    transition: domain_commands::Create,
    aggregate: Todo,
    input: TodoCreateInput,
    outcome: Eventual<TodoCreatePayload>,
    shard: |input| input.todo_id.clone(),
    roles: ["user", "admin"],
    field: "todos_create",
    load: create,
    guard: guards::authenticated,
    invoke: |todo, input, principal| todo.create(&input.todo_id, principal, &input.title),
    payload: |todo, _principal| {
        let state = TodoState::from(&**todo);
        TodoCreatePayload {
            todo_id: state.todo_id,
            owner_id: state.owner_id,
            title: state.title,
            status: state.status,
        }
    },
    defaults: command_input_defaults! {
        input: TodoCreateInput;
        default input.todo_id = uuid_v7();
    },
}
```

Optimistic effects are derived, not written per command: the events' declared
`known(...)` values (see
[flat events and optimistic updates](event-sourcing.md#flat-events-and-optimistic-updates))
flow through the projection's mutation program into the client replica.

The engine is built over the same read-model list and `Service`
([`graphql.rs`](../../tests/e2e-ui/crates/service/src/modules/graphql.rs),
[`host.rs`](../../tests/e2e-ui/crates/service/src/host.rs)):

```rust,ignore
use distributed::graphql::{graphiql_enabled_from_env, GraphqlEngine};
use distributed::microsvc::{Routes, Service};

let service = Service::new()
    .named("todos")
    .routes(
        Routes::for_aggregate(&stores)
            .mount(todo_domain::commands::create())
            .consume_projection(projections.projector(TODOS)),
    );

let engine = GraphqlEngine::builder(stores.graphql_pool())
    // Stable nonzero deployment secret shared by replicas of this endpoint.
    .protocol_token_key(protocol_token_key)
    .roles(&["user", "admin", "anonymous"])
    // Every model with its `AuthorizedReadModel` permissions.
    .read_models(&read_models())
    // This exact executable inventory is the only mutation source.
    .service(&service)
    .identity(identity) // required; see Identity
    .change_stream(stores.read_model_changes()) // live queries
    .graphiql(graphiql_enabled_from_env()) // local only; see GraphiQL
    .build()?;

let service = service.try_with_graphql(engine)?;

// POST /graphql           — queries + command mutations
// GET  /graphql           — GraphiQL when enabled
// GET  /graphql/ws        — subscriptions (graphql-transport-ws / graphql-ws)
```

`try_with_graphql` refuses an engine whose service id or typed-command
contracts differ from the `Service` it is attached to.

## Permissions (deny by default)

Three axes — **grant** a role, **columns** they may see, **rows** they may access.
Unmentioned models/roles fail closed (that is the deny). There is no separate
`.deny()` list: omit the role, narrow columns, or tighten `.rows(...)`.
Permissions live on the model
([`todos.rs`](../../tests/e2e-ui/crates/readmodels/src/models/todos.rs)):

```rust,ignore
use distributed::graphql::{claim, col, read, AuthorizedReadModel, ModelPermissions, ReadModels};

impl AuthorizedReadModel for Todos {
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new()
            .grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("owner_id").eq(claim("x-user-id"))),
            )
            .grant("admin", read().all_columns()) // all rows
            .grant("anonymous", read().columns(["todo_id", "status"]))
    }
}

// One list feeds schema bootstrap, the GraphQL engine and client export.
pub fn read_models() -> ReadModels {
    ReadModels::new("todos").model::<Todos>()
}
```

Row predicates can bind session claims (`claim("x-user-id")`, …) so multi-tenant
RLS lives in the engine, not ad-hoc handler SQL. `GraphqlEngineBuilder::model`
and `::permission::<M>(role, read)` register permissions on the builder
instead, for models assembled without `AuthorizedReadModel`.

With the causal protocol enabled, required session presets must be present and
valid for their declared codecs before execution. Missing or malformed values
return `BAD_REQUEST` for both ordinary and streaming requests, without running
resolvers or issuing a partial protocol envelope. The error does not disclose
claim names or values; configure the identity mapping to supply those inputs.

## Identity (first-class OIDC)

Auth is a **built-in GraphQL concern**, not a separate product you wire after the
fact. The engine validates tokens, maps claims into a `microsvc::Session`, and
feeds the same claim map into RLS (`claim("x-user-id")`, roles, …). There are
three `IdentityConfig` modes and **no default**: `GraphqlEngine::build()` fails
until `.identity(..)` names one. They live under
[`src/graphql/identity/`](../../src/graphql/identity/).

Only a validated OIDC bearer yields a **verified principal**. Command
mutations and command status require one and return `401 UNAUTHORIZED`
("durable commands require a verified OIDC bearer") otherwise. The other modes
resolve an unverified session that serves queries and subscriptions under RLS.

| Mode | Resolves | Allows |
|---|---|---|
| **`OidcBearer`** (`IdentityConfig::oidc_bearer(OidcConfig)`) | JWT access token (`Authorization: Bearer …`) validated against JWKS (incl. discovery), iss/aud/exp/nbf, alg allowlist (no `alg=none`); claims → session and engine roles. Client `x-user-id` / `x-roles` headers are ignored. No token: 401 when `require_auth`, otherwise an anonymous session. | Queries, subscriptions **and command mutations**. The public edge. |
| **`Gateway`** (`IdentityConfig::gateway(header, secret)`) | A request is trusted only when it carries the shared secret (compared in constant time); its `x-user-id` / `x-roles` then become the session. Without the secret: 401. | Queries and subscriptions; command mutations are refused (unverified). |
| **`DevHeaders`** (`IdentityConfig::dev_headers()`) | Every request header is believed. **Never** on a public edge. | Local queries and subscriptions; command mutations are refused (unverified). |

`public_oidc_identity_from_env()` builds `OidcBearer` with `require_auth` from
`OIDC_ISSUER` / `OIDC_AUDIENCE` (falling back to `OIDC_CLIENT_ID`) and
optional `OIDC_JWKS_URI`; with them unset it uses a placeholder issuer, so
every request still needs a bearer and fails closed. Public services use it, never
DevHeaders.

Credentials never enter the `Session` handed to command handlers:
`authorization`, `cookie`, `proxy-authorization` and the gateway secret header
are dropped in every mode. If the issuer's JWKS endpoint fails, requests keep
validating against the keys already cached, and the issuer is retried after
`OidcConfig::jwks_refresh_failure_backoff` (30s by default).

**Provider-portable by design.** Live compose + bootstrap + e2e binaries ship for:

- **Zitadel** — `tests/graphql/graphql_oidc_zitadel` + `scripts/oidc-zitadel-up.sh` (JWT-bearer mint; also powers e2e-ui login)
- **Keycloak** — `tests/graphql/graphql_oidc_keycloak` + `scripts/oidc-keycloak-up.sh` (client_credentials + realm roles)
- **Authentik** — `tests/graphql/graphql_oidc_authentik` + `scripts/oidc-authentik-up.sh` (client_credentials + groups)

Shared assertions (E1–E8): discovery/JWKS, happy path, role isolation,
multi-audience / `azp`, expired and forged tokens, etc. Generic OIDC also works
for SaaS IdPs (e.g. Okta) without a dedicated compose stack. How to run them is
in [Testing](testing.md#first-class-oidc-zitadel-keycloak-authentik).

**WebSocket subscriptions:** browsers cannot set `Authorization` on the upgrade.
Clients send the access token in `connection_init` (`authorization` /
`accessToken` / nested headers). Identity is never read from the URL query
string. Under `Gateway` the secret must be on the upgrade request and the
`connection_init` payload is ignored. e2e-ui chat demonstrates the OIDC path.

Each origin WebSocket is bounded by `WebsocketLimits` (set with
`.websocket_limits(..)`): it closes when the token that opened it expires
(close code `4401`) or after `max_lifetime` (12h), whichever is first; the
server pings and drops a peer silent for `idle_timeout` (60s); and one
connection holds at most `max_live_queries` (100) subscriptions.

> **Note:** Raw microsvc HTTP/gRPC routes still treat `Session` as opaque unless
> you terminate auth at a proxy **or** put the public API on GraphQL
> (`OidcBearer`). The GraphQL edge is where first-class token validation lives.

## Command mutations vs HTTP commands

Command fields on the GraphQL schema are an RPC facade: same guards, same
handlers, same outbox/projector path as other transports. Prefer a
**GraphQL-only public API** for browser apps so the edge is one protocol:
HTTP `POST /{command}` stays off unless you expose one command with
`.with_http_command(command, edge)` (its edge authenticates the request) or
call `.with_http_command_routes()` behind a trusted proxy (see
[HTTP transport](commands.md#http-transport-requires-http-feature)). Handler
guards should require a session user (and role where needed); never trust
client-supplied owner fields over the session principal.

## Live subscriptions

Projection commits publish `ReadModelChange`s. Pass the store's change stream
to the engine with `.change_stream(stores.read_model_changes())`; the engine's
`ChangeHub` then invalidates matching `@live` queries so clients receive
updated lists without polling. Projectors must write the same database the
engine reads; the e2e-ui chat subscription is the reference. Delivery
semantics are in [live query delivery](../live-query-delivery.md).

## GraphiQL

```bash
cargo run --example graphiql --features "graphql,sqlite"
# open http://127.0.0.1:4000/graphql  (override with GRAPHIQL_ADDR)
```

GraphiQL is a **developer** tool. The example uses `DevHeaders`, and the
playground sends default `x-roles` / `x-user-id` headers. For real services:

- Pass **`graphiql(false)`** or `graphiql_enabled_from_env()` (`GRAPHIQL=0` /
  `false` / `off` / `no` disables it; otherwise it is off when `RUST_ENV`,
  `ENV` or `APP_ENV` is `production` / `prod`) so production never ships the IDE.
- Treat GraphiQL + DevHeaders as local-only; pair public services with
  `OidcBearer`.

## Client surfaces, generated artifacts, and CI

```bash
# Optional human-readable GraphQL SDL artifact
distributed schema --format graphql --out schema.graphql
git diff --exit-code schema.graphql   # drift gate
```

The Rust `Service` inventory and GraphQL `Surface` IR are the source of truth
for schema, authorization, commands, optimistic effects, and client artifacts.
Each named application surface (`client_application_surface(..)` on the
engine builder) gets its own client: common and elevated applications use
separate manifest entrypoints, document sets, generated directories, virtual
modules, and request-local replicas; an admin superset is never bundled into
the common client.

For an application such as `tests/e2e-ui`:

```bash
distributed build  # manifest + clients + Svelte check/build + atomic activation
distributed dev    # same generation lifecycle with supervised API/UI processes
```

Application code does not invoke the client compiler directly or commit its
outputs. Internally the lifecycle lowers the Rust surface with
`distributed client-manifest` (one role or named application surface) and
`distributed client` (that manifest plus co-located `.graphql` operations →
typed query/live/command modules); those commands are documented for compiler
integration and diagnostics, not as parallel application workflows. See the
[CLI guide](cli.md).

`distributed dev` tells each supervised process whether it belongs to the
active generation. A process honours that by composing its services with
`.with_lifecycle(LifecycleGate::from_env())` (resolved once, cloned into every
`Service`): outside the active generation, writes fail with
`ApplicationReloading`. Without the call a service is always open. See
[event delivery during reload](../design/reloading-event-delivery.md).

See [`js/README.md`](../../js/README.md) for the package API and
[`tests/e2e-ui/README.md`](../../tests/e2e-ui/README.md) for the complete integration
flow.

## Full-stack template (`tests/e2e-ui`)

Copyable product shape (not a toy workshop): multi-crate domains, GraphQL-only
edge, real OIDC, SSR, live subscriptions, and a teaching **Blob** aggregate whose
commands return `Atomic<BlobGames>`: the handler commits the row built by the
projection's own mutation with the events, so the response carries the row and
no async projector is involved.

| Piece | Role |
|---|---|
| Domain crates | Pure aggregates: todos, chat, blob |
| Read models | Eventual projector rows (todos/chat/auth users) *and* `Atomic` direct-projection rows (blob) — one mutation IR, different apply site |
| GraphQL edge | Owner RLS, admin surfaces, joins to `auth_users`, chat live sub, blob commands |
| Identity | Zitadel + Auth.js (PKCE), optional Zitadel user-scrape → `auth_users` |
| SvelteKit | `$distributed` / `$distributed/admin` / `$distributed/public`, SSR from co-located `+page.graphql`, hydration, generated live ops + optimistic commands |
| Suite | GraphQL-only edge, IDOR, OIDC isolation, Playwright |

```bash
cd tests/e2e-ui
make up           # once: Postgres + Zitadel, writes e2e-ui.env
make run          # runs `distributed dev` via cargo (the binary is not on PATH from a checkout)
# UI and GraphQL: http://localhost:8791  ·  http://localhost:8791/graphql
# (the gateway proxies the internal SvelteKit server on :5180; use localhost,
#  not 127.0.0.1, so the Auth.js cookie is sent)
# /todos  /chat  /blob  /admin  /session  /login
make test         # offline domain + suite + UI build/typecheck/structural tests

# Same UI against celld (Todo + chat.post wait-dispatch; @live stays on GraphQL)
make up-celld-nats && cd ../e2e-celld && make run
```

## TypeScript client (`js/` → `@hops-ops/distributed`)

The client core is framework-neutral; SvelteKit is the shipped adapter.

| Export | Purpose |
|---|---|
| `@hops-ops/distributed` | HTTP/WebSocket GraphQL client, auth and protocol primitives |
| `…/replica` | Normalized causal replica, GraphQL transport, generated command runtime (receipts and status carry record revisions), query-plan and `@window` keyset helpers |
| `…/replica/lazy` | `createLazyReplicaCommandRuntime` only, so lazily loaded commands stay out of the first import graph |
| `…/diagnostics` | Redacted replica diagnostics and artifact inspection |
| `…/sveltekit` | Svelte stores, island SSR load/hydrate, auth lifecycle, generated bindings (`searchParam`, …) |
| `…/sveltekit/vite` | Vite plugin (GraphQL clients and game surfaces), virtual module aliases, GraphQL proxy helpers |
| `…/sveltekit/game` | Runes state for UI around a `distributed_game` zone, ticket source |
| `…/sveltekit/game/server` | Server-only game ticket handler and minting |
| `…/game` | `distributed_game` zone protocol client (codec, connection, prediction) |
| `…/game/vite` | Vite plugin generating game clients in any Vite app |

All other subpaths are private. See [`js/README.md`](../../js/README.md) for
the package API and packaging.
