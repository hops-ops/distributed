# Commands and the service runtime

Typed commands, the `microsvc` service that hosts them, and the transports a
command can arrive on.

## Command contracts

**Commands on the aggregate.** Most commands are one aggregate method:
take the actor from the session, load (or create) the aggregate, call the
method, reject on its error, commit with a payload. Write the method and
mark it `#[command]` inside the `#[sourced]` impl; the macro generates the
handler and the portable mount (`pub struct RecordKill`, mounted with
`routes.mount(RecordKill)` or `AggregateCell::mount(RecordKill)`):

```rust,ignore
#[sourced(entity, aggregate_type = "Crawler", events = "CrawlerEvent", domain_state = CrawlerState)]
impl Crawler {
    #[command("crawler.record_kill", shard = principal, load = or_create, outcome = succeeded)]
    pub fn record_kill(
        &mut self,
        #[principal] principal: &str,
        input: RecordKillInput,
    ) -> Result<KillReceipt, String> {
        let xp = xp_for_tier(input.tier).ok_or("unknown tier")?;
        self.mob_killed(principal.into(), input.name, input.mob, input.tier, xp)
            .map_err(|e| e.to_string())?;
        // Receipt helper: the recorded event's stream position and time.
        let recorded = self.entity.last_recorded().ok_or("nothing recorded")?;
        Ok(KillReceipt { sequence: recorded.sequence, at_ms: recorded.unix_ms(), .. })
    }

    #[event("crawler.mob_killed", version = 1, domain)]
    fn mob_killed(&mut self, id: String, name: String, mob: String, tier: u8, xp: u32) { .. }
}
```

- **Principal.** Exactly one parameter is `#[principal]` (`&str` or
  `String`); the generated handler fills it from the verified session
  (`x-user-id`, else `Unauthorized`), never from the input. It is
  mandatory, and an input whose field (Rust or serialized name) is named
  like it does not compile.
- **Input.** The other parameter is the `CommandInput` type; or, with
  `input = Type`, the other parameters are that input's fields by name
  (`&str` parameters borrow), so a domain method keeps plain arguments.
- **`shard`** is an input field (also exposed as `RecordKill::shard`) or
  the principal parameter; **`load`** is `required` (`NotFound` when
  missing), `or_create` or `create`.
- **`outcome`** is `succeeded` / `eventual` (the method's `Ok` value is
  the payload) or `Succeeded<P>` / `Eventual<P>` with `payload = f`
  (`fn(&Aggregate) -> P`). The method's error (`Display`) becomes
  `Rejected`. Captured domain events are always published with the commit.
- Optional: `roles = ["player"]`, `guard = f`, `field = "graphql_field"`,
  `transition = path` (the emitted-event witness; by default the
  `domain_commands::*` type `#[sourced]` derives for the method),
  `mount = Name` (the struct name, default from the command name's last
  segment).

The generated registration is exactly the hand-written one
(`command_transition::<domain_commands::X, I, K>(name)` + `field_name`,
`roles`, `guarded`/`handle`): the command specs, GraphQL SDL and client
manifest are identical (`tests/core/aggregate_command`). For unusual bodies write
the handler and register it with `command! { .., handle: f }` or
the builder below.

**Contracts.** Command semantics live in `distributed::command`, independently of the query
transport. Use `CommandInput` and `CommandOutput` to describe Serde input/output
shapes, and `command::{typed_command, Succeeded, Eventual, Atomic, PreparedCommand}`
to declare and prepare commands. The GraphQL adapter derives its surface from
those contracts; GraphQL naming restrictions are checked when exposing them.

```rust,ignore
use distributed::command::{typed_command, CommandInput, CommandOutput, Succeeded};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, CommandInput)]
struct RenameInput { id: String, title: String }

#[derive(Serialize, CommandOutput)]
struct RenameOutput { id: String }

let declaration = typed_command::<RenameInput, Succeeded<RenameOutput>>("todo.rename");
```

Nested input and output types derive `CommandInput` / `CommandOutput` too.
A hand-written metadata implementation uses `CommandInputType` /
`CommandOutputType` with `CommandTypeDef` / `CommandTypeField`.

GraphQL is the application's UI gateway: it exposes the command contract
alongside queries and subscriptions. Command handlers describe their data and
consistency through the core API, independently of where that gateway runs.

`Atomic<M>` obtains its response shape from the relational read model and
requires the same-transaction projection proof.

## Microservice Framework (`microsvc`)

The `microsvc` module provides a convention-based async command/event handler framework. Register typed commands and event handlers on `Routes<D>` bundles, collect them into a non-generic `Service`, then expose that service over HTTP, gRPC, the bus, or in-process dispatch. Every command, from every ingress, is typed, authorized by its declared roles, and recorded in the command ledger under its command ID.

### Defining a Service

A `Routes<D>` bundle is generic over a dependency type `D`. Build one fluently from `Routes::new()`: add `.with_repo(repo)` for aggregate command handlers, `.with_read_model_store(store)` for projection handlers (chain both when a handler needs both), or `.with_dependencies(deps)` for event handlers with custom dependencies. Add one or more route bundles to `Service::new().named(..)` with `.routes(routes)`, then use `.with_bus(bus)` to consume from / publish to a transport. A service that runs commands must be named: the name scopes its command ledger.

Commands are registered with a fluent builder. `.typed_command(declaration)` starts a registration; `.handle(handler)` adds an unguarded handler and `.guarded(guard, handler)` adds a guarded one. The handler receives `&CausalCommandContext<A>` and the decoded input, and returns the prepared commit:

```rust,ignore
use std::sync::Arc;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::command_dispatch::VerifiedPrincipal;
use distributed::microsvc::{CausalCommandContext, HandlerError, Routes, Service, Session};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::json;

async fn initialize(
    ctx: &CausalCommandContext<'_, Counter>,
    input: CreateCounter,
) -> Result<PreparedCommand<Succeeded<CounterValue>>, HandlerError> {
    let mut counter = ctx.repo().create();
    counter.create(input.id)?;
    let value = counter.value;
    ctx.repo().commit(counter)?.succeeded(CounterValue { value })
}

async fn increment(
    ctx: &CausalCommandContext<'_, Counter>,
    input: IncrementCounter,
) -> Result<PreparedCommand<Succeeded<CounterValue>>, HandlerError> {
    let mut counter = ctx.repo().get(&input.id).await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    counter.increment(input.amount)?;
    let value = counter.value;
    ctx.repo().commit(counter)?.succeeded(CounterValue { value })
}

let routes = Routes::new()
    .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
    .typed_command(typed_command::<CreateCounter, Succeeded<CounterValue>>("counter.initialize"))
    .handle(initialize)
    .typed_command(typed_command::<IncrementCounter, Succeeded<CounterValue>>("counter.increment"))
    .handle(increment);
let service = Arc::new(Service::new().named("counters").routes(routes));

// In-process dispatch: a UUIDv7 command ID and the principal the caller verified
let _result = service
    .dispatch_causal(
        "counter.initialize",
        "0190a000-0000-7000-8000-000000000001",
        json!({ "id": "c1" }),
        Session::new(),
        VerifiedPrincipal::internal("counters-job")?,
    )
    .await?;
```

`CreateCounter` and `IncrementCounter` derive `Deserialize` and `distributed::command::CommandInput`; `CounterValue` derives `Serialize` and `distributed::command::CommandOutput`. The handler stages its commit and returns; the framework writes it with the ledger receipt in one transaction, so dispatching the same command ID again replays the receipt instead of running the handler.

`Service` implements `distributed::command_dispatch::CommandHost`: `dispatch_causal` returns the payload, `dispatch_causal_with_receipt` the payload with its receipt, and every transport ends in `CommandHost::invoke` on the same ledger.

### Roles and Guards

A command declares the roles that may run it with `.roles([..])`; a session without one of them is refused before the handler or guard runs. A command with no declared roles admits any caller. `.guarded(guard, handler)` then runs the guard before the handler — if it returns `false`, the command is rejected:

```rust,ignore
let routes = routes
    .typed_command(typed_command::<ResetInput, Succeeded<ResetOutput>>("admin.reset").roles(["admin"]))
    .guarded(
        |ctx: &CausalCommandContext<'_, Counter>| ctx.claim("x-region") == Some("eu"),
        reset,
    );
```

### Handler File Convention

For larger services, organize handlers into separate files. Each command module exports a `COMMAND` name, a `command()` declaration, and an async `handle`:

```rust,ignore
// src/handlers/counter_create.rs
use serde::{Deserialize, Serialize};
use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use crate::models::counter::Counter;

pub const COMMAND: &str = "counter.initialize";

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct Input { pub id: String }

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output { pub id: String }

pub fn command() -> TypedCommand<Input, Succeeded<Output>> {
    typed_command::<Input, Succeeded<Output>>(COMMAND).roles(["user"])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Counter>,
    input: Input,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    if ctx.repo().get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!("counter {} already exists", input.id)));
    }

    let mut counter = ctx.repo().create();
    counter.create(input.id.clone())?;

    // `counter.initialized` is domain-marked on the aggregate.
    ctx.repo().publish_events().commit(counter)?.succeeded(Output { id: input.id })
}
```

Register them on the route bundle:

```rust,ignore
let routes = Routes::new()
    .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
    .typed_command(handlers::counter_create::command())
    .handle(handlers::counter_create::handle)
    .typed_command(handlers::counter_increment::command())
    .handle(handlers::counter_increment::handle);
let service = Service::new().named("counters").routes(routes);
```

Event projection handlers export `EVENT` / `EVENTS`, a `guard(ctx) -> bool` and an async `handle(ctx) -> Result<Value, HandlerError>`, and register with the `routes!` macro: `distributed::routes!(routes, event handlers::counter_rebuilt, events handlers::counter_projection)`. Inside an event handler, `ctx.message()` gives the raw transport `Message` and `ctx.input::<T>()` decodes its payload.

### HTTP Transport (requires `http` feature)

The `http` feature adds an axum-based HTTP transport. With `Service::with_http_command_routes()`, every registered command becomes a `POST /{command}` endpoint whose body is `{ "commandId", "input" }`. Request headers flow into the `Session` verbatim — including identity claims, which the framework does **not** authenticate. Deploy behind a trusted proxy that strips client-supplied identity headers and injects authenticated ones (see [Security / Trust Boundary](#security--trust-boundary)).

```rust,ignore
use std::sync::Arc;
use distributed::microsvc::{self, Service};

let service = Arc::new(
    Service::new()
        .named("counters")
        .with_http_command_routes()
        .routes(routes),
);

// Get an axum Router to compose with other routes
let app = microsvc::router(service.clone());

// Or serve directly
microsvc::serve(service, "0.0.0.0:3000").await?;
```

Routes:

| Method | Path | Description |
|---|---|---|
| `POST` | `/{command}` | Dispatch a command. Body = `{ "commandId", "input" }`, headers = session variables. A body without a `commandId` is a 400. |
| `GET` | `/health` | Health check: `{ "ok": true, "commands": ["counter.initialize", ...] }` |

```bash
curl -X POST http://localhost:3000/counter.initialize \
  -H 'Content-Type: application/json' \
  -H 'x-user-id: user-42' \
  -H 'x-roles: user' \
  -d '{"commandId": "0190a000-0000-7000-8000-000000000001", "input": {"id": "c1"}}'
# => {"payload": {"id": "c1"}, "receipt": {"commandId": "...", "causationId": "...", "state": "succeeded"}}

curl http://localhost:3000/health
```

To expose one command without trusting any header, give it an edge with `Service::with_http_command`. The edge sees the request headers and JSON body (`null` when empty) and returns an `HttpCommandDispatch { command_id, input, session, principal }`, or a `CausalDispatchError` (a 401 for a bad secret). The command runs through the ledger like any other dispatch, with only the session the edge built, and answers `{ payload, receipt }`. No other command path is mounted. Behind a gateway, `Service::command_gateway_routes(binding)` (feature `gateway`) gives every command path to the service binding, so an unexposed one is the service's 404 rather than a UI page:

```rust,ignore
fn webhook_edge(
    headers: &HeaderMap,
    body: Value,
) -> Result<HttpCommandDispatch, CausalDispatchError> {
    verify_shared_secret(headers)?;
    Ok(HttpCommandDispatch {
        command_id: delivery_command_id(&body),
        input: body,
        session: webhook_session(),
        principal: VerifiedPrincipal::internal("system:webhook").expect("a subject"),
    })
}

let service = Service::new()
    .named("identity")
    .routes(routes)
    .with_http_command("webhook.receive", webhook_edge);
gateway_routes.extend(service.command_gateway_routes("service"));
```

`microsvc::command_router(service.clone())` is the command paths alone, to compose under your own router and middleware; finish it with `.with_state(service)`.

The command ID is a client-created UUIDv7 (anything else is a 400): retrying a
request with the same ID replays the committed receipt. Over HTTP and gRPC a
command needs an authenticated user (`x-user-id`, else 401) and, when its
declaration lists roles, one of them in `x-roles`.

`x-user-id` / `x-roles` are convenience keys for `Session::user_id()` /
`Session::roles()` only — not a required protocol. Your gateway can inject any
claim names; handlers read them with `session.get("…")` or map claims to the
convenience keys at the edge.

### Authenticated cell wait-path metadata

An internal command host can carry a small, explicit set of already-authenticated
facts across the private cell HTTP boundary. Do not put producer or operation
claims in `Session`: ordinary cell calls forward only the authenticated user and
role keys. `CellRequestContext` requires an internal HTTP secret and reserves
framework identity and transport-control headers from application metadata.

```rust,ignore
use distributed::command_dispatch::{
    CellRequestContext, HttpCommandHost, TrustedRequestMetadata,
};

let metadata = TrustedRequestMetadata::try_from_pairs([
    ("x-source-kind", "canonical-fact"),
])?;
let context = CellRequestContext::new("writer-service", "principal-partition")
    .with_causation_id("0190a000-0000-7000-8000-000000000001")
    .with_trusted_metadata(metadata);
let host = HttpCommandHost::new_internal("http://cell.internal", secret)?;
let (_status, body) = host
    .post_cell_wait_path_with_context(
        "order.submit",
        "0190a000-0000-7000-8000-000000000002",
        serde_json::json!({ "order_id": "order-1" }),
        &session,
        &context,
    )
    .await?;
```

Metadata names must be canonical lowercase and values are bounded visible HTTP
text. Public callers cannot authenticate this path by supplying the reserved
headers themselves; the cell must validate the shared internal secret before
constructing its typed command identity.

### gRPC Transport (requires `grpc` feature)

The `grpc` feature adds a tonic-based gRPC transport using standard protobuf wire format (no `.proto` file needed):

```rust,ignore
// Get a CommandServiceServer to compose with other tonic routes
let grpc_svc = microsvc::grpc::grpc_server(service.clone());

// Or serve directly
microsvc::grpc::serve_grpc(service, "[::1]:50051").await?;
```

| RPC | Input | Output | Description |
|---|---|---|---|
| `Dispatch` | `GrpcRequest` | `GrpcResponse` | Dispatch `command`. `input` = the JSON string `{ "commandId", "input" }`, `session_variables` = string map. The response is `{ status, body }`, `body` the JSON string the HTTP transport would answer. |
| `Health` | `HealthRequest` | `HealthResponse` | Health check. |

Session handling mirrors HTTP — gRPC metadata headers are merged with payload `session_variables`. **Transport metadata (trusted, proxy-injected) takes precedence over the client-controlled payload**, so a client cannot spoof identity via the request body. See [Security / Trust Boundary](#security--trust-boundary) below. Errors are returned inside `GrpcResponse.status` (HTTP-style status codes) with internal (5xx) error detail masked to a generic message, keeping client behavior identical across transports.

### Bus Transport

Attach a bus with `service.with_bus(bus)` and drive it with `run(opts)`: it
derives `listen` (point-to-point commands) and `subscribe` (fan-out events) from
the registered handlers, and makes `repo.outbox(msg).commit(&mut agg)` publish on
commit. The same `Service` can handle commands from multiple transports
simultaneously — HTTP, gRPC, bus, and in-process dispatch all share the same handlers,
repository and command ledger. A bus delivers a command as the transport
identity (`system:transport`): the command must list
`microsvc::TRANSPORT_ROLE` (`"transport"`) in its roles, or the delivery is
refused, and the message must carry an id. The ledger command ID is derived
from that id, so a redelivery replays the receipt. For finer-grained control, call the `listen` / `subscribe` facade
methods directly. See [Service Bus](bus-and-outbox.md#service-bus).

### Error Handling

A command that does not commit returns a `CausalDispatchError`; the HTTP and
gRPC transports answer with its status and a `{ "code", "error" }` body. A
handler's client error (4xx `HandlerError`) is recorded as that command ID's
rejection, so a retry replays it; a server error records nothing final, so a
retry runs the command again:

| Cause | Status | `code` |
|---|---|---|
| Unknown command, bad envelope or command ID, input that does not decode | 400 | `BAD_REQUEST` |
| No authenticated user (or handler `Unauthorized`) | 401 | `UNAUTHORIZED` |
| Session lacks a declared role | 403 | `FORBIDDEN` |
| Handler `NotFound` | 404 | `NOT_FOUND` |
| Command ID reused for different input, or still in progress | 409 | `COMMAND_ID_REUSE` / `COMMAND_IN_PROGRESS` |
| Command ID past its replay retention | 410 | `COMMAND_EXPIRED` |
| Handler `Rejected`, guard returned `false` | 422 | `REJECTED` |
| Repository or other host failure | 500 | `INTERNAL` |
| Lost the aggregate's stream-version race to concurrent writers; nothing committed, retry | 503 | `CONCURRENT_WRITE` |
| Service lifecycle gate closed (HTTP) | 503 | `APPLICATION_RELOADING` |

Internal (5xx) errors are **masked** before being returned to clients — the
response body carries a generic `"internal error"` so SQL text, driver
detail, or internal paths never leak. The original error is logged
server-side. Client-fault (4xx) errors keep their descriptive message. This
applies identically to the HTTP and gRPC transports.

### Security / Trust Boundary

**This framework does NOT authenticate requests.** The `Session` is an opaque
string map built from whatever the transport provides — HTTP request headers,
gRPC metadata, and (for gRPC) the request payload's `session_variables`.
Identity claims are trusted at face value by handlers. Claim **names** are
deployment convention, not a fixed protocol (`Session::user_id` /
`Session::roles` only look up the convenience keys `x-user-id` / `x-roles`).

You **must** deploy `microsvc` behind a **trusted proxy / API gateway**
(JWT middleware, authenticating ingress, a query-layer action such as Hasura,
a custom BFF, …) that:

- **Strips** any client-supplied identity headers/metadata on the way in, and
- **Injects** only identity claims it has authenticated.

Without that proxy, any caller can set identity keys and assume any identity
or role.

**Source precedence:** when identity arrives in more than one place, the trusted
transport channel wins over the client-controlled payload. For gRPC, transport
**metadata overrides** payload `session_variables` — a client cannot override a
proxy-injected subject claim via the request body. For HTTP, request headers
populate the session and the proxy is responsible for ensuring they are
authenticated. Never trust the request body for identity.

A bus or Knative delivery authenticates nobody: whoever may publish to the
broker is one subject. Its command runs as `system:transport` with only the
`transport` role, and identity and role keys the message carries are
discarded. Only commands that list that role accept it.
