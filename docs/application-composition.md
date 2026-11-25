# Application composition (logical + runtime)

**Status:** accepted direction (product DX)  
**Goal:** Make the same packages easy to run as a monolith or as microservices, with persistence/locks/transports as a separate, boring plane.  
**Non-goal:** Back-compat aliases or dual public APIs for the same concept.

Motivating pain (now resolved): e2e-ui once had a single `service.rs` holding the projection catalog, surfaces and command walls, and a runner that hand-wired SQLite/Postgres × API/outbox/consumer. Today the composition root is `tests/e2e-ui/crates/service/src/application.rs` (application name and client surfaces), infrastructure is `host.rs` (one path for SQLite and Postgres), each bounded context is a module under `modules/` (listed by `modules/compose.rs`), the application manifest is derived from the composed `Service`, and `crates/runner/src/main.rs` is a short entrypoint.

---

## Two planes

| Plane | Contents | Changes when… |
|--------|----------|----------------|
| **Logical** | Domains, command transitions, projections, client surfaces | Product features |
| **Runtime** | Event store, read models, locks, bus, outbox/consumer, GraphQL/HTTP, identity | Deploy cut & dialect |

Microservices re-cut **logical mounts** and **process role**.  
Dialect (SQLite ↔ Postgres) re-cuts **runtime only**.

### CAP / Atomic

| Consistency | Split command process vs projector process? |
|-------------|-----------------------------------------------|
| **Eventual** | Yes — command emits; projector applies the same mutation IR async |
| **Atomic** | No for the seal — handler applies IR in-process / same tx as commit. Other services may *read* the row later; they cannot own the atomic seal |

Framework must **reject** Atomic command mounts on projector-only or query-only processes.

Client optimism is always the same path (transition event → role-visible mutation IR → optimistic layer → seal). Eventual vs Atomic only changes **where the server applies IR** and **how the client proves the seal**.

---

## Logical building blocks

### Packages (write once)

```text
domains/        aggregates + events
projections/    descriptors + mutation IR
commands/       command transition + handler — not bound to one process
readmodels/     schemas + grants
```

### Mounts (compose)

Each fact is stated once. A projection's name, epoch and model come from its
`projection!` descriptor; a read model's permissions live next to it
(`impl AuthorizedReadModel`); a module is generic over one `ServiceStores`
bound; the application's modules are the Service's command namespaces.

```rust
// modules/projections.rs
LocalProjectionMounts::builder("e2e-ui", "ordered-domain-events")
    .eventual(TODOS)
    .eventual(CHAT_MESSAGES)
    .direct(BLOB_GAMES) // sealed by blob commands in their own transaction
    .eventual(AUTH_USERS)
    .build()?;

// modules/todo.rs
pub fn routes<St: ServiceStores>(stores: &St, projections: &LocalProjectionMounts)
    -> Routes<AggregateDependencies<St, Todo>>
{
    Routes::for_aggregate(stores)
        .mount(todo_domain::commands::create())
        .consume_projection(projections.projector(TODOS))
}

// readmodels: one list for schema bootstrap, the engine and client export
ReadModels::new("e2e-ui").model::<Todos>().model::<ChatMessages>()…;

// application.rs: each client surface declared once
pub const CLIENT_SURFACE: ClientSurface = ClientSurface {
    name: "e2e-ui", eligible_roles: &["admin", "user"], schema_roles: &["user"],
};

// the manifest: derived, never listed by hand
service.application("e2e-ui", SurfaceSpec::from_surface("e2e-ui", &surface)?)?;
```

### Command defs co-located with handlers

Product intent lives next to the handler; `service`/app only **registers**:

```rust
// handlers/todo_complete.rs (sketch)
pub fn def() -> CommandDef {
    command_transition::<
        domain_commands::Complete,
        TodoCompleteInput,
        Eventual<TodoStatusPayload>,
    >(COMMAND)
        .field_name("todos_complete") // or derive from COMMAND
        .roles(["user", "admin"])     // or route-group default
        .handle(handle)
}
```

The generated transition supplies its event set and statically known recorder
values. The projection owns the event→mutation mapping; unknown values stay
unknown and force revalidation. Do hide: topology digests, partition codec
version, source binding, catalog activation ceremony.

### SystemSlice (microservices re-cut)

```rust
// package-level slice
fn todo_system() -> SystemSlice {
    SystemSlice::new()
        .commands([/* … */])
        .projection(TODOS).eventual().epoch("todos-v2")
}

// deployables
Application::new("todos-write").include(todo_system().commands_only())…
Application::new("todos-projector").include(todo_system().projectors_only())…
Application::new("e2e-ui").include(todo_system()).include(chat_system())… // Full
```

---

## Runtime building blocks

### One dialect choice pairs store + locks + bus

```rust
// host.rs: written once against SqlStores
if postgres_url {
    host(PostgresStores::connect(database_url).await?, options).await
} else {
    host(SqliteStores::connect(database_url).await?, options).await
}

async fn host<St: SqlStores>(stores: St, options: HostOptions) -> Result<(), BoxError> {
    stores.bootstrap_read_models(read_models().catalog()).await?;
    let service = build_service(&stores).with_bus(stores.bus("e2e-ui")); // also drains the outbox
    // GraphQL over stores.graphql_pool(), spawn_service_consumer_loop, serve
}

// client-manifest / unit tests
build_service(&Stores::new(InMemoryRepository::new(), InMemoryLockManager::new(), repo));
```

Defaults from the same pool (e2e today):

| Concern | Paired implementation |
|---------|------------------------|
| Event store | Sqlite / Postgres / InMemory repository |
| Read models | Same store unless explicitly split later |
| Locks | Matching lock manager |
| Bus | Matching bus + group name |
| Migrations | `connect_and_migrate` at runtime build |

### Process roles (what this process runs)

| Role | Spawns / enables |
|------|------------------|
| **Full** | Commands + eventual projectors + outbox + consumer + GraphQL (monolith / e2e) |
| **CommandWriter** | Aggregates, commands, outbox publish; no consumer |
| **EventualProjector** | Bus consume + projector mounts; no public write GraphQL required |
| **QueryApi** | GraphQL + surfaces; read path |

```rust
runtime
    .app(logical_app)
    .role(ProcessRole::Full) // or Writer / Projector / QueryApi
    .identity(identity_from_env())
    .bind(bind)
    .run()
    .await?;
```

Outbox dispatcher and eventual consumer are **role-selected**, not copy-pasted per dialect in `main`.

### Transports as edges

| Transport | Typical role |
|-----------|----------------|
| GraphQL (queries + commands) | Full / CommandWriter / QueryApi |
| Bus (outbox → consumer) | Full / Writer publish + Projector consume |
| HTTP command routes | Optional / ingress only (e.g. Zitadel) |
| OIDC / identity | API-facing processes |

Primary write path for apps remains GraphQL commands. HTTP command routes stay off: a process exposes one command with `with_http_command(command, edge)` (the edge authenticates the request), or every command with `with_http_command_routes` behind a trusted proxy. `command_gateway_routes` keeps every command path on the service binding of a gateway.

---

## Layered mental model

```text
┌──────────────────────────────────────────────────────────┐
│  Packages: domain · commands · projections · read models   │  write once
├──────────────────────────────────────────────────────────┤
│  Logical app: mounts, surfaces, Eventual vs Atomic         │  product graph
├──────────────────────────────────────────────────────────┤
│  Process role: Full | Writer | Projector | Query           │  microservice cut
├──────────────────────────────────────────────────────────┤
│  Runtime: store + locks + bus + workers + HTTP/GraphQL     │  dialect / host
└──────────────────────────────────────────────────────────┘
```

---

## Target shape of e2e-ui

| Today | Target |
|--------|--------|
| Done: projection mounts read name, epoch and model from the descriptor | `LocalProjectionMounts::builder(..).eventual(TODOS)` |
| Done: commands are `command!` declarations mounted with `.mount(..)` | — |
| Done: one `ReadModels` list and one `ClientSurface` table feed engine and export | — |
| Done: one host path over `SqlStores`; `with_bus` drains the outbox | `Runtime::from_env().role(Full).app(...).run()` (process roles) |

Rough size: **~100–150 lines of product wiring** for logical app + a tiny deployable main — not ~950 lines of framework internals.

---

## Implementation order

1. **Projection mount / inventory** — hide catalog, topology digests, activation. Done: `LocalProjectionMounts::builder`.  
2. **CommandDef + `.register()`** — co-locate transitions/handlers with command modules.
3. **Pair repo + locks + bus** — done: `Stores`, `SqliteStores`, `PostgresStores` (`SqlStores`).  
4. **ProcessRole** — Full / CommandWriter / EventualProjector / QueryApi spawn policy.  
5. **Atomic mount checks** — fail closed if Atomic is mounted without write path.  
6. **Collapse e2e-ui `service.rs` + `runner`** onto the new APIs (no dual/legacy surface). Done: see `application.rs`, `host.rs` and `modules/` in `tests/e2e-ui/crates/service/src/`.

No second application-owned optimism map: projection mutation IR is the source
of cache effects.

---

## Acceptance sketch (later)

- [ ] e2e-ui logical app does not mention partition codec, topology digests, or manual catalog activate.  
- [ ] Adding a command is “def next to handler + one register line,” not a 12-line block in a central file.  
- [x] Runner has one path for sqlite/postgres selected by URL/env.  
- [ ] A dual-process smoke (command writer + eventual projector) can share packages with Full e2e.  
- [ ] Atomic blob commands refuse projector-only process configuration.  
- [ ] Client gen still works from logical app (preview IR) without a live pool.

---

## Non-goals

- Hiding the transition or projection that defines optimistic behavior.
- Pretending remote Atomic projectors exist.  
- Second “simple” API that still requires the full catalog dance in user code.
