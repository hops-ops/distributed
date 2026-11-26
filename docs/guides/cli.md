# The `distributed` CLI

## Service CLI (`distributed`)

The [`distributed_cli`](../../distributed_cli/) crate ships `distributed` — tooling to build and run an
application, inspect a service's logical application artifact, and render physical
read-model schema artifacts. It is
also a library, so another CLI can mount the same commands under one of its own.

The CLI exists to keep the generated and handwritten parts of a back-end service
separate. A Distributed service should usually reduce to a small custom surface:
aggregate models, command/event handlers, read models, and the occasional
handwritten integration. The framework, macros, application artifacts, and CLI generate the
repeatable wiring around that surface.

That boundary matters for AI-assisted development. AI generation is
probabilistic, so Distributed tries to make the AI-authored surface small and
make the surrounding structure deterministic. Event storming produces commands,
past-tense events, aggregates, policies, and read models. Those names map
directly onto Distributed conventions, so an AI assistant can generate or revise
a smaller target: model fields, event methods, handler bodies, and projection
shapes. Manifest discovery, client generation and schema output stay deterministic.

For an existing typed `Service`, derive the logical application from its command
inventory and the same full GraphQL `Surface` used by the runtime:

```rust,ignore
let surface = distributed::application::SurfaceSpec::from_surface("catalog", &full_surface)?;
let application = service.application("catalog", surface)?;
let manifest = application.manifest();
```

Command namespaces such as `orders.submit` supply module names; there is no
second module list to maintain. Assembly rejects commands missing from the
Surface or exposed by the Surface without a Service owner. Role-selected client
exports remain authorization views of that full contract. A complete
application manifest, including all surfaces and module inventories, is
bounded at 16 MiB (the CLI's manifest ingress budget); each opaque JSON
contract inside it is bounded at 1 MiB. A generated Surface contract uses the
complete-manifest budget rather than the opaque-value cap; its typed contents
are still checked against the collection, string and nesting limits.

An event-driven policy that emits through another aggregate can explicitly carry
the incoming command's causal identity before recording its events:

```rust,ignore
ctx.inherit_causation(&mut downstream)?;
downstream.record(observation)?;
```

This requires an incoming causation ID and does not manufacture one for external
events. NATS preserves the message's payload content type and keeps reserved
transport headers out of user metadata across delivery.

The crates and the npm package ship as one release: the CLI, the
`distributed` crate and `@hops-ops/distributed` must be the exact same version.
Once published, `cargo install distributed_cli` installs a matching
`distributed`; it cannot serve a source checkout, whose versions only match the
CLI built from that checkout. From a checkout, run the CLI with
`cargo run -p distributed_cli --bin distributed -- <cmd>`; application
checkouts such as [`tests/e2e-ui`](../../tests/e2e-ui/) wrap this as `make run`.
The CLI does not generate a project. `distributed skills init` installs agent
skills that describe the service layout; see `distributed-usage`.

```bash
cd orders
cargo test
distributed describe                  # print the ApplicationManifest as JSON
distributed schema --dialect postgres # render migration SQL from read models
```

The serialized manifest stores module declarations and selected Surface
contracts without repeating top-level `commands`, `events`, `models`, or
`projections`; the Rust `ApplicationManifest` decoder reconstructs those
inventories and validates their ownership.

Modeled projection contracts [share repeated operations and selector schemas](../compact-projection-contracts.md)
when smaller, preserving every current and retained event selector within the
same manifest budget.

For a full application, run `distributed build` or `distributed dev` from its
Cargo workspace root. The CLI discovers the typed application, runtime binary,
conventional `ui/` SvelteKit app, and `@hops-ops/distributed` dependency. A
published app must use the same exact Distributed version in Cargo, the CLI,
and npm because those artifacts ship as one release; a mismatch fails before
the build and names all three resolved versions.

A UI may instead use a local dependency such as
`"@hops-ops/distributed": "file:../../../js"`. When Rust, CLI, and JS resolve
from the same checkout, the lifecycle installs that package's build
dependencies, rebuilds stale source, verifies its exported files, and records a
receipt under the application `.distributed/` directory. `distributed dev`
keeps the linked package compiled while Vite handles application HMR. Changed
framework outputs are built in isolation and published content-by-content, so
unchanged files do not create a Vite reload storm. The framework receipt is a
declared application input, so a real framework change still activates a new
coherent generation. There is no separate JS preparation command for
application authors.

When `ui/distributed.clients.json` is present, `distributed build` and
`distributed dev` also own client generation. The inventory and
`ui/distributed.config.js` become declared lifecycle inputs; generated client
trees are stored beside the typed application manifest in the same immutable
generation. A clean checkout contains no generated client tree. A
GraphQL document or colocated binding edit invalidates only the client node,
while a Rust surface edit invalidates the application node and its client
successor. Failed client compilation never advances the active pointer.

### Coherent development reloads

CSS and HMR-safe Svelte modules stay on Vite's native fast path. A Rust,
generated-client, worker, required WASM, or linked-framework change that needs
the whole application follows one bounded transaction:

```mermaid
sequenceDiagram
    participant Edit as Source edit
    participant Dev as distributed dev
    participant Browser as SvelteKit browser
    participant API as API / workers

    Edit->>Dev: declared input changes
    Dev->>Dev: stage manifest + generated clients; validate pending generation
    Dev-->>Browser: preparing(from, to, compatibility)
    Browser->>Browser: close command gate; capture declared state + confirmed replica
    Browser-->>Dev: bounded prepare acknowledgement
    Dev->>API: restart affected processes with pending IDs
    Dev->>Dev: readiness probes; atomically activate
    Dev-->>Browser: active generation
    Browser->>Browser: one controlled document reload
    Browser->>Browser: restore compatible partitions; revalidate the rest
```

The browser capsule is size/depth/deadline bounded. It contains only explicitly
declared JSON state, confirmed replica data, and pending command IDs—never auth
credentials or command inputs. Browser commands close before optimism and
transport, while API mutations and worker/service message dispatch fail closed
until the process generation is active. A failed prepare, process readiness
probe, or activation leaves the prior generation active and continues watching
for the next edit.

Use the event-storming board as the input:

- Aggregates become `#[sourced]` models.
- Commands become typed command handlers, one module each.
- Events and policy/projection subscriptions become event handlers.
- Query views become `#[derive(ReadModel)]` structs registered in
  `read_model_catalog()`.

The `metrics` feature adds `GET /metrics` to an HTTP service.
Bus-only and worker services can expose the same registry on a side port with
`distributed::metrics::serve_http("0.0.0.0:9100", Some("orders-worker")).await?`,
or compose `distributed::metrics::http_router_for_service("orders-worker")` into
an existing Axum app. Scrape `GET /metrics` (Prometheus text). Keep `/metrics` on
a **private** listener — unauthenticated by design.

**Label policy (closed set):** `service`, `version` (service info only),
`message_kind`, `message`, `status`, `transport`, `outcome`, `failure_class`,
`action`, histogram `le`, plus GraphQL `root_field` when applicable. Do **not** label metrics with `user_id`, `tenant_id`, free-form paths,
or raw command input (unknown commands bucket as `message=unknown`).

`describe`/`schema` compile your crate and call explicit artifact entrypoints
(override with `--entrypoint`). `describe` reads the logical
`application_manifest()` owner; `schema` reads the separate
`read_model_catalog()` owner that registers the [read models](projections-and-read-models.md#read-models) and
tables defining physical schema:

```rust,ignore
pub fn read_model_catalog() -> distributed::table::ReadModelCatalog {
    distributed::table::ReadModelCatalog::new("orders").read_model::<OrderView>()
}
```

### Apply schema in-cluster with Atlas

`distributed schema --format atlas` wraps the desired-state SQL into an `AtlasSchema`
(`db.atlasgo.io/v1alpha1`) for the [ariga atlas-operator](https://github.com/ariga/atlas-operator),
so migrations apply declaratively in-cluster. The resource is written to
**stdout** — redirect it wherever you keep schema manifests (a file, or a separate
GitOps repo); `distributed` does not choose a location for it.

```bash
distributed schema --format atlas --name orders --db-secret orders-db > orders.schema.yaml
```

Use `--db-secret`/`--db-secret-key` for a Secret reference (GitOps-friendly) or
`--db-url` for an inline dev URL; `--namespace` and `--dev-url` are optional. Full
reference: [`distributed_cli/README.md`](../../distributed_cli/README.md).
