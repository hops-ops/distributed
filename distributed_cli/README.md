# distributed_cli (`distributed`)

Service tooling for [Distributed](https://crates.io/crates/distributed): a `distributed`
binary — and a library — that scaffolds service crates, inspects a service's
logical ApplicationManifest, and renders physical read-model schema artifacts
(SQL or an Atlas Operator resource).

```bash
cargo install distributed_cli   # installs the `distributed` binary
```

It is also a library, so another CLI can mount its commands instead of
reimplementing them. `hops`, for example, exposes the same surface under
`hops service` by depending on this crate and dispatching with
`distributed_cli::run`. **Everything below documented as `distributed <cmd>` is also
available as `hops service <cmd>`.**

## `distributed build` — one coherent application generation

```bash
cd my-application
distributed build

# Equivalent from another directory:
distributed build path/to/my-application
```

Application authors write Rust crates and, optionally, a conventional `ui/`
SvelteKit project. They do not write a lifecycle catalog, executor list, or
process-supervisor JSON.

The CLI asks Cargo for the workspace model, locates the crate that exports the
typed `ApplicationManifest`, locates the runtime binary, and detects
`ui/package.json`. A generated scaffold records the two Rust targets in Cargo
package metadata. Existing workspaces with one conventional `*-service` library
and one runtime binary need no metadata at all.

The typed application export is the semantic boundary. It is composed from the
authored crates and preserves the framework's separation of responsibility:

- domain code contributes command APIs and their RBAC;
- read models contribute query/subscription GraphQL surfaces and their RBAC;
- projections map domain events to read-model mutations used by the server and
  browser optimism;
- Rust/WASM pure functions cover optimistic transitions that cannot be
  predicted from command inputs alone; once declared, they are required build
  artifacts compiled by `distributed build` and `distributed dev` before Vite;
- SvelteKit `+page.graphql` documents add page-level `@load` loaders and `@live`
  subscriptions, generated against the read-model query surface and hydrated
  into the browser replica.

`distributed build` compiles the Rust runtime, then builds the typed application
manifest and every client declared by `ui/distributed.clients.json` as one
dependency-ordered lifecycle generation. The generated clients and SvelteKit
boundary plans live in that immutable generation rather than being published
into the source tree. Vite builds against the candidate client tree, and the CLI
only activates it after the UI build succeeds. An invalid manifest, GraphQL
document, boundary binding, or UI build therefore leaves the prior generation
active.
Lifecycle receipts, the application manifest, and generated client trees are
tool-owned state under `.distributed/lifecycle/`.
During `distributed dev`, each process launch has a supervisor-owned identity.
Client-only generations can retain an unchanged API process: the active cohort
explicitly admits that instance, and its GraphQL responses advertise the current
generation. Preparing, replaced and retired instances cannot dispatch mutations.
Rollback launches receive new identities; schema compatibility alone never
authorizes an old process. Upgrade the CLI and runtime together and restart dev
after an upgrade so every member receives the matching lifecycle contract.
Rust binaries remain in Cargo's target directory and SvelteKit output remains
in its adapter-selected output directory.

Use `--output json` for a machine-readable lifecycle report. The compatibility
flags `--root`, `--catalog`, and `--config` exist for older low-level lifecycle
fixtures; normal application builds do not use them.

## `distributed dev` — coherent local supervision

```bash
cd my-application
distributed dev
```

`distributed dev` first generates and activates the typed application manifest
and every declared client, then starts the Cargo runtime and `npm run dev` for
`ui/`, waits for both readiness probes, and prints the usable API and browser
URLs. No generated client tree or generation preflight exists in application
source. Vite owns Svelte/CSS HMR; GraphQL or binding-only changes rebuild the
client node without restarting Rust, while Rust contract changes rebuild the
application and its downstream clients together. The browser switches
`$distributed` imports only
after the immutable generation pointer activates. Ctrl-C shuts down all process
groups and their descendants.

Shell environment variables win. The CLI then loads `<project-name>.env` and
`.env` when present, without displaying their values. `BIND` selects the API
address; `UI_HOST` and `UI_PORT` select the Vite URL. Infrastructure setup is
still project-specific—for example, the e2e UI's `make up` creates its local
Zitadel/Postgres environment—but starting the application no longer needs a
lifecycle shell script.

The lifecycle CLI contract has black-box Bats coverage. It builds and starts a
generated Rust application plus conventional SvelteKit project with no lifecycle JSON,
as well as lower-level coverage for atomic activation, rollback, selective
restarts, readiness, cancellation, and descendant cleanup:

```bash
# Requires Bats; CI installs the pinned version used by the project.
make test-cli-lifecycle
```

## `distributed scaffold <name>` — generate a service crate

```bash
distributed scaffold orders --store postgres --transport http --gitops
```

Writes a ready-to-build Distributed service under `./<name>` (override with
`--path`). Common flags: `--store <postgres|sqlite|in-memory>`, `--transport
<http|knative>`, `--model <name>` (repeatable), `--read-models`, `--command` /
`--event` (repeatable), `--bus <rabbitmq|kafka|psql|nats>`, `--gitops`,
`--metrics prometheus`, `--tracing` / `--otel`, `--gitops-promote <argo|flux>`,
`--github OWNER/REPO`, `--force`. See `distributed scaffold --help` for the full list.

When used with `--gitops`, `--metrics prometheus` emits Prometheus Operator
`ServiceMonitor` and `PrometheusRule` templates for HTTP services. The
generated values default both resources to disabled; enable them only in
clusters with the Prometheus Operator CRDs installed. Plain `--gitops` does not
emit `monitoring.coreos.com` resources.

`--tracing` enables Distributed's optional `otel` span feature, emits a default
OTLP tracing setup in the generated `main.rs`, and renders OTLP environment
values in the Helm chart without hard-coding an endpoint.

## `distributed skills init` — extract agent skills into a project

```bash
distributed skills init                    # writes ./.distributed/skills/ and wires harnesses
distributed skills list                    # names + descriptions of the embedded skills
```

Materializes the **agent skills** embedded in the binary — markdown guidance
for coding agents on using Distributed (`distributed-usage`, `distributed-ci`,
`distributed-schema`) — into `.distributed/skills/<name>/SKILL.md` (override
the container with `--path <dir>`, which yields `<dir>/skills/...`). No network
and no repo checkout: the binary that scaffolded your service carries the
matching guidance for it.

`--agents <list>` wires the skills for native discovery by agent harnesses.
The canonical files live under the container; each harness location gets a
**per-skill symlink** to the canonical folder (a real copy on platforms
without reliable symlinks), anchored at the container's parent directory —
one on-disk copy, and your own skills coexist next to the links:

| value | effect |
|------|--------|
| `auto` (default) | wire every harness with evidence in the project root (`.claude/` → claude; `AGENTS.md`/`.agents/`/`.gemini/`/`.pi/` → agents); a fresh project wires both |
| `claude` | link each skill at `.claude/skills/<name>` (Claude Code) |
| `codex`, `grok`, `openai`, `gemini`, `pi`, `agents` | link each skill at `.agents/skills/<name>` (Codex, Grok Build, Gemini CLI, Pi) and maintain a sentinel-delimited managed block in `AGENTS.md` (created if absent); user content outside the sentinels is preserved |
| `none` | canonical `.distributed/skills/` files only |

Re-runs are safe and idempotent — per file: absent → `created`, identical →
`unchanged`, locally edited → skipped with a warning (`--force` to overwrite,
printed as `updated`). A harness path that is not a link to the canonical
folder (a stale link, or a directory from an older copy-based layout) is
likewise skipped unless `--force` replaces it. Files you add under the skills
directories are never touched. After a CLI upgrade, re-run with `--force` to
refresh existing skill files to the binary's embedded content; without
`--force`, differing files are treated as local edits and skipped.

## The artifact entrypoints

`describe` compiles your service crate and calls the explicit logical
application-manifest entrypoint — by default `<crate>::application_manifest`.
`schema` calls the separate physical read-model catalog entrypoint — by default
`<crate>::read_model_catalog`. Keep those owners separate:

```rust
use distributed::{ReadModelCatalog, ReadModel};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("orders")]
pub struct OrderView {
    #[id("order_id")]
    pub order_id: String,
    pub status: String,
}

pub fn read_model_catalog() -> ReadModelCatalog {
    ReadModelCatalog::new("orders").read_model::<OrderView>()
}
```

Point at a different function with `--entrypoint <path>`. Application-manifest
exports compile with the dependencies already resolved by the target crate.
Schema generation also invokes framework schema helpers directly, so those
commands need the local `distributed` crate to be resolvable — found
automatically from the workspace, or pass `--distributed-path` / set
`DISTRIBUTED_PATH`.

## `distributed client-manifest` — authorized client surface

```bash
distributed client-manifest > target/distributed-client.json
```

Compiles the service's `distributed_client_surface` export into the versioned,
role/application-selected manifest used by the operation compiler. The export
already contains one concrete role or named application surface; it is not an
admin catalog that downstream tools filter themselves.

The client-manifest harness uses the framework type already resolved by the
target service. A published service therefore does not need a local
`distributed` checkout or `--distributed-path` for client generation.

## `distributed client` — typed query, live, and command artifacts

```bash
distributed client \
  --manifest target/distributed-client.json \
  --role user \
  --documents 'src/**/*.graphql' \
  --out src/generated/distributed

# CI: parse, validate, and compare without writing
distributed client \
  --manifest target/distributed-client.json \
  --role user \
  --documents 'src/**/*.graphql' \
  --out src/generated/distributed \
  --check
```

The requested `--role` or `--surface` must exactly match the manifest's
authorized identity. Generation validates the GraphQL documents, injects only
authorized wire-only identity metadata, derives an exact live companion for
`@live`, and emits framework-neutral TypeScript replica artifacts alongside the
manifest-owned command and protocol operations. Operation IDs hash the exact
full document sent over GraphQL; they do not imply an APQ/persisted-operation
registry.

Each operation artifact also contains the closed variable/input codec compiled
from that selected surface. The runtime applies GraphQL singleton-list coercion,
canonical scalar encoding, unknown-field rejection, and deterministic deep
freezing before variables can identify a cache entry or reach the network.
Manifest v7 and variable codec v2 carry the selected service's exact
`max_depth`, `max_bool_width`, and `max_in_list` contract. Static literal and
mixed-variable filters are rejected during generation when their known shape
exceeds those limits; runtime variables carry per-use `filterBaseDepth` and
`maxItems` constraints. Reused variables receive the most restrictive
intersection across every root, relationship selection, and aggregate use.
The current query surface supports complete and offset-window roots; cursor
artifacts are not certified and therefore fail closed to revalidation.

`@load` and `@live` compile into framework-neutral island metadata. Rendering
adapters own placement: the SvelteKit adapter discovers route documents and
sibling component documents, promotes them to page/layout boundaries, and
supports typed explicit boundary registration when static discovery cannot
prove ownership. Unsupported or unprovable selections fail at build time with
their source location; the compiler does not emit a partial normalization plan
or an adapter-specific route registry.

## `distributed describe` — manifest as JSON

```bash
distributed describe                       # current directory
distributed describe --manifest-path path/to/Cargo.toml --package orders-service
```

Prints the versioned manifest envelope (schemas, services, transports) as JSON —
a stable contract for other tooling.

## `distributed schema` — schema artifacts

Renders the **desired-state** schema for the manifest's read models and
operational tables. Output goes to stdout by default (or `--out <file>`).

### SQL (default)

```bash
distributed schema --dialect postgres      # or --dialect sqlite
```

### Atlas Operator resource (`--format atlas`)

Wraps the desired-state SQL into an `AtlasSchema` (`db.atlasgo.io/v1alpha1`) for
the [ariga atlas-operator](https://github.com/ariga/atlas-operator), so the
operator diffs the live database against it and applies the migration in-cluster.

The resource is written to **stdout** — `distributed` deliberately does not pick a
location for it. Redirect it wherever you keep schema manifests: a file in the
service repo, or a separate GitOps/schema repo.

```bash
distributed schema --format atlas \
  --name orders \
  --namespace data \
  --db-secret orders-db \
  --db-secret-key url \
  > orders.schema.yaml
```

| flag | maps to |
|------|---------|
| `--name` | `metadata.name` (required; RFC-1123 label) |
| `--namespace` | `metadata.namespace` (optional) |
| `--db-secret` / `--db-secret-key` | `spec.urlFrom.secretKeyRef` — the GitOps-friendly choice, no credentials in the manifest (`--db-secret-key` defaults to `url`) |
| `--db-url` | inline `spec.url` — convenient for dev; avoid committing real credentials |
| `--dev-url` | `spec.devURL` — a scratch database Atlas uses to plan changes |
| `--dialect` | SQL dialect of the wrapped schema (`postgres` default) |

Provide a database reference via **either** `--db-secret` or `--db-url` (not
both). Example output:

```yaml
apiVersion: db.atlasgo.io/v1alpha1
kind: AtlasSchema
metadata:
  name: orders
  namespace: data
spec:
  urlFrom:
    secretKeyRef:
      name: orders-db
      key: url
  schema:
    sql: |
      CREATE TABLE IF NOT EXISTS "orders" (
        ...
      );
```

Names are validated as RFC-1123 labels (lowercase letters, digits, hyphens; no
leading/trailing hyphen) so generation fails with a clear message rather than
emitting YAML the API server would reject.
