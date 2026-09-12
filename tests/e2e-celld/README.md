# e2e-celld

New example, sibling of `tests/e2e-ui`. It is **not** `make run` in e2e-ui.

| Crate | Role |
|---|---|
| `todo-domain` / `chat-domain` / `blob-domain` | **same** domain crates as e2e-ui (path deps) |
| `e2e-celld-todo` | Todo mounts + `CelldRoute` (kind/shard/payload) for `TodoCell` |
| `e2e-celld-chat` | Chat mounts + `CelldRoute` for `ChatCell`; `@live` stays here |
| `e2e-celld-blob` | Blob Atomic commands (in-process) |
| `e2e-celld-identity` | Zitadel ingress/scrape + AuthUsers projector |
| `e2e-celld-graphql` | GraphQL process (`graphql_router_with_host`) |
| `tests/celld/worker` | `TodoCell` + `ChatCell` |
| `tests/celld/relay-worker` | Queue-only canonical-message relay |

`distributed::cell_host::CelldCommandHost` wait-dispatches `todo.create` /
`todo.complete` / `chat.post` to `{CELLD_URL}/{kind}/{shard}/{command}`.
Aggregate crates only supply a `CelldRoute` (kind, shard id, payload map).
The cell commits its complete aggregate state and outbox as one versioned
SQLite value, then publishes pending messages to celld Queue through
`CelldQueuePublisher`. A Queue-only Worker relays canonical messages to the
authenticated native endpoint. The endpoint uses `BusPublisher<NatsBus>`;
Kafka, RabbitMQ, or Knative only change that native publisher. There is no
response-based outbox fallback. Eventual projectors fill SQL so `@live` still
fires. Blob and identity stay in-process. GraphQL is the user edge
(`OidcBearer` on the engine); Zitadel Actions and the Queue relay are internal
HTTP on the same process. GraphQL and projectors are not cell class methods.

## Private HTTP boundary

All cell reads, commands, Queue relay deliveries, and Zitadel internal routes
require `DISTRIBUTED_INTERNAL_SECRET` in the
`x-distributed-internal-secret` header. Startup fails when the secret is
missing or invalid. Local compose ports and the GraphQL listener bind to
loopback by default. The checked-in value in the worker config is test-only;
real deployments must install a unique secret binding and still use TLS and
network policy. Health endpoints remain public and disclose no secret.

The threat model assumes the GraphQL edge has already verified user identity.
The internal secret prevents a network caller from forging trusted user/role,
service, partition, or relay headers. Strict envelopes, body limits, URL
encoding, disabled redirects, timeouts, Queue leases, and stable message ids
bound the damage from malformed, replayed, slow, or concurrent requests.

Workspace tests (no live celld):

```sh
make test               # cargo test --workspace (CI)
```

```sh
cd tests/e2e-ui
make up                 # Zitadel + Postgres (read models + login)
make up-celld-nats      # celld 0.4 local store + NATS

cd ../e2e-celld
make run                # public gateway :8791 + internal UI :5180 (watches sources)
```

Eventual projectors and `@live` use **Postgres** from `e2e-ui.env`
(`DATABASE_URL`). There is no SQLite read-model path. Cells still keep
private SQLite per Durable Object. Override with `E2E_CELLD_DATABASE_URL`.

`make run` delegates the application to `distributed dev`:

| Surface | How |
|---|---|
| Svelte UI | Lifecycle-owned sibling UI + Vite HMR |
| GraphQL host | Coherent Distributed generation rebuild/restart |
| Cell worker | `cargo-watch` → `worker-build --dev` + `celld deploy` + compose restart |

`WATCH_WORKER=0` turns the independent worker loop off. Compose
`CELLD_WATCH` is the node's SQLite working directory, not a source watcher
— celld loads a deployment at startup, so worker changes need a restart
(the watch target does that). First `cargo-watch` install: `cargo install cargo-watch`.

The Cargo workspace declares `../e2e-ui/ui` as its UI component. Both
`distributed build` and `distributed dev` resolve it within the repository;
CI and users do not launch a second unmanaged `npm run dev` process.

Open `http://localhost:8791`. The navbar shows a **celld** badge. Sign in
(`alice` / `Password1!` when Zitadel is up). Todos create/complete and
lobby posts go to cells; open Chat in two tabs to see `@live` still fire.

Override a busy celld port: `CELLD_HTTP_PORT=18880 make run`.
