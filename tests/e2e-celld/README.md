# e2e-celld

The e2e-ui application on another command host. It is **not** `make run` in
e2e-ui: the modules are the same, the host is not.

| Crate | Role |
|---|---|
| `e2e-service` (`../e2e-ui/crates/service`) | **the** e2e-ui service: modules, projections, GraphQL surfaces, Zitadel ingestor, `build_service` |
| `e2e-celld-runner` (`crates/runner`) | the celld host: command routing, NATS bus, Queue relay route, `e2e-celld` binary |
| `tests/celld/worker` | `TodoCell` + `ChatCell` over the same `todo-domain` / `chat-domain` declarations |
| `tests/celld/relay-worker` | Queue-only canonical-message relay |

The runner mounts `e2e_service::build_service` unchanged and adds only what
celld changes:

```rust
CelldCommandHost::new(celld_url, service, internal_secret)?
    .route_aggregate::<Todo>()?
    .route_aggregate::<ChatMessage>()?
```

`route_aggregate::<A>()` routes every command of `A` mounted on the service
to `{CELLD_URL}/{kind}/{shard}/{command}`: `kind` is `A::aggregate_type()`
(`todo`, `chat_message`), `shard` the declaration's `shard:`. The cell answers
with the command's typed payload, and the host returns it as-is after checking
it against the declared output: a missing, undeclared or mistyped field fails
the command; nothing is filled in. A command of `A` without a declared shard
fails `route_aggregate` at startup.

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
make up-celld-nats      # celld local store + Queue relay + NATS

cd ../e2e-celld
make run                # public gateway :8791 + internal UI :5180 (watches sources)
make test-browser       # the full e2e-ui Playwright suite against this app
```

`make test-browser` runs every Playwright project (setup, user, admin, anon)
of `tests/e2e-ui` against this app; CI's live celld job does the same. On a
cold checkout, build once first (`cargo run -p distributed_cli -- build
tests/e2e-celld` from the repository root): `distributed dev` gives the API
five minutes to become ready, and a cold compile can take longer.

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

celld listens on 18880 by default (Zitadel owns 18080). Override a busy port
with `CELLD_HTTP_PORT=<port>` on both `make up-celld-nats` and `make run`.
