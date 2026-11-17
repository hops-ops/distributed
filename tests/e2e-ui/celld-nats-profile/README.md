# Optional celld + NATS profile (not the default playground)

This directory is an **optional** split-process profile of the same e2e-ui
Svelte app. It is **not** `make run` and **not** a replacement for the
one-process playground (`DCS-DEC-001`, `ESM-REQ-009`).

Default remains:

```sh
cd tests/e2e-ui
make up    # Postgres + Zitadel
make run   # one backend + UI
```

Optional profile:

```sh
cd tests/e2e-ui
make up-celld-nats     # celld local store + Queue relay + NATS
make test-celld        # live --test celld + GraphQL wait-path smoke (CI)
make test-celld-nats   # GraphQL wait-path smoke + SQL list only
make down-celld-nats   # NATS only
make down-celld        # celld; local state is preserved

cd ../e2e-celld
make run               # new GraphQL service crates + the Svelte UI
```

`tests/e2e-ui/crates/service/src/host.rs` stays a single backend process.
The playground UI against celld is the sibling example `tests/e2e-celld/`
(new service crates; same domain crates). Do not add that topology here.

## What this profile is

| Path | Where |
|---|---|
| GraphQL wait-path mutations | `CelldCommandHost` → celld `POST /todo/{id}/todo.create` (`{ commandId, input }` + internal identity headers) |
| Fire-and-forget / events | NATS JetStream `publish` / `subscribe` |
| Todo / Chat lists | SQL read models (projectors subscribe on NATS, **not** in cells) |
| BlobGames by-id | `ReadStore::CellByKey` GET of the sealed row |
| `@live` / SQL joins on Blob | rejected by the cell-by-key compiler (`DCS-5`) |

GraphQL, `@live`, and Eventual projectors are **not** cell class methods
(`DCS-AC-008.1`, `PCH-REQ-005`).

## Bring-up (local only)

celld runs through its CLI and keeps the object store under
`tests/celld/worker/.celld/dev`. The Make target first registers the separate
Queue consumer in that store, then serves the aggregate Worker. NATS is the
only Docker service in this optional profile.

```sh
cd tests/e2e-ui
make up-celld-nats
make test-celld-nats
```

celld listens on 18880 by default; 18080 belongs to Zitadel's edge from
`make up`, so both stacks run side by side. Override ports if busy:
`CELLD_HTTP_PORT=18881 NATS_PORT=14223 make up-celld-nats`.

The celld version is pinned in `/.celld-version` (currently 0.6.2) and
checked by `scripts/celld.sh`. To keep an older `celld` on PATH, point the
profile at the pinned binary: `CELLD_BIN=/path/to/celld make up-celld-nats`.
If `14222` is already taken by a leftover `docker run` NATS, `make down-celld-nats` removes that container too.

The relevant manual commands are:

```sh
# Build both Workers. The Make target also copies the relay bundle beneath the
# aggregate project so both deployments share worker/.celld/dev.
(cd tests/celld/worker && worker-build --release)
(cd tests/celld/relay-worker && worker-build --release)

# Register the Queue-only deployment, stop that dev listener, then serve the
# aggregate project from the same local store. Use make up-celld-nats for the
# complete, guarded lifecycle.
celld dev tests/celld/worker/relay.wrangler.jsonc --port 18880 --logs
celld dev tests/celld/worker --port 18880 --logs

# NATS for this optional profile only
docker compose -f tests/e2e-ui/celld-nats-profile/docker-compose.yml up -d

export CELLD_URL=http://127.0.0.1:${CELLD_HTTP_PORT:-18880}
export NATS_URL=nats://127.0.0.1:${NATS_PORT:-14222}

cargo test --test e2e_ui_celld_nats_profile --features graphql,http,sqlite
```

Without `CELLD_URL` **and** `NATS_URL`, the test still checks that the
default host is one-process and this profile is documented; live smoke
is skipped (`PCH-AC-006.1`).

Tear down NATS only: `docker compose -f tests/e2e-ui/celld-nats-profile/docker-compose.yml down`.
Do not use that as `make down` for the playground.

## Identity

Reuse e2e-ui OIDC / DevHeaders. No new secret files or object-store emulator
credentials are required. The checked-in internal relay secret is test-only.
