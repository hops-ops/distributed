# Zitadel ingestor local runbook

Normative architecture, authenticity, reconciliation, identity, and projection
design lives in the Distributed GitKB at
`specs/e2e-ui/zitadel-ingestor`. This file intentionally contains only the
commands needed to operate and verify the in-repository teaching fixture.

## Configuration

| Environment variable | Purpose |
|---|---|
| `ZITADEL_INGESTOR_SECRET` | shared secret required by Action ingress and the on-demand scrape command |
| `ZITADEL_INGESTOR_ALLOW_ACTION_EVENTS` | local-only exception for native Actions v2 bodies |
| `ZITADEL_SERVICE_USER_TOKEN` | PAT used to list users through the Management API |
| `ZITADEL_API_URL` or `OIDC_ISSUER` | Zitadel API base |
| `ZITADEL_SCRAPE_INTERVAL_SECS` | background reconciliation interval; `0` disables it |
| `ZITADEL_SCRAPE_ON_START` | run one reconciliation at process start |

The fixture accepts the ingress secret through
`x-zitadel-ingestor-secret: <secret>` or
`Authorization: Bearer <secret>`. A missing or invalid secret returns HTTP 401.

## Start the fixture

```bash
cd tests/e2e-ui
export ZITADEL_INGESTOR_SECRET=dev-secret-change-me
# The same value may instead be placed in e2e-ui.env before make run.
make run
```

The `e2e-runner` process starts the outbox dispatcher and bus consumers.

## Run an on-demand scrape

```bash
curl -sS -X POST "http://127.0.0.1:8791/zitadel.scrape.v1" \
  -H "content-type: application/json" \
  -H "x-zitadel-ingestor-secret: $ZITADEL_INGESTOR_SECRET" \
  -d '{}'
```

The response reports `{ listed, published, skipped, errors }`.

## Verify ingress authentication

```bash
BASE=http://127.0.0.1:8791

curl -sS -o /dev/null -w "%{http_code}\n" \
  -X POST "$BASE/zitadel.ingress.v1" \
  -H 'content-type: application/json' \
  -d '{"event_type":"user.human.created","provider_subject":"u1","delivery_id":"d0"}'
```

The unauthenticated request must return `401`.

## Import one human

Run this after `make up` has produced `e2e-ui.env`:

```bash
set -a && source e2e-ui.env && set +a
BASE=http://127.0.0.1:8791
SECRET="${ZITADEL_INGESTOR_SECRET:-e2e-zitadel-ingestor-secret}"

curl -sS -X POST "$BASE/zitadel.ingress.v1" \
  -H "content-type: application/json" \
  -H "x-zitadel-ingestor-secret: $SECRET" \
  -d "{
    \"delivery_id\": \"local-create-alice\",
    \"event_type\": \"user.human.created\",
    \"provider_subject\": \"$E2E_HUMAN_ALICE_UID\",
    \"email\": \"alice@e2e.local\",
    \"display_name\": \"Alice\",
    \"approval_status\": \"approved\"
  }"
```

The request publishes `zitadel.user.human.created.v1`. After the outbox drains,
the corresponding `auth_users` row supplies chat-author and Blob Game owner
display data.

## Verify GraphQL joins

```graphql
query DirectoryJoins {
  chat_messages(where: { room_id: { _eq: "lobby" } }) {
    body
    author_id
    author { display_name email status }
  }
  blob_games {
    game_id
    owner_id
    owner { display_name email }
  }
  todos {
    title
    owner { display_name }
  }
}
```

`auth_users.user_id` is the Zitadel subject. Chat `author_id`, Blob Game
`owner_id` and Todo `owner_id` are the OIDC `sub`, so the joins resolve after
that subject has been ingested.

The relationships are one way: `ChatMessages.author`, `BlobGames.owner` and
`Todos.owner` point at `AuthUsers`, and `AuthUsers` declares none back (see
`crates/readmodels/src/lib.rs`), so `auth_users { chat_messages { … } }` is not
part of the schema. Query the referencing model and filter by its foreign key
instead.

## Tests

```bash
cd tests/e2e-ui
cargo test -p e2e-service --lib
cargo test -p e2e-readmodels --lib
```
