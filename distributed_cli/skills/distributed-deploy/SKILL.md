---
name: distributed-deploy
description: Deploy a Distributed service - container image, runtime environment, a Hops WebService resource, and an Atlas Operator schema resource for its read models. Use when writing deployment manifests, CI image builds, or database schema rollout for a Distributed service.
---

# Deploying a Distributed service

`distributed` does not generate deployment manifests, charts or CI workflows.
A service is an ordinary HTTP container; write the few manifests it needs by
hand (or have your platform's tooling do it). The only deploy artifact the CLI
renders is the read-model schema, because that is derived from the code.

## What the container needs

| Setting | Purpose |
|---|---|
| Listen address | The service binds whatever its `main.rs` reads (`BIND` in the layout from the `distributed-usage` skill). Bind `0.0.0.0:<port>` in a container. |
| `DATABASE_URL` | Postgres or SQLite URL for the repository and read models. |
| `DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY` | GraphQL services only. Exactly 32 bytes, stable across replicas and restarts. Keep it in a Secret. |
| `OIDC_ISSUER`, `OIDC_AUDIENCE` (or `OIDC_CLIENT_ID`) | GraphQL services only. Bearer-token identity; see the `distributed-graphql` skill. |
| `GRAPHIQL=0` | Turn the GraphiQL IDE off outside development. |

`GET /health` is the readiness and liveness probe. `GET /metrics` serves
Prometheus text when the service is built with the `metrics` feature.

A service that consumes a bus, relays an outbox or runs timers does work
between HTTP requests. Do not scale it to zero.

## Workload: Hops `WebService`

On a Hops platform one `WebService` gives the container a Knative service,
TLS, routing and optional platform sign-in:

```yaml
apiVersion: workloads.hops.ops.com.ai/v1alpha1
kind: WebService
metadata:
  name: orders
  namespace: orders
spec:
  image: ghcr.io/example/orders:v1.4.0
  port: 8080
  exposure: private            # cluster | private | public
  scaling:
    minReplicas: 1             # background work: never zero
  env:
    - name: BIND
      value: 0.0.0.0:8080
    - name: DATABASE_URL
      valueFrom:
        secretKeyRef: { name: orders-db, key: url }
    - name: DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY
      valueFrom:
        secretKeyRef: { name: orders-graphql, key: protocol-token-key }
  probes:
    readiness:
      httpGet: { path: /health, port: 8080 }
```

`PORT` is reserved by `WebService`; set the address through your own variable
as above. Use an immutable image tag, never `latest`.

`WebService` is HTTP only. It does not create brokers, queues or
subscriptions: provision the bus separately and pass its URL as environment.

Without Hops, the same container runs as a plain Kubernetes `Deployment` and
`Service` with the same environment and probe.

## Schema: `AtlasSchema`

The read-model tables come from `read_model_catalog()`. Render them as an
Atlas Operator resource and commit the output next to the workload manifest:

```bash
distributed schema --format atlas --name orders --namespace orders \
  --db-secret orders-db > orders.schema.yaml
```

Regenerate it in CI and fail on a diff so the manifest cannot drift from the
code. Flags and the SQL and GraphQL formats are in the `distributed-schema`
skill.

The framework's own tables (events, outbox, command ledger) are not in this
resource; the repository creates them with `connect_and_migrate`.
