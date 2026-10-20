# Secrets (local gitops)

Gitops YAML is **non-secret**. Live `Secret` objects stay on the cluster (or
later ESO/SOPS). Config only **references** names/keys.

## AuthStack / Zitadel (`ns: auth`)

| Secret | Purpose | How to create |
|--------|---------|----------------|
| `zitadel-masterkey` | 32-byte Zitadel at-rest key (`AuthStack` `masterkey.secretRef`) | `kubectl -n auth create secret generic zitadel-masterkey --from-literal=masterkey='…'` (exactly 32 bytes) |
| `iam-admin-pat` | Chart FirstInstance PAT (auto after setup) | AuthStack / helm hook |
| `iam-admin` | Machine key JSON | AuthStack / helm hook |
| `login-client` | Login client PAT | AuthStack / helm hook |

ProviderConfig for MRs (after PAT exists):

```bash
hops local zitadel --context kind-hops --source-context kind-hops --source-namespace auth \
  --domain zitadel-zitadel.auth.svc.cluster.local --port 8080 --insecure
# creates default/zitadel-credentials + ProviderConfig secretRef
```

Helm default fullname is **`zitadel-zitadel`** — use that Service FQDN for
`domain` / OIDC issuer (no alias Service). Optional later:
`chartValues.fullnameOverride: zitadel`.

## Identity (UI chart)

Renders from **`ui/.gitops/deploy`** when `identity.enabled: true`
(see `gitops/envs/local/ui.yaml`).

| Scope | Resources |
|-------|-----------|
| **Cluster** | Project `e2e-ui`, roles, humans `alice`/`bob`/`admin` |
| **Worktree** | OIDC web app only (`e2e-ui-<ws>-web` — redirects + Login V2 baseUri) |

| Secret | Purpose |
|--------|---------|
| `e2e-human-passwords` | Keys `alice`, `bob`, `admin` (cluster-shared; local chart seeds when `local: true`) |
| `zitadel-credentials` | ProviderConfig / ClusterProviderConfig (`hops local zitadel`) |
| Oidc connection Secret | Oidc MR `writeConnectionSecretToRef` → `attribute.client_id` / `attribute.client_secret` (UI mounts these) |
| `e2e-ui-oidc` | `AUTH_SECRET` (+ optional `ZITADEL_SERVICE_USER_TOKEN` from AuthStack `auth/login-client`) |

```bash
# Humans (if not using chart local seed):
kubectl -n default create secret generic e2e-human-passwords \
  --from-literal=alice=Password1! \
  --from-literal=bob=Password1! \
  --from-literal=admin=Password1!

# Login V2 service user (after AuthStack Ready):
kubectl -n <workspace> create secret generic e2e-ui-oidc \
  --from-literal=AUTH_SECRET='local-workbench-dev-auth-secret-not-for-prod' \
  --from-literal=ZITADEL_SERVICE_USER_TOKEN="$(kubectl -n auth get secret login-client -o jsonpath='{.data.pat}' | base64 -d)" \
  --dry-run=client -o yaml | kubectl apply -f -
# OIDC_CLIENT_ID/SECRET: do not hand-copy — Deployment reads Oidc connection Secret.
```

Demo login (every env): **alice / bob / admin · Password1!**

Do **not** commit live `orgId`, Project external ids, or OIDC client ids into Application values. MRs use `projectIdRef` and ProviderConfig org default.
## e2e-ui app (`ns: --name`)

| Secret | Keys | Used by |
|--------|------|---------|
| `e2e-ui-oidc` | `AUTH_SECRET`, `ZITADEL_SERVICE_USER_TOKEN` | UI session/login residuals |
| `e2e-ui-<workspace>-oidc-conn` | `attribute.client_id`, `attribute.client_secret` | Oidc MR connection secret; UI client env |

```bash
# Login-client PAT: AuthStack residual secret auth/login-client key pat
kubectl -n dogfood create secret generic e2e-ui-oidc \
  --from-literal=AUTH_SECRET='local-workbench-dogfood-auth-secret-not-for-prod' \
  --from-literal=ZITADEL_SERVICE_USER_TOKEN="$(kubectl -n auth get secret login-client -o jsonpath='{.data.pat}' | base64 -d)"
```

Do not copy OIDC client IDs or secrets into this Secret. The Oidc managed
resource writes them to `e2e-ui-<workspace>-oidc-conn`, and the Deployment
references `attribute.client_id` / `attribute.client_secret` there.

Non-secret OIDC config (issuer, AUTH_URL) stays in `ui/.gitops/deploy/values.yaml`.

### Login V2 (required for browser sign-in)

Gitops (UI chart when `identity.enabled`):

1. **Oidc** app MR — redirects + `loginVersion.loginV2.baseUri` from release ns  
2. **Features** MR (`instance.zitadel…/Features`) — instance `loginV2.required`
   + `baseUri` so authorize redirects to this UI’s `/login`

Each worktree's OIDC app sets `loginVersion.loginV2.baseUri` to
`http://e2e-ui-ui.<workspace>.svc.cluster.local:5180`. That app-level URI is
what lets alice and bob sign in without sharing a host.

Instance Features are global on one AuthStack. The worktree env leaves
`identity.instanceLoginV2` false. Turn it on for one primary lane only. A
second true value replaces the first lane's login host. Do not hand-`PUT`
`/v2/features/instance`.

## SecretStack / ESO + Vault (`secrets/`)

Local fixture uses **`backend: vault`** with **`vault.install: true`** (in-cluster
Vault Helm + ESO). No AWS PodIdentity on dory.

| Resource | Purpose |
|----------|---------|
| `secrets/stack.yaml` | SecretStack XR — ESO + Vault Helm + ClusterSecretStore |
| `secrets/vault-auth-delegator.yaml` | CRB: Vault SA → `system:auth-delegator` (TokenReview) |
| Helm `vault` `server.postStart` | Init/unseal (once), KV mount, k8s auth + ESO role |
| PVC (`dataStorage` 1Gi) | File storage under `/vault/data` (survives pod restarts) |
| ClusterSecretStore `vault` | ESO → Vault (Ready after postStart + CRB) |

### Persistence

`server.dev` is **off**. Vault runs standalone with `storage "file"` on a PVC
(default StorageClass, dory: `local-path`). First start initializes and writes:

```text
/vault/data/.hops-init   # Unseal Key 1 + Initial Root Token (chmod 600)
```

Later starts: unseal from that file, re-apply k8s auth (idempotent). **KV data
survives pod restarts**; deleting the PVC is a full reset.

Root token is **not** the fixed string `root` (that was dev-only). For CLI writes:

```bash
export VAULT_TOKEN="$(
  kubectl -n vault exec vault-0 -- \
    awk '/^Initial Root Token:/{print $NF}' /vault/data/.hops-init
)"
export VAULT_ADDR=http://127.0.0.1:8200
# kubectl port-forward -n vault svc/vault 8200:8200   # if needed
hops secrets sync vault -y
```

### Declarative trust path

1. Apply cluster gitops (SecretStack + CRB).
2. postStart: init (once) → unseal → `secret/` KV v2 → k8s auth for ESO.
3. ClusterSecretStore `vault` Ready.
4. **Write** with `hops secrets sync vault`.
5. **Read** via ExternalSecrets (auth masterkey, UI oidc/humans).

Install package from source before first apply:

```bash
hops config install --path <meta>/xrs/stacks/aws/secret --context dory
hops local gitops cluster ./gitops/cluster
```

Switching an existing **dev/inmem** Vault release to file storage may need a
one-time recreate of the Helm release / PVC (data was ephemeral anyway).

Do **not** put the SecretStack claim under meta `local/` — that folder is for
ad-hoc colima-style one-off claims, not the workbench cluster gitops path.

## Rules

1. **Never** put passwords, PATs, client secrets, or masterkeys in committed YAML.
2. Gitops may name Secrets and keys (`secretRef` / `secretEnv`).
3. Create Secrets before the XRs/Deployments that need them (or accept CrashLoop until present).
4. Cloud: prefer ESO / SOPS; local: `kubectl create` or `hops local zitadel|aws|github`.
5. Platform secrets engine for local CP lives in **`gitops/cluster/secrets/`**, not `local/`.
