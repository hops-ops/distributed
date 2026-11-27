#!/usr/bin/env bash
# Chart contract tests for multi-workspace identity scopes.
# No cluster required — helm template only.
#
#   cd tests/e2e-ui && ./scripts/helm-contract-test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UI_CHART="$ROOT/api/.gitops/local"
IDENTITY_CHART="$ROOT/ui/.gitops/test-users"
API_CHART="$ROOT/api/.gitops/local"
fail=0

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required tool: $1" >&2
    exit 2
  }
}
need helm
need rg

render_ui() {
  local ns="$1"
  local oidc_generation="${2:-0}"
  local workspace="${3:-}"
  helm template "e2e-ui-application-${ns}" "$UI_CHART" \
    --namespace "$ns" \
    --set local=true \
    --set preview=false \
    --set "environment.name=${ns}" \
    --set "environment.namespace=${ns}" \
    --set identity.enabled=true \
    --set "identity.oidcGeneration=${oidc_generation}" \
    --set "identity.workspace=${workspace}" \
    --set identity.projectName=e2e-ui \
    --set identity.projectNamespace=default \
    --set identity.humansNamespace=default \
    --set identity.providerConfigRef.name=default \
    --set identity.providerConfigRef.kind=ClusterProviderConfig
}

render_identity() {
  local ns="$1"
  local features="${2:-true}"
  local oidc_generation="${3:-0}"
  local workspace="${4:-}"
  helm template "e2e-ui-test-users-${ns}" "$IDENTITY_CHART" \
    --namespace "$ns" \
    --set local=true \
    --set preview=false \
    --set "environment.name=${ns}" \
    --set "environment.namespace=${ns}" \
    --set identity.enabled=true \
    --set "identity.oidcGeneration=${oidc_generation}" \
    --set "identity.workspace=${workspace}" \
    --set identity.projectName=e2e-ui \
    --set identity.projectNamespace=default \
    --set identity.humansNamespace=default \
    --set "identity.instanceLoginV2=${features}" \
    --set identity.providerConfigRef.name=default \
    --set identity.providerConfigRef.kind=ClusterProviderConfig
}

render() {
  local ns="$1"
  local features="${2:-true}"
  local oidc_generation="${3:-0}"
  local workspace="${4:-}"
  render_ui "$ns" "$oidc_generation" "$workspace"
  render_identity "$ns" "$features" "$oidc_generation" "$workspace"
}

assert_contains() {
  local hay="$1" needle="$2" msg="$3"
  if ! printf '%s' "$hay" | rg -q --fixed-strings -- "$needle"; then
    echo "FAIL: $msg (missing: $needle)" >&2
    fail=1
  else
    echo "ok: $msg"
  fi
}

assert_not_contains() {
  local hay="$1" needle="$2" msg="$3"
  if printf '%s' "$hay" | rg -q --fixed-strings -- "$needle"; then
    echo "FAIL: $msg (unexpected: $needle)" >&2
    fail=1
  else
    echo "ok: $msg"
  fi
}

assert_not_matches() {
  local hay="$1" pattern="$2" msg="$3"
  if printf '%s' "$hay" | rg -q "$pattern"; then
    echo "FAIL: $msg (unexpected match: $pattern)" >&2
    fail=1
  else
    echo "ok: $msg"
  fi
}

assert_matches() {
  local hay="$1" pattern="$2" msg="$3"
  if ! printf '%s' "$hay" | rg -U -q -- "$pattern"; then
    echo "FAIL: $msg (missing match: $pattern)" >&2
    fail=1
  else
    echo "ok: $msg"
  fi
}

check_workspace() {
  local ns="$1"
  local features="$2"
  echo "=== workspace ${ns} (instanceLoginV2=${features}) ==="
  local out
  out="$(render "$ns" "$features")"

  assert_contains "$out" "name: e2e-ui-${ns}-web" "OIDC app name is worktree-scoped"
  assert_contains "$out" "namespace: ${ns}" "worktree resources reference ${ns}"
  assert_contains "$out" "baseUri: \"http://e2e-ui-ui.${ns}.svc.cluster.local:5180\"" \
    "Login V2 / OIDC baseUri uses release namespace"
  assert_contains "$out" "http://e2e-ui-ui.${ns}.svc.cluster.local:5180/auth/callback/oidc" \
    "OIDC redirect uses release FQDN"
  assert_contains "$out" "namespace: default" "shared identity keeps default namespace"
  assert_contains "$out" "name: e2e-ui" "shared Project name e2e-ui"
  assert_contains "$out" "projectRoleCheck: true" \
    "Project requires a GitOps-owned role grant"
  assert_contains "$out" "name: e2e-role-user" "shared role e2e-role-user"
  assert_contains "$out" "name: e2e-alice" "shared human e2e-alice"
  assert_contains "$out" "apiVersion: auth.hops.ops.com.ai/v1alpha1" \
    "demo humans use the auth-stack HumanUser XR"
  assert_contains "$out" "name: e2e-alice-e2e-ui" "shared Grant for alice"
  assert_contains "$out" "name: e2e-bob-e2e-ui" "shared Grant for bob"
  assert_contains "$out" "name: e2e-admin-e2e-ui" "shared Grant for admin"
  assert_contains "$out" "userIdRef:" "Grant resolves HumanUser by reference"
  assert_contains "$out" "projectIdRef:" "Grant resolves Project by reference"
  assert_contains "$out" "apiVersion: auth.hops.ops.com.ai/v1alpha1" \
    "Grant resolves the auth-stack HumanUser XR"
  assert_contains "$out" 'roles: ["user","admin"]' "admin receives user and admin roles"
  assert_not_contains "$out" "oidc-local-seed" \
    "GitOps does not overwrite the residual OIDC session/PAT Secret"
  assert_contains "$out" "name: \"e2e-ui-${ns}-oidc-conn\"" \
    "OIDC connection secret is worktree-scoped"
  assert_contains "$out" "key: attribute.client_id" \
    "UI reads client id from Oidc connection secret"
  assert_contains "$out" "key: attribute.client_secret" \
    "UI reads client secret from Oidc connection secret"
  assert_contains "$out" "namespace: default" "projectIdRef targets shared Project ns"
  assert_contains "$out" "projectIdRef:" "OIDC references Project"
  # Project MR must declare default ns (not worktree)
  if ! printf '%s' "$out" | rg -U -q 'kind: Project\nmetadata:\n  name: e2e-ui\n  namespace: default'; then
    # tolerate key reorder from helm
    if ! printf '%s' "$out" | awk '/kind: Project/{p=1} p&&/namespace: default/{found=1} p&&/^---/{exit} END{exit !found}'; then
      echo "FAIL: Project not clearly in default namespace" >&2
      fail=1
    else
      echo "ok: Project in default namespace"
    fi
  else
    echo "ok: Project in default namespace"
  fi

  if [ "$features" = "true" ]; then
    assert_contains "$out" "kind: Features" "Features MR rendered when instanceLoginV2"
    assert_contains "$out" "name: e2e-ui-login-v2" "Features name stable"
  else
    assert_not_contains "$out" "kind: Features" "no Features when instanceLoginV2=false"
  fi

  assert_not_contains "$out" "hops-wt-" "no legacy hops-wt- prefix"
  assert_not_matches "$out" 'orgId:\s*"[0-9]{15,}"' \
    "no generated organization UUID in GitOps"
}

check_workspace alice true
check_workspace bob false

# Chart default must not emit instance Features. A new lane that forgets the
# flag would otherwise clobber the primary lane's login host.
default_out="$(helm template e2e-ui-test-users-carol "$IDENTITY_CHART" \
  --namespace carol \
  --set local=true \
  --set preview=false \
  --set environment.name=carol \
  --set environment.namespace=carol \
  --set identity.enabled=true \
  --set identity.projectName=e2e-ui \
  --set identity.projectNamespace=default \
  --set identity.humansNamespace=default \
  --set identity.providerConfigRef.name=default \
  --set identity.providerConfigRef.kind=ClusterProviderConfig)"
assert_not_contains "$default_out" "kind: Features" \
  "omitted instanceLoginV2 does not render global Features"
assert_contains "$default_out" "baseUri: \"http://e2e-ui-ui.carol.svc.cluster.local:5180\"" \
  "omitted instanceLoginV2 still sets the app-level Login V2 baseUri"

# Distinct OIDC names across workspaces
alice_out="$(render alice true)"
bob_out="$(render bob false)"
assert_contains "$alice_out" "e2e-ui-alice-web" "alice OIDC name"
assert_contains "$bob_out" "e2e-ui-bob-web" "bob OIDC name"
assert_not_contains "$alice_out" "e2e-ui-bob-web" "alice render has no bob OIDC"
assert_not_contains "$bob_out" "e2e-ui-alice-web" "bob render has no alice OIDC"

# Rotation changes the managed OIDC resource and connection Secret generation.
# The new UI pod cannot start until Crossplane publishes that generation's
# credentials, avoiding a race with stale data under a stable Secret name.
rotated_out="$(render alice true 1)"
assert_contains "$rotated_out" "name: e2e-ui-alice-web-g1" "OIDC generation rotates MR name"
assert_contains "$rotated_out" 'hops.ops.com.ai/oidc-generation: "1"' \
  "OIDC generation rolls UI pod template"
assert_contains "$rotated_out" 'name: "e2e-ui-alice-oidc-conn-g1"' \
  "OIDC rotation uses matching connection secret generation"

# The combined application derives the connection Secret once for its API and
# UI processes. No Zitadel UUID belongs in values.
api_out="$(helm template e2e-ui-api-alice "$API_CHART" \
  --namespace alice \
  --set local=true \
  --set preview=false \
  --set environment.name=alice \
  --set environment.namespace=alice \
  --set identity.enabled=true \
  --set identity.oidcGeneration=1 \
  --set identity.providerConfigRef.name=default \
  --set identity.providerConfigRef.kind=ClusterProviderConfig)"
assert_contains "$api_out" 'hops.ops.com.ai/oidc-generation: "1"' \
  "API OIDC generation rolls pod template"
assert_contains "$api_out" 'name: "e2e-ui-alice-oidc-conn-g1"' \
  "API uses matching generated OIDC connection secret"
assert_contains "$api_out" "name: OIDC_AUDIENCE" \
  "API audience comes from the generated connection secret"
assert_contains "$api_out" "key: attribute.client_id" \
  "API audience/client id use the generated client id key"
assert_matches "$api_out" \
  'name: OIDC_CLIENT_ID\n[ ]+valueFrom:\n[ ]+secretKeyRef:\n[ ]+name: "e2e-ui-alice-oidc-conn-g1"\n[ ]+key: attribute.client_id\n[ ]+- name: OIDC_CLIENT_SECRET\n[ ]+valueFrom:\n[ ]+secretKeyRef:\n[ ]+name: "e2e-ui-alice-oidc-conn-g1"\n[ ]+key: attribute.client_secret' \
  "UI maps generated OIDC client id and secret to distinct keys"
assert_contains "$api_out" "--package distributed_cli --bin distributed" \
  "combined application starts Distributed dev"
assert_contains "$api_out" "name: e2e-ui-api" \
  "combined application exposes the API Service"
assert_contains "$api_out" "name: e2e-ui-ui" \
  "combined application exposes the UI Service"
assert_not_contains "$api_out" "cargo watch" \
  "chart does not bypass the lifecycle with cargo-watch"
deployment_count="$(printf '%s\n' "$api_out" | rg -c '^kind: Deployment$')"
if [ "$deployment_count" != 1 ]; then
  echo "FAIL: combined application must render one Deployment (got $deployment_count)" >&2
  fail=1
else
  echo "ok: combined application renders one Deployment"
fi

environment_manifest="$(cat "$ROOT/.gitops/local/environment.yaml")"
assert_contains "$environment_manifest" "path: api/.gitops/local" \
  "Environment deploys the combined application chart"
assert_not_contains "$environment_manifest" "path: ui/.gitops/local" \
  "Environment has no independent UI runtime deploy"

# An explicit workspace override is shared by the UI and test-users charts.
# The environment should pass this value through its shared values, rather than
# overriding only one deploy and producing a client-Secret name mismatch.
workspace_out="$(render alice true 1 dogfood)"
assert_contains "$workspace_out" "name: e2e-ui-dogfood-web" \
  "shared workspace override names the OIDC app"
assert_contains "$workspace_out" 'name: "e2e-ui-dogfood-oidc-conn-g1"' \
  "shared workspace override names the OIDC connection Secret"
assert_not_contains "$workspace_out" "e2e-ui-alice-web" \
  "shared workspace override removes the environment-name OIDC app"

api_workspace_out="$(helm template e2e-ui-api-alice "$API_CHART" \
  --namespace alice \
  --set local=true \
  --set preview=false \
  --set environment.name=alice \
  --set environment.namespace=alice \
  --set identity.enabled=true \
  --set identity.workspace=dogfood \
  --set identity.oidcGeneration=1 \
  --set identity.providerConfigRef.name=default \
  --set identity.providerConfigRef.kind=ClusterProviderConfig)"
assert_contains "$api_workspace_out" 'name: "e2e-ui-dogfood-oidc-conn-g1"' \
  "API uses the shared workspace override"

# Local residual OIDC credentials are opt-in and cannot coexist with the
# ExternalSecret path for the same target Secret.
seeded_out="$(helm template e2e-ui-test-users-seeded "$IDENTITY_CHART" \
  --namespace alice \
  --set local=true \
  --set identity.enabled=true \
  --set identity.seedLocalOidcSecret=true)"
assert_contains "$seeded_out" "hops.ops.com.ai/secret: oidc-local-seed" \
  "explicit local OIDC seed renders when enabled"
external_out="$(helm template e2e-ui-test-users-external "$IDENTITY_CHART" \
  --namespace alice \
  --set local=true \
  --set identity.enabled=true \
  --set identity.seedLocalOidcSecret=true \
  --set externalSecrets.enabled=true)"
assert_not_contains "$external_out" "hops.ops.com.ai/secret: oidc-local-seed" \
  "local OIDC seed is suppressed when ExternalSecrets are enabled"
assert_contains "$external_out" "kind: ExternalSecret" \
  "ExternalSecret remains the sole OIDC Secret owner"

registry_manifest="$(cat "$ROOT/.gitops/local/cluster/registry/deployment.yaml")"
assert_contains "$registry_manifest" "mountPath: /var/lib/registry" \
  "package registry mounts persistent storage"
assert_contains "$registry_manifest" "claimName: registry" \
  "package registry uses the registry PVC"

if [ "$fail" -ne 0 ]; then
  echo "helm-contract-test: FAILED" >&2
  exit 1
fi
echo "helm-contract-test: OK"
