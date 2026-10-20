#!/usr/bin/env bash
# Local dual-workspace suite: two git worktrees, hops gitops for each, HMR title check.
#
# Prerequisites:
#   - local CP with hops (dory/colima/kind) and kubectl context
#   - AuthStack optional for identity; HMR does not require login
#   - hops binary on PATH or HOPS=path
#   - helm, git, node, npm
#
#   cd tests/e2e-ui && ./scripts/dual-worktree-suite.sh
#
# Policy: alice owns instance Login V2 Features; bob sets instanceLoginV2=false.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST_ROOT="$(cd "$ROOT/../.." && pwd)"
SCRATCH="${DUAL_SCRATCH:-${SCRATCH_DIR:-$(pwd)/.dual-worktree-scratch}}"
HOPS="${HOPS:-hops}"
KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
export KUBECONFIG

ALICE_NAME="${ALICE_NAME:-alice}"
BOB_NAME="${BOB_NAME:-bob}"
SKIP_GITOPS="${SKIP_GITOPS:-0}"
CLUSTER_PROVIDER="${CLUSTER_PROVIDER:-kind}"
DOCKER_PROVIDER="${DOCKER_PROVIDER:-dory}"
CLUSTER_NAME="${CLUSTER_NAME:-hops}"
CONTEXT="${CONTEXT:-kind-hops}"

log() { printf '%s\n' "$*"; }
die() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing $1"; }
need git
need helm
need node
need npm
need kubectl

if ! command -v "$HOPS" >/dev/null 2>&1; then
  # Prefer worktree-built hops-cli
  if [[ -x "$DIST_ROOT/../cli/target/release/hops-cli" ]]; then
    HOPS="$DIST_ROOT/../cli/target/release/hops-cli"
  elif [[ -x "$DIST_ROOT/../../cli/target/release/hops-cli" ]]; then
    HOPS="$DIST_ROOT/../../cli/target/release/hops-cli"
  else
    die "hops binary not found (set HOPS=)"
  fi
fi

mkdir -p "$SCRATCH"
log "scratch=$SCRATCH hops=$HOPS"

# --- helm contract (always) ---
chmod +x "$ROOT/scripts/helm-contract-test.sh"
"$ROOT/scripts/helm-contract-test.sh" | tee "$SCRATCH/helm-contract.log"

# --- two worktrees of distributed ---
ALICE_WT="$SCRATCH/wt-${ALICE_NAME}"
BOB_WT="$SCRATCH/wt-${BOB_NAME}"
BRANCH_ALICE="suite/dual-${ALICE_NAME}-$$"
BRANCH_BOB="suite/dual-${BOB_NAME}-$$"

cleanup_worktrees() {
  git -C "$DIST_ROOT" worktree remove --force "$ALICE_WT" 2>/dev/null || true
  git -C "$DIST_ROOT" worktree remove --force "$BOB_WT" 2>/dev/null || true
  git -C "$DIST_ROOT" branch -D "$BRANCH_ALICE" "$BRANCH_BOB" 2>/dev/null || true
}
trap cleanup_worktrees EXIT

cleanup_worktrees
git -C "$DIST_ROOT" worktree add -b "$BRANCH_ALICE" "$ALICE_WT" HEAD
git -C "$DIST_ROOT" worktree add -b "$BRANCH_BOB" "$BOB_WT" HEAD

# Overlay working-tree e2e-ui (includes uncommitted chart/script fixes) so the
# suite exercises the same tree under development, not only last commit.
overlay_e2e() {
  local dest="$1"
  rsync -a --delete \
    --exclude node_modules --exclude .svelte-kit --exclude dist \
    --exclude playwright-report --exclude test-results --exclude target \
    "$ROOT/" "$dest/tests/e2e-ui/"
}
overlay_e2e "$ALICE_WT"
overlay_e2e "$BOB_WT"

# Alice owns the one global Login V2 Features record. Bob must not.
set_instance_login() {
  local env_file="$1" value="$2"
  [[ -f "$env_file" ]] || return 0
  python3 - "$env_file" "$value" <<'PY'
import pathlib, sys
path, value = sys.argv[1], sys.argv[2]
file = pathlib.Path(path)
file.write_text(file.read_text().replace("instanceLoginV2: true", f"instanceLoginV2: {value}").replace("instanceLoginV2: false", f"instanceLoginV2: {value}"))
PY
}
set_instance_login "$ALICE_WT/tests/e2e-ui/gitops/envs/local/ui.yaml" true
set_instance_login "$BOB_WT/tests/e2e-ui/gitops/envs/local/ui.yaml" false

ALICE_E2E="$ALICE_WT/tests/e2e-ui"
BOB_E2E="$BOB_WT/tests/e2e-ui"
ALICE_URL="http://e2e-ui-ui.${ALICE_NAME}.svc.cluster.local:5180"
BOB_URL="http://e2e-ui-ui.${BOB_NAME}.svc.cluster.local:5180"

wait_http() {
  local url="$1" label="$2" n="${3:-90}"
  local i code
  for i in $(seq 1 "$n"); do
    code=$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 2 "$url/" 2>/dev/null || echo 000)
    if [[ "$code" == "200" ]]; then
      log "ready $label $url ($i)"
      return 0
    fi
    sleep 3
  done
  die "timeout waiting for $label at $url (last=$code)"
}

if [[ "$SKIP_GITOPS" != "1" ]]; then
  if ! kubectl get ns >/dev/null 2>&1; then
    die "kubectl cannot talk to a cluster (set KUBECONFIG); dual gitops requires local CP"
  fi
  log "reconciling workspace $ALICE_NAME from $ALICE_E2E"
  (cd "$ALICE_E2E" && "$HOPS" local gitops environment ./.gitops/local/environment.yaml \
    --name "$ALICE_NAME" --cluster-provider "$CLUSTER_PROVIDER" \
    --docker-provider "$DOCKER_PROVIDER" --cluster-name "$CLUSTER_NAME" \
    --context "$CONTEXT" --once) \
    | tee "$SCRATCH/gitops-alice.log" \
    || die "workspace gitops alice failed — see $SCRATCH/gitops-alice.log"
  log "reconciling workspace $BOB_NAME from $BOB_E2E"
  (cd "$BOB_E2E" && "$HOPS" local gitops environment ./.gitops/local/environment.yaml \
    --name "$BOB_NAME" --cluster-provider "$CLUSTER_PROVIDER" \
    --docker-provider "$DOCKER_PROVIDER" --cluster-name "$CLUSTER_NAME" \
    --context "$CONTEXT" --once) \
    | tee "$SCRATCH/gitops-bob.log" \
    || die "workspace gitops bob failed — see $SCRATCH/gitops-bob.log"
fi

wait_http "$ALICE_URL" alice 120
wait_http "$BOB_URL" bob 120

# Assert gitops isolation: distinct OIDC names if present
if kubectl get oidc.application.zitadel.m.crossplane.io -A >/dev/null 2>&1; then
  kubectl get oidc.application.zitadel.m.crossplane.io -A -o wide 2>/dev/null | tee "$SCRATCH/oidc-apps.log" || true
  kubectl get project.project.zitadel.m.crossplane.io -A 2>/dev/null | tee "$SCRATCH/projects.log" || true
fi

# Playwright HMR — fail the suite if this fails (do not swallow via bare pipe).
(
  cd "$ROOT"
  if [[ ! -d node_modules/playwright ]]; then
    npm install --no-fund --no-audit 2>&1 | tail -5
  fi
  npx playwright install chromium 2>&1 | tail -5
  node scripts/dual-worktree-hmr.mjs \
    --alice-url "$ALICE_URL" \
    --bob-url "$BOB_URL" \
    --alice-root "$ALICE_E2E" \
    --bob-root "$BOB_E2E" \
    --timeout-ms 180000
) 2>&1 | tee "$SCRATCH/dual-worktree-hmr.log"
hmr_rc=${PIPESTATUS[0]}
if [[ "$hmr_rc" -ne 0 ]]; then
  die "HMR check failed (exit $hmr_rc) — see $SCRATCH/dual-worktree-hmr.log"
fi

# Prove dory desktop name was not rewritten by workspace --name
dory_name_file="${HOME}/.hops/local/dory-name"
if [[ -f "$dory_name_file" ]]; then
  dn=$(tr -d '[:space:]' <"$dory_name_file")
  if [[ "$dn" == "alice" || "$dn" == "bob" ]]; then
    die "dory-name was corrupted to workspace name ($dn) — --dory-name / --name isolation broken"
  fi
  log "dory-name ok: $dn"
fi

log "dual-worktree-suite: OK"
log "  alice: $ALICE_URL"
log "  bob:   $BOB_URL"
