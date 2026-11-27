# Shared by the Mob Pit Hops setup scripts (sourced). Hops runs setup with
# HOPS_ENVIRONMENT_NAMESPACE and HOPS_LOCAL_CONTEXT; only the local Hops
# cluster is ever used (the default kubectl context may be a real cluster).
# `orbstack` is the machine cluster; `kind-hops` is the retired one.
set -euo pipefail

CONTEXT="${HOPS_LOCAL_CONTEXT:-orbstack}"
if [[ "$CONTEXT" != orbstack && "$CONTEXT" != kind-hops ]]; then
	echo "mobpit setup: refusing kube context '$CONTEXT' (Hops local only: orbstack, kind-hops)" >&2
	exit 1
fi
NAMESPACE="${HOPS_ENVIRONMENT_NAMESPACE:?run by Hops (HOPS_ENVIRONMENT_NAMESPACE)}"

k() { kubectl --context "$CONTEXT" "$@"; }
has_secret() { k -n "$1" get secret "$2" >/dev/null 2>&1; }
hex() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
log() { printf '[mobpit setup] %s\n' "$*"; }
