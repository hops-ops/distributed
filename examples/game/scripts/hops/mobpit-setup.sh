#!/usr/bin/env bash
# Mob Pit Environment setup (runs on the host before the chart):
# - builds what the pods load from this checkout: JS SDK, WebAssembly, the
#   cells worker (`./run.sh build`; incremental);
# - mobpit: this Environment's ticket, internal and admin secrets (made once);
# - mobpit-rustfs: the fleet bucket's credentials, for the worker deployer.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
GAME="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! has_secret mobpit-celld celld-rustfs; then
	echo "mobpit setup: mobpit-celld/celld-rustfs missing; enable mobpit-system first (hops local env enable mobpit-system)" >&2
	exit 1
fi

"$GAME/run.sh" build

if ! has_secret "$NAMESPACE" mobpit; then
	log "creating the Environment's secrets"
	# Not test-only: the worker relays to a cluster Service, not loopback.
	k -n "$NAMESPACE" create secret generic mobpit \
		--from-literal=GAME_TICKET_SECRET="$(hex 32)" \
		--from-literal=DISTRIBUTED_INTERNAL_SECRET="$(hex 32)" \
		--from-literal=DG_DEMO_ADMIN_SECRET="$(hex 24)"
fi

# The fleet's bucket credentials (AWS_*, S3_ENDPOINT), refreshed each run.
k -n mobpit-celld get secret celld-rustfs -o json |
	python3 -c 'import json,sys; s=json.load(sys.stdin); print(json.dumps({"apiVersion":"v1","kind":"Secret","type":"Opaque","metadata":{"name":"mobpit-rustfs"},"data":s["data"]}))' |
	k -n "$NAMESPACE" apply -f - >/dev/null
