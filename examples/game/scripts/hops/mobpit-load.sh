#!/usr/bin/env bash
# Mob Pit load test on Hops local: runs the `mobpit-load` bots inside the
# Mob Pit services container (Rust, the mounted checkout, the warm cargo
# cache) against celld's public listener, and samples the celld container's
# CPU / memory from the kind node (`crictl stats`) while they play.
#
#   examples/game/scripts/hops/mobpit-load.sh --steps 8,16,32 --hold-secs 30
#
# Arguments go to `mobpit-load`. Raise the room cap first (Environment
# value `rooms.maxPlayers`), and set the celld CPU limit on the CelldStack
# (`values.resources.limits.cpu`). Output: the table on stdout; JSON rows
# and celld samples (one JSON line every ~2 s) under $OUT
# (default ./.data/load).
set -euo pipefail

CONTEXT=kind-hops
NAMESPACE="${MOBPIT_NAMESPACE:-distributed-mobpit-world}"
CELLD_NAMESPACE="${CELLD_NAMESPACE:-mobpit-celld}"
NODE="${HOPS_NODE:-hops-control-plane}"
OUT="${OUT:-.data/load}"
RUN="$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

k() { kubectl --context "$CONTEXT" "$@"; }

pod="$(k -n "$NAMESPACE" get pods -l app.kubernetes.io/name=mobpit -o jsonpath='{.items[0].metadata.name}')"
limit="$(k -n "$CELLD_NAMESPACE" get pod celld-0 -o jsonpath='{.spec.containers[?(@.name=="celld")].resources.limits.cpu}')"
echo "bots in $NAMESPACE/$pod · celld CPU limit: ${limit:-none} · run $RUN" >&2

# celld container samples: Unix seconds, cores, working set bytes.
sampler() {
	while true; do
		docker exec "$NODE" crictl stats -o json --label io.kubernetes.container.name=celld \
			--label "io.kubernetes.pod.namespace=$CELLD_NAMESPACE" -s 2 2>/dev/null |
			jq -c --arg at "$(date +%s)" '.stats[0] | {at: ($at | tonumber),
				cores: ((.cpu.usageNanoCores.value // 0 | tonumber) / 1e9),
				workingSetMb: ((.memory.workingSetBytes.value // 0 | tonumber) / 1048576)}' || true
	done
}
sampler >"$OUT/$RUN-celld.jsonl" &
trap 'kill $! 2>/dev/null || true' EXIT

k -n "$NAMESPACE" exec "$pod" -c services -- bash -c '
	set -euo pipefail
	# exec starts in the container workingDir: examples/game/server.
	cargo run -q --locked --release --example mobpit-load -- \
		--url ws://celld.'"$CELLD_NAMESPACE"'.svc.cluster.local:8080 \
		--secret "$DISTRIBUTED_GAME_SECRET" --json /tmp/mobpit-load.jsonl "$@" 2>/dev/null
' bash "$@" | tee "$OUT/$RUN.txt"
k -n "$NAMESPACE" exec "$pod" -c services -- cat /tmp/mobpit-load.jsonl >"$OUT/$RUN.jsonl"
echo "rows $OUT/$RUN.jsonl · celld samples $OUT/$RUN-celld.jsonl" >&2
