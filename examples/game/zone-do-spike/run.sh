#!/usr/bin/env bash
# Spike gr-p42: Mob Pit rooms as celld Durable Objects on a local
# multi-node celld fleet (production mode, one shared S3 bucket).
#
#   ./run.sh up [N]      S3 container + bucket, build, deploy, N nodes (default 3)
#   ./run.sh deploy      rebuild and publish a new version (nodes adopt it live)
#   ./run.sh nodes [N]   start N nodes        ./run.sh kill <a|b|c>  SIGKILL one node
#   ./run.sh status      per-node owned/resident cells (operator view)
#   ./run.sh down        stop nodes, remove the S3 container
#
# Nodes: node-a/b/c, public 127.0.0.1:18101-18103 (open any of them in a
# browser: /?room=pit-1&name=ana), operator 127.0.0.1:18201-18203.
# Needs: CELLD (celld >= 0.6.2), worker-build, esbuild (CELLD_ESBUILD), docker.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE="${SPIKE_STATE:-$HERE/.run}"
CELLD="${CELLD:-celld}"
export CELLD_ESBUILD="${CELLD_ESBUILD:-$HERE/../client/node_modules/.bin/esbuild}"
export AWS_ACCESS_KEY_ID=spikeadmin AWS_SECRET_ACCESS_KEY=spikesecret123 AWS_REGION=us-east-1
S3_PORT="${S3_PORT:-19400}"
export S3_ENDPOINT="http://127.0.0.1:$S3_PORT"
BUCKET="s3://zone-spike/zone"
NAMES=(a b c d)
mkdir -p "$STATE"

s3_up() {
	S3_PORT="$S3_PORT" docker compose -p zone-do-spike -f "$HERE/tools/s3-compose.yaml" up -d >/dev/null 2>&1
	for _ in $(seq 1 30); do curl -s -o /dev/null "$S3_ENDPOINT/" && break; sleep 1; done
	aws --endpoint-url "$S3_ENDPOINT" s3 mb s3://zone-spike >/dev/null 2>&1 || true
}

build() {
	(cd "$HERE" && worker-build --release >/dev/null)
	"$CELLD_ESBUILD" "$HERE/page/main.ts" --bundle --format=esm --target=es2022 \
		--outfile="$HERE/public/page.js" --log-level=warning
}

deploy() {
	build
	(cd "$HERE" && "$CELLD" deploy . --bucket "$BUCKET" 2>&1 | grep -E "Version ID|rror")
}

nodes() {
	local n="${1:-3}"
	for i in $(seq 0 $((n - 1))); do
		local nm=${NAMES[$i]} dir="$STATE/node-${NAMES[$i]}"
		mkdir -p "$dir"
		[[ -f "$dir/pid" ]] && kill -0 "$(cat "$dir/pid")" 2>/dev/null && continue
		CELLD_NODE="node-$nm" CELLD_WATCH="$dir/watch" RUST_LOG="${RUST_LOG:-warn}" \
			CELLD_IDLE_EVICT_S="${CELLD_IDLE_EVICT_S:-10}" \
			nohup "$CELLD" --bucket "$BUCKET" --listen "127.0.0.1:$((18101 + i))" \
			--internal-listen "127.0.0.1:$((18201 + i))" --advertise "127.0.0.1:$((18201 + i))" \
			>"$dir/celld.log" 2>&1 &
		echo $! >"$dir/pid"
		echo "node-$nm: http://127.0.0.1:$((18101 + i))/  (operator :$((18201 + i)), pid $!)"
	done
	for i in $(seq 0 $((n - 1))); do
		for _ in $(seq 1 60); do curl -fs "http://127.0.0.1:$((18101 + i))/ticket" >/dev/null && break; sleep 0.5; done
	done
}

status() {
	for i in 0 1 2 3; do
		curl -s -m 2 "http://127.0.0.1:$((18201 + i))/state" 2>/dev/null | python3 -c '
import json,sys
d=json.load(sys.stdin); l=d["node_load"]
print(sys.argv[1], "owned", d["owned_cells"], "resident", l["resident_cells"], "cpu%", l["cpu_percent_x100"]/100, "rebalanced", d["rebalanced"], "version", d["deployment"]["version"])' "node-${NAMES[$i]}" 2>/dev/null || true
	done
}

down() {
	for nm in "${NAMES[@]}"; do
		local f="$STATE/node-$nm/pid"
		[[ -f $f ]] && { kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; }
	done
	docker compose -p zone-do-spike -f "$HERE/tools/s3-compose.yaml" down -v >/dev/null 2>&1 || true
}

case "${1:-}" in
up) s3_up; deploy; nodes "${2:-3}" ;;
deploy) deploy ;;
nodes) nodes "${2:-3}" ;;
kill) kill -9 "$(cat "$STATE/node-$2/pid")"; rm -f "$STATE/node-$2/pid"; echo "killed node-$2" ;;
status) status ;;
down) down ;;
*) sed -n '2,13p' "$0"; exit 1 ;;
esac
