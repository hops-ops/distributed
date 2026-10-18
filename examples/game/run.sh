#!/usr/bin/env bash
# distributed_game demos — one command per demo (dev only).
#
#   ./run.sh arena          build what is needed, start the arena server
#                           (dev routes on) + Vite, print the URL
#   ./run.sh smoke          every demo's smoke tests (Rust bots + headless JS)
#   ./run.sh list           demos this script knows
#
# Env: GAME_DEMOS_PORT (server, default 2567), GAME_DEMOS_CLIENT_PORT (Vite,
# default 5173), DG_NET_LATENCY_MS / DG_NET_JITTER_MS / DG_NET_LOSS (initial
# server NetSim; the HUD sliders change it live).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
SERVER="$HERE/server"
CLIENT="$HERE/client"
BIN="$SERVER/target/debug/game-demos"
DEMOS=(arena gallery freeze)

log() { printf '\033[1;36m[demos]\033[0m %s\n' "$*"; }

ensure_sdk() {
	# The client depends on the local SDK build (file:../../../js).
	if [[ ! -d "$REPO/js/node_modules" ]]; then
		log "installing js SDK dependencies"
		(cd "$REPO/js" && npm ci --no-audit --no-fund)
	fi
	local marker="$REPO/js/dist/game/index.js"
	if [[ ! -f "$marker" ]] || [[ -n "$(find "$REPO/js/src" -newer "$marker" -name '*.ts' -print -quit)" ]]; then
		log "building js SDK"
		(cd "$REPO/js" && npm run build >/dev/null)
	fi
}

ensure_client() {
	local stamp="$CLIENT/node_modules/.package-lock.json"
	if [[ ! -f "$stamp" ]] || [[ "$CLIENT/package-lock.json" -nt "$stamp" ]]; then
		log "installing client dependencies (npm ci)"
		(cd "$CLIENT" && npm ci --no-audit --no-fund)
	fi
}

ensure_server() {
	log "building game-demos server"
	cargo build --quiet --manifest-path "$SERVER/Cargo.toml"
}

run_demo() {
	local demo="$1"
	local port="${GAME_DEMOS_PORT:-2567}"
	local client_port="${GAME_DEMOS_CLIENT_PORT:-5173}"
	ensure_sdk
	ensure_client
	ensure_server

	local log_file
	log_file="$(mktemp -t game-demos.XXXXXX)"
	"$BIN" "$demo" --dev --port "$port" >"$log_file" 2>&1 &
	local server_pid=$!
	trap 'kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; rm -f "$log_file"' EXIT INT TERM
	for _ in $(seq 1 100); do
		grep -q '^LISTENING' "$log_file" && break
		if ! kill -0 "$server_pid" 2>/dev/null; then
			cat "$log_file"
			echo "game-demos $demo failed to start" >&2
			exit 1
		fi
		sleep 0.1
	done
	sed -n '2p' "$log_file"
	echo
	log "open  http://localhost:$client_port/$demo/   (server :$port, dev routes ON — local only)"
	echo
	cd "$CLIENT"
	GAME_DEMOS_SERVER="http://127.0.0.1:$port" GAME_DEMOS_CLIENT_PORT="$client_port" npx vite
}

smoke() {
	ensure_sdk
	ensure_client
	log "server: fmt, clippy, tests (bots smoke + movement fixture)"
	cargo fmt --manifest-path "$SERVER/Cargo.toml" --check
	cargo clippy --quiet --manifest-path "$SERVER/Cargo.toml" --all-targets -- -D warnings
	cargo test --quiet --manifest-path "$SERVER/Cargo.toml"
	cargo build --quiet --manifest-path "$SERVER/Cargo.toml"
	log "client: type-check, build, tests (movement parity + headless SDK)"
	(cd "$CLIENT" && npm run --silent check && npx vite build --logLevel warn && GAME_DEMOS_BIN="$BIN" npm test --silent)
	log "smoke OK"
}

case "${1:-}" in
	smoke) smoke ;;
	list) printf '%s\n' "${DEMOS[@]}" ;;
	"" | -h | --help | help) sed -n '2,13p' "$0" ;;
	*)
		if [[ " ${DEMOS[*]} " == *" $1 "* ]]; then
			run_demo "$1"
		else
			echo "unknown demo '$1' (known: ${DEMOS[*]})" >&2
			exit 2
		fi
		;;
esac
