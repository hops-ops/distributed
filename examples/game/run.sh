#!/usr/bin/env bash
# distributed_game demos — one command per demo (dev only).
#
#   ./run.sh arena          build what is needed, start the arena server
#                           (dev routes on) + Vite, print the URL
#   ./run.sh mobpit         same for Mob Pit; `leaderboard` = same server, board page
#   ./run.sh grid|ripple    same with TWO hosts (A on GAME_DEMOS_PORT, B on
#                           the next port), one process each
#   ./run.sh gen            regenerate client/src/generated/<surface>/ from the
#                           Rust surfaces (`distributed game-manifest` +
#                           `distributed game-client`) and compile each
#                           surface's `#[predict]` step and `#[game_pure]`
#                           functions to <surface>/wasm/<surface>.wasm
#                           (`distributed game-wasm`, gitignored, stamped so
#                           an unchanged module is not rebuilt); `gen --check`
#                           verifies the TypeScript (and still builds WASM)
#   ./run.sh smoke          every demo's smoke tests (Rust bots + headless JS,
#                           generated clients checked)
#   ./run.sh list           demos this script knows
#
# Env: GAME_DEMOS_PORT (server, default 2567), GAME_DEMOS_CLIENT_PORT (Vite,
# default 5173), DG_NET_LATENCY_MS / DG_NET_JITTER_MS / DG_NET_LOSS (initial
# server NetSim; the HUD sliders change it live), MOBPIT_DATA_DIR (Mob Pit /
# Leaderboard cells, default examples/game/.data/mobpit), REDIS_URL (Redis
# leaderboard instead of in-memory).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
SERVER="$HERE/server"
CLIENT="$HERE/client"
BIN="$SERVER/target/debug/game-demos"
DEMOS=(arena mobpit leaderboard gallery freeze grid ripple)
export MOBPIT_DATA_DIR="${MOBPIT_DATA_DIR:-$HERE/.data/mobpit}"
# Multi-host demos: `game-demos <demo> --host-id A|B --peer …`.
CLUSTER_DEMOS=(grid ripple)
# Game surfaces with a generated client (`game_demos::<surface>::game_surface`);
# the leaderboard page uses Mob Pit's.
SURFACES=(arena mobpit gallery freeze grid ripple)
CLI="$REPO/target/debug/distributed"

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

ensure_cli() {
	log "building distributed CLI"
	cargo build --quiet --manifest-path "$REPO/Cargo.toml" -p distributed_cli --bin distributed
}

# gen [--check]: GameSurface JSON from each Rust surface export → typed
# TypeScript in client/src/generated/<surface>/ (surface.json included, so a
# changed Rust definition shows up as drift), then the surface's predicted
# step and pure functions → <surface>/wasm/<surface>.wasm. `--check` only
# applies to the checked-in TypeScript; the WASM is a build output and is
# (re)built.
gen() {
	local check="${1:-}"
	ensure_cli
	local tmp status=0
	tmp="$(mktemp -d "${TMPDIR:-/tmp}/game-surfaces.XXXXXX")"
	for surface in "${SURFACES[@]}"; do
		"$CLI" game-manifest --manifest-path "$SERVER/Cargo.toml" \
			--entrypoint "game_demos::$surface::game_surface" --out "$tmp/$surface.json" &&
			"$CLI" game-client --manifest "$tmp/$surface.json" \
				--out "$CLIENT/src/generated/$surface" $check &&
			wasm_for "$tmp/$surface.json" "$surface" ||
			status=1
	done
	rm -rf "$tmp"
	return "$status"
}

# wasm_for <surface.json> <surface>: compile the surface's `#[predict]` step
# and `#[game_pure]` functions (if it declares any) next to its generated
# client.
wasm_for() {
	grep -q -e '"predict"' -e '"pures"' "$1" || return 0
	"$CLI" game-wasm --manifest "$1" --manifest-path "$SERVER/Cargo.toml" \
		--out "$CLIENT/src/generated/$2" >/dev/null
}

# ensure_wasm: build every module the pages load (no TypeScript generation;
# the stamps make this quick when nothing changed).
ensure_wasm() {
	ensure_cli
	log "compiling predicted steps and pure functions to WebAssembly"
	local tmp status=0
	tmp="$(mktemp -d "${TMPDIR:-/tmp}/game-surfaces.XXXXXX")"
	for surface in "${SURFACES[@]}"; do
		"$CLI" game-manifest --manifest-path "$SERVER/Cargo.toml" \
			--entrypoint "game_demos::$surface::game_surface" --out "$tmp/$surface.json" &&
			wasm_for "$tmp/$surface.json" "$surface" ||
			status=1
	done
	rm -rf "$tmp"
	return "$status"
}

run_demo() {
	local demo="$1"
	local port="${GAME_DEMOS_PORT:-2567}"
	local client_port="${GAME_DEMOS_CLIENT_PORT:-5173}"
	ensure_sdk
	ensure_client
	ensure_server
	ensure_wasm

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

wait_listening() {
	local log_file="$1" pid="$2" what="$3"
	for _ in $(seq 1 100); do
		grep -q '^LISTENING' "$log_file" && return 0
		if ! kill -0 "$pid" 2>/dev/null; then
			cat "$log_file"
			echo "$what failed to start" >&2
			exit 1
		fi
		sleep 0.1
	done
}

run_cluster() {
	local demo="$1"
	local port_a="${GAME_DEMOS_PORT:-2567}"
	local port_b=$((port_a + 1))
	local client_port="${GAME_DEMOS_CLIENT_PORT:-5173}"
	ensure_sdk
	ensure_client
	ensure_server
	ensure_wasm

	local dir secret
	dir="$(mktemp -d "${TMPDIR:-/tmp}/game-demos-$demo.XXXXXX")"
	# Both hosts must share the ticket secret (and derive the admin secret).
	secret="${DISTRIBUTED_GAME_SECRET:-$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')}"
	DISTRIBUTED_GAME_SECRET="$secret" "$BIN" "$demo" --dev --port "$port_a" --host-id A \
		--peer "B=127.0.0.1:$port_b" --state-dir "$dir" >"$dir/A.log" 2>&1 &
	local pid_a=$!
	DISTRIBUTED_GAME_SECRET="$secret" "$BIN" "$demo" --dev --port "$port_b" --host-id B \
		--peer "A=127.0.0.1:$port_a" --state-dir "$dir" >"$dir/B.log" 2>&1 &
	local pid_b=$!
	trap 'kill "$pid_a" "$pid_b" 2>/dev/null || true; wait "$pid_a" "$pid_b" 2>/dev/null || true; rm -rf "$dir"' EXIT INT TERM
	wait_listening "$dir/A.log" "$pid_a" "game-demos $demo host A"
	wait_listening "$dir/B.log" "$pid_b" "game-demos $demo host B"
	sed -n '2p' "$dir/A.log"
	sed -n '2p' "$dir/B.log"
	echo
	log "open  http://localhost:$client_port/$demo/   (hosts A :$port_a, B :$port_b, dev routes ON — local only)"
	log "snapshots in $dir (removed on exit)"
	echo
	cd "$CLIENT"
	GAME_DEMOS_SERVER="http://127.0.0.1:$port_a" GAME_DEMOS_CLIENT_PORT="$client_port" npx vite
}

smoke() {
	ensure_sdk
	ensure_client
	log "generated clients are current; predicted steps and pure functions compiled to WASM"
	gen --check
	log "server: fmt, clippy, tests (bots smoke)"
	cargo fmt --manifest-path "$SERVER/Cargo.toml" --check -p game_demos -p game_sim
	cargo clippy --quiet --manifest-path "$SERVER/Cargo.toml" --workspace --all-targets -- -D warnings
	cargo test --quiet --manifest-path "$SERVER/Cargo.toml" --workspace
	cargo build --quiet --manifest-path "$SERVER/Cargo.toml"
	log "client: type-check, build, tests (WASM steps + pure functions exact match, headless SDK)"
	(cd "$CLIENT" && npm run --silent check && npx vite build --logLevel warn && GAME_DEMOS_BIN="$BIN" npm test --silent)
	log "smoke OK"
}

case "${1:-}" in
	smoke) smoke ;;
	gen) gen "${2:-}" ;;
	list) printf '%s\n' "${DEMOS[@]}" ;;
	"" | -h | --help | help) sed -n '2,24p' "$0" ;;
	*)
		if [[ " ${CLUSTER_DEMOS[*]} " == *" $1 "* ]]; then
			run_cluster "$1"
		elif [[ " ${DEMOS[*]} " == *" $1 "* ]]; then
			run_demo "$1"
		else
			echo "unknown demo '$1' (known: ${DEMOS[*]})" >&2
			exit 2
		fi
		;;
esac
