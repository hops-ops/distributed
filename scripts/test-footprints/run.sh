#!/usr/bin/env bash
# Per-test coverage footprints: `make test-footprints` runs this after it has
# recreated the test databases and exported the service variables.
#
# Usage: scripts/test-footprints/run.sh OUT_DIR [nextest args...]
#
# Builds the workspace instrumented into target/llvm-cov-target (the normal
# target/ build stays warm), runs every selected test in a process of its own
# under nextest's `footprints` profile, and writes the outputs into OUT_DIR.
# A failing test does not fail the run; a tooling failure does.
set -euo pipefail

die() { printf 'test-footprints: %s\n' "$*" >&2; exit 2; }

[ $# -ge 1 ] || die "usage: run.sh OUT_DIR [nextest args...]"
OUT=$1
shift
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"

# ---- tools
command -v python3 >/dev/null || die "python3 not found"
cargo llvm-cov --version >/dev/null 2>&1 || die "cargo-llvm-cov not found: cargo install cargo-llvm-cov --locked"
cargo nextest --version >/dev/null 2>&1 || die "cargo-nextest not found: cargo install cargo-nextest --locked"
HOST=$(rustc -vV | sed -n 's/^host: //p')
SYSROOT=$(rustc --print sysroot)
export FOOTPRINTS_PROFDATA="$SYSROOT/lib/rustlib/$HOST/bin/llvm-profdata"
export FOOTPRINTS_LLVM_COV="$SYSROOT/lib/rustlib/$HOST/bin/llvm-cov"
[ -x "$FOOTPRINTS_PROFDATA" ] && [ -x "$FOOTPRINTS_LLVM_COV" ] \
	|| die "llvm-tools not installed: rustup component add llvm-tools-preview"

# ---- the runner's interpreter: not SIP-protected on macOS (see runner.sh)
usable_bash() {
	[ -x "$1" ] || return 1
	[ "$(uname -s)" = Darwin ] || return 0
	case "$1" in
		/bin/* | /usr/bin/* | /sbin/* | /usr/sbin/* | /System/*) return 1 ;;
	esac
}
find_bash() {
	local candidate
	for candidate in ${FOOTPRINTS_BASH:-$(type -ap bash) /opt/homebrew/bin/bash /usr/local/bin/bash}; do
		usable_bash "$candidate" && { printf '%s\n' "$candidate"; return; }
	done
}
RUNNER_BASH=$(find_bash || true)
[ -n "$RUNNER_BASH" ] || die "no usable bash for the test runner${FOOTPRINTS_BASH:+ (FOOTPRINTS_BASH=$FOOTPRINTS_BASH)}. \
On macOS the system shells are SIP-protected and drop DYLD_* variables, which \
proc-macro test binaries need: brew install bash, or set FOOTPRINTS_BASH to a bash outside /bin and /usr/bin."
case "$RUNNER_BASH $HERE" in
	*" "*" "*) die "paths with spaces are not supported in the runner command: $RUNNER_BASH $HERE" ;;
esac

# ---- output directory: only ever replace one this tool wrote
if [ -e "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ] && [ ! -e "$OUT/.test-footprints" ]; then
	die "$OUT exists and was not written by this tool; refusing to replace it"
fi
rm -rf "$OUT"
mkdir -p "$OUT/work/profiles"
touch "$OUT/.test-footprints"
OUT=$(cd "$OUT" && pwd)
WORK="$OUT/work"
export FOOTPRINTS_PROFILES="$WORK/profiles"

# ---- selection: the whole workspace unless the caller names packages
select=(--workspace)
for arg in "$@"; do
	case "$arg" in
		-p | -p* | --package | --package=* | --workspace) select=() ;;
	esac
done
args=(${select[@]+"${select[@]}"} --all-features --all-targets "$@")

{
	echo "commit: $(git rev-parse HEAD 2>/dev/null || echo unknown)$( [ -n "$(git status --porcelain --untracked-files=no 2>/dev/null)" ] && echo ' (with uncommitted changes)')"
	echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "nextest args: ${args[*]}"
	echo "load at start: $(uptime | sed 's/.*load average[s]*: //')"
} > "$WORK/meta.txt"

# ---- instrumented environment, in its own target directory
export FOOTPRINTS_SAVED_RUSTC_WRAPPER="${RUSTC_WRAPPER:-}"
export FOOTPRINTS_SAVED_CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-}"
export CARGO_TARGET_DIR="$ROOT/target/llvm-cov-target"
eval "$(cargo llvm-cov show-env --sh 2>/dev/null)"
runner_var="CARGO_TARGET_$(printf '%s' "$HOST" | tr 'a-z.-' 'A-Z__')_RUNNER"
export "$runner_var=$RUNNER_BASH $HERE/runner.sh"
export FOOTPRINTS_RUNNER_VAR="$runner_var"

started=$(date +%s)
cargo nextest list "${args[@]}" --message-format json > "$WORK/list.json" \
	|| die "cargo nextest list failed (build error?)"
built=$(date +%s)

rm -f "$ROOT/target/nextest/footprints/junit.xml"
set +e
cargo nextest run --profile footprints "${args[@]}"
status=$?
set -e
# 0: all passed; 100: some tests failed (listed in summary.txt); 4: no test
# matched the filter. Anything else is the tooling's failure.
case $status in
	0 | 100 | 4) ;;
	*) die "cargo nextest run exited $status" ;;
esac
# nextest keeps its store in the workspace target/, whatever CARGO_TARGET_DIR says.
cp "$ROOT/target/nextest/footprints/junit.xml" "$WORK/junit.xml" || die "no JUnit report from nextest"
ran=$(date +%s)

python3 -B "$HERE/export.py" "$WORK"
rm -rf "$WORK/profiles"
python3 -B "$HERE/analyze.py" "$WORK" "$OUT"
finished=$(date +%s)

{
	echo
	echo "== WALL TIME"
	echo "seconds: build $((built - started)), tests $((ran - built)), export and analysis $((finished - ran)), total $((finished - started))"
	echo "load at end: $(uptime | sed 's/.*load average[s]*: //')"
} >> "$OUT/summary.txt"

echo "test-footprints: wrote $OUT (see summary.txt)"
