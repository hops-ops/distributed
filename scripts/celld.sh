#!/usr/bin/env bash
# The one celld version this repository is tested against lives in
# `.celld-version` at the repository root. Every Makefile, run.sh and CI job
# reads it through this script.
#
#   scripts/celld.sh version        print the pinned version (e.g. 0.6.2)
#   scripts/celld.sh check [BIN]    verify BIN (default: $CELLD_BIN, else
#                                   `celld` on PATH) is the pinned version or
#                                   newer; print its absolute path
#   scripts/celld.sh install        install exactly the pinned version into
#                                   ~/.local/bin (CI; overwrites that binary)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PINNED="$(tr -d '[:space:]' < "$ROOT/.celld-version")"
INSTALL_HINT="curl -fsSL https://celld.dev/install.sh | CELLD_VERSION=v$PINNED sh"

case "${1:-}" in
version)
	echo "$PINNED"
	;;
check)
	bin="${2:-${CELLD_BIN:-}}"
	[[ -n "$bin" ]] || bin="$(command -v celld || true)"
	if [[ -z "$bin" ]] || ! command -v "$bin" >/dev/null 2>&1; then
		echo "celld not found: set CELLD_BIN to a celld $PINNED binary" >&2
		echo "  install: $INSTALL_HINT" >&2
		echo "  (or install elsewhere and point CELLD_BIN at it)" >&2
		exit 1
	fi
	bin="$(command -v "$bin")"
	version="$("$bin" --version 2>/dev/null | sed -n 's/^celld \([0-9][0-9.]*\).*/\1/p' | head -1)"
	if [[ -z "$version" ]] || [[ "$(printf '%s\n%s\n' "$PINNED" "$version" | sort -V | head -1)" != "$PINNED" ]]; then
		echo "$bin is celld ${version:-(unknown version)}; this repository needs celld $PINNED" >&2
		echo "  upgrade: $INSTALL_HINT" >&2
		echo "  or keep the old binary and set CELLD_BIN=/path/to/celld-$PINNED" >&2
		exit 1
	fi
	echo "$bin"
	;;
install)
	curl -fsSL https://celld.dev/install.sh | CELLD_VERSION="v$PINNED" sh
	"$HOME/.local/bin/celld" --version
	;;
*)
	sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
	exit 2
	;;
esac
