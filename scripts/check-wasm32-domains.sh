#!/bin/sh
# Domain crates must compile to wasm32-unknown-unknown (game-services D4):
# celld runs them as Durable Objects, so no Tokio runtime, sockets, native
# SQLite or other native-only dependency may reach a domain crate.
#
# Builds every domain crate's library for wasm32 with the dependency features
# it declares (domain crates depend on `distributed` with
# `default-features = false`), then proves the check bites: the negative
# fixture tests/fixtures/wasm32-native-domain (Tokio `net`) must fail.
#
#   scripts/check-wasm32-domains.sh             # check every domain crate
#   scripts/check-wasm32-domains.sh --list      # print the crates it checks
#
# A domain crate is any of:
#   tests/e2e-ui/crates/*-domain, examples/game/server/crawler,
#   game_services/crates/*-domain, game_services/crates/kernel
# plus the framework crates domains depend on: distributed (no features) and
# distributed_game (no features; the FactCommand derive), and distributed_game
# with `celld-zone` (the celld zone host: zones as Durable Objects), plus the
# game zones hosted that way (examples/game/server/rooms).
#
# Excluded, with the reason (a crate here never runs in a cell):
#   tests/e2e-ui/crates/blob-domain  e2e-ui blob game host: its `domain`
#     feature mounts SQL read models (e2e-readmodels, workspace distributed
#     with sqlx/tokio). Its pure board core builds for wasm via `--features wasm`.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
target=wasm32-unknown-unknown
cargo=${CARGO:-cargo}
target_dir=${CARGO_TARGET_DIR:-$root/target/wasm32-domains}

manifests() {
  echo Cargo.toml
  echo distributed_game/Cargo.toml
  for manifest in tests/e2e-ui/crates/*-domain/Cargo.toml \
    examples/game/server/crawler/Cargo.toml \
    examples/game/server/rooms/Cargo.toml \
    game_services/crates/*-domain/Cargo.toml \
    game_services/crates/kernel/Cargo.toml; do
    case "$manifest" in
      tests/e2e-ui/crates/blob-domain/Cargo.toml) continue ;;
    esac
    [ -f "$manifest" ] && echo "$manifest"
  done
  return 0
}

if [ "${1:-}" = "--list" ]; then
  manifests
  exit 0
fi

failed=0
for manifest in $(manifests); do
  printf '==> wasm32 %s\n' "$manifest"
  if [ "$manifest" = Cargo.toml ] || [ "$manifest" = distributed_game/Cargo.toml ]; then
    features="--no-default-features"
  else
    features=""
  fi
  # shellcheck disable=SC2086 # $features is intentionally word-split.
  if ! "$cargo" build --quiet --lib --target "$target" --manifest-path "$manifest" \
    --target-dir "$target_dir" $features; then
    printf 'FAIL: %s does not build for %s\n' "$manifest" "$target" >&2
    failed=1
  fi
done

# The celld zone host is wasm32 code by design.
printf '==> wasm32 distributed_game/Cargo.toml --features celld-zone\n'
if ! "$cargo" build --quiet --lib --target "$target" --manifest-path distributed_game/Cargo.toml \
  --target-dir "$target_dir" --no-default-features --features celld-zone; then
  printf 'FAIL: distributed_game with celld-zone does not build for %s\n' "$target" >&2
  failed=1
fi

fixture=tests/fixtures/wasm32-native-domain/Cargo.toml
printf '==> negative fixture %s (must fail)\n' "$fixture"
if "$cargo" build --quiet --lib --target "$target" --manifest-path "$fixture" \
  --target-dir "$target_dir" >/dev/null 2>&1; then
  printf 'FAIL: %s built for %s; the check no longer detects native-only dependencies\n' \
    "$fixture" "$target" >&2
  failed=1
else
  printf 'ok: native-only dependency rejected\n'
fi

exit "$failed"
