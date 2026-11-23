#!/usr/bin/env bats

setup_file() {
  : "${DISTRIBUTED_BIN:?set DISTRIBUTED_BIN to the compiled distributed binary}"
  [ -x "$DISTRIBUTED_BIN" ]
  DISTRIBUTED_ROOT="$(cd "$(dirname "$DISTRIBUTED_BIN")/../.." && pwd -P)"
  export DISTRIBUTED_ROOT
}

setup() {
  ROOT="$(mktemp -d "$BATS_TEST_TMPDIR/distributed-lifecycle.XXXXXX")"
  ROOT="$(cd "$ROOT" && pwd -P)"
  DEV_LOG="$ROOT/dev.log"
  SUPERVISOR_PID=""
  PROJECT=""
}

teardown() {
  if [ -n "${SUPERVISOR_PID:-}" ] && kill -0 "$SUPERVISOR_PID" 2>/dev/null; then
    kill -INT "$SUPERVISOR_PID" 2>/dev/null || true
    wait_for_exit "$SUPERVISOR_PID" 100 || kill -TERM "$SUPERVISOR_PID" 2>/dev/null || true
    wait "$SUPERVISOR_PID" 2>/dev/null || true
  fi
  # The test project's processes start descendants that ignore SIGTERM.
  for log in "$PROJECT/dev-family.log" "$PROJECT/build-family.log"; do
    if [ -n "$PROJECT" ] && [ -f "$log" ]; then
      for pid in $(cat "$log"); do
        kill -KILL "$pid" 2>/dev/null || true
      done
    fi
  done
}

@test "project build and dev are zero-config and invalid typed exports fail before UI" {
  PROJECT="$ROOT/zero-config-app"
  run "$DISTRIBUTED_BIN" scaffold zero-config-app \
    --path "$PROJECT" \
    --distributed-path "$DISTRIBUTED_ROOT" \
    --query-api \
    --store sqlite
  [ "$status" -eq 0 ]

  mkdir -p "$PROJECT/ui" "$ROOT/bin"
  printf '{"name":"zero-config-ui","private":true,"scripts":{"build":"vite build"},"dependencies":{"@hops-ops/distributed":"file:%s/js"}}\n' \
    "$DISTRIBUTED_ROOT" \
    > "$PROJECT/ui/package.json"
  ZERO_CONFIG_REAL_NPM="$(command -v npm)"
  [ -n "$ZERO_CONFIG_REAL_NPM" ]
  cat > "$ROOT/bin/npm" <<'SCRIPT'
#!/bin/sh
set -eu
if [ -f package.json ] && grep -Fq '"name": "@hops-ops/distributed"' package.json; then
  exec "$ZERO_CONFIG_REAL_NPM" "$@"
fi
printf '%s:%s\n' "$PWD" "$*" > "$ZERO_CONFIG_NPM_LOG"
if [ "$1" = "run" ] && [ "$2" = "dev" ]; then
  exec python3 -m http.server "${UI_PORT:-15180}" --bind 127.0.0.1
fi
mkdir -p node_modules/@hops-ops
[ -e node_modules/@hops-ops/distributed ] || \
  ln -s "$DISTRIBUTED_ROOT/js" node_modules/@hops-ops/distributed
mkdir -p .svelte-kit/output
SCRIPT
  chmod +x "$ROOT/bin/npm"

  run env \
    PATH="$ROOT/bin:$PATH" \
    ZERO_CONFIG_NPM_LOG="$ROOT/npm.log" \
    ZERO_CONFIG_REAL_NPM="$ZERO_CONFIG_REAL_NPM" \
    "$DISTRIBUTED_BIN" build "$PROJECT" --output json
  [ "$status" -eq 0 ]
  [[ "$output" == *'"ok":true'* ]]
  [ -x "$PROJECT/target/debug/zero-config-app" ]
  [ -f "$ROOT/npm.log" ]
  grep -F "$PROJECT/ui:run build" "$ROOT/npm.log"
  [ -f "$DISTRIBUTED_ROOT/js/dist/index.js" ]
  [ "$(find "$PROJECT/.distributed/javascript" -name '*.json' -type f | wc -l | tr -d ' ')" -eq 1 ]
  [ ! -e "$PROJECT/distributed.contracts.json" ]
  [ ! -e "$PROJECT/distributed.lifecycle.json" ]
  manifest_count="$(find "$PROJECT/.distributed/lifecycle/generations" \
    -path '*/artifacts/application-manifest.json' -type f | wc -l | tr -d ' ')"
  [ "$manifest_count" -eq 1 ]

  API_PORT="$(free_port)"
  UI_PORT_VALUE="$(free_port)"
  while [ "$UI_PORT_VALUE" = "$API_PORT" ]; do
    UI_PORT_VALUE="$(free_port)"
  done
  env \
    PATH="$ROOT/bin:$PATH" \
    ZERO_CONFIG_NPM_LOG="$ROOT/npm.log" \
    ZERO_CONFIG_REAL_NPM="$ZERO_CONFIG_REAL_NPM" \
    BIND="127.0.0.1:$API_PORT" \
    UI_HOST="127.0.0.1" \
    UI_PORT="$UI_PORT_VALUE" \
    DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY="0123456789abcdef0123456789abcdef" \
    "$DISTRIBUTED_BIN" dev "$PROJECT" > "$DEV_LOG" 2>&1 &
  SUPERVISOR_PID=$!
  wait_for_log 'lifecycle dev: ready generation=' 300
  grep -F "lifecycle dev: process api ready http://127.0.0.1:$API_PORT" "$DEV_LOG"
  grep -F "lifecycle dev: process ui ready http://127.0.0.1:$UI_PORT_VALUE" "$DEV_LOG"

  kill -INT "$SUPERVISOR_PID"
  wait_for_exit "$SUPERVISOR_PID" 200
  wait "$SUPERVISOR_PID"
  dev_status=$?
  SUPERVISOR_PID=""
  [ "$dev_status" -eq 0 ]

  # A conventionally named service is only a candidate until its typed export
  # compiles. Reject an older/incompatible checkout before rebuilding the UI.
  active_before="$(cat "$PROJECT/.distributed/lifecycle/active.json")"
  sed 's/application_manifest, //' "$PROJECT/src/lib.rs" > "$PROJECT/src/lib.rs.next"
  mv "$PROJECT/src/lib.rs.next" "$PROJECT/src/lib.rs"
  printf 'npm-must-not-run\n' > "$ROOT/npm.log"

  run env \
    PATH="$ROOT/bin:$PATH" \
    ZERO_CONFIG_NPM_LOG="$ROOT/npm.log" \
    ZERO_CONFIG_REAL_NPM="$ZERO_CONFIG_REAL_NPM" \
    "$DISTRIBUTED_BIN" build "$PROJECT"
  [ "$status" -ne 0 ]
  if [[ "$output" != *'must publicly export a zero-argument function returning `distributed::ApplicationManifest`'* ]]; then
    printf 'unexpected invalid-export diagnostic:\n%s\n' "$output" >&2
    false
  fi
  [[ "$output" != *'distributed build: UI '* ]]
  [ "$(cat "$ROOT/npm.log")" = 'npm-must-not-run' ]
  [ "$(cat "$PROJECT/.distributed/lifecycle/active.json")" = "$active_before" ]
}

@test "linked checkout skew is structured and fails before build or dev startup" {
  PROJECT="$ROOT/skewed-app"
  run "$DISTRIBUTED_BIN" scaffold skewed-app \
    --path "$PROJECT" \
    --distributed-path "$DISTRIBUTED_ROOT" \
    --query-api \
    --store sqlite
  [ "$status" -eq 0 ]

  OTHER_JS="$ROOT/other-distributed/js"
  mkdir -p "$PROJECT/ui" "$OTHER_JS"
  printf '{"name":"@hops-ops/distributed","version":"0.1.0","scripts":{"build":"tsc"},"exports":{".":"./dist/index.js"}}\n' \
    > "$OTHER_JS/package.json"
  printf '{"name":"skewed-ui","private":true,"dependencies":{"@hops-ops/distributed":"file:%s"}}\n' \
    "$OTHER_JS" \
    > "$PROJECT/ui/package.json"

  run "$DISTRIBUTED_BIN" build "$PROJECT" --output json
  [ "$status" -eq 1 ]
  [[ "$output" == *'"code":"CTL-FRAMEWORK-IDENTITY-MISMATCH"'* ]]
  [[ "$output" == *'"affected_components":["rust","cli","javascript"]'* ]]
  [[ "$output" == *'"expected"'* ]]
  [[ "$output" == *'"repair"'* ]]
  [ ! -e "$PROJECT/target/debug/skewed-app" ]
  [ ! -e "$PROJECT/ui/node_modules" ]
  [ ! -e "$PROJECT/.distributed/lifecycle/active.json" ]

  run "$DISTRIBUTED_BIN" dev "$PROJECT"
  [ "$status" -eq 1 ]
  [[ "$output" == *'incompatible Distributed framework members'* ]]
  [ ! -e "$PROJECT/target/debug/skewed-app" ]
  [ ! -e "$PROJECT/ui/node_modules" ]
  [ ! -e "$PROJECT/.distributed/lifecycle/active.json" ]
}

@test "build activates atomically and check reports drift without replacing active" {
  write_project
  ACTIVE="$PROJECT/.distributed/lifecycle/active.json"

  run project build --output json
  [ "$status" -eq 0 ]
  [[ "$output" == *'"ok":true'* ]]
  [[ "$output" == *'"executed":["application"]'* ]]
  [ -f "$ACTIVE" ]
  active_before="$(cat "$ACTIVE")"

  run project build --check --output json
  [ "$status" -eq 0 ]
  [[ "$output" == *'"drift":[]'* ]]
  [ "$(cat "$ACTIVE")" = "$active_before" ]

  printf 'renamed\n' > "$PROJECT/service/src/name.txt"
  run project build --check --output json
  [ "$status" -eq 1 ]
  [[ "$output" == *'"ok":false'* ]]
  [[ "$output" == *'"node_id":"application"'* ]]
  [ "$(cat "$ACTIVE")" = "$active_before" ]

  : > "$PROJECT/fail-manifest"
  run project build --output json
  [ "$status" -ne 0 ]
  [[ "$output" == *'injected manifest failure'* ]]
  [ "$(cat "$ACTIVE")" = "$active_before" ]
}

@test "dev reports process readiness, rebuilds selectively, and cleans descendants" {
  write_project
  project dev > "$DEV_LOG" 2>&1 &
  SUPERVISOR_PID=$!

  wait_for_log 'lifecycle dev: ready generation=' 6000
  grep -F "lifecycle dev: process api ready http://127.0.0.1:$API_PORT" "$DEV_LOG"
  grep -F "lifecycle dev: process ui ready http://127.0.0.1:$UI_PORT_VALUE" "$DEV_LOG"
  grep -Fx "api:$PROJECT" "$PROJECT/dev-environment.log"
  grep -Fx "ui:$PROJECT/ui" "$PROJECT/dev-environment.log"

  printf 'renamed\n' > "$PROJECT/service/src/name.txt"
  wait_for_log 'invalidated=application restarted=api' 6000
  [ "$(grep -c '^api:' "$PROJECT/dev-process.log")" -eq 2 ]
  [ "$(grep -c '^ui:' "$PROJECT/dev-process.log")" -eq 1 ]

  kill -INT "$SUPERVISOR_PID"
  wait_for_exit "$SUPERVISOR_PID" 600
  wait "$SUPERVISOR_PID"
  dev_status=$?
  SUPERVISOR_PID=""
  [ "$dev_status" -eq 0 ]
  grep -F 'process=api restarts=1' "$DEV_LOG"
  grep -F 'process=ui restarts=0' "$DEV_LOG"

  # Three processes ran, each with a descendant.
  [ "$(wc -w < "$PROJECT/dev-family.log" | tr -d ' ')" -eq 6 ]
  for pid in $(cat "$PROJECT/dev-family.log"); do
    ! kill -0 "$pid" 2>/dev/null
  done
}

@test "Ctrl-C cancels the initial build before any process starts" {
  write_project
  : > "$PROJECT/slow-manifest"
  project dev > "$DEV_LOG" 2>&1 &
  SUPERVISOR_PID=$!
  wait_for_file "$PROJECT/build-family.log" 6000
  # The file exists before the harness has written its record.
  while [ ! -s "$PROJECT/build-family.log" ]; do sleep 0.05; done

  kill -INT "$SUPERVISOR_PID"
  wait_for_exit "$SUPERVISOR_PID" 600
  wait "$SUPERVISOR_PID" || dev_status=$?
  dev_status="${dev_status:-0}"
  SUPERVISOR_PID=""

  [ "$dev_status" -ne 0 ]
  grep -F 'lifecycle build was canceled' "$DEV_LOG"
  [ ! -e "$PROJECT/dev-process.log" ]
  for pid in $(cat "$PROJECT/build-family.log"); do
    ! kill -0 "$pid" 2>/dev/null
  done
}

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
}

wait_for_log() {
  needle=$1
  attempts=$2
  i=0
  while [ "$i" -lt "$attempts" ]; do
    if [ -f "$DEV_LOG" ] && grep -Fq "$needle" "$DEV_LOG"; then
      return 0
    fi
    if [ -n "${SUPERVISOR_PID:-}" ] && ! kill -0 "$SUPERVISOR_PID" 2>/dev/null; then
      cat "$DEV_LOG" >&3
      return 1
    fi
    sleep 0.05
    i=$((i + 1))
  done
  cat "$DEV_LOG" >&3
  return 1
}

wait_for_file() {
  path=$1
  attempts=$2
  i=0
  while [ "$i" -lt "$attempts" ]; do
    [ -f "$path" ] && return 0
    if [ -n "${SUPERVISOR_PID:-}" ] && ! kill -0 "$SUPERVISOR_PID" 2>/dev/null; then
      cat "$DEV_LOG" >&3
      return 1
    fi
    sleep 0.05
    i=$((i + 1))
  done
  cat "$DEV_LOG" >&3
  return 1
}

wait_for_exit() {
  pid=$1
  attempts=$2
  i=0
  while [ "$i" -lt "$attempts" ]; do
    ! kill -0 "$pid" 2>/dev/null && return 0
    sleep 0.05
    i=$((i + 1))
  done
  return 1
}

# The lifecycle test project (tests/fixtures/lifecycle-project) as $PROJECT:
# a Cargo workspace whose API and UI listen on ports of their own.
write_project() {
  PROJECT="$ROOT/project"
  API_PORT="$(free_port)"
  UI_PORT_VALUE="$(free_port)"
  while [ "$UI_PORT_VALUE" = "$API_PORT" ]; do
    UI_PORT_VALUE="$(free_port)"
  done
  cp -R "$DISTRIBUTED_ROOT/distributed_cli/tests/fixtures/lifecycle-project" "$PROJECT"
  for manifest in "$PROJECT/Cargo.toml" "$PROJECT/service/Cargo.toml"; do
    sed -e "s|@DISTRIBUTED@|$DISTRIBUTED_ROOT|" \
      -e "s|@API_PORT@|$API_PORT|" \
      -e "s|@UI_PORT@|$UI_PORT_VALUE|" \
      "$manifest" > "$manifest.next"
    mv "$manifest.next" "$manifest"
  done
  # The framework's lock file resolves the project without the network.
  cp "$DISTRIBUTED_ROOT/Cargo.lock" "$PROJECT/Cargo.lock"
}

# `distributed <command> $PROJECT ...`, replacing the subshell that `run` or
# `&` started, so that `$!` is the CLI itself. The test projects of this file
# share one Cargo target directory, and their own metadata decides where they
# listen.
project() {
  command=$1
  shift
  exec env -u BIND -u UI_HOST -u UI_BIND -u UI_PORT -u UI_URL -u PUBLIC_ORIGIN \
    CARGO_TARGET_DIR="$BATS_FILE_TMPDIR/lifecycle-project-target" \
    "$DISTRIBUTED_BIN" "$command" "$PROJECT" "$@"
}
