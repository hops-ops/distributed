# nextest target runner for `make test-footprints`; run.sh installs it as
# CARGO_TARGET_<TRIPLE>_RUNNER="<bash> <this file>". No shebang on purpose:
# on macOS a SIP-protected interpreter (/bin/sh, /bin/bash, /usr/bin/env)
# strips DYLD_* from the environment, so run.sh picks a bash outside the
# system paths and names it in the runner command.
#
# Each test process writes its raw profiles into a directory of its own
# under $FOOTPRINTS_PROFILES, merged into t.profdata when the test exits.
# Child processes the test spawns (the CLI binary) inherit LLVM_PROFILE_FILE
# and land in the same directory.

if [ -z "${NEXTEST_TEST_NAME:-}" ]; then
	# `--list` and other non-test invocations: nothing to measure.
	exec "$@"
fi

dir=$(mktemp -d "$FOOTPRINTS_PROFILES/t.XXXXXXXX") || exit 70
printf '%s\n%s\n%s\n' "$NEXTEST_BINARY_ID" "$NEXTEST_TEST_NAME" "$1" > "$dir/test"
export LLVM_PROFILE_FILE="$dir/%p-%18m.profraw"

# The instrumentation build variables and this runner are for the outer
# cargo only. Nested builds a test runs (trybuild, rustc_fail, CLI fixture
# projects) build and run as they do under `make test-local`, uninstrumented
# and in their usual target directories.
unset "$FOOTPRINTS_RUNNER_VAR" RUSTC_WRAPPER CARGO_TARGET_DIR CARGO_LLVM_COV CARGO_LLVM_COV_SHOW_ENV \
	CARGO_LLVM_COV_TARGET_DIR CARGO_LLVM_COV_BUILD_DIR \
	__CARGO_LLVM_COV_RUSTC_WRAPPER __CARGO_LLVM_COV_RUSTC_WRAPPER_RUSTFLAGS \
	__CARGO_LLVM_COV_RUSTC_WRAPPER_CRATE_NAMES
if [ -n "${FOOTPRINTS_SAVED_RUSTC_WRAPPER:-}" ]; then
	export RUSTC_WRAPPER="$FOOTPRINTS_SAVED_RUSTC_WRAPPER"
fi
if [ -n "${FOOTPRINTS_SAVED_CARGO_TARGET_DIR:-}" ]; then
	export CARGO_TARGET_DIR="$FOOTPRINTS_SAVED_CARGO_TARGET_DIR"
fi

"$@"
rc=$?

shopt -s nullglob
raw=("$dir"/*.profraw)
if [ ${#raw[@]} -gt 0 ]; then
	if "$FOOTPRINTS_PROFDATA" merge -sparse "${raw[@]}" -o "$dir/t.profdata" 2>"$dir/merge.err"; then
		rm -f "${raw[@]}" "$dir/merge.err"
	fi
fi
exit $rc
