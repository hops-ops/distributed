#!/usr/bin/env bash
# Run a `cargo test` command and fail unless exactly <count> tests passed,
# summed over every `test result:` line it prints.
#
#   scripts/expect-tests-passed.sh <count> cargo test ... -- --ignored
#
# A live test is `#[ignore = "live: ..."]` and runs only under `--ignored`,
# where a name filter that matches nothing is green with `0 passed`. The jobs
# that own live tests run them through this script so a typo in a filter, a
# renamed test or a lost `#[ignore]` fails the job instead. Adding a live test
# to a job means raising its count.
set -euo pipefail

if [ "$#" -lt 2 ] || ! [[ "$1" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 <count> <command>..." >&2
  exit 2
fi
expected=$1
shift

log=$(mktemp)
trap 'rm -f "$log"' EXIT
"$@" 2>&1 | tee "$log"

passed=$(sed -n 's/^test result: [a-zA-Z]*\. \([0-9][0-9]*\) passed;.*/\1/p' "$log" |
  awk '{ total += $1 } END { print total + 0 }')
if [ "$passed" != "$expected" ]; then
  echo "expected $expected passed test(s), got $passed: $*" >&2
  exit 1
fi
echo "expect-tests-passed: $passed passed, as expected"
