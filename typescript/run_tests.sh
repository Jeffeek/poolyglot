#!/usr/bin/env bash
# Run the connpool test suite under coverage and enforce the 100% gate.
#
#   ./run_tests.sh          # pass/fail + coverage summary (gate: 100% line + branch)
#   ./run_tests.sh -m       # ...also list the uncovered lines/branches per file
#
# Mirrors python/run_tests.sh, csharp/run_tests.sh, java/run_tests.sh and go/run_tests.sh.
# Needs Node 22+ and the dev deps (`npm ci`): tests run on the stdlib `node:test` runner via
# `tsx`, under c8 coverage. The gate (100% line + branch, enforced in .c8rc.json) fails below 100%.
set -uo pipefail
cd "$(dirname "$0")"

reporter=text-summary          # compact pass/fail + totals
[[ "${1:-}" == "-m" ]] && reporter=text   # per-file table with uncovered line #s

# Discover the test files (node:test spawns a subprocess per file; c8 aggregates via
# NODE_V8_COVERAGE, so every subprocess's coverage rolls up into the one gate).
mapfile -t specs < <(find test -name '*.test.ts' | sort)
if [[ ${#specs[@]} -eq 0 ]]; then
  echo "no test files found under test/" >&2
  exit 1
fi

# Always also emit coverage/lcov.info for Codecov (CI uploads it); the gate + thresholds come
# from .c8rc.json. The on-screen report is $reporter (summary, or the per-file table with -m).
exec npx c8 --reporter="$reporter" --reporter=lcovonly node --import tsx --test "${specs[@]}"
