#!/usr/bin/env bash
# Run the connpool test suite under coverage and print a compact report.
#
#   ./run_tests.sh          # pass/fail + per-file & overall coverage
#   ./run_tests.sh -m       # also list the uncovered line numbers
#
# On success only the summary is shown; on failure the full test output is
# dumped so you can see what broke. Needs coverage.py (`pip install coverage`).
set -euo pipefail

cd "$(dirname "$0")"
export PYTHONPATH=src

PY=python
command -v python >/dev/null 2>&1 || PY=python3

if ! "$PY" -m coverage --version >/dev/null 2>&1; then
    echo "coverage.py not found. Install it with: $PY -m pip install coverage" >&2
    exit 1
fi

report_args=(--precision=1 --sort=cover)
[[ "${1:-}" == "-m" ]] && report_args+=(--show-missing)

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

if "$PY" -m coverage run --source=src -m unittest discover -s tests -t . >"$log" 2>&1; then
    grep -E "^Ran [0-9]+ tests" "$log" || true
    echo "status: PASS"
    echo
    "$PY" -m coverage report "${report_args[@]}"
else
    echo "status: FAIL" >&2
    echo >&2
    cat "$log" >&2
    exit 1
fi
