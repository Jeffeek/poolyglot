#!/usr/bin/env bash
# Run the connpool test suite under coverage and enforce the 100% gate.
#
#   ./run_tests.sh          # pass/fail + coverage summary (gate: 100% statements)
#   ./run_tests.sh -m       # ...also list the uncovered statements
#
# Mirrors python/run_tests.sh, csharp/run_tests.sh and java/run_tests.sh. Needs Go 1.21+.
# Go's cover tool measures *statement* coverage (its nearest equivalent to the line+branch
# gate the other languages use); the gate below fails below 100%.
set -uo pipefail
cd "$(dirname "$0")"

show_missing=false
[[ "${1:-}" == "-m" ]] && show_missing=true

profile=coverage.out
if go test -covermode=set -coverprofile="$profile" ./...; then
  status=0
else
  status=$?
fi

if [[ ! -f "$profile" ]]; then
  echo "no coverage profile produced"
  exit "${status:-1}"
fi

echo
echo "=== coverage ==="
total="$(go tool cover -func="$profile" | awk '/^total:/ {print $NF}')"
echo "  statements: ${total}"

# Fail the gate if coverage is below 100%.
if [[ "$total" != "100.0%" ]]; then
  echo "  gate: FAILED (need 100.0%)"
  status=1
else
  echo "  gate: ok"
fi

if $show_missing; then
  echo
  echo "=== uncovered statements ==="
  # coverage.out lines: "file:startLine.col,endLine.col numStmts count"; count 0 = uncovered.
  awk 'NR>1 && $NF==0 {
         split($1, a, ":"); split(a[2], r, ",");
         split(r[1], s, "."); split(r[2], e, ".");
         print "  " a[1] ":" s[1] "-" e[1]
       }' "$profile" | sort -u
  if ! awk 'NR>1 && $NF==0 {found=1} END {exit !found}' "$profile"; then
    echo "  none - 100% statements"
  fi
fi

exit "$status"
