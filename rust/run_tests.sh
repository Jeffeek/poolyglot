#!/usr/bin/env bash
# Run the connpool test suite under coverage and enforce the 100% line gate.
#
#   ./run_tests.sh          # pass/fail + coverage summary (gate: 100% lines)
#   ./run_tests.sh -m       # ...also list the uncovered lines
#
# Mirrors python/run_tests.sh, csharp/run_tests.sh, java/run_tests.sh and go/run_tests.sh.
# Needs a Rust toolchain (edition 2024 => 1.85+), the llvm-tools-preview component, and
# cargo-llvm-cov:
#   rustup component add llvm-tools-preview
#   cargo install cargo-llvm-cov
#
# The suite is run ONCE under instrumentation (--all-features, so the tokio-gated async pool is
# covered); every report below is generated from that cached profile (fast, no re-run). The gate
# is 100% *line* coverage measured from the LCOV output — the same metric Codecov reports — with
# the test-only support modules excluded. (LLVM region/branch coverage is not gated: `async fn`
# state-machine codegen leaves a few region artifacts that aren't real untested paths.)
set -uo pipefail
cd "$(dirname "$0")"

show_missing=false
[[ "${1:-}" == "-m" ]] && show_missing=true

ignore='test_support'
lcov="lcov.info"

# 1. run the instrumented tests once.
if ! cargo llvm-cov --all-features --no-report; then
  echo "tests failed"
  exit 1
fi

# 2. human-readable per-file summary (from cached profile).
echo
cargo llvm-cov report --ignore-filename-regex "$ignore" --summary-only

# 3. LCOV for the gate (and for Codecov in CI) (from cached profile).
cargo llvm-cov report --ignore-filename-regex "$ignore" --lcov --output-path "$lcov" >/dev/null

# 4. gate on 100% line coverage: zero LCOV DA entries with an execution count of 0.
read -r total uncovered < <(awk '/^DA:/{t++; if($0 ~ /,0$/) u++} END{print t+0, u+0}' "$lcov")
pct=$(awk -v t="$total" -v u="$uncovered" 'BEGIN{ printf (t>0)? "%.2f" : "0", (t-u)/t*100 }')

echo
echo "=== coverage gate ==="
echo "  lines: ${pct}%  (${uncovered} uncovered of ${total}; gate: 100%)"

if $show_missing && [[ "$uncovered" -gt 0 ]]; then
  echo
  echo "=== uncovered lines ==="
  awk '/^SF:/{f=$0; sub("SF:","",f)} /^DA:/{if($0 ~ /,0$/){split($0,a,","); split(a[1],b,":"); print "  " f ":" b[2]+0}}' "$lcov"
fi

if [[ "$uncovered" -gt 0 ]]; then
  echo "  gate: FAILED"
  exit 1
fi
echo "  gate: ok (100% lines)"
exit 0
