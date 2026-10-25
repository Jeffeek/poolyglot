#!/usr/bin/env bash
# Lint & format with SpotBugs (bug finder) + Spotless (google-java-format).
#
#   ./lint.sh          # gate: spotbugs:check at High confidence (what CI blocks on)
#   ./lint.sh -f       # auto-format: spotless:apply
#   ./lint.sh -m       # also list Medium-confidence findings (review, not gated)
#
# Mirrors the lint.sh in the sibling languages. Needs Maven + a JDK 17. Plugin config in pom.xml.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "-f" || "${1:-}" == "--fix" ]]; then
  mvn -q -B spotless:apply
  exit 0
fi

# Gate: high-confidence bug patterns. CI fails on these.
mvn -q -B -DskipTests compile spotbugs:check
# Report-only: formatting + lower-confidence findings (never fail the gate).
echo "--- spotless (report-only) ---"
mvn -q -B spotless:check || true
if [[ "${1:-}" == "-m" ]]; then
  echo "--- spotbugs Medium findings (review-only) ---"
  mvn -q -B -DskipTests spotbugs:check -Dspotbugs.threshold=Medium || true
fi
