#!/usr/bin/env bash
# Lint & format the connpool package with golangci-lint + gofmt.
#
#   ./lint.sh          # gate: golangci-lint bug / dead-code linters (what CI blocks on)
#   ./lint.sh -f       # auto-fix: gofmt -w + golangci-lint --fix
#
# Mirrors the lint.sh in the sibling languages. Needs golangci-lint v2
# (https://golangci-lint.run/docs/welcome/install/). Config: .golangci.yml.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "-f" || "${1:-}" == "--fix" ]]; then
  gofmt -w .
  golangci-lint run --fix || true
  exit 0
fi

# Gate: govet, staticcheck (SA), ineffassign, unused, errcheck.
golangci-lint run
# Report-only: formatting (never fail the gate on style).
echo "--- gofmt (report-only) ---"
gofmt -l . || true
