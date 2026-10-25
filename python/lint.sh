#!/usr/bin/env bash
# Lint & format the connpool sources with ruff.
#
#   ./lint.sh          # gate: high-signal bug / dead-code rules (what CI blocks on)
#   ./lint.sh -f       # auto-fix lint findings + reformat (ruff --fix + ruff format)
#
# Mirrors the lint.sh in the sibling languages. Needs ruff (`pip install -e ".[dev]"`).
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "-f" || "${1:-}" == "--fix" ]]; then
  ruff check --fix src tests
  ruff format src tests
  exit 0
fi

# Gate: pyflakes (F = unused/undefined) + bugbear (B = likely bugs). CI fails on these.
ruff check --select F,B src tests
# Report-only: the full style set + formatting (never fail the gate on style).
echo "--- style / format (report-only) ---"
ruff check src tests || true
ruff format --check src tests || true
