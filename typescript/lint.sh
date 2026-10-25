#!/usr/bin/env bash
# Lint & format the library with Biome. (Biome, not ESLint: typescript-eslint hard-blocks
# TypeScript 7 — https://github.com/typescript-eslint/typescript-eslint/issues/10940 — and
# Biome has no dependency on the `typescript` package, so TS 7 is a non-issue. tsc still does
# the type-checking via `npm run typecheck`.)
#
#   ./lint.sh          # gate: biome lint, errors only (what CI blocks on)
#   ./lint.sh -f       # auto-fix lint findings + reformat
#
# Mirrors the lint.sh in the sibling languages. Needs dev deps (`npm install`).
# Config: biome.json. Also available as npm scripts: lint / lint:fix / format / format:check.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "-f" || "${1:-}" == "--fix" ]]; then
  npm run lint:fix
  npm run format
  exit 0
fi

# Gate: Biome's recommended rules (error-severity diagnostics fail; warnings are shown).
npm run lint
# Report-only: formatting (never fail the gate on style).
echo "--- biome format (report-only) ---"
npm run format:check || true
