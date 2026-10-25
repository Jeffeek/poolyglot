#!/usr/bin/env bash
# Lint & format ConnPool with the Roslyn analyzers + dotnet format.
#
#   ./lint.sh          # gate: build with analyzers as errors (what CI blocks on)
#   ./lint.sh -f       # auto-fix formatting + fixable style (dotnet format)
#
# Mirrors the lint.sh in the sibling languages. Needs the .NET SDK (see global.json).
# Analyzer rule calibration lives in .editorconfig.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "-f" || "${1:-}" == "--fix" ]]; then
  dotnet format ConnPool.sln
  exit 0
fi

# Gate: every analyzer warning is an error.
dotnet build src/ConnPool/ConnPool.csproj -c Release -warnaserror
# Report-only: formatting differences (never fail the gate on style).
echo "--- format (report-only) ---"
dotnet format ConnPool.sln --verify-no-changes || true
