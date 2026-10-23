#!/usr/bin/env bash
# Run the ConnPool test suite under coverage and enforce the 100% gate.
#
#   ./run_tests.sh          # pass/fail + coverage summary (gate: 100% line & branch)
#   ./run_tests.sh -m       # ...also list the uncovered lines / partial branches
#
# Mirrors python/run_tests.sh. Needs the .NET SDK (see global.json) - coverlet is a dev
# dependency of the test project, so no extra install is required. The -m report parses
# coverlet's OpenCover output (python3, already present in this polyglot repo).
set -euo pipefail
cd "$(dirname "$0")"

show_missing=false
[[ "${1:-}" == "-m" ]] && show_missing=true

fmt=cobertura
out=./coverage/coverage.cobertura.xml
if $show_missing; then
  fmt=opencover
  out=./coverage/coverage.opencover.xml
fi

args=(
  tests/ConnPool.Tests/ConnPool.Tests.csproj
  -p:CollectCoverage=true
  -p:CoverletOutputFormat=$fmt
  -p:CoverletOutput=$out
  -p:Threshold=100
  -p:ThresholdType=line%2cbranch
  -p:ThresholdStat=total
)

dotnet test "${args[@]}"

if $show_missing; then
  echo
  echo "=== uncovered lines / partial branches ==="
  # coverlet writes one report per target framework, suffixed (…opencover.net8.0.xml, …net10.0.xml)
  report="$(ls -1 tests/ConnPool.Tests/coverage/coverage.opencover*.xml 2>/dev/null | tail -1)"
  python3 - "$report" <<'PY'
import sys, xml.etree.ElementTree as ET, collections
root = ET.parse(sys.argv[1]).getroot()
files = {f.get("uid"): f.get("fullPath").replace("\\", "/") for f in root.iter("File")}
miss = collections.defaultdict(lambda: {"lines": set(), "branches": set()})
for m in root.iter("Method"):
    for sp in m.iter("SequencePoint"):
        if sp.get("vc") == "0":
            miss[files.get(sp.get("fileid"))]["lines"].add(int(sp.get("sl")))
    for bp in m.iter("BranchPoint"):
        if bp.get("vc") == "0":
            miss[files.get(bp.get("fileid"))]["branches"].add(int(bp.get("sl")))
clean = True
for path in sorted(k for k in miss if k):
    rel = "src/ConnPool" + path.split("/ConnPool", 1)[-1]
    lines = sorted(miss[path]["lines"])
    branches = sorted(miss[path]["branches"] - miss[path]["lines"])
    if lines or branches:
        clean = False
        print("  " + rel)
        if lines:
            print("    lines    : " + ", ".join(map(str, lines)))
        if branches:
            print("    branches : " + ", ".join(map(str, branches)))
if clean:
    print("  none - 100% line & branch")
PY
fi
