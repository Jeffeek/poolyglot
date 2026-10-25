#!/usr/bin/env bash
# Run the connpool test suite under coverage and enforce the 100% gate.
#
#   ./run_tests.sh          # pass/fail + coverage summary (gate: 100% line & branch)
#   ./run_tests.sh -m       # ...also list the uncovered lines / partial branches
#
# Mirrors python/run_tests.sh and csharp/run_tests.sh. Needs Maven + a JDK 17+.
# The gate itself is enforced by the jacoco:check bound to `verify` (pom.xml); the -m
# report parses JaCoCo's XML (python3, already present in this polyglot repo).
set -uo pipefail
cd "$(dirname "$0")"

show_missing=false
[[ "${1:-}" == "-m" ]] && show_missing=true

if mvn -q -B clean verify; then
  status=0
else
  status=$?
fi

report=target/site/jacoco/jacoco.xml
if [[ -f "$report" ]]; then
  echo
  echo "=== coverage ==="
  python3 - "$report" "$show_missing" <<'PY'
import sys, xml.etree.ElementTree as ET

path, show_missing = sys.argv[1], sys.argv[2] == "true"
root = ET.parse(path).getroot()

def totals(counter):
    for c in root.findall("counter"):
        if c.get("type") == counter:
            missed, covered = int(c.get("missed")), int(c.get("covered"))
            total = missed + covered
            pct = 100.0 if total == 0 else 100.0 * covered / total
            return missed, covered, pct
    return 0, 0, 100.0

for name in ("LINE", "BRANCH"):
    missed, covered, pct = totals(name)
    print(f"  {name.lower():7}: {pct:6.2f}%  ({covered}/{covered + missed})")

if show_missing:
    print()
    print("=== uncovered lines / partial branches ===")
    clean = True
    for pkg in root.iter("package"):
        pkg_name = pkg.get("name")
        for sf in pkg.findall("sourcefile"):
            lines, branches = [], []
            for ln in sf.findall("line"):
                nr = int(ln.get("nr"))
                if int(ln.get("ci")) == 0:
                    lines.append(nr)
                elif int(ln.get("mb")) > 0:
                    branches.append(nr)
            if lines or branches:
                clean = False
                print(f"  {pkg_name}/{sf.get('name')}")
                if lines:
                    print("    lines    : " + ", ".join(map(str, lines)))
                if branches:
                    print("    branches : " + ", ".join(map(str, branches)))
    if clean:
        print("  none - 100% line & branch")
PY
fi

exit "$status"
