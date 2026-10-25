#!/usr/bin/env bash
# Lint & format the crate with clippy + rustfmt.
#
#   ./lint.sh          # gate: clippy with warnings-as-errors (what CI blocks on)
#   ./lint.sh -f       # auto-fix: cargo fmt + cargo clippy --fix
#
# Mirrors the lint.sh in the sibling languages. Needs the clippy + rustfmt components
# (`rustup component add clippy rustfmt`).
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "-f" || "${1:-}" == "--fix" ]]; then
  cargo fmt
  cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged -- -D warnings || true
  exit 0
fi

# Gate: clippy over all targets + features, warnings are errors.
cargo clippy --all-targets --all-features -- -D warnings
# Report-only: formatting (never fail the gate on style).
echo "--- rustfmt (report-only) ---"
cargo fmt --check || true
