#!/usr/bin/env bash
# Run all three formatter gates without writing. Mirrors what
# .github/workflows/format.yml runs on PRs. Use scripts/format-apply.sh
# to auto-fix.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> cargo fmt --check"
cargo +nightly fmt --all -- --check

echo "==> mix format --check-formatted"
mix format --check-formatted

echo "==> mvn spotless:check"
mvn -q -f sdk/java/pom.xml -DskipTests spotless:check
