#!/usr/bin/env bash
# Auto-fix every formatter we enforce. Run before pushing if
# scripts/format-check.sh fails. CI's format.yml uses the matching
# `--check` variants.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> cargo fmt"
cargo +nightly fmt --all

echo "==> mix format"
mix format

echo "==> mvn spotless:apply"
mvn -q -f sdk/java/pom.xml -DskipTests spotless:apply
