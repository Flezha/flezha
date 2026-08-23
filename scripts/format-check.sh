#!/usr/bin/env bash
# Run the formatter and licensing gates without writing. Mirrors what
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

# lint-file over git-listed paths: a full `reuse lint` walks the tree and chokes on
# the stale test sockets under app_data/, which git never lists. CI lints the
# whole clean checkout.
echo "==> reuse lint"
git ls-files -co --exclude-standard -z | xargs -0 reuse lint-file

echo "==> THIRD_PARTY_NOTICES.md is fresh"
cargo about generate about.hbs -o "${TMPDIR:-/tmp}/third_party_notices.md"
diff -u THIRD_PARTY_NOTICES.md "${TMPDIR:-/tmp}/third_party_notices.md"
