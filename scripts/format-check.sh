#!/usr/bin/env bash
# The source-shape gate: every formatter in check mode, then the licensing
# checks. Mirrors what .github/workflows/format.yml runs on PRs. Auto-fix
# with scripts/format-apply.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

scripts/format-apply.sh --check

# lint-file over git-listed paths: a full `reuse lint` walks the tree and chokes on
# the stale test sockets under app_data/, which git never lists. CI lints the
# whole clean checkout.
echo "==> reuse lint"
git ls-files -co --exclude-standard -z | xargs -0 reuse lint-file

echo "==> THIRD_PARTY_NOTICES.md is fresh"
cargo about generate about.hbs -o "${TMPDIR:-/tmp}/third_party_notices.md"
diff -u THIRD_PARTY_NOTICES.md "${TMPDIR:-/tmp}/third_party_notices.md"
