#!/usr/bin/env bash
#
# Point this clone's git hooks at the repo-tracked `.githooks/` directory
# so the pre-commit `dist/` staleness check runs on every commit.
#
# Run once after cloning:
#   scripts/install-hooks.sh
#
# To disable for this clone:
#   git config --unset core.hooksPath

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"

cd "${REPO_ROOT}"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "Not a git repository: ${REPO_ROOT}" >&2
  exit 1
fi

git config core.hooksPath .githooks
chmod +x .githooks/pre-commit

echo "git hooks now point at .githooks/"
echo "  pre-commit  — refuse commits that change SDK/test-app sources"
echo "                without staging refreshed dist/ jars"
echo
echo "To bypass once:        git commit --no-verify"
echo "To disable persistently: git config --unset core.hooksPath"
