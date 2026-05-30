#!/usr/bin/env bash
# Reset the local runtime data dirs between test/soak runs.
#
# Deletes only the regenerable per-run state under this repo's app_data/
# (WAL state, ProcessHub registry, sockets, persisted executor ids). It
# never touches durable config (certs, auth) and refuses to run anywhere
# but the repo root, so a stray invocation cannot delete outside the
# project. Run it as its OWN command (not chained after cd/mix) so the
# permission allowlist matches it cleanly.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# Guard: only ever operate on this project's app_data.
case "${repo_root}" in
  /home/moon/Code/flezha) : ;;
  *) echo "test_reset: refusing to run outside the flezha repo (${repo_root})" >&2; exit 1 ;;
esac

for sub in state process_hub sockets exec-id; do
  rm -rf "app_data/${sub:?}"/* 2>/dev/null || true
done

echo "test_reset: cleared app_data/{state,process_hub,sockets,exec-id}"
