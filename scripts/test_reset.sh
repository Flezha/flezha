#!/usr/bin/env bash
# Reset the local runtime data dirs between test/soak runs.
#
# Deletes only the regenerable per-run state under this repo's storage/app/
# (WAL state, ProcessHub registry, sockets, persisted runner ids). It
# never touches durable config (certs, auth) and refuses to run unless the
# tree it resolved declares this application, so a copy of this script
# dropped into another project cannot delete that project's state. Run it
# as its OWN command (not chained after cd/mix) so the permission allowlist
# matches it cleanly.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# Guard: only ever operate on this project's storage. Identity, not location —
# a path says where a tree sits, which a clone or a worktree changes; mix.exs
# says which project it is. Anchored at both ends so `:flezha_other` cannot
# pass, and the trailing comma is optional so reordering the keyword list does
# not break the guard.
if ! grep -Eq '^[[:space:]]*app:[[:space:]]*:flezha[[:space:]]*,?[[:space:]]*$' mix.exs 2>/dev/null; then
  echo "test_reset: refusing to run in ${repo_root}: its mix.exs does not declare 'app: :flezha'" >&2
  exit 1
fi

for sub in state process_hub sockets exec-id; do
  rm -rf "storage/app/${sub:?}"/* 2>/dev/null || true
done

echo "test_reset: cleared storage/app/{state,process_hub,sockets,exec-id}"
