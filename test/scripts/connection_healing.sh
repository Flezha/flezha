#!/usr/bin/env bash
# Runner <-> agent connection-healing smoke test.
#
# Boots the dev compose stack (agent + Rust compute engine), captures
# the agent-minted runner_id for the rust runner, then exercises the
# two restart paths the connection-healing change covers:
#
#   1. agent restart -> rust runner stays alive, reconnects, picks up a new
#      runner_id within the keepalive window.
#   2. rust runner restart -> agent stays alive, mints a new
#      runner_id when the runner reconnects.
#
# After each restart the script asserts the agent observes a fresh, distinct
# runner_id within $REJOIN_TIMEOUT_S seconds. That is the proxy for
# "runner's data-stream traffic resumes" — without the rebind contract from
# this change, the agent would still see the runner connect, but data
# streams would silently drop on the agent side.
#
# Output delivery is intentionally NOT verified by this script: wiring an
# output relay and a output plugin into the dev compose default bootstrap is
# out of scope for the connection-healing change.
#
# Usage:
#   test/scripts/connection_healing.sh        # full cycle, ~5 min on a primed
#                                             # build cache; budget 15-20 min
#                                             # on first run while elixir +
#                                             # cargo compile from scratch.
#   FLEZHA_TRANSPORT=quic test/scripts/connection_healing.sh
#
# Override REJOIN_TIMEOUT_S (default 300) for the per-restart wait; bump it
# above ~600 if you're running this on a cold cache. Pass FLEZHA_KEEP_STACK=1
# to skip teardown for post-mortem inspection.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# Self-contained compose file just for this test — does NOT inherit the dev
# compose's named volumes, which would shadow the host's build cache and
# force a from-scratch elixir + cargo compile on every run.
HEALING_COMPOSE="${REPO_ROOT}/docker/dev/docker-compose.healing.yml"

REJOIN_TIMEOUT_S="${REJOIN_TIMEOUT_S:-300}"
DEMO_NAMESPACE="${FLEZHA_DEMO_NAMESPACE:-default}"

# Use a dedicated compose project so this test never collides with the
# developer's normal `docker/dev` stack (containers, volumes, network IDs
# are all namespaced by project). Override via PROJECT=foo to pin the name.
PROJECT="${PROJECT:-flezha_healing}"

COMPOSE=(
  docker compose
    --project-name "${PROJECT}"
    -f "${HEALING_COMPOSE}"
)

cd "${REPO_ROOT}/docker/dev"

cleanup() {
  if [ "${FLEZHA_KEEP_STACK:-0}" = "1" ]; then
    echo "[connection_healing] FLEZHA_KEEP_STACK=1, leaving stack up for inspection"
    return
  fi
  echo "[connection_healing] tearing down compose stack (project=${PROJECT})"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

note() {
  echo "[connection_healing] $*"
}

# Read the agent's currently-active runner_id for the demo namespace
# by inspecting the per-CE primary stream UDS socket files the agent creates
# inside `<repo>/storage/app/sockets/`. The bind-mounted sockets directory makes
# this a no-overhead host-side check that doesn't depend on the BEAM having
# distribution wired up. Returns the most recently created socket's ce_id, or
# "" if no handshake has happened yet.
SOCKETS_DIR="${REPO_ROOT}/storage/app/sockets"

fetch_active_ce_id() {
  # shellcheck disable=SC2012  # ls -t for mtime sort is intentional and portable here
  ls -t "${SOCKETS_DIR}"/primary_stream_${DEMO_NAMESPACE}_*.sock 2>/dev/null \
    | head -1 \
    | sed -E "s|.*/primary_stream_(${DEMO_NAMESPACE}_[0-9]+)\\.sock|\\1|"
}

# Wait until fetch_active_ce_id returns a non-empty value distinct from $1.
wait_for_new_ce_id() {
  local previous_ce_id="$1"
  local deadline=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
  local current=""
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    current="$(fetch_active_ce_id)"
    if [ -n "${current}" ] && [ "${current}" != "${previous_ce_id}" ]; then
      echo "${current}"
      return 0
    fi
    sleep 2
  done
  echo "[connection_healing] timed out waiting for fresh runner_id (last seen: '${current}')" >&2
  return 1
}

note "bringing up dev compose stack (project=${PROJECT}, transport=${FLEZHA_TRANSPORT:-uds})"
"${COMPOSE[@]}" up -d controller rust

note "waiting for agent + rust runner to handshake (this includes a one-time elixir + cargo compile on first run; budget ~3-8 min)"
INITIAL_CE_ID=""
DEADLINE=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
while [ -z "${INITIAL_CE_ID}" ] && [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  INITIAL_CE_ID="$(fetch_active_ce_id)"
  [ -z "${INITIAL_CE_ID}" ] && sleep 5
done

if [ -z "${INITIAL_CE_ID}" ]; then
  echo "[connection_healing] FAIL: rust runner never handshook with the agent" >&2
  "${COMPOSE[@]}" logs --tail=200 agent rust >&2 || true
  exit 1
fi

note "initial runner_id: ${INITIAL_CE_ID}"

# --- Path 1: restart the agent ------------------------------------------------
note "restarting agent"
"${COMPOSE[@]}" restart agent

NEW_CE_ID="$(wait_for_new_ce_id "${INITIAL_CE_ID}")"
note "after agent restart, fresh runner_id: ${NEW_CE_ID}"

# --- Path 2: restart the rust runner container ------------------------------
note "restarting rust compute engine container"
"${COMPOSE[@]}" restart rust

NEXT_CE_ID="$(wait_for_new_ce_id "${NEW_CE_ID}")"
note "after rust restart, fresh runner_id: ${NEXT_CE_ID}"

note "PASS: both restart paths re-established the runner <-> agent link"
