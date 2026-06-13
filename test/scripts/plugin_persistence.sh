#!/usr/bin/env bash
# Executor-restart plugin-persistence smoke test.
#
# Asserts the contract from
# openspec/specs/plugin-subscription-persistence/spec.md: plugins
# enabled via `flz plugin enable` after `flz relay start` SHALL
# survive a `docker compose restart rust` without executor
# intervention. Before the cspec-persistence change, this fails — the
# Rust restart triggers `alter_child_data` to rewrite the cspec start
# args with a fresh executor_id, ProcessHub stops + restarts
# the Overwatch under the new spec, and the runtime plugin extensions
# table boots empty (only plugins from `flz relay start
# --plugins` are restored).
#
# Reuses the same dedicated compose project pattern as
# `connection_healing.sh` so this test does not collide with the
# developer's regular dev stack.
#
# Usage:
#   test/scripts/plugin_persistence.sh
#
# Override REJOIN_TIMEOUT_S (default 300) for the per-restart wait.
# Pass FLEZHA_KEEP_STACK=1 to skip teardown for post-mortem.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HEALING_COMPOSE="${REPO_ROOT}/docker/dev/docker-compose.healing.yml"

REJOIN_TIMEOUT_S="${REJOIN_TIMEOUT_S:-300}"
DEMO_NAMESPACE="${FLEZHA_DEMO_NAMESPACE:-default}"
PROCESSOR_ID="${FLEZHA_PERSISTENCE_PROCESSOR:-pp_test_relay}"
PLUGIN_WS_ID="pp_test_ws"
PLUGIN_KAFKA_ID="pp_test_kafka"

PROJECT="${PROJECT:-flezha_plugin_persistence}"

COMPOSE=(
  docker compose
    --project-name "${PROJECT}"
    -f "${HEALING_COMPOSE}"
)

cd "${REPO_ROOT}/docker/dev"

cleanup() {
  if [ "${FLEZHA_KEEP_STACK:-0}" = "1" ]; then
    echo "[plugin_persistence] FLEZHA_KEEP_STACK=1, leaving stack up"
    return
  fi
  echo "[plugin_persistence] tearing down compose stack (project=${PROJECT})"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

note() {
  echo "[plugin_persistence] $*"
}

flz() {
  "${COMPOSE[@]}" exec -T controller /flezha/target/release/flz "$@"
}

SOCKETS_DIR="${REPO_ROOT}/app_data/sockets"

fetch_active_ce_id() {
  # shellcheck disable=SC2012
  ls -t "${SOCKETS_DIR}"/primary_stream_${DEMO_NAMESPACE}_*.sock 2>/dev/null \
    | head -1 \
    | sed -E "s|.*/primary_stream_(${DEMO_NAMESPACE}_[0-9]+)\\.sock|\\1|"
}

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
  echo "[plugin_persistence] timed out waiting for fresh executor_id (last seen: '${current}')" >&2
  return 1
}

assert_plugin_listed() {
  local plugin_id="$1"
  local stage="$2"
  if ! flz relay info "${DEMO_NAMESPACE}" 2>/dev/null | grep -q "${plugin_id}"; then
    echo "[plugin_persistence] FAIL (${stage}): plugin '${plugin_id}' not listed by 'flz relay info'" >&2
    flz relay info "${DEMO_NAMESPACE}" >&2 || true
    return 1
  fi
  note "${stage}: plugin '${plugin_id}' present"
}

note "bringing up dev compose stack (project=${PROJECT})"
"${COMPOSE[@]}" up -d controller rust

note "waiting for agent + rust executor to handshake"
INITIAL_CE_ID=""
DEADLINE=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
while [ -z "${INITIAL_CE_ID}" ] && [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  INITIAL_CE_ID="$(fetch_active_ce_id)"
  if [ -z "${INITIAL_CE_ID}" ]; then sleep 5; fi
done

if [ -z "${INITIAL_CE_ID}" ]; then
  echo "[plugin_persistence] FAIL: rust executor never handshook with the agent" >&2
  exit 1
fi
note "initial executor_id: ${INITIAL_CE_ID}"

note "starting relay ${PROCESSOR_ID}"
flz relay start "${PROCESSOR_ID}" store_input \
  --init_values '{"binary_inputs": [], "text_inputs": ["FIRST"]}'

note "enabling websocket plugin ${PLUGIN_WS_ID}"
flz plugin enable "${PLUGIN_WS_ID}" relay_websocket \
  --Xrelay_id="${PROCESSOR_ID}" \
  --Xtrigger="proactive" \
  --Xuri="ws://localhost:4500" \
  --Xposition=0

note "enabling kafka producer plugin ${PLUGIN_KAFKA_ID}"
flz plugin enable "${PLUGIN_KAFKA_ID}" relay_kafka_producer \
  --Xrelay_id="${PROCESSOR_ID}" \
  --Xtrigger=on_output \
  --Xhosts=kafka:9092 \
  --Xtopic="${PROCESSOR_ID}.test.v1"

assert_plugin_listed "${PLUGIN_WS_ID}" "baseline"
assert_plugin_listed "${PLUGIN_KAFKA_ID}" "baseline"

note "restarting rust compute engine container"
"${COMPOSE[@]}" restart rust

NEW_CE_ID="$(wait_for_new_ce_id "${INITIAL_CE_ID}")"
note "after rust restart, fresh executor_id: ${NEW_CE_ID}"

# Give the agent a beat to finish alter_child_data + Overwatch init/1
# before reading the cspec back out.
sleep 5

assert_plugin_listed "${PLUGIN_WS_ID}" "post-restart"
assert_plugin_listed "${PLUGIN_KAFKA_ID}" "post-restart"

note "PASS: both plugins survived the executor restart without re-issue"
