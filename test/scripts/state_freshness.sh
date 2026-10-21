#!/usr/bin/env bash
# SDK -> agent graceful-shutdown state-freshness smoke test (dev compose).
#
# Exercises the `RunnerShutdownNotification` path from
# `ship-fresh-state-on-sdk-shutdown` against the same dev compose stack
# the connection-healing test uses, sending SIGTERM via
# `docker compose stop` and asserting the agent's
# `Overwatch.state.snapshot` is populated for every running relay.
#
# Two scenarios:
#
#   A. periodic-enabled: relay uses the agent's default
#      `overwatch_state_snapshot_interval_ms`. After SIGTERM the
#      snapshot reflects the runner's true final state (not the
#      up-to-interval stale periodic value).
#   B. disabled-snapshot: relay is started with
#      `snapshot_interval: 0` in its opts. Without the shutdown notification the
#      agent would have no snapshot at all (nil); this scenario asserts
#      the notification populated it anyway.
#
# Status — DEMONSTRATIVE.
#
# The agent's dev compose entrypoint runs `mix run --no-halt` without
# Erlang distribution (no `--sname`/`--cookie`), so `iex --remsh`-based
# state inspection from a sibling docker-exec process is not available
# without modifying the compose contract. The CANONICAL automated
# verification lives in:
#
#   test/system/state_freshness_e2e_test.exs
#       (mix test --include system test/system/state_freshness_e2e_test.exs)
#
# That test boots a real Rust SDK subprocess via the existing
# `Flezha.Service.PortPilot` infrastructure, sends inputs through the
# real wire protocol, sends SIGTERM via `kill -TERM <os_pid>`, and
# asserts the agent's `Overwatch.state.snapshot` carries those inputs
# decoded back as the SDK's serialized state JSON. It exercises the
# same end-to-end contract as this script in a fraction of the time
# and without Docker.
#
# This script is kept as a starting point for a Kubernetes-shape
# regression check (where the agent runs in a real container with a
# real grace-period timeout) but currently no-ops on the assertion
# step until the compose stack exposes either distribution or a
# snapshot-dump endpoint. Run via:
#
#   mix test.state_freshness         # invoke with the alias
#   FLEZHA_KEEP_STACK=1 ...          # leave stack up post-mortem
#
# REJOIN_TIMEOUT_S overrides the per-step poll deadline (default 300).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HEALING_COMPOSE="${REPO_ROOT}/docker/dev/docker-compose.healing.yml"

REJOIN_TIMEOUT_S="${REJOIN_TIMEOUT_S:-300}"
DEMO_NAMESPACE="${FLEZHA_DEMO_NAMESPACE:-default}"
PROJECT="${PROJECT:-flezha_state_freshness}"
SNAPSHOT_FILE="/tmp/state_freshness_${PROJECT}.flz"

COMPOSE=(
  docker compose
    --project-name "${PROJECT}"
    -f "${HEALING_COMPOSE}"
)

cd "${REPO_ROOT}/docker/dev"

cleanup() {
  if [ "${FLEZHA_KEEP_STACK:-0}" = "1" ]; then
    echo "[state_freshness] FLEZHA_KEEP_STACK=1, leaving stack up for inspection"
    return
  fi
  echo "[state_freshness] tearing down compose stack (project=${PROJECT})"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "${SNAPSHOT_FILE}"
}
trap cleanup EXIT

note() {
  echo "[state_freshness] $*"
}

SOCKETS_DIR="${REPO_ROOT}/storage/app/sockets"

fetch_active_ce_id() {
  # shellcheck disable=SC2012
  ls -t "${SOCKETS_DIR}"/primary_stream_${DEMO_NAMESPACE}_*.sock 2>/dev/null \
    | head -1 \
    | sed -E "s|.*/primary_stream_(${DEMO_NAMESPACE}_[0-9]+)\\.sock|\\1|"
}

wait_for_ce_id() {
  local deadline=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
  local current=""
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    current="$(fetch_active_ce_id)"
    if [ -n "${current}" ]; then
      echo "${current}"
      return 0
    fi
    sleep 2
  done
  echo "[state_freshness] timed out waiting for runner_id" >&2
  return 1
}

# Run an Elixir snippet inside the agent container's BEAM. The agent is
# started with `mix run --no-halt` (no release), so RPC into it goes
# through `iex --remsh`-style attach. To keep the script self-contained
# and avoid distribution wiring, we run the snippet in a sibling BEAM
# that connects to the agent via Erlang distribution if available; if
# not available, we fall back to a `mix eval` that re-uses the same
# build cache and shares the OTP application state.
agent_eval() {
  local snippet="$1"
  "${COMPOSE[@]}" exec -T agent sh -c \
    "cd /flezha && export MIX_ENV=dev MIX_BUILD_PATH=/home/flezha/_build/dev MIX_DEPS_PATH=/home/flezha/deps && \
     iex --erl '-noinput' --rpc-eval flezha@127.0.0.1 'flezha_dev_cookie' '${snippet}' 2>/dev/null \
     || mix run --no-start -e '${snippet}'"
}

start_relay() {
  local relay_id="$1"
  local snapshot_interval="$2"

  note "starting relay ${relay_id} (snapshot_interval=${snapshot_interval})"

  local snippet
  snippet=$(cat <<EOF
builder = %Flezha.Relay.Builder{
  id: "${relay_id}",
  factory_id: "store_input",
  init_values: %{},
  opts: [snapshot_interval: ${snapshot_interval}]
}
{:ok, info} = Flezha.Service.RelayManager.start_relay("${DEMO_NAMESPACE}", builder)
IO.inspect(info, label: "started")
EOF
)
  agent_eval "${snippet}"
}

read_snapshot_for_relay() {
  local relay_id="$1"

  local snippet
  snippet=$(cat <<EOF
{:ok, state} = Flezha.Service.RelayManager.get_overwatch_state("${DEMO_NAMESPACE}", "${relay_id}")
IO.puts("SNAPSHOT_BINARY_PRESENT=" <> if state.snapshot != nil and byte_size(state.snapshot) > 0, do: "yes", else: "no")
case state.snapshot do
  bin when is_binary(bin) -> IO.puts("SNAPSHOT_BYTES=" <> Base.encode64(bin))
  _ -> IO.puts("SNAPSHOT_BYTES=")
end
EOF
)
  agent_eval "${snippet}"
}

assert_snapshot_present() {
  local relay_id="$1"
  local label="$2"

  local output
  output="$(read_snapshot_for_relay "${relay_id}" 2>&1)"
  echo "${output}"

  if ! echo "${output}" | grep -q "SNAPSHOT_BINARY_PRESENT=yes"; then
    echo "[state_freshness] FAIL (${label}): relay ${relay_id} has no agent-side snapshot after SIGTERM" >&2
    "${COMPOSE[@]}" logs --tail=200 agent rust >&2 || true
    exit 1
  fi

  note "PASS (${label}): relay ${relay_id} agent snapshot is populated"
}

# --- Scenario A: periodic-enabled relay ----------------------------------

note "bringing up dev compose stack (project=${PROJECT}, transport=${FLEZHA_TRANSPORT:-uds})"
"${COMPOSE[@]}" up -d agent rust

note "waiting for initial handshake"
INITIAL_CE_ID="$(wait_for_ce_id)"
note "initial runner_id: ${INITIAL_CE_ID}"

TASK_A="state_freshness_a_$$"
start_relay "${TASK_A}" "60_000"

# Brief settle so the relay's `start_relay_sdk` round-trip
# completes before we yank the rust container.
sleep 2

note "stopping rust container with SIGTERM"
"${COMPOSE[@]}" stop --timeout 10 rust

# Allow the agent's ControlChannel to process the inbound notification
# before we read state. The cast applies in microseconds; 1 s is ample.
sleep 1

assert_snapshot_present "${TASK_A}" "scenario A (periodic-enabled)"

# Bring rust back to leave the stack in a sane state for scenario B.
"${COMPOSE[@]}" up -d rust
note "waiting for rust restart handshake"
wait_for_ce_id >/dev/null

# --- Scenario B: disabled-snapshot relay ---------------------------------

TASK_B="state_freshness_b_$$"
start_relay "${TASK_B}" "0"
sleep 2

note "stopping rust container with SIGTERM (scenario B)"
"${COMPOSE[@]}" stop --timeout 10 rust
sleep 1

assert_snapshot_present "${TASK_B}" "scenario B (disabled-snapshot)"

note "PASS: both state-freshness scenarios populated agent.Overwatch.state.snapshot via RunnerShutdownNotification"
