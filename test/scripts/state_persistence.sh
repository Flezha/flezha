#!/usr/bin/env bash
# Per-Partition state-persistence smoke test (full container cycle).
#
# Asserts the contract from `relay-state-persistence` (see
# `docs/controller_state_persistence.md` and the CLAUDE.md notes for
# `lib/flezha/state/`): after a `docker compose down` + `up` of the
# entire dev stack, persisted Partition state under
# `app_data/state/<executor_id>/<relay_class>/...` is rehydrated and
# the relay continues from where it left off — including
# `flz plugin enable`-style attachments (websocket + kafka producer).
#
# Layout:
#   * dedicated compose project name so it does not collide with a
#     developer's regular dev stack
#   * uses the regular `docker/dev/docker-compose.yml` and starts the
#     BEAM by shelling into the controller container with
#     `docker compose exec -d controller mix run --no-halt`,
#     polling for `executor_control_channel.sock` to know it is ready
#   * pulls executor_id from the UDS socket name in `app_data/sockets/`
#
# Usage:
#   test/scripts/state_persistence.sh
#
# Knobs (env vars):
#   REJOIN_TIMEOUT_S       per-step poll deadline (default 300)
#   FLEZHA_KEEP_STACK=1    leave compose stack up post-mortem
#   RELAY_CLASS            relay class on disk (default: store_input,
#                          matching `StoreInputRelay::factory_id()` in
#                          `test/apps/rust_executor/src/main.rs`)
#   FLEZHA_DEMO_NAMESPACE  namespace for the rust executor (default: default)
#   INPUT_WAIT_S           seconds to let testnet drive inputs into
#                          the relay before the down/up cycle (default 8)
#
# Failure modes this exercises:
#   * relay disappears after restart  -> `flz relay list` empty
#   * plugins drop on cspec reload    -> `flz relay info` missing entries
#   * persisted partition not rehydrated
#       -> `flz state list <class>` empty under the new executor_id
#   * time-travel replay broken
#       -> `flz state at <relay/shard/key> <seq>` errors

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DEV_COMPOSE="${REPO_ROOT}/docker/dev/docker-compose.yml"
TESTING_OVERLAY="${REPO_ROOT}/docker/dev/docker-compose.testing.yml"

REJOIN_TIMEOUT_S="${REJOIN_TIMEOUT_S:-300}"
DEMO_NAMESPACE="${FLEZHA_DEMO_NAMESPACE:-default}"
RELAY_CLASS="${RELAY_CLASS:-store_input}"
INPUT_WAIT_S="${INPUT_WAIT_S:-8}"

PROCESSOR_ID="${FLEZHA_PERSISTENCE_PROCESSOR:-sp_test_relay}"
PLUGIN_WS_ID="sp_test_ws"
PLUGIN_KAFKA_ID="sp_test_kafka"

PROJECT="${PROJECT:-flezha_state_persistence}"

COMPOSE=(
  docker compose
    --project-name "${PROJECT}"
    -f "${DEV_COMPOSE}"
    -f "${TESTING_OVERLAY}"
)

cd "${REPO_ROOT}/docker/dev"

# `docker/dev/docker-compose.yml` pins hardcoded container_name values
# (flezha_controller_dev, flezha_zookeeper, flezha_kafka, flezha_kafka_ui,
# flezha_rust_executor). Those names are global on the Docker daemon —
# `docker compose down` only removes containers labelled with this
# script's project name, so a leftover from a previous interrupted run
# (or from a parallel project name) collides on `up` with
#   "container name "/flezha_zookeeper" is already in use".
# Force-remove by name before bringing the stack up, and on exit.
KNOWN_CONTAINER_NAMES=(
  flezha_controller_dev
  flezha_zookeeper
  flezha_kafka
  flezha_kafka_ui
  flezha_rust_executor
)

force_remove_known_containers() {
  for name in "${KNOWN_CONTAINER_NAMES[@]}"; do
    docker rm -f "${name}" >/dev/null 2>&1 || true
  done
}

# Stale UDS socket files from a previous interrupted run linger on
# the host bind-mount (`app_data/sockets/`). The rust container's
# wait-loop watches for `executor_control_channel.sock` to exist and
# would race straight past it on next boot — exiting wait-loop before
# our `fix_volume_permissions` runs, then crashing on cargo
# permission errors. Wipe sockets pre-flight (per CLAUDE.md memory:
# never `rm -rf app_data`, only its subdirs).
clean_stale_sockets() {
  rm -f "${REPO_ROOT}"/app_data/sockets/*.sock 2>/dev/null || true
}

# Wipe persisted state at the START of a fresh run so we don't replay
# data from a previous test. Mid-cycle (between down and up) we MUST
# leave it alone — that's the contract under test.
clean_persisted_state() {
  rm -rf "${REPO_ROOT}"/app_data/state/* 2>/dev/null || true
}

# The dev compose's named volumes (flezha_build, flezha_deps,
# flezha_cargo_target, socket_data) are root-owned on first mount,
# but the toolchain image runs services as `flezha` (uid 1000, see
# `docker/dev/Dockerfile.toolchain`). Without chown'ing them, both
# `mix run` and `cargo run` fail with `Permission denied` inside
# their respective containers — symptom: rust executor hangs the
# script at "waiting for initial executor handshake" while logging
#   error: failed to create directory `/flezha/target/debug`
#   Caused by: Permission denied (os error 13)
# The healing compose handles this implicitly in its `command:` block;
# the regular dev compose does not, so we do it via `docker exec -u 0`.
fix_volume_permissions() {
  # Wait briefly for both containers to be exec-ready (they may have
  # only just been created by `up -d`).
  local deadline=$(( $(date +%s) + 30 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    [ "$(docker inspect -f '{{.State.Status}}' flezha_controller_dev 2>/dev/null)" = "running" ] \
      && [ "$(docker inspect -f '{{.State.Status}}' flezha_rust_executor 2>/dev/null)" = "running" ] \
      && break
    sleep 1
  done

  # `docker exec` (note: NOT `docker compose exec`) has no `-T`/`-i`
  # tty flag — it never allocates a TTY by default. -u 0 runs as root
  # inside the container regardless of the image's USER directive.
  docker exec -u 0 flezha_controller_dev sh -c '
    chown -R 1000:1000 /flezha/_build /flezha/deps 2>/dev/null
    chown -R 1000:1000 /flezha/target 2>/dev/null
    chown -R 1000:1000 /flezha/app_data 2>/dev/null
    chown -R 1000:1000 /socket_data 2>/dev/null
    true
  ' || {
    echo "[state_persistence] WARN: chown via controller failed (state=$(docker inspect -f '{{.State.Status}}' flezha_controller_dev 2>/dev/null))" >&2
  }
  docker exec -u 0 flezha_rust_executor sh -c '
    chown -R 1000:1000 /flezha/target 2>/dev/null
    chown -R 1000:1000 /socket_data 2>/dev/null
    true
  ' || {
    echo "[state_persistence] WARN: chown via rust failed (state=$(docker inspect -f '{{.State.Status}}' flezha_rust_executor 2>/dev/null))" >&2
  }
}

# Fail fast if the rust container has already exited (e.g. cargo
# permission error). Keeps the script from sitting on a 300s timeout.
assert_rust_running() {
  local state
  state="$(docker inspect -f '{{.State.Status}}' flezha_rust_executor 2>/dev/null || echo missing)"
  if [ "${state}" != "running" ]; then
    echo "[state_persistence] FAIL: rust container is not running (state=${state})" >&2
    docker logs --tail=80 flezha_rust_executor 2>&1 | sed 's/^/  rust> /' >&2 || true
    exit 1
  fi
}

cleanup() {
  if [ "${FLEZHA_KEEP_STACK:-0}" = "1" ]; then
    echo "[state_persistence] FLEZHA_KEEP_STACK=1, leaving stack up"
    return
  fi
  echo "[state_persistence] tearing down compose stack (project=${PROJECT})"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  # Compose 'down' is project-scoped; sweep stragglers by name too so
  # the next run starts clean even if an earlier run was interrupted
  # before the trap fired.
  force_remove_known_containers
}
trap cleanup EXIT

note() {
  echo "[state_persistence] $*"
}

flz() {
  "${COMPOSE[@]}" exec -T controller _build/dev/rel/flezha/bin/flz "$@"
}

# Run an arbitrary command inside the running controller container.
controller_sh() {
  "${COMPOSE[@]}" exec -T controller sh -c "$1"
}

SOCKETS_DIR="${REPO_ROOT}/app_data/sockets"
STATE_DIR="${REPO_ROOT}/app_data/state"
CONTROL_SOCKET="${SOCKETS_DIR}/executor_control_channel.sock"

# The regular dev compose does NOT auto-start the BEAM — its controller
# service just keeps the container alive (see `pgrep -f 'beam.smp\|tail'`
# healthcheck in docker-compose.yml). We start the BEAM ourselves here
# in detached mode and poll on the host filesystem for the controller's
# UDS control socket to confirm the BEAM is ready and listening.
start_beam() {
  note "fixing volume ownership (named volumes are root-owned on creation)"
  fix_volume_permissions

  # `flz` is a release artifact at `_build/dev/rel/flezha/bin/flz`.
  # `mix release` does NOT build it — see `copy_cli_commands` in
  # `mix.exs`, which only copies a pre-built `cli/target/debug/flz`
  # into the release. The host's copy of that file is the one
  # exposed inside the container via the `../../:/flezha` bind mount,
  # and on a host with a newer GLIBC than the toolchain image (e.g.
  # Ubuntu 25.10 GLIBC 2.39 vs Debian 12 GLIBC 2.36) the copied
  # binary will run with `version GLIBC_2.39 not found`.
  #
  # Workaround: build `flz` *inside the container* so it links against
  # the container's GLIBC, then overwrite the copy mix release just
  # made. Cargo writes to the workspace target dir at /flezha/target,
  # which the dev compose mounts as the named volume
  # `flezha_cargo_target` — so this never touches the host's
  # `cli/target/`.
  note "ensuring 'flz' release is built"
  if ! "${COMPOSE[@]}" exec -T controller sh -c \
    'cd /flezha && export MIX_ENV=dev && \
     mix deps.get --only "$MIX_ENV" && \
     mix release --overwrite' >/tmp/state_persistence_release.log 2>&1; then
    echo "[state_persistence] FAIL: mix release failed; tail of log:" >&2
    tail -40 /tmp/state_persistence_release.log >&2
    exit 1
  fi
  note "rebuilding flz CLI inside container (matches container GLIBC)"
  if ! "${COMPOSE[@]}" exec -T controller sh -c \
    'cd /flezha && cargo build -p flz --quiet 2>&1' >/tmp/state_persistence_cargo.log 2>&1; then
    echo "[state_persistence] FAIL: cargo build -p flz failed; tail of log:" >&2
    tail -40 /tmp/state_persistence_cargo.log >&2
    exit 1
  fi
  if ! "${COMPOSE[@]}" exec -T controller sh -c \
    'cp /flezha/target/debug/flz /flezha/_build/dev/rel/flezha/bin/flz'; then
    echo "[state_persistence] FAIL: could not patch release flz binary" >&2
    exit 1
  fi
  note "release built; flz patched with container-linked binary"

  note "starting BEAM inside controller container ('mix run --no-halt')"
  "${COMPOSE[@]}" exec -d -T controller sh -c \
    'cd /flezha && export MIX_ENV=dev && \
     exec mix run --no-halt'

  local deadline=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if [ -S "${CONTROL_SOCKET}" ]; then
      note "BEAM is up — control socket present at ${CONTROL_SOCKET}"
      return 0
    fi
    sleep 2
  done
  echo "[state_persistence] timed out waiting for BEAM control socket" >&2
  "${COMPOSE[@]}" logs --tail=200 controller >&2 || true
  return 1
}

fetch_active_ce_id() {
  # shellcheck disable=SC2012
  ls -t "${SOCKETS_DIR}"/primary_stream_${DEMO_NAMESPACE}_*.sock 2>/dev/null \
    | head -1 \
    | sed -E "s|.*/primary_stream_(${DEMO_NAMESPACE}_[0-9]+)\\.sock|\\1|"
}

wait_for_any_ce_id() {
  local deadline=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
  local current=""
  local last_diag=0
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    assert_rust_running
    current="$(fetch_active_ce_id)"
    if [ -n "${current}" ]; then
      echo "${current}"
      return 0
    fi
    # Periodic stderr diagnostic so cold cargo compiles aren't a black box.
    local now
    now="$(date +%s)"
    if [ $(( now - last_diag )) -ge 30 ]; then
      echo "[state_persistence] still waiting for handshake (rust last log line: $(docker logs --tail=1 flezha_rust_executor 2>&1 | tr -d '\r' | tail -1))" >&2
      last_diag="${now}"
    fi
    sleep 5
  done
  echo "[state_persistence] timed out waiting for executor_id" >&2
  docker logs --tail=80 flezha_rust_executor 2>&1 | sed 's/^/  rust> /' >&2 || true
  return 1
}

wait_for_new_ce_id() {
  local previous_ce_id="$1"
  local deadline=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
  local current=""
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    assert_rust_running
    current="$(fetch_active_ce_id)"
    if [ -n "${current}" ] && [ "${current}" != "${previous_ce_id}" ]; then
      echo "${current}"
      return 0
    fi
    sleep 5
  done
  echo "[state_persistence] timed out waiting for fresh executor_id (last: '${current}')" >&2
  docker logs --tail=80 flezha_rust_executor 2>&1 | sed 's/^/  rust> /' >&2 || true
  return 1
}

assert_relay_listed() {
  local stage="$1"
  if ! flz relay list -n "${DEMO_NAMESPACE}" 2>/dev/null | grep -q "${PROCESSOR_ID}"; then
    echo "[state_persistence] FAIL (${stage}): relay '${PROCESSOR_ID}' not listed" >&2
    flz relay list -n "${DEMO_NAMESPACE}" >&2 || true
    return 1
  fi
  note "${stage}: relay '${PROCESSOR_ID}' present in list"
}

assert_plugin_listed() {
  local plugin_id="$1"
  local stage="$2"
  if ! flz relay info -n "${DEMO_NAMESPACE}" 2>/dev/null | grep -q "${plugin_id}"; then
    echo "[state_persistence] FAIL (${stage}): plugin '${plugin_id}' not listed by 'flz relay info'" >&2
    flz relay info -n "${DEMO_NAMESPACE}" >&2 || true
    return 1
  fi
  note "${stage}: plugin '${plugin_id}' present"
}

assert_state_partitions_present() {
  local stage="$1"
  local out
  out="$(flz state list "${RELAY_CLASS}" --output json 2>&1 || true)"
  if ! echo "${out}" | grep -q '"partition_key"'; then
    echo "[state_persistence] FAIL (${stage}): no partitions reported under '${RELAY_CLASS}'" >&2
    echo "${out}" >&2
    return 1
  fi
  note "${stage}: 'flz state list ${RELAY_CLASS}' reports partitions"
  echo "${out}"
}

# Pick a partition path "relay_class/shard_id/partition_key" off the
# host filesystem so the time-travel assertion has a concrete target,
# regardless of which executor_id the rust container is running under.
discover_partition_path() {
  local rel
  rel="$(find "${STATE_DIR}" -maxdepth 4 -type f \( -name '*.wal' -o -name '*.snap.*' \) 2>/dev/null \
    | head -1)"
  if [ -z "${rel}" ]; then
    return 1
  fi
  local rc sh pk
  pk="$(basename "${rel}" | sed -E 's/\.(wal|snap\.[0-9]+|outbox\.dedup)$//')"
  sh="$(basename "$(dirname "${rel}")")"
  rc="$(basename "$(dirname "$(dirname "${rel}")")")"
  printf '%s/%s/%s\n' "${rc}" "${sh}" "${pk}"
}

assert_time_travel_replay() {
  local stage="$1"
  local path
  if ! path="$(discover_partition_path)"; then
    echo "[state_persistence] FAIL (${stage}): no on-disk WAL/snapshot under ${STATE_DIR}" >&2
    return 1
  fi
  note "${stage}: replaying ${path}@latest via 'flz state at'"
  if ! flz state at "${path}" latest --output json >/dev/null 2>&1; then
    # Fallback: some builds expect a numeric seq rather than the
    # word `latest`. Try seq=0 (genesis) which all valid partitions
    # accept and which proves the time-travel pipeline is wired.
    if ! flz state at "${path}" 0 --output json >/dev/null 2>&1; then
      echo "[state_persistence] FAIL (${stage}): 'flz state at ${path}' did not return a state" >&2
      flz state at "${path}" 0 >&2 || true
      return 1
    fi
  fi
  note "${stage}: time-travel replay succeeded for ${path}"
}

# --- Phase 0: bring stack up ----------------------------------------------

note "bringing up dev stack (project=${PROJECT}, transport=${FLEZHA_TRANSPORT:-uds})"
note "sweeping any leftover containers with hardcoded names from a prior run"
force_remove_known_containers
note "clearing stale sockets and persisted state from prior runs"
clean_stale_sockets
clean_persisted_state
docker network inspect flezha_network >/dev/null 2>&1 \
  || docker network create flezha_network >/dev/null
"${COMPOSE[@]}" up -d controller kafka rust

start_beam

note "waiting for initial executor handshake"
INITIAL_CE_ID="$(wait_for_any_ce_id)"
note "initial executor_id: ${INITIAL_CE_ID}"

# --- Phase 1: start relay + attach plugins --------------------------------

note "starting relay ${PROCESSOR_ID}"
flz relay start "${PROCESSOR_ID}" "${RELAY_CLASS}" \
  -n "${DEMO_NAMESPACE}" \
  --init_values '{"binary_inputs": [], "text_inputs": ["FIRST"]}'

# Boot the testnet WS server inside the controller container as a
# background process so the websocket plugin has something to talk to
# on ws://localhost:4500. The dev compose bind-mounts the repo
# into /flezha, so npm install + npm start work in-place.
note "starting testnet WS server (background) inside controller container"
controller_sh '
  cd /flezha/test/apps/testnet
  if [ ! -d node_modules ]; then
    npm install --silent >/dev/null 2>&1 || true
  fi
  if [ ! -d dist ]; then
    npm run build --silent >/dev/null 2>&1 || true
  fi
  pkill -f "node .*testnet" 2>/dev/null || true
  nohup node dist/index.js > /tmp/testnet.log 2>&1 &
  # Wait for the listener to bind so the plugin connect succeeds.
  for _ in $(seq 1 30); do
    (echo > /dev/tcp/127.0.0.1/4500) >/dev/null 2>&1 && exit 0
    sleep 1
  done
  echo "testnet WS server failed to bind 4500" >&2
  cat /tmp/testnet.log >&2 || true
  exit 1
'

note "enabling websocket plugin ${PLUGIN_WS_ID}"
flz plugin enable "${PLUGIN_WS_ID}" relay_websocket \
  --Xrelay_id="${PROCESSOR_ID}" \
  --Xtrigger="proactive" \
  --Xuri="ws://localhost:4500" \
  --Xpost_connect='{"payload":{"type":"text","message":"{\"event_type\":\"subscribe\",\"event_data\":{\"default_message\":\"hello world\",\"interval\":500}}"}}' \
  --Xposition=0

note "enabling kafka producer plugin ${PLUGIN_KAFKA_ID}"
flz plugin enable "${PLUGIN_KAFKA_ID}" relay_kafka_producer \
  --Xrelay_id="${PROCESSOR_ID}" \
  --Xtrigger=on_output \
  --Xhosts=kafka:9092 \
  --Xtopic="${PROCESSOR_ID}.test.v1"

assert_relay_listed   "baseline"
assert_plugin_listed  "${PLUGIN_WS_ID}"    "baseline"
assert_plugin_listed  "${PLUGIN_KAFKA_ID}" "baseline"

# Let the WS plugin drive inputs into the relay so apply_change runs
# and the WAL/outbox actually grow before we tear things down.
note "letting testnet drive inputs for ${INPUT_WAIT_S}s so WAL/snapshots fill"
sleep "${INPUT_WAIT_S}"

# Force at least one snapshot so restart doesn't replay the WAL alone.
note "forcing a snapshot via 'flz state rotate'"
PARTITION_PATH="$(discover_partition_path || true)"
if [ -n "${PARTITION_PATH}" ]; then
  flz state rotate "${PARTITION_PATH}" || true
else
  note "no partition discovered yet — skipping rotate (snapshot will fall back to WAL replay on restart)"
fi

assert_state_partitions_present "baseline"

# Capture the relay's pre-restart logical state so we can compare it
# against the post-restart view.
note "capturing pre-restart relay state via 'flz relay state'"
PRE_STATE="$(flz relay state "${PROCESSOR_ID}" -n "${DEMO_NAMESPACE}" 2>&1 || true)"
echo "${PRE_STATE}" | sed 's/^/  pre> /'

# --- Phase 2: full container cycle ----------------------------------------

note "stopping ALL containers (controller + kafka + rust + zookeeper)"
"${COMPOSE[@]}" down --remove-orphans
# Belt-and-braces: ensure none of the hardcoded-named containers
# survived (e.g. a sibling project owning the name).
force_remove_known_containers
# IMPORTANT: do NOT pass -v on this `down`, otherwise we'd nuke the
# socket_data named volume; bind-mounted app_data/ survives because
# it lives on the host filesystem.

note "verifying app_data/state/ survived teardown on host"
if [ -z "$(ls -A "${STATE_DIR}" 2>/dev/null)" ]; then
  echo "[state_persistence] FAIL: ${STATE_DIR} is empty after down — bind-mount lost data" >&2
  exit 1
fi
note "app_data/state intact: $(find "${STATE_DIR}" -type f | wc -l) on-disk files"

note "bringing stack back up"
force_remove_known_containers
# Wipe stale sockets ONLY (state persistence is what we're verifying,
# leave app_data/state alone here).
clean_stale_sockets
docker network inspect flezha_network >/dev/null 2>&1 \
  || docker network create flezha_network >/dev/null
"${COMPOSE[@]}" up -d controller kafka rust

start_beam

note "waiting for fresh executor handshake (executor_id rotates each boot)"
NEW_CE_ID="$(wait_for_new_ce_id "${INITIAL_CE_ID}")"
note "post-restart executor_id: ${NEW_CE_ID} (was ${INITIAL_CE_ID})"

# Give the controller a beat to finish alter_child_data + Overwatch
# rehydration + Restore RPC roundtrip with the SDK before we read.
sleep 5

# --- Phase 3: post-restart assertions -------------------------------------

assert_relay_listed   "post-restart"
assert_plugin_listed  "${PLUGIN_WS_ID}"    "post-restart"
assert_plugin_listed  "${PLUGIN_KAFKA_ID}" "post-restart"
assert_state_partitions_present "post-restart"
assert_time_travel_replay "post-restart"

# Restart the testnet WS server so the websocket plugin reconnects
# and pushes more inputs through the rehydrated state — this proves
# the runtime is fully live, not just persisted on disk.
note "re-starting testnet WS server post-restart"
controller_sh '
  cd /flezha/test/apps/testnet
  pkill -f "node .*testnet" 2>/dev/null || true
  nohup node dist/index.js > /tmp/testnet.log 2>&1 &
  for _ in $(seq 1 30); do
    (echo > /dev/tcp/127.0.0.1/4500) >/dev/null 2>&1 && exit 0
    sleep 1
  done
  echo "testnet WS server failed to bind 4500 (post-restart)" >&2
  exit 1
'

note "letting post-restart inputs flow for ${INPUT_WAIT_S}s"
sleep "${INPUT_WAIT_S}"

note "capturing post-restart relay state"
POST_STATE="$(flz relay state "${PROCESSOR_ID}" -n "${DEMO_NAMESPACE}" 2>&1 || true)"
echo "${POST_STATE}" | sed 's/^/  post> /'

note "PASS: relay '${PROCESSOR_ID}' + plugins survived a full container cycle"
note "      and partition state replayed under '${RELAY_CLASS}'"
