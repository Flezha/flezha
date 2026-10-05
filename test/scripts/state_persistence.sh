#!/usr/bin/env bash
# Per-Cell state-persistence smoke test (engagement-only).
#
# Verifies the data-plane half of the `relay-state-persistence`
# contract (see `docs/internals/state_persistence.md`): with a relay
# running, plugins attached, and inputs flowing, the controller writes
# WAL files under the namespaced layout
# `app_data/state/<namespace>/<relay_class>/<tasklet_id>/<cell_key>.wal`
# and `flz state list <relay_class>` surfaces the live cell.
#
# `relay_class` is NOT the factory id: `Overwatch.relay_class/1`
# falls back to the relay's own id, so the class level on disk — and
# the argument `flz state list` takes — is the relay id, while
# `flz relay start` takes the factory id. The script keeps them in
# two variables because they are two different names for two different
# things.
#
# Cost: 2.5-4 min from a clean state, dominated by the `cargo build` and
# `mix release` inside the container — `cleanup` runs `down -v`, which
# removes the `_build`/`deps`/`target` volumes, so EVERY run rebuilds.
# Manual — see "Keeping it honest" at the end of this header.
#
# Out of scope (deliberately, see end-of-script PASS notes):
#   * Full controller-side BEAM down/up cycle. The controller does
#     not auto-recreate relays after its own BEAM restart in this
#     dev setup, so a stop/start of the whole stack would intentionally
#     leave us with "No relays found" until someone re-issues
#     `flz relay start`. The SDK-side `RestoreInitState` handshake
#     does work for runner-only restarts; that flavor of test
#     belongs next to `plugin_persistence.sh`.
#   * `flz state at` / `flz state get` / `flz state rotate`. An older note
#     here claimed these crash the BEAM; they do not — all three were driven
#     against this stack and the node stayed up. `state get` and `state at`
#     belong to `cell-state-live-query` and the time-travel CLI, `rotate`
#     to `cell-compaction`; wiring them in is those capabilities' job, not
#     this one's. Note `state at` takes the DECODED cell key, so the
#     on-disk filename (`_.wal` for the empty key) is not a path fragment.
#
# Layout:
#   * dedicated compose project name so it does not collide with the
#     dev stack a developer might already have running
#   * uses `docker/dev/docker-compose.yml` plus a small testing-only
#     overlay (`docker-compose.testing.yml`) that adds `stdin_open: true`
#     to the rust service so the runner's `std_keeper` doesn't EOF
#   * starts the BEAM by `docker compose exec -d controller bash -c
#     '... mix run --no-halt'`, with explicit `-name`/`-setcookie`
#     ERL_FLAGS so `flz` (which shells out to `flzvm rpc`) can reach
#     the running BEAM via Erlang distribution
#   * pulls runner_id from the UDS socket name in `app_data/sockets/`
#
# Usage:
#   test/scripts/state_persistence.sh
#   FLEZHA_KEEP_STACK=1 test/scripts/state_persistence.sh
#       # leave the stack up post-mortem
#
# Knobs (env vars):
#   REJOIN_TIMEOUT_S       per-step poll deadline (default 300)
#   FLEZHA_KEEP_STACK=1    leave compose stack up post-mortem
#   FACTORY_ID             factory the relay is started from (default:
#                          store_input, matching `StoreInputRelay::
#                          factory_id()` in `test/apps/rust_runner/src/main.rs`)
#   FLEZHA_PERSISTENCE_RELAY  relay id, which is also its state class
#                          on disk (default: sp_test_relay)
#   FLEZHA_DEMO_NAMESPACE  namespace for the rust runner (default: default)
#   INPUT_WAIT_S           seconds to let testnet drive inputs into
#                          the relay before assertions (default 8)
#
# Keeping it honest:
#   This script is NOT wired into CI — it needs a Docker daemon and a
#   full in-container toolchain build, none of which the
#   `elixir`/`naming`/`docker` workflows carry. Run it by hand when you
#   touch `lib/flezha/state/`, the plugin-attach CLI, or this script.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DEV_COMPOSE="${REPO_ROOT}/docker/dev/docker-compose.yml"
TESTING_OVERLAY="${REPO_ROOT}/docker/dev/docker-compose.testing.yml"

REJOIN_TIMEOUT_S="${REJOIN_TIMEOUT_S:-300}"
DEMO_NAMESPACE="${FLEZHA_DEMO_NAMESPACE:-default}"
INPUT_WAIT_S="${INPUT_WAIT_S:-8}"

# The factory `flz relay start` instantiates, and the relay id it
# is instantiated under. The id is ALSO the class level of the on-disk
# state layout and the argument `flz state list` takes — see the header.
FACTORY_ID="${FACTORY_ID:-store_input}"
RELAY_ID="${FLEZHA_PERSISTENCE_RELAY:-sp_test_relay}"
PLUGIN_WS_ID="sp_test_ws"
PLUGIN_SINK_ID="sp_test_sink"

PROJECT="${PROJECT:-flezha_state_persistence}"

# The BEAM is started with distribution enabled and `flz` reaches it via
# `flzvm rpc`, which sources `rel/env.sh.eex` and refuses to start
# distribution without a cookie. One constant so the two call sites cannot
# drift apart.
RELEASE_COOKIE_VALUE="flezha_cluster_cookie"

COMPOSE=(
  docker compose
    --project-name "${PROJECT}"
    -f "${DEV_COMPOSE}"
    -f "${TESTING_OVERLAY}"
)

cd "${REPO_ROOT}/docker/dev"

# Every line this script prints carries its tag, so the tag is written twice
# rather than once per message. `diag` is `note`'s stderr twin — diagnostics
# and failures — so a dump always lands next to the message explaining it.
note() {
  echo "[state_persistence] $*"
}

diag() {
  echo "[state_persistence] $*" >&2
}

# The rust runner's tail, prefixed so it cannot be mistaken for this script's
# own output. Every "the runner did not do X" path wants exactly this, and the
# depth is one fact.
dump_rust_logs() {
  docker logs --tail=80 flezha_rust_runner 2>&1 | sed 's/^/  rust> /' >&2 || true
}

# `docker/dev/docker-compose.yml` pins hardcoded container_name values
# (the controller, the broker stack and the rust runner). Those names are global on the Docker daemon —
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
  flezha_rust_runner
)

force_remove_known_containers() {
  for name in "${KNOWN_CONTAINER_NAMES[@]}"; do
    docker rm -f "${name}" >/dev/null 2>&1 || true
  done
}

# Stale UDS socket files from a previous interrupted run linger on
# the host bind-mount (`app_data/sockets/`). The rust container's
# wait-loop watches for `runner_control_channel.sock` to exist and
# would race straight past it on next boot and hand off to a runner
# the controller has not registered. Wipe sockets pre-flight (per
# CLAUDE.md memory: never `rm -rf app_data`, only its subdirs).
clean_stale_sockets() {
  rm -f "${REPO_ROOT}"/app_data/sockets/*.sock 2>/dev/null || true
}

# Wipe persisted state at the START of a fresh run so we don't replay
# data from a previous test. Mid-cycle (between down and up) we MUST
# leave it alone — that's the contract under test.
clean_persisted_state() {
  rm -rf "${REPO_ROOT}"/app_data/state/* 2>/dev/null || true
}

# Fail fast if the rust container has already exited (e.g. a cargo build
# failure). Keeps the script from sitting on a 300s timeout.
assert_rust_running() {
  local state
  state="$(docker inspect -f '{{.State.Status}}' flezha_rust_runner 2>/dev/null || echo missing)"
  if [ "${state}" != "running" ]; then
    diag "FAIL: rust container is not running (state=${state})"
    dump_rust_logs
    exit 1
  fi
}

cleanup() {
  if [ "${FLEZHA_KEEP_STACK:-0}" = "1" ]; then
    note "FLEZHA_KEEP_STACK=1, leaving stack up"
    return
  fi
  note "tearing down compose stack (project=${PROJECT})"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  # Compose 'down' is project-scoped; sweep stragglers by name too so
  # the next run starts clean even if an earlier run was interrupted
  # before the trap fired.
  force_remove_known_containers
}
trap cleanup EXIT

flz() {
  "${COMPOSE[@]}" exec -T -e "RELEASE_COOKIE=${RELEASE_COOKIE_VALUE}" \
    controller _build/dev/rel/flezha/bin/flz "$@"
}

# Run an arbitrary command inside the running controller container.
# Uses `bash` (not the default `/bin/sh`, which is dash on Debian and
# lacks /dev/tcp support — our port-readiness probes need bash).
controller_sh() {
  "${COMPOSE[@]}" exec -T controller bash -c "$1"
}

SOCKETS_DIR="${REPO_ROOT}/app_data/sockets"
STATE_DIR="${REPO_ROOT}/app_data/state"
CONTROL_SOCKET="${SOCKETS_DIR}/runner_control_channel.sock"

# Run one long build inside the controller container, keeping its output out
# of the run log unless it fails. Both builds below are this shape, and the
# tail depth is one fact rather than two that can drift.
build_step() {
  local label="$1" logfile="$2" script="$3"

  note "${label}"
  if ! controller_sh "${script}" >"${logfile}" 2>&1; then
    diag "FAIL: ${label} failed; tail of ${logfile}:"
    tail -40 "${logfile}" >&2
    exit 1
  fi
}

# The regular dev compose does NOT auto-start the BEAM — its controller
# service just keeps the container alive (see `pgrep -f 'beam.smp\|tail'`
# healthcheck in docker-compose.yml). We start the BEAM ourselves here
# in detached mode and poll on the host filesystem for the controller's
# UDS control socket. That is the transport being bound — the precondition for
# every `flz` call below — and not the controller being ready.
start_beam() {
  # `flz` is a release artifact at `_build/dev/rel/flezha/bin/flz`.
  # `mix release` does NOT build it — see `copy_cli_commands` in
  # `mix.exs`, which only copies a pre-built `target/debug/flz`
  # into the release. The host's copy of that file is the one
  # exposed inside the container via the `../../:/flezha` bind mount,
  # and on a host with a newer GLIBC than the toolchain image (e.g.
  # Ubuntu 25.10 GLIBC 2.39 vs Debian 12 GLIBC 2.36) the copied
  # binary will run with `version GLIBC_2.39 not found`.
  #
  # So `flz` is built *inside the container*, and built BEFORE the release:
  # `copy_cli_commands` copies whatever `target/debug/flz` holds, and the
  # compose mounts `/flezha/target` as a named volume that starts empty, so
  # a release run ahead of the cargo build fails outright. Cargo writes to
  # that volume, never to the host's `target/`.
  build_step "building flz CLI inside container (matches container GLIBC)" \
    /tmp/state_persistence_cargo.log \
    'cd /flezha && cargo build -p flz --quiet 2>&1'

  build_step "building the flezha release" \
    /tmp/state_persistence_release.log \
    'cd /flezha && export MIX_ENV=dev && mix deps.get --only "$MIX_ENV" && mix release --overwrite'

  note "release built with the container-linked flz"

  # We can't use `flzvm daemon` (the release entrypoint) because the
  # default release pulls in `wx`, whose NIF needs libGL — absent on
  # the Debian 12 toolchain image — and the BEAM crashes on boot:
  #   Failed to load NIF library: '.../wx-2.4.3/priv/erl_gl.so:
  #   cannot open shared object file: No such file or directory'
  # Excluding wx would mean editing mix.exs releases config, which
  # is out-of-scope for this script.
  #
  # Instead: start BEAM via `mix run --no-halt` (the same way the
  # other Docker test scripts do), but layer in the same node name +
  # cookie the release would use, so `flz` -> `flzvm rpc` (via Erlang
  # distribution) can still reach it. RELEASE_COOKIE / node name
  # come from `rel/env.sh.eex`.
  note "starting BEAM inside controller container ('mix run --no-halt' + distribution)"
  "${COMPOSE[@]}" exec -d -T -e "RELEASE_COOKIE=${RELEASE_COOKIE_VALUE}" \
    -e "ERL_FLAGS=-name flezha@127.0.0.1 -setcookie ${RELEASE_COOKIE_VALUE}" \
    controller sh -c 'cd /flezha && export MIX_ENV=dev && exec mix run --no-halt'

  local deadline=$(( $(date +%s) + REJOIN_TIMEOUT_S ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if [ -S "${CONTROL_SOCKET}" ]; then
      note "BEAM is up — control socket present at ${CONTROL_SOCKET}"
      return 0
    fi
    sleep 2
  done
  diag "timed out waiting for BEAM control socket"
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
      diag "still waiting for handshake (rust last log line: $(docker logs --tail=1 flezha_rust_runner 2>&1 | tr -d '\r' | tail -1))"
      last_diag="${now}"
    fi
    sleep 5
  done
  diag "timed out waiting for runner_id"
  dump_rust_logs
  return 1
}

# Poll a `flz` read command until its output contains `needle`. Every runtime
# assertion below is this shape: what the CLI reports is updated
# asynchronously w.r.t. the RPC that requested the change, so a single read is
# a race, and a failure has to show what the command actually
# said.
#
# The output is CAPTURED and the SAME text is both matched and dumped. The
# assertions this replaced discarded stderr, threw away what they matched
# against, and then re-ran the command to build the failure message — so a
# failure could (and did) print a dump containing the very plugin it had just
# reported missing, with nothing to say whether the poll had seen different
# output or no output at all. Reporting `flz`'s exit code alongside the text
# is what separates "the CLI said no" from "the CLI failed".
await_flz_output() {
  local what="$1" needle="$2" budget_s="$3"
  shift 3
  local started deadline out="" rc=0
  started="$(date +%s)"
  deadline=$(( started + budget_s ))

  while [ "$(date +%s)" -lt "${deadline}" ]; do
    rc=0
    out="$(flz "$@" 2>&1)" || rc=$?

    if [ "${rc}" -eq 0 ]; then
      case "${out}" in
        *"${needle}"*)
          note "${what} (after $(( $(date +%s) - started ))s)"
          return 0
          ;;
      esac
    fi

    sleep 1
  done

  diag "FAIL: ${what} — '${needle}' absent after ${budget_s}s (flz exit=${rc})"
  printf '%s\n' "${out}" | sed 's/^/  flz> /' >&2
  return 1
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
"${COMPOSE[@]}" up -d controller rust

start_beam

note "waiting for initial runner handshake"
INITIAL_CE_ID="$(wait_for_any_ce_id)"
note "initial runner_id: ${INITIAL_CE_ID}"

# --- Phase 1: start relay + attach plugins --------------------------------

note "starting relay ${RELAY_ID}"
flz relay start "${RELAY_ID}" "${FACTORY_ID}" \
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
  # Avoid `pkill -f`: with `bash -c` running the wrapper script, the
  # pattern string lives in the bash argv, so `pkill -f "..."` would
  # kill the parent bash (exit 137 silently). The port-check loop
  # below tolerates a prior listener; if 4500 is already bound, we
  # treat it as healthy and proceed.
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

# `--X…` flags are PROVIDER options and reach `init_provider/1`; a provider
# with a strict allow-list refuses every key it does not own. Attachment
# (`--relay_id`) is a plain CLI option and `--hook` names what the plugin
# sends; the enable refuses any other plain option. The websocket plugin is a
# source here, so it has no hook.
note "enabling websocket plugin ${PLUGIN_WS_ID}"
flz plugin enable "${PLUGIN_WS_ID}" relay_websocket \
  --relay_id="${RELAY_ID}" \
  --Xuri="ws://localhost:4500" \
  --Xpost_connect='{"payload":{"type":"text","message":"{\"event_type\":\"subscribe\",\"event_data\":{\"default_message\":\"hello world\",\"interval\":500}}"}}'

note "enabling file sink plugin ${PLUGIN_SINK_ID}"
flz plugin enable "${PLUGIN_SINK_ID}" relay_file_sink \
  --relay_id="${RELAY_ID}" \
  --hook=on_output \
  --Xpath="${RELAY_ID}.test.ndjson"

# Let the WS plugin drive inputs into the relay so apply_change runs
# and the WAL/outbox actually grow before we make the assertions.
note "letting testnet drive inputs for ${INPUT_WAIT_S}s so WAL/snapshots fill"
sleep "${INPUT_WAIT_S}"

# The runtime, not the RPC return codes: `flz plugin enable` answers as soon
# as the cspec is persisted, so only these reads prove the relay and its
# plugins are actually live. `set -e` already aborted on a failed enable.
await_flz_output "relay '${RELAY_ID}' is listed" "${RELAY_ID}" 30 \
  relay list -n "${DEMO_NAMESPACE}"

await_flz_output "plugin '${PLUGIN_WS_ID}' is in 'flz relay info'" "${PLUGIN_WS_ID}" 30 \
  relay info -n "${DEMO_NAMESPACE}"

await_flz_output "'flz state list ${RELAY_ID}' reports cells" '"cell_key"' 30 \
  state list "${RELAY_ID}" --output json

# The namespaced layout itself, not merely "some file exists": a WAL under
# `<state_root>/<namespace>/<relay_class>/<tasklet_id>/<key>.wal` is
# what `Flezha.State.Paths.class_dir/1` promises, and asserting the shape
# is what catches the layout moving out from under the contract.
CLASS_DIR="${STATE_DIR}/${DEMO_NAMESPACE}/${RELAY_ID}"
note "verifying the namespaced WAL layout under ${CLASS_DIR}"
mapfile -t WAL_FILES < <(find "${CLASS_DIR}" -mindepth 2 -maxdepth 2 -type f -name '*.wal' 2>/dev/null)
if [ "${#WAL_FILES[@]}" -eq 0 ]; then
  diag "FAIL: no <tasklet_id>/<key>.wal under ${CLASS_DIR} — persistence not engaged"
  find "${STATE_DIR}" 2>/dev/null | sed 's/^/  /' >&2
  exit 1
fi
note "${#WAL_FILES[@]} WAL file(s) in the expected layout:"
printf '  %s\n' "${WAL_FILES[@]}"

note "PASS: per-Cell state persistence is engaged"
note "  relay='${RELAY_ID}' (factory='${FACTORY_ID}') in namespace '${DEMO_NAMESPACE}'"
note "  plugins enabled=${PLUGIN_WS_ID},${PLUGIN_SINK_ID} (presence asserted for ${PLUGIN_WS_ID})"
note ""
note "Scope notes:"
note "  * This script verifies the data-plane WAL is being written under the"
note "    namespaced layout and that 'flz state list' surfaces the live cell."
note "  * It does NOT exercise a controller-side BEAM restart. The controller"
note "    does not auto-recreate relays after its own BEAM restarts (only"
note "    SDK/runner restarts trigger the SDK-side RestoreInitState handshake"
note "    while the controller stays up). Follow-up testing of runner-only"
note "    restart belongs alongside test/scripts/plugin_persistence.sh."
