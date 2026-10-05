#!/usr/bin/env bash
# Default entrypoint for flezha/flezha:latest (prod runtime) and
# flezha/flezha:latest-dev (runtime + testnet — same script, the
# testnet branch only fires when FLEZHA_TESTNET_WS_PORT is set and the
# pre-built bundle is present).
#
# Responsibilities:
#   1. Optionally launch the testnet WS server.
#   2. When FLEZHA_PLUGINS is set, even empty, make the plugin root hold exactly
#      the packages it declares (`flz plugin sync`); a failed sync exits
#      non-zero and starts nothing.
#   3. Start the controller (bin/flzvm start) in the background, wait for it
#      to bind its control socket, then wait for it to report ready.
#   4. Trap SIGTERM/SIGINT and delegate to `flz controller stop`, which
#      drives the BEAM's shutdown coordinator.
#
# The controller recovers its own relays and plugins on boot from
# ProcessHub's durable registry and the per-Cell WAL under
# FLEZHA_PROCESS_HUB_REGISTRY_ROOT and FLEZHA_STATE_DIR. This script
# neither writes nor reads any cluster-wide snapshot file.

set -uo pipefail

log() { printf '%s graceful-restart: %s\n' "$(date -u +%FT%TZ)" "$*"; }

SOCKET_DIR="${FLEZHA_SOCKET_FILES_PATH:-/flezha/app_data/sockets}"
READY_WAIT_TIMEOUT_S="${FLEZHA_RESTORE_WAIT_TIMEOUT_S:-60}"
FLZ="${FLZ:-/flezha/bin/flz}"

# 1) Optional testnet WS server. The :latest image does not ship the
#    bundle; the :latest-dev image does. Either way, only start it when
#    FLEZHA_TESTNET_WS_PORT is explicitly set.
TESTNET_PID=""
if [ -n "${FLEZHA_TESTNET_WS_PORT:-}" ] && [ -f /flezha/testnet/dist/index.js ]; then
  log "starting testnet WS server on port ${FLEZHA_TESTNET_WS_PORT}"
  ( cd /flezha/testnet && tail -f /dev/null | node dist/index.js ) &
  TESTNET_PID=$!
fi

CONTROL_SOCKET="$SOCKET_DIR/runner_control_channel.sock"

# Remove the controller's stale control socket from any previous container that
# wrote to this same named volume. Without this, the transport wait below
# matches the leftover file before the new BEAM has bound to it. Only the
# controller's own sockets are touched — SDK-owned sockets (e.g.
# primary_stream_*) are held open by external processes and must not be removed.
if [ -e "$CONTROL_SOCKET" ]; then
  rm -f "$CONTROL_SOCKET"
fi

# 2) The declared plugin list, before the controller that loads it starts.
if [ -n "${FLEZHA_PLUGINS+x}" ]; then
  log "syncing the plugin root to FLEZHA_PLUGINS"
  if ! "$FLZ" plugin sync; then
    log "ERROR flz plugin sync failed; the controller is not started"
    exit 1
  fi
fi

# 3) Start the controller.
/flezha/bin/flzvm start &
AGENT_PID=$!

# Wait for the BEAM to bind its control socket. This is the transport being
# bound, not the controller being ready: it appears before any hub has replayed
# anything. It is the precondition for issuing the readiness RPC below. Cap the
# wait at FLEZHA_RESTORE_WAIT_TIMEOUT_S so a wedged or refusing BEAM doesn't
# hang the boot indefinitely.
log "waiting up to ${READY_WAIT_TIMEOUT_S}s for controller control socket at $CONTROL_SOCKET"
ready_deadline=$(( $(date +%s) + READY_WAIT_TIMEOUT_S ))
while [ "$(date +%s)" -lt "$ready_deadline" ]; do
  if [ -S "$CONTROL_SOCKET" ]; then
    break
  fi
  if ! kill -0 "$AGENT_PID" 2>/dev/null; then
    log "ERROR controller process exited before opening control socket"
    exit 1
  fi
  sleep 1
done
if [ ! -S "$CONTROL_SOCKET" ]; then
  log "ERROR controller control socket did not appear within ${READY_WAIT_TIMEOUT_S}s"
  exit 1
fi

# Now the real readiness wait: the boot sequence complete, every namespace hub
# out of the recovery state machine, the durable registry reloaded. `flz boot
# await` blocks on that verdict and names the precondition still unmet when it
# gives up.
remaining=$(( ready_deadline - $(date +%s) ))
[ "$remaining" -lt 1 ] && remaining=1
log "waiting up to ${remaining}s for the controller to report ready"
if ! "$FLZ" boot await --timeout "$remaining"; then
  log "ERROR controller did not report ready within ${remaining}s"
  exit 1
fi
log "controller ready"

# 4) SIGTERM/SIGINT trap.
#
# Default branch (FLEZHA_GRACEFUL_SHUTDOWN_ENABLED unset/truthy):
#   delegate to `flz controller stop`, which drives Flezha.Shutdown.Coordinator
#   inside the BEAM (quiesce → drain via HotSwap → finalise).
#
# Disabled branch (FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false/0/no/off):
#   SIGTERM the agent directly and let kernel's erl_signal_handler take it
#   down, which is what the same flag means inside the BEAM.
GRACEFUL_TIMEOUT_MS="${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS:-60000}"

on_term() {
  case "${FLEZHA_GRACEFUL_SHUTDOWN_ENABLED:-true}" in
    false|0|no|off|FALSE|NO|OFF)
      log "SIGTERM received; graceful shutdown disabled, terminating the agent directly"
      if [ -n "$TESTNET_PID" ] && kill -0 "$TESTNET_PID" 2>/dev/null; then
        kill -TERM "$TESTNET_PID" 2>/dev/null || true
      fi
      if kill -0 "$AGENT_PID" 2>/dev/null; then
        kill -TERM "$AGENT_PID" 2>/dev/null || true
      fi
      ;;
    *)
      log "SIGTERM received; delegating to flz controller stop --timeout-ms ${GRACEFUL_TIMEOUT_MS}"
      "$FLZ" controller stop --timeout-ms "$GRACEFUL_TIMEOUT_MS" || \
        log "flz controller stop exited non-zero; BEAM will be forcibly terminated"
      if [ -n "$TESTNET_PID" ] && kill -0 "$TESTNET_PID" 2>/dev/null; then
        kill -TERM "$TESTNET_PID" 2>/dev/null || true
      fi
      ;;
  esac
}
trap on_term TERM INT

# wait returns 128+signo when interrupted; that's not a failure for us.
wait "$AGENT_PID"
exit $?
