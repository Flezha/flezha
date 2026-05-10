#!/usr/bin/env bash
# Default entrypoint for alfetahe/flezha:latest (prod runtime) and
# alfetahe/flezha:latest-dev (runtime + testnet — same script, the
# testnet branch only fires when FLEZHA_TESTNET_WS_PORT is set and the
# pre-built bundle is present).
#
# Responsibilities:
#   1. Optionally launch the testnet WS server.
#   2. Start the controller (bin/flzvm start) in the background.
#   3. Optionally wait for an external SDK to attach, then run
#      `flz cluster restore` if a snapshot file is present.
#   4. Trap SIGTERM/SIGINT and run snapshot-on-stop.sh before exiting,
#      so docker/compose stops still write a snapshot. (In Kubernetes
#      lifecycle.preStop already runs the same script — this trap is
#      a belt-and-braces fallback; the script is idempotent.)

set -uo pipefail

log() { printf '%s graceful-restart: %s\n' "$(date -u +%FT%TZ)" "$*"; }

SOCKET_DIR="${FLEZHA_SOCKET_FILES_PATH:-/flezha/app_data/sockets}"
SNAPSHOT_DIR="${FLEZHA_SNAPSHOT_DIR:-/flezha/app_data/snapshots}"
SNAPSHOT_FILE="${SNAPSHOT_DIR}/latest.flz"
STALENESS_THRESHOLD_S="${FLEZHA_SNAPSHOT_STALENESS_THRESHOLD_S:-86400}"
SNAPSHOT_PEER_TIMEOUT_MS="${FLEZHA_SNAPSHOT_PEER_TIMEOUT_MS:-5000}"
RESTORE_WAIT_TIMEOUT_S="${FLEZHA_RESTORE_WAIT_TIMEOUT_S:-60}"
RESTORE_WAIT_GLOB="${FLEZHA_RESTORE_WAIT_SOCKET_GLOB:-}"
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

CONTROL_SOCKET="$SOCKET_DIR/executor_control_channel.sock"

# Remove the controller's stale control socket from any previous container that
# wrote to this same named volume. Without this, the readiness wait below
# matches the leftover file before the new BEAM has bound to it, and
# `flz cluster restore` then fails with :noconnection. Only the controller's
# own sockets are touched — SDK-owned sockets (e.g. primary_stream_*) are
# held open by external processes and must not be removed.
if [ -e "$CONTROL_SOCKET" ]; then
  rm -f "$CONTROL_SOCKET"
fi

# 2) Start the controller.
/flezha/bin/flzvm start &
AGENT_PID=$!

# Wait for the BEAM to open its control socket. This is the "controller is
# ready" signal — without it, `flz cluster restore` (which RPCs into
# the controller) fails with :noconnection. Same readiness signal the
# HEALTHCHECK keys on. Cap the wait at FLEZHA_RESTORE_WAIT_TIMEOUT_S
# so a wedged BEAM doesn't hang the boot indefinitely.
log "waiting up to ${RESTORE_WAIT_TIMEOUT_S}s for controller control socket at $CONTROL_SOCKET"
ready_deadline=$(( $(date +%s) + RESTORE_WAIT_TIMEOUT_S ))
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
  log "ERROR controller control socket did not appear within ${RESTORE_WAIT_TIMEOUT_S}s"
  exit 1
fi

# 3) Optional restore lifecycle.
case "${FLEZHA_SNAPSHOT_ENABLED:-true}" in
  false|0|no|off|FALSE|NO|OFF)
    log "restore disabled (FLEZHA_SNAPSHOT_ENABLED=${FLEZHA_SNAPSHOT_ENABLED})"
    ;;
  *)
    if [ -f "$SNAPSHOT_FILE" ]; then
      if [ -n "$RESTORE_WAIT_GLOB" ]; then
        log "waiting up to ${RESTORE_WAIT_TIMEOUT_S}s for SDK socket matching ${SOCKET_DIR}/${RESTORE_WAIT_GLOB}"
        ce_deadline=$(( $(date +%s) + RESTORE_WAIT_TIMEOUT_S ))
        while [ "$(date +%s)" -lt "$ce_deadline" ]; do
          if ls "$SOCKET_DIR"/${RESTORE_WAIT_GLOB} >/dev/null 2>&1; then
            break
          fi
          if ! kill -0 "$AGENT_PID" 2>/dev/null; then
            log "ERROR controller process exited before SDK connected"
            exit 1
          fi
          sleep 1
        done
        if ! ls "$SOCKET_DIR"/${RESTORE_WAIT_GLOB} >/dev/null 2>&1; then
          log "ERROR no SDK socket matching ${RESTORE_WAIT_GLOB} after ${RESTORE_WAIT_TIMEOUT_S}s; cannot restore"
          exit 1
        fi
      fi
      log "restoring from $SNAPSHOT_FILE (warn-if-older-than ${STALENESS_THRESHOLD_S}s)"
      if ! "$FLZ" cluster restore "$SNAPSHOT_FILE" \
            --warn-if-older-than "${STALENESS_THRESHOLD_S}s" \
            --peer-timeout-ms "$SNAPSHOT_PEER_TIMEOUT_MS"; then
        log "ERROR restore failed for $SNAPSHOT_FILE"
        exit 1
      fi
      log "restore complete"
    else
      log "no snapshot file at $SNAPSHOT_FILE, cold start"
    fi
    ;;
esac

# 4) SIGTERM/SIGINT trap.
#
# Default branch (FLEZHA_GRACEFUL_SHUTDOWN_ENABLED unset/truthy):
#   delegate to `flz controller stop`, which drives Flezha.Shutdown.Coordinator
#   inside the BEAM (quiesce → drain via HotSwap → finalise).
#   `snapshot-on-stop.sh` is NOT invoked — HotSwap migration replaces
#   snapshot-driven restart-survival for steady-state Relays.
#
# Disabled branch (FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false/0/no/off):
#   keep today's behaviour — write a snapshot, then SIGTERM the agent.
GRACEFUL_TIMEOUT_MS="${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS:-60000}"

on_term() {
  case "${FLEZHA_GRACEFUL_SHUTDOWN_ENABLED:-true}" in
    false|0|no|off|FALSE|NO|OFF)
      log "SIGTERM received; graceful shutdown disabled, running snapshot-on-stop.sh"
      if [ -x /usr/local/bin/snapshot-on-stop.sh ]; then
        /usr/local/bin/snapshot-on-stop.sh || log "snapshot-on-stop.sh exited non-zero"
      fi
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
