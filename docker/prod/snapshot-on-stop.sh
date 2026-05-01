#!/usr/bin/env bash
# Pre-stop hook for the flezha agent container. Writes a cluster snapshot
# atomically (tmp + rename) so a graceful stop preserves processor + plugin
# state for the next start. Invoked from two places:
#   1. agent-entrypoint.sh's SIGTERM/SIGINT trap (docker stop, compose down).
#   2. Kubernetes lifecycle.preStop.exec.command on the agent's pod.
#
# Exit 0 on success and on FLEZHA_SNAPSHOT_ENABLED=false (a no-op log).
# Exit non-zero only when an attempted snapshot actually failed — the
# orchestrator surfaces the failure rather than masking torn state.

set -uo pipefail

SNAPSHOT_DIR="${FLEZHA_SNAPSHOT_DIR:-/flezha/app_data/snapshots}"
SNAPSHOT_FILE="${SNAPSHOT_DIR}/latest.flz"
SNAPSHOT_TMP="${SNAPSHOT_FILE}.tmp"
PEER_TIMEOUT_MS="${FLEZHA_SNAPSHOT_PEER_TIMEOUT_MS:-5000}"
FLZ="${FLZ:-/flezha/bin/flz}"

log() { printf '%s graceful-restart: %s\n' "$(date -u +%FT%TZ)" "$*"; }

case "${FLEZHA_SNAPSHOT_ENABLED:-true}" in
  false|0|no|off|FALSE|NO|OFF)
    log "snapshot disabled (FLEZHA_SNAPSHOT_ENABLED=${FLEZHA_SNAPSHOT_ENABLED}); leaving ${SNAPSHOT_FILE} untouched"
    exit 0
    ;;
esac

mkdir -p "$SNAPSHOT_DIR"

log "writing snapshot to ${SNAPSHOT_TMP} (peer-timeout-ms=${PEER_TIMEOUT_MS})"
if "$FLZ" cluster snapshot "$SNAPSHOT_TMP" --peer-timeout-ms "$PEER_TIMEOUT_MS"; then
  if mv -f "$SNAPSHOT_TMP" "$SNAPSHOT_FILE"; then
    log "snapshot written to ${SNAPSHOT_FILE}"
    exit 0
  fi
  log "ERROR: failed to rename ${SNAPSHOT_TMP} -> ${SNAPSHOT_FILE}"
  rm -f "$SNAPSHOT_TMP"
  exit 1
fi

log "ERROR: flz cluster snapshot failed; leaving previous ${SNAPSHOT_FILE} in place"
rm -f "$SNAPSHOT_TMP"
exit 1
