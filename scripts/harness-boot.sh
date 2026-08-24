#!/bin/sh
# Shared entrypoint for the chaos and dev-healing compose stacks: make the
# mounted paths usable, then exec the service command.
#
# `docker/dev/Dockerfile.toolchain` creates every named-volume mountpoint
# owned by uid 1000, and Docker initialises a fresh volume from the image
# directory, ownership included — so the normal path needs no privileges.
# Two cases remain:
#
#   * Started as root (FLEZHA_CONTAINER_USER=0:0, for a host where the
#     image-supplied ownership does not apply, e.g. userns-remap): chown
#     only the mountpoints whose top-level owner is wrong, then re-exec
#     unprivileged. setpriv replaces the process image, so the workload
#     keeps PID 1 and receives SIGTERM directly. `su` would answer SIGTERM
#     by reaping its child seconds later, truncating the controller's
#     graceful drain.
#
#   * A mountpoint that is not writable: fail here with the remedy, rather
#     than surfacing as a permission error inside cargo or a handshake
#     timeout. The remedy differs by mount kind, so name both.
#
# FLEZHA_HARNESS_PATHS         space-separated mountpoints to guard
# FLEZHA_HARNESS_WAIT_SOCKET   block until this UDS path exists
# MIX_DEPS_PATH                set on services that run mix; also selects
#                              the lock-guarded dependency fetch below

set -e

uid=1000
paths="${FLEZHA_HARNESS_PATHS:-}"

if [ "$(id -u)" = 0 ]; then
  for d in $paths; do
    mkdir -p "$d"
    [ "$(stat -c %u "$d")" = "$uid" ] || chown -R "$uid:$uid" "$d"
  done
  exec setpriv --reuid="$uid" --regid="$uid" --init-groups "$0" "$@"
fi

for d in $paths; do
  if [ ! -w "$d" ]; then
    echo "[harness-boot] $d is not writable by uid $(id -u)." >&2
    echo "[harness-boot] A named-volume mountpoint comes from the toolchain" >&2
    echo "[harness-boot]   image — rebuild it: make docker-build-toolchain" >&2
    echo "[harness-boot] A bind-mounted path comes from the host — start this" >&2
    echo "[harness-boot]   service as root instead: FLEZHA_CONTAINER_USER=0:0" >&2
    exit 1
  fi
done

# Fetch deps only when the lock file moved. The stamp lives in the deps
# volume it describes, so dropping that volume — the only case needing a
# re-fetch — drops the stamp with it. Written only after a successful
# fetch, so an interrupted one retries on the next boot.
if [ -n "${MIX_DEPS_PATH:-}" ]; then
  lock="$(md5sum /flezha/mix.lock | cut -d' ' -f1)"
  stamp="$MIX_DEPS_PATH/.lock_hash"
  if [ "$(cat "$stamp" 2>/dev/null)" != "$lock" ]; then
    mix deps.get --only "$MIX_ENV" >/dev/null
    printf '%s' "$lock" >"$stamp"
  fi
fi

# A runner cannot connect before the controller has bound its control
# channel, and compose's `service_healthy` only gates the container start.
if [ -n "${FLEZHA_HARNESS_WAIT_SOCKET:-}" ]; then
  while [ ! -S "$FLEZHA_HARNESS_WAIT_SOCKET" ]; do
    echo "[harness-boot] waiting for $FLEZHA_HARNESS_WAIT_SOCKET..."
    sleep 1
  done
fi

exec "$@"
