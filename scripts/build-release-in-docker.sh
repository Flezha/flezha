#!/usr/bin/env bash
# Build the dev `mix release` inside the dev controller container so
# `_build/dev/rel/flezha/bin/flz` (and `flzvm`) exist for subsequent
# CLI calls — `flz` is a release artifact, not something `mix run`
# produces, so anything that shells out to `flz` (e.g.
# `test/scripts/state_persistence.sh`) needs this run at least once
# against a fresh `flezha_build` named volume.
#
# Uses the regular `docker/dev/docker-compose.yml`. Brings up *only*
# the controller container (no kafka/zookeeper/rust needed for a
# release build), runs `mix release`, then leaves the container up
# so a follow-up script can re-use it. Pass DOWN_AFTER=1 to tear it
# down on exit.
#
# Usage:
#   scripts/build-release-in-docker.sh
#   DOWN_AFTER=1 scripts/build-release-in-docker.sh
#   PROJECT=flezha_state_persistence scripts/build-release-in-docker.sh
#       # match the project name a sibling test script will use,
#       # so both share the same `flezha_build` named volume.
#   MIX_ENV=prod scripts/build-release-in-docker.sh
#       # build the prod release instead of dev.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEV_COMPOSE="${REPO_ROOT}/docker/dev/docker-compose.yml"

PROJECT="${PROJECT:-flezha_dev}"
MIX_ENV="${MIX_ENV:-dev}"

COMPOSE=(
  docker compose
    --project-name "${PROJECT}"
    -f "${DEV_COMPOSE}"
)

note() {
  echo "[build-release] $*"
}

# `docker/dev/docker-compose.yml` pins hardcoded container_name values
# that are global on the Docker daemon. A leftover from a prior
# (interrupted) run, or from a different compose project name, will
# collide on `up` with "container name already in use". Compose 'down'
# is project-scoped and won't sweep them, so we force-remove by name.
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

cleanup() {
  if [ "${DOWN_AFTER:-0}" = "1" ]; then
    note "tearing down controller container (DOWN_AFTER=1)"
    "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
    force_remove_known_containers
  else
    note "leaving controller container up — re-use this project name"
    note "  (PROJECT=${PROJECT}) for any follow-up script that needs"
    note "  the same flezha_build volume; or pass DOWN_AFTER=1 to clean."
  fi
}
trap cleanup EXIT

note "ensuring flezha_network exists"
docker network inspect flezha_network >/dev/null 2>&1 \
  || docker network create flezha_network >/dev/null

note "sweeping any leftover containers with hardcoded names from a prior run"
force_remove_known_containers

note "bringing up controller container (project=${PROJECT})"
"${COMPOSE[@]}" up -d controller

note "running 'mix deps.get' (only :${MIX_ENV})"
"${COMPOSE[@]}" exec -T controller sh -c \
  "cd /flezha && export MIX_ENV=${MIX_ENV} && mix deps.get --only \$MIX_ENV"

note "running 'mix release' for MIX_ENV=${MIX_ENV}"
"${COMPOSE[@]}" exec -T controller sh -c \
  "cd /flezha && export MIX_ENV=${MIX_ENV} && mix release --overwrite"

note "verifying release artifacts on bind-mounted host filesystem"
REL_BIN="${REPO_ROOT}/_build/${MIX_ENV}/rel/flezha/bin/flz"
if [ -x "${REL_BIN}" ]; then
  note "OK: ${REL_BIN#${REPO_ROOT}/} is present and executable"
else
  # _build is a named volume in the dev compose, so the release may
  # not surface on the host. Verify inside the container instead.
  note "release not visible on host (named volume); verifying inside container"
  if "${COMPOSE[@]}" exec -T controller test -x "/flezha/_build/${MIX_ENV}/rel/flezha/bin/flz"; then
    note "OK: /flezha/_build/${MIX_ENV}/rel/flezha/bin/flz is present in container"
  else
    echo "[build-release] FAIL: release binary not found inside container" >&2
    exit 1
  fi
fi

note "PASS: ${MIX_ENV} release built. flz CLI is ready for follow-up scripts."
