#!/usr/bin/env bash
# Prod-image validation for the durability posture and the platform-store
# scope (`durability-tier-default-on`).
#
# Three things can only be shown against a real production release, and this
# script shows them in throwaway containers:
#
#   1. A prod boot with FLEZHA_DURABILITY_BACKEND unset starts, warns naming
#      both valid moves, and reports the same local-only posture an explicit
#      `none` does. The warning keys off `config_env() == :prod`, so no
#      `mix test` can reach it.
#   2. The explicit opt-out boots inert and reports the local-only posture on
#      `/healthz` and `flz controller status`.
#   3. A controller that dies WITH its disk comes back: on a fresh container
#      with an empty storage and the same tier storage, the parked DLQ record,
#      the authz state (token still authenticates, bootstrap already claimed)
#      and the schedule definitions are all back — and the control-plane stores
#      hydrate before the hubs start, which only a real boot can prove.
#
# Cell WAL recovery is NOT re-proved here; that is
# `test/system/remote_durability_e2e_test.exs`, which destroys the state
# directory of a live steps relay and a live checkpoint stream against real
# runners.
#
# Usage:
#   test/scripts/durability_posture.sh                 # local_path + postgres
#   BACKENDS=local_path test/scripts/durability_posture.sh
#   FLEZHA_KEEP_STACK=1 test/scripts/durability_posture.sh   # skip teardown
#
# Pre-req: the prod image. Built here when absent:
#   VERSION=durability-validation make docker-build

set -uo pipefail

IMAGE="${FLEZHA_IMAGE:-flezha/flezha:durability-validation}"
BACKENDS="${BACKENDS-local_path postgres}"
# The release refuses to start Erlang distribution without one, and every `flz`
# verb reaches the BEAM over distribution.
COOKIE="${RELEASE_COOKIE:-flz-durability-validation}"
PREFIX="flz_durval"
NET="${PREFIX}_net"
PG="${PREFIX}_postgres"
HEALTHZ_PORT="${HEALTHZ_PORT:-19569}"
READY_TIMEOUT_S="${READY_TIMEOUT_S:-120}"
LOGDIR="$(mktemp -d "${TMPDIR:-/tmp}/${PREFIX}_logs.XXXXXX")"
# The tier storage is a host bind mount rather than a named volume: a named
# volume is created root-owned at mount time and the image runs as an
# unprivileged user, so every ship would fail with EACCES.
TIERDIR="$(mktemp -d "${TMPDIR:-/tmp}/${PREFIX}_tier.XXXXXX")"
chmod 0777 "$TIERDIR"

FAILURES=0

log() { printf '\n=== %s\n' "$*"; }
pass() { printf '  PASS  %s\n' "$*"; }
fail() {
  printf '  FAIL  %s\n' "$*"
  FAILURES=$((FAILURES + 1))
}

check() { # check <description> <file> <pattern>
  if grep -qF -- "$3" "$2"; then pass "$1"; else
    fail "$1 (no '$3' in $2)"
    sed -n '1,40p' "$2" | sed 's/^/        /'
  fi
}

refute() { # refute <description> <file> <pattern>
  if grep -qF -- "$3" "$2"; then
    fail "$1 (unexpected '$3' in $2)"
  else pass "$1"; fi
}

# ---------- lifecycle ----------

teardown() {
  if [ "${FLEZHA_KEEP_STACK:-0}" = "1" ]; then
    log "FLEZHA_KEEP_STACK=1 — leaving containers, volumes and image in place"
    return
  fi

  log "teardown"
  docker rm -f "$PG" >/dev/null 2>&1
  for c in $(docker ps -aq --filter "name=^${PREFIX}_" 2>/dev/null); do
    docker rm -f "$c" >/dev/null 2>&1
  done
  for v in $(docker volume ls -q --filter "name=^${PREFIX}_" 2>/dev/null); do
    docker volume rm -f "$v" >/dev/null 2>&1
  done
  docker network rm "$NET" >/dev/null 2>&1
  docker image rm -f "$IMAGE" >/dev/null 2>&1
  rm -rf "$LOGDIR" "$TIERDIR"
}
trap teardown EXIT

ensure_image() {
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "building $IMAGE"
    VERSION="${IMAGE##*:}" make -C "$(cd "$(dirname "$0")/../.." && pwd)" docker-build \
      >"$LOGDIR/build.log" 2>&1 ||
      {
        fail "image build"
        exit 1
      }
  fi
  pass "prod image $IMAGE"
}

# The controller's own readiness verdict — the same one the image HEALTHCHECK
# and the entrypoint wait on. A bound control socket is NOT it: it appears
# before any hub has replayed anything.
await_ready() { # await_ready <container>
  local deadline=$(($(date +%s) + READY_TIMEOUT_S))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if docker exec "$1" /flezha/bin/flz boot await --timeout 0 >/dev/null 2>&1; then
      return 0
    fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" != "true" ]; then
      return 1
    fi
    sleep 2
  done
  return 1
}

flz() { # flz <container> <args...>
  local c="$1"
  shift
  docker exec "$c" /flezha/bin/flz "$@"
}

# `rpc` prints nothing on its own, so every expression reports its own result.
rpc() { # rpc <container> <elixir>
  docker exec "$1" /flezha/bin/flzvm rpc "$2 |> IO.inspect()"
}

# ---------- 1. boot posture ----------

boot_posture() {
  log "boot posture: an unset backend warns and boots"
  docker run -d --name "${PREFIX}_unset" \
    -e FLEZHA_HEALTHZ_ENABLED=true \
    -e RELEASE_COOKIE="$COOKIE" \
    "$IMAGE" >/dev/null 2>&1

  if await_ready "${PREFIX}_unset"; then pass "booted with no durability decision"; else
    fail "an unset backend did not boot"
  fi

  docker logs "${PREFIX}_unset" >"$LOGDIR/unset.log" 2>&1
  check "names the variable" "$LOGDIR/unset.log" "FLEZHA_DURABILITY_BACKEND is not set"
  check "names the backend move" "$LOGDIR/unset.log" "FLEZHA_DURABILITY_BACKEND=postgres"
  check "names the opt-out move" "$LOGDIR/unset.log" "FLEZHA_DURABILITY_BACKEND=none"

  # The point of turning the refusal into a warning: the node that made no
  # decision must still be findable by whatever a fleet audit reads.
  flz "${PREFIX}_unset" controller status >"$LOGDIR/unset-status.log" 2>&1
  check "a defaulted posture reads local_only" "$LOGDIR/unset-status.log" \
    "durability_posture:            local_only"

  docker rm -f "${PREFIX}_unset" >/dev/null 2>&1

  log "boot posture: the explicit opt-out boots inert and says so"
  docker run -d --name "${PREFIX}_none" \
    -e FLEZHA_DURABILITY_BACKEND=none \
    -e FLEZHA_HEALTHZ_ENABLED=true \
    -e RELEASE_COOKIE="$COOKIE" \
    -p "${HEALTHZ_PORT}:9569" \
    "$IMAGE" >/dev/null 2>&1

  if await_ready "${PREFIX}_none"; then pass "booted with backend=none"; else
    fail "the explicit opt-out did not boot"
    docker logs "${PREFIX}_none" >"$LOGDIR/none.log" 2>&1
    sed -n '1,40p' "$LOGDIR/none.log" | sed 's/^/        /'
    return
  fi

  flz "${PREFIX}_none" controller status >"$LOGDIR/none-status.log" 2>&1
  check "flz controller status" "$LOGDIR/none-status.log" "durability_posture:            local_only"

  curl -sS "http://127.0.0.1:${HEALTHZ_PORT}/healthz" >"$LOGDIR/none-healthz.log" 2>&1
  check "/healthz" "$LOGDIR/none-healthz.log" '"durability":"local_only"'

  docker rm -f "${PREFIX}_none" >/dev/null 2>&1
}

# ---------- 2. disk death ----------

start_postgres() {
  docker rm -f "$PG" >/dev/null 2>&1
  docker run -d --name "$PG" --network "$NET" \
    -e POSTGRES_USER=flezha -e POSTGRES_PASSWORD=flezha_test -e POSTGRES_DB=flezha \
    postgres:16.15-alpine >/dev/null 2>&1

  local deadline=$(($(date +%s) + 60))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if docker exec "$PG" pg_isready -U flezha >/dev/null 2>&1; then
      pass "throwaway postgres ready"
      return 0
    fi
    sleep 2
  done
  fail "postgres never became ready"
  return 1
}

backend_env() { # backend_env <backend>
  case "$1" in
    local_path)
      printf -- '-e FLEZHA_DURABILITY_BACKEND=local_path -e FLEZHA_DURABILITY_PATH=/tier'
      ;;
    postgres)
      printf -- '-e FLEZHA_DURABILITY_BACKEND=postgres -e FLEZHA_DURABILITY_PG_HOST=%s' "$PG"
      printf -- ' -e FLEZHA_DURABILITY_PG_DATABASE=flezha -e FLEZHA_DURABILITY_PG_USERNAME=flezha'
      printf -- ' -e FLEZHA_DURABILITY_PG_PASSWORD_ENV=PGPASS -e PGPASS=flezha_test'
      ;;
  esac
}

# One controller lifetime. storage lives INSIDE the container so `docker rm -f`
# destroys it exactly as a dying disk would; only /tier survives.
start_controller() { # start_controller <name> <backend> <writer-id>
  # shellcheck disable=SC2046 # the env fragment is intentionally word-split
  docker run -d --name "$1" --network "$NET" \
    -v "$TIERDIR:/tier" \
    $(backend_env "$2") \
    -e FLEZHA_DURABILITY_SHIP_INTERVAL_MS=2000 \
    -e FLEZHA_DURABILITY_WRITER_ID="$3" \
    -e RELEASE_COOKIE="$COOKIE" \
    "$IMAGE" >/dev/null 2>&1
}

disk_death() { # disk_death <backend>
  local backend="$1"
  local first="${PREFIX}_${backend}_a"
  local second="${PREFIX}_${backend}_b"

  log "disk death on ${backend}: seeding one controller lifetime"
  rm -rf "${TIERDIR:?}"/* 2>/dev/null
  start_controller "$first" "$backend" "durval-a"

  if await_ready "$first"; then pass "seed controller ready"; else
    fail "seed controller did not boot on ${backend}"
    docker logs "$first" >"$LOGDIR/${backend}-a.log" 2>&1
    sed -n '1,40p' "$LOGDIR/${backend}-a.log" | sed 's/^/        /'
    return
  fi

  # Authz: claim the one-shot bootstrap, then issue a token a user would
  # actually use afterwards.
  local boot_token
  boot_token="$(docker exec "$first" cat /flezha/storage/app/auth/bootstrap.token 2>/dev/null)"
  printf '%s\n' "$boot_token" | docker exec -i "$first" /flezha/bin/flz auth bootstrap \
    >"$LOGDIR/${backend}-bootstrap.log" 2>&1
  local admin_token
  admin_token="$(grep -oE 'flzt_[A-Za-z0-9_-]+' "$LOGDIR/${backend}-bootstrap.log" | head -1)"

  flz "$first" token issue alice _system admin --ttl 3600 --token "$admin_token" \
    >"$LOGDIR/${backend}-issue.log" 2>&1
  local alice_token
  alice_token="$(grep -oE 'flzt_[A-Za-z0-9_-]+' "$LOGDIR/${backend}-issue.log" | head -1)"

  if [ -n "$alice_token" ]; then pass "issued a token before the crash"; else
    fail "could not issue a token"
    sed -n '1,20p' "$LOGDIR/${backend}-issue.log" | sed 's/^/        /'
  fi

  # A parked failure, through the store's real durable append.
  rpc "$first" 'Flezha.Dlq.Store.append(%{relay_class: "durval_class", tasklet_id: "task", cell_key: "k1", channel: "input", origin_offset: nil, error: "poison", attempts: 3, payload: "poison-bytes", park_id: "durval-1"})' \
    >"$LOGDIR/${backend}-park.log" 2>&1
  check "parked a record" "$LOGDIR/${backend}-park.log" "durval_class/task/"

  # A schedule: user-declared intent, and the thing that creates a registry
  # cspec on this node.
  flz "$first" schedule add durval-job --cron "0 3 * * *" --tz UTC --factory durval_factory \
    >"$LOGDIR/${backend}-schedule.log" 2>&1

  # Trigger the boundary instead of waiting for the backstop.
  rpc "$first" 'Flezha.State.DurabilityTier.Shipper.sweep_now()' \
    >"$LOGDIR/${backend}-sweep.log" 2>&1
  check "forced one ship" "$LOGDIR/${backend}-sweep.log" ":ok"

  flz "$first" controller status >"$LOGDIR/${backend}-status.log" 2>&1
  check "reports the remote posture" "$LOGDIR/${backend}-status.log" \
    "durability_posture:            remote_tier"

  log "disk death on ${backend}: the controller dies with its disk"
  docker rm -f "$first" >/dev/null 2>&1
  # A new writer id: a controller that came back with an empty disk genuinely is
  # a new claimant, and this is the shape the tier documents.
  start_controller "$second" "$backend" "durval-b"

  if await_ready "$second"; then pass "replacement controller ready"; else
    fail "replacement controller did not boot on ${backend}"
    docker logs "$second" >"$LOGDIR/${backend}-b.log" 2>&1
    sed -n '1,60p' "$LOGDIR/${backend}-b.log" | sed 's/^/        /'
    return
  fi

  # Parked failures hydrate WITH their relay, so the replacement has to bind
  # it — the same call `Flezha.Relay.Replay` makes on the bind path.
  rpc "$second" 'Flezha.State.DurabilityTier.hydrate_relay("durval_class")' \
    >"$LOGDIR/${backend}-dlq-hydrate.log" 2>&1

  flz "$second" dlq list >"$LOGDIR/${backend}-dlq-after.log" 2>&1
  check "the parked record survived" "$LOGDIR/${backend}-dlq-after.log" "durval_class"

  flz "$second" auth whoami --token "$alice_token" >"$LOGDIR/${backend}-whoami.log" 2>&1
  check "the token still authenticates" "$LOGDIR/${backend}-whoami.log" "alice"

  flz "$second" schedule list >"$LOGDIR/${backend}-schedule-after.log" 2>&1
  check "the schedule survived" "$LOGDIR/${backend}-schedule-after.log" "durval-job"

  # The one-shot claim must read already-claimed, not hand out a second cluster.
  docker exec "$second" test -f /flezha/storage/app/auth/bootstrap.token
  if [ $? -ne 0 ]; then pass "no fresh bootstrap token was written"; else
    fail "the replacement re-entered the bootstrap flow"
  fi

  printf '%s\n' "$boot_token" | docker exec -i "$second" /flezha/bin/flz auth bootstrap \
    >"$LOGDIR/${backend}-reclaim.log" 2>&1
  refute "the bootstrap claim is not re-runnable" "$LOGDIR/${backend}-reclaim.log" "admin@_system"

  docker rm -f "$second" >/dev/null 2>&1
}

# ---------- run ----------

log "logs in $LOGDIR"
ensure_image
docker network create "$NET" >/dev/null 2>&1

boot_posture

for backend in $BACKENDS; do
  if [ "$backend" = "postgres" ]; then
    start_postgres || continue
  fi
  disk_death "$backend"
done

log "result"
if [ "$FAILURES" -eq 0 ]; then
  echo "  all checks passed"
  exit 0
fi
echo "  $FAILURES check(s) failed"
exit 1
