#!/usr/bin/env bash
# A private controller for a guide walk, and nothing else on the machine touched.
#
#   wip/tools/env.sh up <sdk>              start it; prints the file to `source`
#   wip/tools/env.sh controller            start the controller again after a stop
#   wip/tools/env.sh start <name> <dir> <cmd...>
#                                          run a process (a runner) in <dir> with
#                                          its standard input held open, logging to
#                                          $RUN/logs/<name>.log
#   wip/tools/env.sh stop <name> [signal]  signal it (TERM by default) and wait
#   wip/tools/env.sh down                  stop everything `up` and `start` started
#
# `up` builds the controller release once into wip/tools/.cache/rel, compiled
# under wip/tools/.cache/build so the tree's own _build is untouched, and reuses
# it (FLEZHA_TOOLS_REBUILD=1 builds it again, and runs the SDK's
# scripts/build-<sdk>-sdk.sh too). It then starts the release under its own
# state directory, node name, socket directory and ports, and the demo exchange
# from FLEZHA_TESTNET_DIR (default ../flezha_testnet, beside the repository) on a
# free port. Elixir's native library is built by the first `mix compile` of the
# desk; `up` only says when it is not there yet. A failure stops whatever `up`
# had started. Every setting lands in $RUN/vars.sh; `source` it in any shell that runs `flz`
# or a runner. The controller comes up with authorization on and no admin
# claimed: claiming one is the guides' first step. FLEZHA_AUTHZ_ENABLED=false
# set for `up` starts it with authorization off instead, as Part 1 runs it. The exchange takes its
# manual commands (`/pause`, `/resume`, `/jump ...`) written to $RUN/exchange.in.
#
# The owner's release in _build/prod/rel and its app_data are never touched,
# and neither is port 4500.
set -Eeuo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
CACHE=$ROOT/wip/tools/.cache
RUN=$CACHE/run
REL=$CACHE/rel
TESTNET=${FLEZHA_TESTNET_DIR:-$ROOT/../flezha_testnet}

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

# Says why, stops whatever `up` had started, and exits.
fail() {
  echo "env.sh: $*" >&2
  down >/dev/null 2>&1 || true
  exit 1
}

wait_for() { # seconds, what, command...
  local n=$(($1 * 4)) what=$2
  shift 2
  for _ in $(seq "$n"); do "$@" >/dev/null 2>&1 && return 0; sleep 0.25; done
  fail "$what did not come up; see $RUN/logs"
}

build_release() {
  [ -x "$REL/bin/flzvm" ] && [ "${FLEZHA_TOOLS_REBUILD:-0}" != 1 ] && return
  echo "env.sh: building the controller release into $REL (once)"
  (cd "$ROOT" && cargo build --release -p flz_nif -p flz &&
    MIX_ENV=prod MIX_BUILD_PATH="$CACHE/build" mix release flezha --path "$REL" --overwrite --quiet) \
    >"$CACHE/build-release.log" 2>&1 || fail "release build failed, see $CACHE/build-release.log"
}

# The SDK's native library. Elixir's is built from source by the first `mix
# compile` of any project that depends on the SDK, into sdk/elixir/priv/native,
# which the SDK, its test app and every example share; the other SDKs have a
# build script of their own.
native_library() {
  case "$1" in
    elixir) [ -f "$ROOT/sdk/elixir/priv/native/flz_sdk_nif.so" ] ||
      echo "env.sh: no sdk/elixir/priv/native/flz_sdk_nif.so yet; the desk's first mix compile builds it" ;;
    java | rust) ;;
    *) [ -x "$ROOT/scripts/build-$1-sdk.sh" ] || fail "no SDK named $1"
      [ "${FLEZHA_TOOLS_REBUILD:-0}" = 1 ] && "$ROOT/scripts/build-$1-sdk.sh" ;;
  esac
  return 0
}

up() {
  local sdk=${1:?usage: env.sh up <sdk>}
  [ -f "$RUN/vars.sh" ] && { echo "env.sh: already up; run env.sh down first" >&2; exit 1; }
  mkdir -p "$CACHE" "$RUN/ctl" "$RUN/logs"
  trap 'fail "up stopped at line $LINENO"' ERR
  build_release
  native_library "$sdk"

  # A Unix socket path must fit in 108 bytes, so the directory stays short.
  local sockets
  sockets=$(mktemp -d /tmp/flzt.XXXX)
  cat >"$RUN/vars.sh" <<EOF
export PATH=$REL/bin:\$PATH
export FLEZHA_TOOLS_RUN=$RUN
export FLEZHA_TOOLS_SDK=$sdk
export FLEZHA_CTL_DIR=$RUN/ctl
export FLEZHA_SOCKET_FILES_PATH=$sockets
export FLEZHA_NODE_NAME=flzt$$
export FLEZHA_STATE_DIR=$RUN/ctl/app_data/state
export FLEZHA_HEALTHZ_PORT=$(free_port)
export FLEZHA_PROMETHEUS_PORT=$(free_port)
export FLEZHA_TESTNET_WS_PORT=$(free_port)
export FLEZHA_RUNNER_ID_PATH=$RUN/runner-id
EOF
  # shellcheck disable=SC1091
  source "$RUN/vars.sh"

  # Part 1 runs with authorization off: no admin to claim, so no bootstrap token.
  if [ "${FLEZHA_AUTHZ_ENABLED:-true}" = false ]; then
    echo "export FLEZHA_AUTHZ_ENABLED=false" >>"$RUN/vars.sh"
    controller
  else
    controller
    wait_for 30 "the bootstrap token" test -f "$RUN/ctl/app_data/auth/bootstrap.token"
  fi

  FLEZHA_TESTNET_MANUAL=1 start exchange "$TESTNET" node dist/index.js
  wait_for 30 "the demo exchange" bash -c "echo >/dev/tcp/127.0.0.1/$FLEZHA_TESTNET_WS_PORT"

  trap - ERR
  echo "env.sh: up; source $RUN/vars.sh (exchange on ws://localhost:$FLEZHA_TESTNET_WS_PORT/)"
}

# The controller, started from $RUN/ctl, where its app_data lives.
controller() {
  (cd "$RUN/ctl" && nohup flzvm start </dev/null >>"$RUN/logs/controller.out" 2>&1 &)
  # Up once it answers, if only to refuse a caller with no token (exit 4, denied).
  wait_for 90 "the controller" bash -c 'flz controller status >/dev/null 2>&1; s=$?; [ $s -eq 0 ] || [ $s -eq 4 ]'
  pgrep -f "beam.smp.* $FLEZHA_NODE_NAME@" >"$RUN/controller.pid" ||
    fail "the controller answered, but no beam.smp named $FLEZHA_NODE_NAME@ is running"
}

# A process whose standard input stays open until `stop`: a runner, or the
# exchange, reads its closing as the signal to shut down.
start() {
  local name=${1:?name} dir=${2:?dir}
  shift 2
  [ -d "$dir" ] || fail "no directory $dir to start $name in"
  rm -f "$RUN/$name.in" && mkfifo "$RUN/$name.in"
  (sleep infinity >"$RUN/$name.in" 2>/dev/null & echo $! >"$RUN/$name.hold")
  # Its own process group, so `stop` reaches whatever the command forks.
  (cd "$dir" || exit 1
    setsid "$@" <"$RUN/$name.in" >>"$RUN/logs/$name.log" 2>&1 &
    echo $! >"$RUN/$name.pid")
}

stop() {
  local name=${1:?name} sig=${2:-TERM} pid
  pid=$(cat "$RUN/$name.pid" 2>/dev/null) || return 0
  kill -s "$sig" -- "-$pid" 2>/dev/null || kill -s "$sig" "$pid" 2>/dev/null || true
  for _ in $(seq 120); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
  kill -KILL -- "-$pid" 2>/dev/null || true
  kill "$(cat "$RUN/$name.hold" 2>/dev/null)" 2>/dev/null || true
  rm -f "$RUN/$name.pid" "$RUN/$name.hold" "$RUN/$name.in"
}

down() {
  [ -f "$RUN/vars.sh" ] || { echo "env.sh: nothing is up"; return 0; }
  # shellcheck disable=SC1091
  source "$RUN/vars.sh"
  for pidfile in "$RUN"/*.pid; do
    [ -f "$pidfile" ] || continue
    local name
    name=$(basename "$pidfile" .pid)
    [ "$name" = controller ] || stop "$name"
  done
  flzvm stop >/dev/null 2>&1 || true
  stop controller
  rm -rf "$FLEZHA_SOCKET_FILES_PATH" "$RUN"
  echo "env.sh: down"
}

case "${1:-}" in
  up) up "${2:-}" ;;
  controller | start | stop) [ -f "$RUN/vars.sh" ] && source "$RUN/vars.sh"; "$@" ;;
  down) down ;;
  *) echo "usage: env.sh up <sdk> | controller | start <name> <dir> <cmd...> | stop <name> [signal] | down" >&2
    exit 2 ;;
esac
