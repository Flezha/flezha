#!/usr/bin/env bash
# The relay API hand run (give-each-relay-a-public-api task 7.1), against the
# private controller `FLEZHA_AUTHZ_ENABLED=false wip/tools/env.sh up java` started
# from a release built from this tree (`FLEZHA_TOOLS_REBUILD=1` the first time
# after a core change), with a test app's `orders` fixture as the runner.
#
#   wip/tools/walk-api.sh <http.fpz> [java|rust]
#
# The package is built with `MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix
# flezha.plugin.package --output <dir>` in plugins/flezha_http_plugin. The walk
# installs it, restarts the controller, and shows: `flz relay info` with the API,
# `flz relay call --arg` and `--text`, `curl` to `/v1/relays/<id>/place` and
# `/audit`, `flz mcp` listing and calling `orders_place`, a thousand instances
# listed as one tool, a runner restart announcing the API again, and a controller
# restart refusing the call until the runner reconnects. Output goes
# to $OUT (default wip/tools/.cache/walk/api); the transcript is $OUT/transcript.txt.
set -uo pipefail

NS=default
OUT=${WALK_OUT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/wip/tools/.cache/walk/api}
# shellcheck source=walk-lib.sh
source "$(dirname "$0")/walk-lib.sh"
HTTP_FPZ=${1:?usage: walk-api.sh <http.fpz> [java|rust]}
SDK=${2:-java}
T=$OUT/transcript.txt
: >"$T"

say() { echo "$*" | tee -a "$T"; }
run() { say "\$ $*"; "$@" 2>&1 | tee -a "$T"; say "exit=${PIPESTATUS[0]}"; }
free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

META='{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}'
mcp() { printf '{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}\n' "$1" "$2" | timeout 60 flz mcp 2>>"$OUT/mcp.err"; }
tools_named() { python3 -c 'import json, sys; print([t["name"] for t in json.load(sys.stdin)["result"]["tools"] if t["name"].startswith(sys.argv[1])])' "$1"; }

install_package() {
  (cd "$RUN/ctl" && flz plugin install "$HTTP_FPZ") >"$OUT/install.txt" 2>&1 ||
    { cat "$OUT/install.txt" >&2; exit 1; }
  restart_controller
  for _ in $(seq 120); do
    flz plugin list --installed | grep -q ' loaded' && return 0
    sleep 0.5
  done
  echo "walk: the package did not load" >&2
  exit 1
}

restart_controller() {
  (cd "$RUN/ctl" && flzvm stop >/dev/null 2>&1)
  for _ in $(seq 120); do pgrep -f "beam.smp.* $FLEZHA_NODE_NAME@" >/dev/null || break; sleep 0.5; done
  "$ENV" controller
}

start_test_runner() {
  "$ENV" stop runner
  case "$SDK" in
    java) "$ENV" start runner "$ROOT" java -jar "$ROOT/test/apps/java_runner/target/java_runner.jar" "$NS" ;;
    rust) start_runner ;;
  esac
}

# Until `flz relay info <id>` shows the API, for at most $1 seconds.
await_api() {
  for _ in $(seq "$1"); do flz relay info orders-eu 2>/dev/null | grep -q '^  place' && return 0; sleep 1; done
  return 1
}

install_package
start_test_runner
start_relay orders orders-eu

say "## flz relay info"
await_api 60
run flz relay info orders-eu
run flz relay info orders-eu --json

say "## flz relay call"
run flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1000
run flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=lots
run flz relay call orders-eu --channel audit --text checked
run flz relay call orders-eu --channel plce --text x

say "## curl"
port=$(free_port)
flz plugin enable api http_api --namespace _system "Xport=$port" >/dev/null
token=$(flz token issue assistant "$NS" ops --ttl 3600 | grep -o 'flzt_[a-z0-9]*')
sleep 2
say "\$ curl -X POST http://localhost:$port/v1/relays/orders-eu/place -d '{\"pair\":\"EUR/USD\",\"amount\":1000}'"
curl -s -i -X POST "http://127.0.0.1:$port/v1/relays/orders-eu/place" -H "Authorization: Bearer $token" \
  -d '{"pair":"EUR/USD","amount":1000}' | tr -d '\r' | grep -E '^(HTTP|content-type)|^\{' | tee -a "$T"
say "\$ curl -X POST http://localhost:$port/v1/relays/orders-eu/audit -d checked"
curl -s -i -X POST "http://127.0.0.1:$port/v1/relays/orders-eu/audit" -H "Authorization: Bearer $token" \
  -d checked | tr -d '\r' | grep -E '^(HTTP|content-type)|^\{' | tee -a "$T"
say "\$ curl -X POST /mcp tools/list | orders_*"
curl -s -X POST "http://127.0.0.1:$port/mcp" -H "Authorization: Bearer $token" \
  -H 'content-type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/list' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{\"_meta\":$META}}" |
  tools_named orders_ | tee -a "$T"
flz plugin disable api --namespace _system >/dev/null 2>&1

say "## flz mcp"
say "\$ flz mcp <<< tools/list | orders_place"
mcp tools/list '{}' |
  python3 -c 'import json, sys; print(json.dumps([t for t in json.load(sys.stdin)["result"]["tools"] if t["name"] == "orders_place"], indent=2))' |
  tee -a "$T"
CALL='{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"orders_place","arguments":{"relay":"orders-eu","pair":"EUR/USD","amount":1000}}}'
say "\$ echo '$CALL' | flz mcp"
echo "$CALL" | timeout 60 flz mcp 2>>"$OUT/mcp.err" | tee -a "$T"

say "## a thousand instances are one tool"
for i in $(seq 1 1000); do flz relay start orders --id "orders-$i" >/dev/null 2>&1 & (( i % 50 == 0 )) && wait; done
wait
say "\$ flz relay list | grep -c orders-"
flz relay list | grep -c ' orders-' | tee -a "$T"
say "\$ flz mcp <<< tools/list | orders_*"
mcp tools/list '{}' | tools_named orders_ | tee -a "$T"
for i in $(seq 1 1000); do flz relay stop "orders-$i" >/dev/null 2>&1 & (( i % 50 == 0 )) && wait; done
wait

say "## a runner restart announces the API again"
"$ENV" stop runner
sleep 3
run flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1
flz plugin enable api http_api --namespace _system "Xport=$port" >/dev/null
sleep 2
say "\$ curl -X POST http://localhost:$port/v1/relays/orders-eu/audit -d checked"
curl -s -i -X POST "http://127.0.0.1:$port/v1/relays/orders-eu/audit" -H "Authorization: Bearer $token" \
  -d checked | tr -d '\r' | grep -E '^HTTP|^\{' | tee -a "$T"
flz plugin disable api --namespace _system >/dev/null 2>&1
start_test_runner
await_api 60
run flz relay info orders-eu

say "## a controller restart: refused until the runner reconnects"
"$ENV" stop runner
restart_controller
run flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1
start_test_runner
await_api 120
run flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1
