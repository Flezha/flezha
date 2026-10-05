#!/usr/bin/env bash
# The `flz relay diagnose` hand run, against the private controller
# `FLEZHA_AUTHZ_ENABLED=false wip/tools/env.sh up java` started, with the Java test app as
# its runner and the core's dev broker up
# (`docker compose -f docker/dev/docker-compose.yml up -d zookeeper kafka`).
#
#   wip/tools/walk-diagnose.sh <kafka.fpz> <http.fpz>
#
# Each package is built with `MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix
# flezha.plugin.package --output <dir>` in its plugin directory. The walk installs both,
# restarts the controller, and diagnoses: a healthy relay, three dead letters, a sleeping
# session, a Kafka consumer whose broker stopped, a blocked tasklet with inputs arriving,
# the runner gone, an unknown id, and one relay through `flz mcp` and `POST /v1/call`.
# Output goes to $OUT (default wip/tools/.cache/walk/diagnose); the transcript is
# $OUT/transcript.txt.
set -uo pipefail

NS=default
OUT=${WALK_OUT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/wip/tools/.cache/walk/diagnose}
# shellcheck source=walk-lib.sh
source "$(dirname "$0")/walk-lib.sh"
KAFKA_FPZ=${1:?usage: walk-diagnose.sh <kafka.fpz> <http.fpz>}
HTTP_FPZ=${2:?usage: walk-diagnose.sh <kafka.fpz> <http.fpz>}
JAR=$ROOT/test/apps/java_runner/target/java_runner.jar
T=$OUT/transcript.txt
: >"$T"

say() { echo "$*" | tee -a "$T"; }
run() { say "\$ $*"; "$@" 2>&1 | tee -a "$T"; say "exit=${PIPESTATUS[0]}"; }

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

# Diagnose until the first line matches, for at most $2 seconds.
diagnose_until() {
  for _ in $(seq "$2"); do flz relay diagnose "$1" | head -1 | grep -q "$3" && break; sleep 1; done
  run flz relay diagnose "$1"
}

install_packages() {
  (cd "$RUN/ctl" && flz plugin install "$KAFKA_FPZ" && flz plugin install "$HTTP_FPZ") >"$OUT/install.txt" 2>&1 ||
    { cat "$OUT/install.txt" >&2; exit 1; }
  (cd "$RUN/ctl" && flzvm stop >/dev/null 2>&1)
  for _ in $(seq 120); do pgrep -f "beam.smp.* $FLEZHA_NODE_NAME@" >/dev/null || break; sleep 0.5; done
  "$ENV" controller
  for _ in $(seq 120); do
    [ "$(flz plugin list --installed | grep -c ' loaded')" = 2 ] && return 0
    sleep 0.5
  done
  echo "walk: the packages did not load" >&2
  exit 1
}

start_java_runner() {
  "$ENV" stop runner
  FLEZHA_FIXTURE_SLEEP_AFTER_MS=5000 "$ENV" start runner "$ROOT" java -jar "$JAR" "$NS"
}

install_packages
start_java_runner

say "## healthy"
start_relay do_nothing orders-eu
diagnose_until orders-eu 30 '^OK'
run flz relay diagnose orders-eu --json

say "## three dead letters"
start_relay failing_turn_loop_steps turns
for _ in 1 2 3; do flz relay send turns --text boom >/dev/null; sleep 2; done
diagnose_until turns 30 dead_letters

say "## a sleeping session"
start_relay sleeping_counter sleepy
flz relay send sleepy --text a:1 >/dev/null
diagnose_until sleepy 30 asleep

say "## a Kafka consumer whose broker stopped"
docker exec flezha_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic diagnose --partitions 1 --replication-factor 1 >/dev/null 2>&1
flz plugin enable orders_in kafka_consumer Xhosts=localhost:19092 Xtopics=diagnose \
  "Xgroup_id=diagnose_$$" >/dev/null
flz plugin bind orders_in --relay_id orders-eu --channel input >/dev/null
diagnose_until orders-eu 30 '^OK'
docker stop flezha_kafka >/dev/null
diagnose_until orders-eu 90 plugin_unhealthy
docker start flezha_kafka >/dev/null
flz plugin unbind orders_in --relay_id orders-eu >/dev/null 2>&1
diagnose_until orders-eu 60 '^OK'

say "## through flz mcp and POST /v1/call"
meta='{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}'
call='{"name":"relay_diagnose","arguments":{"namespace":"default","relay_id":"orders-eu"}'
say "\$ flz mcp <<< tools/call relay_diagnose"
printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":$call,\"_meta\":$meta}}" |
  timeout 30 flz mcp 2>&1 | tee -a "$T"
port=$(free_port)
flz plugin enable api http_api --namespace _system "Xport=$port" >/dev/null
token=$(flz token issue assistant "$NS" ops --ttl 3600 | grep -o 'flzt_[a-z0-9]*')
sleep 2
say "\$ curl -X POST /mcp tools/list | relay_diagnose"
curl -s -X POST "http://127.0.0.1:$port/mcp" -H "Authorization: Bearer $token" \
  -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/list' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{\"_meta\":$meta}}" |
  python3 -c 'import json, sys; print(json.dumps([t for t in json.load(sys.stdin)["result"]["tools"] if t["name"] == "relay_diagnose"]))' |
  tee -a "$T"
say "\$ curl -X POST /v1/call/Relay.diagnose -d '[\"default\",\"orders-eu\"]'"
curl -s -X POST "http://127.0.0.1:$port/v1/call/Relay.diagnose" -H "Authorization: Bearer $token" \
  -d '["default","orders-eu"]' | tee -a "$T"
say ""
flz plugin disable api --namespace _system >/dev/null 2>&1

say "## a blocked tasklet with inputs arriving"
start_relay wedge_probe wedged
flz relay send wedged --text wedge >/dev/null
for i in $(seq 1 30); do
  flz relay send wedged --text "x$i" >/dev/null 2>&1
  flz relay diagnose wedged | head -1 | grep -q data_plane_wedge && break
  sleep 1
done
run flz relay diagnose wedged
say "\$ grep runner_pool_wedged controller.out"
grep -m2 "runner_pool_wedged" "$RUN/logs/controller.out" | tee -a "$T"

say "## the runner gone"
diagnose_until orders-eu 60 no_runner
run flz relay diagnose orders-eu --json

say "## an unknown id"
run flz relay diagnose ghost

start_java_runner
