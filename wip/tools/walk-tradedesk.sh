#!/usr/bin/env bash
# Walk the Part 2 guides (docs/guides/tradedesk) for one SDK's tradedesk,
# against the private controller `env.sh up <sdk>` started.
#
#   wip/tools/walk-tradedesk.sh <sdk> [page...]
#
# Pages, in the guides' order: authorization namespaces partitioning orders
# plugins durability-runner durability-controller durability-undo. With none
# named, all of them run; a page picks up what the pages before it left, so
# after a failure the rest can run on their own, and each stays well inside a
# ten-minute run. `orders` is the desk README's "start an order by hand", and
# the three `durability-` pages are durability.md's halves. The desk is built
# and run from this checkout as its README says (`example_build` and
# `example_run` in walk-steps.sh), so the SDK's artifacts must be current.
#
# Every step's command and output go to $OUT/<page>/<nn>-<step>.txt, and the
# desk runner's own output to $OUT/runner.log, so an output block in a guide is
# copied from a capture rather than typed.
set -uo pipefail

SDK=${1:?usage: walk-tradedesk.sh <sdk> [page...]}
shift
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
ENV=$ROOT/wip/tools/env.sh
RUN=$ROOT/wip/tools/.cache/run
OUT=${WALK_OUT:-$ROOT/wip/tools/.cache/walk/$SDK}
DESK=$ROOT/sdk/$SDK/examples/tradedesk
NS=trading
[ -f "$RUN/vars.sh" ] || { echo "walk: run wip/tools/env.sh up $SDK first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$RUN/vars.sh"
mkdir -p "$OUT"

page=""
n=0
# shellcheck source=walk-steps.sh
source "$ROOT/wip/tools/walk-steps.sh"
[ "$SDK" = php ] && php_checkout >/dev/null
runner_log_mark() { { wc -l <"$RUN/logs/desk.log"; } 2>/dev/null || echo 0; }
runner_log_since() {
  mkdir -p "$OUT/$page"
  tail -n +"$(($1 + 1))" "$RUN/logs/desk.log" >"$OUT/$page/$2.runner.txt"
}

key() { grep -o 'flzt_[a-z0-9]*' "$OUT/authorization/02-token-issue.txt" | head -1; }
start_runner() {
  "$ENV" start desk "$DESK" env FLEZHA_NAMESPACE_SECRET="$(key)" bash -c "exec $(example_run "$SDK" tradedesk)"
  until_ok 90 "flz runner list --namespace $NS | grep -q ${NS}_"
}
healthy() { # seconds, partitions
  until_ok "$1" "flz relay info trade_watcher --namespace $NS | grep -cE '^  p[0-9]+ .* healthy\$' | grep -qx $2"
}
finished() { # relay id, seconds
  until_ok "$2" "flz relay info $1 --namespace $NS | awk '/^Relay: $1\$/{f=1} f && /Phase:/{print \$2; exit}' | grep -qx finished"
}
trade() { # partition, symbol, price
  echo "flz relay send 'trade_watcher#$1' --namespace $NS --channel trades --text '{\"channel\": \"trade\", \"data\": [{\"symbol\": \"$2\", \"price\": $3, \"ord_type\": \"market\"}]}'"
}

authorization() {
  step bootstrap "cd $FLEZHA_CTL_DIR && flz auth bootstrap \"\$(cat app_data/auth/bootstrap.token)\""
  local token
  token=$(grep -o 'flzt_[a-z0-9]*' "$OUT/authorization/00-bootstrap.txt" | head -1)
  echo "export FLEZHA_TOKEN=$token" >>"$RUN/vars.sh"
  export FLEZHA_TOKEN=$token
  step whoami "flz auth whoami"
  step token-issue "flz token issue runner@$NS $NS runner --ttl never"
}

namespaces() {
  step create "flz namespace create $NS"
  step list "flz namespace list"
}

partitioning() {
  build_example "$DESK"
  local mark
  mark=$(runner_log_mark)
  start_runner
  runner_log_since "$mark" start
  step runner-list "flz runner list --namespace $NS"
  step role-bind "flz role bind runner@$NS spawner $NS"
  step start "flz relay start trade_watcher --id trade_watcher --partitions 2 --namespace $NS"
  healthy 60 2
  step info "flz relay info trade_watcher --namespace $NS"
  mark=$(runner_log_mark)
  step send-p0-1 "$(trade p0 EUR/USD 1.0850)"
  step send-p0-2 "$(trade p0 EUR/USD 1.0853)"
  step send-p0-3 "$(trade p0 EUR/USD 1.1100) --await 3000"
  step send-p1 "$(trade p1 GBP/USD 1.3400)"
  sleep 3
  runner_log_since "$mark" sends
  step state-eurusd "flz state get trade_watcher/track_prices/EUR/USD --namespace $NS"
  step state-all "flz state get trade_watcher/track_prices --namespace $NS"
  step scale "flz relay scale trade_watcher 3 --namespace $NS"
  healthy 60 3
  step info-after-scale "flz relay info trade_watcher --namespace $NS"
  step state-eurusd-after-scale "flz state get trade_watcher/track_prices/EUR/USD --namespace $NS"
}

orders() {
  local mark
  mark=$(runner_log_mark)
  step start-filled "flz relay start order_executor --id order_by_hand --namespace $NS --text '{\"symbol\": \"EUR/USD\", \"quantity\": 10}' --channel order"
  finished order_by_hand 30
  step info-filled "flz relay info order_by_hand --namespace $NS"
  step start-refused "flz relay start order_executor --id order_too_big --namespace $NS --text '{\"symbol\": \"EUR/USD\", \"quantity\": 500}' --channel order"
  finished order_too_big 60
  step info-refused "flz relay info order_too_big --namespace $NS"
  runner_log_since "$mark" orders
}

plugins() {
  local mark
  mark=$(runner_log_mark)
  step enable-exchange "flz plugin enable exchange relay_websocket --relay_id=trade_watcher --namespace $NS --Xuri=ws://localhost:$FLEZHA_TESTNET_WS_PORT/ --Xchannel=trades --Xsubscription_items='[\"EUR/USD\", \"GBP/USD\", \"USD/JPY\"]' --Xsubscribe_template='{\"method\": \"subscribe\", \"params\": {\"channel\": \"trade\", \"symbol\": {{items}}}, \"req_id\": {{req_id}}}' --Xunsubscribe_template='{\"method\": \"unsubscribe\", \"params\": {\"channel\": \"trade\", \"symbol\": {{items}}}, \"req_id\": {{req_id}}}'"
  sleep 20
  step info "flz relay info trade_watcher --namespace $NS"
  step state "flz state get trade_watcher/track_prices --namespace $NS"
  step dlq-list "flz dlq list 'trade_watcher#p1' --namespace $NS"
  step enable-signals "flz plugin enable signals relay_file_output --relay_id=trade_watcher --namespace $NS --hook=on_output --Xpath=signals.txt --Xchannel_out=signals"
  until_ok 120 "[ \$(wc -l <$FLEZHA_CTL_DIR/app_data/files/signals.txt) -ge 4 ]"
  step signals "cd $FLEZHA_CTL_DIR && head -4 app_data/files/signals.txt"
  step start-order-with-plugin "flz relay start order_executor --id order_1 --namespace $NS --text '{\"symbol\": \"EUR/USD\", \"quantity\": 10}' --channel order --plugins '[{\"name\": \"order_1_file\", \"provider\": \"relay_file_output\", \"opts\": {\"path\": \"order_1.txt\"}, \"hooks\": [[\"on_output\", {}]]}]'"
  finished order_1 30
  step order-file "cd $FLEZHA_CTL_DIR && wc -c app_data/files/order_1.txt"
  runner_log_since "$mark" plugins
}

durability-runner() {
  local mark
  echo /pause >"$RUN/exchange.in"
  sleep 3
  step state-before "flz state get trade_watcher/track_prices --namespace $NS"

  mark=$(runner_log_mark)
  "$ENV" stop desk TERM
  runner_log_since "$mark" clean-stop
  until_ok 60 "! flz relay info trade_watcher --namespace $NS | grep -qE '^  p[0-9]+ .* healthy\$'"
  step info-no-runner "flz relay info trade_watcher --namespace $NS"
  step state-no-runner "flz state get trade_watcher/track_prices --namespace $NS"
  start_runner
  healthy 90 3
  step info-back "flz relay info trade_watcher --namespace $NS"
  step state-after-clean-stop "flz state get trade_watcher/track_prices --namespace $NS"

  step runner-list "flz runner list --namespace $NS"
  "$ENV" stop desk KILL
  start_runner
  healthy 90 3
  step state-after-crash "flz state get trade_watcher/track_prices --namespace $NS"

  mark=$(runner_log_mark)
  step order-start "flz relay start order_executor --id order_2 --namespace $NS --text '{\"symbol\": \"EUR/USD\", \"quantity\": 10}' --channel order"
  sleep 2
  step order-journal "flz state journal order_2 --namespace $NS"
  "$ENV" stop desk KILL
  runner_log_since "$mark" order-before-crash
  mark=$(runner_log_mark)
  start_runner
  finished order_2 60
  step order-journal-after "flz state journal order_2 --namespace $NS"
  step order-info "flz relay info order_2 --namespace $NS"
  runner_log_since "$mark" order-after-crash
}

durability-controller() {
  local mark
  mark=$(runner_log_mark)
  step controller-stop "flz controller stop"
  "$ENV" controller
  step whoami "flz auth whoami"
  step namespace-list "flz namespace list"
  healthy 120 3
  step state-after-controller "flz state get trade_watcher/track_prices --namespace $NS"
  runner_log_since "$mark" controller-restart
}

durability-undo() {
  local mark
  mark=$(runner_log_mark)
  step send-poison "flz relay send 'trade_watcher#p1' --namespace $NS --channel trades --text '{\"channel\": \"trade\", \"data\": [{\"symbol\": \"EUR/USD\", \"price\": \"one euro\", \"ord_type\": \"market\"}]}'"
  until_ok 30 "flz dlq list 'trade_watcher#p1' --namespace $NS | grep -q '^trade_watcher'"
  runner_log_since "$mark" poison
  step dlq-list "flz dlq list 'trade_watcher#p1' --namespace $NS"
  local entry
  entry=$(grep -o "^trade_watcher#p1/[^ ]*" "$OUT/$page/$(printf '%02d' $((n - 1)))-dlq-list.txt" | head -1)
  step dlq-show "flz dlq show '$entry' --namespace $NS"
  step dlq-drop "flz dlq drop '$entry' --namespace $NS"
  step dlq-list-empty "flz dlq list 'trade_watcher#p1' --namespace $NS"

  mark=$(runner_log_mark)
  echo /resume >"$RUN/exchange.in"
  echo "/jump EUR/USD 12" >"$RUN/exchange.in"
  # The order is tried on its retry ladder first; a Python runner takes minutes.
  until_ok 240 "tail -n +$((mark + 1)) $RUN/logs/desk.log | grep -q '\[undo\] released'"
  runner_log_since "$mark" undo
  cp "$RUN/logs/exchange.log" "$OUT/$page/exchange.log"
}

pages="authorization namespaces partitioning orders plugins durability-runner"
for p in "${@:-$pages durability-controller durability-undo}"; do
  for page in $p; do
    n=0
    echo "$page"
    "$page"
  done
done
cp "$RUN/logs/desk.log" "$OUT/runner.log" 2>/dev/null
echo "captures in $OUT"
