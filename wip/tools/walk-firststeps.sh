#!/usr/bin/env bash
# Walk the Part 1 guides (docs/guides: controller_deployment, runner_deployment,
# running_operators, sources_and_sinks, stop_everything) for one SDK's echo
# example, against the private controller `env.sh up <sdk>` started with
# authorization off, as Part 1 runs it:
#
#   FLEZHA_AUTHZ_ENABLED=false wip/tools/env.sh up <sdk>
#   wip/tools/walk-firststeps.sh <sdk> [page...]
#
# Pages, in the guides' order: controller runner operators plugins stop. With
# none named, all of them run; a page picks up what the pages before it left.
# The echo runner is built and run from this checkout as its README says
# (`example_build` and `example_run` in walk-steps.sh), so the SDK's artifacts
# must be current. `stop` stops the controller last: `env.sh controller` starts
# it again, and `env.sh down` ends everything.
#
# Every step's command and output go to $OUT/<page>/<nn>-<step>.txt, and the
# runner's own output to $OUT/runner.log, so an output block in a guide is
# copied from a capture rather than typed.
set -uo pipefail

SDK=${1:?usage: walk-firststeps.sh <sdk> [page...]}
shift
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
ENV=$ROOT/wip/tools/env.sh
RUN=$ROOT/wip/tools/.cache/run
OUT=${WALK_OUT:-$ROOT/wip/tools/.cache/walk/firststeps-$SDK}
DESK=$ROOT/sdk/$SDK/examples/echo
[ -f "$RUN/vars.sh" ] || { echo "walk: run FLEZHA_AUTHZ_ENABLED=false wip/tools/env.sh up $SDK first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$RUN/vars.sh"
mkdir -p "$OUT"

page=""
n=0
# shellcheck source=walk-steps.sh
source "$ROOT/wip/tools/walk-steps.sh"
[ "$SDK" = php ] && php_checkout >/dev/null
FILES=$FLEZHA_CTL_DIR/app_data/files

controller() {
  step boot-await "flz boot await"
  step boot-status "flz boot status"
  step runner-check "flz runner check"
}

runner() {
  build_example "$DESK"
  "$ENV" start echo "$DESK" bash -c "exec $(example_run "$SDK" echo)"
  until_ok 90 "flz runner list | grep -q default_"
  step runner-list "flz runner list"
}

operators() {
  step start "flz operator start echo --id my_echo"
  step list "flz operator list"
  step send "flz operator send my_echo --text 'hello world' --await 2000"
  step listen "flz operator listen my_echo --duration-ms 4000" &
  local listener=$!
  n=$((n + 1)) # the listener's step counted in its own shell
  sleep 1
  flz operator send my_echo --text 'hello world' >/dev/null
  wait "$listener"
}

plugins() {
  step root "flz controller status | grep file_plugin_root"
  step enable-writer "flz plugin enable writer operator_file_sink --operator_id=my_echo --hook=on_output --Xpath=echo-out.txt"
  step send "flz operator send my_echo --text hello"
  until_ok 30 "grep -q 'echo: hello' $FILES/echo-out.txt"
  step cat-out "cd $FLEZHA_CTL_DIR && cat app_data/files/echo-out.txt"
  step enable-reader "flz plugin enable reader operator_file_source --operator_id=my_echo --Xpath=echo-in.txt --Xchannel_in=in"
  step append "cd $FLEZHA_CTL_DIR && echo 'from a file' >> app_data/files/echo-in.txt"
  until_ok 30 "grep -q 'from a file' $FILES/echo-out.txt"
  step cat-out-again "cd $FLEZHA_CTL_DIR && cat app_data/files/echo-out.txt"
  step info "flz operator info"
  step disable-reader "flz plugin disable reader"
}

stop() {
  step disable-writer "flz plugin disable writer"
  step stop-operator "flz operator stop my_echo"
  "$ENV" stop echo INT
  step controller-stop "flz controller stop"
}

for page in ${@:-controller runner operators plugins stop}; do
  n=0
  echo "$page"
  "$page"
done
cp "$RUN/logs/echo.log" "$OUT/runner.log" 2>/dev/null
echo "captures in $OUT"
