#!/usr/bin/env bash
# Walk the Part 3 guides (docs/guides/advanced) for one SDK's agentdesk, in
# Docker Compose and, for the last page, on a private k3s: up, the pages'
# steps, down.
#
#   wip/tools/walk-agentdesk.sh <sdk> [page...]
#
# Pages, in the guides' order: session streamed sleeping rewinding plugins
# gateway browser budget cluster once k8s. With none named, all of them run; a
# page picks up what the pages before it left, so `session` always runs first,
# and the HTTP pages need `plugins`. The Compose project is
# agentdesk-walk-<sdk>, and everything it started (containers, network,
# volumes, the images Compose built) is removed at the end, as are the plugin
# package copied into the project, the scratch copies `plugins` packages from,
# and `k8s`'s k3s container, kubeconfig and images.
#
# It needs the controller image flezha/flezha:0.1.0, built here from this tree
# when absent (FLEZHA_WALK_REBUILD=1 builds it again), and the SDK installed the
# way the project's README says for a checkout. `plugins` also needs the HTTP
# plugin's checkout beside this one (FLEZHA_HTTP_PLUGIN_DIR).
#
# Every step's command and output go to $OUT/<page>/<nn>-<step>.txt, so an
# output block in a guide is copied from a capture rather than typed. `flz` in
# a step is the pages' alias: the CLI inside the controller's container. The
# HTTP pages reach the gateway on the host's port 8080, which compose.yaml
# publishes.
set -uo pipefail

SDK=${1:?usage: walk-agentdesk.sh <sdk> [page...]}
shift
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
OUT=${WALK_OUT:-$ROOT/wip/tools/.cache/walk/agentdesk-$SDK}
DESK=$ROOT/sdk/$SDK/examples/agentdesk
PLUGIN_SRC=${FLEZHA_HTTP_PLUGIN_DIR:-$ROOT/../flezha_http_plugin}
SCRATCH=$ROOT/wip/tools/.cache/agentdesk-src
IMAGE=flezha/flezha:0.1.0
BUILDER=flezha/plugin-builder:0.1.0
export COMPOSE_PROJECT_NAME=agentdesk-walk-$SDK
page=""
n=0
# shellcheck source=walk-steps.sh
source "$ROOT/wip/tools/walk-steps.sh"

flz() { docker compose exec controller bin/flz "$@"; }
export -f flz

# The project's build, which the runner's image copies.
build_desk() {
  case "$SDK" in
    java) echo "mvn -q package" ;;
    rust) echo "cargo build --release --offline" ;; # --offline: the checkout's .cargo/config.toml, per the README
    *) echo "walk: no build command for $SDK yet; add it here" >&2; exit 1 ;;
  esac
}

phase() { # operator id, phase, seconds: waits until `flz operator info` shows it in that phase
  until_ok "$3" "flz operator info $1 --namespace agents | awk '/^Operator: $1\$/{f=1} f && /Phase:/{print \$2; exit}' | grep -qx $2"
}
runner_up() { # waits for that many runners (1) in agents
  until_ok 90 "[ \$(flz runner list --namespace agents | grep -c agents_) -ge ${1:-1} ]"
}
logs() { # keeps each named service's log beside the page's captures
  for service; do docker compose logs --no-log-prefix "$service" >"$OUT/$page/$service.log" 2>&1; done
}
ENABLE_WEB="flz plugin enable web http_server --operator_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http"

# The scratch copies hold build output the builder writes as root, so the
# builder removes it before they go.
clean_scratch() {
  [ -d "$SCRATCH" ] || return 0
  docker run --rm -v "$SCRATCH:/s" "$BUILDER" rm -rf /s/flezha_http_plugin/_build /s/flezha_http_plugin/deps
  rm -rf "$SCRATCH"
}

down() {
  (cd "$DESK" && COMPOSE_FILE=compose.yaml:cluster.yaml RELEASE_COOKIE=${RELEASE_COOKIE:-down} DURABILITY_PASSWORD=${DURABILITY_PASSWORD:-down} \
    timeout 180 docker compose down -v --rmi local) >"$OUT/down.log" 2>&1
  rm -f "$DESK"/controller/plugins/*.fpz
  clean_scratch >>"$OUT/down.log" 2>&1
  k3s_down
  echo "down"
}

session() {
  step build "$(build_desk)"
  step up "docker compose up -d --build --wait"
  step ps "docker compose ps --format '{{.Service}}\t{{.Status}}'"
  runner_up
  step runner-list "flz runner list --namespace agents"
  step start "flz operator start agent --id anna --namespace agents"
  step call-1 "flz operator call anna --namespace agents --text 'Hello'"
  step call-2 "flz operator call anna --namespace agents --text 'What can you do?'"
  step start-bob "flz operator start agent --id bob --namespace agents"
  step call-bob "flz operator call bob --namespace agents --text 'Hi, I am Bob'"
}

streamed() {
  step listen "flz operator listen anna --namespace agents --duration-ms 5000" &
  local listener=$!
  n=$((n + 1))                           # the listener's step counted in its own shell
  sleep 2
  step call "flz operator call anna --namespace agents --text 'Tell me a story'"
  wait "$listener"
}

sleeping() {
  phase anna suspended 60
  step info-asleep "flz operator info anna --namespace agents"
  step restart-runner "docker compose restart runner"
  runner_up
  step call "flz operator call anna --namespace agents --text 'Are you still there?'"
  phase anna running 30
  step info-awake "flz operator info anna --namespace agents"
}

rewinding() {
  step state "flz state get anna/reply --namespace agents"
  step rewind "flz operator rewind anna --to-iteration 1 --namespace agents"
  until_ok 30 "[ \$(docker compose logs runner | grep -c undo) -ge 2 ]"
  step undo-log "docker compose logs runner | grep undo"
  step state-after "flz state get anna/reply --namespace agents"
  step call-after "flz operator call anna --namespace agents --text 'Where were we?'"
  step call-long "flz operator call anna --namespace agents --text \"\$(printf '%0250d' 0)\""
  step info-failed "flz operator info anna --namespace agents"
  step dlq-list "flz dlq list anna --namespace agents"
  step call-on "flz operator call anna --namespace agents --text 'Sorry, go on'"
  logs runner
}

plugins() {
  [ -d "$PLUGIN_SRC" ] || { echo "walk: no HTTP plugin checkout at $PLUGIN_SRC" >&2; exit 1; }
  clean_scratch
  mkdir -p "$SCRATCH/flezha" "$SCRATCH/flezha_http_plugin"
  git -C "$ROOT" ls-files --recurse-submodules -z | rsync -a --from0 --files-from=- "$ROOT/" "$SCRATCH/flezha/"
  git -C "$PLUGIN_SRC" ls-files -z | rsync -a --from0 --files-from=- "$PLUGIN_SRC/" "$SCRATCH/flezha_http_plugin/"
  cd "$SCRATCH/flezha" || exit 1
  step builder "docker build --target plugin-builder -t $BUILDER -f docker/prod/Dockerfile.controller ."
  cd "$SCRATCH/flezha_http_plugin" || exit 1
  step package "docker run --rm -v \"\$PWD/..:/src\" -w /src/flezha_http_plugin $BUILDER sh -c 'mix deps.get && mix flezha.plugin.package'"
  cd "$DESK" || exit 1
  cp "$SCRATCH/flezha_http_plugin/_build/flezha_http-0.1.0.fpz" controller/plugins/
  step up "docker compose up -d --build --wait controller"
  step plugin-list "flz plugin list --installed"
  runner_up
  step call "flz operator call anna --namespace agents --text 'Did you notice anything?'"
  logs runner
}

gateway() {
  step rebuild "$(build_desk) && docker compose up -d --build --wait runner"
  runner_up
  step start "flz operator start gateway --id gateway --namespace agents"
  step enable "$ENABLE_WEB"
  until_ok 30 "curl -s localhost:8080/"
  step post-anna "curl -si -d 'Hello from the web' localhost:8080/sessions/anna/messages"
  step post-carol "curl -si -d 'Hi, I am Carol' localhost:8080/sessions/carol/messages"
  until_ok 30 "flz state get carol/reply --namespace agents | grep -q Carol"
  step state-carol "flz state get carol/reply --namespace agents"
  step not-found "curl -si localhost:8080/sessions/carol"
}

browser() {
  step events "curl -sN --max-time 5 localhost:8080/sessions/anna/events" &
  local stream=$!
  n=$((n + 1))
  sleep 2
  step post "curl -s -d 'Tell me more' localhost:8080/sessions/anna/messages"
  wait "$stream"
}

budget() {
  step posts "for text in One Two Three Four; do curl -s -o /dev/null -w '%{http_code}\\n' -d \"\$text\" localhost:8080/sessions/dave/messages; sleep 2; done"
  step over "curl -si -d 'Five' localhost:8080/sessions/dave/messages"
  step state "flz state get gateway/budget/dave --namespace agents"
  step call "flz operator call dave --namespace agents --text 'Still there?'"
  logs runner
}

# Waits until no operator of agents is moving: none failed, recovering, parked or unbound.
settled() { until_ok "${1:-120}" "flz operator list --namespace agents | awk 'NR>4 && \$2 !~ /^(healthy|suspended)\$/ {bad=1} END {exit bad}'"; }

cluster() {
  export RELEASE_COOKIE=$(openssl rand -hex 32) DURABILITY_PASSWORD=$(openssl rand -hex 16)
  export COMPOSE_FILE=compose.yaml:cluster.yaml
  step up "docker compose up -d --build --wait"
  runner_up 2
  settled
  step runner-list "flz runner list --namespace agents"
  step list "flz operator list --namespace agents"
  step status "flz cluster status --namespace agents"
  step stop "time docker compose stop controller-2 runner-2"
  settled
  step list-after "flz operator list --namespace agents"
  step call "flz operator call anna --namespace agents --text 'Are you still there?'"
  step post-dave "curl -si -d 'Six' localhost:8080/sessions/dave/messages"
  step post-anna "curl -si -d 'Good' localhost:8080/sessions/anna/messages"
  logs controller controller-2
}

once() {
  phase anna suspended 30
  step inbox-anna "flz state inbox anna --namespace agents"
  step journal-anna "flz state journal anna --namespace agents"
  step inbox-gateway "flz state inbox gateway --namespace agents"
  step budget-before "flz state get gateway/budget/anna --namespace agents"
  grep state: "$OUT/$page/03-budget-before.txt" >"$OUT/$page/budget-before.state"
  step crash "curl -s -d 'Tell me everything' localhost:8080/sessions/anna/messages && sleep 1 && docker compose kill runner"
  step start "docker compose start runner"
  runner_up
  settled
  until_ok 90 "! flz state get gateway/budget/anna --namespace agents | grep state: | cmp -s - $OUT/$page/budget-before.state"
  step budget-after "flz state get gateway/budget/anna --namespace agents"
  step call "flz operator call anna --namespace agents --text 'What did I ask?'"
  step inbox-gateway-after "flz state inbox gateway --namespace agents"
  logs runner controller
}

# Page 11 runs on a private k3s in a Docker container. The page's reader uses
# their own cluster; the walk must never reach any other, so every kubectl here
# goes through a guard that refuses a kubeconfig not pointing at 127.0.0.1.
K3S=agentdesk-walk-k3s-$SDK
K3S_DIR=$ROOT/wip/tools/.cache/k3s-$SDK
kubectl() {
  local server
  server=$(command kubectl config view --minify -o 'jsonpath={.clusters[0].cluster.server}' 2>/dev/null)
  case "$server" in
    https://127.0.0.1:*) command kubectl "$@" ;;
    *) echo "walk: refusing kubectl: KUBECONFIG=${KUBECONFIG:-} points at '$server', not 127.0.0.1" >&2; return 1 ;;
  esac
}
export -f kubectl

k3s_up() {
  docker run -d --name "$K3S" --privileged --tmpfs /run --tmpfs /var/run -p 127.0.0.1::6443 \
    rancher/k3s:v1.31.4-k3s1 server --disable=traefik --disable=metrics-server >"$OUT/$page/k3s.log" 2>&1 || exit 1
  mkdir -p "$K3S_DIR"
  export KUBECONFIG=$K3S_DIR/kubeconfig
  until_ok 120 "docker exec $K3S test -s /etc/rancher/k3s/k3s.yaml"
  local port
  port=$(docker port "$K3S" 6443/tcp | sed -n 's/^127\.0\.0\.1://p' | head -1)
  (umask 077 && docker exec "$K3S" cat /etc/rancher/k3s/k3s.yaml |
    sed "s#https://127.0.0.1:6443#https://127.0.0.1:$port#" >"$KUBECONFIG")
  kubectl get nodes >/dev/null || exit 1
  until_ok 180 "kubectl wait --for=condition=Ready node --all --timeout=5s"
}

k3s_down() {
  [ -n "${FORWARD:-}" ] && kill "$FORWARD" 2>/dev/null
  docker rm -f -v "$K3S" >/dev/null 2>&1
  rm -rf "$K3S_DIR"
  docker rmi agentdesk/controller:0.1.0 agentdesk/runner:0.1.0 >/dev/null 2>&1
  return 0
}

k8s() {
  step images "docker build -q -t agentdesk/controller:0.1.0 controller && docker build -q -t agentdesk/runner:0.1.0 ."
  k3s_up
  # The page's `sudo k3s ctr images import -`, run in the walk's k3s container.
  step import "docker save agentdesk/controller:0.1.0 agentdesk/runner:0.1.0 | docker exec -i $K3S ctr images import -"
  step apply "kubectl apply -f k8s/agentdesk.yaml"
  step rollout "kubectl -n agentdesk rollout status statefulset/agentdesk --timeout=300s"
  step pods "kubectl -n agentdesk get pods"
  flz() { kubectl -n agentdesk exec agentdesk-0 -c controller -- bin/flz "$@"; }
  export -f flz
  runner_up
  step runner-list "flz runner list --namespace agents"
  step start "flz operator start agent --id anna --namespace agents && flz operator start gateway --id gateway --namespace agents"
  step enable "$ENABLE_WEB"
  kubectl -n agentdesk port-forward service/agentdesk 8090:8080 >"$OUT/$page/port-forward.log" 2>&1 &
  FORWARD=$!
  until_ok 30 "curl -s localhost:8090/"
  step post "curl -si -d 'Hello from k3s' localhost:8090/sessions/anna/messages"
  until_ok 30 "flz state get anna/reply --namespace agents | grep -q k3s"
  kill "$FORWARD"; FORWARD=""
  step delete-pod "kubectl -n agentdesk delete pod agentdesk-0"
  step rollout-again "kubectl -n agentdesk rollout status statefulset/agentdesk --timeout=300s"
  runner_up
  step call "flz operator call anna --namespace agents --text 'Are you still there?'"
  kubectl -n agentdesk logs agentdesk-0 -c controller >"$OUT/$page/controller.log" 2>&1
  step delete "kubectl delete -f k8s/agentdesk.yaml"
}

cd "$DESK" || exit 1
mkdir -p "$OUT"
if [ "${FLEZHA_WALK_REBUILD:-0}" = 1 ] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "building $IMAGE"
  (cd "$ROOT" && timeout 2400 docker build -f docker/prod/Dockerfile.controller -t "$IMAGE" .) >"$OUT/image.log" 2>&1 ||
    { echo "walk: the controller image did not build, see $OUT/image.log" >&2; exit 1; }
fi
trap down EXIT
for page in ${@:-session streamed sleeping rewinding plugins gateway browser budget cluster once k8s}; do
  n=0
  echo "$page"
  "$page"
done
echo "captures in $OUT"
