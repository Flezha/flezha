#!/usr/bin/env bash
# Walk the starter templates (create-a-project-from-a-template, tasks 5.1-5.3).
#
#   wip/tools/build-sdks.sh                       the artifacts the READMEs install
#   wip/tools/walk-templates.sh <step>...         steps: a language, list, page, tar, mcp
#
# A language: `flz template create` into a scratch folder outside the clone,
# the SDK made available the way the starter's README says before it is
# published (with this checkout's path), then every command `create` printed:
# build, test, the runner against a controller of its own (`env.sh up`,
# authorization off, and `env.sh down` after), `flz relay start` and the call.
# `list` prints the templates and the refusals, `page` runs the commands of
# docs/getting_started/start_a_project.md from a release folder, `tar` pipes `--tar` into `tar x` and compares the
# files with a folder `create` wrote, and `mcp` compares `flezha_new_project`
# through `flz mcp` with the files `create` wrote; those four reach no
# controller. The release is env.sh's (FLEZHA_TOOLS_REBUILD=1 rebuilds it from
# this tree). Projects go to $WALK_SCRATCH (default /tmp/flz-templates), and
# installs stay in each project or the walks' caches. Each step's transcript is
# $OUT/<step>.txt (default wip/tools/.cache/walk/templates).
set -uo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
ENV=$ROOT/wip/tools/env.sh
RUN=$ROOT/wip/tools/.cache/run
REL=$ROOT/wip/tools/.cache/rel
OUT=${WALK_OUT:-$ROOT/wip/tools/.cache/walk/templates}
SCRATCH=${WALK_SCRATCH:-/tmp/flz-templates}
mkdir -p "$OUT" "$SCRATCH"
# shellcheck source=walk-steps.sh
source "$ROOT/wip/tools/walk-steps.sh"
export PATH=$REL/bin:$PATH
# The first build of the core and the Rust SDK is shared by the walks.
export CARGO_TARGET_DIR=$ROOT/wip/tools/.cache/walk-target
# Gems install here, not into the user's gem home.
export GEM_HOME=$SCRATCH/gems
GEM_PATH=$GEM_HOME:$(ruby -e 'print Gem.default_dir')
export GEM_PATH PATH=$GEM_HOME/bin:$PATH

page=notes # where until_ok notes a wait it gave up on
say() { echo "$*" | tee -a "$T"; }
run() { # command, run by bash in the current folder and shown as typed
  say "\$ $1"
  bash -c "$1" </dev/null >>"$T" 2>&1
  local status=$?
  say "exit=$status"
  return $status
}

# The README's "Before the SDK is published" step, with this checkout's path.
local_sdk() {
  case "$1" in
    rust) mkdir -p .cargo && printf '%s\n' '[patch.crates-io]' \
      "flezha_runner = { path = \"$ROOT/sdk/rust\" }" '' '[patch."https://github.com/flezha/flezha"]' \
      "flz_core = { path = \"$ROOT/flz_core\" }" "flz_transport = { path = \"$ROOT/flz_transport\" }" \
      >.cargo/config.toml ;;
    python) python3 -m venv .venv && .venv/bin/pip install --quiet "$ROOT"/sdk/python/dist/flezha-0.1.0-*.whl ;;
    node) npm install --no-save --silent "$ROOT/sdk/node" ;;
    ruby) gem install --silent "$ROOT"/sdk/ruby/pkg/flezha-0.1.0-*.gem ;;
    php) php_checkout ;;
    elixir) sed -i "s|{:flezha_sdk, \"~> 0.1.0\"}|{:flezha_sdk, path: \"$ROOT/sdk/elixir\"}|" mix.exs ;;
    java) ;; # build-sdks.sh installed the jars into the local Maven repository
  esac
}

# A printed command as it runs before the SDK is published: Cargo offline, and
# nothing for an install the local step replaced.
as_run() { # language, command
  case "$1:$2" in
    rust:cargo*) echo "$2 --offline" ;;
    "python:python3 -m venv .venv" | python:*requirements.txt | "node:npm install") ;;
    *) echo "$2" ;;
  esac
}

language() {
  local lang=$1 status
  T=$OUT/$lang.txt
  : >"$T"
  FLEZHA_AUTHZ_ENABLED=false "$ENV" up "$lang" >"$OUT/$lang-env.log" 2>&1 ||
    { say "env.sh up failed, see $OUT/$lang-env.log"; return 1; }
  # shellcheck disable=SC1091
  source "$RUN/vars.sh"
  project "$lang"
  status=$?
  "$ENV" stop runner
  cp "$RUN/logs/runner.log" "$OUT/$lang-runner.log" 2>/dev/null
  "$ENV" down >>"$OUT/$lang-env.log" 2>&1
  return $status
}

project() {
  local lang=$1 app=$SCRATCH/my-app-$1 line cmd runner="" calls=()
  rm -rf "$app"

  cd "$SCRATCH" && run "flz template create $lang my-app-$lang" || return 1
  cd "$app" || return 1
  say "# the README's step before the SDK is published"
  local_sdk "$lang" >>"$T" 2>&1 || { say "the local SDK step failed"; return 1; }

  mapfile -t lines < <(flz template list --json | python3 -c '
import json, sys
print("\n".join(next(t["run"] for t in json.load(sys.stdin) if t["language"] == sys.argv[1])))' "$lang")
  for line in "${lines[@]}"; do
    case "$line" in
      flz*) calls+=("$line") ;;
      *) [ -n "$runner" ] && { cmd=$(as_run "$lang" "$runner"); [ -z "$cmd" ] || run "$cmd" || return 1; }
        runner=$line ;;
    esac
  done

  cmd=$(as_run "$lang" "$runner")
  say "# the runner, in the background: $cmd"
  "$ENV" start runner "$app" bash -c "exec $cmd"
  until_ok 420 "flz runner list | grep -q default_"
  for line in "${calls[@]}"; do run "$line" || return 1; done
  run "flz relay call starter --channel hit --arg page=home" &&
    run "flz relay call starter --channel hit --arg page=about" &&
    run "flz relay info starter"
}

list() {
  T=$OUT/list.txt
  : >"$T"
  cd "$SCRATCH" && run "flz template list"
  run "flz template create cobol my-app"
  mkdir -p taken && touch taken/notes
  run "flz template create java taken"
}

# The page start_a_project.md's commands, from a release folder, as it shows them.
page_commands() {
  T=$OUT/page.txt
  : >"$T"
  rm -rf "$SCRATCH/page" && mkdir -p "$SCRATCH/page/flezha" && ln -s "$REL/bin" "$SCRATCH/page/flezha/bin"
  cd "$SCRATCH/page/flezha" || return 1
  run "./bin/flz template list"
  run "./bin/flz template create java ../my-app"
  run "ls -A ../my-app"
}

tar_pipe() {
  T=$OUT/tar.txt
  : >"$T"
  rm -rf "$SCRATCH/tar" && mkdir -p "$SCRATCH/tar" && cd "$SCRATCH/tar" || return 1
  run "flz template create python my-app --tar | tar x --keep-old-files"
  run "ls -A my-app"
  flz template create python created >/dev/null
  run "diff -r my-app created && echo the same files"
}

mcp() {
  T=$OUT/mcp.txt
  : >"$T"
  rm -rf "$SCRATCH/mcp" && mkdir -p "$SCRATCH/mcp" && cd "$SCRATCH/mcp" || return 1
  flz template create java my-app >/dev/null
  local call='{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"flezha_new_project","arguments":{"language":"java"}}}'
  say "\$ echo '$call' | flz mcp"
  echo "$call" | timeout 120 flz mcp 2>"$OUT/mcp.err" >answer.json
  run "python3 -c '
import json, os
files = json.load(open(\"answer.json\"))[\"result\"][\"structuredContent\"][\"files\"]
written = {os.path.relpath(os.path.join(d, f), \"my-app\") for d, _, fs in os.walk(\"my-app\") for f in fs}
assert {f[\"path\"] for f in files} == written, \"the paths differ\"
for f in files:
    path = os.path.join(\"my-app\", f[\"path\"])
    assert open(path).read() == f[\"content\"], path
    assert os.access(path, os.X_OK) == f[\"executable\"], path
print(len(files), \"files, the same paths, contents and executable bits as flz template create java\")
'"
}

for step in "$@"; do
  case "$step" in
    list) list ;;
    tar) tar_pipe ;;
    page) page_commands ;;
    mcp) mcp ;;
    *) language "$step" ;;
  esac
  echo "walk-templates: $step done, see $OUT/$step.txt"
done
