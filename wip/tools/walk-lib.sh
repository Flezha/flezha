# The pieces every walk against `env.sh up rust` shares; sourced by a walk that
# has set NS (its namespace) and OUT (where its output goes).
ROOT=${ROOT:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}
ENV=$ROOT/wip/tools/env.sh
RUN=$ROOT/wip/tools/.cache/run
[ -f "$RUN/vars.sh" ] || { echo "walk: run wip/tools/env.sh up rust first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$RUN/vars.sh"
mkdir -p "$OUT"

# The admin is claimed once per controller; its token is kept beside the run.
admin() {
  if [ ! -f "$RUN/admin.token" ]; then
    (cd "$RUN/ctl" && flz auth bootstrap "$(cat app_data/auth/bootstrap.token)") >"$OUT/bootstrap.txt" 2>&1 ||
      { cat "$OUT/bootstrap.txt" >&2; exit 1; }
    sed -n 's/.*(shown once, store it now): //p' "$OUT/bootstrap.txt" >"$RUN/admin.token"
  fi
  FLEZHA_TOKEN=$(cat "$RUN/admin.token")
  export FLEZHA_TOKEN
}

# The runner proves its namespace with a runner key, issued once per run.
runner_key() {
  flz namespace create "$NS" >/dev/null 2>&1
  [ -f "$RUN/runner.key" ] ||
    flz token issue "runner@$NS" "$NS" runner --ttl never | grep -o 'flzt_[a-z0-9]*' | head -1 >"$RUN/runner.key"
  cat "$RUN/runner.key"
}

# The Rust test app as the namespace's runner; `start_relay <factory> <id>`
# retries until the runner serves the factory.
start_runner() {
  cargo build --quiet --manifest-path="$ROOT/sdk/rust/test_app/Cargo.toml" --target-dir="$ROOT/target" ||
    exit 1
  "$ENV" start runner "$ROOT" env FLEZHA_NAMESPACE_SECRET="$(runner_key)" \
    "$ROOT/target/debug/rust_runner" "$NS"
}

start_relay() {
  for _ in $(seq 120); do
    flz relay start "$1" --id "$2" --namespace "$NS" >"$OUT/start-$2.txt" 2>&1 && return 0
    sleep 0.5
  done
  cat "$OUT/start-$2.txt" >&2
  exit 1
}
