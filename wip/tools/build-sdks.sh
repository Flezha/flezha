#!/usr/bin/env bash
# Build every SDK's artifacts from this checkout, the ones the guide walks
# install the way each example's README says for a checkout: each runtime's
# release build from its conformance manifest (scripts/build-runner.sh), which
# gives the Node, PHP, Python and Ruby native libraries and the Java jars in the
# local Maven repository, then the Python wheel in sdk/python/dist and the Ruby
# gem in sdk/ruby/pkg. Elixir's native library is built by the first `mix compile` of
# a project that depends on the SDK.
#
#   wip/tools/build-sdks.sh [sdk...]
#
# The wheel and the gem name the core by its git tag; a scratch
# .cargo/config.toml patches it to this checkout while they build, and is
# removed after. Each build's output goes to $LOGS/<step>.log.
set -uo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
LOGS=${LOGS:-$ROOT/wip/tools/.cache/sdk-build}
mkdir -p "$LOGS"
cd "$ROOT" || exit 1
# shellcheck source=walk-steps.sh
source "$ROOT/wip/tools/walk-steps.sh"

patched() { # dir, command...: runs the command in dir with the core patched to this checkout
  local dir=$1
  shift
  mkdir -p "$dir/.cargo"
  core_patch >"$dir/.cargo/config.toml"
  (cd "$dir" && "$@")
  local status=$?
  rm -rf "$dir/.cargo"
  return $status
}

build() { # the manifest's release build, then the wheel and the gem it does not describe
  case "$1" in
    node | php | java) scripts/build-runner.sh "$1" bench ;;
    python) scripts/build-runner.sh python bench && rm -f sdk/python/dist/flezha-0.1.0-*.whl &&
      patched sdk/python .venv/bin/maturin build --release --out dist --features python-extension,python-abi3 ;;
    ruby) scripts/build-runner.sh ruby bench && rm -f sdk/ruby/pkg/flezha-0.1.0*.gem && patched sdk/ruby rake native gem ;;
    rust | elixir) ;;
    *) echo "build-sdks: no SDK named $1" >&2; return 1 ;;
  esac
}

failed=0
for sdk in ${@:-node php python ruby java}; do
  if timeout 1500 bash -c "$(declare -f core_patch patched build); ROOT=$ROOT; build $sdk" >"$LOGS/$sdk.log" 2>&1; then
    echo "$sdk: built"
  else
    echo "$sdk: failed, see $LOGS/$sdk.log"
    failed=1
  fi
done
exit $failed
