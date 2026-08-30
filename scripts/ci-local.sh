#!/usr/bin/env bash
#
# Mirror what GitHub Actions runs in `.github/workflows/{elixir,java-sdk}.yml`
# so failures surface here, not after a 10-minute remote run.
#
# Usage:
#   scripts/ci-local.sh                # all sections
#   scripts/ci-local.sh elixir         # only mix compile + mix test
#   scripts/ci-local.sh java           # only Java unit tests + integration matrix
#   scripts/ci-local.sh elixir java    # any combination
#
# Sections (each runs the verbatim sequence of commands from the YAML):
#   elixir  mix deps.get, mix compile --warnings-as-errors, cargo test for
#           the flezha_runner crate, mix test, and the uncaptured-error-log
#           gate (matches elixir.yml)
#   java    mvn verify with -P coverage,native-verify on the SDK reactor,
#           then the shared smoke tier under FLEZHA_E2E_RUNTIME=java
#           (matches java-sdk.yml's two jobs)
#
# No build artifact is tracked in git, so the `java` section builds what it
# needs: the HOST triple's libflz_core_ffi and the two jars, exactly as the
# workflow does. `scripts/build-dist.sh` is the five-triple release path and is
# not on any test path.
#
# Exit code is non-zero on the first failing section. On success the
# script prints `ALL CI CHECKS PASSED`.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"
cd "${REPO_ROOT}"

# Isolate Cargo build artifacts to keep the script robust to whatever local
# state lives in `target/` (e.g. root-owned subdirs from a prior dev-container
# run). CI doesn't set this and uses default `target/`.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target-ci-local}"

if [[ $# -eq 0 ]]; then
  SECTIONS=("elixir" "java")
else
  SECTIONS=("$@")
fi

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

section() {
  echo
  bold "============================================================"
  bold "  $1"
  bold "============================================================"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    red "missing required command: $1 ($2)"
    exit 1
  }
}

# ---------------------------------------------------------------------------
# elixir  —  matches elixir.yml `Run mix test`.
# ---------------------------------------------------------------------------
run_elixir() {
  section "elixir  —  deps.get + compile --warnings-as-errors + cargo test + mix test"
  require_cmd mix "install Elixir 1.17+ via mise/asdf"
  require_cmd cargo "install Rust stable"

  bold ">>> scripts/publish-image-test.sh (immutable version tags)"
  scripts/publish-image-test.sh

  bold ">>> mix deps.get"
  mix deps.get

  bold ">>> mix compile --warnings-as-errors"
  mix compile --warnings-as-errors

  bold ">>> cargo test -p flezha_runner (Rust SDK unit + integration tests)"
  cargo test --quiet \
    -p flezha_runner --features bench-internals \
    --lib --tests

  bold ">>> mix test"
  local log
  log="$(mktemp)"
  mix test 2>&1 | tee "${log}"

  bold ">>> uncaptured error logs"
  if grep -nE '\[(error|critical)\]' "${log}"; then
    rm -f "${log}"
    red "uncaptured error/critical log during mix test — wrap the failure-path test"
    red "in ExUnit.CaptureLog and assert the message"
    return 1
  fi
  rm -f "${log}"
}

# ---------------------------------------------------------------------------
# java  —  matches java-sdk.yml.
# ---------------------------------------------------------------------------
run_java() {
  section "java  —  Java unit tests + Java SDK integration matrix"
  require_cmd mvn "install Maven 3.8+"
  require_cmd java "install JDK 21"
  require_cmd mix "install Elixir 1.17+"
  require_cmd cargo "install Rust stable"
  require_cmd npm "install Node.js 20+"

  # Mirrors java-sdk.yml's "Build the host triple" step. Every Java path below
  # reads what this stages; the other four triples are a release concern.
  bold ">>> scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native"
  scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native

  # Mirrors java-sdk.yml `java-unit-tests` job.
  bold ">>> mvn -B -f sdk/java/pom.xml -P coverage,native-verify verify"
  mvn -B -f sdk/java/pom.xml -P coverage,native-verify verify

  # The remaining steps mirror java-sdk.yml `mix-integration-matrix`.
  # Flezha.Testnet is a child of Flezha.Application in :test env and runs
  # `npm start --prefix test/apps/testnet`, which expects dist/index.js
  # (gitignored, so it must be built).
  bold ">>> npm ci || npm install + npm run build (testnet)"
  ( cd test/apps/testnet && (npm ci || npm install) && npm run build )

  bold ">>> mix deps.get"
  mix deps.get

  # PortPilot launches test/apps/java_runner/target/java_runner.jar, which
  # resolves com.flezha:flezha-sdk from ~/.m2.
  # `clean` where CI gets a fresh checkout: the shade plugin refuses to shade
  # an already-shaded jar, and the `verify` above left one in target/.
  bold ">>> mvn install (SDK) + mvn package (java_runner)"
  mvn -B -q -DskipTests -P native-verify -f sdk/java/pom.xml clean install
  mvn -B -q -DskipTests -f test/apps/java_runner/pom.xml clean package

  # PortPilot launches `cargo run --quiet` for rust_runner at boot;
  # BootLoader's post_hook only gives it 20s to connect (config/test.exs).
  # Pre-build the binary so the cargo run is a cache hit.
  bold ">>> cargo build rust_runner (warms ./target/debug)"
  cargo build --quiet \
    --manifest-path=test/apps/rust_runner/Cargo.toml \
    --target-dir=./target

  bold ">>> FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke"
  FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
for s in "${SECTIONS[@]}"; do
  case "$s" in
    elixir) run_elixir ;;
    java)   run_java ;;
    *)
      red "unknown section: $s (valid: elixir, java)"
      exit 1
      ;;
  esac
done

echo
green "============================================================"
green "  ALL CI CHECKS PASSED"
green "============================================================"
