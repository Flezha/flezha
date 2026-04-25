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
#   elixir  mix deps.get, mix compile --warnings-as-errors, mix test
#           (matches elixir.yml)
#   java    mvn verify with -P coverage,native-verify on the SDK reactor,
#           then mix test.java_sdk for the integration matrix
#           (matches java-sdk.yml's two jobs)
#
# Distribution artifacts (5× native binaries + SDK fat jar + test
# compute-engine fat jar) are *committed* to git. Refresh them with one
# command — `scripts/build-dist.sh` — and `git add` the updated dist/
# directories. CI never rebuilds them.
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

# Returns 0 (true) if all five native binaries are committed under resources.
all_native_binaries_present() {
  local nd="${REPO_ROOT}/sdk/java/flezha-sdk-native/src/main/resources/native"
  [[ -f "${nd}/linux-x64/libflz_transport.so"      ]] && \
  [[ -f "${nd}/linux-arm64/libflz_transport.so"    ]] && \
  [[ -f "${nd}/macos-x64/libflz_transport.dylib"   ]] && \
  [[ -f "${nd}/macos-arm64/libflz_transport.dylib" ]] && \
  [[ -f "${nd}/windows-x64/libflz_transport.dll"   ]]
}

# Returns 0 (true) if the test compute-engine jar is committed.
test_app_jar_present() {
  [[ -f "${REPO_ROOT}/test/apps/java_compute_engine/dist/java_compute_engine.jar" ]]
}

# ---------------------------------------------------------------------------
# elixir  —  matches elixir.yml `Run mix test`.
# ---------------------------------------------------------------------------
run_elixir() {
  section "elixir  —  deps.get + compile --warnings-as-errors + mix test"
  require_cmd mix "install Elixir 1.17+ via mise/asdf"

  bold ">>> mix deps.get"
  mix deps.get

  bold ">>> mix compile --warnings-as-errors"
  mix compile --warnings-as-errors

  bold ">>> mix test"
  mix test
}

# ---------------------------------------------------------------------------
# java  —  matches java-sdk.yml.
# ---------------------------------------------------------------------------
run_java() {
  section "java  —  Java unit tests + Java SDK integration matrix"
  require_cmd mvn "install Maven 3.8+"
  require_cmd java "install JDK 21"
  require_cmd mix "install Elixir 1.17+"

  if ! all_native_binaries_present || ! test_app_jar_present; then
    red "Distribution artifacts are missing. Build them with one command:"
    red "  scripts/build-dist.sh"
    red "then 'git add' the updated files under:"
    red "  sdk/java/flezha-sdk-native/src/main/resources/native/"
    red "  sdk/java/flezha-sdk/dist/"
    red "  test/apps/java_compute_engine/dist/"
    return 1
  fi

  # Mirrors java-sdk.yml `java-unit-tests` job.
  bold ">>> mvn -B -f sdk/java/pom.xml -P coverage,native-verify verify"
  mvn -B -f sdk/java/pom.xml -P coverage,native-verify verify

  # Mirrors java-sdk.yml `mix-integration-matrix` job. The alias is now
  # just `mix test --only java_sdk` — Maven is not invoked; the suite
  # runs against the committed test compute-engine jar under dist/.
  bold ">>> mix test.java_sdk"
  mix test.java_sdk
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
