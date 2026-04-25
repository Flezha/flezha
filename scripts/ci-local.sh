#!/usr/bin/env bash
#
# Mirror what GitHub Actions runs in `.github/workflows/{elixir,java-sdk}.yml`
# so failures surface here, not after a 10-minute remote run.
#
# Usage:
#   scripts/ci-local.sh                # all sections
#   scripts/ci-local.sh rust           # only the Rust workspace + cross-build
#   scripts/ci-local.sh java           # only the Maven SDK + test app build
#   scripts/ci-local.sh elixir         # only mix compile + mix test
#   scripts/ci-local.sh rust java      # any combination
#
# Sections:
#   rust        cargo build --workspace + cross-build flz_jni for all 5 triples
#               under RUSTFLAGS="-D warnings" (matches elixir.yml + java-sdk.yml)
#   java        mvn install on the SDK reactor, then build the test compute
#               engine fat jar (matches elixir.yml `Build Java test compute-engine`
#               step exactly)
#   elixir      mix deps.get, mix compile --warnings-as-errors, mix test
#               (matches elixir.yml `Run mix test`)
#
# Exit code is non-zero on the first failing section. Each section prints
# its own banner; on success the script ends with `ALL CI CHECKS PASSED`.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"
cd "${REPO_ROOT}"

# Isolate Cargo build artifacts to keep the script robust to whatever local
# state lives in `target/` (e.g. root-owned subdirs from a prior dev-container
# run). CI doesn't set this and uses default `target/`.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target-ci-local}"

# Sections to run — default to all.
if [[ $# -eq 0 ]]; then
  SECTIONS=("rust" "java" "elixir")
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
# rust: cargo build --workspace + flz_jni cross-build for all 5 triples
# ---------------------------------------------------------------------------
run_rust() {
  section "rust  —  cargo build --workspace + flz_jni cross-build (5 triples)"
  require_cmd cargo "install via rustup: https://rustup.rs"

  # Mirrors elixir.yml `Build Rust workspace` step.
  bold ">>> RUSTFLAGS=-D warnings cargo build --workspace"
  RUSTFLAGS="-D warnings" cargo build --workspace

  # Mirrors java-sdk.yml `Cross-build flz_jni for all five triples` step.
  if ! command -v cargo-zigbuild >/dev/null 2>&1; then
    red "cargo-zigbuild not installed — skipping cross-build."
    red "install with: cargo install cargo-zigbuild --locked"
    red "and ensure 'zig' is on PATH (e.g. 'pip install ziglang' inside a venv)"
    return 1
  fi
  if ! command -v zig >/dev/null 2>&1; then
    red "zig not on PATH — skipping cross-build."
    red "install with: pip install ziglang  (a venv is fine)"
    return 1
  fi

  local triples=(
    x86_64-unknown-linux-gnu
    aarch64-unknown-linux-gnu
    x86_64-apple-darwin
    aarch64-apple-darwin
    x86_64-pc-windows-gnu
  )
  for triple in "${triples[@]}"; do
    bold ">>> cross-build flz_jni for ${triple}"
    if ! rustup target list --installed 2>/dev/null | grep -qx "${triple}"; then
      rustup target add "${triple}"
    fi
    RUSTFLAGS="-D warnings" cargo zigbuild --release --target "${triple}" -p flz_jni
  done
}

# ---------------------------------------------------------------------------
# java: mvn install on SDK reactor + build test compute engine fat jar
# ---------------------------------------------------------------------------
run_java() {
  section "java  —  mvn install (SDK reactor) + test compute engine package"
  require_cmd mvn "install Maven 3.8+"
  require_cmd java "install JDK 21"

  # Mirrors elixir.yml line 136 (post-fix): SDK reactor must `install` so the
  # next reactor can resolve com.flezha:flezha-sdk from ~/.m2.
  bold ">>> mvn -B -f sdk/java/pom.xml -P host-only -DskipTests install"
  mvn -B -f sdk/java/pom.xml -P host-only -DskipTests install

  # Mirrors elixir.yml line 137: package the test compute engine fat jar.
  bold ">>> mvn -B -f test/apps/java_compute_engine/pom.xml -DskipTests package"
  mvn -B -f test/apps/java_compute_engine/pom.xml -DskipTests package
}

# ---------------------------------------------------------------------------
# elixir: deps.get, compile --warnings-as-errors, mix test
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
# main
# ---------------------------------------------------------------------------
for s in "${SECTIONS[@]}"; do
  case "$s" in
    rust)   run_rust ;;
    java)   run_java ;;
    elixir) run_elixir ;;
    *)
      red "unknown section: $s (valid: rust, java, elixir)"
      exit 1
      ;;
  esac
done

echo
green "============================================================"
green "  ALL CI CHECKS PASSED"
green "============================================================"
