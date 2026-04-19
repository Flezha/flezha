#!/usr/bin/env bash
#
# Cross-build flz_jni's cdylib (libflz_transport.{so,dylib,dll}) for the five
# target triples supported by the Flezha Java SDK using `cargo-zigbuild`.
#
# Usage:
#   scripts/build-native.sh --output <dir> --targets <all|host|csv-of-triples>
#
# Output layout mirrors `sdk/java/flezha-sdk-native/src/main/resources/native/`:
#   <dir>/linux-x64/libflz_transport.so
#   <dir>/linux-arm64/libflz_transport.so
#   <dir>/macos-x64/libflz_transport.dylib
#   <dir>/macos-arm64/libflz_transport.dylib
#   <dir>/windows-x64/libflz_transport.dll
#
# Requirements:
#   - Rust stable
#   - cargo-zigbuild  (cargo install cargo-zigbuild)

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"

OUTPUT_DIR=""
TARGETS_ARG="host"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --targets)
      TARGETS_ARG="$2"
      shift 2
      ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${OUTPUT_DIR}" ]]; then
  echo "Error: --output <dir> is required" >&2
  exit 1
fi

declare -A TRIPLE_TO_DIR=(
  ["x86_64-unknown-linux-gnu"]="linux-x64"
  ["aarch64-unknown-linux-gnu"]="linux-arm64"
  ["x86_64-apple-darwin"]="macos-x64"
  ["aarch64-apple-darwin"]="macos-arm64"
  ["x86_64-pc-windows-gnu"]="windows-x64"
)

declare -A TRIPLE_TO_EXT=(
  ["x86_64-unknown-linux-gnu"]="so"
  ["aarch64-unknown-linux-gnu"]="so"
  ["x86_64-apple-darwin"]="dylib"
  ["aarch64-apple-darwin"]="dylib"
  ["x86_64-pc-windows-gnu"]="dll"
)

declare -A TRIPLE_TO_PREFIX=(
  ["x86_64-unknown-linux-gnu"]="lib"
  ["aarch64-unknown-linux-gnu"]="lib"
  ["x86_64-apple-darwin"]="lib"
  ["aarch64-apple-darwin"]="lib"
  ["x86_64-pc-windows-gnu"]=""
)

# Resolve the list of targets requested.
ALL_TRIPLES=(
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
  "x86_64-pc-windows-gnu"
)

TRIPLES=()
case "${TARGETS_ARG}" in
  all)
    TRIPLES=("${ALL_TRIPLES[@]}")
    ;;
  host)
    HOST_TRIPLE="$(rustc -vV | awk '/^host:/ {print $2}')"
    TRIPLES=("${HOST_TRIPLE}")
    ;;
  *)
    IFS=',' read -r -a TRIPLES <<< "${TARGETS_ARG}"
    ;;
esac

# Choose the builder command. zigbuild is used for anything that isn't the
# host triple; the host triple can use plain `cargo build` to avoid the zig
# dependency during local iteration.
HOST_TRIPLE="$(rustc -vV | awk '/^host:/ {print $2}')"

command -v cargo >/dev/null 2>&1 || { echo "cargo not found on PATH" >&2; exit 1; }

mkdir -p "${OUTPUT_DIR}"

for triple in "${TRIPLES[@]}"; do
  if [[ -z "${TRIPLE_TO_DIR[${triple}]:-}" ]]; then
    echo "Unknown target triple: ${triple}" >&2
    echo "Supported: ${!TRIPLE_TO_DIR[*]}" >&2
    exit 1
  fi

  echo "==> Building flz_jni for ${triple}"

  if [[ "${triple}" == "${HOST_TRIPLE}" ]]; then
    (cd "${REPO_ROOT}" && cargo build --release -p flz_jni --target "${triple}")
  else
    command -v cargo-zigbuild >/dev/null 2>&1 || {
      echo "cargo-zigbuild is required for cross-targets. Install with:" >&2
      echo "  cargo install cargo-zigbuild" >&2
      exit 1
    }
    (cd "${REPO_ROOT}" && cargo zigbuild --release -p flz_jni --target "${triple}")
  fi

  DIR_NAME="${TRIPLE_TO_DIR[${triple}]}"
  EXT="${TRIPLE_TO_EXT[${triple}]}"
  PREFIX="${TRIPLE_TO_PREFIX[${triple}]}"

  SRC="${REPO_ROOT}/target/${triple}/release/${PREFIX}flz_transport.${EXT}"
  DST_DIR="${OUTPUT_DIR}/${DIR_NAME}"
  DST="${DST_DIR}/libflz_transport.${EXT}"

  if [[ ! -f "${SRC}" ]]; then
    echo "Expected build output not found: ${SRC}" >&2
    exit 1
  fi

  mkdir -p "${DST_DIR}"
  cp "${SRC}" "${DST}"
  echo "    -> ${DST}"
done

echo "Done. Binaries available under: ${OUTPUT_DIR}"
