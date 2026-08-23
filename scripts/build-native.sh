#!/usr/bin/env bash
#
# Build the flz_core_ffi cdylib (poll-core ABI + JNI, feature "jni") for
# one or more
# target platforms and stage the result into the Java SDK's resources tree.
#
# *This script is runner-driven, not invoked by CI.* When flz_core_ffi or
# flz_transport changes, run it locally to refresh the bundled binaries
# under sdk/java/flezha-sdk-native/src/main/resources/native/, then
# `git add` and commit the updated files. CI only verifies the committed
# binaries are present (it does not rebuild them).
#
# Usage:
#   scripts/build-native.sh --output <dir>                       # host triple only
#   scripts/build-native.sh --output <dir> --targets all         # all 5 triples (needs zig)
#   scripts/build-native.sh --output <dir> --targets a,b,c       # specific triples
#
# Output layout (subdirectories created on demand):
#   <dir>/linux-x64/libflz_core_ffi.so
#   <dir>/linux-arm64/libflz_core_ffi.so
#   <dir>/macos-x64/libflz_core_ffi.dylib
#   <dir>/macos-arm64/libflz_core_ffi.dylib
#   <dir>/windows-x64/libflz_core_ffi.dll
#
# Requirements:
#   - Rust stable
#   - For the host triple: a C linker (default toolchain on every dev machine)
#   - For non-host triples: cargo-zigbuild + zig on PATH
#       cargo install cargo-zigbuild --locked
#       pip install ziglang   # in a venv if your system Python is PEP 668-locked
#       (the `python -m ziglang` shim should be on PATH as `zig`)

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

ALL_TRIPLES=(
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
  "x86_64-pc-windows-gnu"
)

HOST_TRIPLE="$(rustc -vV | awk '/^host:/ {print $2}')"

TRIPLES=()
case "${TARGETS_ARG}" in
  all)
    TRIPLES=("${ALL_TRIPLES[@]}")
    ;;
  host)
    TRIPLES=("${HOST_TRIPLE}")
    ;;
  *)
    IFS=',' read -r -a TRIPLES <<< "${TARGETS_ARG}"
    ;;
esac

command -v cargo  >/dev/null 2>&1 || { echo "cargo not found on PATH" >&2; exit 1; }
command -v rustup >/dev/null 2>&1 || { echo "rustup not found on PATH" >&2; exit 1; }
command -v cargo-about >/dev/null 2>&1 || {
  echo "cargo-about required to regenerate THIRD_PARTY_NOTICES.md." >&2
  echo "  cargo install cargo-about --locked --features cli" >&2
  exit 1
}

mkdir -p "${OUTPUT_DIR}"

for triple in "${TRIPLES[@]}"; do
  if [[ -z "${TRIPLE_TO_DIR[${triple}]:-}" ]]; then
    echo "Unknown target triple: ${triple}" >&2
    echo "Supported: ${!TRIPLE_TO_DIR[*]}" >&2
    exit 1
  fi

  echo "==> Building native crates for ${triple}"

  if ! rustup target list --installed 2>/dev/null | grep -qx "${triple}"; then
    echo "    installing rustup target ${triple}"
    rustup target add "${triple}"
  fi

  if [[ "${triple}" == "${HOST_TRIPLE}" ]]; then
    (cd "${REPO_ROOT}" && cargo build --release -p flz_core_ffi --features jni --target "${triple}")
  else
    command -v cargo-zigbuild >/dev/null 2>&1 || {
      echo "cargo-zigbuild required for non-host triples ('${triple}')." >&2
      echo "  cargo install cargo-zigbuild --locked" >&2
      exit 1
    }
    command -v zig >/dev/null 2>&1 || {
      echo "zig required for non-host triples ('${triple}')." >&2
      echo "  pip install ziglang  (a venv is fine), then ensure 'zig' is on PATH" >&2
      exit 1
    }
    (cd "${REPO_ROOT}" && cargo zigbuild --release -p flz_core_ffi --features jni --target "${triple}")
  fi

  DIR_NAME="${TRIPLE_TO_DIR[${triple}]}"
  EXT="${TRIPLE_TO_EXT[${triple}]}"
  PREFIX="${TRIPLE_TO_PREFIX[${triple}]}"
  DST_DIR="${OUTPUT_DIR}/${DIR_NAME}"
  mkdir -p "${DST_DIR}"

  for crate in flz_core_ffi; do
    SRC="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/${triple}/release/${PREFIX}${crate}.${EXT}"
    DST="${DST_DIR}/lib${crate}.${EXT}"

    if [[ ! -f "${SRC}" ]]; then
      echo "Expected build output not found: ${SRC}" >&2
      exit 1
    fi

    cp "${SRC}" "${DST}"
    echo "    -> ${DST}"
  done
done

# The binaries statically link their Rust dependencies; the attribution file
# travels with them and CI diffs it against a fresh run.
(cd "${REPO_ROOT}" && cargo about generate about.hbs -o THIRD_PARTY_NOTICES.md)
echo "    -> ${REPO_ROOT}/THIRD_PARTY_NOTICES.md"

echo "Done. Binaries staged under: ${OUTPUT_DIR}"
echo "Remember to 'git add' the updated binaries so CI can package them."
