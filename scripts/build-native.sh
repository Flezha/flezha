#!/usr/bin/env bash
#
# Stage a flz_core_ffi cdylib for one or more target platforms into a veneer's
# resources tree. The feature decides which veneer: "jni" (the default) is the
# poll-core ABI plus the Java SDK's exports, "napi" the Node addon's, and so on
# for every veneer the crate carries. The ABI underneath is the same binary
# either way, which is why one script builds them all.
#
# *The binaries are build output and are NOT tracked in git.* Build the host
# triple once after cloning, and again whenever flz_core_ffi, flz_core or
# flz_transport changes; `mvn`, `mix test` and the SDK conformance suite all
# read what this script staged. The other four triples are needed only to
# publish a portable jar.
#
# Usage:
#   scripts/build-native.sh --output <dir>                       # host triple only
#   scripts/build-native.sh --output <dir> --targets all         # all 5 triples (needs zig)
#   scripts/build-native.sh --output <dir> --targets a,b,c       # specific triples
#   scripts/build-native.sh --output <dir> --features napi       # the Node addon
#
# Options:
#   --features <list>      The cargo features to build the cdylib with; the
#                          default is "jni". A veneer is a feature of one crate,
#                          never a crate of its own, so this is the whole of
#                          what distinguishes one veneer's binary from another's.
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
#   - For cross-built non-host triples: cargo-zigbuild + zig on PATH
#       cargo install cargo-zigbuild --locked
#       pip install ziglang   # in a venv if your system Python is PEP 668-locked
#       (the `python -m ziglang` shim should be on PATH as `zig`)
#   - cargo-about (it refreshes THIRD_PARTY_NOTICES.md, which travels with the
#     statically linked binaries)
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"

OUTPUT_DIR=""
TARGETS_ARG="host"
FEATURES="jni"

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
    --features)
      FEATURES="$2"
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

# shellcheck source=scripts/native_triples.sh
source "${SCRIPT_DIR}/native_triples.sh"

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

mkdir -p "${OUTPUT_DIR}"

for triple in "${TRIPLES[@]}"; do
  if [[ -z "${TRIPLE_TO_DIR[${triple}]:-}" ]]; then
    echo "Unknown target triple: ${triple}" >&2
    echo "Supported: ${!TRIPLE_TO_DIR[*]}" >&2
    exit 1
  fi

  DIR_NAME="${TRIPLE_TO_DIR[${triple}]}"
  EXT="${TRIPLE_TO_EXT[${triple}]}"
  PREFIX="${TRIPLE_TO_PREFIX[${triple}]}"
  DST_DIR="${OUTPUT_DIR}/${DIR_NAME}"
  DST="${DST_DIR}/libflz_core_ffi.${EXT}"

  echo "==> Building native crates for ${triple}"

  if ! rustup target list --installed 2>/dev/null | grep -qx "${triple}"; then
    echo "    installing rustup target ${triple}"
    rustup target add "${triple}"
  fi

  if [[ "${triple}" == "${HOST_TRIPLE}" ]]; then
    (cd "${REPO_ROOT}" && cargo build --release -p flz_core_ffi --features "${FEATURES}" --target "${triple}")
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
    (cd "${REPO_ROOT}" && cargo zigbuild --release -p flz_core_ffi --features "${FEATURES}" --target "${triple}")
  fi

  SRC="${CARGO_TARGET_DIR:-${REPO_ROOT}/_build/.target}/${triple}/release/${PREFIX}flz_core_ffi.${EXT}"
  if [[ ! -f "${SRC}" ]]; then
    echo "Expected build output not found: ${SRC}" >&2
    exit 1
  fi

  mkdir -p "${DST_DIR}"
  cp "${SRC}" "${DST}"
  echo "    -> ${DST}"
done


# The binaries statically link their Rust dependencies; the attribution file
# travels with them and CI diffs it against a fresh run.
command -v cargo-about >/dev/null 2>&1 || {
  echo "cargo-about required to regenerate THIRD_PARTY_NOTICES.md." >&2
  echo "  cargo install cargo-about --locked --features cli" >&2
  exit 1
}
(cd "${REPO_ROOT}" && cargo about generate -c rel/third_party/about.toml rel/third_party/about.hbs -o rel/third_party/THIRD_PARTY_NOTICES.md)
echo "    -> ${REPO_ROOT}/rel/third_party/THIRD_PARTY_NOTICES.md"

echo "Done. Binaries staged under: ${OUTPUT_DIR}"
