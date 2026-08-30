#!/usr/bin/env bash
#
# Stage the flz_core_ffi cdylib (poll-core ABI + JNI, feature "jni") for one or
# more target platforms into the Java SDK's resources tree.
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
#   scripts/build-native.sh --output <dir> --targets all \
#       --skip-present --fetch-missing 0.1.0                     # portable packaging
#
# Options:
#   --skip-present         Leave a triple that is already staged alone, instead
#                          of producing it again. A source change therefore does
#                          NOT refresh a staged binary — omit the flag (the plain
#                          invocation above) when you want the host rebuilt.
#   --fetch-missing <ver>  Obtain absent NON-host triples by downloading the
#                          release assets for <ver> and verifying them against
#                          that release's SHA-256 manifest, instead of
#                          cross-building them. The host triple is always built
#                          from source, so a local change is never packaged as a
#                          published binary. Refuses when no release matches.
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
#   - For --fetch-missing: curl and sha256sum. No Rust cross toolchain.
#   - cargo-about, only when a triple is actually compiled (it refreshes
#     THIRD_PARTY_NOTICES.md, which travels with the statically linked binaries)
#
# Environment:
#   FLEZHA_RELEASE_BASE_URL  Override the release download root
#                            (default https://github.com/alfetahe/flezha/releases/download).
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"

OUTPUT_DIR=""
TARGETS_ARG="host"
SKIP_PRESENT=0
FETCH_VERSION=""

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
    --skip-present)
      SKIP_PRESENT=1
      shift
      ;;
    --fetch-missing)
      FETCH_VERSION="$2"
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

RELEASE_BASE_URL="${FLEZHA_RELEASE_BASE_URL:-https://github.com/alfetahe/flezha/releases/download}"
MANIFEST_NAME="SHA256SUMS.txt"
MANIFEST_FILE=""

# The published SHA-256 manifest for --fetch-missing's version, downloaded once
# into MANIFEST_FILE and reused for every asset. A release that does not exist
# is refused here, before anything is staged, rather than as five separate 404s.
ensure_release_manifest() {
  [[ -n "${MANIFEST_FILE}" ]] && return 0

  command -v curl      >/dev/null 2>&1 || { echo "curl required for --fetch-missing" >&2; exit 1; }
  command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum required for --fetch-missing" >&2; exit 1; }

  local url="${RELEASE_BASE_URL}/v${FETCH_VERSION}/${MANIFEST_NAME}"
  local tmp
  tmp="$(mktemp)"
  if ! curl -fsSL "${url}" -o "${tmp}"; then
    rm -f "${tmp}"
    echo "No published release for version ${FETCH_VERSION}: ${url} is not available." >&2
    echo "Cross-build the missing triples locally instead:" >&2
    echo "  scripts/build-native.sh --output ${OUTPUT_DIR} --targets all" >&2
    exit 1
  fi
  MANIFEST_FILE="${tmp}"
}

# Download one triple's asset and stage it only once its SHA-256 matches the
# manifest, so a corrupted download never reaches the resources tree.
fetch_triple() {
  local asset="$1" dst="$2"
  local expected actual tmp
  ensure_release_manifest

  expected="$(awk -v name="${asset}" '$2 == name || $2 == "*" name { print $1 }' "${MANIFEST_FILE}")"
  if [[ -z "${expected}" ]]; then
    echo "Release v${FETCH_VERSION} publishes no checksum for ${asset}" >&2
    echo "  (${RELEASE_BASE_URL}/v${FETCH_VERSION}/${MANIFEST_NAME})" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  if ! curl -fsSL "${RELEASE_BASE_URL}/v${FETCH_VERSION}/${asset}" -o "${tmp}"; then
    rm -f "${tmp}"
    echo "Release v${FETCH_VERSION} lists ${asset} in ${MANIFEST_NAME} but the asset is not downloadable" >&2
    exit 1
  fi

  actual="$(sha256sum "${tmp}" | awk '{print $1}')"
  if [[ "${actual}" != "${expected}" ]]; then
    rm -f "${tmp}"
    echo "Checksum mismatch for ${asset}: expected ${expected}, got ${actual}" >&2
    echo "The asset was NOT staged. Re-run, or cross-build locally with" >&2
    echo "  scripts/build-native.sh --output ${OUTPUT_DIR} --targets all" >&2
    exit 1
  fi

  mkdir -p "$(dirname "${dst}")"
  mv "${tmp}" "${dst}"
  chmod 0644 "${dst}"
}

trap 'rm -f "${MANIFEST_FILE:-}"' EXIT

mkdir -p "${OUTPUT_DIR}"

BUILT_FROM_SOURCE=0

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

  if [[ "${SKIP_PRESENT}" == "1" && -f "${DST}" ]]; then
    echo "==> ${triple}: already staged, kept (${DST})"
    continue
  fi

  if [[ -n "${FETCH_VERSION}" && "${triple}" != "${HOST_TRIPLE}" ]]; then
    echo "==> ${triple}: fetching release asset for v${FETCH_VERSION}"
    fetch_triple "$(release_asset_name "${DIR_NAME}" "${EXT}")" "${DST}"
    echo "    -> ${DST}"
    continue
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

  SRC="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/${triple}/release/${PREFIX}flz_core_ffi.${EXT}"
  if [[ ! -f "${SRC}" ]]; then
    echo "Expected build output not found: ${SRC}" >&2
    exit 1
  fi

  mkdir -p "${DST_DIR}"
  cp "${SRC}" "${DST}"
  BUILT_FROM_SOURCE=1
  echo "    -> ${DST}"
done


# The binaries statically link their Rust dependencies; the attribution file
# travels with them and CI diffs it against a fresh run. Only a compile can
# change the dependency graph, so a run that staged nothing from source leaves
# the file — and its toolchain requirement — alone.
if [[ "${BUILT_FROM_SOURCE}" == "1" ]]; then
  command -v cargo-about >/dev/null 2>&1 || {
    echo "cargo-about required to regenerate THIRD_PARTY_NOTICES.md." >&2
    echo "  cargo install cargo-about --locked --features cli" >&2
    exit 1
  }
  (cd "${REPO_ROOT}" && cargo about generate about.hbs -o THIRD_PARTY_NOTICES.md)
  echo "    -> ${REPO_ROOT}/THIRD_PARTY_NOTICES.md"
fi

echo "Done. Binaries staged under: ${OUTPUT_DIR}"
