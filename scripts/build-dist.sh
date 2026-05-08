#!/usr/bin/env bash
#
# Build EVERYTHING that needs to live in git for CI to skip rebuilds:
#
#   1. flz_jni cdylib for all 5 platforms ->
#      sdk/java/flezha-sdk-native/src/main/resources/native/<sdk-dir>/libflz_transport.<ext>
#   2. SDK fat jar ->
#      sdk/java/flezha-sdk/dist/flezha-sdk.jar
#   3. Test executor fat jar ->
#      test/apps/java_executor/dist/java_executor.jar
#
# Two modes:
#   --host    (default) Run on the host. Auto-installs cargo-zigbuild and
#             zig if missing; uses an isolated CARGO_TARGET_DIR so a
#             root-owned target/ can't block the build.
#   --docker  Run inside a pinned container (docker/build/Dockerfile.dist).
#             No host toolchain required — only docker. Builds the image
#             on first run; cached afterward. Bind-mounts the repo and a
#             persistent named volume for cargo + maven caches.
#
# Then:
#   git add sdk/java/flezha-sdk-native/src/main/resources/native/ \
#           sdk/java/flezha-sdk/dist/ \
#           test/apps/java_executor/dist/
#   git commit -m "Refresh native + Java distribution artifacts"
#
# Host-mode requirements that this script will NOT auto-install:
#   - Rust stable (rustup), Maven 3.8+, JDK 21, python3 + python3-venv
# Docker-mode requirements:
#   - docker (CLI access; group membership or sudo set up by the caller)
#
# Force-rebuild the docker image (e.g. after changing Dockerfile.dist):
#   FLEZHA_REBUILD_IMAGE=1 scripts/build-dist.sh --docker

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
MODE="host"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)   MODE="host"; shift ;;
    --docker) MODE="docker"; shift ;;
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

# ---------------------------------------------------------------------------
# Docker mode: re-exec the script inside the pinned container.
# ---------------------------------------------------------------------------
if [[ "${MODE}" == "docker" ]]; then
  command -v docker >/dev/null 2>&1 || {
    printf '\033[31mdocker not on PATH — install Docker or use --host\033[0m\n' >&2
    exit 1
  }

  IMAGE="flezha-dist-build:latest"
  DOCKERFILE="${REPO_ROOT}/docker/build/Dockerfile.dist"
  CACHE_VOL="flezha-dist-build-cache"

  if [[ "${FLEZHA_REBUILD_IMAGE:-0}" == "1" ]] || ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    printf '\033[1m── building docker image %s (one-time, ~5 min on first run)\033[0m\n' "${IMAGE}"
    docker build -t "${IMAGE}" -f "${DOCKERFILE}" "${REPO_ROOT}/docker/build"
  fi

  printf '\033[1m── running build inside %s\033[0m\n' "${IMAGE}"
  exec docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "${REPO_ROOT}:/flezha" \
    -v "${CACHE_VOL}:/cache" \
    -e HOME=/cache/home \
    -e CARGO_TARGET_DIR=/cache/cargo-target \
    -e MAVEN_OPTS="-Dmaven.repo.local=/cache/m2/repository" \
    -w /flezha \
    "${IMAGE}" \
    bash -c 'mkdir -p "$HOME" "$CARGO_TARGET_DIR" /cache/m2/repository && exec scripts/build-dist.sh --host'
fi

# ---------------------------------------------------------------------------
# Host mode (default): build directly on the host.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
NATIVE_OUT="${REPO_ROOT}/sdk/java/flezha-sdk-native/src/main/resources/native"
SDK_DIST="${REPO_ROOT}/sdk/java/flezha-sdk/dist"
TEST_APP_DIST="${REPO_ROOT}/test/apps/java_executor/dist"

# Isolated cargo target dir — works around a root-owned ./target/release/
# left over from a prior dev-container run.
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${HOME}/.cache/flezha-cargo}"
export CARGO_TARGET_DIR

# Persistent zig venv (survives reboots, unlike /tmp).
ZIG_VENV="${HOME}/.local/share/flezha-zig-venv"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

step() {
  echo
  bold "── $1"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    red "missing required command: $1 ($2)"
    exit 1
  }
}

# ---------------------------------------------------------------------------
# 0. Sanity-check the toolchain we will NOT auto-install.
# ---------------------------------------------------------------------------
require_cmd cargo  "install via rustup: https://rustup.rs"
require_cmd rustup "install via rustup: https://rustup.rs"
require_cmd mvn    "install Maven 3.8+"
require_cmd java   "install JDK 21"
require_cmd python3 "install python3 + python3-venv"

# ---------------------------------------------------------------------------
# 1. Make sure cargo-zigbuild is installed.
# ---------------------------------------------------------------------------
if ! command -v cargo-zigbuild >/dev/null 2>&1; then
  step "installing cargo-zigbuild (one-time, ~30s)"
  cargo install cargo-zigbuild --locked
fi

# ---------------------------------------------------------------------------
# 2. Make sure zig is on PATH (via a self-contained venv we own).
# ---------------------------------------------------------------------------
if ! command -v zig >/dev/null 2>&1; then
  if [[ ! -x "${ZIG_VENV}/bin/zig" ]]; then
    step "creating persistent zig venv at ${ZIG_VENV}"
    python3 -m venv "${ZIG_VENV}"
    "${ZIG_VENV}/bin/pip" install --quiet ziglang
    cat > "${ZIG_VENV}/bin/zig" <<EOF
#!/bin/bash
exec "${ZIG_VENV}/bin/python" -m ziglang "\$@"
EOF
    chmod +x "${ZIG_VENV}/bin/zig"
  fi
  export PATH="${ZIG_VENV}/bin:${PATH}"
fi

# ---------------------------------------------------------------------------
# 3. Make sure all five rustup targets are installed.
# ---------------------------------------------------------------------------
TRIPLES=(
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
  "x86_64-pc-windows-gnu"
)
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

step "ensuring rustup targets are installed"
for t in "${TRIPLES[@]}"; do
  if ! rustup target list --installed 2>/dev/null | grep -qx "${t}"; then
    echo "  rustup target add ${t}"
    rustup target add "${t}"
  fi
done

# ---------------------------------------------------------------------------
# 4. Build flz_jni for each target and stage the binary into resources/.
# ---------------------------------------------------------------------------
HOST_TRIPLE="$(rustc -vV | awk '/^host:/ {print $2}')"
mkdir -p "${NATIVE_OUT}"

for t in "${TRIPLES[@]}"; do
  step "building flz_jni for ${t}"
  if [[ "${t}" == "${HOST_TRIPLE}" ]]; then
    cargo build --release -p flz_jni --target "${t}"
  else
    cargo zigbuild --release -p flz_jni --target "${t}"
  fi

  src="${CARGO_TARGET_DIR}/${t}/release/${TRIPLE_TO_PREFIX[${t}]}flz_transport.${TRIPLE_TO_EXT[${t}]}"
  dst_dir="${NATIVE_OUT}/${TRIPLE_TO_DIR[${t}]}"
  dst="${dst_dir}/libflz_transport.${TRIPLE_TO_EXT[${t}]}"

  if [[ ! -f "${src}" ]]; then
    red "expected build output missing: ${src}"
    exit 1
  fi

  mkdir -p "${dst_dir}"
  cp "${src}" "${dst}"
  echo "  -> ${dst}"
done

# ---------------------------------------------------------------------------
# 5. Build the SDK fat jar (-P native-verify enforces all 5 natives present).
# ---------------------------------------------------------------------------
step "building SDK fat jar"
mvn -B -q -f sdk/java/pom.xml -P native-verify -DskipTests install

mkdir -p "${SDK_DIST}"
cp -f sdk/java/flezha-sdk/target/flezha-sdk-*.jar "${SDK_DIST}/flezha-sdk.jar"
echo "  -> ${SDK_DIST}/flezha-sdk.jar"

# ---------------------------------------------------------------------------
# 6. Build the test executor fat jar.
# ---------------------------------------------------------------------------
step "building test executor fat jar"
mvn -B -q -f test/apps/java_executor/pom.xml -DskipTests package

mkdir -p "${TEST_APP_DIST}"
cp -f test/apps/java_executor/target/java_executor.jar \
      "${TEST_APP_DIST}/java_executor.jar"
echo "  -> ${TEST_APP_DIST}/java_executor.jar"

# ---------------------------------------------------------------------------
# Done.
# ---------------------------------------------------------------------------
echo
green "============================================================"
green "  All distribution artifacts built."
green "============================================================"
echo
echo "Stage and commit with:"
echo "  git add sdk/java/flezha-sdk-native/src/main/resources/native/ \\"
echo "          sdk/java/flezha-sdk/dist/ \\"
echo "          test/apps/java_executor/dist/"
echo "  git commit -m \"Refresh native + Java distribution artifacts\""
