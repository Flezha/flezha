#!/usr/bin/env bash
#
# Build every distribution artefact the Java and PHP SDKs ship:
#
#   1. flz_core_ffi cdylib (poll-core ABI + JNI, feature "jni") for all 5
#      platforms ->
#      sdk/java/flezha-sdk-native/src/main/resources/native/<sdk-dir>/libflz_core_ffi.<ext>
#   1a. flz-fake-controller (feature "fake-controller") for all 5 platforms ->
#      _build/.dist/tools/<sdk-dir>/flz-fake-controller
#   1f. the Elixir SDK's native library (flz_core_beam) for all 5 platforms, packed
#      as rustler_precompiled downloads it, with the checksum file the package
#      carries -> _build/.dist/elixir-nifs/
#   1b. The contract set (test/fixtures/contract.txt) as one archive, staged
#      by --stage-release only: it is files already in the tree, not a build.
#   1g. Each SDK language's starter project (priv/ai_kit/templates/) as
#      flezha-<language>-starter.tar.gz, staged by --stage-release only, for
#      the same reason.
#   1h. The controller release archive of each platform, built and smoke-tested
#      natively by CI's release workflow (scripts/release-archive.sh) and
#      staged by --stage-release only -> _build/.dist/controller/
#   1c. flz_core_ffi cdylib (feature "napi") for every platform the Node
#      package's own table declares -> _build/.dist/node-addons/<suffix>/, packed into
#      the per-platform packages at _build/.dist/node-packages/
#   1d. abi3 wheels (feature "python-abi3") for all 5 platforms, and one
#      free-threaded wheel per platform per supported free-threaded
#      interpreter -> _build/.dist/wheels/
#   1e. precompiled Ruby platform gems, one per platform, carrying one library
#      per supported Ruby minor -> _build/.dist/ruby-gems/
#   2. The same five libraries staged for the PHP package ->
#      sdk/php/lib/<sdk-dir>/libflz_core_ffi.<ext>
#   3. SDK fat jar ->
#      sdk/java/flezha-sdk/dist/flezha-sdk.jar
#   4. Test runner fat jar ->
#      test/apps/java_runner/dist/java_runner.jar
#
# None of it is tracked in git. Day to day you need only the host triple —
# `scripts/build-native.sh` produces that in one step, and every test path
# reads it. This script is the five-triple path: what a RELEASE publishes.
#
# Modes:
#   --host    (default) Run on the host. Auto-installs cargo-zigbuild and
#             zig if missing; uses an isolated CARGO_TARGET_DIR so a
#             root-owned target/ can't block the build.
#   --docker  Run inside a pinned container (deploy/docker/build/Dockerfile.dist).
#             No host toolchain required — only docker. Builds the image
#             on first run; cached afterward. Bind-mounts the repo and a
#             persistent named volume for cargo + maven caches.
#   --stage-release <dir>
#             Do not build. Copy the artefacts already on disk into
#             <dir> under their published asset names and write a SHA-256
#             manifest beside them. This is what the release job uploads,
#             and the names are the ones the SDK repositories fetch.
#   --triples <triple>[,<triple>...]
#             Build or stage only these of the five triples, and leave out the
#             two fat jars, which carry all five. A set this narrow is a
#             pre-release for the SDK repositories' CI, never a release.
#
# FLEZHA_GIT_MIRROR, when set, is a directory of bare repositories named as on
# GitHub (`flezha`, `flezha_ruby_sdk.git`, ...). Cargo takes the core's tag, and
# the submodules it fetches with it, from there instead of from GitHub (the
# wheels and the gems build against that tag), so a release can be rehearsed
# before anything is pushed.
#
# Host-mode requirements that this script will NOT auto-install:
#   - Rust stable (rustup), Maven 3.8+, JDK 21, python3 + python3-venv
#   - Node 20+, and maturin in sdk/python/.venv (scripts/build-python-sdk.sh
#     creates the venv; then `.venv/bin/pip install maturin`)
#   - each free-threaded interpreter FREE_THREADED_PYTHONS names, on PATH
#     (`python3.14t`)
#   - docker, and `rb-sys-dock` from the rb_sys gem the Ruby SDK pins
# Docker-mode requirements:
#   - docker (CLI access; group membership or sudo set up by the caller)
#
# Force-rebuild the docker image (e.g. after changing Dockerfile.dist):
#   FLEZHA_REBUILD_IMAGE=1 scripts/build-dist.sh --docker
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
MODE="host"
STAGE_RELEASE_DIR=""
TRIPLES=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)   MODE="host"; shift ;;
    --docker) MODE="docker"; shift ;;
    --stage-release) MODE="stage-release"; STAGE_RELEASE_DIR="$2"; shift 2 ;;
    --triples) TRIPLES="$2"; shift 2 ;;
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
    bash -c 'mkdir -p "$HOME" "$CARGO_TARGET_DIR" /cache/m2/repository && exec scripts/build-dist.sh --host ${1:+--triples "$1"}' _ "${TRIPLES}"
fi

# ---------------------------------------------------------------------------
# Host mode (default): build directly on the host.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
# The contract set an SDK vendors: the list, and the archive staged from it.
CONTRACT_MANIFEST="test/fixtures/contract.txt"
STARTER_MANIFEST="priv/ai_kit/templates/manifest.json"
NATIVE_OUT="${REPO_ROOT}/sdk/java/flezha-sdk-native/src/main/resources/native"
# The PHP package loads the same libraries through ext-ffi, from directories
# named the same way. Not committed: they are build output the release job
# stages, which is why `sdk/php/.gitignore` excludes them.
PHP_NATIVE_OUT="${REPO_ROOT}/sdk/php/lib"
# The fake controller is loaded by no package; it is a release asset an SDK
# repository fetches, so it is staged on its own rather than into an SDK tree.
TOOLS_OUT="${REPO_ROOT}/_build/.dist/tools"
# The Node addons, one directory per platform suffix, and the per-platform
# packages `scripts/platform-package.mjs` writes around them.
NODE_ADDONS_OUT="${REPO_ROOT}/_build/.dist/node-addons"
NODE_PACKAGES_OUT="${REPO_ROOT}/_build/.dist/node-packages"
NODE_PLATFORM_TABLE="${REPO_ROOT}/sdk/node/dist/esm/platforms.js"
# The wheels: one `abi3` wheel per platform, and one per platform per
# free-threaded interpreter. PyO3 builds no free-threaded extension below 3.14.
WHEELS_OUT="${REPO_ROOT}/_build/.dist/wheels"
FREE_THREADED_PYTHONS=("3.14t")
# Each wheel tier as `<the tag in its file name>|<what maturin is asked for>`:
# the abi3 tier, then one per free-threaded interpreter, because `abi3` has no
# free-threaded form. The build and the staging read the same rows.
wheel_tiers() {
  local v
  echo "abi3|--features python-abi3"
  for v in "${FREE_THREADED_PYTHONS[@]}"; do
    echo "cp${v//[.t]/}-cp${v//[.t]/}t|--interpreter python${v}"
  done
}
# The Elixir SDK's native library archives, and the checksum file the package
# carries beside them.
ELIXIR_NIFS_OUT="${REPO_ROOT}/_build/.dist/elixir-nifs"
ELIXIR_NIF_CHECKSUMS="checksum-Elixir.Flezha.SDK.Native.exs"
# The controller archives: named, packed and checked by their own script, so
# a macOS runner with only its stock bash can build one.
CONTROLLER_OUT="${REPO_ROOT}/_build/.dist/controller"
# The precompiled Ruby gems, and the Ruby minors each one carries a library for.
RUBY_GEMS_OUT="${REPO_ROOT}/_build/.dist/ruby-gems"
RUBY_GEM_MINORS="3.3,3.4"
RELEASE_VERSION="$(awk -F'"' '/^version = / { print $2; exit }' "${REPO_ROOT}/Cargo.toml")"
SDK_DIST="${REPO_ROOT}/sdk/java/flezha-sdk/dist"
TEST_APP_DIST="${REPO_ROOT}/test/apps/java_runner/dist"

# Isolated cargo target dir — works around a root-owned ./_build/.target/release/
# left over from a prior dev-container run.
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${HOME}/.cache/flezha-cargo}"
export CARGO_TARGET_DIR


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

# shellcheck source=scripts/native_triples.sh
source "${SCRIPT_DIR}/native_triples.sh"

if [[ -n "${TRIPLES}" ]]; then
  IFS=',' read -ra selected <<< "${TRIPLES}"
  for t in "${selected[@]}"; do
    [[ -n "${TRIPLE_TO_DIR[${t}]:-}" ]] || { red "--triples: ${t} is not one of: ${ALL_TRIPLES[*]}"; exit 1; }
  done
  ALL_TRIPLES=("${selected[@]}")
fi

ORG_GIT_URL="https://github.com/flezha/"
if [[ -n "${FLEZHA_GIT_MIRROR:-}" ]]; then
  export CARGO_NET_GIT_FETCH_WITH_CLI=true GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0="url.file://${FLEZHA_GIT_MIRROR}/.insteadOf" GIT_CONFIG_VALUE_0="${ORG_GIT_URL}"
fi

# The Node package's platform table, one `<suffix>|<target>` row per platform.
# Read from what the package built rather than transcribed here: the loader
# resolves against that table, and a platform declared in one place and
# forgotten in the other is an optional dependency npm skips in silence at
# install time.
node_platforms() {
  node -e '
    import(process.argv[1]).then(({ NATIVE_PLATFORMS }) => {
      for (const p of NATIVE_PLATFORMS) console.log(`${p.suffix}|${p.target}`);
    });
  ' "${NODE_PLATFORM_TABLE}" | while IFS='|' read -r suffix target; do
    # The table's Windows row is MSVC, which ALL_TRIPLES does not spell, so a
    # narrowed set is matched on the table's own target.
    if [[ -z "${TRIPLES}" || ",${TRIPLES}," == *",${target},"* ]]; then
      echo "${suffix}|${target}"
    fi
  done
}

# ---------------------------------------------------------------------------
# stage-release mode: publish what is already on disk. No build, no toolchain.
# ---------------------------------------------------------------------------
if [[ "${MODE}" == "stage-release" ]]; then
  require_cmd sha256sum "install coreutils"
  require_cmd python3 "install python3"
  [[ -n "${STAGE_RELEASE_DIR}" ]] || { red "--stage-release needs a directory"; exit 1; }

  mkdir -p "${STAGE_RELEASE_DIR}"
  step "staging release assets into ${STAGE_RELEASE_DIR}"

  missing=()
  assets=()
  stage_asset() {
    local src="$1" name="$2"
    if [[ ! -f "${src}" ]]; then missing+=("${src}"); return; fi
    cp -f "${src}" "${STAGE_RELEASE_DIR}/${name}"
    assets+=("${name}")
  }

  # One packed package per platform the Node package declares, named by the
  # table rather than globbed: a release that publishes a parent declaring five
  # optional dependencies and carries four is an install that fails at first
  # load, on the one platform nobody tested.
  stage_node_packages() {
    if [[ ! -f "${NODE_PLATFORM_TABLE}" ]]; then
      missing+=("${NODE_PLATFORM_TABLE} (the Node platform table; build the package first)")
      return 0
    fi
    while IFS='|' read -r suffix _; do
      [[ -n "${suffix}" ]] || continue
      stage_asset "${NODE_PACKAGES_OUT}/flezha-sdk-${suffix}-${RELEASE_VERSION}.tgz" \
                  "flezha-sdk-${suffix}-${RELEASE_VERSION}.tgz"
    done < <(node_platforms)
  }

  # One wheel per platform per tier. The file name carries a platform tag this
  # script cannot predict (`manylinux_2_34_x86_64`, `macosx_11_0_arm64`), so the
  # count per tier is what is checked: a release that shipped four of five would
  # leave one platform on the source distribution and a Rust toolchain. No wheel
  # of a tier is a build that did not run, not an empty release.
  stage_wheels() {
    local tag wheels=()
    shopt -s nullglob
    while IFS='|' read -r tag _; do
      wheels=("${WHEELS_OUT}"/flezha-"${RELEASE_VERSION}"-*"${tag}"-*.whl)
      if (( ${#wheels[@]} != ${#ALL_TRIPLES[@]} )); then
        missing+=("${#wheels[@]} of ${#ALL_TRIPLES[@]} ${tag} wheels in ${WHEELS_OUT}")
      fi
      for src in "${wheels[@]}"; do
        stage_asset "${src}" "$(basename "${src}")"
      done
    done < <(wheel_tiers)
    shopt -u nullglob
  }

  # The contract set as one archive: every member `test/fixtures/contract.txt`
  # lists, at the same repository-relative path inside it, so an SDK unpacks it
  # at `contract/` and its tests resolve what they already spell. Built from
  # the list and never from a glob: a file the list does not name is not part
  # of the contract, and one it names and the tree has not got is a staging
  # failure rather than a short archive nobody notices.
  stage_fixture_archive() {
    local archive="flezha-fixtures-${RELEASE_VERSION}.tar.gz" members=()

    while IFS= read -r member; do
      members+=("${member}")
      [[ -f "${REPO_ROOT}/${member}" ]] || missing+=("${member} (named by ${CONTRACT_MANIFEST})")
    done < <(sed -e 's/#.*$//' -e 's/|.*$//' -e '/^[[:space:]]*$/d' "${REPO_ROOT}/${CONTRACT_MANIFEST}")

    if (( ${#members[@]} == 0 )); then
      missing+=("${CONTRACT_MANIFEST} names no members")
      return 0
    fi
    # Something else is already missing, so the release will be refused anyway;
    # building the archive would only add a file nobody uploads. `return 0`
    # rather than a bare `return`: under `set -e` a function that answers the
    # `||` with a non-zero status ends the script, silently.
    (( ${#missing[@]} == 0 )) || return 0

    tar -czf "${STAGE_RELEASE_DIR}/${archive}" -C "${REPO_ROOT}" "${members[@]}"
    assets+=("${archive}")
    echo "  -> ${archive} (${#members[@]} members)"
  }

  # One starter project per language, packed from the copy `flezha_new_project`
  # answers from, so the download and the assistant's answer cannot differ.
  # Like the contract set, its members come from the manifest, never a glob.
  # The `.gitignore` exists only as manifest text and each mode comes from the
  # manifest, which is why `tarfile` packs it rather than `tar`. Written even
  # when other assets are missing: it depends on nothing that is built.
  stage_starter_archives() {
    local packed kind value
    # Captured before it is read, so a failure ends the script under `set -e`
    # rather than staging a release without its starters.
    packed="$(python3 -I - "${REPO_ROOT}" "${STARTER_MANIFEST}" "${STAGE_RELEASE_DIR}" <<'PY'
import io, json, os, sys, tarfile, time

repo, manifest, out = sys.argv[1:]
now = int(time.time())

for t in json.load(open(os.path.join(repo, manifest)))["templates"]:
    if t["example"] != "starter":
        continue
    folder = f"flezha-{t['language']}-starter"
    src = os.path.join(os.path.dirname(manifest), t["language"], "starter")
    files = [(".gitignore", t["gitignore"].encode(), False)] if t.get("gitignore") else []
    complete = True
    for f in t["files"]:
        path = os.path.normpath(f["path"])
        full = os.path.join(repo, src, path)
        if os.path.isabs(path) or path.split(os.sep)[0] == ".." or not os.path.isfile(full):
            print(f"missing\t{os.path.join(src, f['path'])}")
            complete = False
        elif complete:
            files.append((path, open(full, "rb").read(), f["executable"]))
    if not complete:
        continue
    with tarfile.open(os.path.join(out, folder + ".tar.gz"), "w:gz") as tar:
        for path, data, executable in files:
            info = tarfile.TarInfo(f"{folder}/{path}")
            info.size, info.mtime = len(data), now
            info.mode = 0o755 if executable else 0o644
            tar.addfile(info, io.BytesIO(data))
    print(f"asset\t{folder}.tar.gz")
PY
)"
    while IFS=$'\t' read -r kind value; do
      case "${kind}" in
        asset)   assets+=("${value}"); echo "  -> ${value}" ;;
        missing) missing+=("${value} (named by ${STARTER_MANIFEST})") ;;
      esac
    done <<< "${packed}"
  }

  for t in "${ALL_TRIPLES[@]}"; do
    dir="${TRIPLE_TO_DIR[${t}]}"
    ext="${TRIPLE_TO_EXT[${t}]}"
    bin_ext="${TRIPLE_TO_BIN_EXT[${t}]}"
    stage_asset "${NATIVE_OUT}/${dir}/libflz_core_ffi.${ext}" \
                "$(release_asset_name "${dir}" "${ext}")"
    if go_static_triple "${t}"; then
      stage_asset "${NATIVE_OUT}/${dir}/libflz_core_ffi.a" "$(release_asset_name "${dir}" a)"
    fi
    stage_asset "${TOOLS_OUT}/${dir}/flz-fake-controller${bin_ext}" \
                "$(release_binary_name "${dir}" "${bin_ext}")"
    gem="flezha-${RELEASE_VERSION}-${TRIPLE_TO_GEM_PLATFORM[${t}]}.gem"
    stage_asset "${RUBY_GEMS_OUT}/${gem}" "${gem}"
    nif="$(elixir_nif_name "${t}" "${RELEASE_VERSION}").tar.gz"
    stage_asset "${ELIXIR_NIFS_OUT}/${nif}" "${nif}"
  done
  stage_asset "${ELIXIR_NIFS_OUT}/${ELIXIR_NIF_CHECKSUMS}" "${ELIXIR_NIF_CHECKSUMS}"
  if [[ -z "${TRIPLES}" ]]; then
    stage_asset "${SDK_DIST}/flezha-sdk.jar" "flezha-sdk.jar"
    stage_asset "${TEST_APP_DIST}/java_runner.jar" "java_runner.jar"
    for platform in $("${SCRIPT_DIR}/release-archive.sh" platforms); do
      archive="$("${SCRIPT_DIR}/release-archive.sh" name "${platform}")"
      stage_asset "${CONTROLLER_OUT}/${archive}" "${archive}"
    done
  fi
  stage_fixture_archive
  stage_starter_archives
  stage_node_packages
  stage_wheels

  if (( ${#missing[@]} > 0 )); then
    red "cannot stage a release: these artefacts are not built yet"
    printf '  %s\n' "${missing[@]}" >&2
    red "build them first with: scripts/build-dist.sh --host"
    exit 1
  fi

  # Bare names in the manifest, so a consumer verifies the asset it downloaded
  # without knowing where it was staged. Named explicitly rather than globbed,
  # so a stray file in the directory is never published as an asset.
  ( cd "${STAGE_RELEASE_DIR}" && sha256sum -- "${assets[@]}" > SHA256SUMS.txt )
  echo "  -> ${STAGE_RELEASE_DIR}/SHA256SUMS.txt"
  green "${#assets[@]} assets + manifest staged."
  exit 0
fi

# ---------------------------------------------------------------------------
# 0. Sanity-check the toolchain we will NOT auto-install.
# ---------------------------------------------------------------------------
require_cmd cargo  "install via rustup: https://rustup.rs"
require_cmd rustup "install via rustup: https://rustup.rs"
require_cmd mvn    "install Maven 3.8+"
require_cmd java   "install JDK 21"
require_cmd python3 "install python3 + python3-venv"

# ---------------------------------------------------------------------------
# 1-3. cargo-zigbuild, zig and the rustup target of every triple.
# ---------------------------------------------------------------------------
step "ensuring cargo-zigbuild, zig and the rustup targets"
PATH="$("${SCRIPT_DIR}/ensure-zig.sh" "${ALL_TRIPLES[@]}"):${PATH}"
export PATH

# ---------------------------------------------------------------------------
# 4. Build the native crates for each target and stage into resources/.
#    flz_core_ffi carries the poll-core ABI + JNI exports (feature "jni").
# ---------------------------------------------------------------------------
HOST_TRIPLE="$(rustc -vV | awk '/^host:/ {print $2}')"
mkdir -p "${NATIVE_OUT}"

# One release build for one triple: native on the host, through zig everywhere
# else. Every artefact this script produces goes through here, so a change to
# how a triple is cross-built is a change in one place.
cargo_for_triple() {
  local t="$1"; shift
  if [[ "${t}" == "${HOST_TRIPLE}" ]]; then
    cargo build --release --target "${t}" "$@"
  else
    cargo zigbuild --release --target "${t}" "$@"
  fi
}

# The one file a `cargo_for_triple` build produced for `t`, copied to where a
# package or the release takes it from. Output that is not there stops the
# script: the build did not produce what it was asked for.
stage_built() {
  local t="$1" built="$2" dst="$3"
  local src="${CARGO_TARGET_DIR}/${t}/release/${built}"
  if [[ ! -f "${src}" ]]; then
    red "expected build output missing: ${src}"
    exit 1
  fi
  mkdir -p "$(dirname "${dst}")"
  cp "${src}" "${dst}"
  echo "  -> ${dst}"
}

for t in "${ALL_TRIPLES[@]}"; do
  step "building flz_core_ffi for ${t}"
  cargo_for_triple "${t}" -p flz_core_ffi --features jni
  stage_built "${t}" "${TRIPLE_TO_PREFIX[${t}]}flz_core_ffi.${TRIPLE_TO_EXT[${t}]}" \
              "${NATIVE_OUT}/${TRIPLE_TO_DIR[${t}]}/libflz_core_ffi.${TRIPLE_TO_EXT[${t}]}"
  # The Go SDK links the static library through cgo, on Linux and macOS.
  if go_static_triple "${t}"; then
    stage_built "${t}" "libflz_core_ffi.a" "${NATIVE_OUT}/${TRIPLE_TO_DIR[${t}]}/libflz_core_ffi.a"
  fi
done

# ---------------------------------------------------------------------------
# 4a. The fake controller, one per triple.
#
#     An SDK repository's smoke tier fetches this and drives its own runner
#     with it, so it ships beside the libraries and under the same manifest.
#     Built from the same crate with the `fake-controller` feature: the binary
#     and the ABI scenario read one module, so a tier that passes out there and
#     the suite here cannot be measuring two different wires.
# ---------------------------------------------------------------------------
for t in "${ALL_TRIPLES[@]}"; do
  step "building flz-fake-controller for ${t}"
  cargo_for_triple "${t}" -p flz_core_ffi --features fake-controller --bin flz-fake-controller
  bin="flz-fake-controller${TRIPLE_TO_BIN_EXT[${t}]}"
  stage_built "${t}" "${bin}" "${TOOLS_OUT}/${TRIPLE_TO_DIR[${t}]}/${bin}"
done

# ---------------------------------------------------------------------------
# 4f. The Elixir SDK's native library, one per triple.
#
#     Each archive holds the library under the archive's own name, which is
#     what `rustler_precompiled` extracts and loads, and the checksum file is
#     the map the package verifies a download against. An SDK release copies
#     that file into the package; a triple it names no archive for builds from
#     source.
# ---------------------------------------------------------------------------
rm -rf "${ELIXIR_NIFS_OUT}"
mkdir -p "${ELIXIR_NIFS_OUT}"
elixir_checksums=()
for t in "${ALL_TRIPLES[@]}"; do
  step "building flz_core_beam for ${t}"
  cargo_for_triple "${t}" -p flz_core_beam
  nif="$(elixir_nif_name "${t}" "${RELEASE_VERSION}")"
  stage_built "${t}" "${TRIPLE_TO_PREFIX[${t}]}flz_core_beam.${TRIPLE_TO_EXT[${t}]}" \
              "${ELIXIR_NIFS_OUT}/${nif}"
  tar -czf "${ELIXIR_NIFS_OUT}/${nif}.tar.gz" -C "${ELIXIR_NIFS_OUT}" "${nif}"
  rm "${ELIXIR_NIFS_OUT}/${nif}"
  sum="$(sha256sum "${ELIXIR_NIFS_OUT}/${nif}.tar.gz" | cut -d' ' -f1)"
  elixir_checksums+=("  \"${nif}.tar.gz\" => \"sha256:${sum}\"")
done
{
  echo "%{"
  ( IFS=$'\n'; echo "${elixir_checksums[*]}" ) | sed '$!s/$/,/'
  echo "}"
} > "${ELIXIR_NIFS_OUT}/${ELIXIR_NIF_CHECKSUMS}"
echo "  -> ${ELIXIR_NIFS_OUT}/${ELIXIR_NIF_CHECKSUMS}"

# ---------------------------------------------------------------------------
# 4c. The Node addons, one per row of the package's own platform table, and
#     the per-platform packages around them.
#
#     Its Windows row is `x86_64-pc-windows-msvc`, which zig cannot produce:
#     the addon has to be built on an MSVC host. A row this host cannot build
#     is named and skipped rather than filled with a binary of the wrong ABI,
#     and `--stage-release` refuses the incomplete set by name.
# ---------------------------------------------------------------------------
step "building the Node addons"
require_cmd node "install Node 20+"
# What this host cannot produce, collected across the Node, Python and Ruby arms
# and reported once. A release takes each from the job that runs on the right host.
UNBUILDABLE=()

( cd "${REPO_ROOT}/sdk/node" && npm ci --no-audit --no-fund --silent && node scripts/build.mjs )

while IFS='|' read -r suffix target; do
  [[ -n "${suffix}" ]] || continue
  echo
  bold "── building the ${suffix} addon (${target})"
  if ! rustup target list --installed 2>/dev/null | grep -qx "${target}"; then
    rustup target add "${target}" || true
  fi
  if ! cargo_for_triple "${target}" -p flz_core_ffi --features napi; then
    red "  cannot build ${target} on this host; the ${suffix} addon is not staged"
    UNBUILDABLE+=("${suffix} (${target})")
    continue
  fi
  case "${target}" in
    *windows*) built="flz_core_ffi.dll" ;;
    *apple*)   built="libflz_core_ffi.dylib" ;;
    *)         built="libflz_core_ffi.so" ;;
  esac
  stage_built "${target}" "${built}" "${NODE_ADDONS_OUT}/${suffix}/flz_core_ffi.node"
done < <(node_platforms)

step "packing the per-platform Node packages"
( cd "${REPO_ROOT}/sdk/node" \
  && node scripts/platform-package.mjs "${NODE_ADDONS_OUT}" "${NODE_PACKAGES_OUT}" )

# `npm pack` on each, so a release asset is one file with one checksum. The
# tarball npm publishes and the tarball the release carries are then the same
# bytes, which is the only way a consumer can check what they installed.
for package in "${NODE_PACKAGES_OUT}"/*/; do
  [[ -d "${package}" ]] || continue
  ( cd "${package}" && npm pack --pack-destination "${NODE_PACKAGES_OUT}" --silent >/dev/null )
  echo "  -> $(basename "${package%/}").tgz"
done

# ---------------------------------------------------------------------------
# 4d. The `abi3` wheels.
#
#     Built through the package's own `pyproject.toml`, which is the only way
#     the wheel carries the `flezha` project metadata rather than the crate's.
#     That means maturin builds `sdk/python/native`, the wrapper, and cargo
#     fetches the core at the tag it names. A release IS that tag, so the wheel
#     it publishes is built from exactly the source the tag holds; a build from
#     a tree whose tag is not reachable fails here, with cargo's own message.
#
#     This is the one step of this script that reaches the network. The
#     day-to-day loop does not: `scripts/build-python-sdk.sh` builds the
#     feature from the workspace and copies the library into the package.
# ---------------------------------------------------------------------------
step "building the wheels"
MATURIN="${REPO_ROOT}/sdk/python/.venv/bin/maturin"
if [[ ! -x "${MATURIN}" ]]; then
  red "maturin is not installed at ${MATURIN}"
  red "  scripts/build-python-sdk.sh creates the venv; then: .venv/bin/pip install maturin"
  exit 1
fi
for v in "${FREE_THREADED_PYTHONS[@]}"; do
  require_cmd "python${v}" "install the free-threaded CPython ${v%t} build"
done
mkdir -p "${WHEELS_OUT}"
for t in "${ALL_TRIPLES[@]}"; do
  cross=()
  [[ "${t}" == "${HOST_TRIPLE}" ]] || cross=(--zig)
  while IFS='|' read -r tag args; do
    echo
    bold "── building the ${t} ${tag} wheel"
    read -ra tier_args <<< "${args}"
    if ! ( cd "${REPO_ROOT}/sdk/python" &&
           "${MATURIN}" build --release --target "${t}" --out "${WHEELS_OUT}" "${cross[@]}" "${tier_args[@]}" ); then
      red "  cannot build the ${t} ${tag} wheel on this host"
      UNBUILDABLE+=("the ${t} ${tag} wheel")
    fi
  done < <(wheel_tiers)
done

# ---------------------------------------------------------------------------
# 4e. The precompiled Ruby gems.
#
#     A gem links the Ruby it loads into, so one gem per platform carries one
#     library per supported minor, built inside rb-sys-dock's image for that
#     platform against the gem's wrapper crate. The wrapper takes the core at
#     its git tag, so like the wheels this step builds from exactly what the tag
#     holds, and it reaches the network.
#
#     The gem is built from an export of the submodule's pinned commit rather
#     than from the checkout: rb-sys-dock mounts the gem's directory alone, and a
#     submodule's `.git` names a directory outside it, so git fails in the
#     container. The export is also what keeps local edits out of a release.
# ---------------------------------------------------------------------------
step "building the Ruby platform gems"
require_cmd docker "install Docker"
require_cmd rb-sys-dock "gem install rb_sys at the version sdk/ruby/flezha.gemspec pins"
RUBY_GEM_SOURCE="${REPO_ROOT}/_build/.dist/ruby-gem-source"
rm -rf "${RUBY_GEM_SOURCE}"
mkdir -p "${RUBY_GEM_SOURCE}" "${RUBY_GEMS_OUT}"
git -C "${REPO_ROOT}/sdk/ruby" archive HEAD | tar -x -C "${RUBY_GEM_SOURCE}"
# The container sees only the gem's directory and none of this environment, so
# it gets its own copy of the mirror and is told to use it before the build.
dock_prelude=()
if [[ -n "${FLEZHA_GIT_MIRROR:-}" ]]; then
  mkdir -p "${RUBY_GEM_SOURCE}/tmp"
  cp -r "${FLEZHA_GIT_MIRROR}" "${RUBY_GEM_SOURCE}/tmp/git-mirror"
  dock_prelude=(-- "export CARGO_NET_GIT_FETCH_WITH_CLI=true && git config --global url.file://${RUBY_GEM_SOURCE}/tmp/git-mirror/.insteadOf ${ORG_GIT_URL}")
fi
for t in "${ALL_TRIPLES[@]}"; do
  platform="${TRIPLE_TO_GEM_PLATFORM[${t}]}"
  echo
  bold "── building the ${platform} gem"
  if ( cd "${RUBY_GEM_SOURCE}" &&
       rb-sys-dock --platform "${platform}" --ruby-versions "${RUBY_GEM_MINORS}" --build "${dock_prelude[@]}" ); then
    cp -f "${RUBY_GEM_SOURCE}/pkg/flezha-${RELEASE_VERSION}-${platform}.gem" "${RUBY_GEMS_OUT}/"
  else
    red "  cannot build the ${platform} gem on this host"
    UNBUILDABLE+=("the ${platform} gem")
  fi
done

if (( ${#UNBUILDABLE[@]} > 0 )); then
  red "these artefacts need a host this one is not:"
  printf '  %s\n' "${UNBUILDABLE[@]}" >&2
  red "a release takes them from the job that runs on it; --stage-release refuses without them"
fi

# ---------------------------------------------------------------------------
# 4b. Stage the same five binaries into the PHP package.
#
#     The PHP SDK loads the ABI through `ext-ffi`, so it needs the library
#     itself rather than a JNI-carrying jar — and it is the SAME library, which
#     is the point of D2: one binary per triple, not one per PHP version times
#     thread-safety times triple. Copied from what step 4 already built rather
#     than rebuilt, so the two distributions cannot carry different cores.
# ---------------------------------------------------------------------------
step "staging the native libraries into the PHP package"
for t in "${ALL_TRIPLES[@]}"; do
  dir="${TRIPLE_TO_DIR[${t}]}"
  ext="${TRIPLE_TO_EXT[${t}]}"
  mkdir -p "${PHP_NATIVE_OUT}/${dir}"
  cp "${NATIVE_OUT}/${dir}/libflz_core_ffi.${ext}" \
     "${PHP_NATIVE_OUT}/${dir}/${TRIPLE_TO_PREFIX[${t}]}flz_core_ffi.${ext}"
  echo "  -> ${PHP_NATIVE_OUT}/${dir}"
done

if [[ -n "${TRIPLES}" ]]; then
  # The triples were chosen, so anything among them that did not build failed.
  (( ${#UNBUILDABLE[@]} == 0 )) || exit 1
  green "Built the ${TRIPLES} artefacts; the fat jars need all five triples and were left out."
  exit 0
fi

# ---------------------------------------------------------------------------
# 5. Build the SDK fat jar (-P native-build enforces all 5 natives present;
#    every triple was just built above, so it stages nothing and fetches
#    nothing).
# ---------------------------------------------------------------------------
step "building SDK fat jar"
# `clean` keeps the published jar a cold build: the resources plugin copies into
# target/classes and never deletes, so a native triple removed from the source
# tree would otherwise still ship.
mvn -B -q -f sdk/java/pom.xml -P native-build -DskipTests clean install

mkdir -p "${SDK_DIST}"
cp -f sdk/java/flezha-sdk/target/flezha-sdk-*.jar "${SDK_DIST}/flezha-sdk.jar"
echo "  -> ${SDK_DIST}/flezha-sdk.jar"

# ---------------------------------------------------------------------------
# 6. Build the test runner fat jar.
# ---------------------------------------------------------------------------
step "building test runner fat jar"
mvn -B -q -f test/apps/java_runner/pom.xml -DskipTests clean package

mkdir -p "${TEST_APP_DIST}"
cp -f test/apps/java_runner/target/java_runner.jar \
      "${TEST_APP_DIST}/java_runner.jar"
echo "  -> ${TEST_APP_DIST}/java_runner.jar"

# ---------------------------------------------------------------------------
# Done.
# ---------------------------------------------------------------------------
echo
green "============================================================"
green "  All distribution artifacts built."
green "============================================================"
echo
echo "None of it is tracked in git. To publish a release, stage the assets with:"
echo "  scripts/build-dist.sh --stage-release dist-release"
