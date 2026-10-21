#!/usr/bin/env bash
# Plugin packages in the prod image: a package built in the `plugin-builder`
# stage loads in the runtime image built from the same Dockerfile, in each of
# the three deployment shapes, and a declared list that cannot be fetched keeps
# the controller down.
#
#   1. Build the runtime image and the `plugin-builder` target from the working
#      tree, and package the example plugin inside the builder.
#   2. Assert the package's `built_with.otp` is the runtime image's OTP release.
#   3. Named volume: `docker exec ... flz plugin install`, then `docker restart`.
#   4. `FLEZHA_PLUGINS` naming the archive on a read-only bind mount, pinned.
#   5. A derived image carrying the archive, with no volume over the root.
#   6. `FLEZHA_PLUGINS` naming an unreachable URL: the container exits non-zero
#      naming `plugin.fetch_failed` and never starts the controller.
#
# Every container, volume and image it creates is removed on exit.
#
# Usage:
#   test/scripts/plugin_package_docker.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="pkgtest$$"
RUNTIME="flezha/flezha:${RUN_ID}"
BUILDER="flezha/plugin-builder:${RUN_ID}"
DERIVED="flezha/flezha:${RUN_ID}-derived"
WORK="$(mktemp -d)"
CONTAINERS=()
VOLUMES=()

note() { echo "[plugin_package_docker] $*"; }
fail() { note "FAIL: $*"; exit 1; }

cleanup() {
  for c in "${CONTAINERS[@]}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  for v in "${VOLUMES[@]}"; do docker volume rm -f "$v" >/dev/null 2>&1 || true; done
  docker rmi -f "$DERIVED" "$BUILDER" "$RUNTIME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# A controller that needs no durability backend, no token and no cluster.
RUN_ENV=(-e FLEZHA_DURABILITY_BACKEND=none -e FLEZHA_AUTHZ_ENABLED=false)

start() {
  local name="$1"
  shift
  CONTAINERS+=("$name")
  docker run -d --name "$name" "${RUN_ENV[@]}" "$@" >/dev/null
}

await_ready() {
  docker exec "$1" /flezha/bin/flz boot await --timeout 120 >/dev/null \
    || { docker logs "$1" | tail -40; fail "$1 did not report ready"; }
}

assert_loaded() {
  local listing
  listing="$(docker exec "$1" /flezha/bin/flz plugin list --installed)"
  echo "$listing"
  grep -q "flezha_example_plugin 0.1.0  loaded" <<<"$listing" || fail "$1 did not load the package"
}

note "building the runtime image and the plugin-builder target"
docker build -q -f "$REPO_ROOT/docker/prod/Dockerfile.controller" -t "$RUNTIME" "$REPO_ROOT" >/dev/null
docker build -q --target plugin-builder -f "$REPO_ROOT/docker/prod/Dockerfile.controller" \
  -t "$BUILDER" "$REPO_ROOT" >/dev/null

# What compiling flezha as a dependency reads, and the example beside it at the
# paths its mix.exs names: no build products, no host-built deps.
note "packaging the example plugin inside the builder"
mkdir -p "$WORK/src" "$WORK/out"
(cd "$REPO_ROOT" && tar -cf - \
  --exclude='_build' --exclude='deps' --exclude='target' \
  mix.exs mix.lock config lib crates/protobufs rel \
  crates/flz_transport/errors.json crates/flz_core/src/schema.rs crates/flz_core_ffi/src/veneer/shape.rs \
  openspec/specs/java-sdk/spec.md test/fixtures sdk/elixir test/apps/example_plugin) \
  | tar -xf - -C "$WORK/src"

docker run --rm -v "$WORK/src:/src" -v "$WORK/out:/out" -w /src/examples/elixir/plugin "$BUILDER" \
  sh -c "mix deps.get >/dev/null && mix flezha.plugin.package --output /out && chown -R $(id -u):$(id -g) /src /out"

ARCHIVE="$WORK/out/flezha_example_plugin-0.1.0.fpz"
[ -f "$ARCHIVE" ] || fail "the builder wrote no archive"
DIGEST="$(sha256sum "$ARCHIVE" | cut -d' ' -f1)"

PACKAGE_OTP="$(tar -xzOf "$ARCHIVE" manifest.exs | sed -n 's/.*otp: "\([0-9]*\)".*/\1/p')"
RUNTIME_OTP="$(docker run --rm --entrypoint /flezha/bin/flzvm "$RUNTIME" eval 'IO.puts(System.otp_release())' | tail -1)"
note "package built on OTP ${PACKAGE_OTP}, runtime image runs OTP ${RUNTIME_OTP}"
[ -n "$PACKAGE_OTP" ] && [ "$PACKAGE_OTP" = "$RUNTIME_OTP" ] || fail "the builder's OTP is not the image's"

note "shape 1: a named volume, flz plugin install, docker restart"
VOLUMES+=("${RUN_ID}_plugins")
docker volume create "${RUN_ID}_plugins" >/dev/null
start "${RUN_ID}_volume" -v "${RUN_ID}_plugins:/flezha/storage/app/plugins" -v "$WORK/out:/pkgs:ro" "$RUNTIME"
await_ready "${RUN_ID}_volume"
docker exec "${RUN_ID}_volume" /flezha/bin/flz plugin install /pkgs/flezha_example_plugin-0.1.0.fpz
docker restart "${RUN_ID}_volume" >/dev/null
await_ready "${RUN_ID}_volume"
assert_loaded "${RUN_ID}_volume"

note "shape 2: FLEZHA_PLUGINS naming a pinned archive on a read-only bind mount"
start "${RUN_ID}_declared" -v "$WORK/out:/pkgs:ro" \
  -e "FLEZHA_PLUGINS=/pkgs/flezha_example_plugin-0.1.0.fpz#sha256=${DIGEST}" "$RUNTIME"
await_ready "${RUN_ID}_declared"
declared_logs="$(docker logs "${RUN_ID}_declared" 2>&1)"
grep -q "syncing the plugin root" <<<"$declared_logs" || fail "the entrypoint did not sync"
assert_loaded "${RUN_ID}_declared"

note "shape 3: a derived image carrying the archive"
mkdir -p "$WORK/derived"
cp "$ARCHIVE" "$WORK/derived/"
cat >"$WORK/derived/Dockerfile" <<EOF
FROM ${RUNTIME}
COPY --chown=flezha:flezha flezha_example_plugin-0.1.0.fpz /flezha/storage/app/plugins/
EOF
docker build -q -t "$DERIVED" "$WORK/derived" >/dev/null
start "${RUN_ID}_derived" "$DERIVED"
await_ready "${RUN_ID}_derived"
assert_loaded "${RUN_ID}_derived"

note "an unreachable declared package keeps the controller down"
start "${RUN_ID}_unreachable" -e "FLEZHA_PLUGINS=https://127.0.0.1:1/flezha_example_plugin-0.1.0.fpz" "$RUNTIME"
status="$(docker wait "${RUN_ID}_unreachable")"
logs="$(docker logs "${RUN_ID}_unreachable" 2>&1)"
echo "$logs" | tail -5
[ "$status" != "0" ] || fail "the container exited 0"
grep -q "plugin.fetch_failed" <<<"$logs" || fail "the exit does not name plugin.fetch_failed"
if grep -q "waiting up to" <<<"$logs"; then fail "the controller was started"; fi

note "PASS"
