#!/usr/bin/env bash
#
# Publish a flezha controller image to Docker Hub.
#
# *Operator-driven, not invoked by CI.* Builds NATIVELY for the host CPU only
# (no QEMU, no buildx --platform). Each invocation pushes a per-architecture
# tag, then rebuilds the shared manifest list (`:latest` / `:latest-dev`) from
# whichever per-arch tags currently exist on Docker Hub — so building on amd64
# updates the amd64 variant and building on arm64 updates the arm64 variant,
# and neither overwrites the other.
#
# A prod build additionally publishes the immutable version tag taken from
# mix.exs (`:<version>-<arch>` + the `:<version>` manifest). That is the tag a
# cluster pins — `:latest` is for convenience only and must never be what a
# cluster runs. Immutable means it: a prod publish refuses to overwrite a
# per-arch version tag that already exists on Docker Hub, so "changed the
# controller, forgot to bump mix.exs" fails here in seconds instead of
# silently redirecting a pinned tag. REPUBLISH=1 overrides it to repair a
# broken push.
#
# process_hub is fetched from hex at the version mix.lock pins, so a plain git
# checkout is all the build context needs.
#
# Usage (logged in as alfetahe — `docker login -u alfetahe`):
#   scripts/publish-image.sh            # prod (default): :<version> + :latest manifests
#   scripts/publish-image.sh --prod     # same as default
#   scripts/publish-image.sh --dev      # dev: :latest-dev-<arch> + :latest-dev manifest
#   REPUBLISH=1 scripts/publish-image.sh  # overwrite an already-published version tag
#
# Caching: layer cache + the Dockerfile's `--mount=type=cache` mounts (fast
# incremental Rust/Elixir) and `--pull` to refresh base images. Pass NO_CACHE=1
# to force a clean rebuild when bumping the base image or toolchain.
#
# --provenance=false --sbom=false: keep each per-arch push a plain single-image
# manifest. BuildKit otherwise attaches a provenance attestation, turning the
# pushed tag into an image index that `docker manifest create` rejects with
# "is a manifest list".
set -euo pipefail

IMAGE="alfetahe/flezha"

mode="prod"
case "${1:-}" in
  ""|--prod) mode="prod" ;;
  --dev)     mode="dev" ;;
  -h|--help) echo "Usage: $0 [--prod|--dev]"; exit 0 ;;
  *) echo "ERROR: unknown argument '$1' (expected --prod or --dev)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  aarch64|arm64) arch="arm64" ;;
  x86_64|amd64)  arch="amd64" ;;
  *) echo "ERROR: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

cd "$(dirname "$0")/.."

if [ "$mode" = "prod" ]; then
  dockerfile="docker/prod/Dockerfile.controller"
  version=$(sed -n 's/^ *version: *"\([^"]*\)".*/\1/p' mix.exs | head -1)
  [ -n "$version" ] || { echo "ERROR: could not read version from mix.exs" >&2; exit 1; }
  # The immutable version tag first: it is what the k8s manifests pin.
  families=("$version" "latest")
else
  dockerfile="docker/dev/Dockerfile.runtime"
  families=("latest-dev")
fi

[ -f "$dockerfile" ] || { echo "ERROR: $dockerfile not found (run from the flezha repo)" >&2; exit 1; }

build_tag="$IMAGE:${families[0]}-$arch"

# Per-arch, not the manifest list: the first arm64 publish after an amd64 one
# must still go through. Checked before the build so the refusal is cheap.
if [ "$mode" = "prod" ] && [ "${REPUBLISH:-0}" != "1" ] \
   && docker manifest inspect "$build_tag" >/dev/null 2>&1; then
  echo "ERROR: $build_tag is already published — version tags are immutable." >&2
  echo "       Bump 'version:' in mix.exs, or REPUBLISH=1 to overwrite a broken push." >&2
  exit 1
fi

echo "==> Building $build_tag ($mode, native $arch)"
DOCKER_BUILDKIT=1 docker build --pull --provenance=false --sbom=false ${NO_CACHE:+--no-cache} -f "$dockerfile" -t "$build_tag" .
echo "    size: $(docker image inspect "$build_tag" --format '{{.Size}}' | awk '{printf "%.1f MiB", $1/1024/1024}')"

for family in "${families[@]}"; do
  arch_tag="$IMAGE:$family-$arch"
  manifest_tag="$IMAGE:$family"

  docker tag "$build_tag" "$arch_tag"
  echo "==> Pushing $arch_tag"
  docker push "$arch_tag"

  echo "==> Rebuilding manifest $manifest_tag from published per-arch tags"
  members=()
  for a in amd64 arm64; do
    v="$IMAGE:$family-$a"
    if docker manifest inspect "$v" >/dev/null 2>&1; then
      members+=("$v")
      echo "    include $v"
    fi
  done
  docker manifest rm "$manifest_tag" >/dev/null 2>&1 || true
  docker manifest create "$manifest_tag" "${members[@]}"
  docker manifest push "$manifest_tag"
  echo "Done. $manifest_tag → ${members[*]}"
done
