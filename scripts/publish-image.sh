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
# process_hub is fetched from its GitHub `dev` branch at build time
# (mix.exs default), so a plain git checkout is all the build context needs.
#
# Usage (logged in as alfetahe — `docker login -u alfetahe`):
#   scripts/publish-image.sh            # prod (default): :latest-<arch> + :latest manifest
#   scripts/publish-image.sh --prod     # same as default
#   scripts/publish-image.sh --dev      # dev: :latest-dev-<arch> + :latest-dev manifest
#
# Caching: layer cache + the Dockerfile's `--mount=type=cache` mounts (fast
# incremental Rust/Elixir) and `--pull` to refresh base images. Pass NO_CACHE=1
# to force a clean rebuild when bumping the base image or toolchain.
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

if [ "$mode" = "prod" ]; then
  dockerfile="docker/prod/Dockerfile.controller"
  arch_tag="$IMAGE:latest-$arch"
  manifest_tag="$IMAGE:latest"
  variants=("$IMAGE:latest-amd64" "$IMAGE:latest-arm64")
else
  dockerfile="docker/dev/Dockerfile.runtime"
  arch_tag="$IMAGE:latest-dev-$arch"
  manifest_tag="$IMAGE:latest-dev"
  variants=("$IMAGE:latest-dev-amd64" "$IMAGE:latest-dev-arm64")
fi

cd "$(dirname "$0")/.."
[ -f "$dockerfile" ] || { echo "ERROR: $dockerfile not found (run from the flezha repo)" >&2; exit 1; }

echo "==> Building $arch_tag ($mode, native $arch)"
DOCKER_BUILDKIT=1 docker build --pull ${NO_CACHE:+--no-cache} -f "$dockerfile" -t "$arch_tag" .
echo "    size: $(docker image inspect "$arch_tag" --format '{{.Size}}' | awk '{printf "%.1f MiB", $1/1024/1024}')"

echo "==> Pushing $arch_tag"
docker push "$arch_tag"

echo "==> Rebuilding manifest $manifest_tag from published per-arch tags"
members=()
for v in "${variants[@]}"; do
  if docker manifest inspect "$v" >/dev/null 2>&1; then
    members+=("$v")
    echo "    include $v"
  fi
done
docker manifest rm "$manifest_tag" >/dev/null 2>&1 || true
docker manifest create "$manifest_tag" "${members[@]}"
docker manifest push "$manifest_tag"
echo "Done. $manifest_tag → ${members[*]}"
