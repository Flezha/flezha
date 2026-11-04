#!/usr/bin/env bash
# Plugin packages in the reference Kubernetes deployment, on a throwaway
# single-node k3s cluster running in Docker:
#
#   1. A declared, pinned package (a ConfigMap holding the archive, mounted
#      read-only at /pkgs) loads, and its archive is on the recovery volume.
#   2. `flz plugin install` is refused on the node with `plugin.list_declared`.
#   3. Emptying FLEZHA_PLUGINS and restarting the DaemonSet removes it.
#
# The cluster is `k3s_cluster.sh`'s, with a kubeconfig of its own.
# Needs the runtime image and the example archive `plugin_package_docker.sh`
# builds; pass them in, or it builds both:
#
#   RUNTIME_IMAGE=flezha/flezha:x ARCHIVE=/path/flezha_example_plugin-0.1.0.fpz \
#     test/scripts/plugin_package_k3s.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="pkgk3s$$"
WORK="$(mktemp -d)"
BUILT=()

# shellcheck source=test/scripts/k3s_cluster.sh
source "$REPO_ROOT/test/scripts/k3s_cluster.sh"

note() { echo "[plugin_package_k3s] $*"; }
fail() {
  note "FAIL: $*"
  k3s_dump
  exit 1
}

cleanup() {
  k3s_down
  for image in "${BUILT[@]}"; do docker rmi -f "$image" >/dev/null 2>&1 || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

if [ -z "${RUNTIME_IMAGE:-}" ] || [ -z "${ARCHIVE:-}" ]; then
  note "building the runtime image and the example archive"
  RUNTIME_IMAGE="flezha/flezha:${RUN_ID}"
  BUILDER="flezha/plugin-builder:${RUN_ID}"
  BUILT+=("$RUNTIME_IMAGE" "$BUILDER")
  docker build -q -f "$REPO_ROOT/deploy/docker/prod/Dockerfile.controller" -t "$RUNTIME_IMAGE" "$REPO_ROOT" >/dev/null
  docker build -q --target plugin-builder -f "$REPO_ROOT/deploy/docker/prod/Dockerfile.controller" \
    -t "$BUILDER" "$REPO_ROOT" >/dev/null
  k3s_package "$BUILDER" test/apps/example_plugin "$WORK/out"
  ARCHIVE="$WORK/out/flezha_example_plugin-0.1.0.fpz"
fi

FILE="$(basename "$ARCHIVE")"
DIGEST="$(sha256sum "$ARCHIVE" | cut -d' ' -f1)"

note "starting ${K3S_IMAGE}"
k3s_up "$RUN_ID"

note "importing ${RUNTIME_IMAGE} as the manifests' image"
k3s_import "$RUNTIME_IMAGE" flezha/flezha:0.1.1

note "applying the reference manifests with the plugin patch"
for manifest in namespace role serviceaccount rolebinding configmap; do
  kc apply -f "$REPO_ROOT/deploy/kubernetes/flezha-${manifest}.yaml" >/dev/null
done
kc -n flezha create secret generic flezha-secrets \
  --from-literal=RELEASE_COOKIE="$(head -c 48 /dev/urandom | base64 | tr -d '\n')" >/dev/null
kc -n flezha create configmap flezha-pkgs --from-file="$ARCHIVE" >/dev/null
# This tier has no token issuer; the listing below is a gated verb.
kc -n flezha patch configmap flezha-config --type merge \
  -p "{\"data\":{\"FLEZHA_AUTHZ_ENABLED\":\"false\",\"FLEZHA_PLUGINS\":\"/pkgs/${FILE}#sha256=${DIGEST}\"}}" >/dev/null
kc apply -f "$REPO_ROOT/deploy/kubernetes/flezha-daemonset.yaml" >/dev/null
kc -n flezha patch daemonset flezha-daemonset --type json -p '[
  {"op":"add","path":"/spec/template/spec/volumes/-","value":{"name":"pkgs","configMap":{"name":"flezha-pkgs"}}},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"pkgs","mountPath":"/pkgs","readOnly":true}}
]' >/dev/null

await_pod() {
  kc -n flezha rollout status daemonset/flezha-daemonset --timeout=300s >/dev/null || fail "the DaemonSet did not roll out"
  POD="$(kc -n flezha get pods -l app=flezha -o jsonpath='{.items[0].metadata.name}')"
  kc -n flezha wait --for=condition=Ready "pod/${POD}" --timeout=300s >/dev/null || fail "the pod is not ready"
}

pod_flz() { kc -n flezha exec "$POD" -c flezha -- /flezha/bin/flz "$@"; }

await_pod
note "scenario 1: the declared package is loaded and archived on the recovery volume"
listing="$(pod_flz plugin list --installed)"
echo "$listing"
grep -q "flezha_example_plugin 0.1.0  loaded" <<<"$listing" || fail "the package is not loaded"
archives="$(kc -n flezha exec "$POD" -c flezha -- ls /var/lib/flezha/plugins)"
grep -q "flezha_example_plugin.fpz" <<<"$archives" || fail "the archive is not under /var/lib/flezha/plugins"

note "scenario 2: install is refused on a node whose list is declared"
if refusal="$(pod_flz plugin install "/pkgs/${FILE}" 2>&1)"; then fail "install succeeded"; fi
echo "$refusal"
grep -q "plugin.list_declared" <<<"$refusal" || fail "the refusal does not name plugin.list_declared"

note "scenario 3: an emptied list and a rollout restart remove the package"
kc -n flezha patch configmap flezha-config --type merge -p '{"data":{"FLEZHA_PLUGINS":""}}' >/dev/null
kc -n flezha rollout restart daemonset/flezha-daemonset >/dev/null
sleep 5
await_pod
listing="$(pod_flz plugin list --installed)"
echo "$listing"
grep -q "No plugin packages installed." <<<"$listing" || fail "a package is still listed"
archives="$(kc -n flezha exec "$POD" -c flezha -- ls /var/lib/flezha/plugins)"
if grep -q "\.fpz" <<<"$archives"; then fail "an archive is still in the plugin root"; fi

note "PASS"
