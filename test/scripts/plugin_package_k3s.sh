#!/usr/bin/env bash
# Plugin packages in the reference Kubernetes deployment, on a throwaway
# single-node k3s cluster running in Docker:
#
#   1. A declared, pinned package (a ConfigMap holding the archive, mounted
#      read-only at /pkgs) loads, and its archive is on the recovery volume.
#   2. `flz plugin install` is refused on the node with `plugin.list_declared`.
#   3. Emptying FLEZHA_PLUGINS and restarting the DaemonSet removes it.
#
# Uses a kubeconfig of its own in a temp directory, never the default one.
# Needs the runtime image and the example archive `plugin_package_docker.sh`
# builds; pass them in, or it builds both:
#
#   RUNTIME_IMAGE=flezha/flezha:x ARCHIVE=/path/flezha_example_plugin-0.1.0.fpz \
#     test/scripts/plugin_package_k3s.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="pkgk3s$$"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.31.4-k3s1}"
WORK="$(mktemp -d)"
CLUSTER="${RUN_ID}_k3s"
BUILT=()

note() { echo "[plugin_package_k3s] $*"; }
fail() {
  note "FAIL: $*"
  kc -n flezha get pods -o wide || true
  kc -n flezha logs -l app=flezha -c flezha --tail=40 --previous 2>/dev/null \
    || kc -n flezha logs -l app=flezha -c flezha --tail=40 || true
  exit 1
}

cleanup() {
  docker rm -fv "$CLUSTER" >/dev/null 2>&1 || true
  for image in "${BUILT[@]}"; do docker rmi -f "$image" >/dev/null 2>&1 || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

export KUBECONFIG="$WORK/kubeconfig"
kc() { kubectl --kubeconfig "$KUBECONFIG" "$@"; }

if [ -z "${RUNTIME_IMAGE:-}" ] || [ -z "${ARCHIVE:-}" ]; then
  note "building the runtime image and the example archive"
  RUNTIME_IMAGE="flezha/flezha:${RUN_ID}"
  BUILDER="flezha/plugin-builder:${RUN_ID}"
  BUILT+=("$RUNTIME_IMAGE" "$BUILDER")
  docker build -q -f "$REPO_ROOT/docker/prod/Dockerfile.controller" -t "$RUNTIME_IMAGE" "$REPO_ROOT" >/dev/null
  docker build -q --target plugin-builder -f "$REPO_ROOT/docker/prod/Dockerfile.controller" \
    -t "$BUILDER" "$REPO_ROOT" >/dev/null
  mkdir -p "$WORK/src" "$WORK/out"
  (cd "$REPO_ROOT" && tar -cf - \
    --exclude='_build' --exclude='deps' --exclude='target' \
    mix.exs mix.lock config lib protobufs rel \
    flz_transport/errors.json flz_core/src/schema.rs flz_core_ffi/src/veneer/shape.rs \
    openspec/specs/java-sdk/spec.md test/fixtures sdk/elixir examples/elixir/plugin) \
    | tar -xf - -C "$WORK/src"
  docker run --rm -v "$WORK/src:/src" -v "$WORK/out:/out" -w /src/examples/elixir/plugin "$BUILDER" \
    sh -c "mix deps.get >/dev/null && mix flezha.plugin.package --output /out && chown -R $(id -u):$(id -g) /src /out"
  ARCHIVE="$WORK/out/flezha_example_plugin-0.1.0.fpz"
fi

FILE="$(basename "$ARCHIVE")"
DIGEST="$(sha256sum "$ARCHIVE" | cut -d' ' -f1)"

note "starting ${K3S_IMAGE}"
docker run -d --privileged --name "$CLUSTER" -p 127.0.0.1::6443 "$K3S_IMAGE" \
  server --disable traefik --disable metrics-server >/dev/null
PORT="$(docker port "$CLUSTER" 6443/tcp | head -1 | cut -d: -f2)"

for _ in $(seq 1 60); do
  docker exec "$CLUSTER" test -f /etc/rancher/k3s/k3s.yaml 2>/dev/null && break
  sleep 2
done
docker exec "$CLUSTER" cat /etc/rancher/k3s/k3s.yaml | sed "s#https://127.0.0.1:6443#https://127.0.0.1:${PORT}#" >"$KUBECONFIG"
for _ in $(seq 1 90); do
  [ -n "$(kc get nodes -o name 2>/dev/null)" ] && break
  sleep 2
done
kc wait --for=condition=Ready node --all --timeout=180s >/dev/null

note "importing ${RUNTIME_IMAGE} as the manifests' image"
docker save "$RUNTIME_IMAGE" | docker exec -i "$CLUSTER" ctr -n k8s.io images import - >/dev/null
docker exec "$CLUSTER" ctr -n k8s.io images tag "docker.io/${RUNTIME_IMAGE}" docker.io/flezha/flezha:0.1.0 >/dev/null

note "applying the reference manifests with the plugin patch"
for manifest in namespace role serviceaccount rolebinding configmap; do
  kc apply -f "$REPO_ROOT/kubernetes/flezha-${manifest}.yaml" >/dev/null
done
kc -n flezha create secret generic flezha-secrets \
  --from-literal=RELEASE_COOKIE="$(head -c 48 /dev/urandom | base64 | tr -d '\n')" >/dev/null
kc -n flezha create configmap flezha-pkgs --from-file="$ARCHIVE" >/dev/null
# This tier has no token issuer; the listing below is a gated verb.
kc -n flezha patch configmap flezha-config --type merge \
  -p "{\"data\":{\"FLEZHA_AUTHZ_ENABLED\":\"false\",\"FLEZHA_PLUGINS\":\"/pkgs/${FILE}#sha256=${DIGEST}\"}}" >/dev/null
kc apply -f "$REPO_ROOT/kubernetes/flezha-daemonset.yaml" >/dev/null
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
