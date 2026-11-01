# shellcheck shell=bash
# A throwaway k3s cluster in Docker, for the scripts beside this file. Source
# it, then:
#
#   k3s_up <name> [agents]    a server and <agents> agent nodes; exports KUBECONFIG
#   k3s_import <image> [tag]  loads a local image into every node, tagged <tag> too
#   k3s_down                  removes the nodes, their network and the kubeconfig
#   k3s_dump                  prints the controller pods and every container's recent logs
#   kc ...                    kubectl against the cluster
#
# The kubeconfig is a temp file of its own, never the default one. k3s's own
# network policy controller enforces NetworkPolicy objects.

K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.31.4-k3s1}"
K3S_NODES=()
K3S_NAME=""
K3S_DIR=""

kc() { kubectl --kubeconfig "$KUBECONFIG" "$@"; }

k3s_up() {
  local name=$1 agents=${2:-0} token port i
  K3S_NAME=$name
  K3S_DIR="$(mktemp -d)"
  export KUBECONFIG="$K3S_DIR/kubeconfig"
  token="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"

  docker network create "${name}_net" >/dev/null
  docker run -d --privileged --name "${name}_server" --hostname "${name}-server" \
    --network "${name}_net" -p 127.0.0.1::6443 -e K3S_TOKEN="$token" "$K3S_IMAGE" \
    server --disable traefik --disable metrics-server >/dev/null
  K3S_NODES+=("${name}_server")
  for i in $(seq 1 "$agents"); do
    docker run -d --privileged --name "${name}_agent$i" --hostname "${name}-agent$i" \
      --network "${name}_net" -e K3S_URL="https://${name}_server:6443" -e K3S_TOKEN="$token" \
      "$K3S_IMAGE" agent >/dev/null
    K3S_NODES+=("${name}_agent$i")
  done

  port="$(docker port "${name}_server" 6443/tcp | head -1 | cut -d: -f2)"
  for _ in $(seq 1 60); do
    docker exec "${name}_server" test -f /etc/rancher/k3s/k3s.yaml 2>/dev/null && break
    sleep 2
  done
  docker exec "${name}_server" cat /etc/rancher/k3s/k3s.yaml |
    sed "s#https://127.0.0.1:6443#https://127.0.0.1:${port}#" >"$KUBECONFIG"
  for _ in $(seq 1 90); do
    [ "$(kc get nodes -o name 2>/dev/null | wc -l)" -ge $((agents + 1)) ] && break
    sleep 2
  done
  kc wait --for=condition=Ready node --all --timeout=180s >/dev/null
}

k3s_import() {
  local image=$1 tag=${2:-} node
  for node in "${K3S_NODES[@]}"; do
    docker save "$image" | docker exec -i "$node" ctr -n k8s.io images import - >/dev/null
    [ -z "$tag" ] || docker exec "$node" ctr -n k8s.io images tag "docker.io/${image}" "docker.io/${tag}" >/dev/null
  done
}

k3s_dump() {
  kc -n flezha get pods -o wide || true
  kc -n flezha logs -l app=flezha --all-containers --prefix --tail=40 --previous 2>/dev/null ||
    kc -n flezha logs -l app=flezha --all-containers --prefix --tail=40 || true
}

k3s_down() {
  local node
  for node in "${K3S_NODES[@]}"; do docker rm -fv "$node" >/dev/null 2>&1 || true; done
  [ -z "$K3S_NAME" ] || docker network rm "${K3S_NAME}_net" >/dev/null 2>&1 || true
  [ -z "$K3S_DIR" ] || rm -rf "$K3S_DIR"
}
