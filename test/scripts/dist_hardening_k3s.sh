#!/usr/bin/env bash
# The reference Kubernetes manifests' cluster hardening, on a throwaway
# two-node k3s cluster in Docker:
#
#   1. The controllers form a cluster on the pinned distribution port and the
#      health probes pass, while a pod without the controller or console label
#      cannot reach EPMD or the distribution port, and a console-labelled one can.
#   2. A key file the controller's user wrote stays 0600, and the host
#      directories the init container prepares stay closed to other users,
#      across a pod restart.
#   3. With the cert-manager recipe in deploy/dist-tls/kubernetes/, the two
#      controller pods join over mutual TLS; a pod holding a certificate of the
#      distribution authority and the cookie connects, and one holding a
#      certificate of another authority and the cookie is refused.
#
# The cluster is `k3s_cluster.sh`'s, with a kubeconfig of its own. Needs the
# runtime image built from this tree; pass it in, or it builds it:
#
#   RUNTIME_IMAGE=flezha/flezha:x test/scripts/dist_hardening_k3s.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="disthard$$"
CERT_MANAGER="https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml"
WORK="$(mktemp -d)"
BUILT=()

# shellcheck source=test/scripts/k3s_cluster.sh
source "$REPO_ROOT/test/scripts/k3s_cluster.sh"

note() { echo "[dist_hardening_k3s] $*"; }
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

if [ -z "${RUNTIME_IMAGE:-}" ]; then
  note "building the runtime image"
  RUNTIME_IMAGE="flezha/flezha:${RUN_ID}"
  BUILT+=("$RUNTIME_IMAGE")
  docker build -q -f "$REPO_ROOT/deploy/docker/prod/Dockerfile.controller" -t "$RUNTIME_IMAGE" "$REPO_ROOT" >/dev/null
fi

note "starting a two-node ${K3S_IMAGE} cluster"
k3s_up "$RUN_ID" 1
k3s_import "$RUNTIME_IMAGE" flezha/flezha:0.1.0

COOKIE="$(head -c 48 /dev/urandom | base64 | tr -d '\n/+=')"
note "applying the reference manifests"
for manifest in namespace role serviceaccount rolebinding configmap; do
  kc apply -f "$REPO_ROOT/deploy/kubernetes/flezha-${manifest}.yaml" >/dev/null
done
kc -n flezha create secret generic flezha-secrets --from-literal=RELEASE_COOKIE="$COOKIE" >/dev/null
kc apply -f "$REPO_ROOT/deploy/kubernetes/flezha-daemonset.yaml" >/dev/null
kc apply -f "$REPO_ROOT/deploy/kubernetes/flezha-networkpolicy.yaml" >/dev/null

await_pods() {
  kc -n flezha rollout status daemonset/flezha-daemonset --timeout=300s >/dev/null ||
    fail "the DaemonSet did not roll out"
  kc -n flezha wait --for=condition=Ready pod -l app=flezha --timeout=300s >/dev/null ||
    fail "a controller pod is not ready"
  mapfile -t PODS < <(kc -n flezha get pods -l app=flezha -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  [ "${#PODS[@]}" -eq 2 ] || fail "expected two controller pods, found ${#PODS[@]}"
}

pod_rpc() { kc -n flezha exec "$1" -c flezha -- /flezha/bin/flzvm rpc "$2"; }
pod_ip() { kc -n flezha get pod "$1" -o jsonpath='{.status.podIP}'; }

# The node container a pod runs on: node names are the containers' hostnames.
node_of() { kc -n flezha get pod "$1" -o jsonpath='{.spec.nodeName}' | tr - _; }

clustered() {
  local seen
  for _ in $(seq 1 30); do
    seen="$(pod_rpc "${PODS[0]}" 'IO.puts(length(Node.list()))' 2>/dev/null || true)"
    [ "${seen:-0}" -ge 1 ] && return 0
    sleep 2
  done
  return 1
}

# Whether a throwaway pod with `labels` reaches `ip`:`port` over TCP.
reaches() {
  local labels=$1 ip=$2 port=$3 name="probe-$RANDOM" out
  out="$(kc -n flezha run "$name" --image=busybox:1.36.1 --restart=Never --labels="$labels" --rm -i \
    --command -- sh -c "nc -w 3 $ip $port </dev/null >/dev/null 2>&1 && echo open || echo closed" 2>/dev/null)"
  grep -q '^open' <<<"$out"
}

await_pods
note "scenario 1: the controllers cluster on port 9100 and the health probes pass"
clustered || fail "the controllers did not connect"
ports="$(kc -n flezha exec "${PODS[0]}" -c flezha -- sh -c '/flezha/erts-*/bin/epmd -names')"
echo "$ports"
grep -q "at port 9100" <<<"$ports" || fail "EPMD does not list the node at port 9100"

IP="$(pod_ip "${PODS[0]}")"
note "scenario 1: an unlabelled pod cannot reach 4369 or 9100, and reaches 9569"
if reaches run=probe "$IP" 4369; then fail "an unlabelled pod reached EPMD"; fi
if reaches run=probe "$IP" 9100; then fail "an unlabelled pod reached the distribution port"; fi
reaches run=probe "$IP" 9569 || fail "an unlabelled pod could not reach the health port"
reaches app=console "$IP" 4369 || fail "a console-labelled pod could not reach EPMD"
reaches app=console "$IP" 9100 || fail "a console-labelled pod could not reach the distribution port"

note "scenario 2: keys stay 0600 and directories stay closed across a pod restart"
# The node container `$1` names: the host paths a pod there mounts.
check_modes() {
  local node=$1 keys dirs
  keys="$(docker exec "$node" sh -c 'stat -c "%a %n" /opt/flezha-certs/*.key')"
  dirs="$(docker exec "$node" stat -c '%a %n' /opt/flezha-certs /opt/flezha-data /opt/flezha-recovery \
    /opt/flezha-sockets /opt/flezha-logs)"
  echo "$keys"
  echo "$dirs"
  if [ -z "$keys" ] || grep -qv '^600 ' <<<"$keys"; then fail "a key file is not 0600 on $node"; fi
  if awk '{ if (substr($1, length($1), 1) != "0") bad = 1 } END { exit !bad }' <<<"$dirs"; then
    fail "a host directory is readable by other users on $node"
  fi
}
# The manifests run the socket transport, which generates no keys, so the
# controller's own user writes one the way the generator would.
kc -n flezha exec "${PODS[0]}" -c flezha -- \
  sh -c 'umask 077 && echo key >/flezha/storage/app/certs/probe.key'
NODE="$(node_of "${PODS[0]}")"
check_modes "$NODE"
kc -n flezha delete pod "${PODS[0]}" --wait=true >/dev/null
await_pods
check_modes "$NODE"

note "scenario 3: the cert-manager recipe puts the controllers on mutual TLS"
kc apply -f "$CERT_MANAGER" >/dev/null
for deploy in cert-manager cert-manager-webhook cert-manager-cainjector; do
  kc -n cert-manager rollout status "deploy/$deploy" --timeout=300s >/dev/null
done
for _ in $(seq 1 30); do
  kc apply -f "$REPO_ROOT/deploy/dist-tls/kubernetes/issuer.yaml" >/dev/null 2>&1 && break
  sleep 2
done
kc -n flezha wait --for=condition=Ready certificate/flezha-dist-ca --timeout=120s >/dev/null
kc -n flezha create configmap flezha-dist-tls --from-file="$REPO_ROOT/deploy/dist-tls/dist_tls.conf" >/dev/null
kc -n flezha patch daemonset flezha-daemonset \
  --patch-file "$REPO_ROOT/deploy/dist-tls/kubernetes/daemonset-patch.yaml" >/dev/null
sleep 5
await_pods
clustered || fail "the controllers did not connect over mutual TLS"
proto="$(pod_rpc "${PODS[0]}" 'IO.inspect(:init.get_argument(:proto_dist))')"
grep -q 'inet_tls' <<<"$proto" || fail "distribution is not TLS: $proto"

# A pod of the controller image that dials a controller once, with the cookie
# and the certificates in the Secret `secret`, and prints whether it got in.
# Labelled as the console, so only the TLS check can turn it away. It does not
# check the certificate it is answered with, as an intruder would not.
dial() {
  local name=$1 secret=$2 target
  target="flezha@$(pod_ip "${PODS[0]}")"
  kc -n flezha create configmap "${name}-conf" --from-literal=dist_tls.conf="$(
    sed '/{client,/,$ s/{verify, verify_peer}/{verify, verify_none}/' "$REPO_ROOT/deploy/dist-tls/dist_tls.conf"
  )" >/dev/null
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: flezha
  labels:
    app: console
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
  containers:
    - name: dial
      image: flezha/flezha:0.1.0
      imagePullPolicy: IfNotPresent
      env:
        # The eval VM writes a cookie file of its own before the real one is set.
        - name: HOME
          value: /tmp
        - name: RELEASE_COOKIE
          value: "${COOKIE}"
        - name: FLEZHA_DIST_TLS_OPTFILE
          value: /etc/flezha/dist-conf/dist_tls.conf
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: DIAL
          value: |
            if node() == :nonode@nohost,
              do: {:ok, _} = :net_kernel.start([:"${name}@#{System.fetch_env!("POD_IP")}", :longnames])
            Node.set_cookie(String.to_atom(System.fetch_env!("RELEASE_COOKIE")))
            # A new pod may be dropped until the network policy has learned its address.
            connected =
              Enum.any?(1..15, fn _ ->
                Node.connect(:"${target}") == true or (Process.sleep(2_000) && false)
              end)
            IO.puts("connected=#{connected}")
      # A node registers with an EPMD of its own pod before it dials.
      command: ["sh", "-c", '/flezha/erts-*/bin/epmd -daemon && exec /flezha/bin/flzvm eval "\$DIAL"']
      volumeMounts:
        - name: certs
          mountPath: /etc/flezha/dist
        - name: conf
          mountPath: /etc/flezha/dist-conf
  volumes:
    - name: certs
      secret:
        secretName: ${secret}
    - name: conf
      configMap:
        name: ${name}-conf
EOF
  kc -n flezha wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${name}" --timeout=180s >/dev/null ||
    { kc -n flezha logs "$name" || true; fail "the ${name} pod did not finish"; }
  kc -n flezha logs "$name" >&2
  kc -n flezha logs "$name" | grep '^connected='
}

# Leaves of the distribution authority and of another one, each laid out as
# the folder dist_tls.conf reads, in a Secret.
mkdir -p "$WORK/cluster_ca"
kc -n flezha get secret flezha-dist-ca -o jsonpath='{.data.tls\.crt}' | base64 -d >"$WORK/cluster_ca/ca.pem"
kc -n flezha get secret flezha-dist-ca -o jsonpath='{.data.tls\.key}' | base64 -d >"$WORK/cluster_ca/ca.key"
"$REPO_ROOT/deploy/dist-tls/make-certs.sh" node "$WORK/cluster_ca" member --ip 127.0.0.1
"$REPO_ROOT/deploy/dist-tls/make-certs.sh" ca "$WORK/other_ca"
"$REPO_ROOT/deploy/dist-tls/make-certs.sh" node "$WORK/other_ca" intruder --ip 127.0.0.1
kc -n flezha create secret generic member-dist --from-file="$WORK/cluster_ca/member" >/dev/null
kc -n flezha create secret generic intruder-dist --from-file="$WORK/other_ca/intruder" >/dev/null

note "scenario 3: a certificate of the distribution authority gets in"
[ "$(dial member member-dist)" = "connected=true" ] || fail "a member of the authority was refused"
note "scenario 3: a certificate of another authority is refused"
[ "$(dial intruder intruder-dist)" = "connected=false" ] || fail "a certificate of another authority got in"

note "PASS"
