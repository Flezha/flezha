# Kubernetes deployment

Manifests live in `kubernetes/`. The DaemonSet runs one controller pod per
node; `kubernetes/README.md` covers resources, the security profile and the
hostPath-versus-PVC trade-off.

| File | Resource |
|---|---|
| `flezha-namespace.yaml` | Namespace `flezha` |
| `flezha-serviceaccount.yaml` | ServiceAccount `flezha-reader` |
| `flezha-role.yaml` | Role `pod-reader` (`pods`: get, list, watch — pod discovery) |
| `flezha-rolebinding.yaml` | RoleBinding `read-pods` |
| `flezha-configmap.yaml` | ConfigMap `flezha-config` (non-secret env) |
| `flezha-daemonset.yaml` | DaemonSet `flezha-daemonset`, image `alfetahe/flezha:0.2.0`, `imagePullPolicy: IfNotPresent` |
| `service-template.yaml` | Reference Deployment for a runner service sharing the socket hostPath |
| `node-hardening.sh` | Run once per node as root: persistent journald (200 M cap) and `hung_task_panic` |

## Apply

```bash
make k8s-apply      # namespace, serviceaccount, role, rolebinding, configmap, daemonset — in that order
kubectl get pods -n flezha -l app=flezha
```

The image is public; `imagePullSecrets` is commented out in the DaemonSet.
For a private mirror create `dockerhub-secret` in the namespace
(`kubectl create secret docker-registry dockerhub-secret --docker-server=docker.io …`)
and uncomment the block.

## Update the image

Edit `image:` and the `app.kubernetes.io/version` labels in
`kubernetes/flezha-daemonset.yaml`, then `kubectl apply -f kubernetes/flezha-daemonset.yaml`.
Never point the manifest at `:latest` — with `IfNotPresent` a mutable tag
leaves nodes running whatever they cached.

## ConfigMap

The DaemonSet loads `flezha-config` via `envFrom`, then sets per-pod values:
`FLEZHA_NODE_HOST` (`status.podIP`), `FLEZHA_NODE_NAME` (`flezha`),
`FLEZHA_K8S_NAMESPACE`, `FLEZHA_K8S_SERVICE_NAME` (the `app` label),
`FLEZHA_K8S_SELECTOR` (`app=flezha`) and `FLEZHA_SOCKET_FILES_PATH`
(`/flezha/sockets`). The `app: flezha` pod label is load-bearing for discovery.

| ConfigMap key | Value |
|---|---|
| `FLEZHA_TRANSPORT` | `uds` |
| `FLEZHA_CLUSTER_DISCOVERY` | `k8s_pod_discovery` |
| `FLEZHA_K8S_IP_LOOKUP_MODE` / `FLEZHA_K8S_MASTER_NAME` / `FLEZHA_K8S_MASTER_PORT` | `pods` / `kubernetes.default.svc` / `443` |
| `FLEZHA_RECONNECT_INTERVAL` / `FLEZHA_MAX_RECONNECTS` / `FLEZHA_RUNNER_KEEPALIVE` | `2000` / `10` / `30000` |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | `0.0.0.0:4433` |
| `FLEZHA_CA_CERT_PATH` / `FLEZHA_SERVER_CERT_PATH` / `FLEZHA_SERVER_KEY_PATH` | `/flezha/app_data/certs/{ca.pem,server.pem,server.key}` — persisted on the `flezha-certs` hostPath |
| `FLEZHA_LOG_PATH` | `/var/log/flezha/flezha.log` |
| `FLEZHA_AUTO_RESTORE_ENABLED` / `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `true` / `60000` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` / `FLEZHA_STATE_DIR` / `FLEZHA_DLQ_DIR` | `/var/lib/flezha/{process_hub,state,dlq}` — on the `flezha-recovery` hostPath |
| `FLEZHA_HEALTHZ_ENABLED` / `FLEZHA_HEALTHZ_PORT` / `FLEZHA_HEALTHZ_BIND` | `true` / `9569` / `0.0.0.0` (the kubelet probes the pod IP, not loopback) |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000`, paired with `terminationGracePeriodSeconds: 75` |

Not in the shipped ConfigMap and needed for a prod pod: `FLEZHA_DURABILITY_BACKEND`
(the pod does not boot without it), `RELEASE_COOKIE` (from a Secret), and
`FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`,
`FLEZHA_PLUGIN_ROOT` (their defaults sit under the read-only `/flezha`; point
them under `/var/lib/flezha`). To enable the remote durability tier add the
backend family to the ConfigMap; the `_PASSWORD_ENV` / `_SECRET_ENV`
indirection names a Secret-sourced variable rather than carrying the secret.

After editing the ConfigMap:

```bash
kubectl apply -f kubernetes/flezha-configmap.yaml
kubectl rollout restart daemonset/flezha-daemonset -n flezha
```

## Volumes (per-node persistence)

| Volume | Host path | Mount path | Holds |
|---|---|---|---|
| `flezha-sockets` | `/opt/flezha-sockets` | `/flezha/sockets` | UDS socket files, shared with runner pods |
| `flezha-data` | `/opt/flezha-data` | `/flezha/data` | spare data directory |
| `flezha-certs` | `/opt/flezha-certs` | `/flezha/app_data/certs` | TLS material, generated on first boot and reused |
| `flezha-logs` | `/opt/flezha-logs` | `/var/log/flezha` | log file |
| `flezha-recovery` | `/opt/flezha-recovery` | `/var/lib/flezha` | ProcessHub registry, per-Entity WAL and snapshots, DLQ — losing it reverts the node to an empty recovery |
| `release-tmp`, `tmp` | emptyDir | `/flezha/tmp`, `/tmp` | the two non-persistent paths the BEAM writes under a read-only root (`RELEASE_TMP` holds the merged runtime config) |

All five hostPaths are `DirectoryOrCreate`. Because hostPath ignores
`fsGroup`, a root `fix-permissions` init container chowns them to 1000:1000;
swapping them for an fsGroup-honouring volume (a PVC) removes that container
and makes the pod admissible under the `restricted` Pod Security Standard. The
pod itself runs as uid/gid 1000, `readOnlyRootFilesystem: true`, all
capabilities dropped, seccomp `RuntimeDefault`.

## Ports and probes

The container exposes `epmd` 4369/TCP, `quic` 4433/UDP and `healthz`
9569/TCP. Startup (period 10 s, 30 failures) and liveness (period 10 s, timeout
5 s, 3 failures) probes `GET /healthz`; the endpoint answers 503 only on a
genuine wedge, and a planned recovery reports `recovering` with 200 so the
kubelet does not restart mid-replay. `/healthz` is a fold of the convergence
loop's one health record per subject: an operative that is parked, bound to a
dead runner or missing a declared plugin reads `degraded` with `parked`,
`dead_runner` or `plugin_unhealthy` among the joined reasons, never `healthy`,
and the body's `unknown_operatives` counts the ones the loop could not read this
tick — an uncertain probe is reported, never escalated. The fabric arm needs a
NIF driver to judge: both server transports produce one, so a UDS or QUIC
controller's `200` is evidence about the data plane, while a `:native`
in-node controller runs no driver and its `200` is evidence about the loop, the
operatives and the plugins alone. The readiness probe (same period, timeout
and threshold) reads `GET /readyz` instead, because readiness is the opposite
question: it answers 200 only once the boot sequence has completed, every
namespace hub has left the recovery state machine and the durable registry has
been reloaded, and 503 — naming the unmet preconditions — for exactly the window
`/healthz` must keep answering 200. `preStop` runs
`/flezha/bin/flz controller stop --timeout-ms $(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS)`
before SIGTERM. Exposing QUIC outside the node needs a Service with
`protocol: UDP` (a UDP-capable load balancer or a NodePort).

## Status and logs

```bash
kubectl get pods -n flezha -l app=flezha
kubectl describe pod -n flezha <pod-name>
kubectl logs -f -n flezha -l app=flezha --all-containers=true
kubectl exec -it -n flezha <pod-name> -- tail -f /var/log/flezha/flezha.log
sudo tail -f /opt/flezha-logs/flezha.log        # on the node
```

## Delete

```bash
make k8s-delete                    # DaemonSet and ConfigMap only
kubectl delete namespace flezha    # everything, including RBAC
```
