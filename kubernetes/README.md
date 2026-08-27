# Kubernetes Resource Files

These files can be used as a reference for deploying the Flezha controller to a Kubernetes cluster.
They create a namespace, role, service account, role binding, and a daemonset.

## Image and registry

The Flezha controller is a multi-architecture (`linux/amd64`, `linux/arm64`) Docker
image on Docker Hub.

The DaemonSet pins an **immutable version tag** — `alfetahe/flezha:0.1.0` — not
`:latest`. This is deliberate: the pull policy is `IfNotPresent`, so a mutable tag
means a node that already has a layer cached keeps running the controller it pulled
weeks ago, and two nodes silently run different code. **Bump the tag on every
controller release**; `scripts/publish-image.sh` publishes the version tag (read from
`mix.exs`) alongside `:latest`.

### Registry credentials

The image is public: `kubectl apply -f kubernetes/` works on any cluster with Docker Hub
egress, with **no pull secret to create first**. `imagePullSecrets` is left commented out
in `flezha-daemonset.yaml` and `service-template.yaml`.

Only when you mirror the image into a **private** registry do you need a secret —
create it and uncomment the `imagePullSecrets` block in both manifests:

```bash
kubectl create secret docker-registry dockerhub-secret \
  --docker-server=docker.io \
  --docker-username=<your-username> \
  --docker-password=<your-password> \
  -n flezha
```

### Building Custom Images

Build and publish natively per architecture (no QEMU/buildx) with the publish
script — run it once on each architecture host:

```bash
# From flezha root directory, logged in as the image owner
scripts/publish-image.sh          # prod :latest  (host-arch tag + manifest)
scripts/publish-image.sh --dev    # dev  :latest-dev
```

Each run pushes the host-arch tag and rebuilds the manifest list from the
per-arch tags on Docker Hub, so amd64 and arm64 never overwrite each other.
See `docker/prod/README.md` for details.

## Setup
- Apply the namespace: `kubectl apply -f flezha-namespace.yaml`
- Apply the role: `kubectl apply -f flezha-role.yaml`
- Apply the service account: `kubectl apply -f flezha-serviceaccount.yaml`
- Apply the role binding: `kubectl apply -f flezha-rolebinding.yaml`

## Deploy
- Apply the daemonset for the flezha controllers: `kubectl apply -f kubernetes/flezha-daemonset.yaml`
- Apply the deployments for X(runners) services: `kubectl apply -f kubernetes/service-template.yaml`

## Resources

The controller container declares CPU and memory requests and limits. **These are
starting points, not tuned numbers** — they exist so the pod schedules predictably and
a runaway controller cannot starve a node, and you are expected to size them to your
workload:

| | requests | limits |
|---|---|---|
| cpu | `250m` | `2` |
| memory | `512Mi` | `2Gi` |

The BEAM's steady-state footprint scales with the number of bound operatives and the
per-Entity WAL write rate. Symptoms of limits set too low:

- **Memory too low** → the container is `OOMKilled` (`kubectl describe pod` shows
  `Reason: OOMKilled`, and the restart is abrupt — no graceful drain, so in-flight
  work falls back to WAL recovery). Raise the memory limit; do not rely on the
  liveness probe to catch this, the kernel kills the process first.
- **CPU too low** → CPU throttling shows up as latency, not as an error: outputs lag,
  and a badly throttled controller can trip the `output_stall` watchdog and get itself
  rebound. Check `container_cpu_cfs_throttled_seconds_total` before raising anything
  else.

## Health probes

The controller serves `GET /healthz` on port `9569` (`Flezha.Healthz.Server`, enabled
via `FLEZHA_HEALTHZ_ENABLED` in the ConfigMap and bound to `0.0.0.0` so the kubelet can
reach it on the pod IP). The DaemonSet uses it for all three probes:

- `startupProbe` — 30 × 10s = **5 minutes** for boot plus the auto-restore replay.
  Slow-booting clusters should raise `failureThreshold`, not the liveness thresholds.
- `livenessProbe` — restarts a genuinely wedged controller.
- `readinessProbe` — gates readiness-dependent endpoints.

The endpoint returns `503` **only** on a real wedge. A planned recovery reports
`recovering` with a `200` precisely so the kubelet does not restart the pod mid-replay,
and an operative that cannot bind because its runner is gone reports `degraded` with a
`200` — restarting the controller would not re-bind it. Do not "fix" a `degraded`
controller by tightening the liveness probe.

## Security profile

Pod and container run non-root (uid/gid 1000) with `allowPrivilegeEscalation: false`,
all capabilities dropped, `seccompProfile: RuntimeDefault`, and
`readOnlyRootFilesystem: true`.

Under a read-only root filesystem every path the controller writes must be an explicit
mount. Those are:

| path | volume | why |
|---|---|---|
| `/flezha/sockets` | hostPath | UDS transport (`FLEZHA_SOCKET_FILES_PATH`) |
| `/flezha/app_data/certs` | hostPath | certs auto-generated on first run |
| `/var/log/flezha` | hostPath | log file (`FLEZHA_LOG_PATH`) |
| `/var/lib/flezha` | hostPath | all durable state (see below) |
| `/flezha/tmp` | emptyDir | `RELEASE_TMP` — the Mix release materialises its merged runtime config here on **every** boot; without it the controller does not start |
| `/tmp` | emptyDir | general scratch |

The socket directory is one of the paths a runner can also discover on its own,
through the ladder in [`../docs/configuration.md`](../docs/configuration.md).
These manifests need no change for that: both the DaemonSet and
`service-template.yaml` set `FLEZHA_SOCKET_FILES_PATH` explicitly, which is the
ladder's first rung and wins outright — a default moving underneath them
changes nothing. Keep both sides naming the same path; the discovery ladder is
for hosts that configure neither.

Anything whose path defaults to `app_data/*` (relative to the image working dir)
therefore has to be redirected onto the persistent volume, which is what
`FLEZHA_STATE_DIR`, `FLEZHA_DLQ_DIR` and `FLEZHA_SNAPSHOT_DIR` in the ConfigMap do. **If
you enable the audit log, authn/authz, or the plugin installer, point their directories
at `/var/lib/flezha` too** (`FLEZHA_AUDIT_DIR`, `FLEZHA_AUTHZ_DIR`, the plugin install
root) — otherwise they land on the read-only rootfs.

### Pod Security Standards

The default manifests use `hostPath` volumes, which **no** Pod Security Standard above
`privileged` permits. They also need the root `fix-permissions` init container, for one
reason only: `hostPath` ignores `fsGroup`, so the mounted directories come up root-owned
and the non-root controller would hit `EACCES` on its first write.

Both facts have the same fix. Switch the persistent volumes to an **`fsGroup`-honoring**
type — a CSI volume or a `local` PersistentVolume — and the kubelet chowns the mount to
`fsGroup: 1000` for you. Then:

1. delete the `fix-permissions` init container, and
2. uncomment `pod-security.kubernetes.io/enforce: restricted` in
   `flezha-namespace.yaml`.

The pod is admitted under `restricted` with no other change; the `securityContext` is
already compliant. The `preStop` `flz controller stop` exec keeps working — it runs the
release's own binary, which needs no write access to the rootfs.

## Persistent state

The DaemonSet mounts a node-local volume at `/var/lib/flezha` (`FLEZHA_DATA_DIR`). It
carries **everything the controller must not lose**:

- ProcessHub's persistent registry (`FLEZHA_PROCESS_HUB_REGISTRY_ROOT`) — the recovery
  DETS files, i.e. operative child specs and recoverable plugin bindings.
- Per-Entity WAL + snapshot files (`FLEZHA_STATE_DIR`) — operative state.
- The dead-letter queue store (`FLEZHA_DLQ_DIR`).

Losing this volume reverts a controller to an empty recovery on restart. See the
top-level [README.md](../README.md#failure-recovery) for the per-provider
recoverability table and the semantics of the recovery window
(`FLEZHA_RECOVERY_WINDOW_MS`).

### One controller per node

A DaemonSet is one pod per node by construction, so every controller here has the
node's `/var/lib/flezha` to itself and none of the co-location variables need
per-instance values. Running several controllers on one machine is a bare-metal and
Docker arrangement; the variable set each instance must own is in
[`../docs/operations/deployment.md` § Several controllers on one
machine](../docs/operations/deployment.md). A second pod landing on a node — a
mis-scaled Deployment beside the DaemonSet, or two DaemonSets sharing one `hostPath`
— refuses to boot rather than sharing the state root, naming the process that holds
it.

### hostPath vs a PersistentVolume

A DaemonSet cannot use `volumeClaimTemplates`, so node-local storage is the pragmatic
default — each pod is pinned to a node, comes back to that same node, and reads the
files it wrote before going down.

- **`hostPath` (the default)** — zero setup, works on bare metal and single-node
  clusters. Costs: it is not `fsGroup`-aware (hence the root init container), it blocks
  the `restricted` and `baseline` PSS profiles, and the data is invisible to the cluster
  — if the node dies, the state dies with it.
- **A `local` PersistentVolume or a CSI volume** — the portable choice for a cloud
  cluster. Honors `fsGroup` (removes the init container, unlocks `restricted`), and a
  networked CSI volume additionally survives the node. Costs: a PV/PVC per node to
  provision, and a networked volume puts the WAL's fsync path on the network — measure
  before adopting it for a write-heavy controller.

Swap the `flezha-recovery` volume in `flezha-daemonset.yaml`; the commented
`persistentVolumeClaim` block there shows the shape.

## Logs

Controller logs are persisted on the host machine at `/opt/flezha-logs/`.

### Accessing Logs

**Real-time container logs (stdout):**
```bash
# List pods
kubectl get pods -n flezha -l app=flezha

# Follow logs from specific pod
kubectl logs -f -n flezha flezha-daemonset-XXXXX

# Follow logs from all controller pods
kubectl logs -f -n flezha -l app=flezha --all-containers=true
```

**Persistent log files:**
```bash
# Inside pod
kubectl exec -it -n flezha flezha-daemonset-XXXXX -- tail -f /var/log/flezha/flezha.log

# On host machine (direct access)
sudo tail -f /opt/flezha-logs/flezha.log

# Copy logs to local machine
kubectl cp -n flezha flezha-daemonset-XXXXX:/var/log/flezha/flezha.log ./controller-logs.log
```

### Log Configuration

Logs are configured via environment variables:
- `FLEZHA_LOG_PATH`: `/var/log/flezha/flezha.log` (file path)
- `FLEZHA_LOG_LEVEL`: `info` (default, can be: debug, info, warning, error)

The controller logs to both stdout (for `kubectl logs`) and file (for persistence).

## Node hardening (required, once per node)

`node-hardening.sh` makes a node freeze diagnosable and cheaply
self-recovering. It is **not** applied by any manifest — k8s cannot set
kernel sysctls or journald storage for the host — so it must be run by
hand, with sudo, **on every cluster node**, and re-run whenever a node is
reimaged. A node that skips it loses its crash-window logs (journald in
volatile `/run`) and will not panic-reboot on a >120s kernel hang.

```bash
# Copy to each node and run as root, e.g.
scp kubernetes/node-hardening.sh <node>:/tmp/
ssh <node> 'sudo bash /tmp/node-hardening.sh'
```

What it sets (idempotent):
- journald `Storage=persistent`, capped at 200M / 14 days — survives reboot
  so the next wedge's logs are recoverable.
- `kernel.hung_task_panic=1` + `kernel.panic=10` — a >120s uninterruptible
  kernel hang panics and reboots in 10s.

Verify on a node:
```bash
cat /proc/sys/kernel/hung_task_panic   # expect 1
grep Storage /etc/systemd/journald.conf.d/persist.conf   # expect persistent
```

Note: `hung_task_panic` only fires on kernel D-state hangs. A userspace
containerd/kubelet wedge (the 2026-06-11 and 2026-06-13 rpi4 outages) will
not self-reboot from this — persistent journald is what makes that case
diagnosable. The single-Pi SPOF behind those outages (postgres/kafka/KRaft
controller all hostPath-pinned to one node) is a separate, still-open issue.