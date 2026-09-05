# kubernetes-reference-defaults Specification

## Purpose
TBD - created by archiving change harden-kubernetes-reference-defaults. Update Purpose after archive.
## Requirements
### Requirement: Reference workloads declare resource requests and limits
Every container in the reference manifests (`kubernetes/flezha-daemonset.yaml` controller container and `kubernetes/service-template.yaml` runner container) SHALL declare CPU and memory `requests` and `limits` with conservative, documented defaults that an operator can tune. No reference workload container SHALL ship without resource bounds.

#### Scenario: Controller container has resource bounds
- **WHEN** `kubernetes/flezha-daemonset.yaml` is applied
- **THEN** the `flezha` container declares `resources.requests` and `resources.limits` for both `cpu` and `memory`, with values documented as tunable defaults in `kubernetes/README.md`

#### Scenario: Runner template carries the same baseline
- **WHEN** the `service-template.yaml` reference is used
- **THEN** its runner container also declares CPU and memory requests and limits

### Requirement: Controller container declares liveness, readiness, and startup probes

The controller container SHALL declare a `startupProbe`, a `livenessProbe`, and a `readinessProbe` so Kubernetes restarts a wedged controller and gates rollouts and traffic on the right fact. The probes SHALL use a controller-provided signal (an HTTP endpoint the controller itself serves, a `flz` command exec, or a TCP check on a controller port) and SHALL NOT assume any external dependency.

Liveness and readiness are different questions and SHALL NOT share one endpoint. The `livenessProbe` and the `startupProbe` SHALL read the health endpoint, which answers success throughout a planned recovery so the kubelet does not restart a controller mid-replay. The `readinessProbe` SHALL read the readiness endpoint, which answers failure for exactly that same window, so a pod that has not finished coming up is not routed to.

A readiness probe SHALL NOT report a controller ready on the grounds that nothing is yet running on it. A controller in its first tick — no namespace hub created, no operative bound, nothing restored — SHALL read as not ready.

#### Scenario: Probes present and self-contained

- **WHEN** `kubernetes/flezha-daemonset.yaml` is applied
- **THEN** the `flezha` container declares `startupProbe`, `livenessProbe`, and `readinessProbe`, each using a self-contained check served by the controller itself, with no probe depending on a service outside the pod

#### Scenario: Liveness and readiness read different endpoints

- **WHEN** `kubernetes/flezha-daemonset.yaml` is inspected
- **THEN** the `livenessProbe` and `startupProbe` target the health endpoint and the `readinessProbe` targets the readiness endpoint

#### Scenario: A recovering controller is live but not ready

- **GIVEN** a controller whose namespace hubs are replaying their durable registry
- **WHEN** the kubelet runs both probes
- **THEN** the liveness probe SHALL succeed, so the pod is not restarted, and the readiness probe SHALL fail, so the pod is removed from readiness-gated endpoints until the replay completes

#### Scenario: A freshly started controller is not ready

- **GIVEN** a controller pod in its first seconds, before any namespace hub exists
- **WHEN** the kubelet runs the readiness probe
- **THEN** it SHALL fail, and the pod SHALL NOT be added to readiness-gated endpoints

#### Scenario: Failing health restarts the pod

- **WHEN** the controller becomes unhealthy past the liveness threshold
- **THEN** the kubelet restarts the container, and while unready the pod is removed from readiness-gated endpoints

### Requirement: Reference manifests meet the Pod Security Standards restricted profile
The controller (and runner template) Pod and container `securityContext` SHALL satisfy the *restricted* Pod Security Standard: `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `seccompProfile.type: RuntimeDefault`, and `readOnlyRootFilesystem: true` with explicit writable `emptyDir`/volume mounts for any path the process must write. Where a root-running init container (e.g. `fix-permissions`) is required only because a volume does not honor `fsGroup`, the manifest SHALL document that switching to an `fsGroup`-honoring volume removes the need for the root init container under the restricted profile.

#### Scenario: Restricted securityContext fields are set
- **WHEN** `kubernetes/flezha-daemonset.yaml` is applied
- **THEN** the controller container sets `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `seccompProfile.type: RuntimeDefault`, and `readOnlyRootFilesystem: true`, with writable mounts declared for every path the controller writes

#### Scenario: Restricted profile admits the namespace
- **WHEN** the `flezha` namespace is labelled `pod-security.kubernetes.io/enforce: restricted`
- **THEN** the reference DaemonSet and runner template are admitted without warnings, OR the README documents the exact reason and the `fsGroup`-volume alternative that makes them compliant

### Requirement: Reference image is pinned and the pull secret is optional
The reference manifests SHALL reference the controller image by a **pinned version tag** (not `:latest`). Because the published image is public, a stock `kubectl apply -f kubernetes/` SHALL succeed **without** creating any registry secret: `imagePullSecrets` SHALL be optional (absent by default or clearly commented), and the README SHALL document how to add one for a private registry.

#### Scenario: Stock apply needs no pull secret
- **WHEN** a user runs `kubectl apply -f kubernetes/` against a cluster that can reach Docker Hub
- **THEN** the controller image pulls successfully without any `imagePullSecrets` being created first

#### Scenario: Image tag is immutable
- **WHEN** the reference DaemonSet is inspected
- **THEN** the controller `image` uses a pinned version tag rather than `:latest`

### Requirement: Reference manifests carry the recommended Kubernetes labels
All reference manifests SHALL set the recommended `app.kubernetes.io/*` labels (`name`, `instance`, `component`, `part-of`, `managed-by`, and `version`) so standard tooling and selectors work, in addition to any existing selector labels.

#### Scenario: Recommended labels present
- **WHEN** any reference manifest is applied
- **THEN** its resources carry `app.kubernetes.io/name`, `app.kubernetes.io/component`, `app.kubernetes.io/part-of`, and `app.kubernetes.io/managed-by` labels (with `instance`/`version` where applicable)

### Requirement: Persistent storage is portable and parameterized
The reference manifests SHALL NOT hardcode environment-specific assumptions for persistent state. The controller's persistent recovery path MAY default to a node-local volume (correct for a DaemonSet), but the path and volume type SHALL be parameterized and `kubernetes/README.md` SHALL document the portable choice (a CSI/`local` PersistentVolume for cloud clusters) versus `hostPath` (bare-metal/single-node), including the trade-offs.

#### Scenario: Storage is documented and tunable, not assumed
- **WHEN** a cloud user reviews the reference manifests
- **THEN** the persistent-state volume path/type is parameterized (not a fixed host directory baked into the only supported path), and the README explains the CSI/local-PV option alongside `hostPath` with their trade-offs

### Requirement: Reference manifests are correct and environment-agnostic
The reference manifests SHALL be free of YAML/scheduling defects and SHALL NOT embed any environment-specific identifiers (node hostnames, single-cluster IPs, or architecture pins beyond the multi-arch image). The `service-template.yaml` `podAntiAffinity` SHALL use a valid `operator` key in its `matchExpressions`.

#### Scenario: podAntiAffinity expression is valid
- **WHEN** `kubernetes/service-template.yaml` is applied
- **THEN** its `podAntiAffinity` `matchExpressions` entry uses `operator: In` (not the invalid `runner: In`), and the manifest is accepted by the API server

#### Scenario: No environment-specific identifiers
- **WHEN** any reference manifest is reviewed
- **THEN** it contains no node hostnames, no hardcoded cluster IPs, and no architecture-specific node selectors (architecture is resolved by the multi-arch image)

