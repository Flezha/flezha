# Kubernetes

A controller and its runners run on Kubernetes as they run in Docker or Docker Compose: the
same images, the same settings as environment variables, and a shared folder for the Unix
sockets the runner reaches the controller through. Kubernetes adds what keeps them running:
a volume that outlives the pod, probes, and a service in front of a plugin's port.

There are two shapes. A small application can put a controller and its runner in one pod,
which the agentdesk example does. A cluster runs a controller on every node with the
reference manifests in [`kubernetes/`](https://github.com/flezha/flezha/tree/main/kubernetes),
and its runners as deployments beside it.

## How it works

- **The controller and the runner share a folder.** The controller makes its socket folder in
  a volume both containers mount, and the runner finds the sockets there
  (`FLEZHA_SOCKET_FILES_PATH` on both sides, the same path).
- **The controller's data needs a volume of its own.** Everything the controller keeps, the
  operators' state included, is under its `app_data` folder. A StatefulSet gives the pod a
  volume that a new pod of the same name mounts again, so the state outlives the pod.
- **The pod needs time to stop.** A controller that stops drains its work and hands its
  operators over first. `terminationGracePeriodSeconds` must be longer than the controller's
  shutdown timeout (`FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`, 60 seconds by default): 75 seconds
  is the timeout plus 15 seconds.
- **Readiness.** A controller is ready once its boot has finished: `flz boot await --timeout 0`
  answers that from inside the container, and the controller's `GET /readyz` on port 9569
  answers it over HTTP.
- **Images.** The controller's image is Flezha's own plus any plugin packages
  ([installing_plugins.md](installing_plugins.md)); the runner's is the application's.

## Example: one pod

The agentdesk example's `k8s/agentdesk.yaml` runs one pod holding a controller and its runner,
with a volume for what the controller keeps and a service in front of the HTTP plugin's port:

```yaml
# agentdesk on Kubernetes: one pod holding a controller and its runner, the images
# `docker build` makes from this project, and a service in front of the gateway's HTTP port.
apiVersion: v1
kind: Namespace
metadata:
  name: agentdesk
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: agentdesk
  namespace: agentdesk
spec:
  serviceName: agentdesk
  replicas: 1
  selector:
    matchLabels:
      app: agentdesk
  template:
    metadata:
      labels:
        app: agentdesk
    spec:
      terminationGracePeriodSeconds: 75                # time for the controller's own stop
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: controller
          image: agentdesk/controller:0.1.0
          imagePullPolicy: Never                       # imported into the cluster, not pulled
          env:
            - name: FLEZHA_AUTHZ_ENABLED
              value: "false"
            - name: FLEZHA_DURABILITY_BACKEND
              value: none
            - name: FLEZHA_SOCKET_FILES_PATH           # the controller makes this folder its own
              value: /flezha/run/sockets
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            exec:
              command: ["/flezha/bin/flz", "boot", "await", "--timeout", "0"]
            periodSeconds: 5
          volumeMounts:
            - name: app-data
              mountPath: /flezha/app_data
            - name: run
              mountPath: /flezha/run
        - name: runner
          image: agentdesk/runner:0.1.0
          imagePullPolicy: Never
          env:
            - name: FLEZHA_SOCKET_FILES_PATH
              value: /flezha/run/sockets
          volumeMounts:
            - name: run
              mountPath: /flezha/run
      volumes:
        - name: run                                    # shared by the two containers
          emptyDir: {}
  volumeClaimTemplates:
    - metadata:
        name: app-data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: agentdesk
  namespace: agentdesk
spec:
  selector:
    app: agentdesk
  ports:
    - name: http
      port: 8080
      targetPort: http
```

Full file: [`k8s/agentdesk.yaml`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/k8s/agentdesk.yaml)

- **One pod, two containers.** The controller and the runner share the `run` volume, where
  the controller makes its socket folder.
- **A StatefulSet** gives the pod the volume `app-data`, which a new pod of the same name
  mounts again: the sessions outlive the pod.
- **`imagePullPolicy: Never`**: the images are built locally and imported into the cluster,
  not pulled from a registry. A cluster with a registry of its own pushes them there instead,
  and drops `imagePullPolicy: Never`.
- **The service** is the HTTP plugin's port, 8080, inside the cluster. The plugin must listen
  on `0.0.0.0` in a container.

### Build, import and deploy

The two images, built under fixed names (the controller's from the project's `controller/`
folder, with the plugin packages it holds), then imported into k3s:

```sh
$ docker build -q -t agentdesk/controller:0.1.0 controller && docker build -q -t agentdesk/runner:0.1.0 .
```

```output
sha256:6ceccae4bc8ab49232436d0788bf477f223878010a5621badf56b285e96cc381
sha256:73fede5a0f02cdffcf068c5b3fbb9aea93cf97f0a74c485d56a66432d2ff4062
```

```sh
$ docker save agentdesk/controller:0.1.0 agentdesk/runner:0.1.0 | sudo k3s ctr images import -
```

Applying the manifest and waiting for the pod:

```sh
$ kubectl apply -f k8s/agentdesk.yaml
```

```output
namespace/agentdesk created
statefulset.apps/agentdesk created
service/agentdesk created
```

```sh
$ kubectl -n agentdesk rollout status statefulset/agentdesk --timeout=300s
```

```output
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
```

```sh
$ kubectl -n agentdesk get pods
```

```output
NAME          READY   STATUS    RESTARTS   AGE
agentdesk-0   2/2     Running   0          20s
```

### Run `flz` in the pod

The CLI is in the controller's container. An alias makes every `flz` command run there:

```sh
$ alias flz='kubectl -n agentdesk exec agentdesk-0 -c controller -- bin/flz'
$ flz runner list --namespace agents
```

```output
Fetching runners for namespace 'agents'...
Runners in namespace 'agents':
  - agents_4d7736218461
```

Operators are then started and plugins enabled as anywhere else:

```sh
$ flz operator start agent --id anna --namespace agents && flz operator start gateway --id gateway --namespace agents
```

```output
Started anna
Started gateway
```

```sh
$ flz plugin enable web http_server --operator_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
```

```output
Enabling plugin...
Plugin web enabled.
Plugin web bound to gateway.
```

### Reach the service

A port forward makes the service reachable from the machine running `kubectl`, here on port
8090:

```sh
$ kubectl -n agentdesk port-forward service/agentdesk 8090:8080
```

```sh
$ curl -si -d 'Hello from k3s' localhost:8090/sessions/anna/messages
```

```output
HTTP/1.1 202 Accepted
date: Wed, 30 Sep 2026 07:12:16 GMT
content-length: 12
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

sent to anna
```

### Replace the pod

Deleting the pod lets the StatefulSet start a new one, which mounts the same volume:

```sh
$ kubectl -n agentdesk delete pod agentdesk-0
```

```output
pod "agentdesk-0" deleted from agentdesk namespace
```

```sh
$ kubectl -n agentdesk rollout status statefulset/agentdesk --timeout=300s
```

```output
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
```

The new pod's controller restored the session from the volume, and its next answer continues
the conversation:

```sh
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 2: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

Deleting the manifest deletes the namespace, and the pod's volume with it, and the sessions:

```sh
$ kubectl delete -f k8s/agentdesk.yaml
```

```output
namespace "agentdesk" deleted
statefulset.apps "agentdesk" deleted from agentdesk namespace
service "agentdesk" deleted from agentdesk namespace
```

## The reference manifests

The example is one controller with authorization off and nothing encrypted, not a production
setup. The reference manifests in
[`kubernetes/`](https://github.com/flezha/flezha/tree/main/kubernetes) run a controller on
every node as a DaemonSet, with a namespace, role, service account and role binding, and a
template for the runners' deployments. Their
[README](https://github.com/flezha/flezha/blob/main/kubernetes/README.md) has the details;
in short:

- **The cookie Secret comes first.** The controllers form one cluster only with the same
  `RELEASE_COOKIE`, which the DaemonSet reads from a Secret named `flezha-secrets` that is not
  committed. Without it the pods stay pending:

  ```sh
  $ kubectl -n flezha create secret generic flezha-secrets --from-literal=RELEASE_COOKIE="$(openssl rand -base64 48)"
  ```

- **The controllers find each other** with `FLEZHA_CLUSTER_DISCOVERY=k8s_pod_discovery`, by
  the pod label `app: flezha` (`FLEZHA_K8S_SELECTOR`).
- **Probes.** The controller serves `GET /healthz` and `GET /readyz` on port 9569. The startup
  and liveness probes read `/healthz`, which answers 503 only when it is wedged; the readiness
  probe reads `/readyz`, which answers 200 only once the boot has completed and every
  namespace has been restored.
- **Graceful stop.** A `preStop` hook runs `flz controller stop` before Kubernetes sends
  SIGTERM, and `terminationGracePeriodSeconds: 75` pairs with the 60-second shutdown timeout.
- **Resources and security.** The controller declares CPU and memory requests and limits as
  starting points to size to the workload, runs as uid 1000 with a read-only root filesystem
  and no capabilities, and mounts every path it writes.
- **An immutable image tag**, `flezha/flezha:0.1.0`, so that two nodes do not run different
  controllers from a cached `latest`.

## Limits and gotchas

- **One controller is a single point of failure.** Several controllers need the settings in
  [clustering.md](clustering.md), and a durability backend to restore a moved operator from.
- **A plugin's port moves with its operator.** With several controllers, a Service in front of
  an HTTP port needs a proxy that retries a refused connection
  ([http_server.md](../../plugins/http_server.md#deployment)).
- **Deleting the namespace deletes the volumes,** and the state in them.
- **Turn authorization on** outside a local trial ([authorization.md](../basics/authorization.md)).

## See also

- [deploying_a_controller.md](../basics/deploying_a_controller.md): images, settings and
  graceful shutdown.
- [connecting_a_runner.md](../basics/connecting_a_runner.md): how a runner reaches its
  controller.
- [clustering.md](clustering.md), [installing_plugins.md](installing_plugins.md).
- [configuration.md](../../configuration/environment_variables.md#cluster-and-nodes): the Kubernetes discovery
  settings.
- Tutorial: [run_on_kubernetes.md](../../tutorials/agentdesk/run_on_kubernetes.md).
