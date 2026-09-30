# Kubernetes

The desk's two images, the controller with the HTTP plugin and the runner, run on
Kubernetes as they run in Docker Compose. This page puts them on a k3s cluster: one pod
holding a controller and its runner, a volume for what the controller keeps, and a
service in front of the gateway.

## The manifest

`k8s/agentdesk.yaml`, in the project's folder:

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
            - name: FLEZHA_PARK_IDLE_MS
              value: "10000"
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

- **One pod, two containers.** The controller and the runner share the `run` volume,
  where the controller makes its socket folder, as the two containers shared `sockets` in
  Compose.
- **A StatefulSet** gives the pod a volume of its own, `app-data`, which a new pod of the
  same name mounts again: the sessions outlive the pod.
- **`imagePullPolicy: Never`**: the images are built on your machine and imported into
  the cluster, not pulled from a registry.
- **The service** is the gateway's HTTP port, 8080, inside the cluster.

## Build and import the images

> Build the two images under fixed names, from the project's folder. The plugin package
> from [`custom_plugins.md`](custom_plugins.md) is still in `controller/plugins/`.
> {: .step}

```sh
$ docker build -q -t agentdesk/controller:0.1.0 controller && docker build -q -t agentdesk/runner:0.1.0 .
```

```output
sha256:6ceccae4bc8ab49232436d0788bf477f223878010a5621badf56b285e96cc381
sha256:73fede5a0f02cdffcf068c5b3fbb9aea93cf97f0a74c485d56a66432d2ff4062
```

> Import them into k3s.
> {: .step}

```sh
$ docker save agentdesk/controller:0.1.0 agentdesk/runner:0.1.0 | sudo k3s ctr images import -
```

A cluster with a registry of its own pushes them there instead, and drops
`imagePullPolicy: Never`.

## Deploy

> Apply the manifest, and wait for the pod.
> {: .step}

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
agentdesk-0   2/2     Running   0          14s
```

> Point the `flz` alias at the controller in the pod.
> {: .step}

```sh
$ alias flz='kubectl -n agentdesk exec agentdesk-0 -c controller -- bin/flz'
$ flz runner list --namespace agents
```

```output
Fetching runners for namespace 'agents'...
Runners in namespace 'agents':
  - agents_11e8bd94707e
```

The cluster is new, so it holds no sessions yet.

> Start Anna's session and the gateway, and enable the HTTP plugin on it, as in
> [`http_gateway.md`](http_gateway.md#start-the-gateway).
> {: .step}

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
Plugin web enabled successfully.
```

## Send a message

> Forward the service to port 8090 on your machine, in a second terminal.
> {: .step}

```sh
$ kubectl -n agentdesk port-forward service/agentdesk 8090:8080
```

> Send Anna a message through it.
> {: .step}

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

## Replace the pod

> Stop the port forward, delete the pod, and wait for its replacement.
> {: .step}

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

> Send Anna a message from the CLI.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 2: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

The new pod mounted the same volume, and its controller restored the session from it: the
message over HTTP was her first.

> #### What a real deployment adds {: .info}
>
> This is the desk's setup moved to Kubernetes, not a production one: authorization is
> off, nothing is encrypted, and one controller carries everything. The reference
> manifests in [`kubernetes/`](https://github.com/flezha/flezha/tree/main/kubernetes)
> run a controller on every node, with health probes, resources and a locked-down
> container; [`authn_authz.md`](../../operations/authn_authz.md) turns authorization on,
> and [`clustering.md`](clustering.md) has the settings several controllers need.

## Clean up

```sh
$ kubectl delete -f k8s/agentdesk.yaml
```

```output
namespace "agentdesk" deleted
statefulset.apps "agentdesk" deleted from agentdesk namespace
service "agentdesk" deleted from agentdesk namespace
```

Deleting the namespace deletes the pod's volume with it, and the sessions. That is the
end of Part 3.
