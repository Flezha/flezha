# Run on Kubernetes

On this page you run the same images on k3s: one pod holding a controller and
its runner, from `k8s/agentdesk.yaml` in the agentdesk folder.
[Kubernetes](../../topics/deploying/kubernetes.md) covers the manifests in full.

> Build the two images under fixed names, from the agentdesk folder, and import them into k3s. The HTTP plugin's package is still in `controller/plugins/`.
> {: .step}

```sh
$ docker build -q -t agentdesk/controller:0.1.0 controller && docker build -q -t agentdesk/runner:0.1.0 .
$ docker save agentdesk/controller:0.1.0 agentdesk/runner:0.1.0 | sudo k3s ctr images import -
```

> Apply the manifest, and wait for the pod.
> {: .step}

```sh
$ kubectl apply -f k8s/agentdesk.yaml
$ kubectl -n agentdesk rollout status statefulset/agentdesk --timeout=300s
```

```output
namespace/agentdesk created
statefulset.apps/agentdesk created
service/agentdesk created
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
```

> Point the `flz` alias at the controller in the pod, start Anna's session and the gateway, and enable the HTTP plugin on it.
> {: .step}

```sh
$ alias flz='kubectl -n agentdesk exec agentdesk-0 -c controller -- bin/flz'
$ flz operator start agent --id anna --namespace agents && flz operator start gateway --id gateway --namespace agents
$ flz plugin enable web http_server --operator_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
```

> Forward the service to port 8090 in a second terminal, and send Anna a message through it.
> {: .step}

```sh
$ kubectl -n agentdesk port-forward service/agentdesk 8090:8080
```

```sh
$ curl -s -d 'Hello from k3s' localhost:8090/sessions/anna/messages
```

```output
sent to anna
```

> Stop the port forward, delete the pod, wait for its replacement, and send Anna a message from the CLI.
> {: .step}

```sh
$ kubectl -n agentdesk delete pod agentdesk-0
$ kubectl -n agentdesk rollout status statefulset/agentdesk --timeout=300s
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 2: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

Anna's session came back with the new pod. To remove it all:

```sh
$ kubectl delete -f k8s/agentdesk.yaml
```

That is the agentdesk, and the last tutorial.
