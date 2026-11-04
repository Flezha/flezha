# The console on Kubernetes

The console runs as one Deployment in the namespace `flezha`, beside the controllers that
[Kubernetes](../topics/deploying/kubernetes.md) runs from the reference manifests. It reaches
them over their HTTP API through a headless service, with a client certificate and a token,
so a controller pod that is replaced under a new address is found again.
[The console](console.md) explains the channel and every setting used here.

> #### What you need {: .tip}
>
> - The controllers running from `deploy/kubernetes/`, version 0.1.0 or later, with the
>   HTTP API's package `flezha_http` in their `FLEZHA_PLUGINS`
>   ([Plugins in the Kubernetes README](https://github.com/flezha/flezha/blob/main/deploy/kubernetes/README.md#plugins)).
> - `openssl`, which `make-certs.sh` runs.
> - The cluster can pull `flezha/flezha_console:0.1.0` from Docker Hub. To run an image you
>   built yourself ([Docker](console_docker.md#the-image) shows how), import it into k3s, or
>   push it to your registry and change `image:` in the manifest.

## The channel's certificates

> Make the channel's authority, one server certificate for every controller, naming the
> headless service the console calls, and the console's client certificate.
> {: .step}

```sh
$ curl -fsSLO https://raw.githubusercontent.com/flezha/flezha/v0.1.0/deploy/dist-tls/make-certs.sh && chmod +x make-certs.sh
$ ./make-certs.sh ca channel
$ ./make-certs.sh node channel controllers --dns flezha-controllers.flezha.svc.cluster.local
$ ./make-certs.sh node channel console --dns console.flezha.svc.cluster.local
```

Keep `channel/ca.key` off the cluster. The authority is the channel's alone: never use the
distribution authority here, because a certificate it signs joins the cluster.

> Put the controllers' files and the console's files in a Secret each.
> {: .step}

```sh
$ kubectl -n flezha create secret generic flezha-channel --from-file=channel/controllers/
$ kubectl -n flezha create secret generic flezha-console-channel \
  --from-file=ca.pem=channel/console/ca.pem \
  --from-file=console.pem=channel/console/node.pem \
  --from-file=console.key=channel/console/node.key
```

The reference DaemonSet mounts the Secret `flezha-channel` at `/etc/flezha/channel` once it
exists.

> Restart the controllers so every pod has the files.
> {: .step}

```sh
$ kubectl -n flezha rollout restart ds/flezha-daemonset
$ kubectl -n flezha rollout status ds/flezha-daemonset
```

## Enable the HTTP API and issue the console's token

> Claim the admin with the bootstrap token the controller wrote at its first start, if no one
> has yet ([Claim the admin](../topics/core/authorization.md#claim-the-admin)).
> {: .step}

```sh
$ kubectl -n flezha exec ds/flezha-daemonset -c flezha -- \
  sh -c '/flezha/bin/flz auth bootstrap < /var/lib/flezha/auth/bootstrap.token'
```

```output
bootstrap claimed; admin@_system bound to admin-6087a2dd4e861d6e
admin token (shown once, store it now): flzt_…
```

> Enable the HTTP API on every controller's pod address, over TLS, asking each client for a
> certificate from the channel's authority.
> {: .step}

```sh
$ kubectl -n flezha exec ds/flezha-daemonset -c flezha -- \
  env FLEZHA_TOKEN=flzt_… /flezha/bin/flz plugin enable api http_api --namespace _system \
  --Xbind=0.0.0.0 \
  --Xtls_certfile=/etc/flezha/channel/node.pem \
  --Xtls_keyfile=/etc/flezha/channel/node.key \
  --Xtls_client_cacertfile=/etc/flezha/channel/ca.pem
```

```output
Enabling plugin...
Plugin api enabled.
```

> Issue the console a token of its own, as an `admin` in `_system`.
> {: .step}

```sh
$ kubectl -n flezha exec ds/flezha-daemonset -c flezha -- \
  env FLEZHA_TOKEN=flzt_… /flezha/bin/flz token issue console@_system _system admin --ttl never
```

```output
token issued for console@_system (admin@_system)
id:    b6395cb899a6
token (shown once, store it now): flzt_…
```

## The console's Secret

> Put the console's token, its database password, its session key and the first account in
> the Secret `flezha-console`.
> {: .step}

```sh
$ kubectl -n flezha create secret generic flezha-console \
  --from-literal=FLEZHA_TOKEN=flzt_… \
  --from-literal=DB_PASSWORD="$(openssl rand -hex 16)" \
  --from-literal=SECRET_KEY_BASE="$(openssl rand -base64 48)" \
  --from-literal=ADMIN_EMAIL=admin@example.com \
  --from-literal=ADMIN_PASSWORD="$(openssl rand -base64 18)"
```

## The manifest

Save this as `console.yaml`; the console's repository carries it as
<!-- docs-citation-record:begin -->`deploy/kubernetes/console.yaml`<!-- docs-citation-record:end -->.

```yaml
# The console on Kubernetes, beside the controllers that core's deploy/kubernetes/ runs in the
# namespace `flezha`: a Postgres for its accounts, the console itself, and a headless service
# that resolves to every controller pod's HTTP API. The console reaches the controllers over
# HTTPS with a client certificate and a flezha token; it holds no cookie and starts no Erlang
# distribution.
#
# Before applying:
#   - enable the `http_api` plugin in `_system` on the controllers with TLS and
#     `tls_client_cacertfile`, each controller's certificate naming
#     flezha-controllers.flezha.svc.cluster.local;
#   - create the Secret `flezha-console` (see the console guide in the Flezha docs);
#   - create the Secret `flezha-console-channel` holding `ca.pem`, the authority the
#     controllers' certificates chain to, and `console.pem` and `console.key`, a client
#     certificate from the authority `tls_client_cacertfile` names.

# Every controller pod's HTTP API, by address: the console pins one that answers, which is
# one holding `_system`, and moves to another when it stops answering.
apiVersion: v1
kind: Service
metadata:
  name: flezha-controllers
  namespace: flezha
spec:
  clusterIP: None
  selector:
    app: flezha
  ports:
    - name: http-api
      port: 8765
---
apiVersion: v1
kind: Service
metadata:
  name: console-db
  namespace: flezha
spec:
  clusterIP: None
  selector:
    app: console-db
  ports:
    - name: postgres
      port: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: console-db
  namespace: flezha
spec:
  serviceName: console-db
  replicas: 1
  selector:
    matchLabels:
      app: console-db
  template:
    metadata:
      labels:
        app: console-db
    spec:
      containers:
        - name: postgres
          image: postgres:17.11-alpine
          env:
            - name: POSTGRES_USER
              value: console
            - name: POSTGRES_DB
              value: console
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: flezha-console
                  key: DB_PASSWORD
            - name: PGDATA
              value: /var/lib/postgresql/data/pgdata
          ports:
            - name: postgres
              containerPort: 5432
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "console"]
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: console
  namespace: flezha
spec:
  selector:
    app: console
  ports:
    - name: http
      port: 4000
      targetPort: http
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: console
  namespace: flezha
spec:
  replicas: 1
  selector:
    matchLabels:
      app: console
  template:
    metadata:
      labels:
        app: console
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        # The Secret's files belong to this group, so the console's user can read them.
        fsGroup: 1000
      containers:
        - name: console
          image: flezha/flezha_console:0.1.0
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 4000
          env:
            # The controllers' HTTP API, and the files the console proves itself and checks
            # theirs with. A token works only on the controller cluster that issued it.
            - name: FLEZHA_CONTROLLER_URL
              value: https://flezha-controllers.flezha.svc.cluster.local:8765
            - name: FLEZHA_CONTROLLER_CA
              value: /etc/flezha-console/channel/ca.pem
            - name: FLEZHA_CLIENT_CERT
              value: /etc/flezha-console/channel/console.pem
            - name: FLEZHA_CLIENT_KEY
              value: /etc/flezha-console/channel/console.key
            - name: FLEZHA_TOKEN
              valueFrom:
                secretKeyRef:
                  name: flezha-console
                  key: FLEZHA_TOKEN
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: flezha-console
                  key: DB_PASSWORD
            - name: DATABASE_URL
              value: ecto://console:$(DB_PASSWORD)@console-db/console
            - name: SECRET_KEY_BASE
              valueFrom:
                secretKeyRef:
                  name: flezha-console
                  key: SECRET_KEY_BASE
            - name: FLEZHA_CONSOLE_ADMIN_EMAIL
              valueFrom:
                secretKeyRef:
                  name: flezha-console
                  key: ADMIN_EMAIL
            - name: FLEZHA_CONSOLE_ADMIN_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: flezha-console
                  key: ADMIN_PASSWORD
            # The address browsers use; LiveView compares a page's origin with it.
            - name: PHX_HOST
              value: localhost
            - name: PHX_URL_SCHEME
              value: http
            - name: PHX_URL_PORT
              value: "4000"
          volumeMounts:
            - name: channel
              mountPath: /etc/flezha-console/channel
              readOnly: true
          readinessProbe:
            httpGet:
              path: /users/log-in
              port: http
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /users/log-in
              port: http
            initialDelaySeconds: 60
            periodSeconds: 30
          resources:
            requests:
              memory: 128Mi
              cpu: 50m
            limits:
              memory: 512Mi
      volumes:
        - name: channel
          secret:
            secretName: flezha-console-channel
            defaultMode: 0440
```

- **`flezha-controllers`** is a headless service over every controller pod's HTTP API, so its
  DNS name resolves to their addresses. `FLEZHA_CONTROLLER_URL` names it, the name the
  controllers' server certificate carries, and the console pins an address that answers.
- **`flezha-console-channel`** holds the channel's authority and the console's client
  certificate, mounted read-only. The console holds no cookie and starts no Erlang
  distribution, so it stays outside the cluster's trust boundary.
- **`app: console`** is the label `flezha-networkpolicy.yaml` admits to the controllers' HTTP
  API port, 8765, and to nothing else: EPMD and the distribution port stay closed to it.
- **`console-db`** is a small Postgres for the console's accounts, on a volume of its own. To
  use a database you already run, drop it and point `DATABASE_URL` there.
- **`PHX_HOST`, `PHX_URL_SCHEME`, `PHX_URL_PORT`** are the address browsers use. They are set
  for `kubectl port-forward` on port 4000; behind an ingress, set them to its host, `https`
  and `443`.

> Apply it and wait for the console.
> {: .step}

```sh
$ kubectl apply -f console.yaml
$ kubectl -n flezha rollout status deploy/console
$ kubectl -n flezha logs deploy/console | grep -E 'NodeConnector|Endpoint'
```

```output
[info] Running FlezhaConsoleWeb.Endpoint with Bandit 1.10.3 at :::4000 (http)
[info] [NodeConnector] Reached the controller flezha-controllers.flezha.svc.cluster.local at https://10.42.0.4:8765
```

> Reach it from your machine.
> {: .step}

```sh
$ kubectl -n flezha port-forward svc/console 4000:4000
```

Open http://localhost:4000 and sign in with `ADMIN_EMAIL` and `ADMIN_PASSWORD` from the
Secret. The Cluster screen lists the controller node the console reached.

## Add an account

```sh
$ kubectl -n flezha exec deploy/console -- \
  bin/flezha_console rpc 'FlezhaConsole.Release.ensure_admin("ops@example.com", "a long passphrase")'
```

## Remove it

```sh
$ kubectl delete -f console.yaml
$ kubectl -n flezha delete secret flezha-console flezha-console-channel
```

The database's volume claim, `data-console-db-0`, stays until you delete it.
