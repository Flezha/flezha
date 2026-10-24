# The console on Kubernetes

The console runs as one Deployment in the namespace `flezha`, beside the controllers that
[Kubernetes](kubernetes.md) runs from the reference manifests. It finds them through a
headless service, so a controller pod that is replaced under a new address is found again.
[The console](console.md) explains the link and every setting used here. This page was run
on k3s.

> #### What you need {: .tip}
>
> - The controllers running from `deploy/kubernetes/`, with the Secret `flezha-secrets`
>   holding their `RELEASE_COOKIE`. The console needs controller 0.1.2 or later: set the
>   DaemonSet's image to `flezha/flezha:0.1.2` if it pins an earlier one.
> - The console's image in the cluster. Build it as [Docker](console_docker.md#build-the-image)
>   shows, then import it into k3s, or push it to your registry and change `image:` in the
>   manifest.

```sh
$ docker save flezha-console:0.1.0 | sudo k3s ctr images import -
```

## Issue the console's token

> Claim the admin with the bootstrap token the controller wrote at its first start, if no one
> has yet ([Claim the admin](authorization.md#claim-the-admin)).
> {: .step}

```sh
$ kubectl -n flezha exec ds/flezha-daemonset -c flezha -- \
  sh -c '/flezha/bin/flz auth bootstrap "$(cat /var/lib/flezha/auth/bootstrap.token)"'
```

```output
bootstrap claimed; admin@_system bound to admin-6087a2dd4e861d6e
admin token (shown once, store it now): flzt_…
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

`deploy/kubernetes/console.yaml` in the console's repository:

```yaml
# The console on Kubernetes, beside the controllers that core's deploy/kubernetes/ runs in the
# namespace `flezha`: a Postgres for its accounts, the console itself as a hidden node, and a
# headless service that resolves to every controller pod. Before applying, create the Secret
# `flezha-console` (see the console guide in the Flezha docs); `flezha-secrets` holds the
# controllers' RELEASE_COOKIE already.

# Every controller pod, by address: the console names a controller after the address its pod
# has now (FLEZHA_NODE_QUERY below).
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
    - name: epmd
      port: 4369
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
          image: postgres:17-alpine
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
      containers:
        - name: console
          image: flezha-console:0.1.0
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 4000
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            # A hidden node under the controllers' cookie: it reaches the controllers without
            # joining their mesh.
            - name: RELEASE_DISTRIBUTION
              value: name
            - name: RELEASE_NODE
              value: flezha_console@$(POD_IP)
            - name: ERL_FLAGS
              value: -hidden
            - name: RELEASE_COOKIE
              valueFrom:
                secretKeyRef:
                  name: flezha-secrets
                  key: RELEASE_COOKIE
            - name: FLEZHA_NODE_QUERY
              value: flezha@flezha-controllers.flezha.svc.cluster.local
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
```

- **`flezha-controllers`** is a headless service over every controller pod, so its DNS name
  resolves to their addresses. `FLEZHA_NODE_QUERY` names it, and the console connects to
  `flezha@<address>` for one of them, the one it already holds first.
- **`RELEASE_NODE: flezha_console@$(POD_IP)`** names the console after its own pod address, as the
  controllers are named after theirs. The controllers' `RELEASE_COOKIE` comes from
  `flezha-secrets`.
- **`console-db`** is a small Postgres for the console's accounts, on a volume of its own. To
  use a database you already run, drop it and point `DATABASE_URL` there.
- **`PHX_HOST`, `PHX_URL_SCHEME`, `PHX_URL_PORT`** are the address browsers use. They are set
  for `kubectl port-forward` on port 4000; behind an ingress, set them to its host, `https`
  and `443`.

> Apply it and wait for the console.
> {: .step}

```sh
$ kubectl apply -f deploy/kubernetes/console.yaml
$ kubectl -n flezha rollout status deploy/console
$ kubectl -n flezha logs deploy/console | grep -E 'NodeConnector|Endpoint'
```

```output
[info] [NodeConnector] Will connect to flezha@10.42.0.4
[info] Running FlezhaConsoleWeb.Endpoint with Bandit 1.10.3 at :::4000 (http)
[info] [NodeConnector] Connected to flezha@10.42.0.4
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
$ kubectl delete -f deploy/kubernetes/console.yaml
$ kubectl -n flezha delete secret flezha-console
```

The database's volume claim, `data-console-db-0`, stays until you delete it.
