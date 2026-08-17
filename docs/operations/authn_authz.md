# Operating authenticated Flezha (`finish-platform-authn-authz`)

Every transport surface authenticates its peer by default: the QUIC runner
transport verifies client certificates, the UDS socket is filesystem-gated,
Erlang distribution requires a provisioned cookie, and CLI verbs carry a
verified token. This page is the provisioning runbook; the concept guide is
[`../platform_authn_authz.md`](../platform_authn_authz.md).

## 1. Provision the distribution cookie

The release no longer ships a constant cookie. A node booted without
`RELEASE_COOKIE` runs with **distribution disabled** (it logs the refusal and
sets `RELEASE_DISTRIBUTION=none`) — it will not cluster, and `flzvm rpc`
(which every `flz` verb rides) will not reach it.

```sh
# generate once, store in your secret manager
openssl rand -base64 48
```

Kubernetes:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: flezha-cluster-cookie
type: Opaque
stringData:
  RELEASE_COOKIE: "<generated-cookie>"
---
# in the controller container spec:
envFrom:
  - secretRef:
      name: flezha-cluster-cookie
```

Optional TLS distribution: point `FLEZHA_DIST_TLS_OPTFILE` at an
`inet_tls_dist` option file (server/client cert + CA, see the OTP `ssl_dist`
docs); the release then boots with `-proto_dist inet_tls`. The provisioned
cookie remains the mandatory floor either way — TLS-dist is recommended when
controller nodes cross a trust boundary.

## 2. Transport certificates (QUIC deployments)

UDS deployments need no certs: the socket and its directory are owner-only
(`0600` / `0700`), so a local same-uid peer IS the node owner.

For QUIC, the server requires a client certificate chaining to the trust
anchor at `FLEZHA_QUIC_CLIENT_CA` (default `app_data/certs/ca.pem`). On a
single-host deployment the controller's cert generator already produces a
full mutual-TLS set on first boot:

```
app_data/certs/ca.pem          # trust anchor (server AND client verification)
app_data/certs/server.pem/.key # QUIC server identity
app_data/certs/client.pem/.key # runner client identity (clientAuth EKU)
```

A runner sharing `app_data/` authenticates out of the box. Remote runners
get operator-provisioned certs, pointed at via:

```sh
FLEZHA_CA_CERT_PATH=/etc/flezha/certs/ca.pem        # server verification
FLEZHA_QUIC_CLIENT_CERT=/etc/flezha/certs/client.pem
FLEZHA_QUIC_CLIENT_KEY=/etc/flezha/certs/client.key
```

To mint runner certs from your own CA, any standard tooling works; the leaf
must carry the `clientAuth` EKU. A refused peer surfaces as
`[flz_nif] QUIC peer refused before handshake completed: ...` in the
controller log.

Kubernetes cert mounts:

```yaml
volumes:
  - name: flezha-certs
    secret:
      secretName: flezha-transport-certs   # ca.pem, server.pem, server.key, client.pem, client.key
containers:
  - name: controller
    volumeMounts:
      - { name: flezha-certs, mountPath: /etc/flezha/certs, readOnly: true }
    env:
      - { name: FLEZHA_CA_CERT_PATH,      value: /etc/flezha/certs/ca.pem }
      - { name: FLEZHA_SERVER_CERT_PATH,  value: /etc/flezha/certs/server.pem }
      - { name: FLEZHA_SERVER_KEY_PATH,   value: /etc/flezha/certs/server.key }
      - { name: FLEZHA_QUIC_CLIENT_CA,    value: /etc/flezha/certs/ca.pem }
```

**Opt-out (loud):** `FLEZHA_TRANSPORT_AUTH=off` disables QUIC client-cert
verification. A warning is emitted on every boot for as long as it is set.

**flezha_market:** its ConfigMap/DaemonSet in that repo need the cookie
Secret and the cert mounts above before its next deploy — controller and
runners redeploy together (no wire change; the transport posture flips).

## 3. Claim the bootstrap, issue tokens

On the first enforced boot with no `admin@_system`, the controller writes a
one-time token and logs its path:

```sh
$ sudo cat /var/lib/flezha/app_data/auth/bootstrap.token
$ flz auth bootstrap <token>
# → admin token (shown once, store it now): flzt_...
$ export FLEZHA_TOKEN=flzt_...
```

Until the claim, gated operator verbs refuse naming this procedure while
dataflow continues (`:bootstrap_pending` posture). Then:

```sh
flz namespace create team-orders --Xmax_runners=4 --Xmax_state_bytes=1073741824
flz role bind alice operator team-orders
flz token issue alice team-orders operator --ttl 604800
flz auth whoami --token flzt_...        # or export FLEZHA_TOKEN
```

Every gated verb accepts `--token <t>` or reads `FLEZHA_TOKEN` from the
CLI process's environment. The token resolves server-side into the
principal/namespace/role it was issued for; nothing else is trusted.

## 4. Break-glass: lost `admin@_system`

Recovery requires filesystem access to `app_data/auth/` — the same trust the
original bootstrap assumes — and a STOPPED controller:

```sh
flz controller stop
flz auth rebootstrap          # writes app_data/auth/REBOOTSTRAP
# start the controller; on boot it:
#   - unbinds every admin@_system principal,
#   - revokes every _system admin token,
#   - resets the durable claimed flag,
#   - emits an auditable :rebootstrap_recovery event,
#   - writes a fresh bootstrap.token to claim (step 3).
```

`flz auth rebootstrap` refuses to run while the controller is up.

## 5. Rollback

`FLEZHA_AUTHZ_ENABLED=false` restores the open posture (warned on every
boot); the auth stores are additive DETS files and survive the round-trip.
`FLEZHA_TRANSPORT_AUTH=off` restores unauthenticated QUIC. Neither is
namespaced — both are node-wide switches.
