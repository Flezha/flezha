# Authentication and authorization

For operators running a Flezha Controller: enforcement posture, distribution
cookie and transport certificates, bootstrap admin, tokens, namespaces and
quotas, role bindings, runtime knobs, on-disk layout, recovery and rollback.

## Enforcement posture

`Flezha.AuthZ.enforcement_posture/0` answers one of three states.
`Flezha.AuthZ.authorize/3` is the single policy point every `Flezha.Service.*`
operator action calls before any side effect; it returns `:ok` or
`{:error, {:forbidden, reason}}` and emits `:authorization_denied` on every
refusal.

| Posture | When | Effect |
| --- | --- | --- |
| `:open` | `FLEZHA_AUTHZ_ENABLED=false`, or the dev/test config opt-out | `authorize/3` returns `:ok` unconditionally; a warning is logged on every boot |
| `:bootstrap_pending` | enforcement configured on, no `admin@_system` claimed yet | operator verbs refuse naming `flz auth bootstrap`; `:internal` callers pass and runner-role `:sdk` contexts are policy-checked as usual, so operatives and runner connections continue |
| `:enforced` | the bootstrap was claimed (durable flag in `state.dets`) | every call is policy-checked against the role binding |

The prod default is enabled (`config_env() == :prod`); dev and test carry the
opt-out. An un-bootstrapped upgrade degrades to "operator verbs refused", never
to an outage and never to open.

Identity reaches the policy point only from the transport: a verified token
(`--token` / `FLEZHA_TOKEN`) for CLI callers, the connection's namespace
handshake for runners (`runner@<namespace>`, role `runner`); a caller-supplied
principal is never trusted. The `:internal` source is pre-authorized and only
constructible inside the Controller's BEAM — reaching it from outside needs
distribution, which the cookie gates. `FLEZHA_TOKEN` is read by the `flz`
process: export it in the shell that runs `flz`, not in the Controller's environment.

## Provision the distribution cookie

The release ships no cookie. A node booted without `RELEASE_COOKIE` logs the
refusal and sets `RELEASE_DISTRIBUTION=none`: it does not cluster, and the `rpc`
every `flz` verb rides cannot reach it. Generate one with
`openssl rand -base64 48`, keep it in your secret manager, and on Kubernetes
inject it from a Secret via `envFrom` on the Controller container.

Optional TLS distribution: point `FLEZHA_DIST_TLS_OPTFILE` at an `inet_tls_dist`
option file (server/client cert + CA, see the OTP `ssl_dist` docs); the release
then boots with `-proto_dist inet_tls`. The cookie stays mandatory either way;
use TLS distribution when Controller nodes cross a trust boundary.

## Transport certificates (QUIC)

UDS deployments need no certificates: the socket is created `0600` inside a
`0700` directory, so a local same-uid peer is the node owner. Traversal of that
directory *is* the authentication, which is why a runner run by another user
gets `access denied` rather than a connection — and why the diagnosis it prints
names the directory's owning uid and mode next to the caller's.

That boundary may be widened to exactly one named group, and only so. Setting
`FLEZHA_SOCKET_DIR_GROUP=<group>` on the controller creates the socket directory
`0750` owned by that group, and logs a warning naming it on every boot while the
variable is set — the posture `FLEZHA_TRANSPORT_AUTH=off` already establishes
for a deliberate loosening. A group the controller's user does not belong to
refuses the boot naming the group. Unset, the directory stays owner-only; no
setting makes it world-readable or world-writable.

This is a real loosening and it is on purpose: the alternative on offer is
running the runner as the controller's account, which is a broader grant than
the group, granted invisibly. See
[`deployment.md` § Two users on one machine](deployment.md).

For QUIC the server requires a client certificate chaining to the trust anchor
at `FLEZHA_QUIC_CLIENT_CA`. On first boot the Controller generates a complete
mutual-TLS set — CA, server pair, and a `client.pem`/`client.key` leaf carrying
the `clientAuth` EKU — whenever any member is missing (the CA key is never
persisted, so a partial set is regenerated whole).

| Side | Env var | Default |
| --- | --- | --- |
| Controller | `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` |
| Controller | `FLEZHA_SERVER_CERT_PATH` / `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.pem` / `.key` |
| Controller | `FLEZHA_QUIC_CLIENT_CA` | `app_data/certs/ca.pem` |
| Controller (generator output) | `FLEZHA_CLIENT_CERT_PATH` / `FLEZHA_CLIENT_KEY_PATH` | `app_data/certs/client.pem` / `.key` |
| Runner | `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` |
| Runner | `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY` | `app_data/certs/client.pem` / `.key` |

A runner sharing `app_data/` authenticates out of the box. Remote runners get
leaves minted from your own CA with any standard tooling; the leaf must carry
the `clientAuth` EKU. A refused peer logs
`[Transport.Server] Peer refused at the handshake: ...` and emits
`[:flezha, :transport, :connect_rejected]` with `stage: :handshake`.

Kubernetes: mount a Secret with `ca.pem`, `server.pem`, `server.key`, `client.pem`,
`client.key` read-only and point the Controller variables at it; the `kubernetes/`
manifests instead keep the generated set on a per-node `hostPath` and run `FLEZHA_TRANSPORT=uds`.

Opt-out: `FLEZHA_TRANSPORT_AUTH=off` (also `false|0|no`) disables client-
certificate verification; a warning is logged on every boot while it is set.

## Claim the bootstrap admin

On the first enforced boot with no `admin@_system` binding the Controller writes
a one-time token to `<auth_dir>/bootstrap.token` (mode `0600`) and logs the
path, first stripping group/other-write bits from the parent of `<auth_dir>` (a
umask-inherited `0775` `app_data/`). A sticky shared parent such as `/tmp`, or
one it cannot chmod, is refused: the Controller does not boot and logs the
recovery options (restore `roles.dets`, place a `bootstrap.token`, or set `FLEZHA_AUTHZ_ENABLED=false`).

```sh
sudo cat /var/lib/flezha/app_data/auth/bootstrap.token
flz auth bootstrap <token>
# → bootstrap claimed; admin@_system bound to admin-<random>
# → admin token (shown once, store it now): flzt_...
export FLEZHA_TOKEN=flzt_...
flz auth whoami
```

A successful claim binds `admin@_system`, flips the durable enabled flag (it
stays `true` across restarts regardless of config), unlinks `bootstrap.token`
and emits `:bootstrap_completed`. `Bootstrap.claim/2` resolves the principal as
cert SAN > cert DN > explicit principal > minted admin token; `flz auth
bootstrap` passes neither cert nor principal, so the CLI always mints a token
for a synthetic `admin-<random>` principal with TTL `:token_ttl_default_seconds`
(1 day by default) — issue a longer-lived admin token or bind a stable principal
before it expires. A second claim is rejected with `bootstrap_already_completed`.

## Issue and revoke tokens

A token binds a `(principal, namespace, role)` triple with a TTL. The wire form
is `flzt_<base32>` (256-bit); the store keeps only the SHA-256 hash, so a copied
`tokens.dets` is not a credential. The printed `id` is the first 12 hex
characters of that hash.

```sh
flz token issue bob team-orders operator --ttl 604800   # --ttl defaults to 86400
# → id:    4f2a9b1c2e7d
# → token (shown once, store it now): flzt_...
flz token list                                           # admin@_system sees every namespace
flz token revoke 4f2a9b1c                                # ≥ 8 chars of the id; immediate
```

Every gated verb accepts `--token <t>` or reads `FLEZHA_TOKEN`. Issue and
revoke emit `:token_issued` / `:token_revoked`. Verification rejects a revoked
or expired token on the next call — there is no propagation lag.

## Namespaces and quotas

Namespaces are the tenancy boundary: every Operative, state file, audit record
and observability signal carries one. Names match `[a-z][a-z0-9-]{0,62}`;
`_system` is reserved and exists at boot.

```sh
flz namespace create team-orders --Xmax_runners=4 --Xmax_state_bytes=1073741824
flz namespace show team-orders
flz namespace set-quota team-orders --Xmax_operatives=200
flz namespace delete team-orders
flz namespace list                 # running namespaces, not the catalog
```

Quota overrides ride the repeatable `--X<field>=<value>` flag; an unknown field
or a negative value is refused.

| Quota | Default | Enforced at |
| --- | --- | --- |
| `max_operatives` | `100` | `OperativeManager.start_operative/2` |
| `max_entities_per_operative` | `1_024` | `OperativeManager.start_operative/2` |
| `max_mailbox_bytes_total` | `268_435_456` (256 MiB) | caps the per-Operative credit window (`PersistenceResolver.credit_window_bytes/2`) |
| `max_message_rate_per_second` | `100_000` | stored, not enforced |
| `max_state_bytes` | `107_374_182_400` (100 GiB) | `start_operative` refuses new allocation while over; `State.Watchdog` raises a latched `[:flezha, :state, :disk_quota_breach]` alarm for a running namespace — writes are never interrupted |
| `max_runners` | `32` | `FleetManager.connect_handler/4`; a refusal emits `[:flezha, :runner, :quota_refused]` |

A refusal surfaces as `{:error, {:quota_exceeded, kind, %{current, limit}}}`.
The disk footprint is walked from `app_data/state/<namespace>/` behind a 30 s
cache and rebuilt from disk at boot; the runner count is derived from the live
session map. While the posture is not `:enforced`, every quota check passes.

## Role bindings

Roles are a fixed enumeration: `viewer`, `operator`, `admin`, `runner`; the
role→action map lives in `Flezha.AuthZ.Policy`. A principal holds at most one
`(namespace, role)` binding, so cross-namespace presence means one principal
identity per namespace.

```sh
flz role bind alice operator team-orders
flz role list
flz role unbind alice
```

Auth-state mutations (`authz.*` actions) are admin-only on the bound namespace;
only `admin@_system` may target another namespace. A token carries its own role
and namespace, so it keeps authorizing after its principal's binding is
removed — revoke the token as well.

## Audit-log integration

`:platform_authz` is in the audit log's default allow-list, so with
`:audit_log, :enabled: true` every event below lands as one JSONL record whose
`caller.principal` is the verified principal: `:authorization_denied`,
`:token_issued`, `:token_revoked`, `:namespace_created`, `:namespace_deleted`,
`:namespace_quota_updated`, `:role_bound`, `:role_unbound`,
`:bootstrap_completed`, `:rebootstrap_recovery`. Record shape, rotation and the
`flz audit` verbs are in [audit_log.md](audit_log.md). The `flz audit` verbs do
not pass `--token`; while the posture is enforced they refuse with
`principal_not_bound`, so read the JSONL files directly.

## Runtime knobs

Precedence is env var > `config :flezha, :authn_authz` > default.

| Key | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `:enabled` | `FLEZHA_AUTHZ_ENABLED` | `config_env() == :prod` | `1/true/yes/on` enables; any other value is the explicit opt-out, warned on every boot |
| `:auth_dir` | `FLEZHA_AUTHZ_DIR` | `app_data/auth` | store directory |
| `:bootstrap_token_path` | `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `<auth_dir>/bootstrap.token` | one-time token file |
| `:token_ttl_default_seconds` | — | `86_400` | TTL of the bootstrap-minted admin token (`flz token issue` has its own `--ttl`, default 86400) |
| `:sdk_token_ttl_seconds` | — | `2_592_000` | reserved; nothing consumes it yet |
| `:namespaces` | — | `%{}` | per-namespace quota overrides, e.g. `%{"team-orders" => %{max_operatives: 50}}` |
| `:application_managed` | — | `true` | tests set `false` to run isolated stores |

## On-disk layout

```text
app_data/auth/               # mode 0700; DETS files are 0600
├── state.dets               # durable enabled (claimed) flag
├── roles.dets               # principal → {namespace, role, bound_at, bound_by}
├── namespaces.dets          # name → {quotas, created_at, created_by}
├── tokens.dets              # SHA-256-hashed token registry
├── quota_counters.dets      # per-namespace counters
├── bootstrap.token          # one-time, 0600, unlinked on claim
└── REBOOTSTRAP              # recovery marker, consumed on the next boot
```

Every mutation is written with `:dets.sync/1` before it is acknowledged. Back up
`<auth_dir>` together with `app_data/state/` and `app_data/audit/`. Entity state
lives at `app_data/state/<namespace>/...`; a pre-namespace tree is moved under
`_system` once at boot by `Flezha.Boot.StatePathMigrator`.

## Recovery

**Corrupt store.** Every store opens through `Flezha.AuthZ.Store.open/2`; a file
DETS cannot open is renamed `<name>.corrupt-<unix_ts>` with one `error` log line
and a fresh table is created. The cost depends on the file:

| File | Consequence | Recovery |
| --- | --- | --- |
| `tokens.dets` | every token, including the admin's, is invalid | `flz auth rebootstrap`, then re-issue |
| `roles.dets` | every binding is gone; the next boot writes a fresh `bootstrap.token` (tokens still carry their role and keep working) | claim it, then `flz role bind` the rest |
| `namespaces.dets` | non-`_system` namespaces are forgotten; their `app_data/state/<ns>/` trees stay intact | `flz namespace create` again |
| `quota_counters.dets` | counters restart; `disk_bytes` is rebuilt from disk at boot and `runners` from the live sessions | nothing |
| `state.dets` | the claimed flag resets and the posture falls back to `:bootstrap_pending`; a plain claim is refused because the admin binding still exists | `flz auth rebootstrap` |

**Lost `admin@_system` (emergency recovery / rebootstrap).** Needs filesystem
access to `<auth_dir>` and a stopped Controller:

```sh
flz controller stop
flz auth rebootstrap          # refuses while the Controller is up; writes <auth_dir>/REBOOTSTRAP
# next boot: unbinds every admin@_system, revokes every _system admin token,
# resets the claimed flag, emits :rebootstrap_recovery, writes a fresh bootstrap.token
flz auth bootstrap <token>
```

`flz auth rebootstrap` resolves the directory from `FLEZHA_AUTHZ_DIR`, else
`<app_data>/auth`.

## Rollback

`FLEZHA_AUTHZ_ENABLED=false` restores the open posture (warned on every boot);
the stores are additive DETS files and survive the round-trip.
`FLEZHA_TRANSPORT_AUTH=off` restores unauthenticated QUIC. Both are node-wide
switches, not per-namespace.

## See also

- [audit_log.md](audit_log.md) — record schema, allow-list, `flz audit`
- [../sdk/protocol_spec.md](../sdk/protocol_spec.md) — the QUIC client-auth table on the wire
- [../internals/state_persistence.md](../internals/state_persistence.md) — namespaced state layout
- [../configuration.md](../configuration.md) — every env var in one place
- [deployment.md](deployment.md) — Kubernetes manifests
