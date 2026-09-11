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
| `:bootstrap_pending` | enforcement configured on, no `admin@_system` claimed yet | operator verbs refuse naming `flz auth bootstrap`; `:internal` callers pass and `:sdk` contexts are policy-checked as usual, so operatives and runner connections continue |
| `:enforced` | the bootstrap was claimed (durable flag in `state.dets`) | every call is policy-checked against the role binding |

The prod default is enabled (`config_env() == :prod`); dev and test carry the
opt-out. An un-bootstrapped upgrade degrades to "operator verbs refused", never
to an outage and never to open.

Identity reaches the policy point only from the transport: a verified token
(`--token` / `FLEZHA_TOKEN`) for CLI callers, the connection's namespace
handshake for runners (`runner@<namespace>`, whose role the store answers and
which is `runner` unless an operator bound it); a caller-supplied
principal is never trusted. The `:internal` source is pre-authorized and only
constructible inside the Controller's BEAM — reaching it from outside needs
distribution, which the cookie gates. `FLEZHA_TOKEN` is read by the `flz`
process: export it in the shell that runs `flz`, not in the Controller's environment.

## Provision the distribution cookie

The release ships no cookie and never falls back to a constant. A node booted
without `RELEASE_COOKIE` mints one instead: 32 random bytes from `/dev/urandom`,
written `0600` to `${FLEZHA_STATE_DIR}/cookie` (`FLEZHA_COOKIE_FILE` moves the
file), read back on every later boot, and announced on each of them. That is
enough to run and operate one node, and the value is one no other installation
holds. Only a command that starts a node (`start`, `start_iex`, `daemon`,
`daemon_iex`) writes the file; every other release command, including the
`flzvm rpc` behind each `flz` verb, only reads it. A verb run before the first
start has nothing to read and reports the controller unreachable, which
`flz boot await` waits out, rather than creating a cookie of its own.

It is not enough for a cluster. Two nodes connect only under one cookie that was
given to both, so a minted value leaves a node clustering with nobody. That is
the point, and it is also what a silent failure to form a cluster looks like.
Generate one with `openssl rand -base64 48`, keep it in your secret manager, and
on Kubernetes inject it from a Secret via `envFrom` on the Controller container.
An explicit `RELEASE_COOKIE` wins outright: nothing is minted and no file is
read.

Where the release can neither read nor mint a cookie (no `/dev/urandom`, an
unwritable state directory, a read-only filesystem) it logs the reason and sets
`RELEASE_DISTRIBUTION=none`: the boot continues, but it does not cluster and the
`rpc` every `flz` verb rides cannot reach it.

A rebuilt release under the default path can lose the file: `mix release` writes
into `${RELEASE_ROOT}`, so a rebuild can take `app_data/state` with it and the
next boot mints afresh. Nothing breaks, because the CLI reads the same file, but
a deployment that pins the cookie is a deployment this cannot happen to.

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
[`deployment/bare_metal.md` § Two users on one machine](deployment/bare_metal.md).

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

That log line is written only on the boot that creates the file. A Controller
booting again with the token still unclaimed writes nothing, so the path is not
in the log an operator is most likely to read. Any gated verb refused in that
state names the path itself:

```
$ flz controller status
controller.status: refused: authorization is enforced but the bootstrap has not
been claimed — run `flz auth bootstrap <token>` with the token in
/var/lib/flezha/app_data/auth/bootstrap.token
```

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
flz token issue archivist team-orders viewer --ttl never # no expiry
flz token list                                           # admin@_system sees every namespace
flz token revoke 4f2a9b1c                                # ≥ 8 chars of the id; immediate
```

Every gated verb accepts `--token <t>` or reads `FLEZHA_TOKEN`. Issue and
revoke emit `:token_issued` / `:token_revoked`. Verification rejects a revoked
or expired token on the next call — there is no propagation lag.

The credential is read wherever it is written: `flz operative stop --token t
op1` and `flz operative stop op1 --token t` are the same request. Every `flz`
option works that way — a verb's positional arguments are the words that remain
once its declared options and their values are taken out — so a flag written
before the subject of the command is a flag, and not the subject.

Two verbs are stricter than the rest: `flz boot await` and `flz reset` refuse an
argument they do not recognise, because one blocks for as long as its option
says and the other deletes state. Every other verb passes an unrecognised word
on and answers with its defaults.

### When a token expires

Nothing stops except the CLI. A runner's namespace is proven by its own
credential and that credential never expires (see [Runner credentials](#runner-credentials)),
so operatives and runner connections carry on; an expired operator credential
means gated verbs refuse, and that is all.

There is no automatic renewal, but a lapse is announced before it happens: an
entry that crosses `:token_expiry_warning_ms` (one day by default) of its expiry
emits `:token_expiring` on `:platform_authz`, naming the principal, the
namespace and the expiry. Alert on it. Issue a replacement before the current
one lapses:

```sh
flz token issue admin _system admin --ttl 2592000
```

If your only admin token does lapse, `flz token issue` is itself gated. Recover
with `flz auth rebootstrap` under [Recovery](#recovery). It does **not** need a
stopped Controller.

### Runner credentials

A runner does not name the namespace it works in — it proves it. What counts as
proof depends on how it connected:

| Connection | What proves the namespace |
| --- | --- |
| QUIC, client certificate carrying a SPIFFE `/ns/<name>` entity | the certificate |
| QUIC, certificate with no `/ns/` entity (the pair `flz` generates) | the presented credential |
| UDS | the presented credential |
| in-node (the Elixir SDK inside the Controller) | co-residence in the Controller's BEAM |

The connect frame still carries a namespace field, because a peer entitled to
several has to say which it wants. It is now checked: a frame naming a namespace
the credential does not entitle is refused, the refusal names both namespaces,
and no control or data frame is processed. `_system` is refused on every
transport, so no runner identity can reach the cross-namespace admin gate.

A runner credential is a `runner`-role entry of the same token registry:

```sh
flz token issue runner@team-orders team-orders runner --ttl never
# → token (shown once, store it now): flzt_...
```

Hand it to the runner as `FLEZHA_NAMESPACE_SECRET`. Three rules the registry
enforces on it:

- **It cannot expire.** `--ttl` is refused for a `runner` entry. The holder is an
  unattended process, and a credential that lapses fails it at a restart — a
  deploy or a recovery, which is exactly when nobody wants a new failure.
- **It cannot be `_system`.** Refused at issue as well as at connect.
- **Revoking it closes the sessions it authenticated.** Nothing else would: a
  credential with no expiry leaves no deadline that evicts a holder.

A namespace may hold as many live credentials as you issue, so a rotation is
rolling and disconnects nothing:

```sh
flz token issue runner@team-orders team-orders runner --ttl never   # 1. the new one
# 2. roll the fleet onto it, one runner at a time
flz token list                                                      # 3. find the old id
flz token revoke <old-id>                                           # 4. retire it
```

Step 4 closes any session still on the old credential, which is why step 2 comes
first. A QUIC deployment whose certificates carry `/ns/` entities needs no
credential at all; its expiry is the PKI's to manage.

## Namespaces and quotas

Namespaces are the tenancy boundary: every Operative, state file, audit record
and observability signal carries one. Names match `[a-z][a-z0-9_-]{0,62}`: an
underscore is allowed after the leading letter, never as the first character.
`_system` is reserved and exists at boot.

```sh
flz namespace create team-orders --Xmax_runners=4 --Xmax_state_bytes=1073741824
flz namespace show team-orders
flz namespace set-quota team-orders --Xmax_operatives=200
flz namespace delete team-orders
flz namespace list                 # the catalog, with a hub-running column
```

`create` and `delete` both answer whether they changed anything: a name the
catalog already holds is refused `:already_exists`, and a name it does not hold
is refused `:not_found`. Both refusals go to standard error and exit 1, so a
teardown script cannot read a typo as a successful delete.

A namespace listing shows the caller its own namespace and no other, and
`admin@_system` sees the whole cluster, which is the operator view. The same
rule narrows the published `Flezha.Service.Operative.list_namespaces/1` a
console reads, so a terminal and a console cannot disagree about which
namespaces a tenant knows exist.

A context that carries no namespace at all sees them all. That is the ungated
deployment's operator, and narrowing it to `_system` would hide the namespaces
it just created — but it is not only that caller. Under enforcement a principal
bound in the role store rather than on its context is judged in `_system`, so
one bound there passes and gets the open answer too; one bound to a tenant is
refused `namespace_mismatch` before the listing runs, which is why the open
answer crosses no tenant boundary. `flz token list` takes the closed answer for
the same caller and shows `_system` only.

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

Roles are a fixed enumeration: `viewer`, `app`, `operator`, `admin`, `runner`,
`spawner`;
the role→action map lives in `Flezha.AuthZ.Policy`. A principal holds at most
one `(namespace, role)` binding, so cross-namespace presence means one
principal identity per namespace.

```sh
flz role bind alice operator team-orders
flz role list
flz role unbind alice
```

| Role | What it adds |
| --- | --- |
| `viewer` | the reads: operatives, plugins, entity state, audit, observability, the schedule registry |
| `app` | viewer plus the lifecycle an application runs on its own work: start, stop and send to an operative, including the start and the delivery a procedure's `ctx.spawn_workflow` asks for, enable and disable a plugin. It destroys no record and moves no work it did not create, which is why it is not `operator`: no DLQ drop or replay, no state rotation or deletion, no rewind, no replay and no rescale |
| `operator` | app plus the rest of the operative and plugin lifecycle, audit rotation, observability mutation, the schedule family, the dead-letter queue and the records in flight |
| `admin` | operator plus auth state, namespace and quota mutation, plugin install/upgrade/uninstall, controller shutdown and the controller-wide settings. Only `admin@_system` may target another namespace |
| `runner` | `runner.connect` and nothing else. It is outside the hierarchy: it is what a runner connection resolves to before an operator grants it more |
| `spawner` | runner plus the two rights a procedure's `ctx.spawn_workflow` needs in its own namespace, `operative.start` and `operative.send`, and nothing else. Those rights also open `Operative.start` and `Operative.send` on the SDK administration surface; no stop, plugin or schedule verb. It is outside the hierarchy like `runner`, and `app` holds everything it grants |

`app` and `spawner` also carry `runner.connect`, because the only principal
ever bound to either is a runner's; `operator` and `admin` do not, because an
operator credential is not a runner. For the same reason `flz token issue`
refuses both: a runner proves its namespace with a `runner` credential.

### An application that administers its own namespace

A connected runner's role is read from the role store on every call, keyed on
the `runner@<namespace>` principal the transport minted. Unbound, it is
`runner`, which grants the connect and nothing else: an application is refused
every operator action until somebody says otherwise. Saying otherwise is one
command, once per namespace:

```sh
flz role bind runner@team-orders app team-orders
```

From then on that namespace's application code administers the namespace its
connection proved, with no credential of its own and nothing that expires. The
request names no namespace and no principal, so it cannot reach another
namespace; the controller fills both from the connection. Every act is judged
at the same policy point, against the same row, and writes the same audit
record an operator's would, naming `runner@<namespace>` as the principal and
`sdk` as the surface.

A binding made for a different namespace than the one the connection proved
grants nothing, and `flz role unbind` takes effect on the next call rather than
the next reconnect, because the role is read per call rather than stamped at
connect.

A namespace whose procedures spawn workflows, and that needs nothing else from
this surface, is bound to `spawner` instead. It grants the start and the
delivery a `ctx.spawn_workflow` asks for, and not the stop or the plugin enable
and disable that `app` adds:

```sh
flz role bind runner@team-orders spawner team-orders
```

Those two rights are also what `Operative.start` and `Operative.send` on this
surface are judged against, so a `spawner` namespace's application code can
call those two as well, and no other verb that changes anything.

The SDK side is `docs/sdk/admin.md`.

Auth-state mutations (`authz.*` actions) are admin-only on the bound namespace;
only `admin@_system` may target another namespace. A token carries its own role
and namespace, so it keeps authorizing after its principal's binding is
removed — revoke the token as well.

### The declaration: every verb, its action, its grant, where it is judged

Every operator verb is one row of `Flezha.AuthZ.Verbs`, and one function —
`Flezha.AuthZ.Verb.run/5` — applies the row on every surface: the `flz` wire,
the published service API and the runner session. A row names the action the
policy judges, the rule that resolves the namespace it is judged in, and the
audit it leaves. The role column is the lowest role the action is granted to;
`admin` inherits `operator`, which inherits `viewer`; `runner` and `spawner`
sit outside that line, as roles of a runner connection rather than of an
operator's token. `public` rows are served to any caller; `none` rows are wire
names that are not verbs.

A row whose scope is a stored row's field (a DLQ entry's namespace, a
schedule's, an Entity's class) reads that row only for a caller that is bound
and whose role grants the action, so an unbound or under-privileged caller is
told nothing about whether the name exists.

| Row | `flz` wire name(s) | Action | Role | Judged in | Audit |
| --- | --- | --- | --- | --- | --- |
| `boot_status` | `boot_status` | `public` | - | - | - |
| `boot_await` | `boot_await` | `public` | - | - | - |
| `kind_read` | published surface only | `public` | - | - | - |
| `partition_report` | published surface only | `public` | - | - | - |
| `cron_arithmetic` | published surface only | `public` | - | - | - |
| `retention_default` | published surface only | `public` | - | - | - |
| `authz_bootstrap` | `authz_render_bootstrap` | `none` | - | - | - |
| `build_mem_top_snapshot` | `build_mem_top_snapshot` | `none` | - | - | - |
| `group_partition_rows` | `group_partition_rows` | `none` | - | - | - |
| `audit_list` | `audit_log_render_list` | `audit.read` | viewer | the caller's own namespace | before on `audit_log` |
| `audit_read` | `audit_log_render_read` | `audit.read` | viewer | the caller's own namespace | before on `audit_log` |
| `audit_tail` | `audit_log_render_tail` | `audit.read` | viewer | the caller's own namespace | before on `audit_log` |
| `audit_stats` | `audit_log_render_stats` | `audit.read` | viewer | the caller's own namespace | before on `audit_log` |
| `audit_rotate` | `audit_log_render_rotate` | `audit.rotate` | operator | the caller's own namespace | before on `audit_log` |
| `audit_unsubscribe` | published surface only | `audit.read` | viewer | the caller's own namespace | - |
| `authz_whoami` | `authz_render_whoami`, `authz_whoami` | `authz.whoami` | viewer | the caller's own namespace | - |
| `token_issue` | `authz_render_token_issue` | `authz.token_issue` | admin | argument 2 (the namespace named) | - |
| `token_revoke` | `authz_render_token_revoke` | `authz.token_revoke` | admin | the stored row's `namespace` | - |
| `token_list` | `authz_render_token_list` | `authz.token_list` | viewer | the caller's own namespace | - |
| `role_bind` | `authz_render_role_bind` | `authz.role_bind` | admin | argument 3 (the namespace named) | after on `namespace_service` |
| `role_unbind` | `authz_render_role_unbind` | `authz.role_unbind` | admin | the stored row's `namespace` | after on `namespace_service` |
| `role_list` | `authz_render_role_list` | `authz.role_list` | viewer | the caller's own namespace | - |
| `namespace_create` | `authz_render_namespace_create` | `authz.namespace_create` | admin | argument 1 (the namespace named) | after on `namespace_service` |
| `namespace_delete` | `authz_render_namespace_delete` | `authz.namespace_delete` | admin | argument 1 (the namespace named) | after on `namespace_service` |
| `namespace_set_quota` | `authz_render_namespace_set_quota` | `authz.namespace_set_quota` | admin | argument 1 (the namespace named) | after on `namespace_service` |
| `namespace_show` | `authz_render_namespace_show` | `authz.namespace_show` | viewer | argument 1 (the namespace named) | - |
| `namespace_list` | published surface only | `authz.namespace_list` | viewer | `_system` | - |
| `namespace_roles` | published surface only | `authz.role_list` | viewer | argument 1 (the namespace named) | - |
| `controller_status` | `controller_status` | `controller.status` | viewer | the caller's own namespace | - |
| `controller_mem_top` | `controller_mem_top` | `controller.status` | viewer | the caller's own namespace | - |
| `controller_hub_id` | published surface only | `controller.status` | viewer | argument 1 (the namespace named) | - |
| `controller_recovery` | published surface only | `operative.list` | viewer | argument 1 (the namespace named) | - |
| `controller_stop` | `controller_stop` | `controller.stop` | admin | `_system` | after on `controller_service` |
| `cluster_status` | `cluster_status` | `operative.list` | viewer | argument 1 (the namespace named) | - |
| `list_namespaces` | `list_namespaces` | `operative.list` | viewer | the caller's own namespace | - |
| `list_operatives` | `list_operatives` | `operative.list` | viewer | argument 1 (the namespace named) | - |
| `list_runners` | `list_runners` | `operative.list` | viewer | argument 1 (the namespace named) | - |
| `operative_info` | `operative_info` | `operative.list` | viewer | argument 1 (the namespace named) | - |
| `operative_classes` | published surface only | `operative.list` | viewer | the caller's own namespace | - |
| `operative_start` | `operative_start` | `operative.start` | spawner | argument 1 (the namespace named) | after on `operative_service` |
| `operative_stop` | `operative_stop` | `operative.stop` | app | argument 1 (the namespace named) | after on `operative_service` |
| `operative_scale` | `operative_scale` | `operative.scale` | operator | argument 1 (the namespace named) | after on `workflow_service` |
| `operative_rewind` | `operative_rewind` | `operative.rewind` | operator | argument 1 (the namespace named) | after on `workflow_service` |
| `operative_send` | `operative_send` | `operative.send` | spawner | argument 1 (the namespace named) | after on `operative_service` |
| `spawn_deliver` | runner session only | `operative.send` | spawner | argument 1 (the namespace named) | - |
| `operative_replay` | `operative_replay` | `operative.replay` | operator | the stored row's `namespace` | after on `operative_service` |
| `operative_listen` | `operative_listen` | `plugin.enable` | app | argument 1 (the namespace named) | after on `plugin_service` |
| `entity_state` | `entity_state` | `state.get` | viewer | argument 1 (the namespace named) | - |
| `schedule_run` | `schedule_run` | `operative.start` | spawner | the stored row's `namespace` | after on `operative_service` |
| `runner_connect` | published surface only | `runner.connect` | runner | argument 1 (the namespace named) | - |
| `plugin_list` | `list_plugins` | `plugin.list` | viewer | the caller's own namespace | - |
| `list_installed_plugins` | `list_installed_plugins` | `plugin.list` | viewer | the caller's own namespace | - |
| `plugin_info` | `installed_plugin?`, `installed_plugin_info`, `plugin_help` | `plugin.info` | viewer | the caller's own namespace | - |
| `plugin_enable` | `enable_plugin` | `plugin.enable` | app | the stored row's `namespace` | after on `plugin_service` |
| `plugin_disable` | `disable_plugin` | `plugin.disable` | app | the stored row's `namespace` | after on `plugin_service` |
| `install_plugin` | `install_plugin` | `plugin.install` | admin | `_system` | after on `plugin_service` |
| `upgrade_plugin` | `upgrade_plugin` | `plugin.upgrade` | admin | `_system` | after on `plugin_service` |
| `uninstall_plugin` | `uninstall_plugin` | `plugin.uninstall` | admin | `_system` | after on `plugin_service` |
| `installer_enable_plugin` | `installer_enable_plugin` | `plugin.enable` | operator | `_system` | after on `plugin_service` |
| `installer_disable_plugin` | `installer_disable_plugin` | `plugin.disable` | operator | `_system` | after on `plugin_service` |
| `observability_sources` | `observability_render_sources` | `observability.query` | viewer | the caller's own namespace | - |
| `observability_sinks` | `observability_render_sinks` | `observability.query` | viewer | the caller's own namespace | - |
| `observability_query` | published surface only | `observability.query` | viewer | the caller's own namespace | - |
| `observability_subscribe` | published surface only | `observability.subscribe` | viewer | the caller's own namespace | before on `observability_service` |
| `observability_unsubscribe` | published surface only | `observability.subscribe` | viewer | the caller's own namespace | before on `observability_service` |
| `observability_tail` | `observability_render_tail` | `observability.subscribe` | viewer | the caller's own namespace | before on `observability_service` |
| `observability_enable` | `observability_render_enable` | `observability.enable_source` | operator | the caller's own namespace | before on `observability_service` |
| `observability_disable` | `observability_render_disable` | `observability.disable_source` | operator | the caller's own namespace | before on `observability_service` |
| `dlq_list` | `dlq_list` | `dlq.list` | operator | the stored row's `namespace` | - |
| `dlq_stats` | published surface only | `dlq.list` | operator | `_system` | - |
| `dlq_show` | `dlq_show` | `dlq.show` | operator | the stored row's `namespace` | - |
| `dlq_drop` | `dlq_drop` | `dlq.drop` | operator | the stored row's `namespace` | after on `dlq_service` |
| `dlq_replay` | `dlq_replay` | `dlq.replay` | operator | the stored row's `namespace` | after on `dlq_service` |
| `schedule_list` | `schedule_list` | `schedule.list` | viewer | `_system` | - |
| `schedule_add` | `schedule_add` | `schedule.add` | operator | `_system` | after on `schedule_service` |
| `schedule_remove` | `schedule_remove` | `schedule.remove` | operator | `_system` | after on `schedule_service` |
| `schedule_assert` | published surface only | `schedule.assert` | operator | `_system` | after on `schedule_service` |
| `schedule_retract` | published surface only | `schedule.retract` | operator | `_system` | after on `schedule_service` |
| `state_get` | `state_get` | `state.get` | viewer | the stored row's `namespace` | - |
| `state_list` | `state_list` | `state.list` | viewer | the stored row's `namespace` | - |
| `state_at` | `state_at` | `state.at` | viewer | the stored row's `namespace` | - |
| `state_rotate` | `state_rotate` | `state.rotate` | operator | the stored row's `namespace` | after on `state_service` |
| `state_delete` | `state_delete` | `state.delete` | admin | the stored row's `namespace` | after on `state_service` |
| `state_retention` | `state_retention` | `state.retention` | admin | `_system` | after on `state_service` |
| `state_outbox` | `state_outbox` | `state.records` | operator | the stored row's `namespace` | - |
| `state_inbox` | `state_inbox` | `state.records` | operator | the stored row's `namespace` | - |
| `state_journal` | `state_journal` | `state.records` | operator | the stored row's `namespace` | - |
| `state_metrics` | published surface only | `state.list` | viewer | `_system` | - |

Four of those choices are worth stating plainly.

**A parked payload is business data.** `dlq list` and `dlq show` are the
operator's, not the viewer's, because a DLQ entry holds the record that failed —
verbatim, with its entity key and its error. Reading one is at least as
privileged as the drop that destroys it, and no viewer workflow needs it. The
same rule places `state inbox`, `state journal` and `state outbox`, which read
records in flight, under `state.records` beside `operative.rewind` and
`operative.replay`.

**Schedule management is a `_system` privilege.** The schedule store is one
durable table for the whole controller and the hub is a cluster-wide singleton;
a schedule's `--namespace` says where its firings land, it does not partition
the registry. So a credential for `flz schedule add | list | remove` is one
bound to `_system` (`flz token issue ci _system operator`), or `admin@_system`.
A namespace-bound operator cannot manage schedules.

**`flz schedule run` is judged where the instance starts.** Firing a schedule by
hand starts an operative, so it authorizes `operative.start` in the schedule's
own namespace rather than in `_system`. An `operator@_system` token — the
credential above for managing schedules — can add, list and remove a schedule
but cannot run one; a token bound to the schedule's namespace with
`operative.start` (an `operator` or `admin` there), or `admin@_system`, can.

**A spawn's delivery is judged and not recorded.** A procedure's `ctx.spawn_workflow`
that names a running instance feeds it, and that delivery is judged as
`operative.send` through the `spawn_deliver` row, which writes no record of a
permitted delivery because it happens once per event. A refused one is audited as
`authorization_denied`, like every refusal. The start a spawn makes is
`operative_start` and is recorded like any other.

**`flz auth whoami` is a gated read.** It answers the identity a token
resolves to, so while authorization is enforced an invocation with no token is
refused naming the credential rather than printing an unbound identity.

Every grant is reversible in `Flezha.AuthZ.Policy`: move an action between
`@viewer_actions`, `@operator_extra` and `@admin_extra` and the grant moves
with it, on every surface at once.

## Audit-log integration

Two kinds of record reach the audit log. The auth stores' own events land on
`:platform_authz` with the verified principal in `caller.principal`:
`:authorization_denied`, `:token_issued`, `:token_revoked`,
`:bootstrap_completed`, `:rebootstrap_recovery`. An operator verb's act lands on
the source its row declares — `namespace_service` for namespace and role
mutations, `dlq_service`, `schedule_service`, `operative_service`,
`plugin_service`, `state_service`, `controller_service`, `workflow_service`,
`observability_service` and `audit_log` — in one record shape: the record's
`name` is the verb, `metadata.action` the action it was judged as,
`metadata.outcome` (`ok` or `error`) for a mutation, `namespace` where it was
judged, and `caller` who asked and from which surface. Every declared source is
in the audit log's default allow-list by construction. Record shape, rotation
and the `flz audit` verbs are in [audit_log.md](audit_log.md); the `flz audit`
verbs carry `--token` like every other verb.

## Runtime knobs

Precedence is env var > `config :flezha, :authn_authz` > default.

| Key | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `:enabled` | `FLEZHA_AUTHZ_ENABLED` | `config_env() == :prod` | `1/true/yes/on` enables; any other value is the explicit opt-out, warned on every boot |
| `:auth_dir` | `FLEZHA_AUTHZ_DIR` | `app_data/auth` | store directory |
| `:bootstrap_token_path` | `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `<auth_dir>/bootstrap.token` | one-time token file |
| `:token_ttl_default_seconds` | — | `86_400` | TTL of the bootstrap-minted admin token (`flz token issue` has its own `--ttl`, default 86400, and takes `never`) |
| `:token_expiry_warning_ms` | `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` | `86_400_000` | how far ahead of its expiry an entry emits `:token_expiring` |
| `:token_expiry_check_interval_ms` | — | `300_000` | how often the registry looks for one |
| `:namespaces` | — | `%{}` | per-namespace quota overrides, e.g. `%{"team-orders" => %{max_operatives: 50}}` |
| `:application_managed` | — | `true` | tests set `false` to run isolated stores |

## On-disk layout

```text
app_data/auth/               # mode 0700; DETS files are 0600
├── state.dets               # durable enabled (claimed) flag
├── roles.dets               # principal → {namespace, role, bound_at, bound_by}
├── namespaces.dets          # name → {quotas, created_at, created_by}
├── tokens.dets              # SHA-256-hashed token registry, runner credentials included
├── quota_counters.dets      # per-namespace counters
├── bootstrap.token          # one-time, 0600, unlinked on claim
└── REBOOTSTRAP              # recovery marker, consumed on the next boot
```

Every mutation is written with `:dets.sync/1` before it is acknowledged. Back up
`<auth_dir>` together with `app_data/state/` and `app_data/audit/`. Entity state
lives at `app_data/state/<namespace>/...`; a pre-namespace tree is moved under
`_system` once at boot by `Flezha.Boot.StatePathMigrator`.

## Recovery

**Corrupt store.** Every store opens through `Flezha.DetsStore.open/2`; a file
DETS cannot open is renamed `<name>.corrupt-<unix_ts>` with one `error` log line
and a fresh table is created. The cost depends on the file:

| File | Consequence | Recovery |
| --- | --- | --- |
| `tokens.dets` | every token is invalid, the admin's and every runner credential | `flz auth rebootstrap`, then re-issue both |
| `roles.dets` | every binding is gone; the next boot writes a fresh `bootstrap.token` (tokens still carry their role and keep working) | claim it, then `flz role bind` the rest |
| `namespaces.dets` | non-`_system` namespaces are forgotten; their `app_data/state/<ns>/` trees stay intact | `flz namespace create` again |
| `quota_counters.dets` | counters restart; `disk_bytes` is rebuilt from disk at boot and `runners` from the live sessions | nothing |
| `state.dets` | the claimed flag resets and the posture falls back to `:bootstrap_pending`; a plain claim is refused because the admin binding still exists | `flz auth rebootstrap` |

**Lost `admin@_system` (emergency recovery / rebootstrap).** Needs filesystem
access to `<auth_dir>` and nothing else. That access is already enough to stop
the Controller, so a claim against a running one grants no authority it did not
already carry — and recovery therefore costs no downtime.

With the Controller **running**:

```sh
flz auth rebootstrap          # mints <auth_dir>/bootstrap.token (0600) and prints it
flz auth bootstrap <token>    # unbinds every admin@_system, revokes every _system
                              # admin token, binds the new admin, emits
                              # :rebootstrap_recovery with mode: :online
```

With the Controller **stopped**, the marker path is unchanged:

```sh
flz controller stop
flz auth rebootstrap          # writes <auth_dir>/REBOOTSTRAP
# next boot: unbinds every admin@_system, revokes every _system admin token,
# resets the claimed flag, emits :rebootstrap_recovery, writes a fresh bootstrap.token
flz auth bootstrap <token>
```

Either way the claim succeeds exactly once — it unlinks the file — and a claim
with no file present is refused naming the path it expected. Operatives, runner
connections and their credentials are untouched by a recovery: only
`admin@_system` and its `_system` admin tokens are invalidated.

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
- [deployment/kubernetes.md](deployment/kubernetes.md) — Kubernetes manifests
