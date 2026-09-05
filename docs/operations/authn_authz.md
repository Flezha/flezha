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

The credential is read wherever it is written: `flz operative stop --token t
op1` and `flz operative stop op1 --token t` are the same request. Every `flz`
option works that way — a verb's positional arguments are the words that remain
once its declared options and their values are taken out — so a flag written
before the subject of the command is a flag, and not the subject.

Two verbs are stricter than the rest: `flz boot await` and `flz reset` refuse an
argument they do not recognise, because one blocks for as long as its option
says and the other deletes state. Every other verb passes an unrecognised word
on and answers with its defaults.

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

### The declaration: every verb, its action, its grant, where it is judged

Every operator verb is one row of `Flezha.AuthZ.Verbs`, and one function —
`Flezha.AuthZ.Verb.run/5` — applies the row on every surface: the `flz` wire,
the published service API and the runner session. A row names the action the
policy judges, the rule that resolves the namespace it is judged in, and the
audit it leaves. The role column is the lowest role the action is granted to;
`admin` inherits `operator`, which inherits `viewer`. `public` rows are served
to any caller; `none` rows are wire names that are not verbs.

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
| `operative_start` | `operative_start` | `operative.start` | operator | argument 1 (the namespace named) | after on `operative_service` |
| `operative_stop` | `operative_stop` | `operative.stop` | operator | argument 1 (the namespace named) | after on `operative_service` |
| `operative_scale` | `operative_scale` | `operative.scale` | operator | argument 1 (the namespace named) | after on `workflow_service` |
| `operative_rewind` | `operative_rewind` | `operative.rewind` | operator | argument 1 (the namespace named) | after on `workflow_service` |
| `operative_send` | `operative_send` | `operative.send` | operator | argument 1 (the namespace named) | after on `operative_service` |
| `operative_replay` | `operative_replay` | `operative.replay` | operator | the stored row's `namespace` | after on `operative_service` |
| `operative_listen` | `operative_listen` | `plugin.enable` | operator | argument 1 (the namespace named) | after on `plugin_service` |
| `entity_state` | `entity_state` | `state.get` | viewer | argument 1 (the namespace named) | - |
| `schedule_run` | `schedule_run` | `operative.start` | operator | the stored row's `namespace` | after on `operative_service` |
| `runner_connect` | published surface only | `runner.connect` | runner | argument 1 (the namespace named) | - |
| `plugin_list` | `list_plugins` | `plugin.list` | viewer | the caller's own namespace | - |
| `list_installed_plugins` | `list_installed_plugins` | `plugin.list` | viewer | the caller's own namespace | - |
| `plugin_info` | `installed_plugin?`, `installed_plugin_info`, `plugin_help` | `plugin.info` | viewer | the caller's own namespace | - |
| `plugin_enable` | `enable_plugin` | `plugin.enable` | operator | the stored row's `namespace` | after on `plugin_service` |
| `plugin_disable` | `disable_plugin` | `plugin.disable` | operator | the stored row's `namespace` | after on `plugin_service` |
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

**Corrupt store.** Every store opens through `Flezha.DetsStore.open/2`; a file
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
