# Authorization

Authorization decides who may do what. With it on, the controller takes no
command on trust: every caller says who it is, and may do only what that
identity is allowed. An `flz` command says it with a token, and a runner with a
credential of its own.

It is on by default in a release. A token names a principal, such as an admin
or `runner@trading`, and grants it a role in one namespace; the role decides
which commands that principal may run. A role is granted inside one
[namespace](namespaces.md), so a token is good in that namespace alone. The
exception is the admin in `_system`, the controller's own namespace, which
reaches every other one.

## How it works

The controller is in one of three postures:

| Posture | When | Effect |
| --- | --- | --- |
| open | `FLEZHA_AUTHZ_ENABLED=false`, or the dev and test configuration | every call is allowed; a warning is logged on every boot |
| bootstrap pending | enforcement on, no admin claimed yet | administration verbs are refused, naming `flz auth bootstrap`; operators and runner connections carry on |
| enforced | the admin was claimed (a durable flag) | every call is checked against the caller's roles |

A release enforces by default; dev and test builds do not. An upgrade that was
never bootstrapped degrades to "administration verbs refused", not to an
outage and not to open.

Identity comes only from the transport, never from what a caller claims: a
verified token (`--token` or `FLEZHA_TOKEN`) for `flz`, and the connection's
proven namespace for a runner (`runner@<namespace>`). Each refusal is recorded
as `:authorization_denied`. `FLEZHA_TOKEN` is read by the `flz` process: export
it in the shell that runs `flz`, not in the controller's environment.

## Claim the admin

With enforcement on and no admin claimed, the controller writes a one-time
token to `<auth_dir>/bootstrap.token` (`app_data/auth/bootstrap.token` by
default, mode `0600`) and logs its path. Whoever claims that token becomes the
first administrator, and the file is deleted.

```sh
$ flz auth bootstrap "$(cat app_data/auth/bootstrap.token)"
```

```output
bootstrap claimed; admin@_system bound to admin-8cc3f398fb1b4ae5
admin token (shown once, store it now): flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
```

```sh
$ export FLEZHA_TOKEN=flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
$ flz auth whoami
```

```output
principal: admin-8cc3f398fb1b4ae5
namespace: _system
role:      admin
```

The admin token from the claim lasts one day. Issue a longer one before it
lapses ([below](#tokens)). A second claim is refused
(`bootstrap_already_completed`).

The path is logged only on the boot that creates the file. A controller that
boots again with the token still unclaimed logs nothing, but every gated verb
refused in that state names the path:

```output
error: authorization is enforced and no admin has claimed the bootstrap yet; its token is in /var/lib/flezha/app_data/auth/bootstrap.token on the controller (authz.bootstrap_pending)
hint: flz auth bootstrap <token>
```

Before writing the token, the controller removes group and other write
permission from the parent of the auth directory (such as an `app_data/`
created `0775` by a umask). A sticky shared parent such as `/tmp`, or one it
cannot change, is refused: the controller does not boot, and logs the ways out
(restore `roles.dets`, place a `bootstrap.token`, or set
`FLEZHA_AUTHZ_ENABLED=false`).

**Two refusals tell you which state you are in.** `authz.bootstrap_pending`
names a file: the admin was never claimed, and the token is in that file.
`authz.principal_not_bound` means it was claimed, and this shell has no token:

```output
error: the call carries no verifiable identity (authz.principal_not_bound)
hint: export FLEZHA_TOKEN=<token>
```

## Tokens

A token binds a principal, a namespace and a role, with a lifetime. It looks
like `flzt_<base32>` (256 bits); the controller keeps only its SHA-256 hash, so
a copied token file is not a credential. The printed `id` is the first 12 hex
characters of that hash.

```sh
$ flz token issue bob team-orders ops --ttl 604800
$ flz token issue archivist team-orders viewer --ttl never
$ flz token issue admin _system admin --ttl 2592000
$ flz token list
$ flz token revoke 4f2a9b1c
```

The positionals are `<principal> <namespace> <role>`. `--ttl` is in seconds,
86 400 (one day) by default, or `never`. `flz token list` shows the caller's
namespace (`admin@_system` sees each one). `revoke` takes at least 8
characters of the id and takes effect on the next call. Each gated verb takes
`--token <t>` or reads `FLEZHA_TOKEN`.

An option is read wherever it is written: `flz operator stop --token t op1`
and `flz operator stop op1 --token t` are the same request. Two verbs are
stricter: `flz boot await` and `flz reset` refuse an argument they do not
recognise, because one blocks for as long as its option says and the other
deletes data. Other verbs ignore an unknown word and use their defaults.

**When a token expires**, only the CLI stops: operators and runner connections
carry on, because a runner proves its namespace with a credential that does
not expire. There is no automatic renewal, but a lapse is announced: a token
within `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` (one day) of its expiry emits
`:token_expiring` on `:platform_authz`, naming the principal, the namespace and
the expiry. Alert on it, and issue a replacement before the current one
lapses. If the only admin token lapses, recover with `flz auth rebootstrap`
([below](#a-lost-admin)), which does not need the controller stopped.

## Runner credentials

A runner does not just name the namespace it works in; it proves it:

| Connection | What proves the namespace |
| --- | --- |
| QUIC, client certificate carrying a SPIFFE `/ns/<name>` entry | the certificate |
| QUIC, certificate without `/ns/` (the pair the controller generates) | the presented credential |
| socket (UDS) | the presented credential |
| in-node (the Elixir SDK inside the controller) | running in the controller's own node |

The connect still names a namespace, because a runner entitled to several has
to say which it wants. A namespace the credential does not entitle is refused,
naming both namespaces, before any frame is processed. `_system` is refused on
every transport.

A runner credential is a token with the `runner` role:

```sh
$ flz token issue runner@trading trading runner --ttl never
```

```output
token issued for runner@trading (runner@trading)
id:    c5c749c28131
token (shown once, store it now): flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra
```

Give it to the runner as `FLEZHA_NAMESPACE_SECRET`. A credential can be issued
before its namespace exists. Three rules hold for it:

- **It cannot expire.** It is issued with `--ttl never`; any other lifetime,
  the default included, is refused (`authz.runner_token_expiry`). A runner is
  unattended, and a credential that lapsed would stop it at its next restart.
- **It cannot be for `_system`**, refused at issue and at connect.
- **Revoking it closes the connections it authenticated.**

A runner without a credential, or with one for another namespace, is refused.
The runner only reports that it keeps reconnecting; the reason is in the
controller's log: `Rejected runner connect (:namespace_not_proven)`.

A namespace may hold as many live credentials as you issue, so rotating one
disconnects nothing:

```sh
$ flz token issue runner@team-orders team-orders runner --ttl never   # 1. the new one
$ flz token list                                                      # 3. find the old id
$ flz token revoke <old-id>                                           # 4. retire it
```

Between steps 1 and 3, roll the runners onto the new credential one at a time.
Step 4 closes any connection still on the old credential, which is why the roll
comes first. A QUIC deployment whose certificates carry `/ns/` entries needs no
credential; its expiry is the PKI's.

## Roles

Roles are a fixed set. A principal holds a set of roles in one namespace, and
an action is allowed when any of its roles grants it. Only `admin@_system`
reaches across namespaces.

| Role | What it may do |
| --- | --- |
| `viewer` | read: operators, plugins, cell state, audit, observability, the schedule list |
| `app` | `viewer`, plus what an application does to its own work: start, stop and send to an operator (including what a tasklet's spawn asks for), listen to what it takes in and sends out, enable and disable a plugin. It destroys no record and moves no work it did not create: no dead-letter drop or replay, no state rotation or deletion, no rewind, replay or rescale |
| `ops` | `app`, plus the rest of the operator and plugin lifecycle, audit rotation, observability changes, schedules, the dead-letter queue and records in flight |
| `admin` | `ops`, plus authorization state, namespaces and quotas, stopping the controller and controller-wide settings |
| `runner` | connect a runner, and nothing else. It is what a runner's connection is until a role is bound to it |
| `spawner` | `runner`, plus starting and sending to operators in its own namespace: what a tasklet's spawn and an operator's `call` need |
| `stopper` | `runner`, plus stopping operators in its own namespace |

`spawner` and `stopper` combine: a namespace bound to both starts, sends to
and stops its own work, and still enables no plugin. `app` holds everything
either grants.

```sh
$ flz role bind alice ops team-orders
$ flz role bind runner@team-orders spawner team-orders
$ flz role bind runner@team-orders stopper team-orders
$ flz role list
```

```output
alice  ops@team-orders
runner@team-orders  spawner,stopper@team-orders
```

```sh
$ flz role unbind runner@team-orders stopper   # removes one role
$ flz role unbind alice                        # removes every role
```

- A bind naming a namespace other than the one the principal already holds
  roles in is refused, naming that namespace. Moving an identity is an unbind,
  judged in the namespace it leaves, then a bind in the new one.
- Unbinding the last role removes the binding; unbinding a role the principal
  does not hold changes nothing.
- A token carries its own role and namespace, so it keeps authorizing after
  its principal's binding is removed: revoke the token as well.
- A runner connection holds only those bound roles that grant the connect
  (`runner`, `spawner`, `stopper`, `app`). A runner principal bound to
  `viewer`, `ops` or `admin` connects as `runner` and is refused each
  administration action, whatever `flz role list` shows. For the same reason
  `flz token issue` refuses a token for `app`, `spawner` or `stopper`: a runner
  proves its namespace with a `runner` credential, and the other roles are
  bound to it.
- `authz.*` actions are admin-only in the bound namespace; only
  `admin@_system` may act on another namespace.

What a runner's own code may do with these roles is
[`namespaces.md`](namespaces.md#administering-from-application-code).

### Which role each verb needs

The lowest role each action is granted to (`admin` holds `ops`, which holds
`app`, which holds `viewer`; `runner`, `spawner` and `stopper` are runner
roles outside that line), and the namespace it is judged in:

| Verbs | Action | Lowest role | Judged in |
| --- | --- | --- | --- |
| `flz boot status`, `flz boot await` | public | none | none |
| `flz audit list`, `read`, `tail`, `stats` | `audit.read` | viewer | the caller's namespace |
| `flz audit rotate` | `audit.rotate` | ops | the caller's namespace |
| `flz auth whoami` | `authz.whoami` | viewer | the caller's namespace |
| `flz token issue` | `authz.token_issue` | admin | the namespace named |
| `flz token revoke` | `authz.token_revoke` | admin | the token's namespace |
| `flz token list`, `flz role list` | `authz.token_list`, `authz.role_list` | viewer | the caller's namespace |
| `flz role bind` | `authz.role_bind` | admin | the namespace named |
| `flz role unbind` | `authz.role_unbind` | admin | the binding's namespace |
| `flz namespace create`, `delete`, `set-quota` | `authz.namespace_*` | admin | the namespace named |
| `flz namespace show` | `authz.namespace_show` | viewer | the namespace named |
| `flz controller status`, `mem-top` | `controller.status` | viewer | the caller's namespace |
| `flz controller stop` | `controller.stop` | admin | `_system` |
| `flz cluster status`, `flz operator list`, `info`, `flz runner list` | `operator.list` | viewer | the namespace named |
| `flz operator start`, `flz schedule run` | `operator.start` | spawner | the namespace named (a schedule's own) |
| `flz operator send`, `flz operator call` | `operator.send` | spawner | the namespace named |
| `flz operator stop` | `operator.stop` | stopper | the namespace named |
| `flz operator listen` | `operator.listen` | app | the namespace named |
| `flz operator scale` | `operator.scale` | ops | the namespace named |
| `flz operator rewind` | `operator.rewind` | ops | the namespace named |
| `flz operator replay` | `operator.replay` | ops | the operator's namespace |
| `flz operator state`, `flz state get`, `list`, `at` | `state.get`, `state.list`, `state.at` | viewer | the namespace named, or the cell's |
| `flz state rotate` | `state.rotate` | ops | the cell's namespace |
| `flz state delete` | `state.delete` | admin | the cell's namespace |
| `flz state retention` | `state.retention` | admin | `_system` |
| `flz state outbox`, `inbox`, `journal` | `state.records` | ops | the cell's namespace |
| `flz plugin list`, `info`, installed and enabled lists | `plugin.list`, `plugin.info` | viewer | the caller's namespace, or the namespace named |
| `flz plugin enable`, `bind`, `update` | `plugin.enable` | app | the plugin's namespace |
| `flz plugin disable`, `unbind` | `plugin.disable` | app | the plugin's namespace |
| `flz observe sources`, `sinks` | `observability.query` | viewer | the caller's namespace |
| `flz observe tail` | `observability.subscribe` | viewer | the caller's namespace |
| `flz observe enable`, `disable` | `observability.enable_source`, `disable_source` | ops | the caller's namespace |
| `flz dlq list`, `show` | `dlq.list`, `dlq.show` | ops | the entry's namespace |
| `flz dlq drop`, `replay` | `dlq.drop`, `dlq.replay` | ops | the entry's namespace |
| `flz schedule list` | `schedule.list` | viewer | `_system` |
| `flz schedule add`, `remove` | `schedule.add`, `schedule.remove` | ops | `_system` |

Where a verb is judged in a stored item's namespace (a dead-letter entry's, a
schedule's, a cell's), that item is read only for a caller who is bound and
whose role grants the action, so an unbound or under-privileged caller learns
nothing about whether the name exists. Some choices worth stating plainly:

- **A parked payload is business data.** `flz dlq list` and `show` need `ops`,
  not `viewer`, because an entry holds the failed record verbatim, with its
  cell key and error. The same rule puts `flz state inbox`, `journal` and
  `outbox`, which read records in flight, under `ops`.
- **Managing schedules is a `_system` privilege.** The schedule store is one
  table for the whole controller; a schedule's `--namespace` says where its
  firings land. A credential for `flz schedule add`, `list` and `remove` is
  one bound to `_system` (`flz token issue ci _system ops`), or the admin.
- **`flz schedule run` is judged where the instance starts**, as
  `operator.start` in the schedule's namespace. An `ops@_system` token can
  add, list and remove a schedule but not run one; a token with
  `operator.start` in the schedule's namespace, or the admin, can.
- **A spawn's delivery is judged, not recorded.** A tasklet's spawn that feeds
  a running instance is judged as `operator.send`, and a permitted one writes
  no audit record, since it happens per event. A refused one is audited. The
  start a spawn makes is recorded like any start.
- **A call is a send.** `flz operator call` is judged and audited as
  `operator.send`. A plugin's call on the operator it is enabled on is that
  operator's own input and is not judged again.
- **An operator's `call` is judged as its runner identity**, which needs
  `spawner` or `app`; a refusal ends the call with a response carrying its
  `authz.*` code ([`../advanced/operator_calls.md`](../advanced/operator_calls.md)).
- **`flz auth whoami` is a gated read**: while enforced, a call without a token
  is refused, naming the missing credential.

## A lost admin

Recovery needs file system access to the auth directory, and nothing else.
That access is enough to stop the controller anyway, so recovering against a
running controller grants nothing new, and costs no downtime.

With the controller **running**:

```sh
$ flz auth rebootstrap
```

```output
Bootstrap token written to ./app_data/auth/bootstrap.token.
Claim it against the RUNNING controller:
    flz auth bootstrap 260cd28f8acf6b95e5ac9bec6e7ed73b69665780724253baafcd2ccc14422f11
The claim unbinds every admin@_system and revokes its _system admin tokens before binding the new admin, and it is audited as a recovery.
```

That one-time token is not an admin token, and no other command accepts it.
Claiming it answers the new admin token (`flzt_...`):

```sh
$ flz auth bootstrap 260cd28f8acf6b95e5ac9bec6e7ed73b69665780724253baafcd2ccc14422f11
```

```output
bootstrap claimed; admin@_system bound to admin-4e858b9851ea3e58
admin token (shown once, store it now): flzt_qd4ralq5vlwyvkd2tibw4545jtwc5bu3zr636zkurntnsug5indq
```

With the controller **stopped**, `flz auth rebootstrap` writes a
`REBOOTSTRAP` marker instead; the next boot unbinds each `admin@_system`,
revokes the `_system` admin tokens, resets the claimed flag and writes a fresh
`bootstrap.token` to claim.

Either way the claim succeeds once, and a claim with no file present is
refused, naming the path it expected. Old admin tokens stop working; operators,
runner connections and runner credentials are untouched. The recovery is
recorded as `:rebootstrap_recovery`. `flz auth rebootstrap` finds the directory
from `FLEZHA_AUTHZ_DIR`, else `<app_data>/auth`, and runs behind the same
running-controller check as `flz reset`.

**A damaged store.** A store file that cannot be opened is renamed
`<name>.corrupt-<unix_ts>`, with one `error` log line, and a fresh one is
created:

| File | Consequence | Recovery |
| --- | --- | --- |
| `tokens.dets` | each token is invalid, the admin's and each runner credential | `flz auth rebootstrap`, then issue both again |
| `roles.dets` | each binding is gone; the next boot writes a fresh `bootstrap.token` (tokens still carry their role and keep working) | claim it, then `flz role bind` the rest |
| `namespaces.dets` | namespaces other than `_system` are forgotten; their state directories stay intact | `flz namespace create` again |
| `quota_counters.dets` | counters restart; disk use is rebuilt from disk at boot and runners from the live connections | nothing |
| `state.dets` | the claimed flag resets to bootstrap pending; a plain claim is refused because the admin binding still exists | `flz auth rebootstrap` |

## The node cookie

Controllers, and `flz` reaching a controller, talk over Erlang distribution,
which a cookie gates. The release ships none and does not fall back to a
constant. A node started without `RELEASE_COOKIE` mints one: 32 random bytes,
written `0600` to `${FLEZHA_STATE_DIR}/cookie` (`FLEZHA_COOKIE_FILE` moves it),
read back on later boots and announced on each. That is enough for one node.
Only a command that starts a node writes the file; each `flz` verb only reads
it, so a verb run before the first start reports the controller unreachable.

**A cluster needs a cookie you provide.** Two nodes connect only under one
cookie given to both, so a minted cookie leaves a node clustering with nobody,
which is what a silent failure to form a cluster looks like. Generate one with
`openssl rand -base64 48`, keep it in your secret manager, and in Kubernetes
inject it from a Secret through `envFrom`. An explicit `RELEASE_COOKIE` wins:
nothing is minted and no file is read.

Where the release can neither read nor mint a cookie (no `/dev/urandom`, an
unwritable state directory, a read-only file system), it logs why and starts
without distribution: it does not cluster, and `flz` cannot reach it. A
rebuilt release under the default path can lose the file, and the next boot
mints a new one; nothing breaks, since the CLI reads the same file, but a
deployment that sets the cookie cannot meet this.

For controllers that cross a trust boundary, point `FLEZHA_DIST_TLS_OPTFILE`
at an `inet_tls_dist` option file (server and client certificate and CA; see
OTP's `ssl_dist` documentation): the release then starts with
`-proto_dist inet_tls`. The cookie is still required.

## Transport security

**The socket** needs no certificates. It is created `0600` inside a `0700`
directory, so only the controller's own user can reach it: getting into that
directory is the authentication. A runner run by another user gets
`access denied`, and its diagnosis names the directory's owner and mode next
to the caller's. The boundary can be widened to exactly one named group with
`FLEZHA_SOCKET_DIR_GROUP=<group>` ([`connecting_a_runner.md`](connecting_a_runner.md#where-the-controller-runs)),
a deliberate loosening that is logged on every boot; the alternative, running
the runner as the controller's user, is a broader grant.

**QUIC** ([experimental](../../publishing/maturity.md#quic)) requires a client
certificate that chains to the trust anchor at `FLEZHA_QUIC_CLIENT_CA`. On
first boot the controller generates a complete mutual-TLS set (CA, server
pair, and a `client.pem` / `client.key` leaf with the `clientAuth` extended key
usage) whenever any member is missing; the CA key is not kept, so a partial set
is generated again whole.

| Side | Variable | Default |
| --- | --- | --- |
| controller | `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` |
| controller | `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.pem`, `.key` |
| controller | `FLEZHA_QUIC_CLIENT_CA` | `app_data/certs/ca.pem` |
| controller, generated | `FLEZHA_CLIENT_CERT_PATH`, `FLEZHA_CLIENT_KEY_PATH` | `app_data/certs/client.pem`, `.key` |
| runner | `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` |
| runner | `FLEZHA_QUIC_CLIENT_CERT`, `FLEZHA_QUIC_CLIENT_KEY` | `app_data/certs/client.pem`, `.key` |

A runner sharing `app_data/` authenticates as is. Remote runners get leaves
minted from your own CA with any standard tool; a leaf must carry the
`clientAuth` usage. A refused peer is logged as
`[Transport.Server] Peer refused at the handshake: ...` and emits
`[:flezha, :transport, :connect_rejected]` with `stage: :handshake`. In
Kubernetes, mount a Secret holding `ca.pem`, `server.pem`, `server.key`,
`client.pem` and `client.key` read-only and point the controller variables at
it; the shipped manifests keep the generated set on a per-node `hostPath` and
use the socket. `FLEZHA_TRANSPORT_AUTH=off` (or `false`, `0`, `no`) turns
client-certificate checks off, with a warning on every boot.

## The audit trail

Two kinds of record reach the audit log. The authorization stores' own events
land on `:platform_authz`, with the verified principal: `:authorization_denied`,
`:token_issued`, `:token_revoked`, `:bootstrap_completed` and
`:rebootstrap_recovery`. Each administration act lands on its service's source
(`namespace_service`, `dlq_service`, `schedule_service`, `operator_service`,
`plugin_service`, `state_service`, `controller_service`, `steps_service`,
`observability_service`, `audit_log`) in one shape: `name` is the verb,
`metadata.action` the action it was judged as, `metadata.outcome` (`ok` or
`error`) for a change and `metadata.error_code` for a failed one, `namespace`
where it was judged, `metadata.caller_roles` the roles it was judged with, and
`caller` who asked and through which surface. A refusal carries
`caller_roles` too, empty when it came before any role was resolved. Each of
these sources is in the audit log's default allow-list. The `flz audit` verbs
take `--token` like the others.

## Settings

Precedence is the environment variable, then `config :flezha, :authn_authz`,
then the default.

| Variable | Default | Meaning |
| --- | --- | --- |
| `FLEZHA_AUTHZ_ENABLED` | on in a release | `1`, `true`, `yes` or `on` enables it; any other value is the explicit opt-out, warned on every boot |
| `FLEZHA_AUTHZ_DIR` | `app_data/auth` | where the stores live |
| `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `<auth_dir>/bootstrap.token` | the one-time token file |
| `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` | 86 400 000 | how far ahead of its expiry a token emits `:token_expiring` |
| `:token_ttl_default_seconds` (application env) | 86 400 | lifetime of the admin token a claim mints |
| `:token_expiry_check_interval_ms` (application env) | 300 000 | how often expiring tokens are looked for |
| `:namespaces` (application env) | none | per-namespace quota overrides |

The stores, in the auth directory (mode `0700`, files `0600`):

```text
app_data/auth/
├── state.dets               # the durable claimed flag
├── roles.dets               # principal -> namespace, roles, when and by whom
├── namespaces.dets          # name -> quotas, when and by whom
├── tokens.dets              # hashed tokens, runner credentials included
├── quota_counters.dets      # per-namespace counters
├── bootstrap.token          # one-time, removed on claim
└── REBOOTSTRAP              # recovery marker, used on the next boot
```

Each change is synced to disk before it is acknowledged. Back up the auth
directory together with `app_data/state/` and `app_data/audit/`.

**Turning it off.** `FLEZHA_AUTHZ_ENABLED=false` restores the open posture,
warned on every boot; it hides enforcement rather than clearing anything, so
the admin, the tokens and the credentials come back when the variable goes.
`FLEZHA_TRANSPORT_AUTH=off` restores unauthenticated QUIC. Both are node-wide,
not per namespace. After rolling back to an older release, an identity holding
several roles is refused each call (`role_insufficient`) until it is bound
again with the older `flz`; one holding a single role behaves as before.

## Limits and gotchas

- **The claim's admin token lasts one day.** Issue a longer one at once.
- **A runner credential must be `--ttl never`.** Anything else is refused.
- **Removing a binding does not revoke a token.** Revoke the token too.
- **An embedded controller is open by default**: a library host must set
  `config :flezha, authn_authz: [enabled: true]`
  ([`deploying_a_controller.md`](deploying_a_controller.md)).
- **A minted cookie is one node's alone.** A cluster sets `RELEASE_COOKIE`.

## See also

- [`namespaces.md`](namespaces.md): the boundary each token lives in, quotas, and administering from code.
- [`deploying_a_controller.md`](deploying_a_controller.md): the variables a deployment sets.
- [`connecting_a_runner.md`](connecting_a_runner.md): the socket and QUIC transports.
- [`cli.md`](cli.md): `flz auth`, `flz token`, `flz role`.
- [`../../errors.md`](../../errors.md): every `authz.*` code.
- [Controller environment variables](../../configuration/environment_variables.md#security): every security variable.
- Tutorial: [`open_the_desk.md`](../../tutorials/tradedesk/open_the_desk.md) claims the admin and gives the runner its key.
