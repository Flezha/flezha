# Platform authn/authz foundation (`platform-authn-authz`)

The authn/authz foundation gives every operator-facing service surface
a verified principal, a fixed role enumeration, a namespace-scoped
policy point, and per-namespace resource quotas. Since
`finish-platform-authn-authz`, enforcement is **on by default in
production** — running open requires the explicit
`FLEZHA_AUTHZ_ENABLED=false` opt-out, warned on every boot.

This guide walks through the operator-facing flow: the enforcement
posture, claiming the bootstrap admin, issuing and revoking
scoped tokens, creating and inspecting namespaces, binding roles,
and recovering from a corrupt auth store. Provisioning (cookie, TLS,
certs, k8s manifests) and break-glass recovery live in
[`operations/authn_authz.md`](operations/authn_authz.md). The full
module map and internal contracts live in `CLAUDE.md` under
"Platform authn/authz foundation"; audit-log emission semantics
live in [`audit_log.md`](audit_log.md).

## 1. Enforcement posture

`Flezha.AuthZ.enforcement_posture/0` answers one of three states:

  * **`:open`** — the explicit configuration opt-out
    (`FLEZHA_AUTHZ_ENABLED=false`, or the dev/test config default).
    `authorize/3` returns `:ok` unconditionally and a `warn` line is
    emitted on every boot for as long as it remains set.
  * **`:bootstrap_pending`** — enforcement is configured on but no
    `admin@_system` has been claimed yet. Gated operator verbs are
    REFUSED naming `flz auth bootstrap`, while dataflow — running
    operatives, internal machinery, and runner connections over the
    authenticated transport — continues. An un-bootstrapped upgrade
    degrades to "operator verbs refused", never to an outage and never
    to open.
  * **`:enforced`** — the bootstrap was claimed (the durable flag in
    `<auth_dir>/state.dets` flipped); every call is policy-checked.

Identity reaches the policy point ONLY from the authenticated
transport: a verified token (`flz --token` / `FLEZHA_TOKEN`) for CLI
callers, and the connection's namespace handshake for runners
(`runner@<namespace>`, `runner` role, bound by construction). A
caller-supplied principal is never trusted.

```elixir
# config/runtime.exs — the prod default
config :flezha, :authn_authz,
  enabled: true,                                  # config_env() == :prod
  auth_dir: "app_data/auth",
  bootstrap_token_path: "app_data/auth/bootstrap.token",
  token_ttl_default_seconds: 86_400,              # CLI tokens, 1 day
  sdk_token_ttl_seconds: 2_592_000,               # SDK tokens, 30 days
  namespaces: %{
    "tenant-a" => %{max_operatives: 50, max_entities_per_operative: 256}
  }
```

## 2. Bootstrap: claim `admin@_system`

On first boot with `:enabled: true` and no `admin@_system` binding,
`Flezha.AuthZ.Bootstrap` writes a one-time token to
`<auth_dir>/bootstrap.token` (mode `0600`) and logs the path. The
operator claims it once:

```sh
# read the token off the Controller host
$ sudo cat /var/lib/flezha/auth/bootstrap.token
flzt_a1b2c3d4...

# claim it from an authenticated session
$ flz auth bootstrap flzt_a1b2c3d4... --principal alice@example.com
```

Principal resolution order is **cert SAN > cert DN > `--principal`
> minted admin token**. When the Controller's transport saw a
client cert during the call, the SPIFFE URI SAN (or RFC 4514 DN
fallback) becomes the durable identity. With no cert and no
`--principal`, the bootstrap mints a long-lived admin token and
prints it once — store it like any other secret.

Successful claim:

  1. binds `admin@_system` to the resolved principal,
  2. flips `AuthState.enabled` durably (`:enabled` is now `true`
     across restarts regardless of config),
  3. unlinks `bootstrap.token`,
  4. emits `:bootstrap_completed` on the `:platform_authz` source.

A second claim attempt is rejected with
`{:error, :bootstrap_already_completed}`.

**Permissive parent dir**: `Bootstrap.check_and_write/0` refuses
to write the token when `<auth_dir>`'s parent has group- or
other-write bits set (`mode & 0o022 != 0`). Tighten the parent
(`chmod 0700`) and restart — never `chmod` `app_data/auth/` itself
to bypass the check.

## 3. Issue and revoke tokens

Tokens are scoped to a `(principal, namespace, role)` triple with
an explicit TTL. The wire form is `flzt_<base32>`; the on-disk row
stores only `SHA-256(token)`, so a stolen DETS file is not a
credential.

```sh
# issue a 7-day operator token for tenant-a, owned by bob
$ flz auth issue \
    --principal bob@example.com \
    --namespace tenant-a \
    --role operator \
    --ttl 604800 \
    --label "bob laptop, jira-1234"
flzt_xyz123... (only shown once)

# list tokens visible to the caller (admin@_system sees all)
$ flz auth list

# revoke by hash prefix (>= 8 chars). Effective immediately —
# no propagation lag, the next verify/2 call rejects.
$ flz auth revoke 4f2a9b1c
```

Every issue/revoke emits `:token_issued` / `:token_revoked` on the
`:platform_authz` source; the audit log captures them when the
audit subsystem is enabled.

## 4. Namespaces and quotas

Namespaces are the hard tenancy boundary. Every Operative, persistent
state file, audit record, and observability signal carries one.
Name format: `[a-z][a-z0-9-]{0,62}`. `_system` is reserved.

```sh
# create with default quotas
$ flz ns create tenant-a

# create with per-namespace caps
$ flz ns create tenant-b \
    --quota max_operatives=20 \
    --quota max_entities_per_operative=128

$ flz ns list
$ flz ns show tenant-a

# raise (or lower) quotas in place
$ flz ns set-quota tenant-a max_operatives=200

# delete an empty namespace (counters dropped)
$ flz ns delete tenant-a
```

Enforced quota kinds and their defaults:

| Key                            | Default        | Enforced at                          |
| ------------------------------ | -------------- | ------------------------------------ |
| `max_operatives`               | `100`          | `OperativeManager.start_operative/2` |
| `max_entities_per_operative`   | `1_024`        | `OperativeManager.start_operative/2` |
| `max_mailbox_bytes_total`      | `256 MiB`      | caps the per-Operative credit window (`PersistenceResolver.credit_window_bytes/2`) |
| `max_message_rate_per_second`  | `100_000`      | deferred (data-plane rate limiting is a performance feature) |
| `max_state_bytes`              | `100 GiB`      | `start_operative` refuses NEW allocation while over; the `State.Watchdog` raises a latched `[:flezha, :state, :disk_quota_breach]` alarm for a running namespace — durable writes are never interrupted |
| `max_runners`                  | `32`           | `FleetManager.connect_handler/4` (refusal observable via `[:flezha, :runner, :quota_refused]`) |

A quota rejection surfaces as
`{:error, {:quota_exceeded, kind, %{current, limit}}}` to the
caller. Disk-footprint is recomputed from the state tree
(`app_data/state/<ns>/`) on a 30 s TTL, and rebuilt from the on-disk
truth on every boot, so accounting drift is bounded by one uptime.
The `:runners` counter is derived from the live session map on every
session mutation rather than incremented at each removal seam.

## 5. Bind roles

Roles are a fixed four-way enum: `viewer`, `operator`, `admin`,
`runner`. The role→action map lives in
`Flezha.AuthZ.Policy`. A principal may hold at most one binding;
cross-namespace presence requires one principal identity per
namespace.

```sh
# operator on tenant-a
$ flz role bind bob@example.com tenant-a operator

# inspect
$ flz role list

# remove
$ flz role unbind bob@example.com
```

`admin@_system` is the only binding that may target a
namespace other than its own — every other admin is scoped to
its bound namespace.

## 6. Audit-log integration

When `:audit_log, :enabled: true`, `:platform_authz` is in the
default subscription allow-list, so every auth-relevant event
lands on disk as one JSONL record per event:

```json
{
  "ts_us": -567819332,
  "ts_iso": "2026-05-19T08:11:02.418Z",
  "source": "platform_authz",
  "name": "token_issued",
  "namespace": "tenant-a",
  "measurements": {"count": 1},
  "metadata": {
    "principal": "bob@example.com",
    "namespace": "tenant-a",
    "role": "operator",
    "created_by_principal": "alice@example.com",
    "token_hash_prefix": "4f2a9b1c2e7d",
    "expires_at": 1748448662,
    "label": "bob laptop, jira-1234"
  },
  "caller": {"source": "cli", "principal": "alice@example.com"}
}
```

`caller.principal` is the verified principal (resolved from the
CLI/SDK handshake) once `:enabled` is `true`. Names emitted by
this subsystem: `:authorization_denied`, `:token_issued`,
`:token_revoked`, `:namespace_created`, `:namespace_deleted`,
`:namespace_quota_updated`, `:role_bound`, `:role_unbound`,
`:bootstrap_completed`.

## 7. Runtime knobs

```elixir
config :flezha, :authn_authz,
  enabled: false,
  auth_dir: "app_data/auth",
  bootstrap_token_path: "app_data/auth/bootstrap.token",
  token_ttl_default_seconds: 86_400,
  sdk_token_ttl_seconds: 2_592_000,
  namespaces: %{},
  application_managed: true            # tests flip to false
```

Env-var overrides (`env > app config > default`):

  * `FLEZHA_AUTHZ_ENABLED` — `true|1|yes|on` flips the master
    flag; `false|0|no|off` explicit-disables and logs a `warn`
    on every boot.
  * `FLEZHA_AUTHZ_DIR` — redirects the on-disk store path.
  * `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` — overrides the
    bootstrap token file location.

## 8. On-disk layout

```
app_data/auth/                 # mode 0700, owned by the running user
├── state.dets                 # durable :enabled flag
├── roles.dets                 # principal → {namespace, role, bound_at, bound_by}
├── namespaces.dets            # name → {quotas, created_at, created_by}
├── tokens.dets                # SHA-256-hashed token registry
├── quota_counters.dets        # per-namespace counters
└── bootstrap.token            # one-time, mode 0600, unlinked on claim
```

The directory mode is enforced at store-open time; if `auth_dir`
is created by hand make sure it is `0700` before the Controller
starts. Persisted state still lives at `app_data/state/`, now
namespaced as `app_data/state/<namespace>/...` — pre-upgrade
trees are moved into `app_data/state/_system/` by the boot-time
`Flezha.Boot.StatePathMigrator` (one-shot, idempotent).

## 9. Recovery: corrupt auth store

Every store opens its DETS file via `Flezha.AuthZ.Store.open/2`. A
DETS corruption is detected on open; the store rotates the bad
file to `<name>.corrupt-<unix_ts>` and re-opens a fresh table. A
single `error` log line names the rotated file. The practical
consequences depend on which file was lost:

  * `tokens.dets` — every issued token is invalidated; re-issue
    via `flz auth issue`.
  * `roles.dets` — every role binding is lost. The supervisor
    refuses to start (no `admin@_system`) unless either
    `bootstrap.token` is restored or `FLEZHA_AUTHZ_ENABLED=false`
    is set as a temporary kill-switch while bindings are
    re-created.
  * `namespaces.dets` — non-`_system` namespaces are forgotten.
    Re-create with `flz ns create`; existing
    `app_data/state/<namespace>/` trees stay intact, just
    unlisted.
  * `state.dets` — durable `:enabled` flag resets to `false`
    (the upgrade-default). Re-run the bootstrap flow.

Treat `<auth_dir>` as part of the Controller's durable state for
backup purposes. Copy it alongside `app_data/state/` and
`app_data/audit/`.

## Cross-references

  * `CLAUDE.md` → "Platform authn/authz foundation (`platform-authn-authz`)"
    — module map, supervision-tree placement, internal contracts.
  * [`docs/audit_log.md`](audit_log.md) — JSONL record schema,
    `:platform_authz` subscription, principal denormalisation.
  * `docs/glossary.md` — Controller / Runner / Operative / Procedure /
    Entity vocabulary used in role-action names.
