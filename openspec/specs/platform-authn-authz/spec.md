# platform-authn-authz Specification

## Purpose

The platform-authn-authz capability provides the Flezha Controller's authentication, authorization, namespace, and per-namespace quota foundation. It defines a verified-principal model, a single policy point (`Flezha.AuthZ.authorize/3`) consulted by the operator-action service functions it wires, a fixed role set (`viewer`, `operator`, `admin`, `executor`) bound to principals on namespaces, a namespace catalog (`_system` as the singleton compatibility namespace, additional namespaces created by `admin@_system`), per-namespace quotas enforceable at allocation time (with the relay/partition enforcement point shipping in this change), a local-managed token registry with instant revocation, a one-time on-disk bootstrap-token flow that replaces today's open-auth model without bricking upgrades, and mTLS cert principal/namespace extraction primitives (`Flezha.AuthN.Cert`) that downstream changes can wire into the live transport. Federated identity providers, encryption-at-rest, custom roles, per-message authorization inside user code, the web UI, transport-layer cert wiring, the CLI socket handshake, and deeper namespacing of the State subsystem are explicitly out of scope.

## Requirements

### Requirement: Verified-principal model

The system SHALL define a Principal as a verified identity. For mTLS-authenticated callers the Principal SHALL be derived from the X.509 client cert via `Flezha.AuthN.Cert.principal_from_peer/1`: if the cert carries a SPIFFE URI SAN of the form `spiffe://<trust-domain>/...` the SAN SHALL be the Principal; otherwise the cert subject DN normalised to RFC 4514 form SHALL be the Principal. For callers presenting a scoped token, the Principal SHALL be the `principal` field stored in the token registry entry. No code path SHALL invent a Principal from any other source (request body, header, environment variable, peer-credential UID). Wiring this extraction into the live transport handshake is out of scope for this change; the extraction primitives are provided.

#### Scenario: SPIFFE SAN wins over subject DN
- **WHEN** a cert with subject DN `CN=executor-7,O=team-orders` and a SPIFFE SAN `spiffe://flezha.local/ns/team-orders/sa/executor-7` is decoded
- **THEN** `Flezha.AuthN.Cert.principal_from_peer/1` SHALL return `{:ok, "spiffe://flezha.local/ns/team-orders/sa/executor-7"}`

#### Scenario: Subject DN used when no SPIFFE SAN
- **WHEN** a cert with subject DN `CN=executor-7,O=team-orders` and no SPIFFE SAN is decoded
- **THEN** `Flezha.AuthN.Cert.principal_from_peer/1` SHALL return `{:ok, "CN=executor-7,O=team-orders"}`

#### Scenario: Token presentation binds the registry principal
- **WHEN** a caller presents a token whose registry entry has `principal: "alice@example.com"`
- **THEN** `Flezha.AuthZ.TokenStore.verify/1` SHALL return `{:ok, %{principal: "alice@example.com", ...}}`

### Requirement: Scoped token registry and verification

The system SHALL provide a token registry (`Flezha.AuthZ.TokenStore`) persisted in `app_data/auth/tokens.dets`. Each entry SHALL store: `token_hash` (SHA-256 of the bare token), `principal`, `namespace`, `role`, `created_at`, `expires_at`, `revoked_at`, `created_by_principal`, `label`. The wire-form token SHALL be a 256-bit cryptographically random value with base32 encoding and a `flzt_` prefix. The on-disk form SHALL be the SHA-256 hash. Verification SHALL look up the hash, reject entries whose `revoked_at` is non-null, reject entries whose `expires_at` has passed, and otherwise return the bound `principal`/`namespace`/`role`. Issuance and revocation SHALL emit `:token_issued` / `:token_revoked` events on the `:platform_authz` source.

#### Scenario: Token issuance returns the raw value exactly once
- **WHEN** `TokenStore.issue/5` succeeds
- **THEN** the function SHALL return `{:ok, raw_token, entry}` where `raw_token` begins with `"flzt_"` AND the DETS row SHALL store only the SHA-256 hash

#### Scenario: Revoked token is rejected at verification
- **WHEN** a token has been revoked via `TokenStore.revoke/2` and `TokenStore.verify/1` is called with the raw value
- **THEN** verification SHALL return `{:error, :token_revoked}`

#### Scenario: Expired token is rejected at verification
- **WHEN** a token's `expires_at` has passed
- **THEN** `TokenStore.verify/1` SHALL return `{:error, :token_expired}`

#### Scenario: Token file mode is restrictive
- **WHEN** `app_data/auth/tokens.dets` is created or reopened
- **THEN** its mode SHALL be `0600`

### Requirement: Single policy point `Flezha.AuthZ.authorize/3`

The system SHALL expose exactly one authorization decision function `Flezha.AuthZ.authorize(caller_context, action, scope) :: :ok | {:error, {:forbidden, reason}}`. `action` SHALL be an atom from the `Flezha.AuthZ.Action` enumeration. `scope` SHALL be a map containing at minimum `:namespace`. Every operator-action function in `Flezha.Service.Observability` and `Flezha.Service.AuditLog` SHALL call this function before any side effect. Failed authorization SHALL emit an `:authorization_denied` event on `:platform_authz`.

#### Scenario: Denial returns forbidden tuple
- **WHEN** a `viewer` invokes a service function whose action requires `operator` or higher
- **THEN** the service function SHALL return `{:error, {:forbidden, :role_insufficient}}` AND SHALL NOT perform the requested action

#### Scenario: Wrong-namespace denial
- **WHEN** an `operator` on `team-orders` invokes a service function whose `scope.namespace` is `team-payments`
- **THEN** the service function SHALL return `{:error, {:forbidden, :namespace_mismatch}}`

#### Scenario: Disabled mode returns ok unconditionally
- **WHEN** `:authn_authz, :enabled` is `false` and any caller invokes any service function
- **THEN** `authorize/3` SHALL return `:ok` unconditionally AND no quota check SHALL execute

### Requirement: Fixed role set

The system SHALL define exactly four roles: `viewer`, `operator`, `admin`, `executor`. The role→action mapping SHALL be a compile-time map in `Flezha.AuthZ.Policy`. `viewer` SHALL grant read-only actions on the bound namespace. `operator` SHALL grant viewer plus relay/plugin lifecycle, observability mutations, and audit rotation. `admin` SHALL grant operator plus namespace/role/quota mutation; only `admin@_system` SHALL be allowed to invoke auth-state mutations on namespaces other than its own. `executor` SHALL grant only `:executor.connect`, `:executor.checkpoint`, `:executor.report` and SHALL NOT grant any operator action.

#### Scenario: Admin on team-orders cannot mutate team-payments
- **WHEN** `admin@team-orders` invokes a service call whose `scope.namespace` is `team-payments` and whose action is `:authz.token_issue`
- **THEN** the call SHALL return `{:error, {:forbidden, :cross_namespace_admin_denied}}`

#### Scenario: Executor role cannot read audit
- **WHEN** an SDK Executor with role `executor` invokes any function in `Flezha.Service.AuditLog`
- **THEN** the call SHALL return `{:error, {:forbidden, :role_insufficient}}`

### Requirement: Namespace catalog

The system SHALL define a Namespace as a hard partition for authorization scope. Every namespace SHALL exist in the durable catalog (`Flezha.AuthZ.NamespaceStore`) before it can be bound to a token or a role. `_system` SHALL exist at boot, SHALL own resources created before this capability lands, and SHALL be the only namespace from which auth-state mutations on other namespaces are permitted (subject to the `admin` role check). Additional namespaces SHALL be created by `admin@_system` via `Flezha.AuthZ.NamespaceStore.create/2`. Deeper namespacing of the on-disk state subsystem itself (per-namespace WAL paths in `Flezha.State.*`) is out of scope for this change.

#### Scenario: System namespace exists at boot
- **WHEN** the Controller boots and `app_data/auth/namespaces.dets` is empty or missing
- **THEN** the namespace catalog SHALL contain exactly one entry named `_system`

#### Scenario: Cross-namespace audit read is filtered
- **WHEN** a caller bound to namespace `team-orders` invokes `Flezha.Service.AuditLog.read/2`
- **THEN** the returned records SHALL contain only records whose `namespace` is `team-orders` or that have no namespace field, AND SHALL NOT include other namespaces' records (unless the caller is `admin@_system`)

### Requirement: Per-namespace quotas at allocation

The system SHALL store per-namespace quotas in `app_data/auth/namespaces.dets`. Quota fields SHALL include `max_relays`, `max_partitions_per_relay`, `max_mailbox_bytes_total`, `max_message_rate_per_second`, `max_disk_bytes_total`. `Flezha.AuthZ.Quotas.check_allocation/3` SHALL be a hot-path check (one ETS lookup against `NamespaceStore` plus one against `QuotaCounters`). `Flezha.Service.RelayManager.start_relay/2` SHALL call `check_allocation/3` for `max_relays` and `max_partitions_per_relay`; `stop_relay/2` SHALL record the dealloc. Adoption at the remaining allocation sites (`State.Supervisor`, `FleetManager.connect_handler/4`, bounded-mailbox provisioning) is left for the modules' owners — one-line integration.

#### Scenario: Max-relays denial on allocation
- **WHEN** namespace `team-orders` has `max_relays: 5`, 5 relays already exist, and an operator invokes `Flezha.Service.RelayManager.start_relay/2` with a 6th
- **THEN** the call SHALL return `{:error, {:quota_exceeded, :max_relays, %{current: 5, limit: 5}}}` AND the 6th Relay SHALL NOT be started

#### Scenario: Quota check latency budget
- **WHEN** `check_allocation/3` runs against a warm ETS cache
- **THEN** the per-call cost SHALL stay well within the documented hot-path budget

### Requirement: Bootstrap flow

On Controller start, the system SHALL detect whether `app_data/auth/roles.dets` contains at least one `admin@_system` binding. If none exists AND `:enabled` is `true` AND `FLEZHA_AUTHZ_ENABLED` is not explicitly set to a false value, the system SHALL: ensure `app_data/auth/` exists with mode `0700`; verify the bootstrap-token-path parent directory mode (abort with `error` on group- or other-writable bits); write a 256-bit random token to `app_data/auth/bootstrap.token` with mode `0600`; emit a single `warn` log line naming the path. `Flezha.AuthZ.Bootstrap.claim/2` SHALL accept that exact token exactly once; on success it SHALL bind the resolved principal (cert SAN > cert DN > `--principal` > freshly-minted admin token) to `admin@_system`, durably flip `:authn_authz, :enabled` to `true`, unlink `bootstrap.token`, and emit a `:bootstrap_completed` event on `:platform_authz`.

#### Scenario: First boot writes bootstrap token
- **WHEN** `Flezha.AuthZ.Bootstrap.check_and_write/0` runs and no `admin@_system` binding exists
- **THEN** `bootstrap.token` SHALL exist with mode `0600`

#### Scenario: Bootstrap claim without cert mints a token
- **WHEN** `Bootstrap.claim/2` is invoked without `:cert_principal` and without `:principal`
- **THEN** the function SHALL return `{:ok, %{principal: "admin-" <> _, minted_token: token}}` where `token` begins with `"flzt_"`

#### Scenario: Bootstrap claim refused after first success
- **WHEN** `Bootstrap.claim/2` has already succeeded and a second claim is attempted
- **THEN** the call SHALL return `{:error, :bootstrap_already_completed}`

#### Scenario: Permissive parent directory aborts bootstrap
- **WHEN** `Bootstrap.check_and_write/0` runs and the parent directory of `bootstrap.token` has group- or other-write bits set
- **THEN** the function SHALL return `{:error, {:permissive_auth_parent, parent}}` AND `bootstrap.token` SHALL NOT be created

### Requirement: Compatibility mode default-off-then-flipped-on

The system SHALL ship the `:authn_authz, :enabled` runtime config flag with default `false`. While `false`, `Flezha.AuthZ.authorize/3` SHALL return `:ok` unconditionally, the audit log SHALL record `caller_context.source` in the principal column, and no quota check SHALL execute. The flag SHALL flip to `true` durably (written to `app_data/auth/state.dets`) on successful bootstrap claim; once `true`, the flag SHALL remain `true` across restarts. The env-var override `FLEZHA_AUTHZ_ENABLED=false` SHALL be honoured and SHALL emit a `warn` on every boot.

#### Scenario: Default-off preserves existing behaviour
- **WHEN** a Controller boots for the first time with this release and no `admin@_system` binding exists
- **THEN** every `Flezha.Service.*` call SHALL succeed without authorization rejection AND no `caller_context.principal` SHALL be required

#### Scenario: Successful bootstrap flips flag durably
- **WHEN** `Bootstrap.claim/2` succeeds
- **THEN** `app_data/auth/state.dets` SHALL contain `enabled: true` AND a subsequent Controller restart SHALL load the flag as `true` even without re-running bootstrap

### Requirement: State-path namespace prefix and migration

The system SHALL move any pre-namespace top-level subtree under `app_data/state/` into `app_data/state/_system/` via a one-shot migration pass added to `Flezha.Boot.StatePathMigrator`. The migration SHALL be atomic per top-level tree (single `rename(2)` per relay-class), safe to interrupt, and idempotent. Deeper plumbing of the namespace through `Flezha.State.PersistenceResolver` / `Wire` / `WalWriter` / `Snapshot` / `Restore` (so writes go to `app_data/state/<namespace>/...` rather than the migrated `_system` prefix) is out of scope for this change.

#### Scenario: Migration moves legacy state under _system
- **WHEN** the Controller boots with `app_data/state/orders/` existing and no `_system/` subdirectory
- **THEN** after the migrator runs, `app_data/state/_system/orders/` SHALL exist AND `app_data/state/orders/` SHALL NOT exist

#### Scenario: Migration is idempotent
- **WHEN** the migrator runs twice in succession on an already-migrated tree
- **THEN** the second run SHALL leave the layout unchanged

### Requirement: Auth-state mutations emit auditable signals

Token issuance, token revocation, namespace creation, namespace deletion, role bind, role unbind, namespace quota updates, and bootstrap completion SHALL each emit exactly one `:event`-kind signal on the `:platform_authz` source carrying the verified `caller_context.principal` (when bound), the target principal/namespace where applicable, the action name, and any relevant metadata. The audit-log subsystem SHALL include `:platform_authz` in its default subscription allow-list so these signals are written durably.

#### Scenario: Token issuance emits and is audited
- **WHEN** `TokenStore.issue/5` succeeds
- **THEN** a `:event` signal `name: :token_issued` SHALL appear on `:platform_authz` carrying the new entry's principal, namespace, role, and a hash-prefix identifier

#### Scenario: Bootstrap completion emits and is audited
- **WHEN** `Bootstrap.claim/2` succeeds
- **THEN** a `:event` signal `name: :bootstrap_completed` SHALL appear on `:platform_authz` carrying the bound admin principal

#### Scenario: Failed authorization itself is audited
- **WHEN** any `AuthZ.authorize/3` call returns `{:error, {:forbidden, _}}`
- **THEN** a `:event` signal `name: :authorization_denied` SHALL appear on `:platform_authz` carrying the action, scope, denial reason, and (where bound) the denied caller's principal

### Requirement: Elixir-side CLI wrappers

The system SHALL expose `Flezha.Service.CliManager.authz_*`, `namespace_*`, and `role_*` wrapper functions covering the documented verb set (`bootstrap`, `issue`, `revoke`, `list`, `whoami`; `create`, `delete`, `list`, `show`, `set_quota`; `bind`, `unbind`, `list`). Each wrapper SHALL build a `caller_context` with `source: :cli`, route through `Flezha.AuthZ.authorize/3` (where the verb maps to a documented action), call the corresponding `Flezha.AuthZ.*` primitive, and emit a `:platform_authz` audit event on mutating verbs. These wrappers form the contract a future Rust CLI consumes; building the Rust CLI binaries themselves is out of scope.

#### Scenario: namespace_create routes through AuthZ.authorize
- **WHEN** `CliManager.namespace_create/3` is invoked
- **THEN** `Flezha.AuthZ.authorize/3` SHALL be called with action `:authz.namespace_create` AND, on success, a `:namespace_created` event SHALL appear on `:platform_authz`

#### Scenario: role_unbind looks up the bound namespace before authorizing
- **WHEN** `CliManager.role_unbind/2` is invoked for a principal currently bound to `team-orders`
- **THEN** `Flezha.AuthZ.authorize/3` SHALL be called with `scope.namespace = "team-orders"` (not the caller's namespace)

### Requirement: Durable token-store and role-store crash recovery

`app_data/auth/tokens.dets`, `app_data/auth/roles.dets`, `app_data/auth/namespaces.dets`, `app_data/auth/quota_counters.dets`, and `app_data/auth/state.dets` SHALL be opened with `:sync` writes so every mutation is durably written before the mutating call returns. On Controller boot, if any of these files is detected as corrupt, the system SHALL rename the corrupt file to `<name>.corrupt-<unix_ts>` and open a fresh empty file. While `:enabled` is `true`, the Controller SHALL refuse to leave `Flezha.AuthZ.Supervisor` healthy unless either (a) the recovered state contains at least one `admin@_system` role binding, or (b) `bootstrap.token` is present, or (c) `FLEZHA_AUTHZ_ENABLED=false` is set in the environment.

#### Scenario: Corrupt DETS is rotated
- **WHEN** the Controller boots and one of the auth DETS files cannot be opened
- **THEN** the file SHALL be renamed to `<name>.corrupt-<unix_ts>` AND a fresh empty file SHALL be opened in its place

### Requirement: Runtime configuration surface

The system SHALL expose runtime configuration under the `:authn_authz` key with at minimum: `:enabled` (boolean, default `false`), `:auth_dir` (default `app_data/auth`), `:bootstrap_token_path` (default `<auth_dir>/bootstrap.token`), `:token_ttl_default_seconds` (default `86_400`), `:sdk_token_ttl_seconds` (default `2_592_000`), `:namespaces` (per-namespace quota overrides), `:application_managed` (boolean, default `true`; tests flip to `false`). Env-var overrides `FLEZHA_AUTHZ_ENABLED`, `FLEZHA_AUTHZ_DIR`, `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` SHALL be honoured with `env > app config > default` precedence.

#### Scenario: Env var overrides config
- **WHEN** the Controller boots with `FLEZHA_AUTHZ_DIR=/srv/flezha/auth` set
- **THEN** every auth-state DETS file SHALL be created under `/srv/flezha/auth/` regardless of any application-config setting

#### Scenario: Default-namespace quotas applied when not overridden
- **WHEN** namespace `team-orders` is created without quota overrides
- **THEN** the namespace catalog entry SHALL store the documented default quotas (`max_relays: 100`, `max_partitions_per_relay: 1_024`, `max_mailbox_bytes_total: 268_435_456`, `max_message_rate_per_second: 100_000`, `max_disk_bytes_total: 107_374_182_400`)
