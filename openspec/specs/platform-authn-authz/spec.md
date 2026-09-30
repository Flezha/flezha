# platform-authn-authz Specification

## Purpose

The platform-authn-authz capability provides the Flezha Controller's authentication, authorization, namespace, and per-namespace quota foundation. It defines a verified-principal model, a single policy point (`Flezha.AuthZ.authorize/3`) consulted by the user-action service functions it wires, a fixed role set (`viewer`, `app`, `ops`, `admin`, `runner`, `spawner`, `stopper`), a set of which is bound to a principal on one namespace, a namespace catalog (`_system` as the singleton compatibility namespace, additional namespaces created by `admin@_system`), per-namespace quotas enforceable at allocation time (with the operator/cell enforcement point shipping in this change), a local-managed token registry with instant revocation, a one-time on-disk bootstrap-token flow that replaces today's open-auth model without bricking upgrades, and mTLS cert principal/namespace extraction primitives (`Flezha.AuthN.Cert`) that downstream changes can wire into the live transport. Since `finish-platform-authn-authz`, authorization is enforced by default, principals derive from the authenticated transport (see the `transport-authentication` capability), namespace isolation extends through the State subsystem, and the Rust CLI verb families are shipped. Federated identity providers, encryption-at-rest, custom roles, per-message authorization inside user code, and the web UI remain out of scope.
## Requirements
### Requirement: Verified-principal model

The system SHALL define a Principal as a verified identity. For mTLS-authenticated callers the Principal SHALL be derived from the X.509 client cert via `Flezha.AuthN.Cert.principal_from_peer/1`: if the cert carries a SPIFFE URI SAN of the form `spiffe://<trust-domain>/...` the SAN SHALL be the Principal; otherwise the cert subject DN normalised to RFC 4514 form SHALL be the Principal. For callers presenting a scoped token, the Principal SHALL be the `principal` field stored in the token registry entry. No code path SHALL invent a Principal from any other source (request body, header, environment variable, peer-credential UID). Wiring this extraction into the live transport handshake is out of scope for this change; the extraction primitives are provided.

#### Scenario: SPIFFE SAN wins over subject DN
- **WHEN** a cert with subject DN `CN=runner-7,O=team-orders` and a SPIFFE SAN `spiffe://flezha.local/ns/team-orders/sa/runner-7` is decoded
- **THEN** `Flezha.AuthN.Cert.principal_from_peer/1` SHALL return `{:ok, "spiffe://flezha.local/ns/team-orders/sa/runner-7"}`

#### Scenario: Subject DN used when no SPIFFE SAN
- **WHEN** a cert with subject DN `CN=runner-7,O=team-orders` and no SPIFFE SAN is decoded
- **THEN** `Flezha.AuthN.Cert.principal_from_peer/1` SHALL return `{:ok, "CN=runner-7,O=team-orders"}`

#### Scenario: Token presentation binds the registry principal
- **WHEN** a caller presents a token whose registry entry has `principal: "alice@example.com"`
- **THEN** `Flezha.AuthZ.TokenStore.verify/1` SHALL return `{:ok, %{principal: "alice@example.com", ...}}`

### Requirement: Scoped token registry and verification

The system SHALL provide a token registry (`Flezha.AuthZ.TokenStore`) persisted in `app_data/auth/tokens.dets`. Each entry SHALL store: `token_hash` (SHA-256 of the bare token), `principal`, `namespace`, `role`, `created_at`, `expires_at`, `revoked_at`, `created_by_principal`, `label`. The wire-form token SHALL be a 256-bit cryptographically random value with base32 encoding and a `flzt_` prefix. The on-disk form SHALL be the SHA-256 hash. Verification SHALL look up the hash, reject entries whose `revoked_at` is non-null, reject entries whose `expires_at` has passed, and otherwise return the bound `principal`/`namespace`/`role` and the entry's hash, so a caller can name the credential that admitted it. An entry MAY be issued with no expiry, and the operations surface SHALL be able to ask for one. A `runner`-role entry is the credential a runner connection proves its namespace with and SHALL be refused any expiry and refused the `_system` namespace. Issuance and revocation SHALL emit `:token_issued` / `:token_revoked` events on the `:platform_authz` source, and an entry approaching its expiry SHALL emit `:token_expiring` on the same source.

#### Scenario: Token issuance returns the raw value exactly once
- **WHEN** `TokenStore.issue/5` succeeds
- **THEN** the function SHALL return `{:ok, raw_token, entry}` where `raw_token` begins with `"flzt_"` AND the DETS row SHALL store only the SHA-256 hash

#### Scenario: Revoked token is rejected at verification
- **WHEN** a token has been revoked via `TokenStore.revoke/2` and `TokenStore.verify/1` is called with the raw value
- **THEN** verification SHALL return `{:error, %Flezha.Error{code: "authz.token_revoked"}}`

#### Scenario: Expired token is rejected at verification
- **WHEN** a token's `expires_at` has passed
- **THEN** `TokenStore.verify/1` SHALL return `{:error, %Flezha.Error{code: "authz.token_expired"}}`

#### Scenario: A non-expiring token is reachable from the operations surface
- **WHEN** a user issues a token asking for no expiry
- **THEN** the entry SHALL record no expiry, verification SHALL keep answering
  for it, and the value SHALL have been obtainable without editing the store

#### Scenario: A runner entry is refused an expiry and refused `_system`
- **WHEN** a user asks for a `runner`-role entry with an expiry, or for one
  in the `_system` namespace
- **THEN** each request SHALL be refused, so that no unattended process holds a
  credential that lapses and no runner identity can be `_system`

#### Scenario: An approaching expiry is announced
- **WHEN** an entry with an expiry crosses the configured warning threshold
- **THEN** a `:token_expiring` event SHALL be emitted on the `:platform_authz`
  source naming the entry, so a lapse is observable before it happens

#### Scenario: Token file mode is restrictive
- **WHEN** `app_data/auth/tokens.dets` is created or reopened
- **THEN** its mode SHALL be `0600`

### Requirement: Single policy point `Flezha.AuthZ.authorize/3`

The system SHALL expose exactly one authorization decision function `Flezha.AuthZ.authorize(caller_context, action, scope) :: :ok | {:error, {:forbidden, reason}}`. `action` SHALL be an atom from the `Flezha.AuthZ.Action` enumeration. `scope` SHALL be a map containing at minimum `:namespace`. Every user-action function in `Flezha.Service.Observability` and `Flezha.Service.AuditLog` SHALL call this function before any side effect. Failed authorization SHALL emit an `:authorization_denied` event on `:platform_authz`.

#### Scenario: Denial returns forbidden tuple
- **WHEN** a `viewer` invokes a service function whose action requires `ops` or higher
- **THEN** `authorize/3` SHALL answer `{:error, {:forbidden, :role_insufficient}}`, the service function SHALL return `{:error, %Flezha.Error{code: "authz.role_insufficient", class: :denied}}`, AND it SHALL NOT perform the requested action

#### Scenario: Wrong-namespace denial
- **WHEN** an `ops` on `team-orders` invokes a service function whose `scope.namespace` is `team-payments`
- **THEN** `authorize/3` SHALL answer `{:error, {:forbidden, :namespace_mismatch}}` and the service function SHALL return `{:error, %Flezha.Error{code: "authz.namespace_mismatch"}}`

#### Scenario: Disabled mode returns ok unconditionally
- **WHEN** `:authn_authz, :enabled` is `false` and any caller invokes any service function
- **THEN** `authorize/3` SHALL return `:ok` unconditionally AND no quota check SHALL execute

### Requirement: Fixed role set

The system SHALL define exactly seven roles: `viewer`, `app`, `ops`, `admin`, `runner`, `spawner`, `stopper`. The role→action mapping SHALL be a compile-time map in `Flezha.AuthZ.Policy`, and a principal MAY hold several of these roles in its namespace, as the "An identity SHALL hold a set of roles in one namespace" requirement states. `viewer` SHALL grant read-only actions on the bound namespace. `app` SHALL grant viewer plus the namespace-scoped lifecycle an application performs on its own work, and SHALL NOT grant any action that destroys a record or moves work it did not create; because the only principal ever bound to it is a runner's, it SHALL also grant `:runner.connect`. `ops` SHALL grant everything `app` adds except that connect, plus the remaining operator and plugin lifecycle, observability mutations, audit rotation, and the reads of records in flight. `admin` SHALL grant ops plus namespace/role/quota mutation and the controller-wide settings; only `admin@_system` SHALL be allowed to invoke auth-state mutations on namespaces other than its own. `runner` SHALL grant only `:runner.connect` and SHALL NOT grant any administration action. `spawner` SHALL grant `:runner.connect`, `:operator.start` and `:operator.send` on the bound namespace and nothing else: the rights a tasklet-requested spawn needs, which equally authorize the same principal's start and send on the administration surface. `stopper` SHALL grant `:runner.connect` and `:operator.stop` on the bound namespace and nothing else: the right to stop any operator in that namespace, which authorizes the same principal's stop on the administration surface. `app` SHALL grant every action `spawner` and `stopper` grant. A role that grants `:runner.connect` is a role for a runner's principal; because a runner proves its namespace with a `runner` credential, the administration surface SHALL NOT issue a credential for any such role other than `runner` itself, which today excludes `app`, `spawner` and `stopper`, and that exclusion SHALL be derived from the grant rather than kept as a list of role names. The enumeration SHALL hold no action in reserve: an action no declared verb names is deleted, not kept. The role name `operator` SHALL NOT be accepted by `flz role bind` or any role store read: it names the unit, never a role.

A runner connection's roles SHALL be resolved from the role store keyed on its principal rather than stamped by the connect path. The connection SHALL hold every role bound to its principal in the namespace the connection proved that grants `:runner.connect`, and an action SHALL be authorized for it when any of those roles grants the action. A principal with no binding, one whose binding names a namespace other than the one the connection proved, or one none of whose bound roles grants `:runner.connect` SHALL resolve to `runner` alone; a bound role that does not grant `:runner.connect` SHALL NOT be held by a runner connection. The resolution SHALL be read per call, so that binding or unbinding a role takes effect on the next call without a reconnect.

#### Scenario: Admin on team-orders cannot mutate team-payments
- **WHEN** `admin@team-orders` invokes a service call whose `scope.namespace` is `team-payments` and whose action is `:authz.token_issue`
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "authz.cross_namespace_admin_denied"}}`

#### Scenario: Runner role cannot read audit
- **WHEN** an SDK Runner with role `runner` invokes any function in `Flezha.Service.AuditLog`
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "authz.role_insufficient"}}`

#### Scenario: The runner grant is one action
- **WHEN** the runner role's grants are enumerated
- **THEN** they SHALL contain `:runner.connect` and nothing else

#### Scenario: An unbound runner principal gains nothing
- **WHEN** a runner connects for a namespace whose `runner@<namespace>` principal
  has no role binding
- **THEN** it SHALL resolve to `runner`, and every administration action SHALL be
  refused

#### Scenario: A bound runner principal acts in its own namespace
- **WHEN** `runner@team-orders` is bound to `app` on `team-orders` and a runner
  on that connection asks to start an operator there
- **THEN** the call SHALL be authorized, and the same call naming another
  namespace SHALL be refused

#### Scenario: Unbinding takes effect without a reconnect
- **WHEN** a runner's principal is unbound while its connection is open
- **THEN** its next call SHALL be refused, without the connection having been
  closed or re-established

#### Scenario: A binding for another namespace grants nothing
- **WHEN** `runner@team-orders` is bound to `app` on `team-payments` and a
  runner connects for `team-orders`
- **THEN** it SHALL resolve to `runner`, and every administration action SHALL be
  refused

#### Scenario: The app role destroys nothing
- **WHEN** the `app` role's grants are enumerated
- **THEN** they SHALL contain no action that drops a parked payload, rotates or
  deletes persisted state, or rewinds or replays work

#### Scenario: The spawner grant is the connect and the two rights a spawn needs
- **WHEN** the spawner role's grants are enumerated
- **THEN** they SHALL contain `:runner.connect`, `:operator.start` and
  `:operator.send` and nothing else, and `app` SHALL grant each of them

#### Scenario: A spawner binding opens neither a stop nor the plugin verbs
- **WHEN** `runner@team-orders` holds `spawner` and no other role on
  `team-orders` and the application code on that connection asks to stop an
  operator, or to enable or disable a plugin
- **THEN** each call SHALL be refused with `authz.role_insufficient`

#### Scenario: A spawner starts nothing in another namespace
- **WHEN** `runner@team-orders` is bound to `spawner` on `team-orders` and a start
  is judged for that identity in `team-payments`
- **THEN** it SHALL be refused with `authz.namespace_mismatch`

#### Scenario: No credential is issued for a runner principal's role
- **WHEN** a caller asks `flz token issue` for a credential with the role
  `app`, `spawner` or `stopper`
- **THEN** the request SHALL be refused as an unknown role and no credential
  SHALL be issued

#### Scenario: The issuable roles follow from the connect grant
- **WHEN** the roles `flz token issue` accepts are enumerated
- **THEN** they SHALL be `runner` and every role that does not grant
  `:runner.connect`, and a role added to the enumeration with the connect SHALL
  be refused a credential without any list of names being edited

#### Scenario: The stopper grant is the connect and the stop
- **WHEN** the stopper role's grants are enumerated
- **THEN** they SHALL contain `:runner.connect` and `:operator.stop` and nothing
  else, and `app` SHALL grant each of them

#### Scenario: A stopper binding opens the stop and nothing else
- **WHEN** `runner@team-orders` holds `stopper` and no other role on `team-orders`
  and the application code on that connection asks to stop an operator there
- **THEN** the stop SHALL be authorized, and a start, a send, and a plugin enable
  or disable on that connection SHALL each be refused with `authz.role_insufficient`

#### Scenario: A stopper stops nothing in another namespace
- **WHEN** `runner@team-orders` is bound to `stopper` on `team-orders` and a stop
  is judged for that identity in `team-payments`
- **THEN** it SHALL be refused with `authz.namespace_mismatch`

#### Scenario: Spawner and stopper combine
- **WHEN** `runner@team-orders` is bound to `spawner` and then to `stopper` on
  `team-orders`
- **THEN** a start, a send and a stop by that connection in `team-orders` SHALL
  each be authorized, and a plugin enable SHALL be refused with
  `authz.role_insufficient`

#### Scenario: A runner connection never holds an administration role
- **WHEN** `runner@team-orders` is bound to `spawner` and to `admin` on
  `team-orders`, and the application code on that connection asks to set the
  namespace's quotas or to delete the namespace
- **THEN** the connection SHALL hold `spawner` alone, and each call SHALL be
  refused with `authz.role_insufficient`

#### Scenario: A runner principal bound only to an administration role is a runner
- **WHEN** `runner@team-orders` holds only `ops` on `team-orders` and a
  runner connects for `team-orders`
- **THEN** it SHALL resolve to `runner`, its connect SHALL be authorized, and
  every administration action SHALL be refused

#### Scenario: The ops role is bound by its new name
- **WHEN** `flz role bind alice ops team-orders` is run
- **THEN** `alice` SHALL hold `ops@team-orders`, `flz role list` SHALL print `ops`, and `flz role bind alice operator team-orders` SHALL be refused as an unknown role

### Requirement: Namespace catalog

The system SHALL define a Namespace as a hard partition for authorization scope. Every namespace SHALL exist in the durable catalog (`Flezha.AuthZ.NamespaceStore`) before it can be bound to a token or a role. `_system` SHALL exist at boot, SHALL own resources created before this capability lands, and SHALL be the only namespace from which auth-state mutations on other namespaces are permitted (subject to the `admin` role check). Additional namespaces SHALL be created by `admin@_system` via `Flezha.AuthZ.NamespaceStore.create/2`. Deeper namespacing of the on-disk state subsystem itself (per-namespace WAL paths in `Flezha.State.*`) is out of scope for this change.

#### Scenario: System namespace exists at boot
- **WHEN** the Controller boots and `app_data/auth/namespaces.dets` is empty or missing
- **THEN** the namespace catalog SHALL contain exactly one entry named `_system`

#### Scenario: Cross-namespace audit read is filtered
- **WHEN** a caller bound to namespace `team-orders` invokes `Flezha.Service.AuditLog.read/2`
- **THEN** the returned records SHALL contain only records whose `namespace` is `team-orders` or that have no namespace field, AND SHALL NOT include other namespaces' records (unless the caller is `admin@_system`)

### Requirement: Per-namespace quotas at allocation

The system SHALL store per-namespace quotas in `app_data/auth/namespaces.dets`. Quota fields SHALL include `max_operators`, `max_cells_per_operator`, `max_mailbox_bytes_total`, `max_message_rate_per_second`, `max_state_bytes`, `max_runners`. `Flezha.AuthZ.Quotas.check_allocation/3` SHALL be a hot-path check (one ETS lookup against `NamespaceStore` plus one against `QuotaCounters`). `Flezha.Service.OperatorManager.start_operator/2` SHALL call `check_allocation/3` for `max_operators` and `max_cells_per_operator`; `stop_operator/2` SHALL record the dealloc. `FleetManager.connect_handler/4` SHALL check `max_runners`; disk accounting and the per-namespace mailbox watermark are specified by the "Runner and disk quota points" requirement.

#### Scenario: Max-operators denial on allocation
- **WHEN** namespace `team-orders` has `max_operators: 5`, 5 operators already exist, and a user invokes `Flezha.Service.OperatorManager.start_operator/2` with a 6th
- **THEN** the call SHALL return `{:error, {:quota_exceeded, :max_operators, %{current: 5, limit: 5}}}` AND the 6th Operator SHALL NOT be started

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

### Requirement: State-path namespace prefix and migration

The system SHALL move any pre-namespace top-level subtree under `app_data/state/` into `app_data/state/_system/` via a one-shot migration pass added to `Flezha.Boot.StatePathMigrator`. The migration SHALL be atomic per top-level tree (single `rename(2)` per operator-class), safe to interrupt, and idempotent. The namespace SHALL be plumbed through `Flezha.State.PersistenceResolver` / `Wire` / `WalWriter` / `Snapshot` / `Restore` so writes land under `app_data/state/<namespace>/...` rather than the migrated `_system` prefix.

#### Scenario: Migration moves legacy state under _system
- **WHEN** the Controller boots with `app_data/state/orders/` existing and no `_system/` subdirectory
- **THEN** after the migrator runs, `app_data/state/_system/orders/` SHALL exist AND `app_data/state/orders/` SHALL NOT exist

#### Scenario: Migration is idempotent
- **WHEN** the migrator runs twice in succession on an already-migrated tree
- **THEN** the second run SHALL leave the layout unchanged

#### Scenario: Namespaced operator writes under its namespace

- **WHEN** an operator in namespace `team-orders` persists cell state
- **THEN** the WAL and snapshot files land under `app_data/state/team-orders/...`
  and restore resolves the same path

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

The system SHALL expose `Flezha.Service.CliManager.authz_*`, `namespace_*`, and `role_*` wrapper functions covering the documented verb set (`bootstrap`, `issue`, `revoke`, `list`, `whoami`; `create`, `delete`, `list`, `show`, `set_quota`; `bind`, `unbind`, `list`). Each wrapper SHALL build a `caller_context` with `source: :cli`, route through `Flezha.AuthZ.authorize/3` (where the verb maps to a documented action), call the corresponding `Flezha.AuthZ.*` primitive, and emit a `:platform_authz` audit event on mutating verbs. The Rust `flz` binary SHALL expose these wrappers as verb families (`flz auth`, `flz role`, `flz token`, and the extended `flz namespace`), each rendering the wrapper's verdicts for the user.

#### Scenario: namespace_create routes through AuthZ.authorize
- **WHEN** `CliManager.namespace_create/3` is invoked
- **THEN** `Flezha.AuthZ.authorize/3` SHALL be called with action `:authz.namespace_create` AND, on success, a `:namespace_created` event SHALL appear on `:platform_authz`

#### Scenario: role_unbind looks up the bound namespace before authorizing
- **WHEN** `CliManager.role_unbind/2` is invoked for a principal currently bound to `team-orders`
- **THEN** `Flezha.AuthZ.authorize/3` SHALL be called with `scope.namespace = "team-orders"` (not the caller's namespace)

#### Scenario: Rust verbs reach the wrappers
- **WHEN** `flz role bind <principal> <role> <namespace>` is invoked with a valid
  admin token
- **THEN** `CliManager.role_bind` runs, the binding is created, and the CLI renders
  the wrapper's success verdict

### Requirement: Durable token-store and role-store crash recovery

`app_data/auth/tokens.dets`, `app_data/auth/roles.dets`, `app_data/auth/namespaces.dets`, `app_data/auth/quota_counters.dets`, and `app_data/auth/state.dets` SHALL be opened with `:sync` writes so every mutation is durably written before the mutating call returns. On Controller boot, if any of these files is detected as corrupt, the system SHALL rename the corrupt file to `<name>.corrupt-<unix_ts>` and open a fresh empty file. While `:enabled` is `true`, the Controller SHALL refuse to leave `Flezha.AuthZ.Supervisor` healthy unless either (a) the recovered state contains at least one `admin@_system` role binding, or (b) `bootstrap.token` is present, or (c) `FLEZHA_AUTHZ_ENABLED=false` is set in the environment.

Each of these files SHALL be one declared durable store: it names the layout subtree it lives in and its filename, and the opening, the corrupt-file rotation, the per-mutation sync and the close on shutdown SHALL be performed by the shared store implementation rather than by the owning module. The directory SHALL be claimed through the layout declaration, so the five files follow the configured auth directory with no module joining a root of its own.

#### Scenario: Corrupt DETS is rotated
- **WHEN** the Controller boots and one of the auth DETS files cannot be opened
- **THEN** the file SHALL be renamed to `<name>.corrupt-<unix_ts>` AND a fresh empty file SHALL be opened in its place

#### Scenario: Every store closes on shutdown through one implementation
- **WHEN** the Controller shuts down cleanly
- **THEN** each declared store's table SHALL be closed by the shared implementation, and no owning module SHALL close its own table

### Requirement: Runtime configuration surface

The system SHALL expose runtime configuration under the `:authn_authz` key with at minimum: `:enabled` (boolean, default `true` in production; dev/test carry the explicit opt-out), `:auth_dir` (default `app_data/auth`), `:bootstrap_token_path` (default `<auth_dir>/bootstrap.token`), `:token_ttl_default_seconds` (default `86_400`), `:token_expiry_warning_ms` (default `86_400_000`), `:token_expiry_check_interval_ms` (default `300_000`), `:namespaces` (per-namespace quota overrides), `:application_managed` (boolean, default `true`; tests flip to `false`). There SHALL be no second, longer default TTL for a machine-held credential, because no unattended process holds an expiring credential. Env-var overrides `FLEZHA_AUTHZ_ENABLED`, `FLEZHA_AUTHZ_DIR`, `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` and `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` SHALL be honoured with `env > app config > default` precedence.

#### Scenario: Env var overrides config
- **WHEN** the Controller boots with `FLEZHA_AUTHZ_DIR=/srv/flezha/auth` set
- **THEN** every auth-state DETS file SHALL be created under `/srv/flezha/auth/` regardless of any application-config setting

#### Scenario: Default-namespace quotas applied when not overridden
- **WHEN** namespace `team-orders` is created without quota overrides
- **THEN** the namespace catalog entry SHALL store the documented default quotas (`max_operators: 100`, `max_cells_per_operator: 1_024`, `max_mailbox_bytes_total: 268_435_456`, `max_message_rate_per_second: 100_000`, `max_state_bytes: 107_374_182_400`, `max_runners: 32`)

### Requirement: Authorization is enforced by default

The default posture SHALL enforce authorization. On a boot where no `admin@_system`
binding exists (bootstrap not yet claimed), gated verbs SHALL be refused with a
message naming the bootstrap tasklet, while already-running dataflow (operator
execution, runner reconnects over the authenticated transport) SHALL continue.
Disabling enforcement SHALL require the explicit `FLEZHA_AUTHZ_ENABLED=false`
opt-out, which SHALL emit a `warn` on every boot for as long as it is set.

The bootstrap refusal SHALL name the bootstrap token's path in the message itself. It
SHALL NOT direct the user to a log line, because the path is logged only on the
boot that writes the file: a controller that boots again with the token still unclaimed
writes nothing, and the user is sent to a line that is not there.

#### Scenario: Bootstrap-pending refuses gated verbs without stopping dataflow

- **WHEN** a controller boots with default configuration and no `admin@_system`
  binding exists
- **THEN** a destructive verb is refused naming the bootstrap tasklet, AND
  running operators continue to process input

#### Scenario: The refusal names the token file

- **WHEN** a gated verb is refused while the bootstrap is unclaimed
- **THEN** the message SHALL carry the configured bootstrap token path, and SHALL NOT
  promise that the path was logged at controller boot

#### Scenario: Open mode is explicit and warned

- **WHEN** a user sets `FLEZHA_AUTHZ_ENABLED=false`
- **THEN** the controller SHALL boot with authorization disabled and SHALL emit a
  `warn` log line on every boot for as long as the opt-out is set

### Requirement: Destructive and mutating service verbs require authorization

Every destructive or mutating service operation SHALL be gated by
`AuthZ.authorize/3` before it takes effect. This includes at least `state_delete`,
`state_rotate`, `dlq_drop`, `dlq_replay`, `operator_stop`, `controller_stop`, and
the plugin mutations (install, enable, disable). An unauthorized caller SHALL be
refused and the refusal SHALL be audited.

#### Scenario: Unauthorized destructive verb is refused

- **WHEN** a caller without the required role invokes `state_delete` (or any
  destructive verb) while authorization is enforced
- **THEN** the operation does not take effect and a denial is audited

#### Scenario: Authorized caller succeeds and is audited

- **WHEN** a caller with the required role invokes a destructive verb
- **THEN** the operation proceeds and an audit event records the actor and target

#### Scenario: Plugin enable is gated

- **WHEN** a `viewer`-role caller invokes plugin enable while authorization is
  enforced
- **THEN** the plugin is not enabled and a denial is audited

### Requirement: Principals derive from the authenticated transport

While authorization is enforced, `caller_context.principal` SHALL derive from what the connection proved, never from an unverified caller-supplied value. For a CLI caller that is a verified token. For a runner connection over a socket transport the namespace SHALL be established by the connection's own credential — a peer certificate naming the namespace, or a per-namespace runner credential from the token registry — and a connect frame naming a namespace the credential does not entitle SHALL be refused. For a runner over the in-node transport the proof SHALL be co-residence in the controller's own BEAM. The `:internal` caller source SHALL remain valid only for calls originating inside the controller's own BEAM. A runner connection SHALL NOT resolve to the `_system` namespace, on any transport.

#### Scenario: Runner principal is bound to what its connection proved

- **WHEN** a runner connects presenting a credential entitling it to namespace
  `team-orders` and its connect frame names `team-orders`
- **THEN** service calls attributed to that connection carry a `runner`-role
  principal bound to `team-orders` and cannot act on another namespace

#### Scenario: A claimed namespace the credential does not entitle is refused

- **WHEN** a runner connects presenting a credential entitling it to
  `team-orders` and its connect frame names `team-payments`
- **THEN** the connection SHALL be refused, no control or data frame SHALL be
  processed, and the refusal SHALL name both namespaces

#### Scenario: A runner cannot connect as the system namespace

- **WHEN** a runner connect frame names `_system`
- **THEN** the connection SHALL be refused, so that no runner identity can
  satisfy the cross-namespace admin gate

#### Scenario: Caller-supplied principal is ignored

- **WHEN** a CLI invocation supplies a `caller_context` naming a principal without
  a verifying token
- **THEN** the supplied principal is not trusted and gated verbs are refused

### Requirement: Namespace isolation through the State subsystem and registry

Namespace-scoped reads and mutations SHALL resolve the target namespace from the
caller's bound principal, and registry or state listings SHALL be filtered to the
namespaces the principal may read. A principal bound to one namespace SHALL NOT be
able to read, list, or mutate another namespace's operators or durable state
through any service verb.

#### Scenario: Cross-namespace state read is refused

- **WHEN** `ops@team-a` invokes a state read naming an operator in `team-b`
- **THEN** the call is refused with a forbidden error and the denial is audited

#### Scenario: Listings are namespace-filtered

- **WHEN** `viewer@team-a` lists operators
- **THEN** the result contains only `team-a` rows, with no indication of other
  namespaces' contents

### Requirement: Runner and disk quota points

Per-namespace quotas SHALL be enforceable for runner connections (`max_runners`,
checked when a runner connects) and disk footprint (`max_state_bytes`, accounted
from the State registry's per-namespace byte totals and enforced at operator
allocation). Exceeding the disk quota with already-running operators SHALL raise
a loud, observable alarm rather than interrupting durable writes. Quota counters
SHALL be rebuilt from a State registry scan on boot.

#### Scenario: Runner connection over quota is refused

- **WHEN** a namespace is at its `max_runners` quota and another runner connects
  for it
- **THEN** the connection is refused and the refusal is observable

#### Scenario: Disk quota breach on a running namespace alarms without data loss

- **WHEN** a namespace's accounted state bytes exceed `max_state_bytes` while its
  operators are running
- **THEN** an alarm event is emitted, new operator allocation in that namespace is
  refused, and in-flight durable writes are not interrupted

### Requirement: Break-glass recovery for a lost admin

The system SHALL provide a documented recovery path for a lost `admin@_system` credential that does not require stopping the controller. A user with filesystem access to `app_data/auth/` SHALL be able to write the bootstrap token file that `Flezha.AuthZ.Bootstrap` already reads, and claim it once against the running controller. The recovery SHALL be impossible without filesystem access and SHALL emit an auditable event recording that recovery occurred. Recovery with the controller stopped SHALL remain available and unchanged.

#### Scenario: Recovery re-enters bootstrap without a stop

- **WHEN** the user writes the bootstrap token file and claims it against a
  running controller
- **THEN** the claim SHALL succeed exactly once, the previous admin binding SHALL
  no longer authorize, an auditable event SHALL record that recovery occurred,
  and the controller SHALL NOT have been stopped

#### Scenario: Recovery is refused without filesystem access

- **WHEN** a caller attempts the recovery claim without having written the
  bootstrap token file
- **THEN** the claim SHALL be refused and the refusal SHALL name the file it
  expected

#### Scenario: Recovery re-enters bootstrap

- **WHEN** the user runs the offline recovery tasklet with the controller
  stopped and boots the controller
- **THEN** a fresh `bootstrap.token` is written, the previous admin binding no
  longer authorizes, and an auditable event records that recovery occurred

### Requirement: Plugin credentials are not persisted in cleartext

Plugin provider credentials (at least Kafka SASL) SHALL NOT be persisted or logged
in cleartext: credential material SHALL ride a closure into the client
configuration, `init_provider/1` SHALL return opts with the secret redacted, and
failure logs SHALL carry endpoint identity only — mirroring the shipped Postgres
provider pattern.

#### Scenario: SASL secret is redacted at rest

- **WHEN** a Kafka plugin with SASL credentials is enabled and its persisted opts
  are read back
- **THEN** the stored and returned opts carry a redacted placeholder, not the
  secret, while the live client authenticates successfully

### Requirement: Every administration verb that reads business data or moves live work SHALL be an authorized action

Such a verb SHALL name an action in the authorizable-action enumeration and SHALL
authorize it before doing anything — a verb that reads parked business data or
records in flight, reads a cell's state live or at a point in time, listens to the
frames an operator ingests or emits, injects input into or replays inputs through an
operator, enables a plugin on an operator, rewrites a controller-wide setting,
creates or destroys a schedule, starts an operator, or moves live work between
runners. A verb whose action is absent from that enumeration is not merely ungated, it
is unauthorizable — the policy point refuses only actions it knows — so the
enumeration is the surface this requirement governs.

A CLI verb that this requirement gates SHALL accept the caller's credential, so that
naming its action does not make it refuse every caller. A verb whose signature cannot
carry a token is not gateable, and widening that signature is part of gating it.

Each new action SHALL be granted to at least the roles that can invoke the verb today,
unless narrowing it is the recorded intent of the change that adds it. A read of
business data SHALL be granted no more widely than the mutation on the same data.

A verb whose effect is one an existing action already names SHALL authorize that action
rather than add a second name for the same right: firing a schedule by hand starts an
operator, and is judged as `operator.start` in the namespace the schedule was added
under; reading a cell's live state is `state.get`. Listening to an operator is its
own action, `operator.listen`, because the right to read the frames an operator
carries is not the right to attach an arbitrary plugin to it: it is granted from `app`
upward, the same floor as `plugin.enable`, so no caller that could listen before the
action existed is refused after, and no caller gains it. `flz operator send --await`
is judged as `operator.send`, the action of the verb the caller invoked; the listener
it attaches to print what came back is that verb's own mechanism, not a second verb.

#### Scenario: A parked payload is not readable by an unauthorized caller

- **WHEN** a caller with no role binding lists or shows a dead-letter entry while
  authorization is enforced
- **THEN** the read SHALL be refused, and no payload bytes SHALL be returned

#### Scenario: A schedule is not created by an unauthorized caller

- **WHEN** a caller with no role binding adds, removes, asserts or retracts a schedule
  while authorization is enforced
- **THEN** the verb SHALL be refused and no schedule SHALL change

#### Scenario: Live partitions are not moved by an unauthorized caller

- **WHEN** a caller with no role binding rescales a partition group or rewinds an
  operator while authorization is enforced
- **THEN** the verb SHALL be refused and no work SHALL move

#### Scenario: A gated verb still reaches the callers it had

- **WHEN** a role that could invoke one of these verbs before its action existed invokes
  it after
- **THEN** the verb SHALL succeed, unless the change that named the action recorded a
  narrower grant as its intent

#### Scenario: A schedule is not fired by hand by an unauthorized caller

- **WHEN** a caller with no role binding issues `flz schedule run <name>` while
  authorization is enforced
- **THEN** the verb SHALL be refused as `operator.start` in the schedule's namespace and
  no instance SHALL spawn

#### Scenario: An operator is not started, fed, replayed or listened to by an unauthorized caller

- **WHEN** a caller with no role binding issues `flz operator start`, `operator send`,
  `operator replay` or `operator listen` while authorization is enforced
- **THEN** each verb SHALL be refused as `operator.start`, `operator.send`,
  `operator.replay` or `operator.listen` respectively, and no operator SHALL start,
  no input SHALL be delivered, no replay SHALL run and no plugin SHALL be enabled

#### Scenario: Listening needs the listen action and not the plugin one

- **WHEN** authorization is enforced and a caller bound to `app` on the operator's
  namespace issues `flz operator listen`
- **THEN** the verb SHALL be authorized as `operator.listen`, and the same call by a
  caller bound to `viewer`, `spawner` or `stopper` SHALL be refused, whether or not
  that caller could enable a plugin

#### Scenario: A controller-wide setting is not rewritten by an unauthorized caller

- **WHEN** a caller whose role is below `admin` issues `flz state retention` while
  authorization is enforced
- **THEN** the verb SHALL be refused as `state.retention` in `_system` and the setting
  SHALL be unchanged

#### Scenario: Records in flight are not read by an unauthorized caller

- **WHEN** a caller with no role binding, or with the `viewer` role, issues `flz state
  inbox`, `state journal` or `state outbox` while authorization is enforced
- **THEN** the read SHALL be refused as `state.records` and no record bytes SHALL be
  returned

#### Scenario: Cell state is read by the role that reads persisted state

- **WHEN** a `viewer` bound to the cell's namespace issues `flz cell state` or
  `flz state at` while authorization is enforced
- **THEN** the read SHALL be served as `state.get` and `state.at` respectively, and the
  same read by a caller with no role binding SHALL be refused

#### Scenario: A gated verb receives the token it was given

- **WHEN** any gated verb is issued from `flz` with a valid token for a role that grants
  its action
- **THEN** the verb SHALL succeed; no gated verb SHALL answer that no identity was
  supplied when one was

### Requirement: Every administration verb SHALL be declared once, and one seam SHALL apply the declaration

Every administration verb SHALL be one row of a declaration table in the policy directory,
naming its action (an atom of the authorizable-action enumeration, or `:public` for a
read the policy calls public, or `:none` for a function that is not a verb), the rule
that resolves its scope (an argument, `_system`, the caller's own namespace, or a field
of a stored row), and its audit (none, before the act, or after the act with its
outcome, on a named source). A row SHALL name no module.

One function SHALL apply a row for every surface — the `flz` wire, the published
service API, the runner session — in one order: identity, scope, policy, act, audit.
A surface SHALL bind its function to a row, and a function on a surface that binds no
row SHALL NOT exist: a wire name with no row is a compile error, and a published
function with no row fails the suite. Every action in the enumeration SHALL be named
by at least one row, so an action can neither outlive its verbs nor be declared without
one.

A verb whose scope is a field of a stored row SHALL refuse a caller with no bound
principal, and a caller whose role does not grant the action, before the row is read,
so that the refusal says nothing about whether the name exists.

#### Scenario: A wire name without a row does not compile

- **WHEN** a delegate is added to `Flezha.Service.CliManager` beside the declared verbs
  without a row for it
- **THEN** compilation SHALL fail naming the wire name and the missing row

#### Scenario: A published function without a row fails the suite

- **WHEN** a public function is added to a published service module that reaches an
  effect without applying a row
- **THEN** the declaration test SHALL fail naming the module, function and arity

#### Scenario: An action no row names fails the suite

- **WHEN** an atom is added to the action enumeration and no row names it
- **THEN** the declaration test SHALL fail naming the action

#### Scenario: Every gated row refuses an unbound caller

- **WHEN** authorization is enforced and every row whose action is not `:public` or
  `:none` is invoked with a caller context carrying no principal
- **THEN** every invocation SHALL be refused, no effect SHALL run, and each refusal SHALL
  be audited as a denial once

#### Scenario: A row-scoped verb refuses before it reads

- **WHEN** authorization is enforced and a verb whose scope is a field of a stored row
  is invoked by a caller with no bound principal, or by a principal whose role does not
  grant the action, naming a row that does not exist
- **THEN** the verb SHALL be refused with the refusal a bound caller would receive, and
  the row store SHALL NOT be read

#### Scenario: A public read is served without a credential

- **WHEN** a verb declared `:public` is invoked with no token while authorization is
  enforced
- **THEN** it SHALL be served, and no denial SHALL be audited

#### Scenario: The same verb is judged identically from every surface

- **WHEN** a verb the terminal and the published surface both carry is invoked from
  each with the same principal and the same target
- **THEN** both SHALL reach the same verdict in the same scope, and an authorized act
  SHALL be audited once with one record shape

### Requirement: Every identifier kind SHALL be one declared rule

An identifier the system accepts or mints SHALL be governed by one declaration holding a row per kind — at minimum the operator id, the namespace name, the plugin id, the schedule name, the authorization token id and the runner id — and each row SHALL carry the kind's minimum length, its maximum length in bytes, and the pattern a value must match.

Validation SHALL be performed by the declaration's own function. No module SHALL carry a second length constant or name pattern for a kind the declaration already holds, and a kind absent from the declaration SHALL NOT be validated by a private rule.

Minting SHALL respect the same row: a value the system generates for a kind SHALL satisfy that kind's rule at the moment it is generated, so a generated identifier is never rejected or rewritten by a consumer downstream.

A rule that a different runtime enforces — the byte bound a runner id must satisfy to fit its socket name — SHALL be one value both runtimes read, rather than a number one side applies to the other side's output.

#### Scenario: A value one byte over its kind's limit is refused
- **WHEN** a value one byte longer than its kind's declared maximum is submitted for any declared kind
- **THEN** validation SHALL refuse it, naming the kind and the limit

#### Scenario: A value at its kind's limit is accepted
- **WHEN** a value of exactly the declared maximum, matching the kind's pattern, is submitted
- **THEN** validation SHALL accept it

#### Scenario: A minted identifier satisfies its own kind's rule
- **WHEN** the system mints an identifier for a kind
- **THEN** the minted value SHALL satisfy that kind's declared rule, including any byte bound another runtime enforces on it

#### Scenario: A new kind is a row, not a module
- **WHEN** a kind of identifier is added to the system
- **THEN** it SHALL be added as a row of the declaration, and no module SHALL gain a length constant or a name pattern of its own

### Requirement: An identity SHALL hold a set of roles in one namespace

The role store SHALL hold, for each principal, one namespace and a non-empty set of roles in it, and an action SHALL be authorized for a caller judged by its stored roles when any role in the set grants it, each role's grants and namespace rule being those of the role alone. Binding a role SHALL add it to the principal's set, and binding a role the principal already holds SHALL change nothing. Binding a role in a namespace other than the one the principal already holds roles in SHALL be refused, naming the namespace it is bound in, and SHALL change nothing; moving an identity is an unbind judged in the namespace it leaves followed by a bind. A caller whose credential carries a role, as a token does, SHALL be judged by that role alone, whatever the store holds for its principal, so that a token authorizes exactly what it authorized before this requirement. A bootstrap or break-glass claim SHALL leave its principal holding exactly `admin` in `_system`, whatever it held before, and a recovery SHALL remove every role of each identity that held `admin` in `_system`.

#### Scenario: A second role is added, not substituted
- **WHEN** `runner@team-orders` is bound to `spawner` on `team-orders` and then to
  `stopper` on `team-orders`
- **THEN** its binding SHALL hold both `spawner` and `stopper` in `team-orders`

#### Scenario: Binding a held role changes nothing
- **WHEN** a principal is bound to a role it already holds in the same namespace
- **THEN** its set, its `bound_at` and its `bound_by` SHALL be unchanged, and the
  stored row SHALL NOT be rewritten

#### Scenario: A bind in another namespace is refused
- **WHEN** `runner@team-b` holds `app` on `team-b` and an admin of `team-a` binds
  it to `viewer` on `team-a`
- **THEN** the bind SHALL be refused naming `team-b`, and `runner@team-b` SHALL
  still hold `app` on `team-b` and nothing on `team-a`

#### Scenario: The policy point grants when any held role grants
- **WHEN** a caller holds `spawner` and `stopper` in `team-orders` and asks to
  start an operator, to stop one and to enable a plugin there, and to stop one in
  `team-payments`
- **THEN** the start SHALL be authorized by `spawner` and the stop by `stopper`,
  the plugin enable SHALL be refused with `authz.role_insufficient`, and the stop in
  `team-payments` SHALL be refused with `authz.namespace_mismatch`

#### Scenario: A token is judged by its own role
- **WHEN** `alice` holds `ops` on `team-orders` in the role store and presents
  a token issued to her with the role `viewer` on `team-orders`, and asks to stop
  an operator there
- **THEN** the stop SHALL be refused with `authz.role_insufficient`

#### Scenario: A claim replaces the claimant's roles
- **WHEN** the bootstrap token is claimed naming a principal that holds `ops`
  on `team-a`
- **THEN** that principal SHALL hold exactly `admin` in `_system` afterwards

#### Scenario: A recovery removes the lost admin whole
- **WHEN** a break-glass recovery runs while a principal holds `admin` and
  `ops` in `_system`
- **THEN** that principal SHALL hold no role afterwards

### Requirement: `flz role` SHALL add a role, remove one role or every role, and list every role an identity holds

`flz role bind <principal> <role> <namespace>` SHALL add `<role>` to the principal's roles and SHALL report the binding and the whole set it now holds. `flz role unbind <principal> <role>` SHALL remove that one role and SHALL delete the principal's binding when it removed the last one; `flz role unbind <principal>` with no role SHALL remove every role the principal holds. Both forms of unbind SHALL be idempotent and SHALL be judged in the namespace the principal is bound in, and a named role SHALL be resolved against the role enumeration so that an unknown name is refused as an unknown role. `flz role list` SHALL print one line per principal naming every role it holds and its namespace, and a principal holding one role SHALL print the line it printed before this requirement. The terminal SHALL always send the role argument of an unbind, empty when none was named, so that the controller reads the principal, the role and the options in fixed positions.

#### Scenario: Unbinding one role keeps the others
- **WHEN** `runner@team-orders` holds `spawner` and `stopper` and an admin runs
  `flz role unbind runner@team-orders stopper`
- **THEN** it SHALL hold `spawner` alone, and its next stop SHALL be refused
  without a reconnect

#### Scenario: Unbinding the last role removes the binding
- **WHEN** `runner@team-orders` holds only `stopper` and an admin runs
  `flz role unbind runner@team-orders stopper`
- **THEN** the principal SHALL have no binding, and its connection SHALL resolve
  to `runner`

#### Scenario: Unbinding with no role removes every role
- **WHEN** `runner@team-orders` holds `spawner` and `stopper` and an admin runs
  `flz role unbind runner@team-orders`
- **THEN** the principal SHALL have no binding

#### Scenario: Unbinding a role not held changes nothing
- **WHEN** an admin unbinds a role the principal does not hold
- **THEN** the verb SHALL succeed and the principal's set SHALL be unchanged

#### Scenario: The list names every role of an identity on one line
- **WHEN** `runner@team-orders` holds `spawner` and `stopper` on `team-orders` and
  `alice` holds `ops` on `team-orders`, and an admin runs `flz role list`
- **THEN** the output SHALL contain the lines
  `runner@team-orders  spawner,stopper@team-orders` and
  `alice  ops@team-orders`

#### Scenario: The terminal carries the role in the position the controller reads
- **WHEN** `flz role unbind runner@ns spawner` and `flz role unbind runner@ns` are
  invoked
- **THEN** they SHALL request
  `authz_render_role_unbind("runner@ns", "spawner", [])` and
  `authz_render_role_unbind("runner@ns", "", [])` respectively, and
  `flz role bind runner@ns stopper ns` SHALL request
  `authz_render_role_bind("runner@ns", "stopper", "ns", [])`

### Requirement: Role-store rows written before sets SHALL load as a set of one

A `roles.dets` row in the one-role shape SHALL load as a principal holding that one role in its namespace, with no migration pass and no rewrite at boot, and SHALL be rewritten in the set shape only when that principal's roles next change. Every row the store writes SHALL carry the set, and SHALL also carry the one-role field an older release reads: the role itself when the set has exactly one member, and no role when it has several, so that an older release reads a one-role row as it always did, grants nothing from a row holding several, and raises on neither. A row matching neither shape SHALL be read as no binding.

#### Scenario: A file written before sets loads as a set of one
- **WHEN** the role store starts on a `roles.dets` written directly with the
  one-role term `{"runner@team-orders", %{principal: "runner@team-orders",
  namespace: "team-orders", role: :app, bound_at: _, bound_by: _}}` and an
  `admin@_system` row in the same shape
- **THEN** `runner@team-orders` SHALL hold `app` alone on `team-orders`, a runner
  connection for `team-orders` SHALL hold `app`, and an `admin@_system` binding
  SHALL be found

#### Scenario: Loading rewrites nothing
- **WHEN** the role store has started on a file of one-role rows and no role has
  been bound or unbound
- **THEN** every row on disk SHALL still be the term that was written before the
  store started

#### Scenario: A change to an old row writes the set shape
- **WHEN** a principal loaded from a one-role row holding `app` is bound to
  `stopper`
- **THEN** its row on disk SHALL carry the set `app`, `stopper` and no one-role
  value

#### Scenario: A one-role row keeps its role for an older reader
- **WHEN** a principal with no binding is bound to `spawner`
- **THEN** its row on disk SHALL carry the set `spawner` and the one-role value
  `spawner`

#### Scenario: A row holding several roles names no single role
- **WHEN** a principal holds `spawner` and `stopper`
- **THEN** its row on disk SHALL carry the one-role field with no role in it, so
  that a reader matching only that field grants nothing and does not raise

#### Scenario: An unreadable row grants nothing
- **WHEN** a row on disk matches neither the one-role nor the set shape
- **THEN** its principal SHALL be read as having no binding, and a runner
  connection for it SHALL resolve to `runner`

### Requirement: An authorization record SHALL name the roles its caller was judged with

Every act record the verb seam writes, and every `:authorization_denied` record, SHALL carry `metadata.caller_roles`: the roles the caller was judged with (a token's one role, a runner connection's resolved set, or the set the store holds for the principal), or no value when the refusal came before any role was resolved. The record SHALL name the set rather than a single granting role, because which held roles grant an action follows from the compile-time policy table. Every key an act record or a denial record carried before this requirement SHALL keep its name and meaning, and a record written before it, which has no `caller_roles`, SHALL be read as a record whose roles were not recorded.

#### Scenario: A runner's act names its set
- **WHEN** `runner@team-orders` holds `spawner` and `stopper` and its application
  code stops operator `o1` through the administration surface
- **THEN** the `stop` record on `operator_service` SHALL carry `caller_roles`
  `["spawner", "stopper"]` beside the principal `runner@team-orders` and the
  caller source `sdk`

#### Scenario: A token's act names the token's role
- **WHEN** an `ops` token stops an operator from `flz`
- **THEN** the `stop` record SHALL carry `caller_roles` `["ops"]`

#### Scenario: A denial names the set that did not grant
- **WHEN** `runner@team-orders` holds `spawner` alone and its application code
  asks to stop an operator
- **THEN** the `:authorization_denied` record SHALL carry the reason
  `role_insufficient` and `caller_roles` `["spawner"]`

#### Scenario: A denial before resolution names no roles
- **WHEN** a gated verb is invoked with no credential while authorization is
  enforced
- **THEN** the `:authorization_denied` record SHALL carry `caller_roles` with no
  value

#### Scenario: A record written before the key is read unchanged
- **WHEN** `flz audit read` reads a file holding a record written before
  `caller_roles` existed
- **THEN** the record SHALL be decoded and rendered as it was before, with no
  error

### Requirement: An outside call SHALL be judged and audited as `operator.send`

A call made through `flz operator call` or the published `Flezha.Service.Operator.call/6` SHALL be
authorized as `operator.send` in the operator's namespace, the action of the act it performs: an
input delivered to the operator, whose answer belongs to the caller that sent it. It SHALL be bound
to its own `Flezha.AuthZ.Verbs` row, `operator_call`, and SHALL write the audit record
`operator_send` writes, after the act, naming the operator, with the error code on a failure. A
refused call SHALL deliver no input. A call a plugin makes on the operator it is enabled on through
`Host.inject/5` is that operator's own input and SHALL NOT be judged again.

#### Scenario: A caller without the send right is refused

- **WHEN** authorization is enforced and a caller bound to `viewer` on the operator's namespace runs `flz operator call`
- **THEN** the call SHALL be refused as `operator.send`, exit 4, and no input SHALL reach the operator

#### Scenario: A call is audited like a send

- **WHEN** a caller bound to `app` calls an operator
- **THEN** the audit log SHALL hold one record for the act naming the operator, in the shape an `operator send` writes

### Requirement: An operator's call SHALL be judged as its runner identity

A call an operator's graph makes SHALL be judged as the runner identity of the caller's namespace,
the identity a tasklet's `ctx.spawn_bounded` is judged as: the creation of a callee against the
`operator_start` row, which authorizes `operator.start` and writes its audit record, and the
delivery of a request against the `spawn_deliver` row, which authorizes `operator.send` and writes
no record of its own. A refused creation or delivery SHALL end the call with a `Response` carrying
the refusal's `authz.*` code, and SHALL start and deliver nothing. The way an answer returns into
the caller SHALL NOT be judged again: it is the caller's own call coming back.

#### Scenario: A runner without the spawner role cannot create a session

- **WHEN** authorization is enforced, the caller's runner identity holds only `runner` in the namespace, and a call would create a session
- **THEN** the caller SHALL receive a `Response` carrying `authz.role_insufficient`, and no session SHALL start

#### Scenario: A creation is audited

- **WHEN** a call creates a session under enforced authorization for a runner identity bound to `spawner`
- **THEN** the audit log SHALL hold one `operator_start` record naming the new instance

