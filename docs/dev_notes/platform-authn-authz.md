# Platform authn/authz foundation (`platform-authn-authz`)

- **Modules**: `lib/flezha/authz/` — `Action`, `Role`, `Principal`,
  `Namespace`, `Scope`, `Policy`, `Config`, `Store`, `AuthState`,
  `RoleStore`, `NamespaceStore`, `TokenStore`, `QuotaCounters`,
  `Quotas`, `Bootstrap`, `Bootstrap.OnBoot`, `SourceRegistration`,
  `Supervisor`; top-level `Flezha.AuthZ` exposes `authorize/3`,
  `require_bound_principal/1`, `enabled?/0`. Cert side lives at
  `lib/flezha/authn/cert.ex` (`principal_from_peer/1` +
  `namespace_from_peer/1`, RFC 4514 normalised DN / SPIFFE URI SAN).
- **Supervision tree**: `Flezha.AuthZ.Supervisor` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.Observability.Supervisor`,
  right after `Flezha.Cluster.Orchestrator` (so after
  `Flezha.Shutdown.Coordinator`). Children, in order
  (`:rest_for_one`): `AuthState` → `RoleStore` → `NamespaceStore`
  → `TokenStore` → `QuotaCounters` → `SourceRegistration` (one-shot
  `:platform_authz` source registration) → `Bootstrap.OnBoot`
  (boot-time token writer + refuse-to-start gate). Gated by
  `:authn_authz, :application_managed` (default `true`; tests flip
  to `false` so they can bring up isolated stores against a tmp
  `auth_dir`).
- **On-disk layout**: `app_data/auth/` (mode `0700`) —
  `state.dets` (durable `:enabled` flag),
  `roles.dets` (principal → {namespace, role}),
  `namespaces.dets` (catalog + quotas),
  `tokens.dets` (SHA-256-hashed registry),
  `quota_counters.dets` (per-namespace counters),
  `bootstrap.token` (one-time, mode `0600`, unlinked on claim).
  Persistent-state tree under `app_data/state/_system/...` after the
  one-shot namespace migration in `Flezha.Boot.StatePathMigrator`.
- **Policy point**: `Flezha.AuthZ.authorize(caller_context, action,
  scope) :: :ok | {:error, {:forbidden, reason}}`. Four roles:
  `viewer`, `operator`, `admin`, `runner`. Role→action map is
  compile-time in `Flezha.AuthZ.Policy`. Auth-state mutations
  (`:authz.*`) are additionally gated to `admin` on the bound
  namespace; only `admin@_system` may target other namespaces.
- **Tokens**: wire form `flzt_<base32>` (256-bit); storage form
  SHA-256 hash. Raw value returned to caller exactly once at
  `TokenStore.issue/5`. Revocation is instant
  (`TokenStore.revoke/2` sets `revoked_at`; `verify/1` rejects).
  Mutations emit `:token_issued` / `:token_revoked` on
  `:platform_authz`.
- **Quota engine**: `Flezha.AuthZ.Quotas.check_allocation/3` —
  one ETS lookup per call (NamespaceStore cache + QuotaCounters
  cache). `record_allocation/3` / `record_deallocation/3` apply
  async. `OperativeManager.start_operative/2` enforces `max_operatives` /
  `max_entities_per_operative` at allocation time and records on
  success; `stop_operative/2` decrements on stop. Mailbox / disk /
  runner enforcement landed with `finish-platform-authn-authz`:
  `max_runners` in `FleetManager.admit_runner`, `max_state_bytes` at
  `start_operative` plus a latched Watchdog breach alarm,
  `max_mailbox_bytes_total` via `PersistenceResolver`.
- **Bootstrap**: on first boot when no `admin@_system` binding
  exists AND `:enabled` is `true`, `Bootstrap.OnBoot` writes
  `app_data/auth/bootstrap.token` (mode `0600`) after verifying the
  parent dir's mode (aborts with `error` on group/other-write
  bits). `flz auth bootstrap <token>` (Elixir wrapper:
  `Flezha.Service.CliManager.authz_bootstrap/2`) claims it,
  resolves the principal (cert SAN > cert DN > `--principal` >
  freshly-minted admin token), binds `admin@_system`, flips
  `AuthState.enabled` durably, unlinks the token file, emits
  `:bootstrap_completed`.
- **CLI surface** (Rust verbs shipped with `finish-platform-authn-authz`:
  `flz auth` / `flz token` / `flz role` / `flz namespace`):
  `Flezha.Service.CliManager.authz_{bootstrap,issue,revoke,list,whoami}`,
  `namespace_{create,delete,list,show,set_quota}`,
  `role_{bind,unbind,list}`. Each wraps in `AuthZ.authorize/3` and
  emits a `:platform_authz` audit event on mutating verbs.
- **Audit log integration**: `:platform_authz` in the default
  subscription allow-list (`Flezha.Observability.AuditLog.Sink`);
  JSONL records gain a top-level `namespace` field (defaults to
  `"_system"` when absent on pre-upgrade rows); `caller.principal`
  is the verified principal when `:enabled` is `true`, else the
  caller-source atom (preserving pre-change behaviour).
- **Enforcement posture** (reshaped by `finish-platform-authn-authz` —
  see its notes): `:authn_authz, :enabled` defaults to
  `config_env() == :prod`, so prod ENFORCES by default; dev/test
  default `false`. The posture is three-way
  (`Flezha.AuthZ.enforcement_posture/0`). While disabled,
  `AuthZ.authorize/3` returns `:ok` unconditionally and
  `require_bound_principal/1` is a no-op.
  `Bootstrap.claim/2` success still flips `:enabled` durably;
  `FLEZHA_AUTHZ_ENABLED=false` is an explicit opt-out that emits a
  `warn` on every boot.
- **Runtime config knobs**: `:authn_authz` map —
  `:enabled` (default `config_env() == :prod`, env `FLEZHA_AUTHZ_ENABLED`),
  `:auth_dir` (default `app_data/auth`, env `FLEZHA_AUTHZ_DIR`),
  `:bootstrap_token_path` (default `<auth_dir>/bootstrap.token`,
  env `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH`),
  `:token_ttl_default_seconds` (`86_400`),
  `:sdk_token_ttl_seconds` (`2_592_000`),
  `:namespaces` (map of per-namespace quota overrides),
  `:application_managed` (default `true`; tests set `false`).
- **Follow-ups — LANDED via `finish-platform-authn-authz`** (see
  `docs/dev_notes/finish-platform-authn-authz.md`): transport cert
  wiring (`flz_transport/src/transport/tls.rs`,
  `transport_auth_required`); CLI identity as a verified
  `--token`/`FLEZHA_TOKEN` context (NOT a socket handshake); the Rust
  CLI verbs; namespace-prefixed state at the `Registry.class_dir/1`
  seam; the remaining quota points; operator guide
  (`docs/operations/authn_authz.md`) + multi-tenant e2e
  (`test/system/multi_tenant_e2e_test.exs`). Not re-verified: the
  ProcessHub-registry namespace prefix and the remaining service-module
  refactors from the original deferral list.
