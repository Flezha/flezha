# Authn/authz finished end to end (`finish-platform-authn-authz`)

- **Enforcement is a THREE-WAY posture, not a boolean.**
  `AuthZ.enforcement_posture/0` → `:open` (explicit `FLEZHA_AUTHZ_ENABLED=false`
  opt-out, warned every boot) | `:bootstrap_pending` (configured on, unclaimed:
  gated verbs refuse naming `flz auth bootstrap`; `:internal` and runner-role
  `:sdk` contexts still pass so dataflow and runner reconnects continue) |
  `:enforced`. `AuthZ.enabled?/0` stays "config AND claimed" — quotas key on it,
  which is what keeps bootstrap-pending from bricking operative starts.
  Prod default is enabled (`config_env() == :prod` in runtime.exs); dev/test
  carry the explicit opt-out and the `AuthZ.Supervisor` warns on any disabled
  boot.
- **Identity comes ONLY from the transport.** CLI: `--token` / `FLEZHA_TOKEN`
  resolved by `cli_context/1` via `TokenStore.verify/1` — the old
  caller-supplied `principal:`/`role:` opts are IGNORED. Runner:
  `FleetManager.runner_caller_context/1` mints `runner@<ns>` (role `:runner`)
  from the connect handshake's namespace — 3.1's open question resolved as
  namespace-handshake binding, NOT peer-cert surfacing across the NIF (wire
  unchanged; the cert is verified at rustls only). GOTCHA: `FLEZHA_TOKEN` must
  be read by the RUST CLI process (`utils.rs auth_token`) — a `System.get_env`
  in the controller reads the CONTROLLER's env, not the caller's.
- **One CLI gating seam.** `authorize_cli!/3` (authorize-or-`cli_error!`,
  returns ctx) under `with_cli_authorization/5` (destructive shape: body + on
  success ONE `emit_cli_audit` naming actor + target). Refusal text renders via
  `render_forbidden/1` (`:bootstrap_pending` names the bootstrap procedure,
  `:principal_not_bound` names `--token`).
- **QUIC mutual TLS.** `build_quic_server_config` takes `client_ca_path`
  (`""` = the warned opt-out via `FLEZHA_TRANSPORT_AUTH=off`);
  `flz_transport::transport::tls` is the ONE set of PEM loaders shared by the
  client (`quic.rs`) and the server (`flz_nif`). `CertGenerator` now mints a
  `client.pem`/`client.key` (clientAuth EKU) beside the server pair — the CA
  key is never persisted, so a set missing ANY member regenerates WHOLE.
  A refused peer logs `[flz_nif] QUIC peer refused before handshake completed`
  (a `ConnectionLost` with a TransportError and zero streams).
  `test/quic_test.exs` runs in default `mix test` and now exercises mutual TLS
  end to end; its `setup_all` runners are the refusal case's positive control
  AND the cargo warm-up.
- **Bootstrap parent-mode guard TIGHTENS, it does not just refuse.** On a
  umask-0002 host (Ubuntu default) the app's own `File.mkdir_p` creates
  `app_data` as 0775, and a refuse-only guard bricked every fresh prod boot
  (found via `scripts/bench.sh`, which runs `MIX_ENV=prod`). `check_parent_mode`
  now strips group/other-write bits in place on a non-sticky parent; refusal
  remains for sticky shared dirs (`/tmp` shape) and parents it cannot chmod.
  GOTCHA: Erlang's `File.chmod/2` MASKS the sticky bit — tests must set 1777
  via `System.cmd("chmod", ...)`, though `File.stat` does report it.
- **GOTCHA — pre-existing bug the tests exposed:**
  `Quotas.refresh_disk_bytes` cached on `System.monotonic_time(:second)` with
  a `cached_at` default of `0`; monotonic time is negative-offset, so
  `now - 0 < 30` was ALWAYS true — the first read served the empty counter
  forever and never walked the disk. Fixed with a nil sentinel. Any
  "cache N seconds, default cached_at=0" pattern on monotonic time has this
  bug.
- **Namespace-prefixed state lives at ONE seam.** The wire's entity_id stays
  the 3-tuple; `Flezha.State.Registry.class_dir/1` resolves
  `<root>/<namespace>/<class>` via a claims ETS (`:flezha_state_namespaces`,
  keyed `{state_root, class}` — root-scoped so tests swapping `:state_root`
  cannot poison each other) → disk layout → `_system`. Claims are recorded at
  `Overwatch.handle_continue` (BEFORE any journal/restore path resolves) and
  pre-checked in `start_operative`. A class owned by ANOTHER namespace refuses
  (`{:namespace_conflict, class, owner}`); a `_system`-migrated class is
  ADOPTED (one atomic rename) by the first real claimant — that is what keeps
  a pre-namespace deployment's operatives restarting seamlessly.
  `delete_files/1` releases the claim when the class dir is gone.
- **GOTCHA — the migrator's two layouts are structurally identical**
  (`runner/class/proc` vs `ns/class/proc`), so the `.layout` marker — not
  shape — is the discriminator; without it the legacy-runner pass would
  FLATTEN `_system` on the next boot. Collision-preserved files now land
  BESIDE the winner (at the `to` path) so the namespace pass lifts them with
  their class dir; `lifted_path/2` re-points the reported paths.
- **Quotas**: `max_runners` (new) checked in `connect_handler` →
  `admit_runner` (authz + quota, refusal via `[:flezha, :runner,
  :quota_refused]`); the `:runners` counter is DERIVED from the live session
  map in `sync_directory` (never incr/decr at the several removal seams — and
  wrapped in a catch so a briefly-down QuotaCounters can't kill the
  FleetManager). `max_state_bytes` (renamed from `max_disk_bytes_total`, no
  shim): rebuilt from disk at Watchdog init, enforced at `start_operative`
  (requested=0 → refuse only when already over), latched
  `[:flezha, :state, :disk_quota_breach]` alarm in the sweep — NEVER a
  write-path kill. `max_mailbox_bytes_total` caps
  `PersistenceResolver.credit_window_bytes/2`.
- **Kafka SASL mirrors Postgres**: `resolve_auth` builds a
  `KafkaEx.Auth.Config` (string-keyed maps normalized; `sasl_password_env`
  overrides at every client (re)start); the identity carries `auth_fun` +
  `auth_key` (phash2 fingerprint) so `WorkerRegistry.canonical_key` groups
  without the secret; returned opts are redacted. An INLINE password cannot
  survive a restart from persisted opts — prefer `sasl_password_env` (same
  documented posture as Postgres).
- **Break-glass**: `flz auth rebootstrap` (offline; reuses `flz reset`'s
  `controller_is_up` probe) writes `app_data/auth/REBOOTSTRAP`;
  `Bootstrap.consume_recovery_marker/0` (first thing in `OnBoot.init`) unbinds
  every `admin@_system`, revokes every `_system` admin token (tokens CARRY
  role, so revocation is required for "no longer authorizes"), resets
  `AuthState`, emits `:rebootstrap_recovery`, and the ordinary
  `check_and_write` mints a fresh token.
- **Release**: `rel/env.sh.eex` has NO cookie fallback — unset
  `RELEASE_COOKIE` → `RELEASE_DISTRIBUTION=none` (loud); TLS-dist via
  `FLEZHA_DIST_TLS_OPTFILE`. The `:internal` caller source is safe because
  presenting it from outside requires BEAM eval over distribution = the
  cookie = node ownership (documented on the `Flezha.AuthZ` moduledoc).
- **GOTCHA — capture_log in this repo needs
  `Logger.configure(level: :warning)`** around the assertion: the test env's
  primary level is `:error`, which filters warnings BEFORE any capture
  handler (the `level:` option of capture_log does not help).
- **Shared test harness**: `FlezhaTest.Helper.AuthTestHelper.
  setup_auth_stores/1` (tmp auth dir nested one level so the bootstrap
  parent-mode check sees 0700, store restart, `:claim` / `:register_source` /
  `stores:` options, LIFO-ordered cleanup) is the ONE auth-store harness — 9
  suites use it; `await_platform_authz_events/3` is the ring-buffer poller.
- **Tests**: `enforcement_test` (posture, transport-only identity, gating +
  audit), `quota_points_test` (runner refusal, disk rebuild/breach latch,
  credit cap), `rebootstrap_test`, `cli_manager_authz_render_test` (whole verb
  family end to end), `registry_namespace_test` (claims/adoption/conflict/
  release), `transport_auth_test` (UDS modes, opt-out warn, cookie refusal via
  rendered `env.sh.eex`, cert EKUs), `kafka_sasl_redaction_test`
  (Mimic-global fake `KafkaEx.start_link_worker` captures what the client
  REALLY received), migrator suite, `quic_test` refusal case, and
  `test/system/multi_tenant_e2e_test.exs` (two tenants, enforcement ON).
- **Docs**: `docs/operations/authn_authz.md` (operator runbook: cookie Secret,
  cert mounts, bootstrap, break-glass; flezha_market needs its ConfigMap/
  DaemonSet updated in that repo before its next deploy),
  `docs/platform_authn_authz.md` (posture + quota table),
  `docs/sdk_protocol_spec.md` (QUIC client-auth table),
  `docs/resilience/state_persistence.md` (namespaced layout), glossary,
  CHANGELOG. Upgrade is BREAKING for operators (certs/cookie/bootstrap);
  rollback = `FLEZHA_AUTHZ_ENABLED=false` + previous release (auth stores are
  additive DETS).
