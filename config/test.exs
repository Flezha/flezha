import Config

config :flezha,
  testnet_enabled: true,
  concurrent_channel_count: 1,
  # Control-plane liveness watchdog (runner-controller-connection-healing).
  # The window MUST stay comfortably above the SDK keepalive cadence (15 s):
  # the long-lived default-namespace test runners only re-arm `last_seen` via
  # their keepalive ping, so a small global window would falsely evict them and
  # break unrelated suites. Dedicated watchdog tests drive `:liveness_tick`
  # directly (or override this via `Application.put_env/3`) for fast assertions.
  control_liveness_window_ms: 45_000,
  control_liveness_interval_ms: 5_000,
  # Graceful shutdown is off in tests so `Application.stop/1` calls
  # at teardown remain fast. Dedicated integration tests under
  # `test/integration/shutdown/` exercise it explicitly.
  graceful_shutdown_enabled: false,
  graceful_shutdown_timeout_ms: 5_000,
  # Observability foundation: PubSub sink is off by default in tests so
  # subscribers in unrelated test modules don't accidentally receive
  # cross-test signal traffic. Tests that exercise the PubSub sink flip
  # this on explicitly via `Application.put_env/3`.
  observability: [
    enabled: true,
    ring_buffer_retention_ms: 900_000,
    ring_buffer_max_entries_per_source: 10_000,
    overflow_policy: :drop,
    cardinality_budget_per_source: 1_000,
    pubsub_sink_enabled: false,
    data_plane_liveness: true,
    # Built-in pollers are off by default in tests so unrelated test
    # modules don't pick up cross-test BEAM/host samples. Tests that
    # exercise the OTP backend opt in via Application.put_env/3.
    otp_backend_enabled: false,
    otp_backend: [
      poll_intervals: %{
        beam_vm: 1_000,
        host: 5_000,
        processhub: 1_000,
        flezha_transport: 1_000,
        flezha_bounded_mailbox: 1_000
      }
    ]
  ],
  # Audit log: OFF by default in tests so no app_data/audit/ directory
  # is created. Tests that exercise the audit subsystem flip
  # `:audit_log, :enabled: true` and point `:dir` at a tmp directory.
  audit_log: [
    enabled: false,
    dir: "app_data/audit",
    max_file_bytes: 67_108_864,
    max_file_age_seconds: 86_400,
    retention_seconds: 30 * 86_400,
    max_total_bytes: 4_294_967_296,
    fsync_mode: :per_record,
    fsync_interval_ms: 100,
    rotation_check_interval_ms: 60_000,
    retention_check_interval_ms: 3_600_000,
    extra_subscriptions: []
  ],
  # Authn/authz is OFF by default in tests so unit-test isolation is
  # preserved. Tests that exercise the auth surface flip `:enabled` to
  # `true` and point `:auth_dir` at a tmp directory.
  authn_authz: [
    enabled: false,
    # Tests bring up their own AuthState/RoleStore/etc. against a
    # per-test tmp auth_dir; the global application tree must NOT
    # start a default Flezha.AuthZ.Supervisor or it would race with
    # the per-test stores on the singleton named GenServers.
    application_managed: false,
    auth_dir: "app_data/auth",
    bootstrap_token_path: "app_data/auth/bootstrap.token",
    token_ttl_default_seconds: 86_400,
    sdk_token_ttl_seconds: 2_592_000,
    namespaces: %{}
  ],
  # Schedule Store/Hub are per-test singletons against a tmp dir — the global
  # tree must not start defaults (same posture as authn_authz), and no test
  # gets a stray `:flz_schedules` hub it did not ask for.
  schedules: [
    application_managed: false,
    dir: "app_data/schedules",
    misfire_grace_ms: 120_000
  ],
  observability_export: [
    prometheus_enabled: false,
    prometheus_port: 9568,
    prometheus_bind: "127.0.0.1",
    otlp_enabled: false,
    otlp_endpoint: nil,
    otlp_headers: %{},
    otlp_flush_interval_ms: 10_000,
    otlp_batch_max: 512
  ],
  # Tests bring up their own runners and operatives — auto-recovery would
  # trigger replay against a not-yet-connected runner, cascading the
  # ProcessHub coordinator into shutdown. The persistent registry still
  # records mutations; only the at-boot replay state machine is off.
  auto_restore_enabled: false,
  # Convergence Loop OFF in test mode so global ticks do not race
  # per-test fixtures. Tests that exercise it start it explicitly.
  # (The always-on Convergence.Snapshot is unaffected by this flag.)
  convergence_health_enabled: false,
  # `/healthz` is opt-in everywhere (off in prod by default too); tests
  # that exercise it bring up the listener inline.
  healthz_enabled: false,
  # Bounded retry on top of `:start_operative` keeps test wall-clock down —
  # one short attempt is enough, and the parked-retry timer is disabled
  # so a failed startup does not periodically re-fire during a test.
  overwatch_retry_backoff_ms: [10, 10, 10],
  overwatch_retry_park_interval_ms: 0,
  overwatch_ping_timeout_ms: 500,
  # Tests that hit the real test runners leave ping enabled and rely on
  # the SDK's `Ping` handler. Tests that drive the retry/parked-state
  # paths in isolation use `Application.put_env(:flezha,
  # :overwatch_ping_fn, ...)` to inject a fake.
  overwatch_ping_enabled: true,
  process_hub_registry_root: Path.join([System.tmp_dir!(), "flezha_test_process_hub"]),
  pre_hooks: [
    fn ->
      # Wipe any stale per-test ProcessHub registry from previous runs so
      # auto-recovery never sees rows it cannot replay.
      :flezha
      |> Application.get_env(:process_hub_registry_root)
      |> case do
        nil -> :ok
        path -> File.rm_rf(path)
      end
    end,
    fn ->
      # Check if local node is running.
      if !Node.alive?() do
        # Start the local node
        {_, 0} = System.cmd("epmd", ["-daemon"])
        Node.start(:"test_agent_node1@127.0.0.1", :longnames)
      end
    end,
    fn ->
      Flezha.Service.EventManager.add_handler(:runner_connected_event)
    end,
    fn ->
      # A runtime directory, not `app_data/`, and a SHORT one: the resolved
      # socket path is absolute, and a UDS path must fit `SUN_LEN` (108 bytes) —
      # a deep checkout plus a long stream-socket name does not. The run token
      # is the same one the state root and the data-plane port derive from
      # (`config/runtime.exs`), so two suites on one checkout cannot meet here
      # either — `node()` alone is `nonode@nohost` for both of them.
      Application.put_env(
        :flezha,
        :socket_files_path,
        Path.join([
          System.tmp_dir!(),
          "flezha_sockets",
          "#{node()}-#{Application.get_env(:flezha, :run_token) || System.pid()}"
        ])
      )
    end
  ],
  post_hooks: [
    fn ->
      for _x <- 1..2 do
        receive do
          {:runner_connected_event, {"default", _}} -> :ok
        after
          20_000 -> raise "Runner not connected"
        end
      end
    end
  ]

config :logger,
  level: :error

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true
