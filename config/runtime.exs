import Config

external_processes =
  if config_env() in [:test] do
    {:ok, current_path} = :file.get_cwd()

    # `--quiet` suppresses the `Finished dev profile [...] / Running [...]`
    # banners cargo emits each time the runner is re-spawned for
    # an ad-hoc test namespace. Build cache hits still keep these calls
    # essentially free; only the stdout noise is gone.
    [
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_runner/Cargo.toml --target-dir=./target",
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_runner/Cargo.toml --target-dir=./target"
    ]
  else
    System.get_env("FLEZHA_EXTERNAL_PROCESSES", "") |> String.split(",", trim: true)
  end

config :flezha,
  # ------------------------------------------------------------------------------------------------
  # Base cluster configurations.

  # Define the cluster handler module.
  #
  # Available options:
  # - "manual_discovery" (default)
  # - "k8s_pod_discovery"
  cluster_discovery: System.get_env("FLEZHA_CLUSTER_DISCOVERY", "manual_discovery"),

  # The reconnect interval in milliseconds.
  reconnect_interval: System.get_env("FLEZHA_RECONNECT_INTERVAL", "1000") |> String.to_integer(),

  # The maximum number of jittered reconnect attempts before falling back to the
  # periodic discovery sweep. Greater than 1 so a single transient failure does
  # not strand a peer until the next sweep.
  max_reconnects: System.get_env("FLEZHA_MAX_RECONNECTS", "5") |> String.to_integer(),

  # Upper bound for the exponential, jittered reconnect backoff (base is
  # `reconnect_interval`). Caps how far apart late reconnect attempts spread.
  reconnect_max_backoff_ms:
    System.get_env("FLEZHA_RECONNECT_MAX_BACKOFF_MS", "30000") |> String.to_integer(),

  # The cluster update interval in milliseconds.
  cluster_update_interval:
    System.get_env("FLEZHA_CLUSTER_UPDATE_INTERVAL", "5000") |> String.to_integer(),

  # Bounded "degrade to solo" window for cluster formation: the Controller waits
  # this long for discovered peers to connect before declaring the cluster ready
  # and allowing hubs to be created. A genuinely single-node deployment still
  # boots after this elapses. See `Flezha.Cluster.Orchestrator.cluster_ready?/0`.
  cluster_formation_timeout_ms:
    System.get_env("FLEZHA_CLUSTER_FORMATION_TIMEOUT_MS", "15000") |> String.to_integer(),

  # The node base name that is to generate the full name including the host address.
  node_base_name: System.get_env("FLEZHA_NODE_NAME", "flezha"),

  # The node host name. For Kubernetes, it is the Pod IP.
  node_host_name: System.get_env("FLEZHA_NODE_HOST", "127.0.0.1"),

  # Number of concurrent channels to use for unix domain sockets.
  concurrent_channel_count:
    System.get_env("FLEZHA_CONCURRENT_CHANNEL_COUNT", "0") |> String.to_integer(),

  # ------------------------------------------------------------------------------------------------
  # Manual cluster discovery.

  # The initial nodes to connect to.
  initial_nodes:
    System.get_env("FLEZHA_INITIAL_NODES", "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom/1),

  # ------------------------------------------------------------------------------------------------
  # Kubernetes settings.

  # The path to the Kubernetes service account directory.
  k8s_service_account_path:
    System.get_env(
      "FLEZHA_K8S_SERVICE_ACCOUNT_PATH",
      "/var/run/secrets/kubernetes.io/serviceaccount"
    ),

  # The Kubernetes cluster name.
  k8s_cluster_name: System.get_env("FLEZHA_K8S_CLUSTER_NAME", "cluster"),

  # The Kubernetes cluster domain.
  k8s_cluster_domain:
    System.get_env(
      "FLEZHA_K8S_CLUSTER_DOMAIN",
      System.get_env("FLEZHA_K8S_CLUSTER_NAME", "")
    ),

  # The host identifier can be either `"ip"` or `"hostname"` or `"dns"`.
  k8s_host_identifier: System.get_env("FLEZHA_K8S_HOST_IDENTIFIER", "ip") |> String.to_atom(),

  # The port Kubernetes API server is listening on.
  k8s_master_port: System.get_env("FLEZHA_K8S_MASTER_PORT", "6443") |> String.to_integer(),

  # The Kubernetes control plane host IP or DNS name.
  # For other than IP identifier, it is usually the DNS name such as: `kubernetes.default.svc`.
  k8s_master_name: System.get_env("FLEZHA_K8S_MASTER_NAME", ""),

  # The Kubernetes namespace.
  k8s_namespace: System.get_env("FLEZHA_K8S_NAMESPACE", "flezha"),

  # The PodIP lookup mode can be either `"endpoints"` or `"pods"`.
  k8s_ip_lookup_mode: System.get_env("FLEZHA_K8S_IP_LOOKUP_MODE", "pods") |> String.to_atom(),

  # The Kubernetes service name.
  k8s_service_name: System.get_env("FLEZHA_K8S_SERVICE_NAME", "flezha"),

  # The Kubernetes selector.
  k8s_selector: System.get_env("FLEZHA_K8S_SELECTOR", "app=flezha"),

  # ------------------------------------------------------------------------------------------------
  # Transport configurations.
  #
  # Available options: "uds" (Unix Domain Sockets, default), "quic"
  transport: System.get_env("FLEZHA_TRANSPORT", "uds") |> String.to_atom(),

  # List of transports to enable simultaneously.
  # In test mode, both UDS and QUIC are enabled by default so all tests run with `mix test`.
  # Can be overridden with FLEZHA_TRANSPORTS=uds,quic
  transports:
    (case {System.get_env("FLEZHA_TRANSPORTS"), config_env()} do
       {nil, :test} ->
         [:uds, :quic]

       {nil, _} ->
         [System.get_env("FLEZHA_TRANSPORT", "uds") |> String.to_atom()]

       {val, _} ->
         val |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)
     end),

  # QUIC transport settings.
  quic_listen_address: System.get_env("FLEZHA_QUIC_ADDRESS", "0.0.0.0:4433"),
  ca_cert_path: System.get_env("FLEZHA_CA_CERT_PATH", "app_data/certs/ca.pem"),
  server_cert_path: System.get_env("FLEZHA_SERVER_CERT_PATH", "app_data/certs/server.pem"),
  server_key_path: System.get_env("FLEZHA_SERVER_KEY_PATH", "app_data/certs/server.key"),

  # ------------------------------------------------------------------------------------------------
  # Socket configurations.
  socket_files_path: System.get_env("FLEZHA_SOCKET_FILES_PATH", "app_data/sockets/"),

  # ------------------------------------------------------------------------------------------------
  # Configuration for external processes.
  external_processes: external_processes,

  # ------------------------------------------------------------------------------------------------
  # Installable plugin subsystem — staged rollout lever. See
  # `docs/plugin_installation.md` for the full design.
  #
  # - `FLEZHA_PLUGIN_INSTALLER_ENABLED=true` flips the feature on in this
  #   environment without a code change — used by staging to validate the
  #   subsystem before flipping the compiled-in default.
  # - `FLEZHA_PLUGIN_ROOT` points staging/prod at a persistent volume
  #   (defaults to `app_data/plugins`).
  # - `FLEZHA_PLUGIN_REQUIRE_SIGNATURE=true` forces every install to
  #   carry a valid Ed25519 signature against `FLEZHA_PLUGIN_TRUSTED_KEYS`
  #   (comma-separated PEM files on disk).
  plugin_installer_enabled:
    Flezha.Plugin.Installer.RuntimeConfig.resolve_enabled(
      System.get_env(),
      Application.get_env(:flezha, :plugin_installer_enabled, false)
    ),

  # ------------------------------------------------------------------------------------------------
  # The minimum number of milliseconds to wait before removing a disconnected runner.
  #
  # This option can be used to prevent the runner from being removed too quickly
  # when it is disconnected from the cluster. For example, if the runner is
  # restarted or updated.
  #
  # Leave 0 to disable this feature.
  #
  # The default is 30 seconds.
  runner_keepalive: System.get_env("FLEZHA_RUNNER_KEEPALIVE", "30000") |> String.to_integer(),

  # ------------------------------------------------------------------------------------------------
  # Control-plane liveness watchdog (unify-executor-liveness-and-identity).
  #
  # `Flezha.Runner.ControlChannel` stamps `last_seen` on every inbound control
  # frame — including the SDK's 15 s keepalive ping — and a jittered watchdog
  # treats sustained silence past `window_ms` as a connection close, closing the
  # UDS wedged-but-connected blind spot and making UDS symmetric with QUIC's
  # explicit idle timeout. The window is generous (3× the keepalive) so a single
  # missed ping or a brief GC/contention pause never evicts a healthy runner.
  # Pair `window_ms` with the QUIC `max_idle_timeout` (FLEZHA_CONTROL_IDLE_TIMEOUT_MS)
  # so both transports declare death in the same window. See
  # `docs/resilience/transport_liveness.md`.
  control_liveness_window_ms:
    System.get_env("FLEZHA_CONTROL_LIVENESS_WINDOW_MS", "45000") |> String.to_integer(),
  control_liveness_interval_ms:
    System.get_env("FLEZHA_CONTROL_LIVENESS_INTERVAL_MS", "15000") |> String.to_integer(),

  # ------------------------------------------------------------------------------------------------
  # Graceful shutdown (controller-graceful-shutdown).
  #
  # When enabled, the controller installs SIGTERM/SIGINT handlers and runs
  # a three-phase shutdown (quiesce → drain → finalise) bounded by
  # `graceful_shutdown_timeout_ms`. See `docs/operations/graceful_shutdown.md`.
  #
  # Truthy: 1, true, yes, on (case-insensitive). Falsy: 0, false, no, off.
  graceful_shutdown_enabled:
    (case System.get_env("FLEZHA_GRACEFUL_SHUTDOWN_ENABLED") do
       nil -> true
       val -> String.downcase(val) in ["1", "true", "yes", "on"]
     end),
  graceful_shutdown_timeout_ms:
    System.get_env("FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS", "60000") |> String.to_integer(),

  # ------------------------------------------------------------------------------------------------
  # Migration consent (processhub-deferred-migration).
  #
  # Gates ProcessHub redistribution behind the Overwatch busy check: a
  # mid-critical-work operative defers its migration and is retried every
  # `retry_interval_ms` until ready, force-migrated after `max_defer_ms`.
  # `max_defer_ms` nil = derive from `graceful_shutdown_timeout_ms`.
  migration_consent: [
    consent_timeout_ms:
      System.get_env("FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS", "5000") |> String.to_integer(),
    retry_interval_ms:
      System.get_env("FLEZHA_MIGRATION_RETRY_INTERVAL_MS", "2000") |> String.to_integer(),
    max_defer_ms:
      case System.get_env("FLEZHA_MIGRATION_MAX_DEFER_MS") do
        nil -> nil
        val -> String.to_integer(val)
      end
  ],

  # ------------------------------------------------------------------------------------------------
  # Observability foundation (platform-observability-foundation).
  #
  # `enabled` is the master flag — when false the supervisor starts no
  # children (no sinks, no logger handler, no telemetry bridge). The
  # service API returns `{:error, :observability_disabled}` in that
  # mode. See `docs/observability_foundation.md`.
  observability: [
    enabled:
      case System.get_env("FLEZHA_OBSERVABILITY_ENABLED") do
        nil -> true
        val -> String.downcase(val) in ["1", "true", "yes", "on"]
      end,
    ring_buffer_retention_ms: 900_000,
    ring_buffer_max_entries_per_source: 10_000,
    overflow_policy: :drop,
    cardinality_budget_per_source: 1_000,
    pubsub_sink_enabled: true,
    # Data-plane liveness instrumentation: always-on atomic counters plus the
    # ~1s driver poll + per-tick stall classifier. Cheap enough to leave on;
    # set FLEZHA_DATA_PLANE_LIVENESS=false to drop the poll + classifier.
    data_plane_liveness:
      System.get_env("FLEZHA_DATA_PLANE_LIVENESS", "true") in ["1", "true", "yes", "on"]
  ],
  # ------------------------------------------------------------------------------------------------
  # Per-Segment state persistence — compaction triggers (snapshot-baseline-durability).
  #
  # Size: a Segment whose un-snapshotted WAL exceeds this is compacted.
  # This is the global default; a class that declares `compact_after_bytes()`
  # in its SDK ships it in the `AddOperative` metadata, and the Watchdog
  # resolves that per-class value (via `ClassRegistry`) ahead of this default.
  # An operator `:compact_after_bytes => %{class => bytes}` override wins over
  # both.
  # Age: a Segment whose oldest un-snapshotted WAL record is older than
  # this is compacted even if it never reaches the size threshold, so
  # low-throughput Operations still get periodic baselines. `0` disables
  # the age trigger (default in :test so the sweep never surprises
  # long-lived test runners; dedicated tests opt in via put_env).
  default_compact_after_bytes:
    System.get_env("FLEZHA_COMPACT_AFTER_BYTES", "8388608") |> String.to_integer(),
  compact_after_age_ms:
    System.get_env(
      "FLEZHA_COMPACT_AFTER_AGE_MS",
      if(config_env() == :test, do: "0", else: "300000")
    )
    |> String.to_integer(),
  # Input-log retention floor: how long a consumed input is held before
  # truncation may remove it. `0` truncates at the min-watermark.
  #
  # This is a FLOOR, not the whole answer: an operative that declares a
  # `correction_window` derives its own hold window from it (the greater of the
  # two wins), so correction works without touching this. Raise it only to hold
  # MORE than every declaration asks for — e.g. to give an operator-driven
  # `flz operative replay` room to reach further back than any automatic
  # correction would.
  input_log_hold_window_ms:
    System.get_env("FLEZHA_INPUT_LOG_HOLD_WINDOW_MS", "0") |> String.to_integer(),
  # Credit-grant flow control on the runner→controller state stream.
  # Kill switch: `false` skips the CreditWindowInit offer entirely, reverting
  # every session to the legacy SegmentMailboxStatus + shed path.
  credit_flow_control_enabled:
    System.get_env("FLEZHA_CREDIT_FLOW_CONTROL", "true") in ["1", "true", "yes", "on"],
  # ------------------------------------------------------------------------------------------------
  # Durable audit log (platform-audit-log).
  #
  # Operator opt-in: default OFF. When enabled the audit-log subsystem
  # subscribes to a configurable allow-list of `(source, name)` pairs
  # on the foundation bus and writes one JSON Lines record per
  # accepted signal to `:dir/audit-current.jsonl`. See
  # `docs/audit_log.md` for the on-disk layout, rotation/retention
  # policies, and CLI verbs.
  audit_log: [
    enabled:
      case System.get_env("FLEZHA_AUDIT_ENABLED") do
        nil -> false
        val -> String.downcase(val) in ["1", "true", "yes", "on"]
      end,
    dir: System.get_env("FLEZHA_AUDIT_DIR", "app_data/audit"),
    max_file_bytes: 67_108_864,
    max_file_age_seconds: 86_400,
    retention_seconds:
      case System.get_env("FLEZHA_AUDIT_RETENTION_DAYS") do
        nil -> 30 * 86_400
        val -> String.to_integer(val) * 86_400
      end,
    max_total_bytes: 4_294_967_296,
    fsync_mode: :per_record,
    fsync_interval_ms: 100,
    rotation_check_interval_ms: 60_000,
    retention_check_interval_ms: 3_600_000,
    extra_subscriptions: []
  ],
  # ------------------------------------------------------------------------------------------------
  # Dead-letter queue (platform-dead-letter-queue): durable store for
  # retry-exhausted poison payloads. Retention deletes already-parked data,
  # so the caps default generous (30 d / 4 GiB, audit-log precedent).
  dlq: [
    dir: System.get_env("FLEZHA_DLQ_DIR", "app_data/dlq"),
    retention_seconds:
      case System.get_env("FLEZHA_DLQ_RETENTION_DAYS") do
        nil -> 30 * 86_400
        val -> String.to_integer(val) * 86_400
      end,
    max_total_bytes: 4_294_967_296,
    retention_check_interval_ms: 3_600_000
  ],
  # ------------------------------------------------------------------------------------------------
  # Authentication and authorization (platform-authn-authz).
  #
  # `enabled` defaults `false` so upgrades do not brick existing
  # deployments. The flag flips durably to `true` on successful
  # `flz auth bootstrap`; once flipped it remains `true` across
  # restarts. The env-var `FLEZHA_AUTHZ_ENABLED=false` is an
  # explicit opt-out and emits a `warn` on every boot.
  # See `docs/platform_authn_authz.md`.
  authn_authz: [
    enabled:
      case System.get_env("FLEZHA_AUTHZ_ENABLED") do
        nil -> false
        val -> String.downcase(val) in ["1", "true", "yes", "on"]
      end,
    auth_dir: System.get_env("FLEZHA_AUTHZ_DIR", "app_data/auth"),
    bootstrap_token_path:
      System.get_env(
        "FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH",
        Path.join(System.get_env("FLEZHA_AUTHZ_DIR", "app_data/auth"), "bootstrap.token")
      ),
    token_ttl_default_seconds: 86_400,
    sdk_token_ttl_seconds: 2_592_000,
    namespaces: %{}
  ]

# ------------------------------------------------------------------------------------------------
# OTP backend + export config (platform-observability-otp-and-export).
#
# Skipped in :test so test fixtures can disable the built-in pollers
# and exporters from `config/test.exs`. Both exporters are OFF by
# default in prod; turning OTLP on adopts the :block overflow policy
# (producer back-pressure during OTLP outages, bounded by the
# configured retry window). See `docs/observability_export.md`.
if config_env() != :test do
  config :flezha, :observability,
    otp_backend_enabled: true,
    otp_backend: [
      poll_intervals: %{
        beam_vm: 1_000,
        host: 5_000,
        processhub: 1_000,
        flezha_transport: 1_000,
        flezha_bounded_mailbox: 1_000
      }
    ]

  prometheus_enabled? =
    case System.get_env("FLEZHA_PROMETHEUS_ENABLED") do
      nil -> false
      val -> String.downcase(val) in ["1", "true", "yes", "on"]
    end

  otlp_enabled? =
    case System.get_env("FLEZHA_OTLP_ENABLED") do
      nil -> false
      val -> String.downcase(val) in ["1", "true", "yes", "on"]
    end

  config :flezha,
         :observability_export,
         prometheus_enabled: prometheus_enabled?,
         prometheus_port: System.get_env("FLEZHA_PROMETHEUS_PORT", "9568") |> String.to_integer(),
         prometheus_bind: System.get_env("FLEZHA_PROMETHEUS_BIND", "127.0.0.1"),
         otlp_enabled: otlp_enabled?,
         otlp_endpoint: System.get_env("FLEZHA_OTLP_ENDPOINT"),
         otlp_headers: %{},
         otlp_flush_interval_ms: 10_000,
         otlp_batch_max: 512
end

# ------------------------------------------------------------------------------------------------
# Installable plugin installer keyword list (env-var driven). Only set when
# any of the FLEZHA_PLUGIN_* vars are present so the compile-time defaults
# from `config/config.exs` win otherwise. The parse logic lives in
# `Flezha.Plugin.Installer.RuntimeConfig` so it stays unit-testable.
plugin_env_map = System.get_env()

if Flezha.Plugin.Installer.RuntimeConfig.any_env_var_present?(plugin_env_map) do
  base = Application.get_env(:flezha, :plugin_installer, [])

  config :flezha,
         :plugin_installer,
         Flezha.Plugin.Installer.RuntimeConfig.resolve_plugin_installer(plugin_env_map, base)
end

# ------------------------------------------------------------------------------------------------
# ProcessHub durable-registry + auto-recovery (auto-restore-on-boot).
#
# Skipped in :test so test fixtures can disable auto-recovery without
# being clobbered. The config compiled into config/test.exs wins instead.
if config_env() != :test do
  config :flezha,
    process_hub_registry_root:
      System.get_env("FLEZHA_PROCESS_HUB_REGISTRY_ROOT", "app_data/process_hub"),
    process_hub_recovery_marker_path: System.get_env("FLEZHA_PROCESS_HUB_RECOVERY_MARKER_PATH"),
    process_hub_recovery_timeout_ms:
      System.get_env("FLEZHA_PROCESS_HUB_RECOVERY_TIMEOUT_MS", "30000")
      |> String.to_integer(),
    recovery_window_ms:
      System.get_env("FLEZHA_RECOVERY_WINDOW_MS", "10000") |> String.to_integer(),
    boot_replay_timeout_ms:
      System.get_env("FLEZHA_BOOT_REPLAY_TIMEOUT_MS", "60000") |> String.to_integer(),
    auto_restore_enabled: System.get_env("FLEZHA_AUTO_RESTORE_ENABLED", "true") in ["true", "1"],
    # Per-Segment output-stall + compaction-timeout watchdog cadence
    # (Flezha.State.Watchdog holds its own timer after
    # `consume-recovery-mode-and-self-heal` §4 retired the
    # Flezha.Reconciler.Loop periodic sweep that previously drove it).
    output_watchdog_interval_ms:
      System.get_env("FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS", "10000") |> String.to_integer(),
    time_travel_retention_check_interval_ms:
      System.get_env("FLEZHA_RETENTION_CHECK_INTERVAL_MS", "60000") |> String.to_integer(),
    # How long a finished bounded operative's data (retained Segment state +
    # completion record) is kept before the retention sweeper reclaims it.
    default_bounded_completed_retention_hours:
      System.get_env("FLEZHA_BOUNDED_RETENTION_HOURS", "72") |> String.to_integer(),
    # The single convergence Loop. `:tick_ms` is the per-operative decision
    # cadence; `:output_stall_grace_ms` is the minimum bound stability
    # window before an output-stall triggers a rebind.
    convergence_health_enabled:
      System.get_env("FLEZHA_USE_CONVERGENCE_HEALTH", "true") in ["true", "1"],
    convergence_tick_ms:
      System.get_env("FLEZHA_CONVERGENCE_TICK_MS", "5000") |> String.to_integer(),
    convergence_output_stall_grace_ms:
      System.get_env("FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS", "60000")
      |> String.to_integer(),
    # Grace before declared-but-unbound operatives flip /healthz to degraded.
    convergence_unmet_intent_grace_ms:
      System.get_env("FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS", "120000")
      |> String.to_integer(),
    convergence_plugin_strike_threshold:
      System.get_env("FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD", "2")
      |> String.to_integer(),
    # A tick slower than this counts as a latency breach; ≥ 3 consecutive
    # breaches (or ticks stopping) flips /healthz to 503 (proposal §2).
    convergence_tick_latency_threshold_ms:
      System.get_env("FLEZHA_CONVERGENCE_TICK_LATENCY_THRESHOLD_MS", "1000")
      |> String.to_integer(),
    # /healthz listener — default bind 0.0.0.0 so a kubelet httpGet probe reaches the pod IP.
    healthz_enabled: System.get_env("FLEZHA_HEALTHZ_ENABLED", "false") in ["true", "1"],
    healthz_port: System.get_env("FLEZHA_HEALTHZ_PORT", "9569") |> String.to_integer(),
    healthz_bind: System.get_env("FLEZHA_HEALTHZ_BIND", "0.0.0.0")
end

# ------------------------------------------------------------------------------------------------
# Logger configuration.

config :logger, :file_log,
  path: System.get_env("FLEZHA_LOG_PATH", "logs/flezha.log"),
  level: System.get_env("FLEZHA_LOG_LEVEL", "info") |> String.to_atom(),
  rotate: %{
    max_bytes: System.get_env("FLEZHA_LOG_MAX_BYTES", "10485760") |> String.to_integer(),
    keep: System.get_env("FLEZHA_LOG_KEEP_FILES", "5") |> String.to_integer()
  }
