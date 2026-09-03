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

# Transport whitelist. Kept a local closure — never a project module — because
# runtime.exs is evaluated before lib/ compiles on a cold checkout. `native` is
# the in-node BEAM SDK transport; it is accepted here but never a default.
transport_atom! = fn value ->
  case value do
    v when v in ["uds", "quic", "native"] ->
      String.to_atom(v)

    other ->
      raise "FLEZHA_TRANSPORT/FLEZHA_TRANSPORTS: unknown transport #{inspect(other)}; " <>
              "expected \"uds\", \"quic\" or \"native\""
  end
end

# Runner-only boot: a standalone SDK node depends on flezha as a library without
# becoming a controller. Default true — every existing deployment is unchanged.
controller_enabled? =
  case System.get_env("FLEZHA_CONTROLLER_ENABLED") do
    nil -> true
    val -> String.downcase(val) not in ["false", "0", "no", "off"]
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
  # Available options: "uds" (Unix Domain Sockets, default), "quic", "native"
  # (in-node BEAM SDK; opt-in only, never a default). Unknown values are
  # refused at boot rather than silently turned into an atom.
  transport: transport_atom!.(System.get_env("FLEZHA_TRANSPORT", "uds")),

  # Runner-only boot gate (see the closure above). When false,
  # Flezha.Application.start/2 starts no controller supervision tree.
  controller_enabled: controller_enabled?,

  # List of transports to enable simultaneously.
  # In test mode, both UDS and QUIC are enabled by default so all tests run with `mix test`.
  # Can be overridden with FLEZHA_TRANSPORTS=uds,quic
  transports:
    (case {System.get_env("FLEZHA_TRANSPORTS"), config_env()} do
       {nil, :test} ->
         [:uds, :quic]

       {nil, _} ->
         [transport_atom!.(System.get_env("FLEZHA_TRANSPORT", "uds"))]

       {val, _} ->
         val
         |> String.split(",", trim: true)
         |> Enum.map(transport_atom!)
     end),

  # QUIC transport settings.
  quic_listen_address: System.get_env("FLEZHA_QUIC_LISTEN_ADDRESS", "0.0.0.0:4433"),
  ca_cert_path: System.get_env("FLEZHA_CA_CERT_PATH", "app_data/certs/ca.pem"),
  server_cert_path: System.get_env("FLEZHA_SERVER_CERT_PATH", "app_data/certs/server.pem"),
  server_key_path: System.get_env("FLEZHA_SERVER_KEY_PATH", "app_data/certs/server.key"),
  client_cert_path: System.get_env("FLEZHA_CLIENT_CERT_PATH", "app_data/certs/client.pem"),
  client_key_path: System.get_env("FLEZHA_CLIENT_KEY_PATH", "app_data/certs/client.key"),

  # Peer authentication posture (finish-platform-authn-authz): required by
  # default; FLEZHA_TRANSPORT_AUTH=off is the explicit wide-open opt-out and
  # is warned on every boot while set.
  transport_auth_required:
    (case System.get_env("FLEZHA_TRANSPORT_AUTH") do
       nil -> true
       val -> String.downcase(val) not in ["off", "false", "0", "no"]
     end),

  # Trust anchor for QUIC client-certificate verification.
  quic_client_ca_path: System.get_env("FLEZHA_QUIC_CLIENT_CA", "app_data/certs/ca.pem"),

  # ------------------------------------------------------------------------------------------------
  # Socket configurations.
  #
  # `:socket_files_path` is deliberately absent: the UDS socket directory is
  # resolved by the ladder every SDK shares (`FLEZHA_SOCKET_FILES_PATH`, then
  # `$XDG_RUNTIME_DIR/flezha`, `/run/flezha`, `app_data/sockets`) at boot, in
  # `Flezha.Socket.Support`. Setting the key here would make this file a second
  # reader of a variable the ladder already owns.

  # ------------------------------------------------------------------------------------------------
  # Configuration for external processes.
  external_processes: external_processes,

  # ------------------------------------------------------------------------------------------------
  # Installable plugin subsystem — staged rollout lever. See
  # `docs/plugins/installation.md` for the full design.
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
  runner_retiring_mark_ttl_ms:
    System.get_env("FLEZHA_RUNNER_RETIRING_MARK_TTL_MS", "30000") |> String.to_integer(),

  # ------------------------------------------------------------------------------------------------
  # Control-plane liveness watchdog (runner-controller-connection-healing).
  #
  # `Flezha.Runner.ControlChannel` stamps `last_seen` on every inbound control
  # frame — including the SDK's 15 s keepalive ping — and a jittered watchdog
  # treats sustained silence past `window_ms` as a connection close, closing the
  # UDS wedged-but-connected blind spot and making UDS symmetric with QUIC's
  # explicit idle timeout. The window is generous (3× the keepalive) so a single
  # missed ping or a brief GC/contention pause never evicts a healthy runner.
  # Pair `window_ms` with the QUIC `max_idle_timeout` (FLEZHA_CONTROL_IDLE_TIMEOUT_MS)
  # so both transports declare death in the same window. See
  # `docs/internals/transport_liveness.md`.
  control_liveness_window_ms:
    System.get_env("FLEZHA_CONTROL_LIVENESS_WINDOW_MS", "45000") |> String.to_integer(),
  control_liveness_interval_ms:
    System.get_env("FLEZHA_CONTROL_LIVENESS_INTERVAL_MS", "15000") |> String.to_integer(),

  # How long an Overwatch waits for the end-to-end `:ping_runner` round-trip it
  # takes before `:start_operative`. It bounds a proof, not a data path: a
  # runner that has not answered within the window is retried under the
  # bounded-retry governor rather than cut.
  overwatch_ping_timeout_ms:
    System.get_env("FLEZHA_OVERWATCH_PING_TIMEOUT_MS", "2000") |> String.to_integer(),

  # ------------------------------------------------------------------------------------------------
  # Graceful shutdown (controller-graceful-shutdown).
  #
  # When enabled, the controller installs SIGTERM/SIGINT handlers and runs
  # a three-phase shutdown (quiesce → drain → finalise) bounded by
  # `graceful_shutdown_timeout_ms`. See `docs/operations/graceful_shutdown.md`.
  #
  # Truthy: 1, true, yes, on (case-insensitive). Falsy: 0, false, no, off.
  # Off by default under `:test` so the suite's own BEAM keeps kernel's
  # signal handling and `Application.stop/1` at teardown stays fast — the
  # intent `config/test.exs` documents, which an unconditional `true` here
  # silently defeated. An explicit env var still wins in every environment.
  graceful_shutdown_enabled:
    (case System.get_env("FLEZHA_GRACEFUL_SHUTDOWN_ENABLED") do
       nil -> config_env() != :test
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
  # mode. See `docs/operations/observability.md`.
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
  # Per-Entity state persistence — compaction triggers (snapshot-baseline-durability).
  #
  # Size: an Entity whose un-snapshotted WAL exceeds this is compacted.
  # This is the global default; a class that declares `compact_after_bytes()`
  # in its SDK ships it in the `AddOperative` metadata, and the Watchdog
  # resolves that per-class value (via `ClassRegistry`) ahead of this default.
  # An operator `:compact_after_bytes => %{class => bytes}` override wins over
  # both.
  # Age: an Entity whose oldest un-snapshotted WAL record is older than
  # this is compacted even if it never reaches the size threshold, so
  # low-throughput Procedures still get periodic baselines. `0` disables
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
  # every session to the legacy EntityMailboxStatus + shed path.
  credit_flow_control_enabled:
    System.get_env("FLEZHA_CREDIT_FLOW_CONTROL", "true") in ["1", "true", "yes", "on"],
  # ------------------------------------------------------------------------------------------------
  # Remote durability tier (remote-durability-tier): ships immutable artifacts —
  # compaction snapshots, seq-addressed WAL record bundles, proc-journal and
  # inbox suffixes, the input log, the ProcessHub registry cspec — to a remote
  # backend so a controller that dies WITH its disk is recoverable.
  #
  # `backend: :none` wires nothing into the supervision tree and leaves every ack
  # path gating on local fsync exactly as before — but in production it has to be
  # asked for. An unset variable resolves to `:unset`, which
  # `DurabilityTier.children/0` refuses to boot on; dev and test keep defaulting
  # to local-only so the development loop is untouched.
  durability_tier: [
    backend:
      case System.get_env("FLEZHA_DURABILITY_BACKEND") do
        nil ->
          if config_env() == :prod, do: :unset, else: :none

        value ->
          case String.downcase(value) do
            "postgres" -> :postgres
            "s3" -> :s3
            "local_path" -> :local_path
            _ -> :none
          end
      end,
    # Backstop for entities that reach no compaction / checkpoint / commit
    # boundary — the bound on how stale a quiet entity's remote copy gets.
    ship_interval_ms:
      System.get_env("FLEZHA_DURABILITY_SHIP_INTERVAL_MS", "30000") |> String.to_integer(),
    # Time since the last successful ship at which the RPO alarm fires. Two
    # sweeps' worth by default, so one missed sweep is not an alarm.
    rpo_target_ms:
      System.get_env("FLEZHA_DURABILITY_RPO_TARGET_MS", "120000") |> String.to_integer(),
    # Orphan-blob collection: blobs no live manifest references (torn uploads,
    # superseded ranges). Never touches a referenced key.
    gc_interval_ms:
      System.get_env("FLEZHA_DURABILITY_GC_INTERVAL_MS", "3600000") |> String.to_integer(),
    # Never collect an artifact younger than this, whoever owns it — an upload
    # still in flight on a peer controller is young by definition, so this is
    # what makes collection safe without coordinating between nodes.
    gc_grace_ms:
      System.get_env("FLEZHA_DURABILITY_GC_GRACE_MS", "3600000") |> String.to_integer(),
    # Collect even outside this node's scope past this age, so a reference no
    # node holds any more does not leak forever. Deliberately far longer than
    # the grace.
    gc_abandoned_after_ms:
      System.get_env("FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS", "604800000")
      |> String.to_integer(),
    backend_opts: [
      # Postgres backend (`postgrex`, already in tree for the Postgres sink).
      hostname: System.get_env("FLEZHA_DURABILITY_PG_HOST", "localhost"),
      port: System.get_env("FLEZHA_DURABILITY_PG_PORT", "5432") |> String.to_integer(),
      database: System.get_env("FLEZHA_DURABILITY_PG_DATABASE", "flezha"),
      username: System.get_env("FLEZHA_DURABILITY_PG_USERNAME", "flezha"),
      # Prefer the env-var form: the password then never lands in config,
      # worker state, or a log line.
      password_env: System.get_env("FLEZHA_DURABILITY_PG_PASSWORD_ENV"),
      ssl: System.get_env("FLEZHA_DURABILITY_PG_SSL", "false") in ["1", "true", "yes", "on"],
      table: System.get_env("FLEZHA_DURABILITY_PG_TABLE", "flezha_durability_artifacts"),
      auto_create:
        System.get_env("FLEZHA_DURABILITY_PG_AUTO_CREATE", "true") in ["1", "true", "yes", "on"],
      # S3-compatible backend (hand-rolled sigv4 over `:mint`).
      endpoint: System.get_env("FLEZHA_DURABILITY_S3_ENDPOINT"),
      bucket: System.get_env("FLEZHA_DURABILITY_S3_BUCKET"),
      region: System.get_env("FLEZHA_DURABILITY_S3_REGION", "us-east-1"),
      access_key_id: System.get_env("FLEZHA_DURABILITY_S3_ACCESS_KEY_ID"),
      secret_access_key_env: System.get_env("FLEZHA_DURABILITY_S3_SECRET_ENV"),
      prefix: System.get_env("FLEZHA_DURABILITY_S3_PREFIX", "flezha"),
      # LocalPath backend: a directory that must outlive the cluster's own
      # disks (NFS mount, externally provisioned volume) — a node-local path
      # shares the fate the tier exists to survive.
      path: System.get_env("FLEZHA_DURABILITY_PATH")
    ]
  ],
  # ------------------------------------------------------------------------------------------------
  # Durable audit log (audit-log).
  #
  # Operator opt-in: default OFF. When enabled the audit-log subsystem
  # subscribes to a configurable allow-list of `(source, name)` pairs
  # on the foundation bus and writes one JSON Lines record per
  # accepted signal to `:dir/audit-current.jsonl`. See
  # `docs/operations/audit_log.md` for the on-disk layout, rotation/retention
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
  # Dead-letter queue (dead-letter-queue): durable store for
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
  # Authentication and authorization (finish-platform-authn-authz).
  #
  # Enforcement is the DEFAULT posture in prod. Until `flz auth bootstrap`
  # is claimed, gated verbs refuse (naming the bootstrap procedure) while
  # dataflow continues — an un-bootstrapped upgrade degrades to "operator
  # verbs refused", never to open. `FLEZHA_AUTHZ_ENABLED=false` is the
  # explicit opt-out and emits a `warn` on every boot; dev/test carry the
  # opt-out in their config for ergonomics, equally warned.
  # See `docs/operations/authn_authz.md`.
  authn_authz: [
    enabled:
      case System.get_env("FLEZHA_AUTHZ_ENABLED") do
        nil -> config_env() == :prod
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
  ],
  # Scheduled workflow spawns. One job process per schedule in the
  # `:flz_schedules` hub; on start it fires the single most recent slot inside
  # `misfire_grace_ms` (covering restart and migration gaps) and drops older
  # ones — missed slots are never back-filled.
  schedules: [
    dir: System.get_env("FLEZHA_SCHEDULE_DIR", "app_data/schedules"),
    misfire_grace_ms:
      String.to_integer(System.get_env("FLEZHA_SCHEDULE_MISFIRE_GRACE_MS", "120000"))
  ]

# ------------------------------------------------------------------------------------------------
# OTP backend + export config (platform-observability-otp-and-export).
#
# Skipped in :test so test fixtures can disable the built-in pollers
# and exporters from `config/test.exs`. Both exporters are OFF by
# default in prod; turning OTLP on adopts the :block overflow policy
# (producer back-pressure during OTLP outages, bounded by the
# configured retry window). See `docs/operations/observability.md`.
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
    # Orphan-reconcile cadence. The grace also bounds how long a hub stays
    # `:recovering`, so it is what the boot gate waits on; it is set above
    # the runner handshake window because an orphan start needs a runner to
    # place against.
    process_hub_reconcile_grace_ms:
      System.get_env("FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS", "45000")
      |> String.to_integer(),
    process_hub_reconcile_interval_ms:
      System.get_env("FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS", "30000")
      |> String.to_integer(),
    boot_replay_timeout_ms:
      System.get_env("FLEZHA_BOOT_REPLAY_TIMEOUT_MS", "60000") |> String.to_integer(),
    auto_restore_enabled: System.get_env("FLEZHA_AUTO_RESTORE_ENABLED", "true") in ["true", "1"],
    # Per-Entity output-stall + compaction-timeout watchdog cadence
    # (Flezha.State.Watchdog holds its own timer after
    # `consume-recovery-mode-and-self-heal` §4 retired the
    # Flezha.Reconciler.Loop periodic sweep that previously drove it).
    output_watchdog_interval_ms:
      System.get_env("FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS", "10000") |> String.to_integer(),
    time_travel_retention_check_interval_ms:
      System.get_env("FLEZHA_RETENTION_CHECK_INTERVAL_MS", "60000") |> String.to_integer(),
    # How long a finished workflow operative's data (retained Entity state +
    # completion record) is kept before the retention sweeper reclaims it.
    default_workflow_completed_retention_hours:
      System.get_env("FLEZHA_WORKFLOW_RETENTION_HOURS", "72") |> String.to_integer(),
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
    # How long one activation may stay outstanding on a runner before the
    # classifier calls that Entity stalled. Far above the longest activation
    # the bench fleet produces on purpose: from the runner, a slow Procedure
    # and a handout its host lost are indistinguishable, so the threshold is
    # what separates them and the remedy is a rebind from durable state.
    convergence_entity_stall_ms:
      System.get_env("FLEZHA_CONVERGENCE_ENTITY_STALL_MS", "300000")
      |> String.to_integer(),
    convergence_plugin_strike_threshold:
      System.get_env("FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD", "2")
      |> String.to_integer(),
    # A tick slower than this counts as a latency breach; ≥ 3 consecutive
    # breaches (or ticks stopping) flips /healthz to 503 (proposal §2).
    convergence_tick_latency_threshold_ms:
      System.get_env("FLEZHA_CONVERGENCE_TICK_LATENCY_THRESHOLD_MS", "1000")
      |> String.to_integer(),
    # Consecutive classifier polls reporting a controller-fabric wedge
    # (`driver_loop_frozen` / `nif_queue_backing_up`) before /healthz answers
    # 503. No BEAM-level heal reaches a wedged NIF driver thread, so the
    # orchestrator's restart is the remedy; the window is what keeps a single
    # transient poll from flapping the pod. `0` disables the fabric 503 —
    # the escape hatch and the knob are the same setting.
    convergence_fabric_503_polls:
      System.get_env("FLEZHA_FABRIC_503_POLLS", "3") |> String.to_integer(),
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
