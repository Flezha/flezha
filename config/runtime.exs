import Config

external_processes =
  if config_env() in [:test] do
    {:ok, current_path} = :file.get_cwd()

    [
      "cargo run --manifest-path=#{current_path}/test/apps/rust_compute_engine/Cargo.toml --target-dir=./target",
      "cargo run --manifest-path=#{current_path}/test/apps/rust_compute_engine/Cargo.toml --target-dir=./target"
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

  # The maximum number of reconnects before giving up.
  max_reconnects: System.get_env("FLEZHA_MAX_RECONNECTS", "1") |> String.to_integer(),

  # The cluster update interval in milliseconds.
  cluster_update_interval:
    System.get_env("FLEZHA_CLUSTER_UPDATE_INTERVAL", "5000") |> String.to_integer(),

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
  # Failure-recovery subsystem env vars.
  recovery_window_ms:
    (case Integer.parse(System.get_env("FLEZHA_RECOVERY_WINDOW_MS", "10000")) do
       {n, ""} when n >= 0 -> n
       _ -> 10_000
     end),
  recovery_data_dir: System.get_env("FLEZHA_DATA_DIR", "app_data/recovery"),
  recovery_enabled:
    (case System.get_env("FLEZHA_RECOVERY_ENABLED") do
       nil -> Application.get_env(:flezha, :recovery_enabled, true)
       val -> String.downcase(val) in ["true", "1", "yes"]
     end),

  # When set to a factory id (e.g. "market-init-state"), `SysMode` defers
  # `Coordinator.start_recovery_async/0` until either a connected node
  # advertises that factory OR the wait timeout fires. Empty string ("")
  # reverts to the raw `recovery_window_ms` timer. See design §D7.
  recovery_wait_factory: System.get_env("FLEZHA_RECOVERY_WAIT_FACTORY", ""),
  recovery_wait_namespace: System.get_env("FLEZHA_RECOVERY_WAIT_NAMESPACE", "default"),
  recovery_wait_timeout_ms:
    (case Integer.parse(System.get_env("FLEZHA_RECOVERY_WAIT_TIMEOUT_MS", "60000")) do
       {n, ""} when n >= 0 -> n
       _ -> 60_000
     end),

  # ------------------------------------------------------------------------------------------------
  # Overwatch periodic state-snapshot task (design §D8).
  #
  # `FLEZHA_OVERWATCH_STATE_SNAPSHOT_INTERVAL_MS` gates the background timer
  # that asks each running processor for an in-memory state snapshot and
  # fanouts to `:on_snapshot` subscribers. A value of `"0"` or `"disabled"`
  # stops the timer from being scheduled — the default, since flezha_market
  # treats Postgres (populated by Kafka-consumed rows) as the authoritative
  # store and any second on-disk copy would drift.
  overwatch_state_snapshot_interval_ms:
    (case System.get_env("FLEZHA_OVERWATCH_STATE_SNAPSHOT_INTERVAL_MS", "0") do
       "disabled" ->
         0

       raw ->
         case Integer.parse(raw) do
           {n, ""} when n >= 0 -> n
           _ -> 0
         end
     end),

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
  # The minimum number of milliseconds to wait before removing a disconnected compute_engine.
  #
  # This option can be used to prevent the compute_engine from being removed too quickly
  # when it is disconnected from the cluster. For example, if the compute_engine is
  # restarted or updated.
  #
  # Leave 0 to disable this feature.
  #
  # The default is 30 seconds.
  compute_engine_keepalive:
    System.get_env("FLEZHA_OPERATOR_KEEPALIVE", "30000") |> String.to_integer()

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
# Logger configuration.

config :logger, :file_log,
  path: System.get_env("FLEZHA_LOG_PATH", "logs/flezha.log"),
  level: System.get_env("FLEZHA_LOG_LEVEL", "info") |> String.to_atom(),
  rotate: %{
    max_bytes: System.get_env("FLEZHA_LOG_MAX_BYTES", "10485760") |> String.to_integer(),
    keep: System.get_env("FLEZHA_LOG_KEEP_FILES", "5") |> String.to_integer()
  }
