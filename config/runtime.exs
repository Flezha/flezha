import Config

external_processes =
  if config_env() in [:test, :dev] do
    {:ok, current_path} = :file.get_cwd()

    [
      "cargo run --manifest-path=#{current_path}/test/apps/rust_operator/Cargo.toml --target-dir=./target",
      "cargo run --manifest-path=#{current_path}/test/apps/rust_operator/Cargo.toml --target-dir=./target"
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
  # Socket configurations.
  socket_files_path: System.get_env("FLEZHA_SOCKET_FILES_PATH", "app_data/sockets/"),

  # ------------------------------------------------------------------------------------------------
  # Configuration for external processes.
  external_processes: external_processes,

  # ------------------------------------------------------------------------------------------------
  # The minimum number of milliseconds to wait before removing a disconnected operator.
  #
  # This option can be used to prevent the operator from being removed too quickly
  # when it is disconnected from the cluster. For example, if the operator is
  # restarted or updated.
  #
  # Leave 0 to disable this feature.
  #
  # The default is 30 seconds.
  operator_keepalive: System.get_env("FLEZHA_OPERATOR_KEEPALIVE", "30000") |> String.to_integer()
