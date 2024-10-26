import Config

external_processes =
  if config_env() in [:test, :dev] do
    {:ok, current_path} = :file.get_cwd()

    [
      "cargo run --manifest-path=#{current_path}/test/apps/rust_app/Cargo.toml --target-dir=./target"
    ]
  else
    System.get_env("FLECHA_EXTERNAL_PROCESSES", "") |> String.split(",", trim: true)
  end

config :flecha_core,
  # General cluster specific configurations.
  reconnect_interval: System.get_env("FLECHA_RECONNECT_INTERVAL", "1000") |> String.to_integer(),
  max_reconnects: System.get_env("FLECHA_MAX_RECONNECTS", "1") |> String.to_integer(),
  cluster_update_interval:
    System.get_env("FLECHA_CLUSTER_UPDATE_INTERVAL", "5000") |> String.to_integer(),

  # Define the cluster handler module.
  #
  # Available options:
  # - "manual_discovery" (default)
  # - "k8s_pod_ip_discovery"
  cluster_discovery: System.get_env("FLECHA_CLUSTER_DISCOVERY", "manual_discovery"),
  node_base_name: System.get_env("FLECHA_NODE_NAME", "flecha"),

  # Manual cluster discovery.
  initial_nodes:
    System.get_env("FLECHA_INITIAL_NODES", "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom/1),

  # Kubernetes cluster Pod IP discovery.
  k8s_node_discovery: System.get_env("FLECHA_K8S_NODE_DISCOVERY", ""),
  k8s_service_account_path:
    System.get_env(
      "FLECHA_K8S_SERVICE_ACCOUNT_PATH",
      "/var/run/secrets/kubernetes.io/serviceaccount"
    ),
  k8s_cluster_name: System.get_env("FLECHA_K8S_CLUSTER_NAME", "cluster"),
  k8s_cluster_domain:
    System.get_env(
      "FLECHA_K8S_CLUSTER_DOMAIN",
      System.get_env("FLECHA_K8S_CLUSTER_NAME", "cluster") <> ".local"
    ),
  k8s_namespace: System.get_env("FLECHA_K8S_NAMESPACE", ""),
  # The PodIP lookup mode can be either `"endpoints"` or `"pods"`.
  k8s_ip_lookup_mode:
    System.get_env("FLECHA_K8S_IP_LOOKUP_MODE", "endpoints") |> String.to_atom(),
  k8s_service_name: System.get_env("FLECHA_K8S_SERVICE_NAME", "flecha"),
  k8s_selector: System.get_env("FLECHA_K8S_SELECTOR", ""),
  k8s_master_name: System.get_env("FLECHA_K8S_MASTER_NAME", "kubernetes.default.svc"),

  # Socket configurations.
  socket_files_path: System.get_env("FLECHA_SOCKET_FILES_PATH", "app_data/sockets/"),

  # IO configurations.
  io_add_timeout: System.get_env("FLECHA_IO_ADD_TIMEOUT", "1000") |> String.to_integer(),
  io_payload_add_timeout:
    System.get_env("FLECHA_IO_PAYLOAD_ADD_TIMEOUT", "1000") |> String.to_integer(),

  # Configuration for external processes.
  external_processes: external_processes
