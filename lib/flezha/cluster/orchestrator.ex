defmodule Flezha.Cluster.Orchestrator do
  alias Flezha.Cluster.DiscoveryProtocol
  alias Flezha.Cluster.ManualDiscovery
  alias Flezha.Cluster.K8sPodDiscovery

  use GenServer

  @type state() :: %{
          discovery_handler: DiscoveryProtocol.t()
        }

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    state = %{
      discovery_handler: get_discovery_handler()
    }

    {:ok, state, {:continue, :connect_nodes}}
  end

  @impl true
  def handle_continue(:connect_nodes, state) do
    discovery_handler = connect_nodes(state.discovery_handler)
    schedule_cluster_update()

    {:noreply, Map.put(state, :discovery_handler, discovery_handler)}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    schedule_reconnect(node, 0)

    {:noreply, state}
  end

  @impl true
  def handle_info(:update_cluster, state) do
    discovery_handler = connect_nodes(state.discovery_handler)
    schedule_cluster_update()

    {:noreply, Map.put(state, :discovery_handler, discovery_handler)}
  end

  @impl true
  def handle_info({:scheduled_reconnect, node, retries}, state) do
    max_reconnects = Application.fetch_env!(:flezha, :max_reconnects)

    cond do
      retries >= max_reconnects ->
        # Max retries has reached, stop trying to reconnect and unmonitor the node.
        Node.monitor(node, false)

      true ->
        case Node.connect(node) do
          true ->
            Node.monitor(node, true)
            notify_recovery_peer_connected(node)

          false ->
            # Retry again.
            schedule_reconnect(node, retries + 1)
        end
    end

    {:noreply, state}
  end

  defp connect_nodes(discovery_handler) do
    {:ok, discovery_handler, to_connect} = DiscoveryProtocol.get_nodes(discovery_handler)
    connected = Node.list()

    Enum.each(to_connect, fn node ->
      if !Enum.member?(connected, node) do
        case Node.connect(node) do
          true ->
            Node.monitor(node, true)
            notify_recovery_peer_connected(node)

          false ->
            schedule_reconnect(node, 0)

          :ignored ->
            nil
        end
      end
    end)

    discovery_handler
  end

  # Forward a successful `Node.connect/1` to the recovery SysMode so it can
  # exchange SYSMODE values with the peer. The SysMode GenServer may not yet
  # be running during the earliest phase of startup — tolerate that silently
  # (peer_connected/1 itself short-circuits on :noproc).
  defp notify_recovery_peer_connected(peer_node) do
    try do
      Flezha.Recovery.SysMode.peer_connected(peer_node)
    catch
      _kind, _reason -> :ok
    end
  end

  defp schedule_reconnect(node, retries_count) do
    reconnect_interval = Application.fetch_env!(:flezha, :reconnect_interval)

    case reconnect_interval do
      0 ->
        nil

      interval ->
        Process.send_after(self(), {:scheduled_reconnect, node, retries_count}, interval)
    end
  end

  defp schedule_cluster_update() do
    interval = Application.fetch_env!(:flezha, :cluster_update_interval)

    Process.send_after(self(), :update_cluster, interval)
  end

  defp get_discovery_handler() do
    result =
      case Application.fetch_env!(:flezha, :cluster_discovery) do
        "manual_discovery" ->
          {:ok,
           %ManualDiscovery{
             initial_nodes: Application.fetch_env!(:flezha, :initial_nodes)
           }}

        "k8s_pod_discovery" ->
          {:ok,
           %K8sPodDiscovery{
             service_account_path: Application.fetch_env!(:flezha, :k8s_service_account_path),
             cluster_domain: Application.fetch_env!(:flezha, :k8s_cluster_domain),
             namespace: Application.fetch_env!(:flezha, :k8s_namespace),
             node_base_name: Application.fetch_env!(:flezha, :node_base_name),
             ip_lookup_mode: Application.fetch_env!(:flezha, :k8s_ip_lookup_mode),
             cluster_name: Application.fetch_env!(:flezha, :k8s_cluster_name),
             service_name: Application.fetch_env!(:flezha, :k8s_service_name),
             selector: Application.fetch_env!(:flezha, :k8s_selector),
             master_name: Application.fetch_env!(:flezha, :k8s_master_name),
             master_port: Application.fetch_env!(:flezha, :k8s_master_port),
             host_identifier: Application.fetch_env!(:flezha, :k8s_host_identifier)
           }}

        handler ->
          {:error, "Unknown cluster discovery handler: #{inspect(handler)}"}
      end

    case result do
      {:ok, handler} -> handler
      {:error, reason} -> raise reason
    end
  end
end
