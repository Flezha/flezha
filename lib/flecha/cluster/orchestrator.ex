defmodule Flecha.Cluster.Orchestrator do
  use GenServer

  @type handler_module() :: Flecha.Cluster.StaticNodes | Flecha.Cluster.K8sPodIpDiscovery

  @type t() :: %__MODULE__{
          handler: handler_module(),
          reconnect_interval: non_neg_integer(),
          max_reconnects: non_neg_integer(),
          connected: [node()],
          unconnected: [node()]
        }
  defstruct [
    :handler,
    :reconnect_interval,
    :max_reconnects,
    connected: [],
    unconnected: []
  ]

  @callback connect_nodes(__MODULE__.t()) :: {conn_nodes :: [node()], unconn_nodes :: [node()]}

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    state = %__MODULE__{
      handler: get_handler(),
      unconnected: Application.fetch_env!(:flecha_core, :static_nodes),
      reconnect_interval: Application.fetch_env!(:flecha_core, :reconnect_interval),
      max_reconnects: Application.fetch_env!(:flecha_core, :max_reconnects)
    }

    {:ok, state, {:continue, :connect_nodes}}
  end

  @impl true
  def handle_continue(:connect_nodes, state) do
    {c_nodes, uc_nodes} = state.handler.connect_nodes(state)

    state =
      state
      |> Map.put(:connected, c_nodes)
      |> Map.put(:unconnected, uc_nodes)

    monitor_nodes(c_nodes)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    case state.reconnect_interval do
      0 -> nil
      interval -> Process.send_after(self(), {:scheduled_reconnect, {node, 0}}, interval)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:scheduled_reconnect, node, retries}, state) do
    state =
      cond do
        retries >= state.max_reconnects ->
          # Stop monitoring the node.
          Node.monitor(node, false)

          # Max retries has reached, remove the node from the list.
          Map.put(state, :unconnected, List.delete(state.unconnected, node))

        true ->
          Process.send_after(
            self(),
            {
              :scheduled_reconnect,
              {node, retries + 1}
            },
            state.reconnect_interval
          )

          state
      end

    {:noreply, state}
  end

  defp get_handler() do
    case Application.fetch_env!(:flecha_core, :cluster_handler) do
      "static_nodes" ->
        validate_handler(:static_nodes)
        Flecha.Cluster.StaticNodes

      "k8s_pod_ip_discovery" ->
        validate_handler(:k8s_pod_ip_discovery)
        Flecha.Cluster.K8sPodIpDiscovery

      handler ->
        raise "Unknown cluster handler: #{handler}"
    end
  end

  defp validate_handler(:static_nodes) do
    static_nodes = Application.fetch_env!(:flecha_core, :static_nodes)

    cond do
      is_list(static_nodes) ->
        nil

      true ->
        raise "Invalid static nodes configuration. Expected a empty list or a list of nodes."
    end
  end

  defp validate_handler(:k8s_pod_ip_discovery) do
    # TODO: Implement the validation.
  end

  defp monitor_nodes(nodes) do
    Enum.each(nodes, fn node ->
      Node.monitor(node, true)
    end)
  end
end
