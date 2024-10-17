defmodule Flecha.Cluster.StaticNodes do
  use GenServer

  defstruct [
    :reconnect_interval,
    :max_reconnects,
    connected: [],
    unconnected: []
  ]

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    state = %__MODULE__{
      unconnected: Application.fetch_env!(:flecha_core, :static_nodes),
      reconnect_interval: Application.fetch_env!(:flecha_core, :reconnect_interval),
      max_reconnects: Application.fetch_env!(:flecha_core, :max_reconnects)
    }

    {:ok, state, {:continue, :connect_nodes}}
  end

  @impl true
  def handle_continue(:connect_nodes, state) do
    state =
      Enum.reduce(state.unconnected, state, fn node, acc ->
        case Node.connect(node) do
          true ->
            Node.monitor(node, true)

            acc
            |> Map.put(:connected, [node | acc.connected])
            |> Map.put(:unconnected, List.delete(acc.unconnected, node))

          false ->
            state
        end
      end)

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
end
