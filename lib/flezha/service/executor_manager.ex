defmodule Flezha.Service.ExecutorManager do
  alias Flezha.Service.HubManager
  alias Flezha.Executor.FleetManager

  def get_global_executors(namespace) do
    hub_nodes = HubManager.get_hub_nodes(namespace, true)

    # Collect executors from all hub nodes
    result =
      Enum.flat_map(hub_nodes, fn node ->
        :erpc.call(node, __MODULE__, :get_local_executors, [namespace])
      end)

    {:ok, result}
  end

  def get_local_executors(namespace) do
    FleetManager.active_executors(namespace)
  end
end
