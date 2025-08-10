defmodule Flezha.Service.ComputeEngineManager do
  alias Flezha.Service.HubManager
  alias Flezha.ComputeEngine.FleetManager

  def get_global_compute_engines(namespace) do
    hub_nodes = HubManager.get_hub_nodes(namespace, true)

    # Collect compute_engines from all hub nodes
    result =
      Enum.flat_map(hub_nodes, fn node ->
        :erpc.call(node, __MODULE__, :get_local_compute_engines, [namespace])
      end)

    {:ok, result}
  end

  def get_local_compute_engines(namespace) do
    FleetManager.active_compute_engines(namespace)
  end
end
