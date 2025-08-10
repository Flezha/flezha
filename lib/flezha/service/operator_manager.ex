defmodule Flezha.Service.OperatorManager do
  alias Flezha.Service.HubManager
  alias Flezha.Operator.FleetManager

  def get_global_operators(namespace) do
    hub_nodes = HubManager.get_hub_nodes(namespace, true)

    # Collect operators from all hub nodes
    result =
      Enum.flat_map(hub_nodes, fn node ->
        :erpc.call(node, __MODULE__, :get_local_operators, [namespace])
      end)

    {:ok, result}
  end

  def get_local_operators(namespace) do
    FleetManager.active_operators(namespace)
  end
end
