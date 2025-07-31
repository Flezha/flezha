defmodule Flezha.Service.OperatorManager do
  alias Flezha.Service.HubManager
  alias Flezha.Operator.FleetManager

  def get_global_operators(group_id) do
    hub_nodes = HubManager.get_hub_nodes(group_id, true)

    # Collect operators from all hub nodes
    result =
      Enum.flat_map(hub_nodes, fn node ->
        :erpc.call(node, __MODULE__, :get_local_operators, [group_id])
      end)

    {:ok, result}
  end

  def get_local_operators(group_id) do
    FleetManager.active_operators(group_id)
  end
end
