defmodule Flezha.Service.RunnerManager do
  alias Flezha.Service.HubManager
  alias Flezha.Runner.FleetManager

  def get_global_runners(namespace) do
    hub_nodes = HubManager.get_hub_nodes(namespace, true)

    # Collect runners from all hub nodes
    result =
      Enum.flat_map(hub_nodes, fn node ->
        :erpc.call(node, __MODULE__, :get_local_runners, [namespace])
      end)

    {:ok, result}
  end

  def get_local_runners(namespace) do
    FleetManager.active_runners(namespace)
  end
end
