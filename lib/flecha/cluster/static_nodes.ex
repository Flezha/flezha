defmodule Flecha.Cluster.StaticNodes do
  @behaviour Flecha.Cluster.Orchestrator

  @impl true
  def connect_nodes(orch) do
    {c_nodes, uc_nodes} =
      Enum.reduce(orch.unconnected, {orch.connected, orch.unconnected}, fn node, acc ->
        case Node.connect(node) do
          true ->
            {[node | acc.connected], List.delete(acc.unconnected, node)}

          false ->
            acc
        end
      end)

    {:ok, {:connected, c_nodes}, {:unconnected, uc_nodes}}
  end
end
