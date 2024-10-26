defmodule Flecha.Cluster.StaticNodes do
  @behaviour Flecha.Cluster.Orchestrator

  @impl true
  def connect_nodes(orch) do
    {c_nodes, uc_nodes} =
      Enum.reduce(orch.unconnected, {orch.connected, orch.unconnected}, fn node, {c, uc} = acc ->
        case Node.connect(node) do
          true ->
            {[node | c], List.delete(uc, node)}

          false ->
            acc
        end
      end)

    if !is_list(c_nodes) || !is_list(uc_nodes) do
      raise "Invalid return value from connect_nodes/1"
    end

    {c_nodes, uc_nodes}
  end

  @impl true
  def update_cluster(orch) do
    {orch.connected, orch.unconnected}
  end

  @impl true
  def validate_settings(:static_nodes) do
    static_nodes = Application.fetch_env!(:flecha_core, :static_nodes)

    cond do
      is_list(static_nodes) ->
        {:ok, __MODULE__}

      true ->
        {:error, "Invalid static nodes configuration. Expected a empty list or a list of nodes."}
    end
  end
end
