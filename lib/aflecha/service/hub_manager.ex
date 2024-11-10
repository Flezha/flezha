defmodule Aflecha.Service.HubManager do
  alias Aflecha.Service.Identificator

  def hook_insert_pid(group_id, {computation_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(group_id)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(group_id))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.cast(pid, {:computation_pid_update, computation_id, updated_pids})
    end)
  end
end
