defmodule Flecha.Service.HubManager do
  def hook_insert_pid({computation_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    Supervisor.which_children(Flecha.ChannelSupervisor)
    |> Enum.each(fn {_, pid, _, _} ->
      GenServer.cast(pid, {:computation_pid_update, computation_id, updated_pids})
    end)
  end
end
