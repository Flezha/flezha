defmodule Flecha.Service.HubManager do
  def hook_insert_pid({pipeline_id, node_pids}) do
    # TODO: Implement this function
    # curr_node = node()

    # updated_pids =
    #   Enum.filter(node_pids, fn {node, _pid} ->
    #     node === curr_node
    #   end)
    #   |> Enum.map(fn {_, pid} -> pid end)

    # Supervisor.which_children(Flecha.ChannelSupervisor)
    # |> Enum.each(fn {_, pid, _, _} ->
    #   GenServer.cast(pid, {:pipeline_pid_update, pipeline_id, updated_pids})
    # end)
  end
end
