defmodule Flecha.Service.HubManager do
  alias Flecha.Warden
  alias Flecha.Utility.Identifier

  def hook_insert_pid({computation_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    Warden.get(:engines)
    |> Enum.each(fn {engine_id, _blueprints} ->
      Supervisor.which_children(Identifier.stream_sup(engine_id))
      |> Enum.each(fn {_, pid, _, _} ->
        GenServer.cast(pid, {:computation_pid_update, computation_id, updated_pids})
      end)
    end)
  end
end
