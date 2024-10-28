defmodule Aflecha.Service.HubManager do
  alias Aflecha.Service.Warden
  alias Aflecha.Service.Identificator

  def hook_insert_pid({computation_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    Warden.get(:engines)
    |> Enum.each(fn {engine_id, _factories} ->
      stream_channels =
        Supervisor.which_children(Identificator.stream_sup_primary(engine_id)) ++
          Supervisor.which_children(Identificator.stream_sup_secondary(engine_id))

      Enum.each(stream_channels, fn {_, pid, _, _} ->
        GenServer.cast(pid, {:computation_pid_update, computation_id, updated_pids})
      end)
    end)
  end
end
