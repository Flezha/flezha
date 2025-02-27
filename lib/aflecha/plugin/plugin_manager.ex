defmodule Aflecha.Plugin.PluginManager do
  alias Aflecha.Plugin.ManagedProcess, as: ManagerProcessPlugin
  alias Aflecha.HubManager

  # TODO: the worker pid extraction should be in a separate service.
  def plugin_attach(%ManagerProcessPlugin{} = settings, event) do
    with {:ok, worker_id} <- extract_worker_id(settings),
         group_id <- Map.get(settings, :group_id, "default"),
         {:ok, worker_pid} <- HubManager.get_pid(group_id, worker_id) do
      GenServer.call(worker_pid, {:add_plugin_subscriber, event, settings})
    else
      err -> err
    end
  end

  defp extract_worker_id(settings) do
    case Map.get(settings, :worker_id, nil) do
      nil -> {:error, "Missing worker id."}
      worker_id -> {:ok, worker_id}
    end
  end
end
