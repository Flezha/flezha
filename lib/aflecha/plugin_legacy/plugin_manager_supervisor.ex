defmodule Aflecha.Plugin.PluginManagerSupervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_args) do
    Supervisor.init(children(), strategy: :one_for_one)
  end

  defp children() do
    managed_plugins = [:managed_process]

    Application.get_env(:aflecha, :enabled_plugins)
    |> Enum.map(fn mod -> {mod, apply(mod, :plugin_type, [])} end)
    |> Enum.filter(fn {_mod, plugin_type} ->
      Enum.member?(managed_plugins, plugin_type)
    end)
    |> Enum.map(fn {mod, _plugin_type} -> mod end)
  end
end
