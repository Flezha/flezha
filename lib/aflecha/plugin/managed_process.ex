defmodule Aflecha.Plugin.ManagedProcess do
  defmacro __using__(_opts) do
    quote do
      @behaviour Aflecha.Plugin.PluginContract
      @behaviour Aflecha.Plugin.ManagedProcess
    end
  end

  @callback start_link(arg :: term()) :: {:ok, pid()} | {:error, term()}

  defstruct [
    :group_id,
    :worker_id,
    :plugin_mod,
    :plugin_func,
    :plugin_args
  ]
end
