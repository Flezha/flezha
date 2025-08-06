defmodule Flezha.Service.PluginManager do
  alias Flezha.Plugin.PluginProtocol

  def enable_plugin(plugin) do
    plugin
    |> PluginProtocol.pre_enable()
    |> case do
      {:ok, settings} -> settings
      {:error, reason} -> raise "Error pre-enabling plugin: #{reason}"
    end
    |> PluginProtocol.enable()
  end

  def disable_plugin(plugin) do
    PluginProtocol.disable(plugin)
  end
end
