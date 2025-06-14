defmodule Flezha.Service.PluginManager do
  alias Flezha.Plugin.PluginProtocol

  def enable_plugin(plugin_settings) do
    plugin_settings
    |> PluginProtocol.pre_enable()
    |> case do
      {:ok, settings} -> settings
      {:error, reason} -> raise "Error pre-enabling plugin: #{reason}"
    end
    |> PluginProtocol.enable()
  end

  def disable_plugin(plugin_settings) do
    PluginProtocol.disable(plugin_settings)
  end
end
