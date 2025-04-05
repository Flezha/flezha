defmodule Aflecha.Service.PluginManager do
  alias Aflecha.Plugin.PluginProtocol

  def enable_plugin(plugin_settings) do
    PluginProtocol.enable(plugin_settings)
  end

  def disable_plugin(plugin_settings) do
    PluginProtocol.disable(plugin_settings)
  end
end
