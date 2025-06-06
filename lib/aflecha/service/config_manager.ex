defmodule Flezha.Service.ConfigManager do
  def get_enabled_plugins() do
    Application.get_env(:aflecha, :enabled_plugins)
  end
end
