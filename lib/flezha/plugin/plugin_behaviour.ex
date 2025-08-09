defmodule Flezha.Plugin.PluginBehaviour do
  @callback enable(plugin_id :: binary(), plugin :: struct()) ::
              {:ok, tuple()} | {:error, reason :: any()}
end
