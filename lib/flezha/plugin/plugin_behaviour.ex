defmodule Flezha.Plugin.PluginBehaviour do
  @callback enable(plugin_id :: binary(), plugin :: struct()) ::
              {:ok, tuple()} | {:error, reason :: any()}

  @callback disable(plugin_id :: binary(), info :: tuple()) :: :ok | {:error, reason :: any()}
end
