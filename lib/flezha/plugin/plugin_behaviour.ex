defmodule Flezha.Plugin.PluginBehaviour do
  @callback enable(plugin_id :: binary(), plugin :: struct()) ::
              {:ok, tuple()} | {:error, reason :: any()}

  @callback disable(plugin_id :: binary(), info :: tuple()) :: :ok | {:error, reason :: any()}

  # Whether a given plugin struct can be faithfully restored after an agent
  # restart. Return `false` for plugins whose state is bound to a caller pid
  # (or any other transient resource that won't survive a BEAM restart) —
  # those plugins will be skipped by the failure-recovery subsystem and must
  # be re-established by the caller on reconnect.
  #
  # Implementations SHOULD use explicit pattern matching on provider types
  # rather than reflecting over struct fields, so that adding a new
  # pid-bound provider is a deliberate, reviewable change.
  #
  # Default when a driver does not implement this callback: `true`.
  @callback recoverable?(plugin :: struct()) :: boolean()

  @optional_callbacks recoverable?: 1
end
