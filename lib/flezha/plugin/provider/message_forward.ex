defmodule Flezha.Plugin.Provider.MessageForward do
  @moduledoc """
  First-party `:on_message` provider. Forwards the request payload to the
  SDK-side relay factory via the `RelayMessage` control-channel
  command and returns the reply.
  """

  alias Flezha.Executor.ControlUnit
  alias Flezha.Plugin.Extension.Ctx

  def on_message(
        %Ctx.Msg{
          relay_id: relay_id,
          executor_id: executor_id,
          payload: payload
        },
        _opts
      )
      when is_binary(payload) do
    ControlUnit.send_relay_message(executor_id, relay_id, payload)
  end
end
