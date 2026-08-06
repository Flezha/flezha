defmodule Flezha.Plugin.Provider.MessageForward do
  @moduledoc """
  First-party `:on_message` provider. Forwards the request payload to the
  SDK-side operative factory via the `OperativeMessage` control-channel
  command and returns the reply.
  """

  alias Flezha.Runner.ControlUnit
  alias Flezha.Plugin.Extension.Ctx

  def on_message(
        %Ctx.Msg{
          operative_id: operative_id,
          runner_id: runner_id,
          payload: payload
        },
        _opts
      )
      when is_binary(payload) do
    ControlUnit.send_operative_message(runner_id, operative_id, payload)
  end
end
