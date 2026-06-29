defmodule Flezha.Plugin.Provider.MessageForward do
  @moduledoc """
  First-party `:on_message` provider. Forwards the request payload to the
  SDK-side operative factory via the `OperationMessage` control-channel
  command and returns the reply.
  """

  alias Flezha.Executor.ControlUnit
  alias Flezha.Plugin.Extension.Ctx

  def on_message(
        %Ctx.Msg{
          operative_id: operative_id,
          executor_id: executor_id,
          payload: payload
        },
        _opts
      )
      when is_binary(payload) do
    ControlUnit.send_operation_message(executor_id, operative_id, payload)
  end
end
