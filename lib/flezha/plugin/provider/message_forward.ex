defmodule Flezha.Plugin.Provider.MessageForward do
  @moduledoc """
  First-party `:on_message` provider. Forwards the request payload to the
  SDK-side processor factory via the `ProcessorMessage` control-channel
  command and returns the reply.
  """

  alias Flezha.ComputeEngine.ControlUnit
  alias Flezha.Plugin.Extension.Ctx

  def on_message(
        %Ctx.Msg{
          processor_id: processor_id,
          compute_engine_id: compute_engine_id,
          payload: payload
        },
        _opts
      )
      when is_binary(payload) do
    ControlUnit.send_processor_message(compute_engine_id, processor_id, payload)
  end
end
