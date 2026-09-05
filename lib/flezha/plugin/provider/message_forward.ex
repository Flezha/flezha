defmodule Flezha.Plugin.Provider.MessageForward do
  @moduledoc """
  First-party `:on_message` provider. Forwards the request payload to the
  SDK-side operative factory via the `OperativeMessage` control-channel
  command and returns the reply.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Runner.ControlUnit

  use ProviderBehaviour,
    options: [key: [type: :string, doc: "The handler key this forwarder answers"]]

  @impl true
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
