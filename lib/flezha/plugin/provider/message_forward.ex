defmodule Flezha.Plugin.Provider.MessageForward do
  @moduledoc """
  First-party `Flezha.Plugin.MessageHandlerBehaviour` provider that forwards
  the request payload to the SDK-side processor factory via a new
  `ProcessorMessage` control-channel command.

  On the SDK side the factory's `handle_message(payload)` callback produces a
  `ProcessorMessageResponse { payload, error }`. An empty `error` maps to
  `{:ok, bytes}`; a non-empty `error` (including the default "unsupported"
  message returned by factories that do not override `handle_message`) maps
  to `{:error, error_string}`.
  """

  alias Flezha.ComputeEngine.ControlUnit

  @behaviour Flezha.Plugin.MessageHandlerBehaviour

  @impl true
  def handle(payload, %{processor_id: processor_id, compute_engine_id: compute_engine_id})
      when is_binary(payload) do
    ControlUnit.send_processor_message(compute_engine_id, processor_id, payload)
  end
end
