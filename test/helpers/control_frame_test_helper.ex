defmodule FlezhaTest.Helper.ControlFrameTestHelper do
  @moduledoc """
  Reads a control-channel frame the way the SDK does.

  Every ControlChannel test that captures what the controller wrote has to peel
  the `Flezha.Service.Payloader` framing (32-byte padded id, 4-byte payload
  size, 4-byte message size, message, 2-byte metadata size, metadata) before it
  can decode the `CommandRequest` inside. This is the one place that knows the
  layout.
  """

  @doc """
  Decode the `CommandRequest` carried by one framed control payload.
  """
  @spec decode_request(binary()) :: RunnerCommands.CommandRequest.t()
  def decode_request(framed_payload) do
    <<_payload_id::binary-size(32), _payload_size::32, message_size::32, rest::binary>> =
      framed_payload

    <<message::binary-size(^message_size), _metadata_size::16, _metadata::binary>> = rest

    {:ok, %RunnerCommands.CommandRequest{} = request} =
      RunnerCommands.CommandRequest.decode(message)

    request
  end
end
