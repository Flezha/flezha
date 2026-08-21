defmodule FlezhaTest.Helper.ControlFrameTestHelper do
  @moduledoc """
  Decodes the `CommandRequest` the controller hands its transport.

  Framing now happens once, in the Rust NIF (`Flezha.Transport.Stream.send_frame/4`),
  so a ControlChannel test's capturing `write_fun` receives the raw protobuf
  `message` directly — there is no Elixir-side frame to peel.
  """

  @doc """
  Decode the `CommandRequest` carried by one control message (raw protobuf).
  """
  @spec decode_request(binary()) :: RunnerCommands.CommandRequest.t()
  def decode_request(message) do
    {:ok, %RunnerCommands.CommandRequest{} = request} =
      RunnerCommands.CommandRequest.decode(message)

    request
  end
end
