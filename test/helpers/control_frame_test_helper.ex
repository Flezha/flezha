defmodule FlezhaTest.Helper.ControlFrameTestHelper do
  @moduledoc """
  Encodes the `CommandRequest` an SDK writes on its control stream, and decodes
  the one the controller hands its transport.

  Framing now happens once, in the Rust NIF (`Flezha.Transport.Stream.send_frame/4`),
  so a ControlChannel test's capturing `write_fun` receives the raw protobuf
  `message` directly — there is no Elixir-side frame to peel.
  """

  @doc "Encode one `CommandRequest` the way an SDK writes it (raw protobuf)."
  @spec encode_request(binary(), tuple()) :: binary()
  def encode_request(reference_id, command) do
    {:ok, iodata, _size} =
      Protox.encode(%RunnerCommands.CommandRequest{reference_id: reference_id, command: command})

    IO.iodata_to_binary(iodata)
  end

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
