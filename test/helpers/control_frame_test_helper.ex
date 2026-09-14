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

  @doc """
  The command answering `reference_id`, from the frames a runner played by the
  calling process receives on `stream`. A handshake command read on the way is
  answered `success`, which is all such a runner owes to be admitted, and any
  other command is passed over.
  """
  @spec await_answering_handshake(term(), binary(), timeout()) :: tuple()
  def await_answering_handshake(stream, reference_id, timeout \\ 5_000) do
    receive do
      {:flz_data, _stream_id, "control_command", message, _metadata} ->
        case decode_request(message) do
          %{reference_id: ^reference_id, command: command} ->
            command

          %{reference_id: handshake, command: {kind, _}}
          when kind in [:assign_runner_id, :connect_socket] ->
            response = Flezha.SDK.Codec.encode_response(handshake, "success")
            Flezha.Transport.Stream.send_frame(stream, "control_command", response, %{})
            await_answering_handshake(stream, reference_id, timeout)

          _other ->
            await_answering_handshake(stream, reference_id, timeout)
        end
    after
      timeout -> ExUnit.Assertions.flunk("nothing answered #{reference_id} in #{timeout} ms")
    end
  end
end
