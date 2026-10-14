defmodule FlezhaTest.Helper.ControlFrameTestHelper do
  @moduledoc """
  Encodes the `CommandRequest` an SDK writes on its control stream, decodes the
  one the controller hands its transport, and plays a runner on the
  controller's real UDS control socket.

  Framing happens once, in the Rust NIF (`Flezha.Transport.Stream.send_frame/4`),
  so a ControlChannel test's capturing `write_fun` receives the raw protobuf
  `message` directly. The runner played here writes and reads the frame itself:
  a space-padded 32-byte payload id, a 4-byte length, then the message and the
  metadata table, each behind its own length.
  """

  import ExUnit.Assertions

  alias Flezha.Socket.Support

  @payload_id_len 32

  @doc "Encode one `CommandRequest` the way an SDK writes it (raw protobuf)."
  @spec encode_request(binary(), tuple()) :: binary()
  def encode_request(reference_id, command) do
    {:ok, iodata, _size} =
      Protox.encode(%RunnerCommands.CommandRequest{reference_id: reference_id, command: command})

    IO.iodata_to_binary(iodata)
  end

  @doc "Encode the `CommandResponse` a runner answers `reference_id` with."
  @spec encode_response(binary(), binary(), [Flezha.SDK.Error.t()]) :: binary()
  def encode_response(reference_id, status, errors \\ []) do
    response = %RunnerCommands.CommandResponse{
      reference_id: reference_id,
      status: status,
      errors:
        Enum.map(errors, fn error ->
          %RunnerCommands.Error{
            code: error.code,
            class: Atom.to_string(error.class),
            message: error.message,
            details_json: Flezha.SDK.Error.details_json(error),
            hint: error.hint || ""
          }
        end)
    }

    {:ok, iodata, _size} = Protox.encode(response)
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
  A runner played by the calling process: it dials the controller's UDS
  control socket and sends `payload` as its connect frame. Answers the socket
  and the connect's outcome: `{:ok, runner_id}`, read off the controller's first
  command (which stays queued for `next_command/2`), or `{:error, reason}`, the
  reason the controller's rejection carried.
  """
  @spec open_runner(map(), timeout()) :: {port(), {:ok, binary()} | {:error, term()}}
  def open_runner(payload, timeout \\ 5_000) do
    test = self()
    handler = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler,
      [:flezha, :transport, :connect_rejected],
      fn _event, _measurements, %{reason: reason}, _ -> send(test, {handler, reason}) end,
      nil
    )

    try do
      socket = dial()
      :ok = send_frame(socket, "connect_control_channel", Jason.encode!(payload))

      outcome =
        receive do
          {^handler, reason} ->
            {:error, reason}

          {:tcp, ^socket, bytes} ->
            buffer(socket, bytes)
            {:ok, assigned_runner_id(socket, timeout)}
        after
          timeout -> flunk("the controller neither admitted nor refused the connect")
        end

      {socket, outcome}
    after
      :telemetry.detach(handler)
    end
  end

  @doc "A socket on the controller's UDS control socket, its bytes delivered to the caller."
  @spec dial() :: port()
  def dial do
    {:ok, socket} =
      :gen_tcp.connect({:local, Support.control_socket_path()}, 0, [:binary, active: true])

    socket
  end

  defp assigned_runner_id(socket, timeout) do
    {_ref, _kind, {:assign_runner_id, %{runner_id: runner_id}}} = peek_command(socket, timeout)
    runner_id
  end

  @doc "Write one frame with no metadata on `socket`."
  @spec send_frame(port(), binary(), binary()) :: :ok | {:error, term()}
  def send_frame(socket, payload_id, message) do
    id = String.pad_trailing(payload_id, @payload_id_len)
    body = <<byte_size(message)::32, message::binary, 0::16>>
    :gen_tcp.send(socket, [id, <<byte_size(body)::32>>, body])
  end

  @doc "Answer the command `reference_id` on `socket`."
  @spec answer(port(), binary(), binary(), [Flezha.SDK.Error.t()]) :: :ok | {:error, term()}
  def answer(socket, reference_id, status, errors \\ []),
    do: send_frame(socket, "control_command", encode_response(reference_id, status, errors))

  @doc "The next control command on `socket`: `{reference_id, kind, command}`."
  @spec next_command(port(), timeout()) :: {binary(), atom(), tuple()}
  def next_command(socket, timeout \\ 5_000) do
    command = peek_command(socket, timeout)
    [_ | rest] = queued(socket)
    Process.put({__MODULE__, :frames, socket}, rest)
    command
  end

  defp peek_command(socket, timeout) do
    case queued(socket) do
      [message | _] ->
        %{reference_id: ref, command: {kind, _} = command} = decode_request(message)
        {ref, kind, command}

      [] ->
        receive do
          {:tcp, ^socket, bytes} ->
            buffer(socket, bytes)
            peek_command(socket, timeout)
        after
          timeout -> flunk("no control command arrived in #{timeout} ms")
        end
    end
  end

  @doc "Whether a frame is already waiting on `socket`."
  @spec frame_waiting?(port()) :: boolean()
  def frame_waiting?(socket) do
    receive do
      {:tcp, ^socket, bytes} -> buffer(socket, bytes)
    after
      0 -> :ok
    end

    queued(socket) != []
  end

  # Whole control frames off the bytes that arrived, kept in arrival order.
  defp buffer(socket, bytes) do
    {frames, rest} = decode_frames(Process.get({__MODULE__, :bytes, socket}, <<>>) <> bytes)
    Process.put({__MODULE__, :bytes, socket}, rest)
    commands = for {"control_command", message, _metadata} <- frames, do: message
    Process.put({__MODULE__, :frames, socket}, queued(socket) ++ commands)
  end

  defp queued(socket), do: Process.get({__MODULE__, :frames, socket}, [])

  @doc """
  The whole frames at the head of `bytes`, each `{payload_id, message,
  metadata}`, and the bytes of a frame not yet whole.
  """
  @spec decode_frames(binary()) :: {[{binary(), binary(), map()}], binary()}
  def decode_frames(bytes), do: decode_frames(bytes, [])

  defp decode_frames(
         <<id::binary-size(@payload_id_len), len::32, payload::binary-size(len), rest::binary>>,
         acc
       ) do
    <<message_len::32, message::binary-size(message_len), table_len::16,
      table::binary-size(table_len)>> = payload

    decode_frames(rest, [{String.trim_trailing(id), message, metadata(table, %{})} | acc])
  end

  defp decode_frames(partial, acc), do: {Enum.reverse(acc), partial}

  defp metadata(
         <<klen, vlen, key::binary-size(klen), value::binary-size(vlen), rest::binary>>,
         acc
       ),
       do: metadata(rest, Map.put(acc, key, value))

  defp metadata(_end, acc), do: acc

  @doc """
  The command answering `reference_id`, from the frames a runner played by the
  calling process receives on `socket`. A handshake command read on the way is
  answered `success`, which is all such a runner owes to be admitted, and any
  other command is passed over.
  """
  @spec await_answering_handshake(port(), binary(), timeout()) :: tuple()
  def await_answering_handshake(socket, reference_id, timeout \\ 5_000) do
    case next_command(socket, timeout) do
      {^reference_id, _kind, command} ->
        command

      {handshake, kind, _} when kind in [:assign_runner_id, :connect_socket] ->
        :ok = answer(socket, handshake, "success")
        await_answering_handshake(socket, reference_id, timeout)

      _other ->
        await_answering_handshake(socket, reference_id, timeout)
    end
  end
end
