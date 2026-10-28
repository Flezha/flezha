defmodule Flezha.Cli.Socket do
  @moduledoc """
  One `flz` request, served on its own stream of the controller's UDS control
  socket by the code every verb already runs through.

  The stream's first frame is `cli_request`, one JSON object: `protocol`,
  `entry` (`cli` for `Flezha.Service.Cli.call/3`, `mcp` for
  `Flezha.Mcp.Stdio.answer/2`), the door `name` and its `args` or the MCP
  `line`, and `opts`, whose keys become the call's options. The request is
  answered `cli_accepted` first; what the call writes to its group leader goes
  back as `cli_stdout` frames and what it writes to the caller's stderr as
  `cli_stderr` frames, in order; `cli_exit` carries the code it ended with.
  A caller that closes the stream first ends the call.

  `Flezha.Transport.Server` hands this module the requests of its UDS listener
  only, and refuses one on any other transport itself.
  """

  alias Flezha.Config.Durations
  alias Flezha.Error
  alias Flezha.Mcp.Stdio
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context
  alias Flezha.Transport.Stream, as: TStream

  @protocol 1

  # Well below any frame limit a deployment sets; a larger write is split.
  @chunk_bytes 65_536

  @doc "The supervisor every request's process runs under."
  def supervisor, do: __MODULE__.Supervisor

  @doc "Serve the request `body` that arrived on `stream`, in a process of its own."
  @spec start(TStream.handle(), binary()) :: :ok
  def start(stream, body) do
    case Task.Supervisor.start_child(supervisor(), fn -> serve(stream, body) end) do
      {:ok, _pid} -> :ok
      _not_started -> TStream.close(stream)
    end

    :ok
  end

  @doc false
  def serve(stream, body) do
    :ok = TStream.controlling_process(stream, self())
    send_json(stream, "cli_accepted", %{protocol: @protocol})
    TStream.set_active(stream)

    request = decode(body)
    stdout = device(stream, "cli_stdout")
    stderr = device(stream, "cli_stderr")
    Context.bind_stderr(stderr)

    # Through `proc_lib`, so a call that crashes logs its report itself before
    # it exits, ahead of the DOWN, rather than through the runtime afterwards.
    {call, monitor} =
      :proc_lib.spawn_opt(
        fn ->
          Process.group_leader(self(), stdout)
          Context.bind_stderr(stderr)
          run(request)
        end,
        [:monitor]
      )

    await(stream, {call, monitor}, [stdout, stderr], format(request))
  end

  # Until the call ends, or the caller closes the stream first and the call is
  # ended for it: its devices die with this process, which ends whatever the
  # call left watching its group leader.
  defp await(stream, {call, monitor} = running, devices, format) do
    receive do
      {:DOWN, ^monitor, :process, ^call, reason} ->
        send_json(stream, "cli_exit", %{code: exit_code(reason, format)})
        close_after_caller(stream)

      {:flz_closed, _stream_id, _reason} ->
        Process.exit(call, :kill)

      {:flz_send_gate, _stream_id, false} ->
        Enum.each(devices, &send(&1, :gate_open))
        await(stream, running, devices, format)

      _other ->
        await(stream, running, devices, format)
    end
  end

  defp exit_code(:normal, _format), do: 0
  defp exit_code({:shutdown, code}, _format) when is_integer(code), do: code

  defp exit_code(reason, format),
    do: reason |> Error.from_reason(nil) |> Context.report(output: format)

  # `flz` closes the stream once it has read `cli_exit`; closing it here first
  # would drop whatever the driver has not yet written.
  defp close_after_caller(stream) do
    receive do
      {:flz_closed, _stream_id, _reason} -> :ok
    after
      Durations.get(:cli_close_wait_ms) -> TStream.close(stream)
    end
  end

  # A refusal renders in the format the request asked for, through the
  # request's stderr, and ends the call with its class's code.
  defp run({:ok, request}) do
    rendering = [output: format({:ok, request})]

    case request do
      %{"protocol" => @protocol} ->
        call(request, options(Map.get(request, "opts", %{}), rendering))

      other ->
        Context.render!(unsupported(other["protocol"]), rendering)
    end
  end

  defp run({:error, unreadable}), do: Context.render!(unreadable, [])

  defp call(%{"entry" => "cli", "name" => name, "args" => args}, opts) when is_binary(name),
    do: Cli.call(name, Jason.encode!(args), opts)

  defp call(%{"entry" => "mcp", "line" => line}, opts) when is_binary(line),
    do: Stdio.answer(line, opts)

  defp call(request, opts),
    do:
      Context.render!(
        Error.new("control.unknown_function", function: inspect(request["entry"])),
        opts
      )

  defp unsupported(got),
    do: Error.new("cli.protocol_unsupported", got: inspect(got), supported: @protocol)

  # The caller's option keys become the keyword list `Cli.call/3` reads, never
  # minting an atom: a key that names none is refused before the call runs.
  defp options(opts, rendering) when is_map(opts),
    do: Enum.map(opts, fn {key, value} -> {option_key(key, rendering), value} end)

  defp options(_opts, rendering),
    do: Context.render!(Error.new("control.unknown_parameter", parameter: "opts"), rendering)

  defp option_key(key, rendering) do
    String.to_existing_atom(key)
  rescue
    ArgumentError ->
      Context.render!(Error.new("control.unknown_parameter", parameter: key), rendering)
  end

  defp decode(body) do
    case Jason.decode(body) do
      {:ok, request} when is_map(request) -> {:ok, request}
      {:ok, _other} -> {:error, unreadable("the request is not a JSON object")}
      {:error, error} -> {:error, unreadable(Exception.message(error))}
    end
  end

  defp unreadable(reason), do: Error.new("control.invalid_args_json", reason: reason)

  defp format({:ok, %{"opts" => %{"output" => "json"}}}), do: "json"
  defp format(_request), do: "text"

  # An IO device of the request: every write becomes one or more frames of
  # `payload_id`. It lives as long as the request's process.
  defp device(stream, payload_id) do
    owner = self()

    spawn(fn ->
      Process.monitor(owner)
      io_loop(stream, payload_id)
    end)
  end

  defp io_loop(stream, payload_id) do
    receive do
      {:io_request, from, reply_as, request} ->
        send(from, {:io_reply, reply_as, io(request, stream, payload_id)})
        io_loop(stream, payload_id)

      {:DOWN, _monitor, :process, _owner, _reason} ->
        :ok

      :gate_open ->
        io_loop(stream, payload_id)
    end
  end

  defp io({:put_chars, encoding, chars}, stream, payload_id),
    do: put(stream, payload_id, bytes(encoding, chars))

  defp io({:put_chars, encoding, module, function, args}, stream, payload_id),
    do: io({:put_chars, encoding, apply(module, function, args)}, stream, payload_id)

  defp io({:requests, requests}, stream, payload_id) do
    Enum.reduce_while(requests, :ok, fn request, :ok ->
      case io(request, stream, payload_id) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp io(:getopts, _stream, _payload_id), do: [binary: true, encoding: :unicode]
  defp io(_request, _stream, _payload_id), do: {:error, :enotsup}

  defp bytes(:unicode, chars), do: :unicode.characters_to_binary(chars)
  defp bytes(_latin1, chars), do: IO.iodata_to_binary(chars)

  defp put(_stream, _payload_id, <<>>), do: :ok

  defp put(stream, payload_id, <<chunk::binary-size(@chunk_bytes), rest::binary>>) do
    with :ok <- send_held(stream, payload_id, chunk), do: put(stream, payload_id, rest)
  end

  defp put(stream, payload_id, chunk), do: send_held(stream, payload_id, chunk)

  # A full send buffer holds the writer until the driver reports it drained,
  # or for the hold budget when that report is lost, then offers the frame
  # again: a caller that reads slowly slows the call down and loses nothing.
  defp send_held(stream, payload_id, chunk) do
    case send_frame(stream, payload_id, chunk) do
      {:error, :send_buffer_full} ->
        receive do
          :gate_open -> :ok
        after
          Durations.get(:send_gate_hold_ms) -> :ok
        end

        send_held(stream, payload_id, chunk)

      answer ->
        answer
    end
  end

  defp send_json(stream, payload_id, map), do: send_frame(stream, payload_id, Jason.encode!(map))

  defp send_frame(stream, payload_id, message),
    do: TStream.send_frame(stream, payload_id, message, %{})
end
