defmodule FlezhaTest.Helper.WebhookServer do
  @moduledoc """
  A keep-alive HTTP/1.1 receiver on `:gen_tcp`, for a test that drives an HTTP
  sending plugin (`hook_webhook`, `http_push`): it reads every request on every
  connection, sends it to the process that started it as
  `{:request, request_line, headers, body}`, and answers it with the next of
  the statuses it was started with (the last one repeats), which `set/2`
  replaces. A status is a code, `{code, headers}` to answer with response
  headers, `:hang` to read the request and never answer, or `:close` to close
  the connection without an answer. `connections/1` counts the connections
  accepted.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  @doc "Listen on a free port; the listener and its acceptor go when the test does."
  @spec start([status]) :: %{
          port: :inet.port_number(),
          agent: pid(),
          accepted: :counters.counters_ref()
        }
        when status: pos_integer() | {pos_integer(), [{binary(), binary()}]} | :hang | :close
  def start(statuses) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start(fn -> statuses end)
    accepted = :counters.new(1, [])
    test = self()
    acceptor = spawn(fn -> accept(listen, {agent, accepted}, test) end)

    on_exit(fn ->
      Process.exit(acceptor, :kill)
      :gen_tcp.close(listen)
    end)

    %{port: port, agent: agent, accepted: accepted}
  end

  @doc "Answer the requests from now on with `statuses`."
  @spec set(map(), list()) :: :ok
  def set(%{agent: agent}, statuses), do: Agent.update(agent, fn _ -> statuses end)

  @doc "How many connections it has accepted."
  @spec connections(map()) :: non_neg_integer()
  def connections(%{accepted: accepted}), do: :counters.get(accepted, 1)

  defp accept(listen, {agent, accepted} = server, test) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        :counters.add(accepted, 1, 1)
        pid = spawn(fn -> serve(socket, agent, test, "") end)
        :gen_tcp.controlling_process(socket, pid)
        accept(listen, server, test)

      {:error, _} ->
        :ok
    end
  end

  defp serve(socket, agent, test, buffer) do
    case read_request(socket, buffer) do
      {:ok, request_line, headers, body, rest} ->
        send(test, {:request, request_line, headers, body})

        if answer(socket, next_status(agent)) == :ok,
          do: serve(socket, agent, test, rest),
          else: :gen_tcp.close(socket)

      :closed ->
        :ok
    end
  end

  defp next_status(agent) do
    Agent.get_and_update(agent, fn
      [only] -> {only, [only]}
      [next | rest] -> {next, rest}
    end)
  end

  defp answer(_socket, :hang), do: Process.sleep(:infinity)
  defp answer(_socket, :close), do: :closed
  defp answer(socket, status) when is_integer(status), do: answer(socket, {status, []})

  defp answer(socket, {status, headers}) do
    lines = Enum.map(headers, fn {name, value} -> "#{name}: #{value}\r\n" end)
    :gen_tcp.send(socket, ["HTTP/1.1 #{status} X\r\n", lines, "content-length: 0\r\n\r\n"])
  end

  defp read_request(socket, buffer) do
    case String.split(buffer, "\r\n\r\n", parts: 2) do
      [head, rest] ->
        [request_line | header_lines] = String.split(head, "\r\n")
        length = content_length(header_lines)
        read_body(socket, {request_line, Enum.join(header_lines, "\n")}, rest, length)

      [_partial] ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, data} -> read_request(socket, buffer <> data)
          _closed -> :closed
        end
    end
  end

  defp read_body(socket, head, rest, length) when byte_size(rest) < length do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} -> read_body(socket, head, rest <> data, length)
      _closed -> :closed
    end
  end

  defp read_body(_socket, {line, headers}, rest, length) do
    <<body::binary-size(^length), after_body::binary>> = rest
    {:ok, line, headers, body, after_body}
  end

  defp content_length(lines) do
    Enum.find_value(lines, 0, fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] ->
          if String.downcase(name) == "content-length", do: String.to_integer(String.trim(value))

        _ ->
          nil
      end
    end)
  end
end
