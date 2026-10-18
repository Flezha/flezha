defmodule FlezhaTest.Helper.WebhookServer do
  @moduledoc """
  A keep-alive HTTP/1.1 origin on `:gen_tcp`, for a test that drives an HTTP
  client or plugin: it reads every request on every connection, records it,
  sends it to the process that started it as
  `{:request, request_line, headers, body}`, and only then answers it with the
  next of the statuses it was started with (the last one repeats), which `set/2`
  replaces.

  A status is a code; `{code, headers}` or `{code, headers, body}` to answer
  with response headers and a body; `:hang` to read the request and never
  answer; `:close` to close the connection without an answer;
  `{:hold, status}` to report `{:holding, conn}` to the starter and answer
  `status` once `release/0` lets it go; `{:conditional, status}` to answer a
  request carrying `if-none-match` 304 with `status`'s headers and no body, and
  any other with `status`; or a function of the socket that writes its own
  response, after which the connection closes. `connections/1` counts the
  connections accepted, `requests/1` returns the requests read.
  """

  import ExUnit.Assertions, only: [assert_receive: 2]
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias FlezhaTest.Helper.NodeTestHelper

  @type status ::
          pos_integer()
          | {pos_integer(), [{binary(), binary()}]}
          | {pos_integer(), [{binary(), binary()}], iodata()}
          | :hang
          | :close
          | {:hold, status()}
          | {:conditional, status()}
          | (:gen_tcp.socket() -> term())

  @type server :: %{
          port: :inet.port_number(),
          listen: :gen_tcp.socket(),
          acceptor: pid(),
          agent: pid(),
          accepted: :counters.counters_ref()
        }

  @doc "Listen on a free port; the listener and its acceptor go when the test does."
  @spec start([status()]) :: server()
  def start(statuses) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 16])

    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start(fn -> %{statuses: statuses, requests: []} end)
    accepted = :counters.new(1, [])
    test = self()
    acceptor = spawn(fn -> accept(listen, {agent, accepted}, test) end)
    server = %{port: port, listen: listen, acceptor: acceptor, agent: agent, accepted: accepted}

    on_exit(fn -> stop(server) end)
    server
  end

  @doc "Stop listening: a new connection is refused from now on."
  @spec stop(server()) :: :ok
  def stop(%{acceptor: acceptor, listen: listen}) do
    Process.exit(acceptor, :kill)
    :gen_tcp.close(listen)
  end

  @doc "Answer the requests from now on with `statuses`."
  @spec set(server(), [status()]) :: :ok
  def set(%{agent: agent}, statuses), do: Agent.update(agent, &%{&1 | statuses: statuses})

  @doc "How many connections it has accepted."
  @spec connections(server()) :: non_neg_integer()
  def connections(%{accepted: accepted}), do: :counters.get(accepted, 1)

  @doc "Every request read so far, oldest first, as `{request_line, headers, body}`."
  @spec requests(server()) :: [{binary(), binary(), binary()}]
  def requests(%{agent: agent}), do: Agent.get(agent, &Enum.reverse(&1.requests))

  @doc "Whether a request read is conditional: it carries `if-none-match`."
  @spec conditional?({binary(), binary(), binary()}) :: boolean()
  def conditional?({_line, headers, _body}),
    do: String.contains?(String.downcase(headers), "if-none-match:")

  @doc "Let the request a `{:hold, status}` answer is holding be answered."
  @spec release() :: :ok
  def release do
    assert_receive {:holding, conn}, 5_000
    send(conn, :unpark)
    :ok
  end

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
        request = {request_line, headers, body}
        status = take_status(agent, request)
        send(test, {:request, request_line, headers, body})

        if answer(socket, status, request, test) == :ok,
          do: serve(socket, agent, test, rest),
          else: :gen_tcp.close(socket)

      :closed ->
        :ok
    end
  end

  defp take_status(agent, request) do
    Agent.get_and_update(agent, fn %{statuses: statuses, requests: requests} ->
      {next, left} =
        case statuses do
          [only] -> {only, [only]}
          [next | rest] -> {next, rest}
        end

      {next, %{statuses: left, requests: [request | requests]}}
    end)
  end

  defp answer(_socket, :hang, _request, _test), do: Process.sleep(:infinity)
  defp answer(_socket, :close, _request, _test), do: :closed

  defp answer(socket, respond, _request, _test) when is_function(respond, 1) do
    respond.(socket)
    :closed
  end

  defp answer(socket, {:hold, status}, request, test) do
    send(test, {:holding, self()})
    NodeTestHelper.park()
    answer(socket, status, request, test)
  end

  defp answer(socket, {:conditional, status}, request, test) do
    if conditional?(request),
      do: answer(socket, {304, response_headers(status), ""}, request, test),
      else: answer(socket, status, request, test)
  end

  defp answer(socket, status, _request, _test) do
    {code, response_headers, body} = response(status)
    lines = Enum.map(response_headers, fn {name, value} -> "#{name}: #{value}\r\n" end)
    head = "HTTP/1.1 #{code} X\r\n"

    :gen_tcp.send(socket, [head, lines, "content-length: #{IO.iodata_length(body)}\r\n\r\n", body])
  end

  defp response(code) when is_integer(code), do: {code, [], ""}
  defp response({code, headers}), do: {code, headers, ""}
  defp response({_code, _headers, _body} = status), do: status

  defp response_headers(status), do: elem(response(status), 1)

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
