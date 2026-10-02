defmodule FlezhaTest.Helper.WebsocketServer do
  @moduledoc """
  A real RFC 6455 peer on `:gen_tcp`, for a test that drives the websocket
  provider through a genuine Fresh connection: it completes the upgrade, sends
  every frame it reads to the process that started it as `{:ws_frame, payload}`,
  and writes nothing unless the test asks it to with `ack/2`.

  Just enough of the protocol for what a Fresh client sends a test: masked
  frames under 64 KiB in, unmasked text frames out. Every connection the server
  accepts is announced as `{:ws_open, socket}` and every one that ends as
  `{:ws_closed, socket}`, so a test can see which one opened first.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  @doc """
  Listen on `port`, a free one by default; the listener and its acceptor go when
  the test does.
  """
  @spec start(:inet.port_number()) :: %{port: :inet.port_number(), agent: pid()}
  def start(port \\ 0) do
    {:ok, listen} =
      :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 4])

    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start(fn -> %{connections: 0, socket: nil, open: []} end)

    test = self()
    acceptor = spawn(fn -> accept_loop(listen, agent, test) end)

    on_exit(fn ->
      if Process.alive?(acceptor), do: Process.exit(acceptor, :kill)
      :gen_tcp.close(listen)
      Agent.stop(agent)
    end)

    %{port: port, agent: agent}
  end

  @doc "Acknowledge `req_id` the way an upstream echoes a correlation id."
  @spec ack(map(), term()) :: :ok | {:error, term()}
  def ack(server, req_id) do
    socket = Agent.get(server.agent, & &1.socket)
    :gen_tcp.send(socket, encode_frame(Jason.encode!(%{"req_id" => req_id, "success" => true})))
  end

  @doc "How many TCP connections the server has accepted."
  @spec connections(map()) :: non_neg_integer()
  def connections(server), do: Agent.get(server.agent, & &1.connections)

  @doc "How many connections are open now."
  @spec open(map()) :: non_neg_integer()
  def open(server), do: Agent.get(server.agent, &length(&1.open))

  @doc "Send one text frame on every open connection."
  @spec broadcast(map(), binary()) :: :ok
  def broadcast(server, payload) do
    server.agent
    |> Agent.get(& &1.open)
    |> Enum.each(&:gen_tcp.send(&1, encode_frame(payload)))
  end

  defp accept_loop(listen, agent, test) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        pid = spawn(fn -> receive(do: (:go -> serve(socket, test, agent))) end)
        :ok = :gen_tcp.controlling_process(socket, pid)
        send(pid, :go)
        accept_loop(listen, agent, test)

      {:error, _closed} ->
        :ok
    end
  end

  defp serve(socket, test, agent) do
    with {:ok, key} <- read_handshake(socket, "") do
      accept = Base.encode64(:crypto.hash(:sha, key <> @guid))

      :gen_tcp.send(socket, [
        "HTTP/1.1 101 Switching Protocols\r\n",
        "upgrade: websocket\r\n",
        "connection: Upgrade\r\n",
        "sec-websocket-accept: ",
        accept,
        "\r\n\r\n"
      ])

      Agent.update(agent, fn s ->
        %{s | connections: s.connections + 1, socket: socket, open: s.open ++ [socket]}
      end)

      send(test, {:ws_open, socket})
      read_loop(socket, test, <<>>)
      Agent.update(agent, fn s -> %{s | open: List.delete(s.open, socket)} end)
      send(test, {:ws_closed, socket})
    end
  end

  defp read_handshake(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} ->
        acc = acc <> chunk

        if String.contains?(acc, "\r\n\r\n"),
          do: handshake_key(acc),
          else: read_handshake(socket, acc)

      {:error, _} ->
        :error
    end
  end

  defp handshake_key(headers) do
    case Regex.run(~r/sec-websocket-key:\s*(\S+)/i, headers) do
      [_, key] -> {:ok, key}
      _ -> :error
    end
  end

  defp read_loop(socket, test, buffer) do
    case decode_frame(buffer) do
      {:ok, payload, rest} ->
        send(test, {:ws_frame, payload})
        read_loop(socket, test, rest)

      :more ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, chunk} -> read_loop(socket, test, buffer <> chunk)
          {:error, _} -> :ok
        end
    end
  end

  # Client frames are always masked (RFC 6455 §5.3).
  defp decode_frame(<<_fin::1, _rsv::3, _op::4, 1::1, len::7, rest::binary>>) when len < 126 do
    unmask(rest, len)
  end

  defp decode_frame(<<_fin::1, _rsv::3, _op::4, 1::1, 126::7, len::16, rest::binary>>) do
    unmask(rest, len)
  end

  defp decode_frame(_), do: :more

  defp unmask(<<mask::binary-4, payload::binary>>, len) when byte_size(payload) >= len do
    <<masked::binary-size(^len), rest::binary>> = payload

    unmasked =
      masked
      |> :binary.bin_to_list()
      |> Enum.with_index()
      |> Enum.map(fn {byte, i} -> Bitwise.bxor(byte, :binary.at(mask, rem(i, 4))) end)
      |> :binary.list_to_bin()

    {:ok, unmasked, rest}
  end

  defp unmask(_, _), do: :more

  # Server frames are never masked.
  defp encode_frame(payload) when byte_size(payload) < 126,
    do: <<1::1, 0::3, 1::4, 0::1, byte_size(payload)::7, payload::binary>>

  defp encode_frame(payload),
    do: <<1::1, 0::3, 1::4, 0::1, 126::7, byte_size(payload)::16, payload::binary>>
end
