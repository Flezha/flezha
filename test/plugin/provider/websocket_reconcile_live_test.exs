defmodule Flezha.Plugin.Provider.WebsocketReconcileLiveTest do
  @moduledoc """
  End-to-end reconcile against a real socket and a real source: a `:gen_tcp`
  RFC 6455 server, a `:gen_tcp` HTTP origin publishing the desired set, and the
  provider driven through its own `init_provider/1` → `activate/1` lifecycle —
  a genuine Fresh connection, a genuine `:httpc` poll, genuine websocket frames.

  The load-bearing assertion is that the server accepts exactly ONE TCP
  connection for the whole run. Adding an item and removing it again both take
  effect on the connection already open; if either had gone through a teardown,
  the accept count would be higher and every one of these would still pass.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.Websocket

  @moduletag :capture_log

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @sub ~s({"method":"subscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})
  @unsub ~s({"method":"unsubscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})

  test "a live socket adds and removes a subscription without ever reconnecting" do
    Process.flag(:trap_exit, true)

    ws = start_ws_server()
    origin = start_origin(["BTC/USD"])

    {:ok, opts} =
      Websocket.init_provider(%{
        uri: "ws://127.0.0.1:#{ws.port}/",
        plugin_id: "live_ws",
        subscription_url: "http://127.0.0.1:#{origin.port}/subs",
        subscription_interval_ms: 1_000,
        subscribe_template: @sub,
        unsubscribe_template: @unsub
      })

    {:ok, conn} = Websocket.activate(opts)
    on_exit(fn -> if Process.alive?(conn), do: Process.exit(conn, :kill) end)

    # ---- the seed: nothing applied yet, so the first poll subscribes the set
    seed = await_frame()
    assert seed["method"] == "subscribe"
    assert seed["params"]["symbol"] == ["BTC/USD"]
    ack(ws, seed["req_id"])

    # ---- add an item: the delta names ONLY the addition
    set_items(origin, ["BTC/USD", "ETH/USD"])

    add = await_frame()
    assert add["method"] == "subscribe"
    assert add["params"]["symbol"] == ["ETH/USD"]
    ack(ws, add["req_id"])

    # ---- remove it again: the delta names ONLY the removal
    set_items(origin, ["BTC/USD"])

    remove = await_frame()
    assert remove["method"] == "unsubscribe"
    assert remove["params"]["symbol"] == ["ETH/USD"]
    ack(ws, remove["req_id"])

    # ---- converged: the source is steady across a full poll cycle
    # (subscription_interval_ms is floored at 1_000 by the provider), so
    # nothing more is sent
    refute_receive {:ws_frame, _}, 1_200

    # ---- and it all happened on ONE connection
    assert connections(ws) == 1
    assert Process.alive?(conn)

    report = Websocket.inspect_state(%{subscription: opts.subscription, owned_pid: conn})
    assert report.applied == ["BTC/USD"]
    assert report.inflight == 0
    assert report.last_error == nil
  end

  ## ---------- websocket server ----------

  defp start_ws_server do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 4])

    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start(fn -> %{connections: 0, socket: nil} end)

    test = self()
    acceptor = spawn(fn -> ws_accept_loop(listen, agent, test) end)
    server = %{port: port, listen: listen, agent: agent, acceptor: acceptor}

    on_exit(fn ->
      if Process.alive?(acceptor), do: Process.exit(acceptor, :kill)
      :gen_tcp.close(listen)
    end)

    server
  end

  defp ws_accept_loop(listen, agent, test) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        Agent.update(agent, fn s -> %{s | connections: s.connections + 1, socket: socket} end)
        spawn(fn -> ws_serve(socket, test) end)
        ws_accept_loop(listen, agent, test)

      {:error, _closed} ->
        :ok
    end
  end

  defp ws_serve(socket, test) do
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

      ws_read_loop(socket, test, <<>>)
    end
  end

  defp read_handshake(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} ->
        acc = acc <> chunk

        if String.contains?(acc, "\r\n\r\n") do
          case Regex.run(~r/sec-websocket-key:\s*(\S+)/i, acc) do
            [_, key] -> {:ok, key}
            _ -> :error
          end
        else
          read_handshake(socket, acc)
        end

      {:error, _} ->
        :error
    end
  end

  defp ws_read_loop(socket, test, buffer) do
    case decode_frame(buffer) do
      {:ok, payload, rest} ->
        send(test, {:ws_frame, payload})
        ws_read_loop(socket, test, rest)

      :more ->
        case :gen_tcp.recv(socket, 0, 30_000) do
          {:ok, chunk} -> ws_read_loop(socket, test, buffer <> chunk)
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

  defp ack(ws, req_id) do
    socket = Agent.get(ws.agent, & &1.socket)
    body = Jason.encode!(%{"req_id" => req_id, "success" => true})
    :gen_tcp.send(socket, encode_frame(body))
  end

  defp connections(ws), do: Agent.get(ws.agent, & &1.connections)

  defp await_frame(timeout \\ 6_000) do
    receive do
      {:ws_frame, payload} -> Jason.decode!(payload)
    after
      timeout -> flunk("no websocket frame within #{timeout}ms")
    end
  end

  ## ---------- subscription source ----------

  defp start_origin(items) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 16])

    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start(fn -> %{items: items, version: 1} end)

    acceptor = spawn(fn -> origin_accept_loop(listen, agent) end)

    on_exit(fn ->
      if Process.alive?(acceptor), do: Process.exit(acceptor, :kill)
      :gen_tcp.close(listen)
    end)

    %{port: port, agent: agent}
  end

  defp set_items(origin, items) do
    Agent.update(origin.agent, fn s -> %{s | items: items, version: s.version + 1} end)
  end

  defp origin_accept_loop(listen, agent) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        spawn(fn -> origin_serve(socket, agent) end)
        origin_accept_loop(listen, agent)

      {:error, _closed} ->
        :ok
    end
  end

  defp origin_serve(socket, agent) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, _request} ->
        state = Agent.get(agent, & &1)
        body = Jason.encode!(%{"version" => state.version, "items" => state.items})

        :gen_tcp.send(socket, [
          "HTTP/1.1 200 OK\r\n",
          "Content-Type: application/json\r\n",
          "Content-Length: ",
          Integer.to_string(byte_size(body)),
          "\r\nConnection: close\r\n\r\n",
          body
        ])

        :gen_tcp.close(socket)

      {:error, _} ->
        :ok
    end
  end
end
