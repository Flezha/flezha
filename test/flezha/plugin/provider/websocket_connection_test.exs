defmodule Flezha.Plugin.Provider.WebsocketWatchdogTest do
  @moduledoc """
  Two-tier watchdog contract for `Plugin.Provider.Websocket`:

    * connection liveness (`last_frame_at`, any inbound frame) drives a
      reconnect only when the socket is genuinely dead;
    * data liveness (`last_data_frame_at`, non-`ignore_pattern` frames)
      drives an in-place re-subscribe when `resubscribe_after_ms > 0`.

  Plus the startup-crash fix: `handle_connect/3` issues the subscribe via
  Fresh's reply form rather than a re-entrant `Fresh.send/2`.
  """

  # async: false: global telemetry handlers.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ProviderPrepare, only: [prepare: 2, activate: 2]

  alias Flezha.Plugin.Provider.Websocket
  alias FlezhaTest.Helper.NodeTestHelper

  @post_connect %{"payload" => %{"type" => "text", "message" => "subscribe-frame"}}

  defp state(overrides) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "test_ws",
        post_connect: %{},
        overwatcher_pid: spawn_dummy(),
        payload_metadata: %{},
        idle_timeout_ms: 100,
        resubscribe_after_ms: 0,
        ignore_pattern: nil,
        websocket_established?: true,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now
      },
      Map.new(overrides)
    )
  end

  defp attach_telemetry(event) do
    parent = self()
    handler_id = {__MODULE__, event, :erlang.unique_integer([:positive])}

    :telemetry.attach(
      handler_id,
      event,
      fn ^event, meas, meta, _ -> send(parent, {:telemetry, event, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp spawn_dummy do
    spawn(fn ->
      receive do
        _ -> :ok
      after
        5_000 -> :ok
      end
    end)
  end

  describe "connection liveness keeps a heartbeat-only feed alive" do
    test "heartbeats over the idle window do not trigger a reconnect" do
      # Mutation check: if `handle_in` stops refreshing `last_frame_at`,
      # the heartbeats would let `last_frame_at` go stale and this would
      # exit with {:shutdown, :idle_timeout}.
      s = state(idle_timeout_ms: 100, ignore_pattern: "heartbeat", last_data_frame_at: 0)

      # `last_data_frame_at: 0` is already older than any window; only the
      # heartbeats' refresh of `last_frame_at` keeps the feed alive.
      s =
        Enum.reduce(1..3, s, fn _, acc ->
          {:ok, ns} = Websocket.Client.handle_in({:text, ~s({"event":"heartbeat"})}, acc)
          ns
        end)

      # last_frame_at is fresh (heartbeats), last_data_frame_at is stale.
      assert {:ok, _} = Websocket.Client.handle_info(:idle_check, s)
    end
  end

  describe "total silence still reconnects" do
    test "no frames for idle_timeout_ms exits with {:shutdown, :idle_timeout} and telemetry" do
      attach_telemetry([:flezha, :plugin, :websocket, :idle_reconnect])
      Process.flag(:trap_exit, true)
      stale = :erlang.monotonic_time(:millisecond) - 500

      {pid, ref} =
        spawn_monitor(fn ->
          s = state(idle_timeout_ms: 100, last_frame_at: stale, last_data_frame_at: stale)
          Websocket.Client.handle_info(:idle_check, s)
          NodeTestHelper.park()
        end)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}, 1_000

      assert_receive {:telemetry, [:flezha, :plugin, :websocket, :idle_reconnect],
                      %{silence_ms: ms}, meta},
                     500

      assert ms >= 100
      assert meta.plugin_id == "test_ws"
    end
  end

  describe "control frames count as liveness" do
    test "ping/pong refresh last_frame_at and do not reconnect" do
      stale = :erlang.monotonic_time(:millisecond) - 500
      s = state(last_frame_at: stale)

      assert {:ok, after_ping} = Websocket.Client.handle_control({:ping, ""}, s)
      assert after_ping.last_frame_at > stale

      assert {:ok, after_pong} = Websocket.Client.handle_control({:pong, ""}, after_ping)
      assert after_pong.last_frame_at >= after_ping.last_frame_at

      # Connection is now fresh — the watchdog must not fire.
      assert {:ok, _} = Websocket.Client.handle_info(:idle_check, after_pong)
    end
  end

  describe "data-liveness re-subscribe on a live socket" do
    test "stale data with resubscribe_after_ms > 0 replies with post_connect and emits telemetry" do
      attach_telemetry([:flezha, :plugin, :websocket, :resubscribe])
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          idle_timeout_ms: 10_000,
          resubscribe_after_ms: 100,
          post_connect: @post_connect,
          last_frame_at: now,
          last_data_frame_at: now - 500,
          last_resubscribe_at: now - 500
        )

      assert {:reply, [{:text, "subscribe-frame"}], ns} =
               Websocket.Client.handle_info(:idle_check, s)

      assert ns.last_resubscribe_at >= now

      assert_receive {:telemetry, [:flezha, :plugin, :websocket, :resubscribe],
                      %{data_silence_ms: ms}, meta},
                     500

      assert ms >= 100
      assert meta.plugin_id == "test_ws"
    end
  end

  describe "re-subscribe is rate-limited" do
    test "a second tick inside the window does not re-subscribe again" do
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          idle_timeout_ms: 10_000,
          resubscribe_after_ms: 1_000,
          post_connect: @post_connect,
          last_frame_at: now,
          last_data_frame_at: now - 5_000,
          last_resubscribe_at: now - 5_000
        )

      assert {:reply, [{:text, _}], ns} = Websocket.Client.handle_info(:idle_check, s)
      # last_resubscribe_at is now fresh; data is still stale, but we are
      # inside the rate-limit window → no second re-subscribe.
      assert {:ok, _} = Websocket.Client.handle_info(:idle_check, ns)
    end
  end

  describe "re-subscribe disabled by default" do
    test "resubscribe_after_ms: 0 never re-subscribes" do
      attach_telemetry([:flezha, :plugin, :websocket, :resubscribe])
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          idle_timeout_ms: 100,
          resubscribe_after_ms: 0,
          post_connect: @post_connect,
          last_frame_at: now,
          last_data_frame_at: now - 5_000
        )

      assert {:ok, _} = Websocket.Client.handle_info(:idle_check, s)
      refute_received {:telemetry, [:flezha, :plugin, :websocket, :resubscribe], _, _}
    end
  end

  describe "subscribe handshake uses the reply form" do
    test "handle_connect returns the subscribe via {:reply, ...} and marks established" do
      s = state(post_connect: @post_connect, websocket_established?: false)

      assert {:reply, [{:text, "subscribe-frame"}], ns} =
               Websocket.Client.handle_connect(101, [], s)

      assert ns.websocket_established? == true
    end

    test "handle_connect without a post_connect payload returns {:ok, ...} and never raises" do
      s = state(post_connect: %{}, websocket_established?: false)
      assert {:ok, ns} = Websocket.Client.handle_connect(101, [], s)
      assert ns.websocket_established? == true
    end
  end

  describe "re-subscribe against an unestablished socket is non-fatal" do
    test "skips the send and stays alive when websocket_established? is false" do
      attach_telemetry([:flezha, :plugin, :websocket, :resubscribe])
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          idle_timeout_ms: 10_000,
          resubscribe_after_ms: 100,
          post_connect: @post_connect,
          websocket_established?: false,
          last_frame_at: now,
          last_data_frame_at: now - 500,
          last_resubscribe_at: now - 500
        )

      # Skip path: no re-entrant send is emitted (no {:reply, ...}), the
      # process stays alive, and the rate-limit guard still advances so we
      # do not retry every tick. No re-subscribe telemetry is emitted.
      assert {:ok, ns} = Websocket.Client.handle_info(:idle_check, s)
      assert ns.last_resubscribe_at >= now
      refute_received {:telemetry, [:flezha, :plugin, :websocket, :resubscribe], _, _}
    end
  end

  describe "idle watchdog stands down while disconnected" do
    # fails when: a transient disconnect leaves last_frame_at stale, and the
    # next idle tick tears down the reconnecting socket with
    # {:shutdown, :idle_timeout} — the daily false "data stops" that only
    # heals minutes later via attempt_restart.
    test "a stale last_frame_at during reconnect does not exit with :idle_timeout" do
      attach_telemetry([:flezha, :plugin, :websocket, :idle_reconnect])
      stale = :erlang.monotonic_time(:millisecond) - 500
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          s =
            state(
              idle_timeout_ms: 100,
              websocket_established?: false,
              last_frame_at: stale,
              last_data_frame_at: stale
            )

          Websocket.Client.handle_info(:idle_check, s)
          # Reached only when the check did not exit the process.
          send(parent, :checked)
          NodeTestHelper.park()
        end)

      assert_receive :checked, 1_000
      refute_received {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}
      refute_received {:telemetry, [:flezha, :plugin, :websocket, :idle_reconnect], _, _}
      Process.exit(pid, :kill)
    end
  end

  describe "per-frame path is single-read, single-classification" do
    test "a data frame derives both timestamps from one clock read" do
      s = state(ignore_pattern: "heartbeat", last_frame_at: 0, last_data_frame_at: 0)

      assert {:ok, ns} = Websocket.Client.handle_in({:text, ~s([{"price":"42000"}])}, s)
      # Both timestamps written from the same single `now` → identical.
      assert ns.last_frame_at == ns.last_data_frame_at
      assert ns.frames_seen == 1
    end

    test "a heartbeat frame advances only connection liveness" do
      s = state(ignore_pattern: "heartbeat", last_frame_at: 0, last_data_frame_at: 0)

      assert {:ok, ns} = Websocket.Client.handle_in({:text, ~s({"event":"heartbeat"})}, s)
      # Monotonic time is large-negative on the BEAM; the seed of 0 is
      # never a real reading, so "advanced" means "moved off the seed".
      assert ns.last_frame_at != 0
      assert ns.last_data_frame_at == 0
    end
  end
end

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

  # async: false: the primary Logger.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ProviderPrepare, only: [prepare: 2, activate: 2]

  alias Flezha.Plugin.Provider.Websocket
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :capture_log

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @sub ~s({"method":"subscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})
  @unsub ~s({"method":"unsubscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})

  test "a live socket adds and removes a subscription without ever reconnecting" do
    Process.flag(:trap_exit, true)

    ws = start_ws_server()
    origin = start_origin(["BTC/USD"])

    {:ok, opts} =
      prepare(Websocket, %{
        uri: "ws://127.0.0.1:#{ws.port}/",
        plugin_id: "live_ws",
        subscription_url: "http://127.0.0.1:#{origin.port}/subs",
        subscription_interval_ms: 1_000,
        subscribe_template: @sub,
        unsubscribe_template: @unsub
      })

    {:ok, conn} = activate(Websocket, opts)
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
    # (subscription_interval_ms is floored at 1_000 by the provider) — the
    # next reconcile finds nothing to send, and nothing more is sent
    assert NodeTestHelper.wait_until(
             fn ->
               Websocket.inspect_state(%{subscription: opts.subscription, owned_pid: conn}).last_reconcile ==
                 %{added: 0, removed: 0, sent: false}
             end,
             5_000
           ),
           "the provider never completed a quiet poll cycle"

    refute_received {:ws_frame, _}

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
          handshake_key(acc)
        else
          read_handshake(socket, acc)
        end

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

defmodule Flezha.Plugin.Provider.WebsocketIdleTimeoutDisabledTest do
  @moduledoc """
  Verifies that runners can fully disable the idle watchdog by passing
  `--Xidle_timeout_ms=0`. Without this opt-out, low-volume streams that
  legitimately go quiet for hours would reconnect every 30 s.

  Failure mode this protects against: the idle watchdog interfering with
  intentionally low-frequency WS subscriptions when a runner has
  declared the stream may stay quiet.
  """

  # async: false: global telemetry handlers.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ProviderPrepare, only: [prepare: 2, activate: 2]

  alias Flezha.Plugin.Provider.Websocket

  @telemetry_event [:flezha, :plugin, :websocket, :idle_reconnect]

  setup do
    parent = self()
    handler_id = "test-disabled-#{:erlang.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        @telemetry_event,
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "build/1 with idle_timeout_ms=\"0\" disables the watchdog" do
    assert {:ok, %Websocket{idle_timeout_ms: 0}} =
             Websocket.build(%{"uri" => "wss://example.com", "idle_timeout_ms" => "0"})
  end

  test "no Process.exit and no telemetry event when watchdog is disabled, even after long silence" do
    Process.flag(:trap_exit, true)
    parent = self()

    stale = System.monotonic_time(:millisecond) - 60_000

    state = %{
      uri: "wss://test.local",
      plugin_id: "test_disabled_ws",
      post_connect: %{},
      overwatcher_pid: parent,
      payload_metadata: %{},
      idle_timeout_ms: 0,
      resubscribe_after_ms: 0,
      ignore_pattern: nil,
      websocket_established?: true,
      frames_seen: 0,
      last_frame_at: stale,
      last_data_frame_at: stale,
      last_resubscribe_at: stale
    }

    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        Websocket.Client.handle_info(:idle_check, state)
        # Reached only when the check did not exit the process.
        send(parent, :checked)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :checked, 1_000
    refute_received {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}
    refute_received {:telemetry, @telemetry_event, _, _}

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
  end

  test "handle_connect/3 does not schedule any watchdog tick when disabled" do
    state = %{
      uri: "wss://test.local",
      plugin_id: "test_disabled_ws",
      post_connect: %{},
      overwatcher_pid: self(),
      payload_metadata: %{},
      idle_timeout_ms: 0,
      resubscribe_after_ms: 0,
      ignore_pattern: nil,
      websocket_established?: false,
      frames_seen: 0,
      last_frame_at: 0,
      last_data_frame_at: 0,
      last_resubscribe_at: 0
    }

    assert {:ok, _} = Websocket.Client.handle_connect(101, [], state)

    # Both tiers off means there is no interval to arm at all.
    assert Websocket.idle_tick_interval(state) == nil
    refute_received :idle_check
  end
end
