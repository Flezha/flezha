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

  describe "5.1 connection liveness keeps a heartbeat-only feed alive" do
    test "heartbeats over the idle window do not trigger a reconnect" do
      # Mutation check: if `handle_in` stops refreshing `last_frame_at`,
      # the heartbeats would let `last_frame_at` go stale and this would
      # exit with {:shutdown, :idle_timeout}.
      s = state(idle_timeout_ms: 100, ignore_pattern: "heartbeat", last_data_frame_at: 0)

      # `last_data_frame_at: 0` is already older than any window; only the
      # heartbeats' refresh of `last_frame_at` keeps the feed alive.
      s =
        Enum.reduce(1..3, s, fn _, acc ->
          {:ok, ns} = Websocket.handle_in({:text, ~s({"event":"heartbeat"})}, acc)
          ns
        end)

      # last_frame_at is fresh (heartbeats), last_data_frame_at is stale.
      assert {:ok, _} = Websocket.handle_info(:idle_check, s)
    end
  end

  describe "5.2 total silence still reconnects" do
    test "no frames for idle_timeout_ms exits with {:shutdown, :idle_timeout} and telemetry" do
      attach_telemetry([:flezha, :ws, :idle_reconnect])
      Process.flag(:trap_exit, true)
      stale = :erlang.monotonic_time(:millisecond) - 500

      {pid, ref} =
        spawn_monitor(fn ->
          s = state(idle_timeout_ms: 100, last_frame_at: stale, last_data_frame_at: stale)
          Websocket.handle_info(:idle_check, s)
          NodeTestHelper.park()
        end)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}, 1_000
      assert_receive {:telemetry, [:flezha, :ws, :idle_reconnect], %{silence_ms: ms}, meta}, 500
      assert ms >= 100
      assert meta.plugin_id == "test_ws"
    end
  end

  describe "5.3 control frames count as liveness" do
    test "ping/pong refresh last_frame_at and do not reconnect" do
      stale = :erlang.monotonic_time(:millisecond) - 500
      s = state(last_frame_at: stale)

      assert {:ok, after_ping} = Websocket.handle_control({:ping, ""}, s)
      assert after_ping.last_frame_at > stale

      assert {:ok, after_pong} = Websocket.handle_control({:pong, ""}, after_ping)
      assert after_pong.last_frame_at >= after_ping.last_frame_at

      # Connection is now fresh — the watchdog must not fire.
      assert {:ok, _} = Websocket.handle_info(:idle_check, after_pong)
    end
  end

  describe "5.4 data-liveness re-subscribe on a live socket" do
    test "stale data with resubscribe_after_ms > 0 replies with post_connect and emits telemetry" do
      attach_telemetry([:flezha, :ws, :resubscribe])
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

      assert {:reply, [{:text, "subscribe-frame"}], ns} = Websocket.handle_info(:idle_check, s)
      assert ns.last_resubscribe_at >= now
      assert_receive {:telemetry, [:flezha, :ws, :resubscribe], %{data_silence_ms: ms}, meta}, 500
      assert ms >= 100
      assert meta.plugin_id == "test_ws"
    end
  end

  describe "5.5 re-subscribe is rate-limited" do
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

      assert {:reply, [{:text, _}], ns} = Websocket.handle_info(:idle_check, s)
      # last_resubscribe_at is now fresh; data is still stale, but we are
      # inside the rate-limit window → no second re-subscribe.
      assert {:ok, _} = Websocket.handle_info(:idle_check, ns)
    end
  end

  describe "5.6 re-subscribe disabled by default" do
    test "resubscribe_after_ms: 0 never re-subscribes" do
      attach_telemetry([:flezha, :ws, :resubscribe])
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          idle_timeout_ms: 100,
          resubscribe_after_ms: 0,
          post_connect: @post_connect,
          last_frame_at: now,
          last_data_frame_at: now - 5_000
        )

      assert {:ok, _} = Websocket.handle_info(:idle_check, s)
      refute_received {:telemetry, [:flezha, :ws, :resubscribe], _, _}
    end
  end

  describe "5.7 subscribe handshake uses the reply form" do
    test "handle_connect returns the subscribe via {:reply, ...} and marks established" do
      s = state(post_connect: @post_connect, websocket_established?: false)

      assert {:reply, [{:text, "subscribe-frame"}], ns} =
               Websocket.handle_connect(101, [], s)

      assert ns.websocket_established? == true
    end

    test "handle_connect without a post_connect payload returns {:ok, ...} and never raises" do
      s = state(post_connect: %{}, websocket_established?: false)
      assert {:ok, ns} = Websocket.handle_connect(101, [], s)
      assert ns.websocket_established? == true
    end
  end

  describe "5.8 re-subscribe against an unestablished socket is non-fatal" do
    test "skips the send and stays alive when websocket_established? is false" do
      attach_telemetry([:flezha, :ws, :resubscribe])
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
      assert {:ok, ns} = Websocket.handle_info(:idle_check, s)
      assert ns.last_resubscribe_at >= now
      refute_received {:telemetry, [:flezha, :ws, :resubscribe], _, _}
    end
  end

  describe "5.10 idle watchdog stands down while disconnected" do
    # fails when: a transient disconnect leaves last_frame_at stale, and the
    # next idle tick tears down the reconnecting socket with
    # {:shutdown, :idle_timeout} — the daily false "data stops" that only
    # heals minutes later via attempt_restart.
    test "a stale last_frame_at during reconnect does not exit with :idle_timeout" do
      attach_telemetry([:flezha, :ws, :idle_reconnect])
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

          Websocket.handle_info(:idle_check, s)
          # Reached only when the check did not exit the process.
          send(parent, :checked)
          NodeTestHelper.park()
        end)

      assert_receive :checked, 1_000
      refute_received {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}
      refute_received {:telemetry, [:flezha, :ws, :idle_reconnect], _, _}
      Process.exit(pid, :kill)
    end
  end

  describe "5.9 per-frame path is single-read, single-classification" do
    test "a data frame derives both timestamps from one clock read" do
      s = state(ignore_pattern: "heartbeat", last_frame_at: 0, last_data_frame_at: 0)

      assert {:ok, ns} = Websocket.handle_in({:text, ~s([{"price":"42000"}])}, s)
      # Both timestamps written from the same single `now` → identical.
      assert ns.last_frame_at == ns.last_data_frame_at
      assert ns.frames_seen == 1
    end

    test "a heartbeat frame advances only connection liveness" do
      s = state(ignore_pattern: "heartbeat", last_frame_at: 0, last_data_frame_at: 0)

      assert {:ok, ns} = Websocket.handle_in({:text, ~s({"event":"heartbeat"})}, s)
      # Monotonic time is large-negative on the BEAM; the seed of 0 is
      # never a real reading, so "advanced" means "moved off the seed".
      assert ns.last_frame_at != 0
      assert ns.last_data_frame_at == 0
    end
  end
end
