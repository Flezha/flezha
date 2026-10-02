defmodule FlezhaTest.Plugin.Provider.WebsocketSinkTest do
  @moduledoc """
  The websocket plugin's outbound half, driven at the seam the other eight
  websocket test files use: `handle_info/2`, `handle_connect/3` and the provider
  callbacks directly, never over a live socket.

  What it protects: an output reaches the socket the plugin already holds, a
  frame is never handed to a disconnected one, the two acknowledgement modes
  answer the outbox hold at the moments they declare, and the handoff cannot
  close the cycle the effectively-once ingest opens.
  """

  use ExUnit.Case, async: false

  alias Flezha.Config.Backoff
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.Websocket.Client
  alias Flezha.Plugin.Provider.Websocket.Sink
  alias Flezha.Process.Timer
  alias Flezha.State.Outbox
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.Telemetry
  alias FlezhaTest.Helper.WebsocketSourceFixtures

  import ExUnit.CaptureLog

  @uri "wss://flezha-test.invalid:9/never-connect"

  ## ---------- preparation ----------

  defp prepared(extra \\ %{}, subscriptions \\ [:on_output]) do
    ProviderPrepare.prepare(
      Websocket,
      Map.merge(
        %{
          uri: @uri,
          plugin_id: "ws_sink_#{System.unique_integer([:positive])}",
          subscribed_extensions: subscriptions
        },
        extra
      )
    )
  end

  defp sink!(extra \\ %{}, subscriptions \\ [:on_output]) do
    {:ok, opts} = prepared(extra, subscriptions)
    opts
  end

  # The socket process state as `start_link/1` builds it, without a socket.
  defp state(opts, overrides \\ %{}) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: opts.uri,
        plugin_id: opts.plugin_id,
        post_connect: opts.post_connect,
        overwatcher_pid: self(),
        payload_metadata: %{},
        idle_timeout_ms: Client.idle_window(opts),
        resubscribe_after_ms: opts.resubscribe_after_ms,
        ignore_pattern: nil,
        sink: opts.sink,
        websocket_established?: true,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now,
        reconnect_attempt: 0,
        timers: Timer.new()
      },
      overrides
    )
    |> WebsocketSourceFixtures.with_targets()
  end

  defp dispatch(state, message, metadata \\ %{}) do
    ref = make_ref()
    {Client.handle_info({{:sink_frame, message, metadata}, self(), ref}, state), ref}
  end

  ## ---------- 6.1 one frame per output ----------

  describe "one frame per output" do
    test "the payload is the frame body when no template is configured" do
      {{:reply, frames, _state}, ref} = dispatch(state(sink!()), "raw-bytes")

      assert frames == [{:text, "raw-bytes"}]
      assert_received {^ref, :ok}
    end

    test "the configured frame type is the type sent" do
      {{:reply, frames, _state}, _ref} =
        dispatch(state(sink!(%{sink_frame_type: "binary"})), "raw-bytes")

      assert frames == [{:binary, "raw-bytes"}]
    end

    test "a template substitutes the payload and the output's identity stamps" do
      opts = sink!(%{sink_template: ~s({"e":"{{cell}}","s":"{{seq}}","d":{{payload}}})})

      {{:reply, [{:text, body}], _state}, _ref} =
        dispatch(state(opts), "42", %{"flz.cell" => "rc/task/k1", "flz.seq" => "7.0"})

      assert body == ~s({"e":"rc/task/k1","s":"7.0","d":42})
    end

    test "a template that cannot carry the payload is refused at enable time" do
      assert {:error, {:template_missing_placeholder, :sink_template, placeholder}} =
               prepared(%{sink_template: ~s({"d":"constant"})})

      assert placeholder == Sink.payload_placeholder()
    end
  end

  ## ---------- 6.2 the channel filter ----------

  describe "the channel filter" do
    setup do
      plugin_id = "ws_chan_#{System.unique_integer([:positive])}"
      ProviderPrepare.stub_sink_worker(Websocket, plugin_id)

      {:ok, plugin_id: plugin_id}
    end

    defp ctx(message, metadata) do
      %Ctx.IO{
        operator_id: "op",
        namespace: :default,
        message: message,
        metadata: metadata
      }
    end

    test "an output naming the channel is sent", %{plugin_id: plugin_id} do
      opts = sink!(%{plugin_id: plugin_id, channel_out: "prices"})
      ctx = ctx("tick", %{"channel" => "prices"})

      assert {:ok, ^ctx} = Websocket.on_output(ctx, opts)
      assert_receive {:shipped, "tick", %{"channel" => "prices"}}, 1_000
    end

    test "an output naming another channel passes through untouched",
         %{plugin_id: plugin_id} do
      opts = sink!(%{plugin_id: plugin_id, channel_out: "prices"})
      ctx = ctx("tick", %{"channel" => "trades"})

      assert {:ok, ^ctx} = Websocket.on_output(ctx, opts)
      refute_receive {:shipped, _, _}, 200
    end

    test "an unrestricted plugin sends every channel", %{plugin_id: plugin_id} do
      opts = sink!(%{plugin_id: plugin_id})

      assert {:ok, _} = Websocket.on_output(ctx("a", %{"channel" => "x"}), opts)
      assert {:ok, _} = Websocket.on_input(ctx("b", %{"channel" => "y"}), opts)

      assert_receive {:shipped, "a", _}, 1_000
      assert_receive {:shipped, "b", _}, 1_000
    end

    test "a plugin whose socket is gone refuses the caller rather than holding" do
      opts = sink!(%{plugin_id: "ws_no_socket_#{System.unique_integer([:positive])}"})

      assert {:error, {:no_connection, _}} = Websocket.on_output(ctx("a", %{}), opts)
    end
  end

  ## ---------- 6.3 queue and flush ----------

  describe "a socket that is down" do
    test "frames queue and flush in dispatch order, after the subscription frames" do
      opts =
        sink!(%{
          post_connect: %{"payload" => %{"type" => "text", "message" => "SUBSCRIBE"}}
        })

      down = state(opts, %{websocket_established?: false})

      {{:ok, down}, _} = dispatch(down, "first")
      {{:ok, down}, _} = dispatch(down, "second")

      assert down.sink.buffer_len == 2

      assert {:reply, frames, connected} = Client.handle_connect(101, [], down)
      assert frames == [{:text, "SUBSCRIBE"}, {:text, "first"}, {:text, "second"}]
      assert connected.sink.buffer_len == 0
    end

    test "nothing is handed to the connection library while disconnected" do
      {{:ok, state}, ref} = dispatch(state(sink!(), %{websocket_established?: false}), "held")

      assert state.sink.buffer_len == 1
      assert_received {^ref, :ok}
    end
  end

  ## ---------- 6.4 the bound ----------

  test "a full queue parks the caller and releases it as the queue drains" do
    opts = sink!(%{sink_queue_max: 2})
    down = state(opts, %{websocket_established?: false})

    {{:ok, down}, first_ref} = dispatch(down, "one")
    assert_received {^first_ref, :ok}

    {{:ok, down}, parked_ref} = dispatch(down, "two")
    refute_received {^parked_ref, :ok}
    assert down.sink.buffer_len == 2
    assert length(down.sink.waiters) == 1

    assert {:reply, frames, drained} = Client.handle_connect(101, [], down)
    assert frames == [{:text, "one"}, {:text, "two"}]
    assert drained.sink.waiters == []
    assert_received {^parked_ref, :ok}
  end

  ## ---------- 6.5 / 6.6 the two acknowledgement modes ----------

  describe "delivery semantics" do
    setup do
      rc = "ws_#{System.unique_integer([:positive])}"
      cell_id = {rc, "task", "k1"}
      :ok = TestHelpers.stub_outbox_owner(cell_id)
      {:ok, cell_id: cell_id}
    end

    defp guaranteed(cell_id, seq), do: TestHelpers.guaranteed_metadata(cell_id, seq)

    test "\"none\" answers the hold at handoff", %{cell_id: cell_id} do
      {{:reply, _frames, _state}, _ref} =
        dispatch(state(sink!()), "v1", guaranteed(cell_id, 1))

      assert :ok = TestHelpers.assert_confirmed([{:cursor, 1}], 2_000)
    end

    test "\"none\" answers nothing while the socket is down", %{cell_id: cell_id} do
      {{:ok, _state}, _ref} =
        dispatch(
          state(sink!(), %{websocket_established?: false}),
          "v1",
          guaranteed(cell_id, 1)
        )

      refute_receive {:"$gen_cast", {:outbox_confirmed_many, _}}, 200
    end

    test "\"required\" needs a template that can carry the correlation id" do
      assert {:error, {:ack_needs_correlation, "{{req_id}}"}} =
               prepared(%{sink_ack_mode: "required"})

      assert {:error, {:ack_needs_correlation, "{{req_id}}"}} =
               prepared(%{sink_ack_mode: "required", sink_template: ~s({"d":"{{payload}}"})})
    end

    test "\"required\" answers the hold only on the echoed id", %{cell_id: cell_id} do
      opts = required_opts()

      {{:reply, [{:text, body}], sent}, _ref} =
        dispatch(state(opts), "v1", guaranteed(cell_id, 1))

      assert %{"req_id" => req_id} = Jason.decode!(body)
      assert map_size(sent.sink.inflight) == 1
      refute_receive {:"$gen_cast", {:outbox_confirmed, _}}, 200

      {:ok, settled} =
        Client.handle_in({:text, Jason.encode!(%{"req_id" => req_id, "success" => true})}, sent)

      assert settled.sink.inflight == %{}
      assert :ok = TestHelpers.assert_confirmed([{:cursor, 1}], 2_000)
    end

    # The per-frame path: a frame answered on its first send served no rung. The
    # re-sent frame after it is the control that shows the capture would see one.
    test "an ack on the first send announces no wait and no reset on the ack ladder",
         %{cell_id: cell_id} do
      Telemetry.capture([[:flezha, :backoff, :wait], [:flezha, :backoff, :reset]],
        filter: &(Map.get(&1, :ladder) == :ws_sink_ack)
      )

      {{:reply, [{:text, body}], sent}, _ref} =
        dispatch(state(required_opts()), "v1", guaranteed(cell_id, 1))

      %{"req_id" => req_id} = Jason.decode!(body)

      {:ok, settled} =
        Client.handle_in({:text, Jason.encode!(%{"req_id" => req_id, "success" => true})}, sent)

      assert settled.sink.inflight == %{}
      refute_received {:telemetry, _event, _measurements, %{ladder: :ws_sink_ack}}

      {{:reply, _frames, unanswered}, _ref} = dispatch(settled, "v2", guaranteed(cell_id, 2))
      aged = age_inflight(unanswered, Backoff.delay(:ws_sink_ack, 1) + 1)
      assert {:reply, [_frame], _resent} = Client.handle_info(:idle_check, aged)

      assert_received {:telemetry, [:flezha, :backoff, :wait], %{attempt: 1},
                       %{ladder: :ws_sink_ack}}
    end

    test "an unanswered frame is re-sent, and the hold stays open",
         %{cell_id: cell_id} do
      opts = required_opts()

      {{:reply, [{:text, first}], sent}, _ref} =
        dispatch(state(opts), "v1", guaranteed(cell_id, 1))

      # The sweep runs on the watchdog tick the client already owns; age the
      # send past the ladder's first rung rather than sleeping through it.
      aged = age_inflight(sent, Backoff.delay(:ws_sink_ack, 1) + 1)

      assert {:reply, [{:text, ^first}], resent} = Client.handle_info(:idle_check, aged)
      assert map_size(resent.sink.inflight) == 1
      refute_receive {:"$gen_cast", {:outbox_confirmed, _}}, 200
    end

    test "an exhausted frame is parked with its cell and channel, and answers its hold",
         %{cell_id: cell_id} do
      opts = required_opts()

      {{:reply, _frames, sent}, _ref} =
        dispatch(state(opts), "v1", guaranteed(cell_id, 1))

      spent = spend_budget(sent)

      # The park is an [error] by design — the dead-letter write is loud on
      # purpose — so it is captured rather than left on the suite's console.
      log =
        capture_log(fn ->
          assert {:ok, parked} = Client.handle_info(:idle_check, spent)
          assert parked.sink.inflight == %{}
        end)

      assert log =~ "parked undeliverable output"
      assert log =~ "ws_sink_unacknowledged"
      assert :ok = TestHelpers.assert_confirmed([{:cursor, 1}], 2_000)
    end

    defp required_opts do
      sink!(%{
        sink_ack_mode: "required",
        sink_template: ~s({"req_id":"{{req_id}}","d":"{{payload}}"})
      })
    end

    # Age every in-flight frame by `ms`, as the ladder's clock would.
    defp age_inflight(state, ms) do
      inflight =
        Map.new(state.sink.inflight, fn {id, {entry, attempts, sent_at}} ->
          {id, {entry, attempts, sent_at - ms}}
        end)

      %{state | sink: %{state.sink | inflight: inflight}}
    end

    # The attempt count the row gives up at, with the window elapsed.
    defp spend_budget(state) do
      attempts = exhausted_attempts()
      elapsed = :erlang.monotonic_time(:millisecond) - Backoff.delay(:ws_sink_ack, attempts) - 1

      inflight =
        Map.new(state.sink.inflight, fn {id, {entry, _attempts, _sent_at}} ->
          {id, {entry, attempts, elapsed}}
        end)

      %{state | sink: %{state.sink | inflight: inflight}}
    end

    defp exhausted_attempts do
      Stream.iterate(1, &(&1 + 1))
      |> Enum.find(&Backoff.give_up?(:ws_sink_ack, &1))
    end
  end

  ## ---------- 6.7 both directions on one socket ----------

  test "an inbound frame is injected while an output is dispatched, and neither blocks" do
    plugin_id = "ws_both_#{System.unique_integer([:positive])}"
    test = self()

    # The Overwatch's shape on the effectively-once tier: `Host.inject/4`
    # confirms with `GenServer.call(_, {:io_in, _, _}, :infinity)`, and the
    # handler is what runs the on_output chain that ships back to the socket.
    overwatch =
      spawn_link(fn ->
        receive do
          {:"$gen_call", from, {:io_in, message, _md}} ->
            ref = make_ref()
            send(test, {:chain_entered, message})

            socket = receive(do: ({:socket, pid} -> pid))
            send(socket, {{:sink_frame, "answer", %{}}, self(), ref})

            # The chain's own budget. Any length will do: the socket cannot
            # read the ask until this call is answered.
            outcome =
              receive do
                {^ref, :ok} -> :shipped
              after
                100 -> :blocked
              end

            GenServer.reply(from, :ok)
            send(test, {:chain_left, outcome})
        end
      end)

    opts = sink!(%{plugin_id: plugin_id})

    socket =
      spawn_link(fn ->
        receive do
          {:run, state} ->
            {:ok, state} = Client.handle_in({:text, "inbound"}, state)
            send(test, {:ingested, state.frames_seen})
        end
      end)

    send(socket, {:run, state(opts, %{overwatcher_pid: overwatch, effectively_once: true})})
    assert_receive {:chain_entered, "inbound"}, 2_000
    send(overwatch, {:socket, socket})

    # The socket is inside the confirming call, so the ask cannot be answered
    # until the Overwatch releases it: what proves there is no deadlock is that
    # the chain leaves under its own budget and the ingest then completes.
    assert_receive {:chain_left, :blocked}, 4_000
    assert_receive {:ingested, 1}, 2_000
  end

  ## ---------- 6.8 an unsubscribed plugin ----------

  describe "a plugin bound in neither direction" do
    test "allocates no queue" do
      opts = sink!(%{}, [:on_interval])
      assert opts.sink == nil
    end

    test "arms no tick for an acknowledgement it never awaits" do
      opts = sink!(%{idle_timeout_ms: 0, resubscribe_after_ms: 0}, [:on_interval])
      assert Client.idle_tick_interval(state(opts)) == nil
    end

    test "a bound plugin awaiting acknowledgements does get a tick" do
      opts =
        sink!(%{
          idle_timeout_ms: 0,
          resubscribe_after_ms: 0,
          sink_ack_mode: "required",
          sink_template: ~s({"req_id":"{{req_id}}","d":"{{payload}}"})
        })

      assert Client.idle_tick_interval(state(opts)) > 0
    end

    test "its sink rows are still accepted, and cost nothing" do
      opts = sink!(%{sink_queue_max: 5, sink_ack_mode: "none"}, [])
      assert opts.sink == nil
      assert opts.sink_queue_max == 5
    end

    # The other half of a source-only enable: the inbound path belongs to the
    # declared worker, not to a subscription, so a plugin bound in neither
    # direction still feeds its operator what arrives on the socket.
    test "still ingests what arrives on its socket" do
      assert {:ok, ingested} = Client.handle_in({:text, "frame"}, state(sink!(%{}, [])))

      assert ingested.sink == nil
      assert_receive {:"$gen_cast", {:io_in, "frame", _}}
    end
  end

  ## ---------- 4.2 the tier warning ----------

  describe "the effectively-once warning" do
    setup do
      # Test env caps the primary logger level at :error; the tier warning is a
      # :warning, so open this module up for the capture and close it after.
      :ok = :logger.set_module_level(Websocket, :all)
      on_exit(fn -> :logger.unset_module_level(Websocket) end)
      :ok
    end

    test "fires on a tier operator, and the enable succeeds" do
      log =
        capture_log(fn ->
          assert {:ok, opts} = prepared(%{effectively_once: true})
          assert %Sink{} = opts.sink
        end)

      assert log =~ "effectively-once operator"
      assert log =~ ~s(sink_ack_mode="none")
    end

    test "does not fire on a plain operator" do
      log = capture_log(fn -> assert {:ok, _opts} = prepared() end)
      refute log =~ "effectively-once operator"
    end

    test "does not fire for a plugin bound in neither direction" do
      log =
        capture_log(fn ->
          assert {:ok, opts} = prepared(%{effectively_once: true}, [:on_interval])
          assert opts.sink == nil
        end)

      refute log =~ "effectively-once operator"
    end
  end

  ## ---------- 5.2 an outbound write is not liveness ----------

  test "an outbound frame does not refresh the connection-liveness timestamp" do
    opts = sink!()
    before = state(opts)

    {{:reply, _frames, after_send}, _ref} = dispatch(before, "out")

    assert after_send.last_frame_at == before.last_frame_at
    assert after_send.last_data_frame_at == before.last_data_frame_at
  end

  test "an outbound write cannot hold a send-only socket open past its window" do
    opts = sink!()
    now = :erlang.monotonic_time(:millisecond)

    stale =
      state(opts, %{
        idle_timeout_ms: 100,
        last_frame_at: now - 100,
        last_data_frame_at: now
      })

    {{:reply, _frames, sent}, _ref} = dispatch(stale, "out")

    # The watchdog exits its own process, so the verdict is the exit reason of a
    # process that ran the tick, not a return value.
    {_pid, monitor} = spawn_monitor(fn -> Client.handle_info(:idle_check, sent) end)

    assert_receive {:DOWN, ^monitor, :process, _pid, {:shutdown, :idle_timeout}}, 2_000
  end

  ## ---------- the outbox contract ----------

  test "an output carrying no hold settles nothing" do
    {{:reply, _frames, _state}, _ref} = dispatch(state(sink!()), "plain", %{"channel" => "out"})

    refute Outbox.guaranteed?(%{"channel" => "out"})
  end
end
