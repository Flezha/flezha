defmodule FlezhaTest.Relay.CallsTest do
  @moduledoc """
  `relay-calls`: the table of outside callers an Overwatch holds, its call
  entry point, and the way an answer leaves both output routes before any
  plugin sees it. The Overwatch cases drive the state machine inside a bare
  host process, as `ingest_test` does, so a reply owed to a caller is a real
  `GenServer.call` that has not returned.
  """
  # async: false: writes Application env; Mimic global mode; telemetry handlers.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Error
  alias Flezha.Relay.{Builder, ClassRegistry, Handover, Lifecycle, Overwatch, Plugins}
  alias Flezha.Relay.Calls.Inbound
  alias Flezha.Relay.Calls.Token
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host, as: PluginHost
  alias Flezha.State.{InputLog, TestHelpers, WriterRegistry}
  alias FlezhaTest.Helper.CallHarness
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OverwatchHost, as: Host
  alias FlezhaTest.Helper.Telemetry

  @callee %{namespace: "default", relay_id: "callee"}

  setup do
    Telemetry.capture(
      for(e <- [:answered, :timeout, :answer_dropped, :refused], do: [:flezha, :call, e]),
      as: fn [:flezha, :call, event], measurements, metadata ->
        {:call_event, event, measurements, metadata}
      end
    )

    :ok
  end

  defp from, do: {self(), make_ref()}

  defp admit(calls, from, deadline_ms \\ 5_000) do
    {:ok, token, calls} = Inbound.admit(calls, @callee, from, %{"channel" => "in"}, deadline_ms)
    {token, calls}
  end

  describe "the table" do
    test "a call is admitted under a fresh token until the relay owes call_pending_max" do
      on_exit(TestHelpers.override_env(:call_pending_max, 2))

      {t1, calls} = admit(%Inbound{}, from())
      {t2, calls} = admit(calls, from())

      assert "w:" <> _ = t1
      assert t1 != t2
      assert byte_size(t1) <= 255
      assert Inbound.waiting(calls) == 2

      assert {:error, %Error{code: "call.busy"}} =
               Inbound.admit(calls, @callee, from(), %{"channel" => "in"}, 5_000)

      assert_received {:call_event, :refused, _, %{code: "call.busy", relay_id: "callee"}}

      # An answered row no longer counts against the bound.
      {:answered, calls, _} = Inbound.divert(calls, @callee, "a", %{"flz.reply_to" => t1})

      assert {:ok, _t3, _calls} =
               Inbound.admit(calls, @callee, from(), %{"channel" => "in"}, 5_000)
    end

    test "the first output answers; every output goes on without its token, none counted as dropped" do
      {pid, ref} = caller = from()
      assert pid == self()
      {token, calls} = admit(%Inbound{}, caller)

      assert :output = Inbound.divert(calls, @callee, "record", %{"channel" => "audit"})

      {:answered, calls, %{"channel" => "ok"} = first} =
        Inbound.divert(calls, @callee, "first", %{"flz.reply_to" => token, "channel" => "ok"})

      assert first == %{"channel" => "ok"}
      assert_received {^ref, {:ok, "first", %{"channel" => "ok"}}}
      assert_received {:call_event, :answered, %{duration_us: _}, %{relay_id: "callee"}}

      assert {:answered, _, %{}} =
               Inbound.divert(calls, @callee, "second", %{"flz.reply_to" => token})

      refute_received {^ref, _}

      for stray <- ["", "w:nobody"] do
        assert {:answered, _, %{"k" => "v"} = output} =
                 Inbound.divert(calls, @callee, "x", %{"flz.reply_to" => stray, "k" => "v"})

        refute Map.has_key?(output, "flz.reply_to")
      end

      refute_received {:call_event, :answer_dropped, _, _}
    end

    test "a named answer channel picks the answer; the first output answers without one" do
      {_, named_ref} = named = from()
      {_, first_ref} = first = from()

      {:ok, named_token, calls} =
        Inbound.admit(
          %Inbound{},
          @callee,
          named,
          %{"channel" => "in", "flz.call_answer_channel" => "quotes"},
          5_000
        )

      {first_token, calls} = admit(calls, first)

      Enum.reduce(
        [
          {named_token, "audit", "a1"},
          {first_token, "audit", "a2"},
          {named_token, "quotes", "q1"},
          {first_token, "quotes", "q2"}
        ],
        calls,
        fn {token, channel, message}, calls ->
          metadata = %{"flz.reply_to" => token, "channel" => channel}
          {:answered, calls, _output} = Inbound.divert(calls, @callee, message, metadata)
          calls
        end
      )

      assert_received {^named_ref, {:ok, "q1", %{"channel" => "quotes"}}}
      assert_received {^first_ref, {:ok, "a2", %{"channel" => "audit"}}}
      refute_received {^named_ref, _}
      refute_received {^first_ref, _}
    end

    test "the answer channel stays in the row, not on the input; an empty one names none" do
      metadata = %{"channel" => "in", "flz.call_answer_channel" => "quotes"}
      {:ok, token, calls} = Inbound.admit(%Inbound{}, @callee, from(), metadata, 5_000)
      assert calls.rows[token].answer_channel == "quotes"

      empty = %{metadata | "flz.call_answer_channel" => ""}
      {:ok, unnamed, calls} = Inbound.admit(calls, @callee, from(), empty, 5_000)
      assert calls.rows[unnamed].answer_channel == nil

      assert Token.entry_metadata(metadata, token) == %{
               "channel" => "in",
               "flz.reply_to" => token
             }
    end

    test "a deadline answers a waiting caller call.timeout and retires an answered row silently" do
      {_, waiting_ref} = waiting = from()
      {_, answered_ref} = answered = from()
      {waiting_token, calls} = admit(%Inbound{}, waiting, 100)
      {answered_token, calls} = admit(calls, answered, 100)

      {:answered, calls, _} =
        Inbound.divert(calls, @callee, "a", %{"flz.reply_to" => answered_token})

      assert_received {^answered_ref, {:ok, "a", _}}

      calls = Inbound.expire(calls, @callee, waiting_token)

      assert_received {^waiting_ref, {:error, %Error{code: "call.timeout", details: details}}}
      assert details["deadline_ms"] == 100
      assert_received {:call_event, :timeout, %{count: 1}, %{relay_id: "callee"}}

      calls = Inbound.expire(calls, @callee, answered_token)
      refute_received {^answered_ref, _}
      assert calls.rows == %{}

      # Its late output names no row now, and is an ordinary output.
      assert {:answered, _, %{}} =
               Inbound.divert(calls, @callee, "late", %{"flz.reply_to" => waiting_token})

      refute_received {:call_event, :answer_dropped, _, _}
    end

    test "an ended instance answers each waiting caller call.callee_ended with its status" do
      {_, r1} = c1 = from()
      {_, r2} = c2 = from()
      {_, r3} = c3 = from()
      {_t1, calls} = admit(%Inbound{}, c1)
      {_t2, calls} = admit(calls, c2)
      {t3, calls} = admit(calls, c3)
      {:answered, calls, _} = Inbound.divert(calls, @callee, "a", %{"flz.reply_to" => t3})
      assert_received {^r3, {:ok, "a", _}}

      assert %Inbound{rows: rows} = Inbound.end_all(calls, @callee, "error")
      assert rows == %{}

      for ref <- [r1, r2] do
        assert_received {^ref, {:error, %Error{code: "call.callee_ended", details: details}}}
        assert details["status"] == "error"
      end

      refute_received {^r3, _}
    end

    test "a handover's rows keep their deadline, the time in flight counted against it" do
      {token, calls} = admit(%Inbound{}, from(), 10_000)
      left = Inbound.left_ms(calls.rows[token].deadline_at)
      assert [{^token, row}] = Inbound.export(calls)

      absorbed = Inbound.absorb([{token, row}])
      assert Inbound.left_ms(absorbed.rows[token].deadline_at) in (left - 1_000)..left
      assert Inbound.waiting(absorbed) == 1

      # The same row arriving after 4 s in flight has 4 s less left.
      late = Inbound.absorb([{token, Map.update!(row, :deadline_at, &(&1 - 4_000))}])
      assert Inbound.left_ms(late.rows[token].deadline_at) in (left - 5_000)..(left - 4_000)
    end

    test "a hook can neither add, drop nor rewrite a token" do
      arrived = %{"flz.reply_to" => "w:t", "channel" => "in"}

      assert Token.restore(%{"channel" => "in"}, arrived) == arrived
      assert Token.restore(%{"flz.reply_to" => "w:other"}, arrived) == %{"flz.reply_to" => "w:t"}
      assert Token.restore(%{"flz.reply_to" => "w:forged", "k" => "v"}, %{}) == %{"k" => "v"}
    end
  end

  describe "the Overwatch" do
    setup :set_mimic_global_until_exit

    setup do
      test_pid = self()

      # Every output the `:on_output` chain is given is reported, with its
      # metadata, so a test can show an answer reaches it without its token.
      stub(Plugins, :transform_io, fn state, chain, message, metadata ->
        if chain == :on_output, do: send(test_pid, {:on_output, message, metadata})
        Mimic.call_original(Plugins, :transform_io, [state, chain, message, metadata])
      end)

      # Only the stand-in channel answers; a real unbound one is not ready.
      stub(Flezha.Runner.StreamChannel, :io_in, fn
        :fake_channel, id, payload ->
          send(test_pid, {:channel_got, id, payload})
          :ok

        channel, id, payload ->
          Mimic.call_original(Flezha.Runner.StreamChannel, :io_in, [channel, id, payload])
      end)

      %{inst: "callee_#{System.unique_integer([:positive])}"}
    end

    # Its input reaches the stand-in stream the setup stubs.
    defp state(inst, overrides \\ []),
      do: Host.state(inst, Keyword.merge([channel: :fake_channel], overrides))

    defp notice(token, entry) do
      %{
        token: token,
        entry: entry,
        error: "boom",
        channel: "in",
        offset: 1,
        tasklet: "t",
        attempts: 3,
        new: true
      }
    end

    defp call(host, message, metadata \\ %{}, deadline_ms \\ 5_000) do
      Task.async(fn -> Inbound.call(host, message, metadata, deadline_ms, "callee", "cli") end)
    end

    # The token the call's input reached the stream with.
    defp called_token(inst) do
      assert_receive {:channel_got, ^inst, {_message, %{"flz.reply_to" => token}}}, 2_000
      token
    end

    test "the answer reaches its caller and then the :on_output chain, without a token",
         %{inst: inst} do
      {:ok, host} = start_supervised({Host, state(inst)})
      forged = %{"channel" => "in", "flz.reply_to" => "o:gateway:c1"}
      caller = call(host, "hello", forged)

      token = called_token(inst)
      assert "w:" <> _ = token, "the entry point minted its own token over the caller's"

      GenServer.cast(host, {:io_out, "record", %{"channel" => "audit"}})
      GenServer.cast(host, {:io_out, "forged", %{"flz.reply_to" => "o:gateway:c1"}})
      GenServer.cast(host, {:io_out, "hello", %{"flz.reply_to" => token, "channel" => "out"}})

      assert Task.await(caller) == {:ok, "hello", %{"channel" => "out"}}
      assert_receive {:on_output, "record", %{"channel" => "audit"}}
      assert_receive {:on_output, "forged", forged_output}
      assert_receive {:on_output, "hello", %{"channel" => "out"} = answer}
      refute Map.has_key?(forged_output, "flz.reply_to")
      refute Map.has_key?(answer, "flz.reply_to")
      refute_received {:call_event, :answer_dropped, _, _}
    end

    test "a call naming its answer channel is answered from that channel alone", %{inst: inst} do
      {:ok, host} = start_supervised({Host, state(inst)})
      caller = call(host, "EUR/USD", %{"flz.call_answer_channel" => "quotes"})

      assert_receive {:channel_got, ^inst, {"EUR/USD", input}}, 2_000
      refute Map.has_key?(input, "flz.call_answer_channel")
      token = input["flz.reply_to"]

      GenServer.cast(host, {:io_out, "seen", %{"flz.reply_to" => token, "channel" => "audit"}})
      GenServer.cast(host, {:io_out, "1.08", %{"flz.reply_to" => token, "channel" => "quotes"}})

      assert Task.await(caller) == {:ok, "1.08", %{"channel" => "quotes"}}
      assert_receive {:on_output, "seen", %{"channel" => "audit"}}
      assert_receive {:on_output, "1.08", %{"channel" => "quotes"}}
    end

    test "an answer in an outbox batch goes to its caller and its entry still settles",
         %{inst: inst} do
      test_pid = self()
      stub(WriterRegistry, :lookup, fn :cell_writer -> {:ok, test_pid} end)
      {:ok, host} = start_supervised({Host, state(inst)})
      caller = call(host, "hello")
      token = called_token(inst)

      outputs = [
        {"hello", %{"flz.reply_to" => token, "channel" => "out"}, %{owner: :cell_writer, key: 7}},
        {"record", %{"channel" => "audit"}, %{owner: :cell_writer, key: 8}}
      ]

      GenServer.cast(host, {:io_out_batch, outputs})

      assert {:ok, "hello", %{"channel" => "out"}} = Task.await(caller)
      assert_receive {:"$gen_cast", {:outbox_settled_many, [{7, 0}, {8, 0}]}}
      assert_received {:on_output, "record", _}
      assert_received {:on_output, "hello", %{"channel" => "out"}}
    end

    test "an answer a bind task held answers when it is released", %{inst: inst} do
      {:ok, host} = start_supervised({Host, state(inst)})
      caller = call(host, "hello")
      token = called_token(inst)

      # A rebind's bind in flight holds the instance's output.
      task = self()

      :sys.replace_state(host, fn state ->
        binding = %{pid: task, then: nil, from: state.phase, extensions: state.extensions}
        %{state | halt_reason: :sdk_start_failed, bind_task: binding}
      end)

      GenServer.cast(host, {:io_out, "hello", %{"flz.reply_to" => token, "channel" => "out"}})
      assert %{held_outputs: [_held], calls: %{rows: %{^token => _}}} = :sys.get_state(host)
      refute Task.yield(caller, 0)

      ExUnit.CaptureLog.capture_log(fn -> send(host, {:EXIT, self(), :bind_failed}) end)

      assert {:ok, "hello", _} = Task.await(caller)
      assert_received {:on_output, "hello", _}
    end

    test "a dead-lettered called input answers its caller call.failed", %{inst: inst} do
      {:ok, host} = start_supervised({Host, state(inst)})
      caller = call(host, "poison")
      token = called_token(inst)

      GenServer.cast(host, {:dead_lettered, notice("w:other", "#{inst}/t/2")})
      GenServer.cast(host, {:dead_lettered, notice(token, "#{inst}/t/1")})

      assert {:error, %Error{code: "call.failed", details: details}} = Task.await(caller)
      assert details["dlq_ref"] == "#{inst}/t/1"
      assert details["error"] == "boom"
      assert :sys.get_state(host).calls.rows == %{}
    end

    # The `relay-outputs` scenario "An outside caller sees the answer alone".
    test "a caller waiting beside a listener receives the answer alone", %{inst: inst} do
      listener =
        Flezha.Plugin.new(
          id: "listener",
          provider: Flezha.Plugin.Provider.MonitoredIOForward,
          opts: %{
            recv_pid: self(),
            plugin_id: "listener",
            relay_id: inst,
            namespace: "default"
          },
          namespace: "default",
          relay_id: inst,
          hooks: [{:on_output, []}, {:on_preview, []}]
        )

      {:ok, table} = Table.bulk_subscribe(state(inst).extensions, [{"listener", listener}])
      {:ok, host} = start_supervised({Host, state(inst, extensions: table)})
      caller = call(host, "turn")
      token = called_token(inst)

      GenServer.cast(host, {:io_part, "hel", %{"channel" => "events", "flz.part" => "0"}})
      GenServer.cast(host, {:io_out, "hello", %{"flz.reply_to" => token, "channel" => "events"}})

      assert Task.await(caller) == {:ok, "hello", %{"channel" => "events"}}
      assert_receive {:preview_forward, {"hel", %{"channel" => "events"}}}
      assert_receive {:message_forward, {"hello", %{"channel" => "events"}}}
      refute_received {:preview_forward, _}
    end

    test "a call with no answer by its deadline is told call.timeout; the input stays accepted",
         %{inst: inst} do
      {:ok, host} = start_supervised({Host, state(inst)})
      caller = call(host, "slow", %{}, 5_000)
      token = called_token(inst)

      assert CallHarness.fire_call_timer(host, {:call_deadline, token}) in 1..5_000
      assert {:error, %Error{code: "call.timeout"}} = Task.await(caller)
      assert :sys.get_state(host).calls.rows == %{}

      GenServer.cast(host, {:io_out, "late", %{"flz.reply_to" => token}})
      assert :pong = GenServer.call(host, :ping)
      assert_received {:on_output, "late", %{}}
      refute_received {:call_event, :answer_dropped, _, _}
    end

    # Fails when: the exit of the relay's process reaches a networked caller as text.
    test "a relay process that exits under a call is unavailable, its exit kept for the log" do
      gone = spawn(fn -> receive do: (_ -> exit({:shutdown, "vault key s3cr3t-7f3a"})) end)

      assert {:error, %Error{code: "call.unavailable"} = error} =
               Inbound.call(gone, "x", %{}, 1_000, "callee", "http")

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert Error.withheld(error, "door").details["reason"] =~
                   "the controller's log names the cause"
        end)

      assert log =~ "s3cr3t-7f3a"
    end

    test "a call the relay cannot take is refused at once, nothing ingested", %{inst: inst} do
      on_exit(TestHelpers.override_env(:call_pending_max, 1))

      parked = start_supervised!({Host, state(inst, halt_reason: :sdk_start_failed)}, id: :parked)
      assert {:error, %Error{code: "call.unavailable"}} = Task.await(call(parked, "x"))

      {:ok, host} = start_supervised({Host, state(inst)})
      waiting = call(host, "first")
      _token = called_token(inst)
      assert {:error, %Error{code: "call.busy"}} = Task.await(call(host, "second"))
      refute_received {:channel_got, _, _}
      Task.shutdown(waiting, :brutal_kill)

      ClassRegistry.ensure_table()
      :ok = ClassRegistry.put_partitioning("once_chat", %{keyless: false, each: :once})
      builder = %Builder{id: inst, factory_id: "once_chat", run: :steps, opts: []}

      once =
        start_supervised!({Host, state(inst, factory_id: "once_chat", builder: builder)},
          id: :once
        )

      first = call(once, "first")
      _token = called_token(inst)
      assert {:error, %Error{code: "call.not_accepted"}} = Task.await(call(once, "second"))
      refute_received {:channel_got, _, _}
      Task.shutdown(first, :brutal_kill)

      for code <- ["call.unavailable", "call.busy", "call.not_accepted"],
          do: assert_received({:call_event, :refused, _, %{code: ^code}})
    end

    test "an input a hook filters answers call.absorbed; one no runner takes call.unavailable",
         %{inst: inst} do
      plugin =
        Flezha.Plugin.new(
          id: "filter",
          provider: Flezha.Plugin.Provider.Filter,
          opts: %{regex_pattern: "drop-me", inverted: true},
          namespace: "default",
          relay_id: inst,
          hooks: [{:on_input, []}]
        )

      {:ok, table} = Table.bulk_subscribe(state(inst).extensions, [{"filter", plugin}])
      filtered = start_supervised!({Host, state(inst, extensions: table)}, id: :filtered)
      assert {:error, %Error{code: "call.absorbed"}} = Task.await(call(filtered, "drop-me"))

      unbound = start_supervised!({Host, state(inst, channel: nil)}, id: :unbound)
      assert {:error, %Error{code: "call.unavailable"}} = Task.await(call(unbound, "x"))
    end

    test "a plugin cannot forge, and a hook cannot strip, a token", %{inst: inst} do
      {:ok, host} = start_supervised({Host, state(inst)})

      # A feeding plugin's confirming inject carries a token: the entry drops it.
      assert :ok = GenServer.call(host, {:ingest, "forged", %{"flz.reply_to" => "w:guess"}})
      assert_receive {:channel_got, ^inst, {"forged", metadata}}
      refute Map.has_key?(metadata, "flz.reply_to")

      # An `:on_input` chain that rebuilds the metadata keeps the call's token.
      stub(Plugins, :transform_io, fn _state, :on_input, message, _metadata -> {message, %{}} end)
      caller = call(host, "hello")
      assert_receive {:channel_got, ^inst, {"hello", %{"flz.reply_to" => "w:" <> _ = token}}}
      GenServer.cast(host, {:io_out, "hi", %{"flz.reply_to" => token}})
      assert {:ok, "hi", _} = Task.await(caller)
    end

    test "a call on an asleep durable steps instance is journaled with its token and waits",
         %{inst: inst} do
      FlezhaTest.Case.install([:state_tree])
      stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, ^inst -> nil end)
      asleep = state(inst, durable_steps: true, phase: :suspended, channel: nil)
      {:ok, host} = start_supervised({Host, asleep})

      caller = call(host, "turn")

      assert NodeTestHelper.wait_until(fn -> InputLog.undelivered(inst) != [] end, 2_000)
      assert [{0, "turn", %{"flz.reply_to" => "w:" <> _ = token}}] = InputLog.undelivered(inst)
      assert Map.has_key?(:sys.get_state(host).calls.rows, token)
      refute Task.yield(caller, 0), "an accepted call waits for its answer"

      GenServer.cast(host, {:io_out, "answer", %{"flz.reply_to" => token}})
      assert {:ok, "answer", _} = Task.await(caller)
    end
  end

  describe "a plugin's call through Host.inject/5" do
    setup :set_mimic_global_until_exit

    setup do
      test_pid = self()

      stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, id, payload ->
        send(test_pid, {:channel_got, id, payload})
        :ok
      end)

      inst = "gateway_#{System.unique_integer([:positive])}"
      %{inst: inst, host: start_supervised!({Host, state(inst)})}
    end

    # A feeding provider's worker: it holds its host and calls it.
    defp worker_call(host, metadata, deadline_ms) do
      Task.async(fn ->
        PluginHost.inject(host, "envelope", metadata, :call, deadline_ms)
      end)
    end

    test "answers the payload and the answering channel; a forged token is replaced",
         %{inst: inst, host: host} do
      worker = worker_call(host, %{"channel" => "http", "flz.reply_to" => "o:gateway:c1"}, 5_000)
      token = called_token(inst)
      assert "w:" <> _ = token

      GenServer.cast(host, {:io_out, "stray", %{"flz.reply_to" => "o:gateway:c1"}})

      GenServer.cast(
        host,
        {:io_out, "answer", %{"flz.reply_to" => token, "channel" => "accepted"}}
      )

      assert Task.await(worker) == {:ok, "answer", %{"channel" => "accepted"}}
    end

    test "times out with the Overwatch's call.timeout, never its own wait",
         %{inst: inst, host: host} do
      worker = worker_call(host, %{"channel" => "http"}, 5_000)
      token = called_token(inst)

      assert CallHarness.fire_call_timer(host, {:call_deadline, token}) in 1..5_000
      assert {:error, %Error{code: "call.timeout"}} = Task.await(worker)
    end

    test "a host that is gone answers call.unavailable", %{host: host} do
      :ok = stop_supervised(Host)
      refute Process.alive?(host)

      assert {:error, %Error{code: "call.unavailable"}} =
               Task.await(worker_call(host, %{"channel" => "http"}, 100))
    end
  end

  describe "a handover and an ending instance" do
    setup :set_mimic_global_until_exit

    setup do
      stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, _id, _payload -> :ok end)
      %{inst: "moved_#{System.unique_integer([:positive])}"}
    end

    # Bounded and bound to no runner: a handover ships nothing to one.
    defp bare(inst, overrides \\ []) do
      builder = %Builder{id: inst, factory_id: "chat", run: :steps, opts: []}
      Host.state(inst, [runner_id: nil, channel: :fake_channel, builder: builder] ++ overrides)
    end

    test "a handover carries a waiting caller to the new node, whose answer reaches it",
         %{inst: inst} do
      {:ok, source} = start_supervised({Host, bare(inst)}, id: :source)
      caller = Task.async(fn -> Inbound.call(source, "hello", %{}, 5_000, inst, "cli") end)
      short = Task.async(fn -> Inbound.call(source, "slow", %{}, 4_000, inst, "cli") end)

      assert NodeTestHelper.wait_until(
               fn -> map_size(:sys.get_state(source).calls.rows) == 2 end,
               2_000
             )

      handover = :sys.get_state(source) |> Handover.prepare()
      assert_received :calls_handed_over
      send(source, :calls_handed_over)
      assert %{calls: %{rows: rows}, cause: :move} = :sys.get_state(source)
      assert rows == %{}, "the source owes the moved callers nothing"

      {:ok, target} = start_supervised({Host, {:absorb, bare(inst), handover}}, id: :target)

      [{token, _row}, {short_token, short_row}] =
        Enum.sort_by(elem(handover, 0).calls, fn {_t, row} -> -row.deadline_ms end)

      # The departing process stopping is no crash of the call's Overwatch: a
      # caller told of the move watches it no longer.
      assert NodeTestHelper.wait_until(
               fn -> {:process, source} not in elem(Process.info(caller.pid, :monitors), 1) end,
               2_000,
               1
             )

      :ok = stop_supervised(:source)
      refute Task.yield(caller, 0)

      GenServer.cast(target, {:io_out, "moved", %{"flz.reply_to" => token}})
      assert {:ok, "moved", _} = Task.await(caller)

      # The other row keeps its original deadline on the new node.
      assert :sys.get_state(target).calls.rows[short_token].deadline_at == short_row.deadline_at
      assert CallHarness.fire_call_timer(target, {:call_deadline, short_token}) in 1..4_000
      assert {:error, %Error{code: "call.timeout"}} = Task.await(short)
    end

    test "a failed steps run answers both waiting callers call.callee_ended", %{inst: inst} do
      FlezhaTest.Case.install([:state_tree])
      {_, r1} = c1 = from()
      {_, r2} = c2 = from()
      {_t1, calls} = admit(%Inbound{}, c1)
      {_t2, calls} = admit(calls, c2)
      terminal = %{status: "error", error: "boom", undo: ""}

      ExUnit.CaptureLog.capture_log(fn ->
        Lifecycle.terminate(bare(inst, calls: calls, terminal: terminal), :normal)
      end)

      for ref <- [r1, r2] do
        assert_received {^ref, {:error, %Error{code: "call.callee_ended", details: details}}}
        assert details["status"] == "error"
      end
    end
  end

  # `relay-listener`: the calls a relay takes and how each ended, as the
  # `on_call` hook hands them to the test standing in for its listener.
  describe "the on_call listeners" do
    setup :set_mimic_global_until_exit

    setup do
      test_pid = self()

      stub(Flezha.Runner.StreamChannel, :io_in, fn
        :fake_channel, id, payload ->
          send(test_pid, {:channel_got, id, payload})
          :ok

        channel, id, payload ->
          Mimic.call_original(Flezha.Runner.StreamChannel, :io_in, [channel, id, payload])
      end)

      %{inst: "watched_#{System.unique_integer([:positive])}"}
    end

    defp calls_listener(inst) do
      Flezha.Plugin.new(
        id: "calls",
        provider: Flezha.Plugin.Provider.MonitoredIOForward,
        opts: %{recv_pid: self(), plugin_id: "calls", relay_id: inst, namespace: "default"},
        namespace: "default",
        relay_id: inst,
        hooks: [{:on_call, []}]
      )
    end

    # `state` with a calls listener sending to the test, and `others` beside it.
    defp watched(state, others \\ []) do
      {:ok, table} =
        Table.bulk_subscribe(state.extensions, [{"calls", calls_listener(state.id)} | others])

      %{state | extensions: table}
    end

    defp event(kind) do
      assert_receive {:message_forward, {payload, %{"flz.call_event" => ^kind} = metadata}}, 2_000
      {payload, metadata}
    end

    defp error_code(document), do: Flezha.SDK.Error.from_document(document).code

    defp relay_call(overrides) do
      Map.merge(
        %{
          caller: "orders",
          call: "c1",
          deadline_at: System.system_time(:millisecond) + 5_000,
          created: false,
          factory: nil
        },
        overrides
      )
    end

    # Also the `plugin-relay-hooks` scenario "A call reaches a calls listener and
    # nothing else": the sink and the output listener see the answer alone.
    test "an answered call: one call event, then answered, under an id that is not the token",
         %{inst: inst} do
      test_pid = self()
      outputs = spawn_link(fn -> tag_outputs(test_pid) end)

      output_listener =
        Flezha.Plugin.new(
          id: "out",
          provider: Flezha.Plugin.Provider.MonitoredIOForward,
          opts: %{recv_pid: outputs, plugin_id: "out", relay_id: inst, namespace: "default"},
          namespace: "default",
          relay_id: inst,
          hooks: [{:on_output, []}, {:on_preview, []}]
        )

      sink =
        Flezha.Plugin.new(
          id: "sink",
          provider: Flezha.Plugin.Provider.BeamMsgForward,
          opts: %{recv_pid: self()},
          namespace: "default",
          relay_id: inst,
          hooks: [{:on_output, []}]
        )

      state = watched(state(inst), [{"out", output_listener}, {"sink", sink}])
      host = start_supervised!({Host, state})
      started = System.system_time(:millisecond)
      caller = call(host, "hello", %{"channel" => "in", "flz.call_source" => "forged"})
      token = called_token(inst)

      {"hello", called} = event("call")

      assert %{
               "flz.call_id" => id,
               "flz.call_source" => "cli",
               "channel" => "in",
               "flz.call_deadline_ms" => 5_000,
               "flz.call_at" => at
             } = called

      assert at in started..System.system_time(:millisecond)
      assert id != token and not String.contains?(id, token)
      assert id == Token.call_id(token)
      refute Map.has_key?(called, "flz.reply_to")
      refute Map.has_key?(called, "flz.call_answer_channel")

      GenServer.cast(host, {:io_out, "hi", %{"flz.reply_to" => token, "channel" => "out"}})
      assert {:ok, "hi", _} = Task.await(caller)

      {"hi", answered} = event("answered")
      assert %{"flz.call_id" => ^id, "channel" => "out", "flz.call_duration_ms" => ms} = answered
      assert is_integer(ms) and ms >= 0

      assert_receive {:output_listener, {"hi", %{"channel" => "out"}}}, 2_000
      assert_receive {:message_forward, "hi"}, 2_000

      # An output after the answer reaches the output listener behind anything
      # the answer sent it.
      GenServer.cast(host, {:io_out, "drained", %{"channel" => "out"}})
      assert_receive {:output_listener, {"drained", _}}, 2_000
      refute_received {:output_listener, _}
      refute_received {:message_forward, {_, %{"flz.call_event" => _}}}
    end

    test "a call past its deadline: timed_out with call.timeout, and no answer", %{inst: inst} do
      host = start_supervised!({Host, watched(state(inst))})
      started = System.monotonic_time(:millisecond)
      caller = call(host, "slow", %{}, 5_000)
      token = called_token(inst)

      {"slow", %{"flz.call_id" => id}} = event("call")
      CallHarness.fire_call_timer(host, {:call_deadline, token})
      assert {:error, %Error{code: "call.timeout"}} = Task.await(caller)
      {document, timed_out} = event("timed_out")
      assert error_code(document) == "call.timeout"
      assert %{"flz.call_id" => ^id, "flz.call_duration_ms" => ms} = timed_out
      assert ms in 0..(System.monotonic_time(:millisecond) - started)
      refute_received {:message_forward, {_, %{"flz.call_event" => "answered"}}}
    end

    test "a busy relay refuses at admission: the call and its refusal together",
         %{inst: inst} do
      on_exit(TestHelpers.override_env(:call_pending_max, 1))
      host = start_supervised!({Host, watched(state(inst))})
      waiting = call(host, "first")
      _token = called_token(inst)
      {"first", %{"flz.call_id" => first}} = event("call")

      assert {:error, %Error{code: "call.busy"}} = Task.await(call(host, "second", %{}, 5_000))
      {"second", %{"flz.call_id" => second, "flz.call_source" => "cli"}} = event("call")
      {document, %{"flz.call_id" => ^second, "flz.call_duration_ms" => 0}} = event("refused")
      assert error_code(document) == "call.busy"
      assert second != first
      Task.shutdown(waiting, :brutal_kill)
    end

    test "refused after admission: absorbed, untaken and dead-lettered inputs", %{inst: inst} do
      filter =
        Flezha.Plugin.new(
          id: "filter",
          provider: Flezha.Plugin.Provider.Filter,
          opts: %{regex_pattern: "drop-me", inverted: true},
          namespace: "default",
          relay_id: inst,
          hooks: [{:on_input, []}]
        )

      filtered = start_supervised!({Host, watched(state(inst), [{"filter", filter}])}, id: :f)
      assert {:error, %Error{code: "call.absorbed"}} = Task.await(call(filtered, "drop-me"))
      {"drop-me", %{"flz.call_id" => id}} = event("call")
      {document, %{"flz.call_id" => ^id}} = event("refused")
      assert error_code(document) == "call.absorbed"

      unbound = start_supervised!({Host, watched(state(inst, channel: nil))}, id: :u)
      assert {:error, %Error{code: "call.unavailable"}} = Task.await(call(unbound, "x"))
      {"x", _called} = event("call")
      {document, _refused} = event("refused")
      assert error_code(document) == "call.unavailable"

      host = start_supervised!({Host, watched(state(inst))}, id: :d)
      caller = call(host, "poison", %{"channel" => "in"})
      token = called_token(inst)
      {"poison", %{"flz.call_id" => id}} = event("call")
      GenServer.cast(host, {:dead_lettered, notice(token, "#{inst}/t/1")})
      assert {:error, %Error{code: "call.failed"}} = Task.await(caller)
      {document, %{"flz.call_id" => ^id, "channel" => "in"}} = event("refused")
      assert error_code(document) == "call.failed"
    end

    test "an ended instance: each waiting call refused call.callee_ended" do
      test_pid = self()
      callee = Map.put(@callee, :activity, &send(test_pid, {:message_forward, {&1, &2}}))
      {t1, calls} = admit(%Inbound{}, from())
      {t2, calls} = admit(calls, from())

      Inbound.end_all(calls, callee, "error")

      ids =
        for _ <- 1..2 do
          {document, %{"flz.call_id" => id, "channel" => "in"}} = event("refused")
          assert error_code(document) == "call.callee_ended"
          id
        end

      assert Enum.sort(ids) == Enum.sort([Token.call_id(t1), Token.call_id(t2)])
    end

    test "an ended instance: a relay's call it took and never ran is refused with no row" do
      test_pid = self()
      callee = Map.put(@callee, :activity, &send(test_pid, {:message_forward, {&1, &2}}))
      tail = Token.relay_token(relay_call(%{}))

      Inbound.end_all(%Inbound{}, callee, "error", [tail])

      {document, refused} = event("refused")
      assert error_code(document) == "call.callee_ended"

      assert %{"flz.call_id" => id, "flz.call_duration_ms" => 0} = refused
      assert id == Token.call_id(tail)
      refute Map.has_key?(refused, "channel")
    end

    test "a restarted callee's answer to a relay's call it keeps no row for is announced",
         %{inst: inst} do
      host = start_supervised!({Host, watched(state(inst))})
      token = Token.relay_token(relay_call(%{}))

      GenServer.cast(host, {:io_out, "late", %{"flz.reply_to" => token, "channel" => "out"}})

      {"late", answered} = event("answered")
      assert %{"flz.call_id" => id, "channel" => "out", "flz.call_duration_ms" => 0} = answered
      assert id == Token.call_id(token)
      refute_received {:message_forward, {_, %{"flz.call_event" => "call"}}}
    end

    test "a relay's call: source relay and its caller, announced once, refused or timed out",
         %{inst: inst} do
      host = start_supervised!({Host, watched(state(inst))}, id: :taken)
      call = relay_call(%{answer_channel: "quotes"})
      assert :ok = GenServer.call(host, {:call, "turn", %{"channel" => "in"}, call})

      {"turn", called} = event("call")

      assert %{
               "flz.call_source" => "relay",
               "flz.call_caller" => "orders",
               "flz.call_answer_channel" => "quotes",
               "channel" => "in",
               "flz.call_deadline_ms" => left
             } = called

      assert left in 4_000..5_000

      # Sent again while its row waits: the same row, not announced again.
      GenServer.call(host, {:call, "turn", %{"channel" => "in"}, call})
      refute_received {:message_forward, {_, %{"flz.call_event" => "call"}}}

      unbound = start_supervised!({Host, watched(state(inst, channel: nil))}, id: :untaken)
      assert {:error, :not_ready} = GenServer.call(unbound, {:call, "x", %{}, relay_call(%{})})
      {"x", %{"flz.call_id" => id}} = event("call")
      {document, %{"flz.call_id" => ^id}} = event("refused")
      assert error_code(document) == "call.unavailable"

      soon = relay_call(%{call: "c2"})
      assert :ok = GenServer.call(host, {:call, "late", %{}, soon})
      {"late", %{"flz.call_id" => id}} = event("call")
      CallHarness.fire_call_timer(host, {:call_deadline, Token.relay_token(soon)})
      {document, %{"flz.call_id" => ^id}} = event("timed_out")
      assert error_code(document) == "call.timeout"
    end

    test "a feeding plugin's call is announced as plugin, and the runner never sees it",
         %{inst: inst} do
      host = start_supervised!({Host, watched(state(inst))})

      worker =
        Task.async(fn ->
          PluginHost.inject(host, "envelope", %{"flz.call_source" => "ui"}, :call, 5_000)
        end)

      assert_receive {:channel_got, ^inst, {"envelope", input}}, 2_000
      refute Map.has_key?(input, "flz.call_source")
      {"envelope", %{"flz.call_source" => "plugin"}} = event("call")
      GenServer.cast(host, {:io_out, "ok", %{"flz.reply_to" => input["flz.reply_to"]}})
      assert {:ok, "ok", _} = Task.await(worker)
    end

    test "a handover: the outcome comes from the arriving process under the call's id",
         %{inst: inst} do
      builder = %Builder{id: inst, factory_id: "chat", run: :steps, opts: []}
      bare = state(inst, runner_id: nil, builder: builder)
      {:ok, source} = start_supervised({Host, watched(bare)}, id: :source)
      caller = Task.async(fn -> Inbound.call(source, "hello", %{}, 5_000, inst, "ui") end)
      token = called_token(inst)
      {"hello", %{"flz.call_id" => id, "flz.call_source" => "ui"}} = event("call")

      handover = :sys.get_state(source) |> Handover.prepare()
      send(source, :calls_handed_over)
      {:ok, target} = start_supervised({Host, {:absorb, bare, handover}}, id: :target)
      :ok = stop_supervised(:source)
      # The arriving relay binds its listener again from its declared entry.
      listener = calls_listener(inst)

      :sys.replace_state(target, fn state ->
        {:ok, table} = Table.bulk_subscribe(state.extensions, [{"calls", listener}])
        %{state | extensions: table}
      end)

      GenServer.cast(target, {:io_out, "moved", %{"flz.reply_to" => token}})
      assert {:ok, "moved", _} = Task.await(caller)
      {"moved", %{"flz.call_id" => ^id}} = event("answered")
    end

    test "with no on_call subscriber nothing is built", %{inst: inst} do
      assert %{activity: nil} = Overwatch.callee(state(inst))
      assert %{activity: activity} = Overwatch.callee(watched(state(inst)))
      assert is_function(activity, 2)

      host = start_supervised!({Host, state(inst)})
      caller = call(host, "hello", %{}, 5_000)
      token = called_token(inst)
      CallHarness.fire_call_timer(host, {:call_deadline, token})
      assert {:error, %Error{code: "call.timeout"}} = Task.await(caller)
      refute_received {:message_forward, _}
    end
  end

  defp tag_outputs(test_pid) do
    receive do
      {:message_forward, frame} -> send(test_pid, {:output_listener, frame})
      _other -> :ok
    end

    tag_outputs(test_pid)
  end
end
