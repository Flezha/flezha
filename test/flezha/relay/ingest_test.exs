defmodule FlezhaTest.Relay.IngestTest do
  @moduledoc """
  `relay-messaging`: the confirming ingest entry point and its
  verdicts. Every case drives `Overwatch.handle_call/3` directly on a struct —
  the state machine is the unit here, as in `overwatch_suspension_test`.

  The companion assertion is that the existing `{:io_in, _, _}` cast is
  unchanged, including its silent drop for a parked relay: a sender that
  needs the payload held uses the confirming path instead.
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Relay.{Ingest, Overwatch, Plugins}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host, as: PluginHost
  alias Flezha.Service.{Relay, RelayManager}
  alias Flezha.State.{InputLog, Paths, Supervisor, TestHelpers}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OverwatchHost, as: Host

  import Host, only: [state: 1, state: 2]
  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  @ui %{source: :http, principal: "console@_system"}

  defp instance_id(), do: "confirm_#{System.unique_integer([:positive])}"

  # Stand-in stream: the forward lands through `StreamChannel.io_in/3`, so the
  # test answers it and records what reached it. Returns an inert channel name.
  defp fake_channel(reply) do
    test_pid = self()

    stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, id, payload ->
      send(test_pid, {:channel_got, id, payload})
      reply
    end)

    :fake_channel
  end

  defp with_filter(state, pattern) do
    plugin =
      Flezha.Plugin.new(
        id: "filter",
        provider: Flezha.Plugin.Provider.Filter,
        opts: %{regex_pattern: pattern, inverted: true},
        namespace: "default",
        relay_id: state.id,
        hooks: [{:on_input, []}]
      )

    {:ok, table} = Table.bulk_subscribe(state.extensions, [{"filter", plugin}])
    %{state | extensions: table}
  end

  test "a healthy bound relay confirms :ok and the payload reaches the channel" do
    inst = instance_id()
    state = state(inst, channel: fake_channel(:ok))

    assert {:reply, :ok, _state} =
             Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)

    assert_receive {:channel_got, ^inst, {"hello", _md}}
  end

  test "a parked target reports not-ready, naming itself, instead of dropping" do
    inst = instance_id()
    state = state(inst, halt_reason: :sdk_start_failed)

    assert {:reply, {:error, {:not_accepting_input, ^inst}}, _state} =
             Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)
  end

  test "a payload filtered by an :on_input plugin reports absorbed" do
    state = state(instance_id()) |> with_filter("drop-me")

    assert {:reply, {:ok, :absorbed}, _state} =
             Overwatch.handle_call({:ingest, "drop-me now", %{}}, self(), state)
  end

  test "a filtered payload reads absorbed through Host.inject/5, not a bare :ok" do
    {:ok, host} = start_supervised({Host, state(instance_id()) |> with_filter("drop-me")})

    assert {:ok, :absorbed} = PluginHost.inject(host, "drop-me now", %{}, :verdict, 1_000)
  end

  # fails when: an input sent while a relay runs nowhere (a cold move's gap) is
  # counted as taken by a sender that waits for the answer, which would then
  # move its position past it; or a fire-and-forget send raises.
  test "a stopped Overwatch takes nothing, and a sender that waits learns so" do
    {:ok, host} = start_supervised({Host, state(instance_id())})
    :ok = stop_supervised(Host)
    refute Process.alive?(host)

    assert {:error, {:unreachable, _}} = PluginHost.inject(host, "v", %{}, :verdict, 1_000)
    assert {:noproc, _} = catch_exit(PluginHost.inject(host, "c", %{}, :confirming, 1_000))

    assert {:error, %Flezha.Error{code: "call.unavailable"}} =
             PluginHost.inject(host, "k", %{}, :call, 1_000)

    assert :ok = PluginHost.inject(host, "f", %{}, :fire_and_forget)
  end

  # fails when: an instance that ends with appends in flight leaves their
  # senders to learn nothing, so each sends again an input its log already took.
  test "an ending instance answers every sender its log is still journaling" do
    echo = spawn_link(&echo/0)
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, _, _, _}
    [taken, refused, lost] = for _ <- 1..3, do: make_ref()

    reqids =
      [{echo, {:ok, 7}, taken}, {echo, {:error, :disk}, refused}, {gone, :none, lost}]
      |> Enum.reduce(:gen_server.reqids_new(), fn {log, answer, tag}, reqids ->
        label = {{:ingest, {self(), tag}}, {"x", %{}}, 0}
        :gen_server.send_request(log, answer, label, reqids)
      end)

    id = instance_id()
    assert :ok = Ingest.settle_journaling(%{state(id) | journal_reqids: reqids}, 1_000)
    assert_received {^taken, :ok}
    assert_received {^refused, {:error, {:input_refused, ^id}}}
    assert_received {^lost, {:error, {:input_refused, ^id}}}
  end

  defp echo do
    receive do
      {:"$gen_call", from, answer} -> GenServer.reply(from, answer)
    end

    echo()
  end

  describe "the keys only the controller sets" do
    setup :set_mimic_global_until_exit

    @controller_keys ["flz.reply_to", "flz.replay", "flz.control", "flz.edge", "flz.offset"]

    test "no inject sets them, a call's included, which rides only the token its entry minted" do
      inst = instance_id()
      {:ok, host} = start_supervised({Host, state(inst, channel: fake_channel(:ok))})
      forged = Map.new(@controller_keys, &{&1, "true"}) |> Map.put("k", "v")

      assert :ok = PluginHost.inject(host, "x", forged, :confirming, 1_000)
      assert_receive {:channel_got, ^inst, {"x", %{"k" => "v"} = metadata}}
      assert Map.take(metadata, @controller_keys) == %{}

      caller = Task.async(fn -> PluginHost.inject(host, "c", forged, :call, 1_000) end)
      assert_receive {:channel_got, ^inst, {"c", %{"flz.reply_to" => "w:" <> _} = metadata}}
      assert Map.take(metadata, tl(@controller_keys)) == %{}
      Task.shutdown(caller, :brutal_kill)
    end

    # fails when: an `:on_input` hook can add a key only the controller sets.
    test "no :on_input hook sets them" do
      inst = instance_id()
      {:ok, host} = start_supervised({Host, state(inst, channel: fake_channel(:ok))})
      forged = Map.new(@controller_keys, &{&1, "true"})

      stub(Plugins, :transform_io, fn
        _state, :on_input, message, metadata ->
          {message, Map.merge(metadata, forged)}

        state, chain, message, metadata ->
          call_original(Plugins, :transform_io, [state, chain, message, metadata])
      end)

      assert :ok = PluginHost.inject(host, "x", %{"k" => "v"}, :confirming, 1_000)
      assert_receive {:channel_got, ^inst, {"x", %{"k" => "v"} = metadata}}
      assert Map.take(metadata, @controller_keys) == %{}
    end
  end

  describe "a durable steps relay's input log" do
    setup :set_mimic_global_until_exit

    setup do
      FlezhaTest.Case.install([:state_tree])
    end

    test "a suspended target confirms {:ok, :suspended} once the append is durable, and wakes" do
      inst = instance_id()
      test_pid = self()

      # No runner to rehydrate onto: the instance stays suspended and the payload
      # waits in the log, which is exactly the guarantee the verdict reports.
      stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, ^inst ->
        send(test_pid, :rehydrate_asked)
        nil
      end)

      state = state(inst, durable_steps: true, phase: :suspended, channel: nil)
      {:ok, host} = start_supervised({Host, state})

      assert {:ok, :suspended} = GenServer.call(host, {:ingest, "turn", %{}})
      assert [{0, "turn", _metadata}] = InputLog.undelivered(inst)
      assert_receive :rehydrate_asked
    end

    test "a duplicate absorbed by the input log reports absorbed" do
      inst = instance_id()
      state = state(inst, durable_steps: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})
      sent_once = %{"flz.msg_id" => "m-again", "flz.replay" => "true"}

      assert :ok = GenServer.call(host, {:ingest, "again", sent_once})
      assert_receive {:channel_got, ^inst, {"again", %{"flz.offset" => "0"} = metadata}}
      assert metadata["flz.msg_id"] == "m-again"
      refute Map.has_key?(metadata, "flz.replay")

      assert {:ok, :absorbed} = GenServer.call(host, {:ingest, "again", sent_once})
      refute_received {:channel_got, _, _}
    end

    test "Relay.send stamps opts[:msg_id], so a repeat the log holds is absorbed" do
      inst = instance_id()
      state = state(inst, durable_steps: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})
      stub(Flezha.Service.HubManager, :hub_alive?, fn _namespace -> true end)
      stub(Flezha.Service.HubManager, :get_pid, fn "default", ^inst -> {:ok, host} end)
      send_it = fn -> Relay.send("default", inst, "o-41", "", [msg_id: "orders/0/41"], @ui) end

      assert {:ok, %{to: ^inst, verdict: :delivered}} = send_it.()
      assert_receive {:channel_got, ^inst, {"o-41", %{"flz.msg_id" => "orders/0/41"}}}

      assert {:ok, %{to: ^inst, verdict: :absorbed}} = send_it.()
      refute_received {:channel_got, _, _}
    end

    # A log holding three records, the second corrupt: it refuses every append.
    defp refusing_log(inst) do
      for i <- 0..2, do: {:ok, _} = InputLog.append(inst, "m#{i}", %{})
      TestHelpers.close_input_log(inst)
      path = Paths.input_log_path(inst)
      [_first, second, _third] = TestHelpers.record_offsets(path)
      TestHelpers.corrupt_record_body(path, second)
      path
    end

    test "a durable-log refusal reports refused so the sender retries, logged once by the log" do
      inst = instance_id()
      path = refusing_log(inst)
      state = state(inst, durable_steps: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          for _ <- 1..3,
              do:
                assert(
                  {:error, {:input_refused, ^inst}} =
                    GenServer.call(host, {:ingest, "doomed", %{}})
                )
        end)

      # append-before-forward: nothing moved, and the refusal is told once, by
      # the log that refused its file, however many inputs it then refuses.
      assert [_, _] = String.split(log, "record_frame_refused")
      assert log =~ path
      refute_received {:channel_got, _, _}
    end

    # A start over a log that already holds records leaves its input for the
    # start's own call, which answers only once the log has taken it.
    defp assert_start_fails(call, code) do
      inst = instance_id()
      refusing_log(inst)
      seed = {"first", %{}, call}
      state = state(inst, durable_steps: true, channel: fake_channel(:ok), seed: seed)
      {:ok, host} = start_supervised({Host, state})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(self(), {:started, GenServer.call(host, {:registry_runner_set, false})})
        end)

      assert_received {:started, {:error, %Flezha.Error{code: ^code}}}
      assert log =~ ~r/record_frame_refused .*reason=crc_mismatch/
      refute_received {:channel_got, _, _}
      assert GenServer.call(host, :get_overwatch_state).calls.rows == %{}
    end

    test "a held input the log takes starts the relay once it is journaled" do
      inst = instance_id()

      state =
        state(inst, durable_steps: true, channel: fake_channel(:ok), seed: {"first", %{}, nil})

      {:ok, host} = start_supervised({Host, state})

      assert {:ok, "ex_1"} = GenServer.call(host, {:registry_runner_set, false})
      assert_received {:channel_got, ^inst, {"first", %{"flz.offset" => "0"}}}
    end

    test "a held --text input the log refuses fails the start" do
      assert_start_fails(nil, "relay.input_refused")
    end

    test "a held spawn call the log refuses fails the start, answering its caller unavailable" do
      deadline_at = System.system_time(:millisecond) + 60_000
      call = %{caller: "gw", call: "c1", created: true, factory: nil, deadline_at: deadline_at}
      assert_start_fails(call, "call.unavailable")
    end

    test "an unbound stream is not-ready for a non-durable target, :ok for a durable one" do
      inst = instance_id()

      assert {:reply, {:error, {:not_accepting_input, ^inst}}, _} =
               Overwatch.handle_call({:ingest, "x", %{}}, self(), state(inst, channel: nil))

      # A durable target answers `:ok` because the bind it armed re-forwards
      # what the log holds. A real unbound channel stands in for the window
      # between the control ack and the data stream the runner opens after it.
      {channel, _pid} = NodeTestHelper.unbound_stream_channel()

      {:ok, host} =
        start_supervised({Host, state(inst, durable_steps: true, channel: channel)})

      assert :ok = GenServer.call(host, {:ingest, "x", %{}})
      assert [{0, "x", _metadata}] = InputLog.undelivered(inst)
    end

    test "a durable target with no channel waits for its next bind" do
      # fails when: a journaled input whose re-forward has no channel to arm is
      # answered not-ready, telling the sender to retry a message the log holds.
      inst = instance_id()
      {:ok, host} = start_supervised({Host, state(inst, durable_steps: true, channel: nil)})

      assert {:ok, :suspended} = GenServer.call(host, {:ingest, "x", %{}})
      assert [{0, "x", _metadata}] = InputLog.undelivered(inst)
    end
  end

  test "an unresolvable target is not-found" do
    stub(Flezha.Service.HubManager, :get_pid, fn ns, id ->
      {:error, {:relay_not_found, ns, id}}
    end)

    assert {:error, {:relay_not_found, "default", "ghost"}} =
             RelayManager.send_io_confirming(
               "ghost",
               "default",
               "hello",
               %{}
             )
  end

  test "a target that exits reads unreachable, the one exit policy Host.inject/5 has" do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    stub(Flezha.Service.HubManager, :get_pid, fn "default", "gone" -> {:ok, pid} end)

    assert {:error, {:unreachable, :noproc}} =
             RelayManager.send_io_confirming("gone", "default", "hello", %{})
  end

  # Stand-in relay that answers the first `answered` ingests and holds every
  # later one until it is told to `:release` them.
  defmodule HoldingStub do
    use GenServer
    def start_link(answered), do: GenServer.start_link(__MODULE__, answered)
    def init(answered), do: {:ok, %{left: answered, held: [], cast: []}}

    def handle_call({:ingest, _message, _md}, _from, %{left: left} = state) when left > 0,
      do: {:reply, {:ok, :absorbed}, %{state | left: left - 1}}

    def handle_call({:ingest, _message, _md}, from, state),
      do: {:noreply, %{state | held: [from | state.held]}}

    def handle_call(:release, _from, state) do
      Enum.each(state.held, &GenServer.reply(&1, :ok))
      {:reply, :ok, %{state | held: []}}
    end

    def handle_call(:cast, _from, state), do: {:reply, Enum.reverse(state.cast), state}

    def handle_cast({:io_in, message, _md}, state),
      do: {:noreply, %{state | cast: [message | state.cast]}}
  end

  describe "send_io_confirming_many/5" do
    # Stand-in relay: answers `{:ingest, ..}` like an Overwatch, records
    # the order it saw inputs in and the deepest queue it ever had — the queue
    # is what proves the caller's round trips overlap.
    defmodule IngestStub do
      use GenServer
      def start_link(reply), do: GenServer.start_link(__MODULE__, reply)
      def init(reply), do: {:ok, %{reply: reply, seen: [], max_queued: 0}}

      def handle_call({:ingest, message, _md}, _from, state) do
        # An overlapping caller has the next request queued here before the
        # first is answered; a caller that waits for each answer never does.
        if state.seen == [] do
          NodeTestHelper.wait_until(
            fn -> Process.info(self(), :message_queue_len) > {:message_queue_len, 0} end,
            2_000,
            1
          )
        end

        {:message_queue_len, queued} = Process.info(self(), :message_queue_len)

        {:reply, state.reply,
         %{state | seen: [message | state.seen], max_queued: max(state.max_queued, queued)}}
      end

      def handle_call(:report, _from, state),
        do: {:reply, {Enum.reverse(state.seen), state.max_queued}, state}
    end

    test "inputs are ingested in order, round trips overlap, the window bounds them" do
      {:ok, stub} = IngestStub.start_link(:ok)
      stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:ok, stub} end)

      inputs = for i <- 1..20, do: {"m#{i}", %{}}

      verdicts =
        RelayManager.send_io_confirming_many(
          "target",
          "default",
          inputs,
          5_000,
          4
        )

      assert verdicts == List.duplicate(:ok, 20)
      {seen, max_queued} = GenServer.call(stub, :report)
      assert seen == Enum.map(inputs, &elem(&1, 0))
      assert max_queued >= 1, "requests never overlapped"
      assert max_queued <= 3, "more than the window was in flight"
    end

    test "verdicts come back per input, in input order" do
      {:ok, stub} = IngestStub.start_link({:ok, :absorbed})
      stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:ok, stub} end)

      assert [{:ok, :absorbed}, {:ok, :absorbed}] =
               RelayManager.send_io_confirming_many(
                 "target",
                 "default",
                 [{"a", %{}}, {"b", %{}}]
               )
    end

    test "an unresolvable target answers not-found for every input" do
      stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:error, "no pid found"} end)

      assert [{:error, :not_found}, {:error, :not_found}] =
               RelayManager.send_io_confirming_many(
                 "ghost",
                 "default",
                 [{"a", %{}}, {"b", %{}}]
               )
    end

    test "inputs the deadline cut answer an unreachable timeout each, rather than an exit" do
      {:ok, stub} = HoldingStub.start_link(0)
      stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:ok, stub} end)

      assert [{:error, {:unreachable, :timeout}}, {:error, {:unreachable, :timeout}}] =
               RelayManager.send_io_confirming_many(
                 "target",
                 "default",
                 [{"a", %{}}, {"b", %{}}],
                 1,
                 1
               )
    end
  end

  describe "Host.inject_many/4" do
    test "a deadline answers what came back and a timeout for the rest, and nothing after" do
      {:ok, stub} = HoldingStub.start_link(1)
      records = for m <- ["a", "b", "c"], do: {m, %{}}

      assert [
               {:ok, :absorbed},
               {:error, {:unreachable, :timeout}},
               {:error, {:unreachable, :timeout}}
             ] = PluginHost.inject_many(stub, records, :verdict, 20)

      # The two held replies are sent now, ahead of the release's own; the
      # abandoned requests drop them.
      assert :ok = GenServer.call(stub, :release)
      refute_received _
    end

    test "a batch keeps at most the ingest window in flight" do
      on_exit(TestHelpers.override_env(:plugin_ingest_window, 4))
      {:ok, stub} = __MODULE__.IngestStub.start_link(:ok)
      records = for i <- 1..20, do: {"m#{i}", %{}}

      assert PluginHost.inject_many(stub, records, :verdict, 5_000) == List.duplicate(:ok, 20)
      {seen, max_queued} = GenServer.call(stub, :report)
      assert seen == Enum.map(records, &elem(&1, 0))
      assert max_queued >= 1, "requests never overlapped"
      assert max_queued <= 3, "more than the window was in flight"
    end

    test "a relay it cannot reach answers unreachable for every record" do
      {pid, ref} = spawn_monitor(fn -> :ok end)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      assert [{:error, {:unreachable, :noproc}}, {:error, {:unreachable, :noproc}}] =
               PluginHost.inject_many(pid, [{"a", %{}}, {"b", %{}}], :verdict, 1_000)
    end

    test "fire-and-forget hands every record over in order and answers :ok" do
      {:ok, stub} = HoldingStub.start_link(0)
      records = for m <- ["a", "b", "c"], do: {m, %{}}

      assert :ok = PluginHost.inject_many(stub, records, :fire_and_forget, 0)
      assert GenServer.call(stub, :cast) == ["a", "b", "c"]
    end
  end

  describe "Host.inject_many/4 on a relay that journals its input" do
    setup :set_mimic_global_until_exit

    setup do
      FlezhaTest.Case.install([:state_tree])
    end

    test "500 records answer 500 ordered verdicts and share the log's syncs" do
      inst = instance_id()
      state = state(inst, effectively_once: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})

      # The first record opens the log, so its syncs can be traced from the next.
      assert [:ok] = PluginHost.inject_many(host, [{"m0", %{}}], :verdict, 5_000)
      {:ok, log} = Supervisor.find_or_start({inst, :input_log})
      :erlang.trace(log, true, [:receive])

      records = for i <- 1..500, do: {"m#{i}", %{}}
      verdicts = PluginHost.inject_many(host, records, :verdict, 30_000)
      :erlang.trace(log, false, [:receive])

      assert verdicts == List.duplicate(:ok, 500)

      assert Enum.map(InputLog.undelivered(inst), &elem(&1, 1)) ==
               ["m0" | Enum.map(records, &elem(&1, 0))]

      syncs = count_syncs(log)
      assert syncs > 0 and syncs < 500, "the batch took #{syncs} log syncs for 500 records"
    end

    test "a duplicate msg id in one batch reaches the relay once" do
      inst = instance_id()
      state = state(inst, durable_steps: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})
      twice = %{"flz.msg_id" => "m-twice"}

      assert [:ok, {:ok, :absorbed}] =
               PluginHost.inject_many(host, [{"x", twice}, {"x", twice}], :verdict, 5_000)

      assert_receive {:channel_got, ^inst, {"x", _metadata}}
      :sys.get_state(host)
      refute_received {:channel_got, _, _}
    end
  end

  defp count_syncs(log) do
    syncs =
      collect do
        {:trace, ^log, :receive, {:"$gen_cast", :fsync}} -> true
        {:trace, ^log, :receive, {:"$gen_call", _from, :fsync}} -> true
        {:trace, ^log, :receive, _other} -> false
      end

    Enum.count(syncs, & &1)
  end

  # `count` appends wait in the input log for credit.
  defp await_parked(inst, count) do
    {:ok, log} = Supervisor.find_or_start({inst, :input_log})

    assert NodeTestHelper.wait_until(
             fn -> :queue.len(:sys.get_state(log).waiters) == count end,
             2_000,
             1
           )
  end

  describe "the effectively-once journal" do
    setup :set_mimic_global_until_exit

    # A 200-byte record overfills the window on its own (an empty ledger admits
    # any size); the short records behind it park until it is consumed, and
    # three of them fit once it is.
    @window 128
    defp fill(), do: String.duplicate("x", 200)
    defp cell(inst), do: {inst, "task", "a"}

    setup do
      FlezhaTest.Case.install([:state_tree])
      on_exit(TestHelpers.override_env(:source_credit_window_bytes, @window))
      inst = instance_id()
      state = state(inst, effectively_once: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})

      # Fill the window: journaled at offset 0 and forwarded at once.
      assert :ok = GenServer.call(host, {:ingest, fill(), %{}})
      assert_receive {:channel_got, ^inst, {_fill, %{"flz.offset" => "0"}}}

      %{inst: inst, host: host}
    end

    test "zero credit parks the caller's call and nothing else", %{inst: inst, host: host} do
      parked = Task.async(fn -> GenServer.call(host, {:ingest, "b", %{}}) end)
      await_parked(inst, 1)
      refute Task.yield(parked, 0), "the caller must park at zero source credit"

      # The host runs on: a control call is answered and an `{:io_out}` chain is
      # run while the caller waits — a cast and then a call from one process are
      # handled in order, so the answered ping proves the chain ran.
      assert %Overwatch{} = GenServer.call(host, :get_overwatch_state)
      GenServer.cast(host, {:io_out, "out", %{}})
      assert :pong = GenServer.call(host, :ping)
      refute_received {:channel_got, _, _}

      # Consuming offset 0 mints the credit: the parked call answers.
      InputLog.record_consumed(inst, cell(inst), 0)
      assert :ok = Task.await(parked, 2_000)
      assert_receive {:channel_got, ^inst, {"b", %{"flz.offset" => "1"}}}
    end

    test "forward order is offset order and verdicts answer in call order, across a park",
         %{inst: inst, host: host} do
      # Issued from one process, so they reach the host — and the log — in order.
      reqids = :gen_server.reqids_new()
      reqids = :gen_server.send_request(host, {:ingest, "b", %{}}, :b, reqids)
      reqids = :gen_server.send_request(host, {:ingest, "c", %{}}, :c, reqids)
      await_parked(inst, 2)
      assert :timeout = :gen_server.wait_response(reqids, 0, true)

      InputLog.record_consumed(inst, cell(inst), 0)
      assert {{:reply, :ok}, :b, reqids} = :gen_server.wait_response(reqids, 2_000, true)
      assert {{:reply, :ok}, :c, _} = :gen_server.wait_response(reqids, 2_000, true)

      assert_receive {:channel_got, ^inst, {first, %{"flz.offset" => o1}}}
      assert_receive {:channel_got, ^inst, {second, %{"flz.offset" => o2}}}
      assert {[first, second], [o1, o2]} == {["b", "c"], ["1", "2"]}
    end

    test "casts into a parked relay are held in order and none is dropped",
         %{inst: inst, host: host} do
      for m <- ["b", "c", "d"], do: GenServer.cast(host, {:io_in, m, %{}})
      assert :pong = GenServer.call(host, :ping)
      refute_received {:channel_got, _, _}

      InputLog.record_consumed(inst, cell(inst), 0)

      forwarded =
        for _ <- 1..3 do
          assert_receive {:channel_got, ^inst, {m, %{"flz.offset" => offset}}}, 2_000
          {m, offset}
        end

      assert forwarded == [{"b", "1"}, {"c", "2"}, {"d", "3"}]
    end

    test "a log that dies mid-append answers refused after one re-issue, forwarding nothing",
         %{inst: inst, host: host} do
      test_pid = self()
      # A log that takes one append to its grave — and keeps being found.
      log = spawn(fn -> receive(do: (_append -> exit(:boom))) end)

      stub(Supervisor, :find_or_start, fn {^inst, :input_log} ->
        send(test_pid, :log_looked_up)
        {:ok, log}
      end)

      assert {:error, {:input_refused, ^inst}} = GenServer.call(host, {:ingest, "b", %{}})

      assert_receive :log_looked_up
      assert_receive :log_looked_up
      refute_received :log_looked_up
      refute_received {:channel_got, _, _}
    end
  end

  test "an ingested message counts its bytes in, and forwarded unless it is filtered" do
    inst = instance_id()
    ref = LivenessCounters.register_relay("default", inst)
    state = state(inst, channel: fake_channel(:ok), liveness_ref: ref) |> with_filter("drop-me")

    assert {:reply, :ok, state} = Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)

    assert {:reply, {:ok, :absorbed}, _state} =
             Overwatch.handle_call({:ingest, "drop-me now", %{}}, self(), state)

    row = Enum.find(LivenessCounters.snapshot_all().relays, &(&1.relay_id == inst))

    assert {row.io_in_received, row.forwarded, row.filtered_nil} == {2, 1, 1}
    assert row.bytes_in == byte_size("hello") + byte_size("drop-me now")
    assert row.bytes_forwarded == byte_size("hello")
  end

  test "the existing cast keeps its silent drop for a parked relay" do
    state = state(instance_id(), halt_reason: :sdk_start_failed, channel: fake_channel(:ok))

    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "hello", %{}}, state)
    refute_received {:channel_got, _, _}
  end

  test "the parked cast's drop is counted, so the loss shows in the liveness metrics" do
    inst = instance_id()
    ref = LivenessCounters.register_relay("default", inst)

    state =
      state(inst,
        halt_reason: :sdk_start_failed,
        channel: fake_channel(:ok),
        liveness_ref: ref
      )

    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "a", %{}}, state)
    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "b", %{}}, state)
    refute_received {:channel_got, _, _}

    row = Enum.find(LivenessCounters.snapshot_all().relays, &(&1.relay_id == inst))

    assert row[:halted_drops] == 2
    assert row[:io_in_received] == 0, "a dropped cast was never received for processing"
  end
end
