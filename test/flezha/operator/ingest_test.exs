defmodule FlezhaTest.Operator.IngestTest do
  @moduledoc """
  `operator-to-operator-messaging`: the confirming ingest entry point and its
  verdicts. Every case drives `Overwatch.handle_call/3` directly on a struct —
  the state machine is the unit here, as in `overwatch_suspension_test`.

  The companion assertion is that the existing `{:io_in, _, _}` cast is
  unchanged, including its silent drop for a parked operator: a sender that
  needs the payload held uses the confirming path instead.
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operator.{Builder, Overwatch}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.OperatorManager
  alias Flezha.State.{InputLog, Supervisor, TestHelpers}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper

  defp instance_id(), do: "confirm_#{System.unique_integer([:positive])}"

  # A live process around the struct, for the cases whose unit is the host's
  # loop: a durable log answers the host's mailbox, and a parked caller is a
  # call that has not returned.
  defmodule Host do
    use GenServer
    def start_link(state), do: GenServer.start_link(__MODULE__, state)
    def init(state), do: {:ok, state}
    defdelegate handle_call(msg, from, state), to: Overwatch
    defdelegate handle_cast(msg, state), to: Overwatch
    defdelegate handle_info(msg, state), to: Overwatch
  end

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

  defp state(inst, overrides \\ []) do
    base = %Overwatch{
      id: inst,
      namespace: "default",
      runner_id: "ex_1",
      factory_id: "chat",
      builder: %Builder{id: inst, factory_id: "chat", kind: :unbounded, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__())
    }

    struct!(base, overrides)
  end

  defp with_filter(state, pattern) do
    plugin =
      Flezha.Plugin.new(
        id: "filter",
        provider: Flezha.Plugin.Provider.Filter,
        opts: %{regex_pattern: pattern, inverted: true},
        namespace: "default",
        operator_id: state.id,
        hooks: [{:on_input, []}]
      )

    {:ok, table} = Table.bulk_subscribe(state.extensions, [{"filter", plugin}])
    %{state | extensions: table}
  end

  test "a healthy bound operator confirms :ok and the payload reaches the channel" do
    inst = instance_id()
    state = state(inst, channel: fake_channel(:ok))

    assert {:reply, :ok, _state} =
             Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)

    assert_receive {:channel_got, ^inst, {"hello", _md}}
  end

  test "a parked target reports not-ready instead of dropping" do
    state = state(instance_id(), init_error: :sdk_start_failed)

    assert {:reply, {:error, :not_ready}, _state} =
             Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)
  end

  test "a payload filtered by an :on_input plugin reports absorbed" do
    state = state(instance_id()) |> with_filter("drop-me")

    assert {:reply, {:ok, :absorbed}, _state} =
             Overwatch.handle_call({:ingest, "drop-me now", %{}}, self(), state)
  end

  describe "a durable bounded's input log" do
    setup :set_mimic_global_until_exit

    setup do
      FlezhaTest.Case.install([:state_tree])
    end

    # A writer that refuses every append.
    defmodule RefusingWriter do
      use GenServer
      def start_link(_), do: GenServer.start_link(__MODULE__, nil)
      def init(nil), do: {:ok, nil}
      def handle_call(_append, _from, state), do: {:reply, {:error, :enospc}, state}
    end

    test "a suspended target confirms {:ok, :suspended} once the append is durable, and wakes" do
      inst = instance_id()
      test_pid = self()

      # No runner to rehydrate onto: the instance stays suspended and the payload
      # waits in the log, which is exactly the guarantee the verdict reports.
      stub(Flezha.Service.OperatorManager, :assign_runner, fn _, _, ^inst ->
        send(test_pid, :rehydrate_asked)
        nil
      end)

      state = state(inst, durable_bounded: true, suspended: true, channel: nil)
      {:ok, host} = start_supervised({Host, state})

      assert {:ok, :suspended} = GenServer.call(host, {:ingest, "turn", %{}})
      assert [{0, "turn", _metadata}] = InputLog.undelivered(inst)
      assert_receive :rehydrate_asked
    end

    test "a duplicate absorbed by the input log reports absorbed" do
      inst = instance_id()
      state = state(inst, durable_bounded: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})
      sent_once = %{"flz.msg_id" => "m-again"}

      assert :ok = GenServer.call(host, {:ingest, "again", sent_once})
      assert_receive {:channel_got, ^inst, {"again", %{"flz.offset" => "0"}}}

      assert {:ok, :absorbed} = GenServer.call(host, {:ingest, "again", sent_once})
      refute_received {:channel_got, _, _}
    end

    test "a durable-log refusal reports refused so the sender retries" do
      inst = instance_id()
      {:ok, writer} = start_supervised(RefusingWriter)
      stub(Supervisor, :find_or_start, fn {^inst, :input_log} -> {:ok, writer} end)

      state = state(inst, durable_bounded: true, channel: fake_channel(:ok))
      {:ok, host} = start_supervised({Host, state})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, :refused} = GenServer.call(host, {:ingest, "doomed", %{}})
        end)

      # append-before-forward: the refusal is loud, and names why nothing moved.
      assert log =~ "durable input append failed for #{inst}"
      assert log =~ ":enospc"
      assert log =~ "input NOT forwarded"

      refute_received {:channel_got, _, _}
    end

    test "an unbound stream is not-ready for a non-durable target, :ok for a durable one" do
      inst = instance_id()

      assert {:reply, {:error, :not_ready}, _} =
               Overwatch.handle_call({:ingest, "x", %{}}, self(), state(inst, channel: nil))

      # A durable target answers `:ok` because the bind re-forwards what the
      # log holds — which needs a channel that can still bind. A real unbound
      # one stands in for the window between the control ack and the data
      # stream the runner opens after it.
      {channel, _pid} = NodeTestHelper.unbound_stream_channel()

      {:ok, host} =
        start_supervised({Host, state(inst, durable_bounded: true, channel: channel)})

      assert :ok = GenServer.call(host, {:ingest, "x", %{}})
      assert [{0, "x", _metadata}] = InputLog.undelivered(inst)
    end

    test "a durable target with no channel that can ever bind is refused" do
      inst = instance_id()
      {:ok, host} = start_supervised({Host, state(inst, durable_bounded: true, channel: nil)})

      # Nothing will re-forward this, so `:ok` would be the accepted-and-dropped
      # claim the input-delivery guarantee forbids. The log still holds the
      # payload, and a retry carrying its id is absorbed by the log's index.
      assert {:error, :not_ready} = GenServer.call(host, {:ingest, "x", %{}})
      assert [{0, "x", _metadata}] = InputLog.undelivered(inst)
    end
  end

  test "an unresolvable target is not-found" do
    stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:error, "no pid found"} end)

    assert {:error, :not_found} =
             OperatorManager.send_io_confirming(
               "ghost",
               "default",
               "hello",
               %{}
             )
  end

  describe "send_io_confirming_many/5" do
    # Stand-in operator: answers `{:ingest, ..}` like an Overwatch, records
    # the order it saw inputs in and the deepest queue it ever had — the queue
    # is what proves the caller's round trips overlap.
    defmodule IngestStub do
      use GenServer
      def start_link(reply), do: GenServer.start_link(__MODULE__, reply)
      def init(reply), do: {:ok, %{reply: reply, seen: [], max_queued: 0}}

      def handle_call({:ingest, message, _md}, _from, state) do
        # An overlapping caller has the next request queued here before this
        # one is answered; a caller that waits for each answer never does.
        NodeTestHelper.wait_until(
          fn -> Process.info(self(), :message_queue_len) > {:message_queue_len, 0} end,
          50,
          1
        )

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
        OperatorManager.send_io_confirming_many(
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
               OperatorManager.send_io_confirming_many(
                 "target",
                 "default",
                 [{"a", %{}}, {"b", %{}}]
               )
    end

    test "an unresolvable target answers not-found for every input" do
      stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:error, "no pid found"} end)

      assert [{:error, :not_found}, {:error, :not_found}] =
               OperatorManager.send_io_confirming_many(
                 "ghost",
                 "default",
                 [{"a", %{}}, {"b", %{}}]
               )
    end
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
      refute Task.yield(parked, 150), "the caller must park at zero source credit"

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
      assert :timeout = :gen_server.wait_response(reqids, 150, true)

      InputLog.record_consumed(inst, cell(inst), 0)
      assert {{:reply, :ok}, :b, reqids} = :gen_server.wait_response(reqids, 2_000, true)
      assert {{:reply, :ok}, :c, _} = :gen_server.wait_response(reqids, 2_000, true)

      assert_receive {:channel_got, ^inst, {first, %{"flz.offset" => o1}}}
      assert_receive {:channel_got, ^inst, {second, %{"flz.offset" => o2}}}
      assert {[first, second], [o1, o2]} == {["b", "c"], ["1", "2"]}
    end

    test "casts into a parked operator are held in order and none is dropped",
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

      assert {:error, :refused} = GenServer.call(host, {:ingest, "b", %{}})

      assert_receive :log_looked_up
      assert_receive :log_looked_up
      refute_received :log_looked_up
      refute_received {:channel_got, _, _}
    end
  end

  test "the existing cast keeps its silent drop for a parked operator" do
    state = state(instance_id(), init_error: :sdk_start_failed, channel: fake_channel(:ok))

    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "hello", %{}}, state)
    refute_received {:channel_got, _, _}
  end

  test "the parked cast's drop is counted, so the loss shows in the liveness metrics" do
    inst = instance_id()
    ref = LivenessCounters.register_operator(inst)

    state =
      state(inst,
        init_error: :sdk_start_failed,
        channel: fake_channel(:ok),
        liveness_ref: ref
      )

    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "a", %{}}, state)
    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "b", %{}}, state)
    refute_received {:channel_got, _, _}

    row = Enum.find(LivenessCounters.snapshot_all().operators, &(&1.operator_id == inst))

    assert row[:parked_drops] == 2
    assert row[:io_in_received] == 0, "a dropped cast was never received for processing"
  end
end
