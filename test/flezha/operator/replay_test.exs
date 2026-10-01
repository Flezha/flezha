defmodule FlezhaTest.Operator.ReplayAdoptionTest do
  @moduledoc """
  `Replay.adopt_moved_state/3` adopting an operator's durable state from
  another controller node, once per arrival and never from a restore.

  A partition rescale gathers the whole group's Cells onto the node that ran
  it and stamps that node on every new member's opts. ProcessHub then places the
  members wherever it likes, so a member that lands elsewhere has to pull its
  class tree across before it restores — otherwise it cold-starts on top of
  state that exists, just not here. Only a FOREIGN node means anything: a local
  value is already where the ordinary restore looks, and pulling from a node
  that has since gone, on every bind, would be noise.
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operator.Builder
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.Replay
  alias Flezha.State.Adoption

  setup :set_mimic_global_until_exit

  defp state(operator_id) do
    %Overwatch{
      id: operator_id,
      namespace: "adoption-test",
      factory_id: "candles",
      runner_id: "runner-1",
      builder: %Builder{id: operator_id, factory_id: "candles", run: :flow}
    }
  end

  defp expect_adopt(reply_to) do
    stub(Adoption, :adopt_newest, fn operator_class, source ->
      send(reply_to, {:adopted, source, operator_class})
      :ok
    end)
  end

  # The restore enumerates the class's Cells off-process: the message this
  # returns marks that it ran.
  defp expect_scan(reply_to) do
    scanned = {:scanned, make_ref()}

    stub(Flezha.State.Paths, :cells_of, fn _operator_class ->
      send(reply_to, scanned)
      []
    end)

    scanned
  end

  defp adopt(operator_id, opts, hub_answer \\ :error) do
    builder = %Builder{id: operator_id, factory_id: "candles", run: :flow, opts: opts}
    Replay.adopt_moved_state(%{state(operator_id) | builder: builder}, builder, hub_answer)
  end

  @peer :"peer@127.0.0.1"

  # fails when: the source stays on the operator's options, so every later bind
  # of the same instance adopts from it again.
  test "a member placed off the assembling node adopts from it once" do
    expect_adopt(self())

    operator = adopt("ohlc#p1", state_source_node: @peer)

    assert_received {:adopted, @peer, "ohlc#p1"}
    refute Keyword.has_key?(operator.opts, :state_source_node)
  end

  test "the operator_class opt names what is adopted, not the instance id" do
    expect_adopt(self())

    adopt("ohlc#p1", state_source_node: @peer, operator_class: "shared_class")

    assert_received {:adopted, @peer, "shared_class"}
  end

  test "a member placed on the assembling node adopts from no node" do
    expect_adopt(self())

    adopt("ohlc#p0", state_source_node: node())

    assert_received {:adopted, nil, "ohlc#p0"}
  end

  test "an operator that was never rescaled adopts from no node" do
    expect_adopt(self())

    adopt("plain", [])

    assert_received {:adopted, nil, "plain"}
  end

  # fails when: the restore a heal kicks on a bound operator adopts again, and
  # swaps the files of writers that are taking its frames.
  test "a restore kicked on a bound operator adopts nothing" do
    expect_adopt(self())
    scanned = expect_scan(self())

    :ok = Replay.dispatch_restore(state("ohlc#p1"), [state_source_node: @peer], "runner-1")

    assert_receive ^scanned, 2_000
    refute_received {:adopted, _, _}
  end
end

defmodule FlezhaTest.Operator.RestoreRefusalTest do
  @moduledoc """
  A refused restore fails the bind instead of cold-starting the Cell
  (`guard-cross-sdk-placement`).

  `RestoreInitState` is an awaited control round trip and the controller acts
  on its answer. Every way an SDK can refuse a fold — a language mismatch, a
  schema mismatch, a CRC failure, a truncated change log, an apply error —
  means the same thing here: the Cell's durable state did not reach the
  runner, so folding new inputs on top of empty state would lose it silently.

  A genuinely empty Cell is a SUCCESS, not a refusal, and must still bind.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.

  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic

  import ExUnit.CaptureLog, only: [with_log: 1]

  alias Flezha.Operator.Builder
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.Replay
  alias Flezha.State.Paths
  alias Flezha.State.RecordFrame
  alias Flezha.State.Restore
  alias Flezha.State.TestHelpers
  alias Flezha.State.WalWriter

  @java 2

  # Both refusal cases stub the SDK with this and then assert the log line
  # carries it back: the diagnostic is what they test, not noise they emit.
  @refusal "sdk_lang mismatch: this SDK is rust (1), the record was written by java (2)"

  defp cell(base) do
    {"#{base}_#{System.unique_integer([:positive])}", "p0", "k"}
  end

  defp assert_refusal_logged(log, cell_id) do
    assert log =~ "restore REFUSED"
    assert log =~ inspect(cell_id)
    assert log =~ @refusal
  end

  # fails when: the bind's restore runs off-process, so its `RestoreInitState`
  # can reach the runner after the hand-over the Overwatch absorbs next and an
  # input after it, resetting the runner's state to the WAL's.
  test "the bind's restore has reached the runner before the bind goes on" do
    {class, _, _} = id = cell("bind")
    TestHelpers.write_wal(id, 0..1)
    on_exit(fn -> Flezha.State.Supervisor.delete_cell(id) end)
    test_pid = self()

    stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, cell_id ->
      send(test_pid, {:restored, cell_id})
      {:ok, <<>>}
    end)

    state = %Overwatch{
      id: class,
      namespace: "_system",
      factory_id: "f",
      runner_id: "runner_1",
      builder: %Builder{id: class, factory_id: "f", run: :flow}
    }

    assert :ok = Replay.restore_on_bind(state, state.builder)

    # Already answered when the bind goes on, not on its way.
    assert_received {:restored, ^id}
  end

  describe "await_restore/2" do
    test "reports the SDK's refusal, with telemetry naming the cell and reason" do
      id = cell("refused")

      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _cell ->
        {:error, @refusal}
      end)

      :telemetry.attach(
        "restore-refusal-test",
        [:flezha, :state, :restore_refused],
        fn _event, measurements, metadata, pid ->
          send(pid, {:refused, measurements, metadata})
        end,
        self()
      )

      on_exit(fn -> :telemetry.detach("restore-refusal-test") end)

      {result, log} = with_log(fn -> Replay.await_restore("runner_1", id) end)

      assert result == {:error, @refusal}
      assert_receive {:refused, %{count: 1}, %{cell: ^id, runner: "runner_1", reason: @refusal}}
      assert_refusal_logged(log, id)
    end

    test "an unanswered round trip is not a refusal" do
      # A dead control unit, a timeout, a runner shutting down: the SDK
      # reported nothing, so there is no verdict on the state. Parking here
      # would turn ordinary teardown churn into stuck operators.
      for reason <- [:timeout, :no_control_unit, {:control_unit_exit, {:shutdown, :normal}}] do
        stub(Flezha.State.Commands, :await_restore_init_state, fn _r, _e -> {:error, reason} end)
        assert Replay.await_restore("runner_1", cell("unanswered")) == :ok
      end
    end

    test "a successful restore is plain :ok" do
      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _cell ->
        {:ok, <<>>}
      end)

      assert Replay.await_restore("runner_1", cell("fine")) == :ok
    end
  end

  describe "the tier bind" do
    test "a refused restore stops before the journal replay" do
      id = cell("tier_refused")
      TestHelpers.write_wal_of_lang(id, @java)

      stub(Flezha.State.Supervisor, :find_or_start, fn _cell -> {:ok, self()} end)

      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _cell ->
        {:error, @refusal}
      end)

      # Re-forwarding the input tail onto a Cell whose baseline never landed
      # is the silent loss this guards, so it must not be reached.
      stub(Flezha.State.InputLog, :tail, fn _class ->
        flunk("the input tail was read after a refused restore")
      end)

      {result, log} = with_log(fn -> Replay.restore_cells_awaited(tier_state(id)) end)

      assert result == {:error, @refusal}
      assert_refusal_logged(log, id)
    end

    test "a Cell with no state binds: it is never restored in the first place" do
      id = cell("empty")
      path = Paths.wal_path(id)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "")

      refute Restore.has_persisted_state?(id),
             "a zero-length WAL is no state — an assembled payload from it would " <>
               "rebuild a live Cell to init_state"
    end
  end

  describe "Restore.sdk_lang/1" do
    test "reads byte 0 of the first WAL record when there is no snapshot" do
      id = cell("wal_only")
      TestHelpers.write_wal_of_lang(id, @java)

      assert Restore.sdk_lang(id) == @java
    end

    test "is nil for a Cell with no durable state" do
      assert Restore.sdk_lang(cell("nothing")) == nil
    end

    test "is nil for a truncated WAL rather than a guess" do
      id = cell("truncated")
      path = Paths.wal_path(id)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, <<0::64, 4::32>>)

      assert Restore.sdk_lang(id) == nil
    end
  end

  describe "a durable log the controller's own reader refused" do
    test "journaled inputs are not re-forwarded from the prefix that verified" do
      id = cell("il_refused")
      {class, _, _} = id

      path = Paths.input_log_path(class)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "there is a journal here")

      stub(Flezha.State.InputLog, :tail, fn ^class -> {:error, {:crc_mismatch, 0, 36}} end)

      stub(Flezha.State.InputLog, :read_range, fn _class, _from, _to ->
        flunk("a refused input log was re-forwarded from its prefix")
      end)

      state = tier_state(id)
      assert Replay.reforward_undelivered_tail(state) == state
    end

    test "a correction refuses rather than replaying over a WAL that did not decode" do
      id = cell("corr_refused")
      header = TestHelpers.stub_header()
      {:ok, writer} = Flezha.State.Supervisor.find_or_start(id)

      for n <- 0..5, do: :ok = WalWriter.append(writer, n, header, "x" <> Integer.to_string(n))

      path = Paths.wal_path(id)
      [_zero, _one, corrupt | _] = TestHelpers.record_offsets(path)
      size = RecordFrame.file_size(path)
      TestHelpers.corrupt_record_body(path, corrupt)

      # The correction's own report is a warning, under the suite's `:error` gate.
      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

      {result, log} = with_log(fn -> Replay.run_replay(tier_state(id), id, 4) end)

      assert {:error, {:crc_mismatch, 1, ^corrupt}} = result
      assert log =~ "durable record refused (crc_mismatch)"
      assert log =~ "correction refused"
      assert RecordFrame.file_size(path) == size
    end
  end

  defp tier_state({operator_class, _, _}) do
    %Overwatch{
      id: operator_class,
      namespace: "default",
      runner_id: "runner_1",
      factory_id: "f",
      effectively_once: true,
      builder: %Builder{id: operator_class, factory_id: "f", run: :flow, opts: []}
    }
  end
end

defmodule FlezhaTest.Operator.BindReforwardTest do
  @moduledoc """
  `input-delivery-guarantee`: an input the Controller accepts is delivered or
  refused, never accepted-and-dropped — plus §6.3, that the `io_in` path counts
  an unbound-stream drop instead of discarding the channel's answer silently.

  The precondition every case here needs is "the operator's data stream is not
  bound yet". It is forced, never raced: a real `StreamChannel` starts with a
  nil `stream_ref` and stays there until this test binds a `LocalStream` to it,
  so the unbound window is as wide as the test wants and the bind happens on the
  product's own `{:bind, ref}` path.
  """
  # async: false: registered channel names, a shared state tree, :state_root,
  # a global telemetry handler.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree], async: false

  alias Flezha.Operator.Ingest
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.State.InputLog
  alias Flezha.State.TaskJournal
  alias Flezha.State.TestHelpers
  alias Flezha.SDK.LocalStream
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    class = "bindfwd_" <> Integer.to_string(System.unique_integer([:positive]))

    {channel, channel_pid} = NodeTestHelper.unbound_stream_channel("bindfwd_runner")
    assert GenServer.call(channel, :stream_ref_status) == nil

    on_exit(fn -> TestHelpers.close_input_log(class) end)

    %{class: class, channel: channel, channel_pid: channel_pid}
  end

  describe "an unbound stream" do
    test "counts the drop instead of discarding the channel's answer", ctx do
      test_pid = self()
      handler = "ow-io-drop-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:flezha, :stream, :drop],
        fn _e, m, md, _ -> send(test_pid, {:drop, m, md}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      # An empty extension catalog makes the `:on_input` chain fall through to
      # the message passthrough, so a non-nil payload reaches the channel.
      state = overwatch(ctx, [])

      assert {:reply, :ok, ^state} =
               Overwatch.handle_call({:io_in, "msg", %{}}, {self(), make_ref()}, state)

      class = ctx.class
      assert_receive {:drop, %{count: 1}, %{operator_id: ^class}}
    end
  end

  describe "face one — an input accepted while the stream is unbound" do
    test "is forwarded once the stream binds", ctx do
      state = overwatch(ctx, effectively_once: true)

      # The cast path: nobody is told anything, so the journal's `:ok` is the
      # only record of what happened to this input.
      assert {:pending, state} = Ingest.confirming_ingest(state, "m0", %{}, :cast)
      assert_receive log_reply
      assert {:ok, state} = Ingest.journal_reply(state, log_reply)

      # Accepted: the input log holds it.
      assert [{0, "m0", _}] = InputLog.read_range(ctx.class, 0, 0)

      # ... and nothing reached a stream, because there was no stream.
      bind_stream(ctx)
      assert drain_frames() == []

      # The bind is what owes the delivery.
      settle_bind(state, ctx)

      assert_receive {:flz_data, _, class, "m0", %{"flz.offset" => "0"}}
      assert class == ctx.class
    end

    test "is refused for a class the Controller does not journal", ctx do
      state = overwatch(ctx, effectively_once: false)

      # Nothing durable holds it, so `:ok` would be a lie. The confirming entry
      # point is told so; the cast has nobody to tell, which is why the bind
      # must not leave this window open at all.
      assert {{:error, :not_ready}, _state} =
               Ingest.confirming_ingest(state, "m0", %{}, {:ingest, {self(), make_ref()}})
    end
  end

  describe "face two — a restore re-forward onto a stream that has not bound" do
    test "is delivered once the stream binds", ctx do
      for i <- 0..2, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})
      state = overwatch(ctx, effectively_once: true)

      # This is the restore's own re-forward, issued while the fresh runner has
      # completed its control handshake but not bound its data stream.
      assert %Overwatch{} = Replay.reforward_undelivered_tail(state)

      bind_stream(ctx)
      assert drain_frames() == []

      settle_bind(state, ctx)

      assert_receive {:flz_data, _, _, "m0", %{"flz.offset" => "0"}}
      assert_receive {:flz_data, _, _, "m1", %{"flz.offset" => "1"}}
      assert_receive {:flz_data, _, _, "m2", %{"flz.offset" => "2"}}
    end

    test "forwards only the tail above the Cell's consumed offset", ctx do
      for i <- 0..3, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})
      InputLog.record_consumed(ctx.class, {ctx.class, "task", "a"}, 2)

      state = overwatch(ctx, effectively_once: true)
      assert %Overwatch{} = Replay.reforward_undelivered_tail(state)

      bind_stream(ctx)
      settle_bind(state, ctx)

      assert_receive {:flz_data, _, _, "m3", %{"flz.offset" => "3"}}
      assert drain_frames() == []
    end
  end

  describe "a durable steps operator's tail" do
    test "is what lies above its journal's floor, each record stamped with its offset", ctx do
      for i <- 0..3, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{})
      # An iteration commit reported offset 1 under the journal's identity.
      :ok = TaskJournal.release_input(ctx.class, 1)

      state = overwatch(ctx, durable_steps: true)
      bind_stream(ctx)
      assert %Overwatch{} = Replay.reforward_undelivered_tail(state)

      assert_receive {:flz_data, _, _, "m2", %{"flz.offset" => "2"}}
      assert_receive {:flz_data, _, _, "m3", %{"flz.offset" => "3"}}
      assert drain_frames() == []
    end

    test "loses the turn parked above its floor before the tail is re-forwarded", ctx do
      FlezhaTest.Helper.DlqFixtures.isolated_dir()
      for i <- 0..3, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{})
      # Iteration 1's turn failed and was recorded; the turn at 3 was parked
      # and the runner died before its record.
      :ok = TaskJournal.append_dead_lettered(ctx.class, 1, 1, "in", "answer", 3, "e1")

      for offset <- [1, 3] do
        {:ok, _ref} =
          Flezha.Dlq.Store.append(
            Flezha.Dlq.Entry.new!(
              operator_class: ctx.class,
              tasklet_id: "_turn",
              cell_key: "_turn",
              channel: "in",
              origin_offset: offset,
              error: "failed",
              payload: "m#{offset}"
            )
          )
      end

      state = overwatch(ctx, durable_steps: true)
      bind_stream(ctx)
      assert %Overwatch{} = Replay.reforward_undelivered_tail(state)

      assert [%{"origin_offset" => 1}] = Flezha.Dlq.Store.list(ctx.class)
      assert_receive {:flz_data, _, _, "m2", %{"flz.offset" => "2"}}
      assert_receive {:flz_data, _, _, "m3", %{"flz.offset" => "3"}}
    end
  end

  describe "a flapping stream" do
    test "re-forwards the same tail rather than an accumulating one", ctx do
      for i <- 0..1, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})
      state = overwatch(ctx, effectively_once: true)

      delivered =
        Enum.flat_map(1..3, fn _ ->
          assert %Overwatch{} = Replay.reforward_undelivered_tail(state)
          bind_stream(ctx)
          settle_bind(state, ctx)
          frames = drain_frames()
          unbind_stream(ctx)
          frames
        end)

      # Three binds with no Cell progress deliver the same two inputs each
      # time; the DISTINCT count is the size of the tail, which is what the
      # effectively-once identity absorbs.
      assert delivered |> Enum.uniq() |> Enum.sort() == ["m0", "m1"]
      assert length(delivered) == 6
    end
  end

  describe "the bind gate" do
    test "passes straight through once a stream is bound", ctx do
      bind_stream(ctx)
      assert :ok = Gate.await_bind(ctx.channel, 0)
    end

    test "returns when the bind lands during the wait", ctx do
      test_pid = self()

      gate =
        spawn_link(fn ->
          send(test_pid, {:gate, Gate.await_bind(ctx.channel, 5_000)})
        end)

      # The interest is recorded in the channel's own turn BEFORE the bind —
      # which is the window the gate exists for, and why deciding and recording
      # cannot be two calls.
      assert NodeTestHelper.wait_until(
               fn -> MapSet.member?(:sys.get_state(ctx.channel_pid).waiters.bind, gate) end,
               1_000
             )

      {_sdk, controller} = LocalStream.open()
      :ok = GenServer.call(ctx.channel, {:bind, controller})

      assert_receive {:gate, :ok}
    end

    test "gives up on a stream that never binds", ctx do
      assert {:error, :stream_unbound} = Gate.await_bind(ctx.channel, 20)
    end

    test "has nothing to wait for when the runner has no channel", _ctx do
      assert :ok = Gate.await_bind(nil, 5_000)
      assert :ok = Gate.await_bind(:no_such_channel_process, 5_000)
    end
  end

  # --- helpers ---

  # Bind a live native stream to the channel through the product's own atomic
  # bind call, with this process owning the SDK side so every forwarded frame
  # lands in the test's mailbox.
  defp bind_stream(%{channel: channel}) do
    {sdk, controller} = LocalStream.open()
    :ok = LocalStream.controlling_process(sdk, self())
    :ok = LocalStream.set_active(sdk)
    :ok = GenServer.call(channel, {:bind, controller})
    on_exit(fn -> LocalStream.close(controller) end)
    :ok
  end

  # Drive the bind notification the way the Overwatch process would. Its
  # absence is tolerated so a failure reads as "the input was never delivered"
  # — the defect — rather than as one missing message.
  defp settle_bind(state, %{channel: channel}) do
    receive do
      {:stream_bound, ^channel} = msg ->
        assert {:noreply, _} = Overwatch.handle_info(msg, state)
    after
      200 -> :no_notification
    end
  end

  # Put the channel back in the unbound state the product reaches on a dropped
  # stream, so the next round starts from the same forced precondition.
  defp unbind_stream(%{channel: channel, channel_pid: pid}) do
    send(pid, {:flz_closed, 0, "test"})
    assert GenServer.call(channel, :stream_ref_status) == nil
    :ok
  end

  # Every frame the stream carried, in order — `[]` when it carried none.
  defp drain_frames(acc \\ []) do
    receive do
      {:flz_data, _, _, message, _} -> drain_frames([message | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp overwatch(%{class: class, channel: channel}, overrides) do
    struct!(
      %Overwatch{
        id: class,
        namespace: "ns",
        default_input_type: "2",
        channel: channel,
        builder: %Flezha.Operator.Builder{id: class, factory_id: "f"},
        extensions: Table.new(%{})
      },
      overrides
    )
  end
end

defmodule FlezhaTest.Operator.OverwatchRestoreAsyncTest do
  @moduledoc """
  The per-Cell restore replay must run off the Overwatch process so its
  blocking disk reads cannot freeze the GenServer mailbox.
  """
  use ExUnit.Case, async: true
  use Mimic

  alias Flezha.Operator.Builder
  alias Flezha.Operator.Overwatch
  alias FlezhaTest.Helper.NodeTestHelper

  test "kick_restore does not block the Overwatch on a slow restore disk scan" do
    # Private stub bites only the in-process (regression) path; the spawned
    # worker calls through to the real (fast) scan. A regression therefore
    # hangs here rather than returning late.
    stub(Flezha.State.Paths, :all_cells, fn ->
      NodeTestHelper.park()
      []
    end)

    state = %Overwatch{
      id: "btc_feed",
      namespace: "default",
      runner_id: "ex_1",
      factory_id: "market",
      builder: %Builder{id: "btc_feed", factory_id: "market", opts: []},
      arrived_via_hotswap?: false
    }

    {elapsed_us, {:noreply, ^state}} =
      :timer.tc(fn -> Overwatch.handle_cast({:kick_restore, "ex_1"}, state) end)

    assert elapsed_us < 500_000,
           "kick_restore blocked the Overwatch for #{div(elapsed_us, 1000)}ms on the restore disk scan"
  end
end
