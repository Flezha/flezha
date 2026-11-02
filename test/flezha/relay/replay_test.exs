defmodule FlezhaTest.Relay.ReplayAdoptionTest do
  @moduledoc """
  `Replay.adopt_moved_state/3` adopting a relay's durable state from
  another controller node on arrival, again before the runner starts while it
  has not landed (`Replay.adopt_unlanded_state/3`), and never from a restore.

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

  alias Flezha.Relay.Builder
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Replay
  alias Flezha.State.Adoption

  setup :set_mimic_global_until_exit

  defp state(relay_id) do
    %Overwatch{
      id: relay_id,
      namespace: "adoption-test",
      factory_id: "candles",
      runner_id: "runner-1",
      builder: %Builder{id: relay_id, factory_id: "candles", run: :flow, opts: [seat: {"", "s"}]}
    }
  end

  defp expect_adopt(reply_to, verdict \\ :ok) do
    stub(Adoption, :adopt_newest, fn relay_class, source, owner ->
      send(reply_to, {:adopted, source, relay_class, owner})
      verdict
    end)
  end

  # The restore enumerates the class's Cells off-process: the message this
  # returns marks that it ran.
  defp expect_scan(reply_to) do
    scanned = {:scanned, make_ref()}

    stub(Flezha.State.Paths, :cells_of, fn _relay_class ->
      send(reply_to, scanned)
      []
    end)

    scanned
  end

  defp adopt(relay_id, opts) do
    builder = %Builder{id: relay_id, factory_id: "candles", run: :flow, opts: opts}
    Replay.adopt_moved_state(%{state(relay_id) | builder: builder}, builder)
  end

  @peer :"peer@127.0.0.1"

  # fails when: the source stays on the relay's options, so every later bind
  # of the same instance adopts from it again.
  test "a member placed off the assembling node adopts from it once" do
    expect_adopt(self())

    relay = adopt("ohlc#p1", state_source_node: @peer)

    assert_received {:adopted, {:node, @peer}, "ohlc#p1", _}
    refute Keyword.has_key?(relay.opts, :state_source_node)
  end

  test "the relay_class opt names what is adopted, not the instance id" do
    expect_adopt(self())

    adopt("ohlc#p1", state_source_node: @peer, relay_class: "shared_class")

    assert_received {:adopted, {:node, @peer}, "shared_class", _}
  end

  test "a member placed on the assembling node adopts from no node" do
    expect_adopt(self())

    adopt("ohlc#p0", state_source_node: node())

    assert_received {:adopted, :backend, "ohlc#p0", _}
  end

  test "a relay that was never rescaled adopts from no node" do
    expect_adopt(self())

    adopt("plain", [])

    assert_received {:adopted, :backend, "plain", _}
  end

  # fails when: a pull that did not land drops its source, so the relay binds
  # without the copy that node still holds and no later bind asks it again.
  test "a pull that did not land keeps its source for the bind to pull again" do
    expect_adopt(self(), :not_landed)

    relay = adopt("ohlc#p1", state_source_node: @peer)
    owner = self()
    assert_received {:adopted, {:node, @peer}, "ohlc#p1", ^owner}
    assert relay.opts[:state_source_node] == @peer

    task = spawn(fn -> :ok end)
    Replay.adopt_unlanded_state(state("ohlc#p1"), relay, task)
    assert_received {:adopted, {:node, @peer}, "ohlc#p1", ^task}
  end

  # fails when: the bind adopts again over a copy that is already here, so a
  # tie with the backend's cut downloads the whole class a second time.
  test "before the runner starts, a copy already here is not adopted again" do
    expect_adopt(self())
    Flezha.State.TestHelpers.write_wal({"here", "task", "k"}, 0..1)
    builder = %Builder{id: "here", factory_id: "candles", run: :flow}

    assert Replay.adopt_unlanded_state(state("here"), builder, self()) == builder
    refute_received {:adopted, _, _, _}
  end

  # fails when: only an arrival adopts, so a node that lost its disk binds a
  # restarted runner with nothing while the backend holds the class.
  test "before the runner starts, a node holding nothing adopts" do
    expect_adopt(self())

    Replay.adopt_unlanded_state(state("cold"), state("cold").builder, self())

    assert_received {:adopted, :backend, "cold", _}
  end

  # fails when: every bind of a class that keeps no state lists the backend.
  test "before the runner starts, a class that keeps no state adopts nothing" do
    expect_adopt(self())
    :ok = Flezha.Relay.ClassRegistry.put_persistent("stateless", false)
    on_exit(fn -> Flezha.Relay.ClassRegistry.delete("stateless") end)

    Replay.adopt_unlanded_state(state("stateless"), state("stateless").builder, self())

    refute_received {:adopted, _, _, _}
  end

  # fails when: arriving by HotSwap is read off the transition under way, so a
  # later rebind, which names its own cause, replays the WAL over the state the
  # handover already delivered.
  test "a relay that arrived by HotSwap restores at no later bind" do
    arrived = %{state("arrived") | arrived_via_hotswap?: true, cause: :rebind}
    scanned = expect_scan(self())

    assert :ok = Replay.restore_on_bind(arrived, arrived.builder)
    refute_received ^scanned
    assert :ok = Replay.restore_on_bind(%{arrived | arrived_via_hotswap?: false}, arrived.builder)
    assert_received ^scanned
  end

  # fails when: the carriers' precedence moves: a handover stops excluding the
  # durable restore, or a shutdown snapshot stops seeding before the init values.
  test "the carriers that seed a bind give today's answer in each combination" do
    snapshot = ~s({"n":1})

    for {hotswap?, stored, init, carrier, seeded} <- [
          {false, nil, %{"n" => 0}, [:durable, :init], %{"n" => 0}},
          {false, snapshot, %{"n" => 0}, [:durable, :shutdown_snapshot], %{"n" => 1}},
          {true, nil, %{"n" => 0}, [:handover, :init], %{"n" => 0}},
          {true, snapshot, %{"n" => 0}, [:handover, :shutdown_snapshot], %{"n" => 1}},
          {false, nil, %{}, [:durable, :init], %{"own" => 2}}
        ] do
      builder = %Builder{id: "carrier", factory_id: "candles", init_values: %{"own" => 2}}

      state = %{
        state("carrier")
        | arrived_via_hotswap?: hotswap?,
          snapshot: stored,
          init_values: init,
          builder: builder
      }

      assert Replay.carrier(state) == carrier
      assert Flezha.Relay.Rebind.bind_relay(state).init_values == seeded
    end
  end
end

defmodule FlezhaTest.Relay.RestoreRefusalTest do
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

  alias Flezha.Relay.Builder
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Replay
  alias Flezha.State.Paths
  alias Flezha.State.RecordFrame
  alias Flezha.State.Restore
  alias Flezha.State.TestHelpers
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.Telemetry

  @java 2

  # Both refusal cases stub the SDK with this and then assert the log line
  # carries it back: the diagnostic is what they test, not noise they emit.
  @refusal "sdk_lang mismatch: this SDK is rust (1), the record was written by java (2)"

  # The SDK's refusal as the control unit hands it over: the runner's error,
  # whose text is the verdict the relay parks on.
  defp sdk_refusal, do: Flezha.Error.new("state.restore_failed", reason: @refusal)

  defp cell(base) do
    {"#{base}_#{System.unique_integer([:positive])}", "p0", "k"}
  end

  defp assert_refusal_logged(log, cell_id) do
    assert log =~ "[flezha_state] restore_refused"

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
      builder: %Builder{id: class, factory_id: "f", run: :flow, opts: [seat: {"", "next"}]}
    }

    assert :ok = Replay.restore_on_bind(state, state.builder)

    # Already answered when the bind goes on, not on its way.
    assert_received {:restored, ^id}
  end

  # fails when: a restoring bind stores the seat it gave before its restore
  # landed, or a refused restore leaves a seat, so the next bind keeps an
  # instance that never took this node's copy.
  test "a restoring bind stores its seat once the restore landed, a refused one none" do
    {class, _, _} = id = cell("seat")
    TestHelpers.write_wal(id, 0..1)
    on_exit(fn -> Flezha.State.Supervisor.delete_cell(id) end)

    state = %Overwatch{
      id: class,
      namespace: "_system",
      factory_id: "f",
      runner_id: "runner_1",
      builder: %Builder{id: class, factory_id: "f", run: :flow, opts: [seat: {"old", "new"}]}
    }

    :ok = Flezha.Relay.ClassRegistry.put_seat(class, "old")
    assert :ok = Replay.reseat_writers(state)
    assert Replay.seat(class) == ""

    stub(Flezha.State.Commands, :await_restore_init_state, fn _r, _e ->
      {:error, sdk_refusal()}
    end)

    {refused, _log} = with_log(fn -> Replay.restore_on_bind(state, state.builder) end)
    assert {:error, _} = refused
    assert Replay.seat(class) == ""

    stub(Flezha.State.Commands, :await_restore_init_state, fn _r, _e -> {:ok, <<>>} end)
    assert :ok = Replay.restore_on_bind(state, state.builder)
    assert Replay.seat(class) == "new"
  end

  # fails when: a bind carried by a hand-over, which restores nothing, leaves its
  # writers owned by the runner the relay left, so the bound runner's records are
  # refused and a stale instance's pull can still become the baseline.
  test "a bind re-seats every live writer of its relay, a hand-over's too" do
    {class, _, _} = id = cell("reseat")
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(id)
    on_exit(fn -> Flezha.State.Supervisor.delete_cell(id) end)
    header = TestHelpers.stub_header()
    :ok = WalWriter.append(writer, 0, header, "a", "runner_a")
    credit = %{channel: self(), session: "s", wire_size: 4}
    WalWriter.append_async(writer, 5, header, "held", "runner_a", credit)

    state = %Overwatch{
      id: class,
      namespace: "_system",
      factory_id: "f",
      runner_id: "runner_b",
      arrived_via_hotswap?: true,
      builder: %Builder{id: class, factory_id: "f", run: :flow}
    }

    assert Replay.reseat_writers(state) == :ok
    assert %{owning_runner: "runner_b", last_persisted_seq: 0} = WalWriter.stall_status(writer)
    assert_received {:credit_released, "s", 4, ^id}
    refute :sys.get_state(writer).gap_open?
  end

  describe "await_restore/2" do
    test "reports the SDK's refusal, with telemetry naming the cell and reason" do
      id = cell("refused")

      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _cell ->
        {:error, sdk_refusal()}
      end)

      Telemetry.capture([:flezha, :state, :restore_refused],
        as: :refused
      )

      {result, log} = with_log(fn -> Replay.await_restore("runner_1", id) end)

      verdict = sdk_refusal().message
      assert result == {:error, verdict}
      assert_receive {:refused, %{count: 1}, %{cell: ^id, runner: "runner_1", reason: ^verdict}}
      assert_refusal_logged(log, id)
    end

    test "an unanswered round trip is not a refusal" do
      # A dead control unit, a timeout, a runner shutting down: the SDK
      # reported nothing, so there is no verdict on the state. Parking here
      # would turn ordinary teardown churn into stuck relays.
      for reason <- [:timeout, :no_control_unit, {:control_unit_exit, {:shutdown, :normal}}] do
        stub(Flezha.State.Commands, :await_restore_init_state, fn _r, _e -> {:error, reason} end)
        assert Replay.await_restore("runner_1", cell("unanswered")) == :ok
      end
    end

    # fails when: a restore the control stream never carried lets the bind go
    # on, and the runner folds new inputs onto an empty Cell.
    test "a restore that was never sent stops the bind" do
      stub(Flezha.State.Commands, :await_restore_init_state, fn _r, _e ->
        {:error, :send_buffer_full}
      end)

      assert Replay.await_restore("runner_1", cell("unsent")) == {:error, :send_buffer_full}
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
        {:error, sdk_refusal()}
      end)

      # Re-forwarding the input tail onto a Cell whose baseline never landed
      # is the silent loss this guards, so it must not be reached.
      stub(Flezha.State.InputLog, :tail, fn _class ->
        flunk("the input tail was read after a refused restore")
      end)

      {result, log} = with_log(fn -> Replay.restore_cells_awaited(tier_state(id)) end)

      assert result == {:error, sdk_refusal().message}
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
      assert log =~ ~r/record_frame_refused .*reason=crc_mismatch/
      assert log =~ "[flezha_correction] refused"
      assert RecordFrame.file_size(path) == size
    end
  end

  defp tier_state({relay_class, _, _}) do
    %Overwatch{
      id: relay_class,
      namespace: "default",
      runner_id: "runner_1",
      factory_id: "f",
      effectively_once: true,
      builder: %Builder{id: relay_class, factory_id: "f", run: :flow, opts: []}
    }
  end
end

defmodule FlezhaTest.Relay.BindReforwardTest do
  @moduledoc """
  `durability-input-log`: an input the Controller accepts is delivered or
  refused, never accepted-and-dropped — plus §6.3, that the `io_in` path counts
  an unbound-stream drop instead of discarding the channel's answer silently.

  The precondition every case here needs is "the relay's data stream is not
  bound yet". It is forced, never raced: a real `StreamChannel` starts with a
  nil `stream_ref` and stays there until this test binds a stream to it,
  so the unbound window is as wide as the test wants and the bind happens on the
  product's own `{:bind, ref}` path.
  """
  # async: false: registered channel names, a shared state tree, :state_root,
  # a global telemetry handler.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree], async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Rebind
  alias Flezha.Relay.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.Runner.StreamPool
  alias Flezha.State.InputLog
  alias Flezha.State.TaskJournal
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.StreamPair
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  setup do
    class = "bindfwd_" <> Integer.to_string(System.unique_integer([:positive]))

    {channel, channel_pid} = NodeTestHelper.unbound_stream_channel("bindfwd_runner")
    assert GenServer.call(channel, :stream_ref_status) == nil

    on_exit(fn -> TestHelpers.close_input_log(class) end)

    %{class: class, channel: channel, channel_pid: channel_pid}
  end

  describe "an unbound stream" do
    test "counts the drop instead of discarding the channel's answer", ctx do
      Telemetry.capture([:flezha, :stream, :drop], as: :drop)

      # An empty extension catalog makes the `:on_input` chain fall through to
      # the message passthrough, so a non-nil payload reaches the channel.
      state = overwatch(ctx, [])

      assert {:reply, :ok, ^state} =
               Overwatch.handle_call({:io_in, "msg", %{}}, {self(), make_ref()}, state)

      class = ctx.class
      assert_receive {:drop, %{count: 1}, %{relay_id: ^class}}
    end
  end

  describe "face one — an input accepted while the stream is unbound" do
    test "is forwarded once the stream binds", ctx do
      state = overwatch(ctx, effectively_once: true)

      # The cast path: nobody is told anything, so the journal's `:ok` is the
      # only record of what happened to this input.
      state = ingest_cast(state, "m0")

      # Accepted: the input log holds it.
      assert [{0, "m0", _}] = InputLog.read_range(ctx.class, 0, 0)

      # ... and nothing reached a stream, because there was no stream.
      bind_stream(ctx)
      assert drain_frames(ctx) == []

      # The bind is what owes the delivery.
      fold_bound(state)

      assert_receive {:flz_data, _, class, "m0", %{"flz.offset" => "0"}}
      assert class == ctx.class
    end

    test "is refused for a class the Controller does not journal", ctx do
      state = overwatch(ctx, effectively_once: false)

      # Nothing durable holds it, so `:ok` would be a lie. The confirming entry
      # point is told so; the cast has nobody to tell, which is why the bind
      # must not leave this window open at all.
      assert {{:error, {:not_accepting_input, id}}, _state} =
               Ingest.confirming_ingest(state, "m0", %{}, {:ingest, {self(), make_ref()}})

      assert id == state.id
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
      assert drain_frames(ctx) == []

      fold_bound(state)

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
      fold_bound(state)

      assert_receive {:flz_data, _, _, "m3", %{"flz.offset" => "3"}}
      assert drain_frames(ctx) == []
    end
  end

  describe "a durable steps relay's tail" do
    test "is what lies above its journal's floor, each record stamped with its offset", ctx do
      for i <- 0..3, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{})
      # An iteration commit reported offset 1 under the journal's identity.
      :ok = TaskJournal.release_input(ctx.class, 1)

      state = overwatch(ctx, durable_steps: true)
      bind_stream(ctx)
      assert %Overwatch{} = Replay.reforward_undelivered_tail(state)

      assert_receive {:flz_data, _, _, "m2", %{"flz.offset" => "2"}}
      assert_receive {:flz_data, _, _, "m3", %{"flz.offset" => "3"}}
      assert drain_frames(ctx) == []
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
              relay_class: ctx.class,
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
          fold_bound(state)
          frames = drain_frames(ctx)
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

  # The session's streams are let go and fresh ones bound under running relays.
  describe "a data-plane rebind" do
    # fails when: an input a broken stream swallowed is never sent again, or goes
    # after an input forwarded on the fresh stream, whose offset raised its
    # Cell's consumed floor past it, so the runner drops it.
    test "re-forwards a parked relay's tail ahead of input taken once the fresh stream bound",
         ctx do
      runner_id = "bindfwd_park_#{System.unique_integer([:positive])}"
      channel = StreamPool.channel_for(runner_id, ctx.class)
      {:ok, pid} = StreamChannel.start_link({"_system", runner_id, channel, :primary})
      on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)
      ctx = %{ctx | channel: channel, channel_pid: pid}
      bind_stream(ctx)

      state = overwatch(ctx, effectively_once: true, runner_id: runner_id, phase: :running)
      state = ingest_cast(state, "swallowed")
      assert drain_frames(ctx) == ["swallowed"]
      unbind_stream(ctx)

      state = Rebind.park_stream(state, runner_id)
      assert state.channel == nil

      # The fresh stream is up, and its notice not yet handled when input comes.
      bind_stream(ctx)
      assert_received {:stream_bound, ^channel} = bound
      state = ingest_cast(state, "fresh")
      assert drain_frames(ctx) == []

      assert {:noreply, %Overwatch{channel: ^channel}} = Overwatch.handle_info(bound, state)
      assert drain_frames(ctx) == ["swallowed", "fresh"]
    end
  end

  # A rebind runs in a task while the Overwatch keeps serving on the channel
  # the relay is leaving, whose runner is gone. ctx.channel is the bind's target.
  describe "a bind in flight" do
    test "an input accepted meanwhile reaches the runner the bind lands on", ctx do
      state = binding_away(ctx)

      state = ingest_cast(state, "m0")

      bind_stream(ctx)
      adopt(state, ctx)

      assert_receive {:flz_data, _, _, "m0", %{"flz.offset" => "0"}}
    end

    test "an input the channel it leaves still carried reaches the runner it lands on", ctx do
      state = binding_away(ctx)
      bind_stream(%{channel: state.channel})

      # The task's re-forward found nothing; the input comes after it.
      assert %Overwatch{} = Replay.reforward_undelivered_tail(bound_copy(state, ctx))
      state = ingest_cast(state, "m0")
      assert drain_frames(state) == ["m0"]

      bind_stream(ctx)
      adopt(state, ctx)

      assert_receive {:flz_data, _, _, "m0", %{"flz.offset" => "0"}}
    end

    for {from, halt_reason} <- [running: nil, halted: :runner_gone] do
      test "its own re-forward, announced before a #{from} relay's bind is adopted, " <>
             "reaches its runner",
           ctx do
        for i <- 0..1,
            do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})

        state = binding_away(ctx, phase: unquote(from), halt_reason: unquote(halt_reason))

        # The task's re-forward meets the target before its stream binds.
        assert %Overwatch{} = Replay.reforward_undelivered_tail(bound_copy(state, ctx))
        bind_stream(ctx)
        state = fold_bound(state)
        assert drain_frames(ctx) == []

        adopt(state, ctx)

        assert_receive {:flz_data, _, _, "m0", %{"flz.offset" => "0"}}
        assert_receive {:flz_data, _, _, "m1", %{"flz.offset" => "1"}}
      end
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

      {controller, close} = StreamPair.open()
      on_exit(close)
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

  # Bind a live stream to the channel through the product's own atomic bind
  # call, with this process reading the runner's side so every forwarded frame
  # lands in the test's mailbox.
  defp bind_stream(%{channel: channel}) do
    {controller, close} = StreamPair.open()
    :ok = GenServer.call(channel, {:bind, controller})
    on_exit(close)
    :ok
  end

  # A cast ingest through the journal. The log answers after a real fsync,
  # which a loaded disk does not finish within any fixed window.
  defp ingest_cast(state, message) do
    assert {:pending, state} = Ingest.confirming_ingest(state, message, %{}, :cast)
    assert {:ok, state} = Ingest.journal_reply(state, receive(do: (reply -> reply)))
    state
  end

  defp binding_away(ctx, overrides \\ []) do
    {leaving, _pid} = NodeTestHelper.unbound_stream_channel("bindfwd_leaving")
    phase = Keyword.get(overrides, :phase, :running)
    {:ok, _floor, journaled} = InputLog.tail(ctx.class)

    overwatch(
      %{ctx | channel: leaving},
      [
        effectively_once: true,
        runner_id: "bindfwd_leaving",
        phase: phase,
        bind_task: %{
          pid: self(),
          then: nil,
          from: phase,
          journaled: journaled,
          extensions: Table.new(%{})
        }
      ] ++ overrides
    )
  end

  # The state the bind task reports: the pre-bind copy, pointed at its target.
  defp bound_copy(state, %{channel: channel}) do
    %{
      state
      | channel: channel,
        runner_id: "bindfwd_runner",
        bind_task: nil,
        phase: :running,
        halt_reason: nil
    }
  end

  # Drive every bind notification already in the mailbox through the
  # Overwatch, as its process would. None is tolerated, so a failure reads as
  # "the input was never delivered" rather than as one missing message. A bind
  # notifies before its call returns, so nothing is waited for.
  defp fold_bound(state) do
    receive do
      {:stream_bound, _channel} = msg ->
        {:noreply, state} = Overwatch.handle_info(msg, state)
        fold_bound(state)
    after
      0 -> state
    end
  end

  defp adopt(state, ctx) do
    ExUnit.CaptureLog.with_log(fn ->
      state |> Rebind.bind_task_reported({:bound, bound_copy(state, ctx)}) |> fold_bound()
    end)
  end

  # Put the channel back in the unbound state the product reaches on a dropped
  # stream, so the next round starts from the same forced precondition.
  defp unbind_stream(%{channel: channel}) do
    NodeTestHelper.close_bound_stream(channel, "test")
    assert GenServer.call(channel, :stream_ref_status) == nil
    :ok
  end

  # Every frame `channel`'s stream carried, in order — `[]` when it carried
  # none. A frame written last on the same stream arrives after all of them.
  defp drain_frames(%{channel: channel}) do
    sentinel = "drained-#{System.unique_integer([:positive])}"
    :ok = StreamChannel.io_in(channel, "drain", {sentinel, %{}})
    assert_receive {:flz_data, _, _, ^sentinel, _}, 2_000

    collect do
      {:flz_data, _, _, message, _} -> message
    end
  end

  defp overwatch(%{class: class, channel: channel}, overrides) do
    struct!(
      %Overwatch{
        id: class,
        namespace: "ns",
        default_input_type: "2",
        channel: channel,
        builder: %Flezha.Relay.Builder{id: class, factory_id: "f"},
        extensions: Table.new(%{})
      },
      overrides
    )
  end
end
