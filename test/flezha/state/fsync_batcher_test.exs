defmodule Flezha.State.FsyncBatcherTest do
  # async: false: writes Application env; a shared on-disk root; the primary Logger.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import ExUnit.CaptureLog

  alias Flezha.Process.Timer
  alias Flezha.State.{FsyncBatcher, Paths, Supervisor, TestHelpers, WalWriter}
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    case start_supervised({Elixir.Registry, keys: :unique, name: Supervisor.registry_name()}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Supervisor) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    :ok
  end

  describe "init validation" do
    test "rejects out-of-range tick interval" do
      Process.flag(:trap_exit, true)

      # capture_log suppresses the proc_lib crash report emitted when init/1
      # returns {:stop, ...} — these tests assert that exact path.
      capture_log(fn ->
        assert {:error, {:interval_out_of_range, 0, [1, 1000]}} =
                 start_unnamed_init(interval_ms: 0)

        assert {:error, {:interval_out_of_range, 1001, [1, 1000]}} =
                 start_unnamed_init(interval_ms: 1001)
      end)
    end

    test "rejects non-integer interval" do
      Process.flag(:trap_exit, true)

      capture_log(fn ->
        assert {:error, {:invalid_interval, "5"}} = start_unnamed_init(interval_ms: "5")
      end)
    end
  end

  describe "mark_dirty + tick" do
    # Reuse the application-started FsyncBatcher (default interval = 5 ms).
    # Stopping it would race with the application supervisor's restart loop.

    test "10 marks of the same writer fan out to 1 fsync per tick window" do
      pid = TestHelpers.sample_cell_id("batcher_one")
      {:ok, _} = Supervisor.find_or_start(pid)

      # Isolated batcher with a long interval so our explicit flush is the only
      # drain. The application's 5 ms singleton would race a background tick
      # into the mark loop and split the batch into two fsyncs (flaky); it also
      # stays idle here, so the global handler only sees this instance's tick.
      {:ok, batcher} = GenServer.start_link(FsyncBatcher, interval_ms: 1000)
      # A fresh batcher starts with every writer registered on the NODE dirty.
      # The drain clears the ones that sync; a neighbouring module's writer whose
      # state root is already gone cannot sync and stays dirty by design, so what
      # the drain leaves is counted rather than assumed to be nothing.
      :ok = FsyncBatcher.flush_now(batcher)
      residue = MapSet.size(FsyncBatcher.dirty_set(batcher))

      {:ok, fakelog} = Agent.start_link(fn -> 0 end)

      :telemetry.attach(
        "fsync-tick-counter",
        [:flezha, :state, :fsync, :tick],
        fn _, %{count: n}, _, _ -> Agent.update(fakelog, &(&1 + n)) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("fsync-tick-counter") end)

      for _ <- 1..10, do: FsyncBatcher.mark_dirty(pid, batcher)
      :ok = FsyncBatcher.flush_now(batcher)

      :telemetry.detach("fsync-tick-counter")
      assert Agent.get(fakelog, & &1) == 1 + residue
    end

    test "tick coalesces dirty set" do
      a = TestHelpers.sample_cell_id("a")
      b = TestHelpers.sample_cell_id("b")
      {:ok, _} = Supervisor.find_or_start(a)
      {:ok, _} = Supervisor.find_or_start(b)

      # Its own batcher, as the test above: the application's singleton seeds
      # itself from every writer registered in the NODE and is flushed by
      # whatever else is running, so `==` against it asserts a neighbouring
      # module's live writers as well as these two.
      {:ok, batcher} = GenServer.start_link(FsyncBatcher, interval_ms: 1000)
      :ok = FsyncBatcher.flush_now(batcher)
      residue = FsyncBatcher.dirty_set(batcher)

      FsyncBatcher.mark_dirty(a, batcher)
      FsyncBatcher.mark_dirty(b, batcher)
      FsyncBatcher.mark_dirty(a, batcher)

      assert MapSet.new([a, b]) ==
               MapSet.difference(FsyncBatcher.dirty_set(batcher), residue)

      :ok = FsyncBatcher.flush_now(batcher)
      assert FsyncBatcher.dirty_set(batcher) == residue
    end

    test "actual fsync runs against live writer" do
      pid = TestHelpers.sample_cell_id("real")
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")

      FsyncBatcher.mark_dirty(pid)
      :ok = FsyncBatcher.flush_now()

      # No exception means the fsync path executed cleanly.
      assert WalWriter.last_persisted_seq(srv) == 0
    end

    test "a writer that refused its own file is not re-marked dirty" do
      pid = TestHelpers.sample_cell_id("refuser")
      {:ok, srv} = Supervisor.find_or_start(pid)
      header = TestHelpers.stub_header()
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

      path = Paths.wal_path(pid)
      [_first, second | _] = TestHelpers.record_offsets(path)
      TestHelpers.corrupt_record_body(path, second)

      # A writer that refused its own file stays alive answering every write
      # with that refusal for the rest of its life, `:fsync` included.
      capture_log(fn ->
        assert {:error, {:crc_mismatch, 0, ^second}} = WalWriter.truncate_up_to(srv, 3)
      end)

      {:ok, batcher} = GenServer.start_link(FsyncBatcher, interval_ms: 1000)
      FsyncBatcher.mark_dirty(pid, batcher)
      :ok = FsyncBatcher.flush_now(batcher)

      refute MapSet.member?(FsyncBatcher.dirty_set(batcher), pid),
             "a sync that can never happen was kept for the next drain"
    end

    test "a writer that refuses its fsync stays dirty for the next drain" do
      pid = TestHelpers.sample_cell_id("refused")
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "x")

      # Its own batcher, as the two tests above: the application's singleton
      # seeds itself from every writer on the node.
      {:ok, batcher} = GenServer.start_link(FsyncBatcher, interval_ms: 1000)
      :ok = FsyncBatcher.flush_now(batcher)
      FsyncBatcher.mark_dirty(pid, batcher)

      # The Registry clears an entry from the exit it is linked by, and a
      # suspended partition handles none, so the drain provably still finds this
      # writer after it is gone. `WalWriter.fsync/1` then answers its declared
      # refusal: a value, and no exit for anyone to catch.
      partition =
        Process.whereis(Module.concat(Supervisor.registry_name(), "PIDPartition0"))

      :sys.suspend(partition)
      on_exit(fn -> :sys.resume(partition) end)

      ref = Process.monitor(srv)
      :ok = WalWriter.close(srv)
      assert_receive {:DOWN, ^ref, :process, ^srv, _}, 1_000

      :ok = FsyncBatcher.flush_now(batcher)

      assert MapSet.member?(FsyncBatcher.dirty_set(batcher), pid),
             "a sync that never happened was dropped from the dirty set"
    end
  end

  # Read from the batcher's own state: a tick that is not armed is a wake that
  # cannot happen, which a window with no `:tick` in it could only suggest.
  describe "the tick is armed only while something is dirty" do
    test "an idle batcher arms none, a mark arms one a second mark keeps, the tick disarms it" do
      id = TestHelpers.sample_cell_id("idle_batcher")
      # Registered before the batcher starts, so `idle_batcher/0` sees it seeded
      # and its tick armed.
      {:ok, _} = Supervisor.find_or_start(id)
      batcher = idle_batcher()

      refute tick_armed?(batcher), "a batcher with nothing dirty kept its tick armed"

      FsyncBatcher.mark_dirty(id, batcher)
      assert %{timers: %{tick: armed}} = :sys.get_state(batcher)
      FsyncBatcher.mark_dirty(id, batcher)

      assert %{timers: %{tick: ^armed}} = :sys.get_state(batcher),
             "a mark on an armed batcher re-armed its tick"

      drained =
        NodeTestHelper.wait_until(
          fn ->
            state = :sys.get_state(batcher)
            MapSet.size(state.dirty_set) == 0 and state
          end,
          5_000,
          10
        )

      assert drained, "the tick never drained the mark"
      refute Timer.armed?(drained.timers, :tick), "the tick re-armed with nothing dirty"
    end

    test "a forget or flush that empties the set disarms it; an unreachable writer keeps it" do
      id = TestHelpers.sample_cell_id("flushed")
      {:ok, srv} = Supervisor.find_or_start(id)
      batcher = idle_batcher()

      FsyncBatcher.mark_dirty(id, batcher)
      FsyncBatcher.forget(id, batcher)
      refute tick_armed?(batcher), "a forget that emptied the set left the tick armed"

      FsyncBatcher.mark_dirty(id, batcher)
      :ok = FsyncBatcher.flush_now(batcher)
      refute tick_armed?(batcher), "a flush that emptied the set left the tick armed"

      # The writer is gone while its Registry entry is kept, as in "a writer
      # that refuses its fsync stays dirty for the next drain" above.
      partition =
        Process.whereis(Module.concat(Supervisor.registry_name(), "PIDPartition0"))

      :sys.suspend(partition)
      on_exit(fn -> :sys.resume(partition) end)
      ref = Process.monitor(srv)
      :ok = WalWriter.close(srv)
      assert_receive {:DOWN, ^ref, :process, ^srv, _}, 1_000

      FsyncBatcher.mark_dirty(id, batcher)
      :ok = FsyncBatcher.flush_now(batcher)
      assert MapSet.member?(FsyncBatcher.dirty_set(batcher), id)
      assert tick_armed?(batcher), "a writer the flush could not reach lost its retry"
    end

    test "a dormant mark waits for the rhythm the last tick set, not a whole interval" do
      id = TestHelpers.sample_cell_id("rhythm")
      batcher = idle_batcher()

      # {ms since the last tick, ms after that tick the next one falls due}
      for {since, due} <- [{0, 1_000}, {300, 1_000}, {2_300, 3_000}] do
        %{last_tick_at: last_tick} =
          :sys.replace_state(batcher, fn state ->
            %{state | last_tick_at: System.monotonic_time(:millisecond) - since}
          end)

        FsyncBatcher.mark_dirty(id, batcher)
        {earliest, latest} = tick_due(batcher)

        assert earliest - last_tick <= due + 2 and latest - last_tick >= due - 1,
               "#{since} ms after a tick, the next fell due #{earliest - last_tick}.." <>
                 "#{latest - last_tick} ms after it, not #{due}"

        FsyncBatcher.forget(id, batcher)
      end
    end
  end

  # An isolated batcher with nothing dirty: it seeds itself with every writer on
  # the node, and arms its tick for them, so those are forgotten. The long
  # interval keeps its tick from firing between two reads of its state.
  defp idle_batcher() do
    {:ok, batcher} = GenServer.start_link(FsyncBatcher, interval_ms: 1000)
    seeded = FsyncBatcher.dirty_set(batcher)

    assert tick_armed?(batcher) == MapSet.size(seeded) > 0,
           "a started batcher seeded with #{MapSet.size(seeded)} writer(s) armed the wrong tick"

    for writer <- seeded, do: FsyncBatcher.forget(writer, batcher)
    batcher
  end

  defp tick_armed?(batcher), do: Timer.armed?(:sys.get_state(batcher).timers, :tick)

  # When the armed tick falls due, bracketed by the clock reads either side of
  # reading the timer.
  defp tick_due(batcher) do
    {ref, :tick} = :sys.get_state(batcher).timers.tick
    before = System.monotonic_time(:millisecond)
    left = Process.read_timer(ref)
    {before + left, System.monotonic_time(:millisecond) + left}
  end

  # init/1 returns its validation error before name registration, so we can
  # exercise it without contending with the application-started instance.
  defp start_unnamed_init(opts) do
    Process.flag(:trap_exit, true)
    GenServer.start(FsyncBatcher, opts)
  end
end
