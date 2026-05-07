defmodule Flezha.State.FsyncBatcherTest do
  use ExUnit.Case, async: false

  alias Flezha.State.{FsyncBatcher, Supervisor, TestHelpers, WalWriter}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    case start_supervised({Elixir.Registry, keys: :unique, name: Supervisor.registry_name()}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Supervisor) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base}
  end

  describe "init validation" do
    test "rejects out-of-range tick interval" do
      Process.flag(:trap_exit, true)

      assert {:error, {:interval_out_of_range, 0, [1, 1000]}} =
               start_unnamed_init(interval_ms: 0)

      assert {:error, {:interval_out_of_range, 1001, [1, 1000]}} =
               start_unnamed_init(interval_ms: 1001)
    end

    test "rejects non-integer interval" do
      Process.flag(:trap_exit, true)
      assert {:error, {:invalid_interval, "5"}} = start_unnamed_init(interval_ms: "5")
    end
  end

  describe "mark_dirty + tick" do
    # Reuse the application-started FsyncBatcher (default interval = 5 ms).
    # Stopping it would race with the application supervisor's restart loop.

    test "10 marks of the same writer fan out to 1 fsync per tick window" do
      pid = TestHelpers.sample_partition_id("batcher_one")
      {:ok, _} = Supervisor.find_or_start(pid)

      {:ok, fakelog} = Agent.start_link(fn -> 0 end)

      :telemetry.attach(
        "fsync-tick-counter",
        [:flezha, :state, :fsync, :tick],
        fn _, %{count: n}, _, _ -> Agent.update(fakelog, &(&1 + n)) end,
        nil
      )

      for _ <- 1..10, do: FsyncBatcher.mark_dirty(pid)
      :ok = FsyncBatcher.flush_now()

      :telemetry.detach("fsync-tick-counter")
      assert Agent.get(fakelog, & &1) == 1
    end

    test "tick coalesces dirty set" do
      a = TestHelpers.sample_partition_id("a")
      b = TestHelpers.sample_partition_id("b")
      {:ok, _} = Supervisor.find_or_start(a)
      {:ok, _} = Supervisor.find_or_start(b)

      FsyncBatcher.mark_dirty(a)
      FsyncBatcher.mark_dirty(b)
      FsyncBatcher.mark_dirty(a)

      assert MapSet.new([a, b]) == FsyncBatcher.dirty_set()

      :ok = FsyncBatcher.flush_now()
      assert MapSet.size(FsyncBatcher.dirty_set()) == 0
    end

    test "actual fsync runs against live writer" do
      pid = TestHelpers.sample_partition_id("real")
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      :ok = WalWriter.append(srv, 1, TestHelpers.stub_header(), "x")

      FsyncBatcher.mark_dirty(pid)
      :ok = FsyncBatcher.flush_now()

      # No exception means the fsync path executed cleanly.
      assert WalWriter.last_persisted_seq(srv) == 1
    end
  end

  # init/1 returns its validation error before name registration, so we can
  # exercise it without contending with the application-started instance.
  defp start_unnamed_init(opts) do
    Process.flag(:trap_exit, true)
    GenServer.start(FsyncBatcher, opts)
  end
end
