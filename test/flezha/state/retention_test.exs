defmodule Flezha.State.RetentionTest do
  use ExUnit.Case, async: false

  alias Flezha.State.{
    Completion,
    FsyncBatcher,
    Registry,
    Retention,
    Snapshot,
    Supervisor,
    TestHelpers,
    WalWriter
  }

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

    case start_supervised(FsyncBatcher) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Retention) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :time_travel_retention_hours)
      Application.delete_env(:flezha, :bounded_completed_retention_hours)
      File.rm_rf!(base)
    end)

    %{base: base, segment_id: TestHelpers.sample_segment_id("ret_" <> uniq())}
  end

  describe "sweep_segment" do
    test "0-retention truncates WAL down to the latest snapshot", %{segment_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      for n <- 0..6, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 4, header, "snap@4")
      # After snapshot the writer is at seq 6 with WAL = [5, 6].
      assert WalWriter.last_persisted_seq(srv) == 6

      :ok = Retention.sweep_segment(pid, 0)

      # Latest snapshot remains; WAL should still be [5, 6] because they're > kept_seq=4.
      assert {4, _} = Snapshot.latest_snapshot(pid)

      # Add an older snapshot to demonstrate it gets reaped under 0 retention.
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")
      :ok = Retention.sweep_segment(pid, 0)
      assert [{5, _}] = Registry.snapshot_paths(pid)
    end

    test "non-zero retention keeps only the latest when all snaps are within window", %{
      segment_id: pid
    } do
      header = TestHelpers.stub_header()
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)

      for n <- 0..3, do: :ok = WalWriter.append(srv, n, header, "x")
      {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
      {:ok, _} = Snapshot.write_snapshot(pid, 3, header, "snap@3")

      # 1-hour retention; both snaps are within the window. Per design.md
      # Decision 5: when no snap is older than cutoff, kept_seq = latest_seq
      # and earlier snaps are deleted as redundant.
      :ok = Retention.sweep_segment(pid, 1)
      assert [{3, _}] = Registry.snapshot_paths(pid)
    end

    test "rewriting preserves crc-validity of kept records", %{segment_id: pid} do
      header = TestHelpers.stub_header()
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)

      for n <- 0..4, do: :ok = WalWriter.append(srv, n, header, "y" <> Integer.to_string(n))
      :ok = Retention.rewrite_wal_excluding_pre_seq(pid, 2)

      {:ok, dev} = :file.open(Registry.wal_path(pid), [:raw, :read, :binary])
      assert {:ok, 3, _, "y3", c1} = WalWriter.decode_record(dev, 0)
      assert {:ok, 4, _, "y4", _} = WalWriter.decode_record(dev, c1)
      :file.close(dev)
    end
  end

  describe "finished-job retention (bounded)" do
    test "retains a completion record within the window, reclaims it after" do
      root = Registry.state_root()
      now = System.system_time(:second)

      # Within the (default 72h) window → retained.
      :ok =
        Completion.write("bnd_live", %{
          namespace: "ns",
          status: "ok",
          error: "",
          result: "r",
          finished_at: now
        })

      # Past the window (0h retention for this class) → reclaimed wholesale.
      Application.put_env(:flezha, :bounded_completed_retention_hours, %{"bnd_old" => 0})

      :ok =
        Completion.write("bnd_old", %{
          namespace: "ns",
          status: "error",
          error: "boom",
          result: "",
          finished_at: now - 10
        })

      :ok = Retention.sweep_now()

      assert Completion.exists?("bnd_live")
      refute Completion.exists?("bnd_old")
      refute File.dir?(Path.join(root, "bnd_old"))
    end
  end

  describe "time-travel retention gating (kind-aware)" do
    test "a bounded class is swept as retention_hours=0 (older snapshot reaped)" do
      header = TestHelpers.stub_header()
      class = "bnd_ret_" <> uniq()
      pid = {class, "op", "seg"}
      Flezha.Operative.ClassRegistry.put(class, :bounded)
      on_exit(fn -> Flezha.Operative.ClassRegistry.delete(class) end)

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x")
      {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")

      # snap@2 predates the window; a bounded class has no time-travel, so the
      # sweep forces retention_hours=0 → only the latest snapshot survives.
      File.touch!(Registry.snapshot_path(pid, 2), System.os_time(:second) - 5 * 24 * 3600)
      Application.put_env(:flezha, :time_travel_retention_hours, %{class => 48})

      :ok = Retention.sweep_now()

      assert [{5, _}] = Registry.snapshot_paths(pid)
    end

    test "an unbounded class keeps the older snapshot under the same window" do
      header = TestHelpers.stub_header()
      class = "cont_ret_" <> uniq()
      pid = {class, "op", "seg"}

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x")
      {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")

      # Same setup, but a (default) unbounded class keeps snap@2 as the
      # retention floor because it is older than the 48h window.
      File.touch!(Registry.snapshot_path(pid, 2), System.os_time(:second) - 5 * 24 * 3600)
      Application.put_env(:flezha, :time_travel_retention_hours, %{class => 48})

      :ok = Retention.sweep_now()

      assert [{2, _}, {5, _}] = Registry.snapshot_paths(pid)
    end
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
