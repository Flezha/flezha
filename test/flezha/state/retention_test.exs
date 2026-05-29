defmodule Flezha.State.RetentionTest do
  use ExUnit.Case, async: false

  alias Flezha.State.{
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

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :time_travel_retention_hours)
      File.rm_rf!(base)
    end)

    %{base: base, partition_id: TestHelpers.sample_partition_id("ret_" <> uniq())}
  end

  describe "sweep_partition" do
    test "0-retention truncates WAL down to the latest snapshot", %{partition_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      for n <- 0..6, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 4, header, "snap@4")
      # After snapshot the writer is at seq 6 with WAL = [5, 6].
      assert WalWriter.last_persisted_seq(srv) == 6

      :ok = Retention.sweep_partition(pid, 0)

      # Latest snapshot remains; WAL should still be [5, 6] because they're > kept_seq=4.
      assert {4, _} = Snapshot.latest_snapshot(pid)

      # Add an older snapshot to demonstrate it gets reaped under 0 retention.
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")
      :ok = Retention.sweep_partition(pid, 0)
      assert [{5, _}] = Registry.snapshot_paths(pid)
    end

    test "non-zero retention keeps only the latest when all snaps are within window", %{
      partition_id: pid
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
      :ok = Retention.sweep_partition(pid, 1)
      assert [{3, _}] = Registry.snapshot_paths(pid)
    end

    test "rewriting preserves crc-validity of kept records", %{partition_id: pid} do
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

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
