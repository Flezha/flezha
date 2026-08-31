defmodule Flezha.State.RetentionTest do
  # async: false: writes Application env; a shared on-disk root; the primary Logger.
  use ExUnit.Case, async: false

  alias Flezha.Operative.ClassRegistry

  alias Flezha.State.{
    Completion,
    Paths,
    Retention,
    Snapshot,
    Supervisor,
    TestHelpers,
    WalWriter
  }

  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    base = TestHelpers.tmp_app_data_dir()

    TestHelpers.start_state_tree([Retention])

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :time_travel_retention_hours)
      Application.delete_env(:flezha, :workflow_completed_retention_hours)
      File.rm_rf!(base)
    end)

    %{base: base, entity_id: TestHelpers.sample_entity_id("ret_" <> uniq())}
  end

  describe "sweep_entity" do
    test "0-retention truncates WAL down to the latest snapshot", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      for n <- 0..6, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 4, header, "snap@4")
      # After snapshot the writer is at seq 6 with WAL = [5, 6].
      assert WalWriter.last_persisted_seq(srv) == 6

      :ok = Retention.sweep_entity(pid, 0)

      # Latest snapshot remains; WAL should still be [5, 6] because they're > kept_seq=4.
      assert {4, _} = Snapshot.latest_snapshot(pid)

      # Add an older snapshot to demonstrate it gets reaped under 0 retention.
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")
      :ok = Retention.sweep_entity(pid, 0)
      assert [{5, _}] = Paths.snapshot_paths(pid)
    end

    test "non-zero retention keeps only the latest when all snaps are within window", %{
      entity_id: pid
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
      :ok = Retention.sweep_entity(pid, 1)
      assert [{3, _}] = Paths.snapshot_paths(pid)
    end

    test "rewriting preserves crc-validity of kept records", %{entity_id: pid} do
      header = TestHelpers.stub_header()
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)

      for n <- 0..4, do: :ok = WalWriter.append(srv, n, header, "y" <> Integer.to_string(n))
      :ok = Retention.rewrite_wal_excluding_pre_seq(pid, 2)

      {:ok, dev} = :file.open(Paths.wal_path(pid), [:raw, :read, :binary])
      assert {:ok, 3, _, "y3", c1} = WalWriter.decode_record(dev, 0)
      assert {:ok, 4, _, "y4", _} = WalWriter.decode_record(dev, c1)
      :file.close(dev)
    end
  end

  describe "finished-job retention (workflow)" do
    test "retains a completion record within the window, reclaims it after" do
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
      Application.put_env(:flezha, :workflow_completed_retention_hours, %{"bnd_old" => 0})

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
      refute File.dir?(Paths.class_dir("bnd_old"))
    end
  end

  describe "time-travel retention gating (kind-aware)" do
    test "a workflow class is swept as retention_hours=0 (older snapshot reaped)" do
      header = TestHelpers.stub_header()
      class = "bnd_ret_" <> uniq()
      pid = {class, "proc", "seg"}
      ClassRegistry.put(class, :workflow)
      on_exit(fn -> ClassRegistry.delete(class) end)

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x")
      {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")

      # snap@2 predates the window; a workflow class has no time-travel, so the
      # sweep forces retention_hours=0 → only the latest snapshot survives.
      File.touch!(Paths.snapshot_path(pid, 2), System.os_time(:second) - 5 * 24 * 3600)
      Application.put_env(:flezha, :time_travel_retention_hours, %{class => 48})

      :ok = Retention.sweep_now()

      assert [{5, _}] = Paths.snapshot_paths(pid)
    end

    test "a stream class keeps the older snapshot under the same window" do
      header = TestHelpers.stub_header()
      class = "cont_ret_" <> uniq()
      pid = {class, "proc", "seg"}

      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x")
      {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")

      # Same setup, but a (default) stream class keeps snap@2 as the
      # retention floor because it is older than the 48h window.
      File.touch!(Paths.snapshot_path(pid, 2), System.os_time(:second) - 5 * 24 * 3600)
      Application.put_env(:flezha, :time_travel_retention_hours, %{class => 48})

      :ok = Retention.sweep_now()

      assert [{2, _}, {5, _}] = Paths.snapshot_paths(pid)
    end
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))

  describe "a tick with nothing to cut" do
    test "leaves the WAL file untouched", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      for n <- 0..6, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 4, header, "snap@4")
      # Compaction already cut the WAL to [5, 6]; a retention tick that keeps
      # the latest snapshot has nothing below seq 4 left to remove.
      {:ok, %{inode: inode}} = File.stat(Paths.wal_path(pid))

      :ok = Retention.sweep_entity(pid, 6)

      assert {:ok, %{inode: ^inode}} = File.stat(Paths.wal_path(pid)),
             "a no-op cut must not rewrite (and re-inode) the file"

      assert WalWriter.last_persisted_seq(srv) == 6
    end
  end

  describe "a compaction cut" do
    test "past the index samples keeps exactly the tail", %{entity_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      seqs = fn ->
        WalWriter.fold_path(Paths.wal_path(pid), [], fn {s, _, _}, acc ->
          {:cont, [s | acc]}
        end)
        |> Enum.reverse()
      end

      for n <- 0..599, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 400, header, "snap@400")
      assert seqs.() == Enum.to_list(401..599)

      # The second cut works against the rebased index and appends land after it.
      :ok = WalWriter.append(srv, 600, header, "x600")
      {:ok, _} = Snapshot.write_snapshot(pid, 550, header, "snap@550")
      assert seqs.() == Enum.to_list(551..600)
      assert WalWriter.last_persisted_seq(srv) == 600
    end
  end

  describe "the class registry" do
    alias Flezha.Operative.ClassRegistry

    # The kill below is the point of the test; the supervisor's report of it
    # is not console noise worth keeping.
    @tag :capture_log
    test "outlives a Retention crash" do
      class = "reg_" <> uniq()
      :ok = ClassRegistry.put_overwatch(class, self())

      old = Process.whereis(Retention)
      Process.exit(old, :kill)

      assert NodeTestHelper.wait_until(
               fn ->
                 pid = Process.whereis(Retention)
                 is_pid(pid) and pid != old
               end,
               1_000,
               5
             ),
             "Retention must be restarted by its supervisor"

      assert ClassRegistry.overwatch_pid(class) == {:ok, self()},
             "the registry every outbox dispatch resolves through must not die with a sweeper"
    end
  end
end
