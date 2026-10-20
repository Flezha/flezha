defmodule Flezha.State.RestoreTest do
  # async: false: writes Application env; a shared on-disk root; node-global tables.
  use FlezhaTest.StateCase, async: false

  import ExUnit.CaptureLog

  alias Flezha.Relay.Replay
  alias Flezha.State.Frame
  alias Flezha.State.{Paths, RecordFrame, Restore, Snapshot, Supervisor, TestHelpers, WalWriter}

  setup do
    %{cell_id: TestHelpers.sample_cell_id("rest_" <> uniq())}
  end

  test "cold cell: empty snapshot, empty change_log, terminal_seq=-1 (no records)", %{
    cell_id: pid
  } do
    assert {:ok, payload} = Restore.payload(pid, :latest)
    assert payload.snapshot_bytes == <<>>
    assert payload.snapshot_header == <<>>
    assert payload.change_log_bytes == <<>>
    assert payload.terminal_seq == -1
    refute Restore.has_persisted_state?(pid)
  end

  test "snapshot only: change_log empty, terminal_seq matches snapshot seq", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 50, header, "snap-bytes")

    assert {:ok, payload} = Restore.payload(pid, :latest)
    assert payload.snapshot_bytes == "snap-bytes"

    # The header travels with the body: it carries the schema_version the
    # snapshot was written at, which the SDK resolves before decoding.
    assert payload.snapshot_header == header
    assert byte_size(payload.snapshot_header) == 16
    assert payload.change_log_bytes == <<>>
    assert payload.terminal_seq == 50
    assert Restore.has_persisted_state?(pid)
  end

  test "snapshot + tail WAL: change_log carries seq > snapshot.seq only", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

    {:ok, _} = Snapshot.write_snapshot(pid, 3, header, "snap@3")

    assert {:ok, payload} = Restore.payload(pid, :latest)
    assert payload.snapshot_bytes == "snap@3"
    assert payload.snapshot_header == header
    assert payload.terminal_seq == 5

    # change_log_bytes contains exactly seq 4 and seq 5 in WAL framing.
    expected =
      Frame.encode_record(4, header, "x4") <>
        Frame.encode_record(5, header, "x5")

    assert payload.change_log_bytes == expected
  end

  test "a corrupt record below the terminal seq refuses the payload and parks the relay",
       %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, srv} = Supervisor.find_or_start(pid)
    for n <- 0..2, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
    TestHelpers.close_and_await(srv, pid)

    path = Paths.wal_path(pid)
    [_first, second, _third] = TestHelpers.record_offsets(path)
    size = RecordFrame.file_size(path)
    TestHelpers.corrupt_record_body(path, second)

    log =
      capture_log(fn ->
        # Shipping seq 0 as the whole change log would restore the Cell from a
        # history that was cut short and report it as complete.
        assert {:error, reason} = Restore.payload(pid, :latest)
        assert is_binary(reason)
        assert reason =~ "crc_mismatch"
      end)

    assert log =~ ~r/record_frame_refused .*reason=crc_mismatch/
    assert RecordFrame.file_size(path) == size

    # The one place the awaited restore's answer is read parks the relay on
    # a reported refusal — the controller's own reader now produces one.
    handler =
      :telemetry_test.attach_event_handlers(self(), [[:flezha, :state, :restore_refused]])

    refused_log =
      capture_log(fn ->
        assert {:error, reason} = Replay.await_restore("exec-absent", pid)
        assert reason =~ "crc_mismatch"
      end)

    assert_receive {[:flezha, :state, :restore_refused], ^handler, %{count: 1},
                    %{cell: ^pid, runner: "exec-absent"}}

    :telemetry.detach(handler)

    assert refused_log =~
             ~r/\[flezha_state\] restore_refused .*reason=.*crc_mismatch.*runner=exec-absent/
  end

  test "a corrupt LAST record is the torn tail the writer repairs, and still ships",
       %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, srv} = Supervisor.find_or_start(pid)
    for n <- 0..2, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
    TestHelpers.close_and_await(srv, pid)

    path = Paths.wal_path(pid)
    [_first, _second, last] = TestHelpers.record_offsets(path)
    TestHelpers.corrupt_record_body(path, last)

    # Nothing complete can begin after it, so the proof holds and the payload
    # is the records before it — not a refusal.
    assert {:ok, payload} = Restore.payload(pid, :latest)
    assert payload.terminal_seq == 1

    assert payload.change_log_bytes ==
             Frame.encode_record(0, header, "x0") <>
               Frame.encode_record(1, header, "x1")
  end
end

defmodule Flezha.State.SnapshotTest do
  # async: false: writes Application env; a shared on-disk root; node-global tables.
  use FlezhaTest.StateCase, async: false

  alias Flezha.State.{Paths, RecordFrame, Snapshot, Supervisor, TestHelpers, WalWriter}

  setup do
    %{cell_id: TestHelpers.sample_cell_id("snap_" <> uniq())}
  end

  test "write_snapshot round-trip", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, path} = Snapshot.write_snapshot(pid, 100, header, "state-blob")
    assert File.exists?(path)
    assert {:ok, ^header, "state-blob"} = Snapshot.read_snapshot(path)
  end

  test "write_snapshot truncates the WAL up to the snapshot's seq", %{cell_id: pid} do
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)
    header = TestHelpers.stub_header()

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))

    {:ok, _} = Snapshot.write_snapshot(pid, 3, header, "snap@3")

    # WAL retains records 4, 5.
    {:ok, dev} = :file.open(Paths.wal_path(pid), [:raw, :read, :binary])
    assert {:ok, 4, _, "x4", c1} = RecordFrame.decode_record(dev, 0)
    assert {:ok, 5, _, "x5", c2} = RecordFrame.decode_record(dev, c1)
    assert {:error, _, _} = RecordFrame.decode_record(dev, c1 + c2)
    :file.close(dev)
  end

  test "latest_snapshot returns the highest-seq file", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 10, header, "a")
    {:ok, _} = Snapshot.write_snapshot(pid, 100, header, "b")
    {:ok, _} = Snapshot.write_snapshot(pid, 50, header, "c")

    assert {100, path} = Snapshot.latest_snapshot(pid)
    assert String.ends_with?(path, ".snap.100")
  end

  test "atomic rename: stale .tmp does not appear as a snapshot", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 7, header, "good")

    # Hand-craft a leftover .tmp like a crash mid-write would leave.
    dir = Paths.state_dir(pid)
    {_, _, key} = pid
    tmp = Path.join(dir, Paths.encode_cell_key(key) <> ".snap.99.tmp")
    File.write!(tmp, "garbage-not-a-snapshot")

    # Latest seq is still 7; the .tmp is filtered out.
    assert {7, _} = Snapshot.latest_snapshot(pid)
    assert %{snapshot_files: snaps} = Paths.scan_dir(pid)
    assert Enum.all?(snaps, fn {_, seq, _} -> seq == 7 end)
  end

  test "delete_snapshot removes only the named seq", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 1, header, "a")
    {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "b")

    :ok = Snapshot.delete_snapshot(pid, 1)
    assert [{2, _}] = Paths.snapshot_paths(pid)
  end
end

defmodule Flezha.State.TimeTravelTest do
  # async: false: writes Application env; a shared on-disk root; node-global tables.
  use FlezhaTest.StateCase, async: false

  alias Flezha.State.Frame
  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.{RecordFrame, Snapshot, Supervisor, TestHelpers, TimeTravel, WalWriter}

  setup do
    %{cell_id: TestHelpers.sample_cell_id("tt_" <> uniq())}
  end

  test "no_data error when cell has no persisted state", %{cell_id: pid} do
    assert {:error, {:no_persisted_state, _cell}} = TimeTravel.query(pid, {:seq, 100})
  end

  test "seq query returns the right slice", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "v" <> Integer.to_string(n))
    {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")

    assert {:ok, result} = TimeTravel.query(pid, {:seq, 4})
    assert result.snapshot_bytes == "snap@2"
    assert result.snapshot_header == header
    assert result.terminal_seq == 4

    expected =
      Frame.encode_record(3, header, "v3") <>
        Frame.encode_record(4, header, "v4")

    assert result.change_log_bytes == expected
  end

  test "a seq query on a Cell with no snapshot keeps its first record", %{cell_id: pid} do
    # fails when: a Cell with no snapshot is read as if a snapshot covered seq 0,
    # so its first record is left out of the change log.
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, srv} = Supervisor.lookup(pid)

    for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "v" <> Integer.to_string(n))

    assert {:ok, result} = TimeTravel.query(pid, {:seq, 3})
    assert result.snapshot_bytes == ""
    assert result.terminal_seq == 3

    expected = for n <- 0..3, into: "", do: Frame.encode_record(n, header, "v#{n}")
    assert result.change_log_bytes == expected
  end

  test "outside_retention error for seq before earliest snapshot", %{cell_id: pid} do
    header = TestHelpers.stub_header()
    {:ok, _} = Snapshot.write_snapshot(pid, 100, header, "snap@100")

    assert {:error, {:outside_retention, _cell, 50, 100}} =
             TimeTravel.query(pid, {:seq, 50})
  end

  test "a steps Cell answers a point-in-time query as a stream Cell does" do
    class = "tt_steps_" <> uniq()
    :ok = ClassRegistry.put(class, :steps)
    on_exit(fn -> ClassRegistry.delete(class) end)
    cell_id = {class, "task", "seg"}
    header = TestHelpers.stub_header()
    {:ok, _} = Supervisor.find_or_start(cell_id)
    {:ok, srv} = Supervisor.lookup(cell_id)

    for n <- 0..3, do: :ok = WalWriter.append(srv, n, header, "i" <> Integer.to_string(n))
    {:ok, _} = Snapshot.write_snapshot(cell_id, 1, header, "iter@1")

    assert {:ok, result} = TimeTravel.query(cell_id, {:seq, 3})
    assert result.snapshot_bytes == "iter@1"
    assert result.snapshot_header == header
    assert result.terminal_seq == 3
  end
end

defmodule Flezha.State.DeleteCellTest do
  @moduledoc """
  dynamic cell mode Section 5: the agent-side hard-delete RPC.
  Covers `Flezha.State.Supervisor.delete_cell/1` — the target of both
  the SDK reaper's fire-and-forget `delete_cell_state` frame and the
  `flz state delete` CLI.
  """
  # async: false: writes Application env; a shared on-disk root; registered singletons.
  use FlezhaTest.StateCase, async: false

  alias Flezha.State.{Metrics, Paths, Supervisor, TestHelpers, WalWriter}
  alias Flezha.State.Snapshot, as: StateSnapshot

  setup do
    %{cell_id: TestHelpers.sample_cell_id("del_" <> uniq())}
  end

  test "deletes WAL + snapshot + outbox files and stops the writer", %{cell_id: pid} do
    # Materialise on-disk state via a live WalWriter.
    {:ok, writer} = Supervisor.find_or_start(pid)
    header = TestHelpers.stub_header()
    assert :ok = WalWriter.append(writer, 0, header, "a")
    assert :ok = WalWriter.append(writer, 1, header, "bb")
    WalWriter.fsync(writer)

    # Snapshot + outbox files so all three families are present.
    {:ok, _} = StateSnapshot.write_snapshot(pid, 1, header, "snap")
    File.write!(Paths.outbox_dedup_path(pid), "dedup")

    assert File.exists?(Paths.wal_path(pid))
    assert Paths.snapshot_paths(pid) != []
    assert File.exists?(Paths.outbox_dedup_path(pid))
    assert List.keymember?(Metrics.snapshot_all(), pid, 0)

    assert {:ok, %{cell_id: ^pid, status: :ok}} = Supervisor.delete_cell(pid)

    # Writer stopped, every file gone, and no longer listed by its counters.
    assert Supervisor.lookup(pid) == :error
    refute File.exists?(Paths.wal_path(pid))
    assert Paths.snapshot_paths(pid) == []
    refute File.exists?(Paths.outbox_dedup_path(pid))
    refute List.keymember?(Metrics.snapshot_all(), pid, 0)
  end

  test "deleting a never-seen cell is idempotent (returns :ok)", %{cell_id: pid} do
    assert Supervisor.lookup(pid) == :error
    assert {:ok, %{cell_id: ^pid, status: :ok}} = Supervisor.delete_cell(pid)
    # Still no writer, still no files — and no crash.
    assert Supervisor.lookup(pid) == :error
    refute File.exists?(Paths.wal_path(pid))
  end
end

defmodule Flezha.State.CompletionTest do
  @moduledoc """
  §5b.3: the per-relay completion record — atomic write, read-back, and its
  presence as the retain-vs-delete signal.
  """
  # async: false: writes Application env; a shared on-disk root.
  use ExUnit.Case, async: false

  alias Flezha.State.{Completion, TestHelpers}

  setup do
    TestHelpers.tmp_app_data_dir()
    :ok
  end

  test "write/read round-trip preserves the record" do
    rec = %{namespace: "ns", status: "ok", error: "", result: "R", finished_at: 123}

    refute Completion.exists?("op1")
    assert :ok = Completion.write("op1", rec)
    assert Completion.exists?("op1")
    assert {:ok, ^rec} = Completion.read("op1")
  end

  test "read/1 returns :error when no record is present" do
    assert :error = Completion.read("absent")
  end

  test "delete/1 removes the record" do
    :ok =
      Completion.write("op2", %{
        namespace: "ns",
        status: "cancelled",
        error: "",
        result: "",
        finished_at: 1
      })

    assert Completion.exists?("op2")
    assert :ok = Completion.delete("op2")
    refute Completion.exists?("op2")
  end

  test "path/1 sits at the relay_class level" do
    assert Completion.path("op3") =~ "/op3/completion.record"
  end
end

defmodule Flezha.State.RetentionTest do
  # async: false: writes Application env; a shared on-disk root; the primary Logger.
  use FlezhaTest.StateCase, async: false

  alias Flezha.Relay.ClassRegistry

  alias Flezha.State.{
    Completion,
    Frame,
    Lineage,
    Paths,
    RecordFrame,
    Retention,
    Snapshot,
    Supervisor,
    TestHelpers,
    WalWriter
  }

  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.OutboxCase

  setup do
    TestHelpers.start_state_tree([Retention])

    on_exit(fn ->
      Application.delete_env(:flezha, :time_travel_retention_hours)
      # A writer a test started runs under the application's supervisor and
      # still syncs after the body, so it is stopped before its root goes.
      TestHelpers.reset_all_cells()
    end)

    %{cell_id: TestHelpers.sample_cell_id("ret_" <> uniq())}
  end

  describe "sweep_cell" do
    test "0-retention truncates WAL down to the latest snapshot", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      for n <- 0..6, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 4, header, "snap@4")
      # After snapshot the writer is at seq 6 with WAL = [5, 6].
      assert WalWriter.last_persisted_seq(srv) == 6

      :ok = Retention.sweep_cell(pid, 0)

      # Latest snapshot remains; WAL should still be [5, 6] because they're > kept_seq=4.
      assert {4, _} = Snapshot.latest_snapshot(pid)

      # Add an older snapshot to demonstrate it gets reaped under 0 retention.
      {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")
      :ok = Retention.sweep_cell(pid, 0)
      assert [{5, _}] = Paths.snapshot_paths(pid)
    end

    test "non-zero retention keeps only the latest when all snaps are within window", %{
      cell_id: pid
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
      :ok = Retention.sweep_cell(pid, 1)
      assert [{3, _}] = Paths.snapshot_paths(pid)
    end

    # fails when: a cut that finds a running writer renames a cut copy under it,
    # so the writer's handle keeps the replaced file and its next record is lost.
    test "a cut that finds a running writer hands the cut to it", %{cell_id: pid} do
      header = TestHelpers.stub_header()
      {:ok, srv} = Supervisor.find_or_start(pid)
      for n <- 0..4, do: :ok = WalWriter.append(srv, n, header, "y" <> Integer.to_string(n))
      TestHelpers.close_and_await(srv, pid)
      {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
      {:ok, srv} = Supervisor.find_or_start(pid)

      :ok = Retention.sweep_cell(pid, 0)
      :ok = WalWriter.append(srv, 5, header, "y5")

      records = RecordFrame.fold_path!(Paths.wal_path(pid), [], &{:cont, [elem(&1, 0) | &2]})
      assert Enum.reverse(records) == [3, 4, 5]
    end

    test "a cut with no writer keeps the consumed offset its dropped records carry", %{
      cell_id: pid
    } do
      # fails when: the offline cut drops records without folding their
      # consumed offsets, so the Cell's floor read from disk falls back.
      header = OutboxCase.effect_header()

      wal =
        for {seq, offset} <- [{0, 39}, {1, 40}, {2, 41}, {3, nil}], into: "" do
          Frame.encode_record(seq, header, OutboxCase.effect_body([], offset))
        end

      wal_path = Paths.wal_path(pid)
      File.mkdir_p!(Path.dirname(wal_path))
      File.write!(wal_path, wal)
      {:ok, _} = Snapshot.write_snapshot(pid, 2, TestHelpers.stub_header(), "snap@2")
      assert WalWriter.consumed_offset_on_disk(pid) == 41

      :ok = Retention.sweep_cell(pid, 0)

      assert Supervisor.lookup(pid) == :error
      assert WalWriter.consumed_offset_on_disk(pid) == 41
    end

    # fails when: a cut with no writer folds records a pending rewind drops, so the
    # floor claims the inputs its correction replays.
    test "a cut with no writer leaves a Cell whose bump is pending as it found it", %{
      cell_id: pid
    } do
      wal =
        for seq <- 0..3, into: "" do
          Frame.encode_record(
            seq,
            OutboxCase.effect_header(),
            OutboxCase.effect_body([], 40 + seq)
          )
        end

      File.mkdir_p!(Path.dirname(Paths.wal_path(pid)))
      File.write!(Paths.wal_path(pid), wal)
      stored = %{id: nil, trail: [], landed: false, epoch: 1, folded_offset: -1, cut: {:above, 0}}
      :ok = Lineage.put(pid, stored)
      {:ok, _} = Snapshot.write_snapshot(pid, 2, TestHelpers.stub_header(), "snap@2")

      :ok = Retention.sweep_cell(pid, 0)

      assert File.read!(Paths.wal_path(pid)) == wal
      assert %{cut: {:above, 0}, progress: %{folded_offset: -1}} = Lineage.read(pid)
    end
  end

  describe "finished-job retention (steps)" do
    test "retains a completion record within the window, reclaims it after" do
      now = System.system_time(:second)
      on_exit(TestHelpers.override_env(:default_relay_completed_retention_hours, 1))

      # Within the one-hour window → retained.
      :ok =
        Completion.write("bnd_live", %{
          namespace: "ns",
          status: "ok",
          error: "",
          result: "r",
          finished_at: now
        })

      # Past it → reclaimed wholesale.
      :ok =
        Completion.write("bnd_old", %{
          namespace: "ns",
          status: "error",
          error: "boom",
          result: "",
          finished_at: now - 7_200
        })

      :ok = Retention.sweep_now()

      assert Completion.exists?("bnd_live")
      refute Completion.exists?("bnd_old")
      refute File.dir?(Paths.class_dir("bnd_old"))
    end
  end

  describe "time-travel retention" do
    test "keeps the older snapshot under the window for a stream and a steps alike" do
      header = TestHelpers.stub_header()

      for kind <- [:flow, :steps] do
        class = "ret_#{kind}_" <> uniq()
        pid = {class, "task", "seg"}
        ClassRegistry.put(class, kind)
        on_exit(fn -> ClassRegistry.delete(class) end)

        {:ok, _} = Supervisor.find_or_start(pid)
        {:ok, srv} = Supervisor.lookup(pid)
        for n <- 0..5, do: :ok = WalWriter.append(srv, n, header, "x")
        {:ok, _} = Snapshot.write_snapshot(pid, 2, header, "snap@2")
        {:ok, _} = Snapshot.write_snapshot(pid, 5, header, "snap@5")

        # snap@2 is older than the 48h window, so it stays as the floor a
        # point-in-time query reads from.
        File.touch!(Paths.snapshot_path(pid, 2), System.os_time(:second) - 5 * 24 * 3600)
        Application.put_env(:flezha, :time_travel_retention_hours, %{class => 48})

        :ok = Retention.sweep_now()

        assert [{2, _}, {5, _}] = Paths.snapshot_paths(pid), "#{kind}"
      end
    end
  end

  describe "a tick with nothing to cut" do
    test "leaves the WAL file untouched", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      for n <- 0..6, do: :ok = WalWriter.append(srv, n, header, "x" <> Integer.to_string(n))
      {:ok, _} = Snapshot.write_snapshot(pid, 4, header, "snap@4")
      # Compaction already cut the WAL to [5, 6]; a retention tick that keeps
      # the latest snapshot has nothing below seq 4 left to remove.
      {:ok, %{inode: inode}} = File.stat(Paths.wal_path(pid))

      :ok = Retention.sweep_cell(pid, 6)

      assert {:ok, %{inode: ^inode}} = File.stat(Paths.wal_path(pid)),
             "a no-op cut must not rewrite (and re-inode) the file"

      assert WalWriter.last_persisted_seq(srv) == 6
    end
  end

  describe "a compaction cut" do
    test "past the index samples keeps exactly the tail", %{cell_id: pid} do
      {:ok, _} = Supervisor.find_or_start(pid)
      {:ok, srv} = Supervisor.lookup(pid)
      header = TestHelpers.stub_header()

      seqs = fn ->
        RecordFrame.fold_path!(Paths.wal_path(pid), [], fn {s, _, _}, acc ->
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
    alias Flezha.Relay.ClassRegistry

    # The kill below is the point of the test; the supervisor's report of it
    # is not console noise worth keeping.
    @tag :capture_log
    test "outlives a Retention crash" do
      class = "reg_" <> uniq()
      :ok = ClassRegistry.put_overwatch(class, self(), nil)

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
