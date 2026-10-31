defmodule Flezha.State.WalWriterFullDiskTest do
  @moduledoc """
  A WAL writer whose disk refuses a write or a sync stays up: what it wrote
  since its last sync is cut from the file and released, never drained, its
  waiters are refused, what was synced before stays, and the owning runner's
  state is pulled again as the new baseline, installed in the writer's order
  with the one Change the runner restated above it. `TestHelpers.fill_disk/1`
  stands in for the full disk.
  """
  # async: false: Mimic global mode; Mimic stubs; the shared state root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  use Mimic
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1]

  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.State.{Commands, Paths, Recovery, Supervisor, TestHelpers, WalWriter}
  alias Flezha.State.WriterRegistry

  unless File.exists?("/dev/full"), do: @moduletag(skip: "needs /dev/full")

  setup :set_mimic_global_until_exit

  @session "wal-full-disk-session"

  setup do
    stub(Flezha.State.FsyncBatcher, :mark_dirty, fn _cell_id -> :ok end)
    stub(Flezha.Runner.ControlChannel, :send_control_async, fn _runner_id, _command -> :ok end)
    on_exit(override_log_level(:warning))
    on_exit(&TestHelpers.reset_all_cells/0)

    cell_id = TestHelpers.sample_cell_id("full_#{System.unique_integer([:positive])}")
    {:ok, pid} = Supervisor.find_or_start(cell_id)

    # Three changes synced: their bytes drained. A fourth written, not synced.
    for seq <- 0..2, do: append!(pid, seq, "ack#{seq}", 10)
    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 30, ^cell_id}
    append!(pid, 3, "unsynced", 5)
    assert WalWriter.last_persisted_seq(pid) == 3

    %{pid: pid, cell_id: cell_id}
  end

  defp append!(pid, seq, body, n) do
    credit = %{channel: self(), session: @session, wire_size: n}
    WalWriter.append_async(pid, seq, TestHelpers.stub_header(), body, "exec-1", credit)
  end

  defp seqs(cell_id) do
    {:ok, seqs} = TestHelpers.record_seqs(Paths.wal_path(cell_id))
    seqs
  end

  defp stub_control() do
    parent = self()

    stub(Flezha.Runner.ControlChannel, :send_control, fn runner_id, {tag, _command} ->
      send(parent, {:control_sent, runner_id, tag})
      {:ok, make_ref()}
    end)
  end

  # The disk refuses the write of seq 4, so the unsynced seq 3 is cut too.
  defp cut!(pid) do
    TestHelpers.fill_disk(pid)

    capture_log(fn ->
      append!(pid, 4, "refused", 40)
      assert WalWriter.last_persisted_seq(pid) == 2
    end)
  end

  test "a refused write releases the change in hand and the unsynced one, and keeps the writer",
       %{pid: pid, cell_id: cell_id} do
    become_loop()
    stub_control()
    TestHelpers.fill_disk(pid)

    log_text =
      capture_log(fn ->
        append!(pid, 4, "refused", 40)
        assert WalWriter.last_persisted_seq(pid) == 2
      end)

    assert log_text =~ "disk refused :enospc"

    # Neither was written durably, so neither may count as drained: a drained
    # byte is what lets a processed-prefix marker confirm the input behind it.
    assert_received {:credit_released, @session, 5, ^cell_id}
    assert_received {:credit_released, @session, 40, ^cell_id}
    refute_received {:credit_drained, _, _, _}
    assert seqs(cell_id) == [0, 1, 2]

    # The cut is declared as a gap naming the runner, so the loop resyncs it.
    assert_receive {:declare, :seq_gap, ^cell_id, %{runner_id: "exec-1", trigger: :unsynced}}

    # Still full: the waiting form is refused, retryably, by the same writer,
    # and the episode already warned.
    TestHelpers.fill_disk(pid)

    refute capture_log(fn ->
             assert {:error, {:wal_writer_unavailable, :enospc}} =
                      WalWriter.append(pid, 3, TestHelpers.stub_header(), "again")
           end) =~ "disk refused"

    assert {:ok, ^pid} = WriterRegistry.lookup(cell_id)

    # Every resync of the cut Cell pulls the runner's state, whichever fact
    # asked for it: a push of the cut log would rewind the live Cell past the
    # changes the cut dropped.
    for reason <- [:seq_gap, :compaction_gave_up] do
      assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: reason)
      assert_receive {:control_sent, "exec-1", :compact_cell}
    end

    refute_received {:control_sent, _, :restore_init_state}

    # The runner drains its change at 5 into the snapshot, and the one record
    # it restates at 6 precedes the snapshot frame: the gap holds the newest,
    # releasing the one before, which the snapshot covers.
    append!(pid, 5, "drained", 7)
    append!(pid, 6, "restated", 8)
    assert WalWriter.last_persisted_seq(pid) == 2
    assert_receive {:credit_released, @session, 7, ^cell_id}
    refute_received {:credit_drained, _, _, _}

    # The baseline at 5, installed in the writer's order, appends the held 6,
    # so the runner's next change lands after it: no gap, and no push.
    :ok = WalWriter.install_snapshot(pid, "exec-1", 5, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(pid) == 6
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}

    append!(pid, 7, "next", 6)
    assert capture_log(fn -> :ok = WalWriter.fsync(pid) end) =~ "the disk takes writes again"
    assert_receive {:credit_drained, @session, 14, ^cell_id}
    assert seqs(cell_id) == [6, 7]
    refute_received {:declare, :seq_gap, ^cell_id, _}
    refute_received {:control_sent, _, :restore_init_state}
  end

  test "a writer stopped while holding a Change releases its credit", %{
    pid: pid,
    cell_id: cell_id
  } do
    stub_control()
    cut!(pid)
    append!(pid, 6, "held", 9)
    assert WalWriter.last_persisted_seq(pid) == 2
    refute_received {:credit_released, @session, 9, _}

    :ok = WalWriter.close(pid)
    assert_received {:credit_released, @session, 9, ^cell_id}
  end

  # fails when: the held Changes of many Cells exhaust the runner's shared
  # credit window, so every pull is refused and nothing frees it.
  test "a pull the runner refuses for credit releases what its writers hold, and pulls again",
       %{pid: pid, cell_id: cell_id} do
    parent = self()
    stub_control()
    cut!(pid)
    append!(pid, 6, "held", 9)
    assert WalWriter.last_persisted_seq(pid) == 2
    refute_received {:credit_released, @session, 9, _}

    stub(Flezha.Runner.ControlUnit, :send_control_await, fn _runner_id, {tag, _command}, _opts ->
      send(parent, {:control_awaited, tag})
      why = "the controller's credit window stayed exhausted"
      {:error, Flezha.Error.new("state.cell_busy", %{cell: "c", why: why})}
    end)

    start_supervised!(%{
      id: :busy_control_unit,
      start:
        {Agent, :start_link, [fn -> nil end, [name: Flezha.Identificator.control_unit("exec-1")]]}
    })

    assert {:ok, :deferred} = Commands.dispatch_compact_cell("exec-1", cell_id, await: true)

    # The release is a cast; the call behind it orders it.
    assert WalWriter.last_persisted_seq(pid) == 2
    assert_received {:credit_released, @session, 9, ^cell_id}

    # The episode stays open, so the next resync pulls again.
    assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)
    assert_receive {:control_awaited, :compact_cell}
    assert_receive {:control_awaited, :compact_cell}
    refute_received {:control_awaited, :restore_init_state}
    refute_received {:control_sent, _, :restore_init_state}
  end

  test "a Cell with no log holds the Change restated above its first baseline and appends it" do
    cold_id = TestHelpers.sample_cell_id("cold_#{System.unique_integer([:positive])}")
    {:ok, cold} = Supervisor.find_or_start(cold_id)

    append!(cold, 7, "drained", 9)
    append!(cold, 8, "restated", 10)
    assert WalWriter.last_persisted_seq(cold) == -1
    assert_receive {:credit_released, @session, 9, ^cold_id}

    :ok = WalWriter.install_snapshot(cold, "exec-1", 7, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(cold) == 8

    append!(cold, 9, "next", 11)
    :ok = WalWriter.fsync(cold)
    assert_receive {:credit_drained, @session, 21, ^cold_id}
    assert seqs(cold_id) == [8, 9]
  end

  # fails when: a late pull answered by an instance the writer does not own
  # becomes the baseline, and the owner's later records drop as duplicates.
  test "only the owning runner's snapshot is installed", %{pid: pid, cell_id: cell_id} do
    become_loop()
    stub_control()
    cut!(pid)
    append!(pid, 6, "held", 9)

    :ok = WalWriter.install_snapshot(pid, "exec-stale", 5, TestHelpers.stub_header(), "stale")
    assert WalWriter.last_persisted_seq(pid) == 2
    refute_received {:declare, :seq_gap_closed, ^cell_id, _}
    refute_received {:credit_released, @session, 9, _}

    # A reconnect swaps the session, not the runner: its snapshot is the owner's.
    :ok = Marks.bind_identity("exec-1", "stable-#{inspect(cell_id)}")
    :ok = Marks.bind_identity("exec-1b", "stable-#{inspect(cell_id)}")
    :ok = WalWriter.install_snapshot(pid, "exec-1b", 5, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(pid) == 6
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}
  end

  # fails when: a reconnected runner's first non-contiguous record is refused
  # as another runner's, instead of taking the gap path that pulls.
  test "a reconnected session's gap is a gap", %{pid: pid, cell_id: cell_id} do
    become_loop()
    :ok = WalWriter.fsync(pid)
    :ok = Marks.bind_identity("exec-1", "stable-#{inspect(cell_id)}")
    :ok = Marks.bind_identity("exec-1b", "stable-#{inspect(cell_id)}")
    credit = %{channel: self(), session: @session, wire_size: 4}
    WalWriter.append_async(pid, 9, TestHelpers.stub_header(), "late", "exec-1b", credit)

    assert WalWriter.last_persisted_seq(pid) == 3
    assert_receive {:declare, :seq_gap, ^cell_id, %{runner_id: "exec-1b", trigger: :seq_gap}}
    assert seqs(cell_id) == [0, 1, 2, 3]
  end

  # fails when: a pull that overtakes a correction's restore installs the state
  # from before the rewind.
  test "after a rewind only a snapshot the log reaches is installed",
       %{pid: pid, cell_id: cell_id} do
    stub_control()
    :ok = WalWriter.fsync(pid)
    :ok = WalWriter.rewind_to(pid, 1, WalWriter.outbox_epoch(pid) + 1)

    :ok = WalWriter.install_snapshot(pid, "exec-1", 3, TestHelpers.stub_header(), "pre-rewind")
    assert WalWriter.last_persisted_seq(pid) == 1

    :ok = WalWriter.install_snapshot(pid, "exec-1", 1, TestHelpers.stub_header(), "at the rewind")
    assert WalWriter.last_persisted_seq(pid) == 1
    assert Enum.map(Paths.snapshot_paths(cell_id), &elem(&1, 0)) == [1]
  end

  test "a snapshot the disk refuses changes nothing, and the gap stays open",
       %{pid: pid, cell_id: cell_id} do
    become_loop()
    stub_control()
    cut!(pid)
    append!(pid, 6, "held", 9)

    stub(Paths, :atomic_install, fn
      _path, _write, :snapshot -> {:error, :enospc}
      path, write, kind -> Mimic.call_original(Paths, :atomic_install, [path, write, kind])
    end)

    assert capture_log(fn ->
             :ok = WalWriter.install_snapshot(pid, "exec-1", 5, TestHelpers.stub_header(), "s")
             assert WalWriter.last_persisted_seq(pid) == 2
           end) =~ "snapshot refused"

    refute_received {:declare, :seq_gap_closed, ^cell_id, _}
    refute_received {:credit_released, @session, 9, _}
  end

  # fails when: a bind that re-seats the runner's Cell leaves the gap open and
  # the old owner, so a stale instance's late pull can still become the baseline.
  test "a bind's re-seat closes the gap, lets the held Change go and names the owner",
       %{pid: pid, cell_id: cell_id} do
    become_loop()
    stub_control()
    cut!(pid)
    append!(pid, 6, "held", 9)

    :ok = WalWriter.reseat(pid, "exec-bound")
    assert %{owning_runner: "exec-bound"} = WalWriter.stall_status(pid)
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}
    assert_received {:credit_released, @session, 9, ^cell_id}
  end

  test "a writer with no runner warns once per episode and opens no gap" do
    become_loop()
    journal_id = TestHelpers.sample_cell_id("journal_#{System.unique_integer([:positive])}")
    {:ok, journal} = Supervisor.find_or_start(journal_id)
    header = TestHelpers.stub_header()
    {:ok, 0} = WalWriter.append_next(journal, header, "a")

    TestHelpers.fill_disk(journal)

    assert capture_log(fn ->
             assert {:error, _} = WalWriter.append_next(journal, header, "b")
           end) =~ "disk refused"

    TestHelpers.fill_disk(journal)

    refute capture_log(fn ->
             assert {:error, _} = WalWriter.append_next(journal, header, "b")
           end) =~ "disk refused"

    assert capture_log(fn ->
             assert {:ok, 1} = WalWriter.append_next(journal, header, "b")
           end) =~ "the disk takes writes again"

    refute_received {:declare, :seq_gap, ^journal_id, _}
  end

  test "a refused sync drops what it did not sync and refuses its waiters",
       %{pid: pid, cell_id: cell_id} do
    waiter =
      WalWriter.append_next_async(
        pid,
        TestHelpers.stub_header(),
        "batched",
        [sync: :batched],
        :gen_server.reqids_new(),
        :batched
      )

    assert WalWriter.last_persisted_seq(pid) == 4
    TestHelpers.fill_disk(pid)

    assert capture_log(fn -> :ok = WalWriter.fsync(pid) end) =~ "disk refused :einval"

    assert {{:reply, {:error, {:wal_writer_unavailable, :einval}}}, :batched, _} =
             :gen_server.wait_response(waiter, 1_000, true)

    assert_received {:credit_released, @session, 5, ^cell_id}
    refute_received {:credit_drained, _, _, _}
    assert WalWriter.last_persisted_seq(pid) == 2
    assert seqs(cell_id) == [0, 1, 2]
    assert {:ok, ^pid} = WriterRegistry.lookup(cell_id)
  end

  # A sync-mode answer waits with the batched ones, so a failed sync refuses it.
  test "a sync-mode append refused by a failed sync is answered with the refusal",
       %{pid: pid, cell_id: cell_id} do
    TestHelpers.fill_disk(pid, "/dev/null")

    assert capture_log(fn ->
             assert {:error, {:wal_writer_unavailable, :einval}} =
                      WalWriter.append_next(pid, TestHelpers.stub_header(), "lost")
           end) =~ "disk refused :einval"

    assert_received {:credit_released, @session, 5, ^cell_id}
    assert WalWriter.last_persisted_seq(pid) == 2
    assert seqs(cell_id) == [0, 1, 2]
  end
end
