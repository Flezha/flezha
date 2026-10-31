defmodule Flezha.State.WalWriterFullDiskTest do
  @moduledoc """
  A WAL writer whose disk refuses a write or a sync stays up: what it wrote
  since its last sync is cut from the file and released, never drained, its
  waiters are refused, what was synced before stays, and the runner's state is
  pulled again as the new baseline, the Changes refused meanwhile appended
  after it. `TestHelpers.fill_disk/1` stands in for the full disk.
  """
  # async: false: Mimic global mode; Mimic stubs; the shared state root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  use Mimic
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 3]

  alias Flezha.State.{Commands, Paths, Recovery, Snapshot, Supervisor, TestHelpers, WalWriter}
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

    assert log_text =~ "the disk refused a write or a sync (:enospc)"
    assert log_text =~ "records from seq 3 on are dropped"

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
           end) =~ "the disk refused"

    assert {:ok, ^pid} = WriterRegistry.lookup(cell_id)

    # Every resync of the cut Cell pulls the runner's state, whichever fact
    # asked for it: a push of the cut log would rewind the live Cell past the
    # changes the cut dropped.
    for reason <- [:seq_gap, :compaction_gave_up] do
      assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: reason)
      assert_receive {:control_sent, "exec-1", :compact_cell}
    end

    refute_received {:control_sent, _, :restore_init_state}

    # The runner drains its change at 5 into the snapshot, and the record it
    # re-states at 6 precedes the snapshot frame: both reach the writer before
    # the baseline, as gaps, and are kept, their credit with them.
    append!(pid, 5, "drained", 7)
    append!(pid, 6, "restated", 8)
    assert WalWriter.last_persisted_seq(pid) == 2
    refute_received {:credit_drained, _, _, _}

    # The baseline at 5 covers the first and appends the second, so the
    # runner's next change lands after it: no gap, and no push.
    assert {:ok, _path} = Snapshot.write_snapshot(cell_id, 5, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(pid) == 6
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}
    assert_receive {:credit_drained, @session, 7, ^cell_id}

    append!(pid, 7, "next", 6)
    assert capture_log(fn -> :ok = WalWriter.fsync(pid) end) =~ "the disk takes writes again"
    assert_receive {:credit_drained, @session, 14, ^cell_id}
    assert seqs(cell_id) == [6, 7]
    refute_received {:declare, :seq_gap, ^cell_id, _}
    refute_received {:control_sent, _, :restore_init_state}
  end

  defp kept_bytes(pid), do: elem(:sys.get_state(pid).kept, 0)

  test "kept credit stays under its cap, and only a baseline at the highest seq let go ends the pull",
       %{pid: pid, cell_id: cell_id} do
    become_loop()
    stub_control()
    cut!(pid)
    :sys.replace_state(pid, &put_in(&1.limits.kept_cap, 20))

    # Counted in the bytes the stream window debited: one larger than the cap
    # alone is released at once, never drained.
    append!(pid, 5, "aaaaaaaa", 25)
    assert kept_bytes(pid) == 0
    assert_received {:credit_released, @session, 25, ^cell_id}

    # Two that do not fit together: the older is released, the newer kept.
    append!(pid, 6, "bbbbbbbb", 11)
    append!(pid, 7, "cccccccc", 12)
    assert kept_bytes(pid) == 12
    assert_received {:credit_released, @session, 11, ^cell_id}
    assert WalWriter.last_persisted_seq(pid) == 2

    # A baseline at 5 is below the 6 let go: the change at 7 stays kept with
    # its credit, the gap stays open, and the next resync pulls again.
    assert {:ok, _path} = Snapshot.write_snapshot(cell_id, 5, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(pid) == 5
    refute_received {:credit_released, @session, 12, _}
    refute_received {:credit_drained, _, _, _}
    refute_received {:declare, :seq_gap_closed, ^cell_id, _}

    assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)
    assert_receive {:control_sent, "exec-1", :compact_cell}
    refute_received {:control_sent, _, :restore_init_state}

    # The baseline at 6 reaches it: 7 is appended and the episode ends.
    assert {:ok, _path} = Snapshot.write_snapshot(cell_id, 6, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(pid) == 7
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}
    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 12, ^cell_id}
    assert seqs(cell_id) == [7]
  end

  # The retention sweep truncates below a Cell's latest snapshot, at or below
  # its last seq, whatever the Cell is waiting for.
  test "a retention cut below what the pull lost keeps the pull open", %{
    pid: pid,
    cell_id: cell_id
  } do
    become_loop()
    stub_control()
    cut!(pid)

    assert {:ok, _path} = Snapshot.write_snapshot(cell_id, 2, TestHelpers.stub_header(), "state")
    refute_received {:declare, :seq_gap_closed, ^cell_id, _}
    assert %{awaiting_baseline?: true} = WalWriter.stall_status(pid)

    # The Cell has state on disk now, and still the resync pulls.
    assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)
    assert_receive {:control_sent, "exec-1", :compact_cell}
    refute_received {:control_sent, _, :restore_init_state}

    assert {:ok, _path} = Snapshot.write_snapshot(cell_id, 4, TestHelpers.stub_header(), "state")
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}
    assert %{awaiting_baseline?: false} = WalWriter.stall_status(pid)
  end

  test "a refused sync that cut nothing ends its pull on the next contiguous change", %{
    pid: pid,
    cell_id: cell_id
  } do
    become_loop()
    :ok = WalWriter.fsync(pid)
    assert_receive {:credit_drained, @session, 5, ^cell_id}
    TestHelpers.fill_disk(pid, "/dev/null")

    assert capture_log(fn -> :ok = WalWriter.fsync(pid) end) =~ "the disk refused"
    assert_receive {:declare, :seq_gap, ^cell_id, %{trigger: :unsynced}}
    assert WalWriter.last_persisted_seq(pid) == 3

    append!(pid, 4, "next", 6)
    assert WalWriter.last_persisted_seq(pid) == 4
    assert_receive {:declare, :seq_gap_closed, ^cell_id, %{}}
    assert %{awaiting_baseline?: false} = WalWriter.stall_status(pid)
  end

  test "a writer stopped while keeping changes releases their credit", %{
    pid: pid,
    cell_id: cell_id
  } do
    stub_control()
    cut!(pid)
    append!(pid, 6, "kept", 9)
    assert WalWriter.last_persisted_seq(pid) == 2
    refute_received {:credit_released, @session, 9, _}

    :ok = WalWriter.close(pid)
    assert_received {:credit_released, @session, 9, ^cell_id}
  end

  test "a pull the runner refuses for credit releases what awaiting writers keep, and pulls again",
       %{pid: pid, cell_id: cell_id} do
    parent = self()
    stub_control()
    cut!(pid)
    append!(pid, 6, "kept", 9)
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
    assert %{awaiting_baseline?: true} = WalWriter.stall_status(pid)
    assert_received {:credit_released, @session, 9, ^cell_id}

    # The episode stays open, so the next resync pulls again.
    assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)
    assert_receive {:control_awaited, :compact_cell}
    assert_receive {:control_awaited, :compact_cell}
    refute_received {:control_awaited, :restore_init_state}
    refute_received {:control_sent, _, :restore_init_state}
  end

  test "a writer booted from a log its predecessor may have cut comes up awaiting a baseline", %{
    pid: pid,
    cell_id: cell_id
  } do
    stub_control()
    :ok = WalWriter.close(pid)
    # The registry drops a stopped writer's name after it exits.
    assert wait_until(fn -> WriterRegistry.lookup(cell_id) == :error end, 1_000, 1)
    {:ok, restarted} = Supervisor.find_or_start(cell_id)
    refute restarted == pid
    assert %{awaiting_baseline?: true} = WalWriter.stall_status(restarted)

    assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)
    assert_receive {:control_sent, "exec-1", :compact_cell}
    refute_received {:control_sent, _, :restore_init_state}
  end

  test "a writer that does not answer is pulled, never pushed", %{pid: pid, cell_id: cell_id} do
    stub_control()
    Application.put_env(:flezha, :wal_writer_call_ms, 50)
    :sys.suspend(pid)

    try do
      assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)
    after
      :sys.resume(pid)
    end

    assert_receive {:control_sent, "exec-1", :compact_cell}
    refute_received {:control_sent, _, :restore_init_state}
  end

  test "a Cell with no log keeps the changes refused before its baseline and appends those above it" do
    cold_id = TestHelpers.sample_cell_id("cold_#{System.unique_integer([:positive])}")
    {:ok, cold} = Supervisor.find_or_start(cold_id)

    append!(cold, 7, "drained", 9)
    append!(cold, 8, "restated", 10)
    assert WalWriter.last_persisted_seq(cold) == -1

    assert {:ok, _path} = Snapshot.write_snapshot(cold_id, 7, TestHelpers.stub_header(), "state")
    assert WalWriter.last_persisted_seq(cold) == 8
    assert_receive {:credit_drained, @session, 9, ^cold_id}

    append!(cold, 9, "next", 11)
    :ok = WalWriter.fsync(cold)
    assert_receive {:credit_drained, @session, 21, ^cold_id}
    assert seqs(cold_id) == [8, 9]
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

    log_text = capture_log(fn -> :ok = WalWriter.fsync(pid) end)
    assert log_text =~ "the disk refused a write or a sync (:einval)"
    assert log_text =~ "records from seq 3 on are dropped"

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
           end) =~ "records from seq 3 on are dropped"

    assert_received {:credit_released, @session, 5, ^cell_id}
    assert WalWriter.last_persisted_seq(pid) == 2
    assert seqs(cell_id) == [0, 1, 2]
  end
end
