defmodule FlezhaTest.State.OpJournalTest do
  @moduledoc """
  `bounded-op-journal` controller store: entry codec round-trip, torn-tail
  recovery, oversize rejection, reserved-type tolerance, lifecycle (path
  helper, invisibility to the Segment scanners, delete), and the bind-time
  restore ship-back of the journal state on `RestoreInitState`.
  """
  use ExUnit.Case, async: false

  import Mimic

  alias Flezha.Executor.ControlChannel
  alias Flezha.Operative.Overwatch
  alias Flezha.State.{OpJournal, Registry, TestHelpers, WalWriter, Wire}

  setup :verify_on_exit!

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  defp instance_id(), do: "opj_inst_#{System.unique_integer([:positive])}"

  defp seg_identity(rc, sh, pk), do: Wire.encode_segment_id(rc, sh, pk)

  defp close_journal_writer(inst) do
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(Registry.journal_id(inst))
    WalWriter.close(pid)
  end

  test "journal path lives with the instance's state files" do
    inst = instance_id()

    assert Registry.journal_path(inst) ==
             Path.join([Registry.state_root(), inst, inst, "op.journal"])

    assert Registry.wal_path(Registry.journal_id(inst)) == Registry.journal_path(inst)
  end

  test "append/read round-trip with segments and handoff" do
    inst = instance_id()
    seg_a = seg_identity(inst, "OpA", "s0")
    handoff = [{2, <<1, 2, 3>>}, {2, <<>>}]

    assert :ok = OpJournal.append_op_committed(inst, 0, 0, [seg_a], handoff)
    assert :ok = OpJournal.append_op_committed(inst, 1, 0, [], [{3, "next"}])

    assert {:ok, [first, second]} = OpJournal.read(inst)

    assert %{seq: 0, type: :op_committed, op_idx: 0, iter: 0} = first
    assert first.payload.segments_ran == [seg_a]
    assert first.payload.handoff == handoff

    assert %{seq: 1, type: :op_committed, op_idx: 1} = second
    assert second.payload.handoff == [{3, "next"}]

    assert File.exists?(Registry.journal_path(inst))
  end

  test "restore_state folds committed indexes and the latest handoff" do
    inst = instance_id()

    assert OpJournal.restore_state(inst) == nil

    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = OpJournal.append_op_committed(inst, 1, 0, [], [{2, "b"}])

    assert %{committed_op_idxs: [0, 1], iter: 0, handoff: [{2, "b"}]} =
             OpJournal.restore_state(inst)
  end

  test "torn tail is discarded; complete prior entries remain readable" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [{1, "kept"}])

    # Kill the live writer so the appended partial frame is observed by a
    # fresh open (mirrors a crash mid-append).
    close_journal_writer(inst)

    path = Registry.journal_path(inst)
    File.write!(path, <<7::64-big, 99::32-big, 1, 2, 3>>, [:append])

    assert {:ok, [entry]} = OpJournal.read(inst)
    assert entry.payload.handoff == [{1, "kept"}]

    # A re-opened writer truncates the torn tail and appends contiguously.
    :ok = OpJournal.append_op_committed(inst, 1, 0, [], [])
    assert {:ok, entries} = OpJournal.read(inst)
    assert Enum.map(entries, & &1.seq) == [0, 1]
  end

  test "oversized handoff is refused with a typed error" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = instance_id()
    max = OpJournal.max_entry_bytes()
    fat = :binary.copy(<<0>>, max + 1)

    assert {:error, {:oversized_handoff, size, ^max}} =
             OpJournal.append_op_committed(inst, 0, 0, [], [{1, fat}])

    assert size > max
    refute File.exists?(Registry.journal_path(inst))
  end

  test "reserved entry types are surfaced with raw payloads; unknown types are skipped" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [])

    close_journal_writer(inst)

    # A follow-up change's entry (type 3 = iter_committed) plus one from a
    # future build this code does not know (type 77): tolerated, not fatal.
    reserved = OpJournal.encode_entry(3, 2, 5, "opaque")
    unknown = OpJournal.encode_entry(77, 0, 0, "future")
    path = Registry.journal_path(inst)
    File.write!(path, TestHelpers.build_wal_record(1, <<0::128>>, reserved), [:append])
    File.write!(path, TestHelpers.build_wal_record(2, <<0::128>>, unknown), [:append])

    assert {:ok, [committed, iter_entry]} = OpJournal.read(inst)
    assert committed.type == :op_committed
    assert %{type: :iter_committed, op_idx: 2, iter: 5, payload: "opaque"} = iter_entry

    # Reserved entries do not disturb the op_committed fold.
    assert %{committed_op_idxs: [0]} = OpJournal.restore_state(inst)
  end

  test "the journal never registers a phantom Segment" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [{1, "x"}])

    # Scoped to this instance: the shared app's live executors may write
    # unrelated Segment state into the test root concurrently.
    refute Enum.any?(Registry.all_segments(), fn {rc, _, _} -> rc == inst end)
    assert Registry.scan_dir({inst, inst, ""}).wal_files == []
  end

  test "delete_segment removes the journal file" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [])
    assert File.exists?(Registry.journal_path(inst))

    {:ok, %{status: :ok}} = Flezha.State.Supervisor.delete_segment(Registry.journal_id(inst))
    refute File.exists?(Registry.journal_path(inst))
    assert OpJournal.restore_state(inst) == nil
  end

  # ---------- transactional outbox (bounded-transactional-outbox) ----------

  defp outbox_item(seg, seq_in_op, payload, channel \\ "out") do
    %{segment_id: seg, seq_in_op: seq_in_op, channel: channel, payload: payload}
  end

  # `flush_pending/2` delivers through `GenServer.cast(overwatch, {:io_out, ..})`,
  # so a plain test pid stands in for the bound Overwatch.
  defp assert_flushed(payload, seq, epoch \\ "0") do
    assert_receive {:"$gen_cast", {:io_out, ^payload, meta}}
    assert meta["flz.seq"] == seq
    assert meta["flz.epoch"] == epoch
    meta
  end

  test "an operation's outbox is journaled inside its op_committed entry" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")

    :ok =
      OpJournal.append_op_committed(inst, 0, 0, [seg], [{1, "h"}], [
        outbox_item(seg, 0, "first"),
        outbox_item(seg, 1, "second")
      ])

    assert {:ok, [entry]} = OpJournal.read(inst)
    # One record carries progress AND output: they cannot tear apart.
    assert entry.payload.handoff == [{1, "h"}]

    assert [%{seq_in_op: 0, payload: "first"}, %{seq_in_op: 1, payload: "second"}] =
             entry.payload.outbox

    assert [^entry] = OpJournal.pending_entries(inst)
  end

  test "flush delivers in journal order with identity stamps, then gates on outbox_flushed" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")

    :ok =
      OpJournal.append_op_committed(inst, 0, 0, [seg], [], [
        outbox_item(seg, 0, "a"),
        outbox_item(seg, 1, "b")
      ])

    :ok = OpJournal.append_op_committed(inst, 1, 0, [seg], [], [outbox_item(seg, 0, "c")])

    assert :ok = OpJournal.flush_pending(inst, self())

    meta = assert_flushed("a", "0.0")
    assert meta["flz.segment"] == "#{inst}/OpA/s0"
    assert meta["channel"] == "out"
    assert_flushed("b", "0.1")
    assert_flushed("c", "1.0")
    refute_receive {:"$gen_cast", {:io_out, _, _}}

    # Delivery is recorded per entry, naming which entry type it flushed.
    assert {:ok, entries} = OpJournal.read(inst)

    assert [%{type: :outbox_flushed, op_idx: 0, payload: %{for_type: 1}}, %{op_idx: 1}] =
             Enum.filter(entries, &(&1.type == :outbox_flushed))

    # An already-flushed op is never re-delivered — a restore re-drive is a no-op.
    assert OpJournal.pending_entries(inst) == []
    assert :ok = OpJournal.flush_pending(inst, self())
    refute_receive {:"$gen_cast", {:io_out, _, _}}
  end

  test "a crash between commit and flush re-drives the outbox with identical keys" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")

    :ok = OpJournal.append_op_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "once")])

    # The durable state a crash-before-flush leaves: the commit landed, the
    # flush marker did not. Capture the file at exactly that point.
    close_journal_writer(inst)
    path = Registry.journal_path(inst)
    committed_only = File.read!(path)

    assert :ok = OpJournal.flush_pending(inst, self())
    first = assert_flushed("once", "0.0")

    # Roll the journal back to the pre-flush bytes (the marker never landed)
    # and restore: the entry is owed again and re-drives with the SAME key.
    close_journal_writer(inst)
    File.write!(path, committed_only)

    assert [%{op_idx: 0}] = OpJournal.pending_entries(inst)
    assert :ok = OpJournal.flush_pending(inst, self())
    second = assert_flushed("once", "0.0")

    assert first == second
    assert OpJournal.pending_entries(inst) == []
  end

  test "the epoch is 0 at first start and stable across respawns" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")

    assert OpJournal.current_epoch(inst) == 0

    :ok = OpJournal.append_op_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "x")])
    assert :ok = OpJournal.flush_pending(inst, self())
    assert_flushed("x", "0.0", "0")

    # A respawn (writer gone, journal on disk) keeps minting the same epoch, so
    # every re-driven output keys identically to its original delivery.
    close_journal_writer(inst)
    :ok = OpJournal.append_op_committed(inst, 1, 0, [seg], [], [outbox_item(seg, 0, "y")])
    assert :ok = OpJournal.flush_pending(inst, self())
    assert_flushed("y", "1.0", "0")

    assert OpJournal.current_epoch(inst) == 0
    assert {:ok, entries} = OpJournal.read(inst)
    assert Enum.all?(entries, &(&1.epoch == 0))
  end

  test "a compensation's undo output is journaled and keyed distinctly from the forward output" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")

    :ok = OpJournal.append_op_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "did")])
    assert :ok = OpJournal.flush_pending(inst, self())
    forward = assert_flushed("did", "0.0")

    # The undo of that very output: same operation, same segment, same per-op
    # sequence — only the comp marker keeps its key from colliding.
    :ok = OpJournal.append_compensated(inst, 0, 0, [outbox_item(seg, 0, "undo")])

    assert [%{type: :compensated, op_idx: 0}] = OpJournal.pending_entries(inst)
    assert :ok = OpJournal.flush_pending(inst, self())
    undo = assert_flushed("undo", "0.0.1")

    assert undo["flz.segment"] == forward["flz.segment"]
    assert undo["flz.seq"] != forward["flz.seq"]

    # The two flush markers are distinct: the forward outbox and the undo outbox
    # of the SAME operation are gated independently.
    assert {:ok, entries} = OpJournal.read(inst)

    assert [%{payload: %{for_type: 1}}, %{payload: %{for_type: 5}}] =
             Enum.filter(entries, &(&1.type == :outbox_flushed))

    assert OpJournal.pending_entries(inst) == []
  end

  test "the frame-size guard covers handoff plus outbox combined" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")
    max = OpJournal.max_entry_bytes()
    half = :binary.copy(<<0>>, div(max, 2) + 8)

    # Neither half exceeds the bound alone; together they do.
    assert {:error, {:oversized_handoff, size, ^max}} =
             OpJournal.append_op_committed(inst, 0, 0, [seg], [{1, half}], [
               outbox_item(seg, 0, half)
             ])

    assert size > max
    refute File.exists?(Registry.journal_path(inst))
  end

  test "bind-time restore ships the journal state on RestoreInitState" do
    inst = instance_id()
    seg = {inst, "OpA", "s0"}

    wal = Registry.wal_path(seg)
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, TestHelpers.build_wal_record(1, <<>>))

    :ok = OpJournal.append_op_committed(inst, 0, 0, [seg_identity(inst, "OpA", "s0")], [{2, "h"}])

    test_pid = self()

    expect(ControlChannel, :send_control, fn "exec-opj", command ->
      send(test_pid, {:captured, command})
      {:ok, make_ref()}
    end)

    Overwatch.dispatch_restore_init_state("exec-opj", seg)

    assert_receive {:captured, {:restore_init_state, cmd}}

    assert %ExecutorCommands.OpJournalState{
             committed_op_idxs: [0],
             iter: 0,
             handoff: [%ExecutorCommands.OpHandoffItem{channel_id: 2, payload: "h"}]
           } = cmd.op_journal
  end

  test "an instance without a journal ships no journal state" do
    inst = instance_id()
    seg = {inst, "OpA", "s0"}
    wal = Registry.wal_path(seg)
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, TestHelpers.build_wal_record(1, <<>>))

    test_pid = self()

    expect(ControlChannel, :send_control, fn "exec-nojournal", command ->
      send(test_pid, {:captured, command})
      {:ok, make_ref()}
    end)

    Overwatch.dispatch_restore_init_state("exec-nojournal", seg)

    assert_receive {:captured, {:restore_init_state, cmd}}
    assert cmd.op_journal == nil
  end
end
