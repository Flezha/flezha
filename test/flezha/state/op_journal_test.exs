defmodule FlezhaTest.State.OpJournalTest do
  @moduledoc """
  `bounded-op-journal` controller store: entry codec round-trip, torn-tail
  recovery, oversize rejection, reserved-type tolerance, lifecycle (path
  helper, invisibility to the Segment scanners, delete), and the resume state
  the bind ships back on `AddOperative.op_journal`.
  """
  use ExUnit.Case, async: false

  alias Flezha.Operative.Overwatch
  alias Flezha.State.{OpJournal, Registry, TestHelpers, WalWriter, Wire}

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

  test "restore_state folds committed indexes and every in-flight op's own handoff" do
    inst = instance_id()

    assert OpJournal.restore_state(inst) == nil

    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = OpJournal.append_op_committed(inst, 1, 0, [], [{2, "b"}])

    # Each committed operation's handoff travels under its own op_idx, so a
    # branching tree re-seeds every uncommitted child from ITS parent's edge and
    # never from a sibling's (`bounded-branching`). The iteration-level handoff
    # is the previous iteration's terminal batch — none here.
    assert %{
             committed_op_idxs: [0, 1],
             iter: 0,
             handoff: [],
             op_handoffs: [
               %{op_idx: 0, handoff: [{1, "a"}]},
               %{op_idx: 1, handoff: [{2, "b"}]}
             ]
           } = OpJournal.restore_state(inst)
  end

  test "complete? derives DONE from the declared leaf set" do
    inst = instance_id()

    # A→(B,C): leaves are ops 1 and 2.
    assert OpJournal.complete?(inst, [1, 2]) == false, "an empty journal is not complete"
    assert OpJournal.complete?(inst, nil) == nil, "a class declaring no leaves is not derivable"

    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = OpJournal.append_op_committed(inst, 1, 0, [], [])
    refute OpJournal.complete?(inst, [1, 2]), "one leaf short"

    :ok = OpJournal.append_op_committed(inst, 2, 0, [], [])
    assert OpJournal.complete?(inst, [1, 2]), "every leaf of the in-flight iteration committed"

    # A new iteration starts uncommitted: the leaves must commit again.
    :ok = OpJournal.append_iter_committed(inst, 0, 1, [])
    refute OpJournal.complete?(inst, [1, 2])
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

  test "an entry shape this build cannot decode is skipped, not fatal" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [])

    close_journal_writer(inst)

    # An entry from a future build (unknown type 77) and a known type carrying a
    # shape this build does not understand: both tolerated, neither fatal.
    unknown = OpJournal.encode_entry(77, 0, 0, "future")
    malformed = OpJournal.encode_entry(4, 0, 5, "not-a-deadline")
    path = Registry.journal_path(inst)
    File.write!(path, TestHelpers.build_wal_record(1, <<0::128>>, unknown), [:append])
    File.write!(path, TestHelpers.build_wal_record(2, <<0::128>>, malformed), [:append])

    assert {:ok, [committed]} = OpJournal.read(inst)
    assert committed.type == :op_committed

    # Undecodable entries do not disturb the fold.
    assert %{committed_op_idxs: [0]} = OpJournal.restore_state(inst)
  end

  test "iteration commit records the consumed offset and the terminal handoff" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")

    assert OpJournal.consumed_offset(inst) == 0

    # Iteration 0: two ops, then the boundary.
    :ok = OpJournal.append_op_committed(inst, 0, 0, [seg], [{1, "a"}])
    :ok = OpJournal.append_op_committed(inst, 1, 0, [], [])
    :ok = OpJournal.append_iter_committed(inst, 0, 4, [{2, "carry"}])

    assert OpJournal.consumed_offset(inst) == 4

    resume = OpJournal.restore_state(inst)

    assert resume.iter == 1, "resume enters the iteration AFTER the last committed one"
    assert resume.committed_op_idxs == [], "iteration 1 has committed no operation yet"
    assert resume.handoff == [{2, "carry"}], "a fresh iteration seeds from the terminal handoff"
    assert resume.consumed_offset == 4
    refute resume.parked

    # Iteration 1 gets one op in before the crash: resume skips it and seeds
    # from ITS handoff instead.
    :ok = OpJournal.append_op_committed(inst, 0, 1, [seg], [{1, "b"}])

    resume = OpJournal.restore_state(inst)
    assert resume.iter == 1
    assert resume.committed_op_idxs == [0]
    assert resume.op_handoffs == [%{op_idx: 0, handoff: [{1, "b"}]}]

    assert resume.handoff == [{2, "carry"}],
           "the iteration-level handoff stays the previous iteration's terminal batch"

    # Every forward commit is carried back with its iteration — the compensation
    # planner's per-iteration `committed` input.
    assert resume.committed_ops == [
             %{op_idx: 0, iter: 0, segments_ran: [seg]},
             %{op_idx: 1, iter: 0, segments_ran: []},
             %{op_idx: 0, iter: 1, segments_ran: [seg]}
           ]
  end

  test "park record survives as the suspension signal until the loop runs again" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [])
    :ok = OpJournal.append_iter_committed(inst, 0, 2, [])

    refute OpJournal.restore_state(inst).parked

    :ok = OpJournal.append_parked(inst, 0, 1_800_000_000_000)

    resume = OpJournal.restore_state(inst)
    assert resume.parked, "the journal's last entry is the park"
    assert resume.wake_deadline_ms == 1_800_000_000_000
    assert resume.iter == 1, "a rehydrate still enters the iteration after the committed one"

    # A park with no deadline waits for input alone.
    :ok = OpJournal.append_iter_committed(inst, 1, 5, [])
    :ok = OpJournal.append_parked(inst, 1, nil)
    resume = OpJournal.restore_state(inst)
    assert resume.parked
    assert resume.wake_deadline_ms == nil

    # Once the loop runs again, the instance is no longer parked: the park is
    # only the LAST entry for as long as nothing has happened since.
    :ok = OpJournal.append_op_committed(inst, 0, 2, [], [])
    refute OpJournal.restore_state(inst).parked
  end

  test "the journal never registers a phantom Segment" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [{1, "x"}])

    # Scoped to this instance: the shared app's live runners may write
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

  # No Segment WAL is seeded: the journal is instance-scoped and rides the bind,
  # so it must assemble for a class that has no per-Segment state at all — the
  # case that ships no `RestoreInitState` and would otherwise resume at 0.
  test "the bind ships the journal state on AddOperative" do
    inst = instance_id()
    seg_id = seg_identity(inst, "OpA", "s0")

    # Iteration 0 committed one operation and finished; iteration 1 has one
    # operation in before the crash. The resume must carry BOTH granularities.
    :ok = OpJournal.append_op_committed(inst, 0, 0, [seg_id], [{2, "h0"}])
    :ok = OpJournal.append_iter_committed(inst, 0, 3, [{2, "carry"}])
    :ok = OpJournal.append_op_committed(inst, 0, 1, [seg_id], [{2, "h"}])

    journal = Overwatch.op_journal_state(inst)

    assert %RunnerCommands.OpJournalState{
             committed_op_idxs: [0],
             iter: 1,
             # The previous iteration's terminal batch (a chained loop's seed) …
             handoff: [%RunnerCommands.OpHandoffItem{channel_id: 2, payload: "carry"}],
             # … and what the in-flight iteration's committed op released, keyed
             # by op_idx, which is what re-seeds its children.
             op_handoffs: [
               %RunnerCommands.OpEdgeHandoff{
                 op_idx: 0,
                 handoff: [%RunnerCommands.OpHandoffItem{channel_id: 2, payload: "h"}]
               }
             ],
             consumed_offset: 3,
             parked: false,
             wake_deadline_ms: 0
           } = journal

    # Every forward op commit, with its iteration — the driver's compensation
    # input. Shipping this empty is how a rollback silently undoes nothing.
    assert [
             %RunnerCommands.OpCommittedOp{op_idx: 0, iter: 0, segments_ran: [^seg_id]},
             %RunnerCommands.OpCommittedOp{op_idx: 0, iter: 1, segments_ran: [^seg_id]}
           ] = journal.committed_ops
  end

  test "an instance without a journal ships no journal state" do
    assert Overwatch.op_journal_state(instance_id()) == nil
  end

  # ---------- bounded-loop-rewind ----------

  # Three committed iterations (0, 1, 2), each one op that ran on `seg` and
  # staged one output — the shape every rewind test below rewinds out of.
  defp three_committed_iterations(inst, seg) do
    for iter <- 0..2 do
      :ok = OpJournal.append_op_committed(inst, 0, iter, [seg], [], [outbox_item(seg, iter)])
      :ok = OpJournal.append_iter_committed(inst, iter, iter + 1, [{1, "carry#{iter}"}])
    end
  end

  defp outbox_item(seg, seq_in_op),
    do: %{segment_id: seg, seq_in_op: seq_in_op, channel: "out", payload: "v#{seq_in_op}"}

  test "a rewind bumps the epoch, and every later append inherits it" do
    inst = instance_id()
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [])
    :ok = OpJournal.append_iter_committed(inst, 0, 1, [])
    assert OpJournal.current_epoch(inst) == 0

    :ok = OpJournal.append_rewound(inst, 0)
    assert OpJournal.current_epoch(inst) == 1

    :ok = OpJournal.append_op_committed(inst, 0, 1, [], [])
    assert OpJournal.current_epoch(inst) == 1

    :ok = OpJournal.append_rewound(inst, 0)
    assert OpJournal.current_epoch(inst) == 2
  end

  test "a rewind folds the resume point, handoff and consumed offset back" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")
    three_committed_iterations(inst, seg)

    assert %{iter: 3, consumed_offset: 3, handoff: [{1, "carry2"}]} =
             OpJournal.restore_state(inst)

    :ok = OpJournal.append_rewound(inst, 1)

    resume = OpJournal.restore_state(inst)
    assert resume.iter == 2
    assert resume.consumed_offset == 2
    assert resume.handoff == [{1, "carry1"}]
    # Iteration 2's op is no longer committed progress — it is outstanding undo.
    assert Enum.map(resume.committed_ops, & &1.iter) == [0, 1]
    assert [%{op_idx: 0, iter: 2}] = resume.rewind_undo

    # The inbox floor moved back with it, which is what re-delivers the rewound
    # turn's input.
    assert OpJournal.consumed_offset(inst) == 2
    assert OpJournal.committed_iterations(inst) == [0, 1]
  end

  test "a rewound iteration's unflushed outbox is no longer owed" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")
    three_committed_iterations(inst, seg)

    assert Enum.map(OpJournal.pending_entries(inst), & &1.iter) == [0, 1, 2]

    :ok = OpJournal.append_rewound(inst, 0)

    assert Enum.map(OpJournal.pending_entries(inst), & &1.iter) == [0]
  end

  test "rewind_undo drains as compensations land, and never double-plans" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")
    three_committed_iterations(inst, seg)
    :ok = OpJournal.append_rewound(inst, 0)

    assert [%{iter: 1}, %{iter: 2}] = OpJournal.restore_state(inst).rewind_undo

    # A crash after compensating iteration 2 only: iteration 1 is what remains.
    :ok = OpJournal.append_compensated(inst, 0, 2, [])
    assert [%{op_idx: 0, iter: 1}] = OpJournal.restore_state(inst).rewind_undo

    :ok = OpJournal.append_compensated(inst, 0, 1, [])
    assert OpJournal.restore_state(inst).rewind_undo == []
  end

  test "a re-run of a rewound iteration folds normally, and can be rewound again" do
    inst = instance_id()
    seg = seg_identity(inst, "OpA", "s0")
    three_committed_iterations(inst, seg)
    :ok = OpJournal.append_rewound(inst, 0)
    :ok = OpJournal.append_compensated(inst, 0, 1, [])
    :ok = OpJournal.append_compensated(inst, 0, 2, [])

    # Iteration 1 re-runs under the bumped epoch and commits.
    :ok = OpJournal.append_op_committed(inst, 0, 1, [seg], [], [outbox_item(seg, 9)])
    :ok = OpJournal.append_iter_committed(inst, 1, 2, [{1, "redone"}])

    resume = OpJournal.restore_state(inst)
    assert resume.iter == 2
    assert resume.handoff == [{1, "redone"}]
    assert resume.rewind_undo == []
    assert OpJournal.committed_iterations(inst) == [0, 1]

    # Only the re-run's output is owed — the unwound run's is gone for good.
    assert [%{iter: 0}, %{iter: 1}] = OpJournal.pending_entries(inst)

    # A second rewind unwinds the re-run, not the undone original: the original's
    # `compensated` entry is newer than its commit, the re-run's commit is newer
    # than that compensation.
    :ok = OpJournal.append_rewound(inst, 0)
    assert [%{op_idx: 0, iter: 1}] = OpJournal.restore_state(inst).rewind_undo
    assert OpJournal.current_epoch(inst) == 2
  end
end
