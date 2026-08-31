defmodule FlezhaTest.State.ProcJournalTest do
  @moduledoc """
  `workflow-proc-journal` controller store: entry codec round-trip, torn-tail
  recovery, oversize rejection, reserved-type tolerance, lifecycle (path
  helper, invisibility to the Entity scanners, delete), and the resume state
  the bind ships back on `AddOperative.proc_journal`.
  """
  # async: false: writes Application env; a shared on-disk root.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{Commands, Outbox, Paths, ProcJournal, TestHelpers, WalWriter, Wire}

  import Flezha.State.TestHelpers,
    only: [next_output: 0, refute_dispatched: 0, refute_dispatched: 1]

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  defp instance_id(), do: "opj_inst_#{System.unique_integer([:positive])}"

  defp seg_identity(rc, sh, pk), do: Wire.encode_entity_id(rc, sh, pk)

  defp close_journal_writer(inst) do
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(Paths.journal_id(inst))
    WalWriter.close(pid)
  end

  test "journal path lives with the instance's state files" do
    inst = instance_id()

    assert Paths.journal_path(inst) ==
             Path.join([Paths.state_root(), "_system", inst, inst, "proc.journal"])

    assert Paths.wal_path(Paths.journal_id(inst)) == Paths.journal_path(inst)
  end

  test "append/read round-trip with entities and handoff" do
    inst = instance_id()
    seg_a = seg_identity(inst, "ProcA", "s0")
    handoff = [{2, <<1, 2, 3>>}, {2, <<>>}]

    assert :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg_a], handoff)
    assert :ok = ProcJournal.append_proc_committed(inst, 1, 0, [], [{3, "next"}])

    assert {:ok, [first, second]} = ProcJournal.read(inst)

    assert %{seq: 0, type: :proc_committed, proc_idx: 0, iter: 0} = first
    assert first.payload.entities_ran == [seg_a]
    assert first.payload.handoff == handoff

    assert %{seq: 1, type: :proc_committed, proc_idx: 1} = second
    assert second.payload.handoff == [{3, "next"}]

    assert File.exists?(Paths.journal_path(inst))
  end

  test "restore_state folds committed indexes and every in-flight proc's own handoff" do
    inst = instance_id()

    assert ProcJournal.restore_state(inst) == nil

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = ProcJournal.append_proc_committed(inst, 1, 0, [], [{2, "b"}])

    # Each committed procedure's handoff travels under its own proc_idx, so a
    # branching tree re-seeds every uncommitted child from ITS parent's edge and
    # never from a sibling's (`workflow-branching`). The iteration-level handoff
    # is the previous iteration's terminal batch — none here.
    assert %{
             committed_proc_idxs: [0, 1],
             iter: 0,
             handoff: [],
             proc_handoffs: [
               %{proc_idx: 0, handoff: [{1, "a"}]},
               %{proc_idx: 1, handoff: [{2, "b"}]}
             ]
           } = ProcJournal.restore_state(inst)
  end

  test "complete? derives DONE from the declared leaf set" do
    inst = instance_id()

    # A→(B,C): leaves are procs 1 and 2.
    assert ProcJournal.complete?(inst, [1, 2]) == false, "an empty journal is not complete"
    assert ProcJournal.complete?(inst, nil) == nil, "a class declaring no leaves is not derivable"

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = ProcJournal.append_proc_committed(inst, 1, 0, [], [])
    refute ProcJournal.complete?(inst, [1, 2]), "one leaf short"

    :ok = ProcJournal.append_proc_committed(inst, 2, 0, [], [])
    assert ProcJournal.complete?(inst, [1, 2]), "every leaf of the in-flight iteration committed"

    # A new iteration starts uncommitted: the leaves must commit again.
    :ok = ProcJournal.append_iter_committed(inst, 0, 1, [])
    refute ProcJournal.complete?(inst, [1, 2])
  end

  test "torn tail is discarded; complete prior entries remain readable" do
    inst = instance_id()
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [{1, "kept"}])

    # Kill the live writer so the appended partial frame is observed by a
    # fresh open (mirrors a crash mid-append).
    close_journal_writer(inst)

    path = Paths.journal_path(inst)
    File.write!(path, <<7::64-big, 99::32-big, 1, 2, 3>>, [:append])

    assert {:ok, [entry]} = ProcJournal.read(inst)
    assert entry.payload.handoff == [{1, "kept"}]

    # A re-opened writer truncates the torn tail and appends contiguously.
    :ok = ProcJournal.append_proc_committed(inst, 1, 0, [], [])
    assert {:ok, entries} = ProcJournal.read(inst)
    assert Enum.map(entries, & &1.seq) == [0, 1]
  end

  test "oversized handoff is refused with a typed error" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = instance_id()
    max = ProcJournal.max_entry_bytes()
    fat = :binary.copy(<<0>>, max + 1)

    assert {:error, {:oversized_handoff, size, ^max}} =
             ProcJournal.append_proc_committed(inst, 0, 0, [], [{1, fat}])

    assert size > max
    refute File.exists?(Paths.journal_path(inst))
  end

  test "an entry shape this build cannot decode is skipped, not fatal" do
    inst = instance_id()
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [])

    close_journal_writer(inst)

    # An entry from a future build (unknown type 77) and a known type carrying a
    # shape this build does not understand: both tolerated, neither fatal.
    unknown = ProcJournal.encode_entry(77, 0, 0, "future")
    malformed = ProcJournal.encode_entry(4, 0, 5, "not-a-deadline")
    path = Paths.journal_path(inst)
    File.write!(path, TestHelpers.build_wal_record(1, <<0::128>>, unknown), [:append])
    File.write!(path, TestHelpers.build_wal_record(2, <<0::128>>, malformed), [:append])

    assert {:ok, [committed]} = ProcJournal.read(inst)
    assert committed.type == :proc_committed

    # Undecodable entries do not disturb the fold.
    assert %{committed_proc_idxs: [0]} = ProcJournal.restore_state(inst)
  end

  test "iteration commit records the consumed offset and the terminal handoff" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    assert ProcJournal.consumed_offset(inst) == 0

    # Iteration 0: two procs, then the boundary.
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg], [{1, "a"}])
    :ok = ProcJournal.append_proc_committed(inst, 1, 0, [], [])
    :ok = ProcJournal.append_iter_committed(inst, 0, 4, [{2, "carry"}])

    assert ProcJournal.consumed_offset(inst) == 4

    resume = ProcJournal.restore_state(inst)

    assert resume.iter == 1, "resume enters the iteration AFTER the last committed one"
    assert resume.committed_proc_idxs == [], "iteration 1 has committed no procedure yet"
    assert resume.handoff == [{2, "carry"}], "a fresh iteration seeds from the terminal handoff"
    assert resume.consumed_offset == 4
    refute resume.parked

    # Iteration 1 gets one proc in before the crash: resume skips it and seeds
    # from ITS handoff instead.
    :ok = ProcJournal.append_proc_committed(inst, 0, 1, [seg], [{1, "b"}])

    resume = ProcJournal.restore_state(inst)
    assert resume.iter == 1
    assert resume.committed_proc_idxs == [0]
    assert resume.proc_handoffs == [%{proc_idx: 0, handoff: [{1, "b"}]}]

    assert resume.handoff == [{2, "carry"}],
           "the iteration-level handoff stays the previous iteration's terminal batch"

    # Every forward commit is carried back with its iteration — the compensation
    # planner's per-iteration `committed` input.
    assert resume.committed_procs == [
             %{proc_idx: 0, iter: 0, entities_ran: [seg]},
             %{proc_idx: 1, iter: 0, entities_ran: []},
             %{proc_idx: 0, iter: 1, entities_ran: [seg]}
           ]
  end

  test "park record survives as the suspension signal until the loop runs again" do
    inst = instance_id()
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [])
    :ok = ProcJournal.append_iter_committed(inst, 0, 2, [])

    refute ProcJournal.restore_state(inst).parked

    :ok = ProcJournal.append_parked(inst, 0, 1_800_000_000_000)

    resume = ProcJournal.restore_state(inst)
    assert resume.parked, "the journal's last entry is the park"
    assert resume.wake_deadline_ms == 1_800_000_000_000
    assert resume.iter == 1, "a rehydrate still enters the iteration after the committed one"

    # A park with no deadline waits for input alone.
    :ok = ProcJournal.append_iter_committed(inst, 1, 5, [])
    :ok = ProcJournal.append_parked(inst, 1, nil)
    resume = ProcJournal.restore_state(inst)
    assert resume.parked
    assert resume.wake_deadline_ms == nil

    # Once the loop runs again, the instance is no longer parked: the park is
    # only the LAST entry for as long as nothing has happened since.
    :ok = ProcJournal.append_proc_committed(inst, 0, 2, [], [])
    refute ProcJournal.restore_state(inst).parked
  end

  test "the journal never registers a phantom Entity" do
    inst = instance_id()
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [{1, "x"}])

    # Scoped to this instance: the shared app's live runners may write
    # unrelated Entity state into the test root concurrently.
    refute Enum.any?(Paths.all_entities(), fn {rc, _, _} -> rc == inst end)
    assert Paths.scan_dir({inst, inst, ""}).wal_files == []
  end

  test "delete_entity removes the journal file" do
    inst = instance_id()
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [])
    assert File.exists?(Paths.journal_path(inst))

    {:ok, %{status: :ok}} = Flezha.State.Supervisor.delete_entity(Paths.journal_id(inst))
    refute File.exists?(Paths.journal_path(inst))
    assert ProcJournal.restore_state(inst) == nil
  end

  # ---------- transactional outbox (workflow-transactional-outbox) ----------

  defp outbox_item(seg, seq_in_proc, payload, channel \\ "out") do
    %{entity_id: seg, seq_in_proc: seq_in_proc, channel: channel, payload: payload}
  end

  # `flush_pending/2` delivers one entry's outbox through
  # `GenServer.cast(overwatch, {:io_out_batch, ..})`, so a plain test pid stands
  # in for the bound Overwatch — including its duty to settle the chain run,
  # which is what lets the `outbox_flushed` marker land.
  defp assert_flushed(payload, seq, epoch \\ "0") do
    assert {^payload, meta, _ack} = next_output()
    assert meta["flz.seq"] == seq
    assert meta["flz.epoch"] == epoch
    :ok = Outbox.settled(meta)
    meta
  end

  # The marker is appended by the journal writer once the delivery is confirmed,
  # off the flushing process — so the journal is read after it lands.
  defp await_markers(inst, count) do
    assert wait_until(fn -> length(markers(inst)) == count end, 2_000),
           "expected #{count} outbox_flushed marker(s), got #{length(markers(inst))}"

    markers(inst)
  end

  defp markers(inst) do
    {:ok, entries} = ProcJournal.read(inst)
    Enum.filter(entries, &(&1.type == :outbox_flushed))
  end

  test "a procedure's outbox is journaled inside its proc_committed entry" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    :ok =
      ProcJournal.append_proc_committed(inst, 0, 0, [seg], [{1, "h"}], [
        outbox_item(seg, 0, "first"),
        outbox_item(seg, 1, "second")
      ])

    assert {:ok, [entry]} = ProcJournal.read(inst)
    # One record carries progress AND output: they cannot tear apart.
    assert entry.payload.handoff == [{1, "h"}]

    assert [%{seq_in_proc: 0, payload: "first"}, %{seq_in_proc: 1, payload: "second"}] =
             entry.payload.outbox

    assert [^entry] = ProcJournal.pending_entries(inst)
  end

  test "flush delivers in journal order with identity stamps, then gates on outbox_flushed" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    :ok =
      ProcJournal.append_proc_committed(inst, 0, 0, [seg], [], [
        outbox_item(seg, 0, "a"),
        outbox_item(seg, 1, "b")
      ])

    :ok = ProcJournal.append_proc_committed(inst, 1, 0, [seg], [], [outbox_item(seg, 0, "c")])

    assert :ok = ProcJournal.flush_pending(inst, self())

    meta = assert_flushed("a", "0.0")
    assert meta["flz.entity"] == "#{inst}/ProcA/s0"
    assert meta["channel"] == "out"
    assert_flushed("b", "0.1")
    assert_flushed("c", "1.0")
    refute_dispatched()

    # Delivery is recorded per entry, naming which entry type it flushed.
    assert [
             %{type: :outbox_flushed, proc_idx: 0, payload: %{for_type: :proc_committed}},
             %{proc_idx: 1}
           ] = await_markers(inst, 2)

    # An already-flushed proc is never re-delivered — a restore re-drive is a no-op.
    assert ProcJournal.pending_entries(inst) == []
    assert :ok = ProcJournal.flush_pending(inst, self())
    refute_dispatched()
  end

  test "a crash between commit and flush re-drives the outbox with identical keys" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "once")])

    # The durable state a crash-before-flush leaves: the commit landed, the
    # flush marker did not. Capture the file at exactly that point.
    close_journal_writer(inst)
    path = Paths.journal_path(inst)
    committed_only = File.read!(path)

    assert :ok = ProcJournal.flush_pending(inst, self())
    first = assert_flushed("once", "0.0")
    await_markers(inst, 1)

    # Roll the journal back to the pre-flush bytes (the marker never landed)
    # and restore: the entry is owed again and re-drives with the SAME key.
    close_journal_writer(inst)
    File.write!(path, committed_only)

    assert [%{proc_idx: 0}] = ProcJournal.pending_entries(inst)
    assert :ok = ProcJournal.flush_pending(inst, self())
    second = assert_flushed("once", "0.0")

    assert first == second
    # The marker is journaled after the settlement, not with the cast.
    await_markers(inst, 1)
    assert ProcJournal.pending_entries(inst) == []
  end

  test "the epoch is 0 at first start and stable across respawns" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    assert ProcJournal.current_epoch(inst) == 0

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "x")])
    assert :ok = ProcJournal.flush_pending(inst, self())
    assert_flushed("x", "0.0", "0")
    await_markers(inst, 1)

    # A respawn (writer gone, journal on disk) keeps minting the same epoch, so
    # every re-driven output keys identically to its original delivery.
    close_journal_writer(inst)
    :ok = ProcJournal.append_proc_committed(inst, 1, 0, [seg], [], [outbox_item(seg, 0, "y")])
    assert :ok = ProcJournal.flush_pending(inst, self())
    assert_flushed("y", "1.0", "0")
    await_markers(inst, 2)

    assert ProcJournal.current_epoch(inst) == 0
    assert {:ok, entries} = ProcJournal.read(inst)
    assert Enum.all?(entries, &(&1.epoch == 0))
  end

  test "a compensation's undo output is journaled and keyed distinctly from the forward output" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "did")])
    assert :ok = ProcJournal.flush_pending(inst, self())
    forward = assert_flushed("did", "0.0")
    await_markers(inst, 1)

    # The undo of that very output: same procedure, same entity, same per-proc
    # sequence — only the comp marker keeps its key from colliding.
    :ok = ProcJournal.append_compensated(inst, 0, 0, [outbox_item(seg, 0, "undo")])

    assert [%{type: :compensated, proc_idx: 0}] = ProcJournal.pending_entries(inst)
    assert :ok = ProcJournal.flush_pending(inst, self())
    undo = assert_flushed("undo", "0.0.1")

    assert undo["flz.entity"] == forward["flz.entity"]
    assert undo["flz.seq"] != forward["flz.seq"]

    # The two flush markers are distinct: the forward outbox and the undo outbox
    # of the SAME procedure are gated independently.
    assert [%{payload: %{for_type: :proc_committed}}, %{payload: %{for_type: :compensated}}] =
             await_markers(inst, 2)

    assert ProcJournal.pending_entries(inst) == []
  end

  test "the frame-size guard covers handoff plus outbox combined" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")
    max = ProcJournal.max_entry_bytes()
    half = :binary.copy(<<0>>, div(max, 2) + 8)

    # Neither half exceeds the bound alone; together they do.
    assert {:error, {:oversized_handoff, size, ^max}} =
             ProcJournal.append_proc_committed(inst, 0, 0, [seg], [{1, half}], [
               outbox_item(seg, 0, half)
             ])

    assert size > max
    refute File.exists?(Paths.journal_path(inst))
  end

  # No Entity WAL is seeded: the journal is instance-scoped and rides the bind,
  # so it must assemble for a class that has no per-Entity state at all — the
  # case that ships no `RestoreInitState` and would otherwise resume at 0.
  test "the bind ships the journal state on AddOperative" do
    inst = instance_id()
    seg_id = seg_identity(inst, "ProcA", "s0")

    # Iteration 0 committed one procedure and finished; iteration 1 has one
    # procedure in before the crash. The resume must carry BOTH granularities.
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg_id], [{2, "h0"}])
    :ok = ProcJournal.append_iter_committed(inst, 0, 3, [{2, "carry"}])
    :ok = ProcJournal.append_proc_committed(inst, 0, 1, [seg_id], [{2, "h"}])

    journal = Commands.proc_journal_state(inst)

    assert %RunnerCommands.ProcJournalState{
             committed_proc_idxs: [0],
             iter: 1,
             # The previous iteration's terminal batch (a chained loop's seed) …
             handoff: [%RunnerCommands.ProcHandoffItem{channel_id: 2, payload: "carry"}],
             # … and what the in-flight iteration's committed proc released, keyed
             # by proc_idx, which is what re-seeds its children.
             proc_handoffs: [
               %RunnerCommands.ProcEdgeHandoff{
                 proc_idx: 0,
                 handoff: [%RunnerCommands.ProcHandoffItem{channel_id: 2, payload: "h"}]
               }
             ],
             consumed_offset: 3,
             parked: false,
             wake_deadline_ms: 0
           } = journal

    # Every forward proc commit, with its iteration — the driver's compensation
    # input. Shipping this empty is how a rollback silently undoes nothing.
    assert [
             %RunnerCommands.ProcCommittedProc{proc_idx: 0, iter: 0, entities_ran: [^seg_id]},
             %RunnerCommands.ProcCommittedProc{proc_idx: 0, iter: 1, entities_ran: [^seg_id]}
           ] = journal.committed_procs
  end

  test "an instance without a journal ships no journal state" do
    assert Commands.proc_journal_state(instance_id()) == nil
  end

  # ---------- workflow-loop-rewind ----------

  # Three committed iterations (0, 1, 2), each one proc that ran on `seg` and
  # staged one output — the shape every rewind test below rewinds out of.
  defp three_committed_iterations(inst, seg) do
    for iter <- 0..2 do
      :ok = ProcJournal.append_proc_committed(inst, 0, iter, [seg], [], [outbox_item(seg, iter)])
      :ok = ProcJournal.append_iter_committed(inst, iter, iter + 1, [{1, "carry#{iter}"}])
    end
  end

  defp outbox_item(seg, seq_in_proc),
    do: %{entity_id: seg, seq_in_proc: seq_in_proc, channel: "out", payload: "v#{seq_in_proc}"}

  test "a rewind bumps the epoch, and every later append inherits it" do
    inst = instance_id()
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [])
    :ok = ProcJournal.append_iter_committed(inst, 0, 1, [])
    assert ProcJournal.current_epoch(inst) == 0

    :ok = ProcJournal.append_rewound(inst, 0)
    assert ProcJournal.current_epoch(inst) == 1

    :ok = ProcJournal.append_proc_committed(inst, 0, 1, [], [])
    assert ProcJournal.current_epoch(inst) == 1

    :ok = ProcJournal.append_rewound(inst, 0)
    assert ProcJournal.current_epoch(inst) == 2
  end

  test "a rewind folds the resume point, handoff and consumed offset back" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")
    three_committed_iterations(inst, seg)

    assert %{iter: 3, consumed_offset: 3, handoff: [{1, "carry2"}]} =
             ProcJournal.restore_state(inst)

    :ok = ProcJournal.append_rewound(inst, 1)

    resume = ProcJournal.restore_state(inst)
    assert resume.iter == 2
    assert resume.consumed_offset == 2
    assert resume.handoff == [{1, "carry1"}]
    # Iteration 2's proc is no longer committed progress — it is outstanding undo.
    assert Enum.map(resume.committed_procs, & &1.iter) == [0, 1]
    assert [%{proc_idx: 0, iter: 2}] = resume.rewind_undo

    # The inbox floor moved back with it, which is what re-delivers the rewound
    # turn's input.
    assert ProcJournal.consumed_offset(inst) == 2
    assert ProcJournal.committed_iterations(inst) == [0, 1]
  end

  test "a rewound iteration's unflushed outbox is no longer owed" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")
    three_committed_iterations(inst, seg)

    assert Enum.map(ProcJournal.pending_entries(inst), & &1.iter) == [0, 1, 2]

    :ok = ProcJournal.append_rewound(inst, 0)

    assert Enum.map(ProcJournal.pending_entries(inst), & &1.iter) == [0]
  end

  test "rewind_undo drains as compensations land, and never double-plans" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")
    three_committed_iterations(inst, seg)
    :ok = ProcJournal.append_rewound(inst, 0)

    assert [%{iter: 1}, %{iter: 2}] = ProcJournal.restore_state(inst).rewind_undo

    # A crash after compensating iteration 2 only: iteration 1 is what remains.
    :ok = ProcJournal.append_compensated(inst, 0, 2, [])
    assert [%{proc_idx: 0, iter: 1}] = ProcJournal.restore_state(inst).rewind_undo

    :ok = ProcJournal.append_compensated(inst, 0, 1, [])
    assert ProcJournal.restore_state(inst).rewind_undo == []
  end

  test "a re-run of a rewound iteration folds normally, and can be rewound again" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")
    three_committed_iterations(inst, seg)
    :ok = ProcJournal.append_rewound(inst, 0)
    :ok = ProcJournal.append_compensated(inst, 0, 1, [])
    :ok = ProcJournal.append_compensated(inst, 0, 2, [])

    # Iteration 1 re-runs under the bumped epoch and commits.
    :ok = ProcJournal.append_proc_committed(inst, 0, 1, [seg], [], [outbox_item(seg, 9)])
    :ok = ProcJournal.append_iter_committed(inst, 1, 2, [{1, "redone"}])

    resume = ProcJournal.restore_state(inst)
    assert resume.iter == 2
    assert resume.handoff == [{1, "redone"}]
    assert resume.rewind_undo == []
    assert ProcJournal.committed_iterations(inst) == [0, 1]

    # Only the re-run's output is owed — the unwound run's is gone for good.
    assert [%{iter: 0}, %{iter: 1}] = ProcJournal.pending_entries(inst)

    # A second rewind unwinds the re-run, not the undone original: the original's
    # `compensated` entry is newer than its commit, the re-run's commit is newer
    # than that compensation.
    :ok = ProcJournal.append_rewound(inst, 0)
    assert [%{proc_idx: 0, iter: 1}] = ProcJournal.restore_state(inst).rewind_undo
    assert ProcJournal.current_epoch(inst) == 2
  end

  # ---------- workflow-durable-path-throughput: the cut ----------

  defp seqs(inst) do
    {:ok, entries} = ProcJournal.read(inst)
    Enum.map(entries, & &1.seq)
  end

  # The cut is the writer's to run, off the caller; every release rewrites at
  # once here so the file can be read.
  defp cuttable(inst) do
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))
    ClassRegistry.put_compensable_ops(inst, [])
  end

  defp cut_and_await(inst, expected) do
    :ok = ProcJournal.cut(inst)

    assert wait_until(fn -> seqs(inst) == expected end, 2_000),
           "journal holds #{inspect(seqs(inst))}"
  end

  # Two procs and the boundary per iteration, nothing staged: seqs 3i..3i+2.
  defp commit_iterations(inst, seg, range) do
    for iter <- range do
      :ok = ProcJournal.append_proc_committed(inst, 0, iter, [seg], [{1, "h#{iter}"}])
      :ok = ProcJournal.append_proc_committed(inst, 1, iter, [], [])
      :ok = ProcJournal.append_iter_committed(inst, iter, iter + 1, [{2, "carry#{iter}"}])
    end
  end

  test "a cut journal restores the state the full history gave" do
    inst = instance_id()
    cuttable(inst)
    seg = seg_identity(inst, "ProcA", "s0")

    commit_iterations(inst, seg, 0..2)
    # Iteration 3 has one proc in before the cut.
    :ok = ProcJournal.append_proc_committed(inst, 0, 3, [seg], [{1, "h3"}])
    assert seqs(inst) == Enum.to_list(0..9)
    full = ProcJournal.restore_state(inst)

    # Only the head — iteration 2's commit — and what followed it remain.
    cut_and_await(inst, [8, 9])
    cut = ProcJournal.restore_state(inst)
    assert Map.drop(cut, [:committed_procs]) == Map.drop(full, [:committed_procs])
    assert cut.committed_procs == Enum.filter(full.committed_procs, &(&1.iter > 2))

    assert %{iter: 3, consumed_offset: 3, handoff: [{2, "carry2"}], committed_proc_idxs: [0]} =
             cut

    assert ProcJournal.consumed_offset(inst) == 3
    assert ProcJournal.current_epoch(inst) == 0

    # Nothing below the head is left to cut.
    cut_and_await(inst, [8, 9])
  end

  test "a pending outbox entry holds the cut, and goes with its flush" do
    inst = instance_id()
    cuttable(inst)
    seg = seg_identity(inst, "ProcA", "s0")

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "owed")])
    :ok = ProcJournal.append_iter_committed(inst, 0, 1, [])
    :ok = ProcJournal.append_proc_committed(inst, 0, 1, [seg], [])
    :ok = ProcJournal.append_iter_committed(inst, 1, 2, [])

    # An owed outbox holds the cut at its entry.
    cut_and_await(inst, [0, 1, 2, 3])
    assert [%{seq: 0}] = ProcJournal.pending_entries(inst)

    assert :ok = ProcJournal.flush_pending(inst, self())
    assert_flushed("owed", "0.0")
    await_markers(inst, 1)

    # The head and the marker after it.
    cut_and_await(inst, [3, 4])
    assert ProcJournal.pending_entries(inst) == []
    assert %{iter: 2, consumed_offset: 2} = ProcJournal.restore_state(inst)
  end

  test "a rewind marker and its unwound commits survive the cut until they are compensated" do
    inst = instance_id()
    cuttable(inst)
    seg = seg_identity(inst, "ProcA", "s0")

    for iter <- 0..2 do
      :ok = ProcJournal.append_proc_committed(inst, 0, iter, [seg], [])
      :ok = ProcJournal.append_iter_committed(inst, iter, iter + 1, [{1, "carry#{iter}"}])
    end

    :ok = ProcJournal.append_rewound(inst, 0)

    # The head is iteration 0's commit; everything the rewind still has to undo
    # lies above it and stays.
    cut_and_await(inst, [1, 2, 3, 4, 5, 6])
    assert [%{iter: 1}, %{iter: 2}] = ProcJournal.restore_state(inst).rewind_undo
    assert ProcJournal.current_epoch(inst) == 1

    :ok = ProcJournal.append_compensated(inst, 0, 2, [])
    :ok = ProcJournal.append_compensated(inst, 0, 1, [])
    # Iteration 1 re-runs under the bumped epoch and commits: the new head.
    :ok = ProcJournal.append_proc_committed(inst, 0, 1, [seg], [])
    :ok = ProcJournal.append_iter_committed(inst, 1, 2, [{1, "redone"}])

    cut_and_await(inst, [10])

    assert %{
             iter: 2,
             consumed_offset: 2,
             handoff: [{1, "redone"}],
             rewind_undo: [],
             parked: false
           } =
             ProcJournal.restore_state(inst)

    assert ProcJournal.current_epoch(inst) == 1
    assert ProcJournal.committed_iterations(inst) == [1]
  end

  test "a class with compensable procedures, or one that declared nothing, keeps its history" do
    compensable = instance_id()
    ClassRegistry.put_compensable_ops(compensable, [0])
    undeclared = instance_id()

    for inst <- [compensable, undeclared] do
      commit_iterations(inst, seg_identity(inst, "ProcA", "s0"), 0..1)
      :ok = ProcJournal.cut(inst)
      assert seqs(inst) == Enum.to_list(0..5)
    end
  end

  test "the outbox_flushed marker waits for the sink, and a re-entered flush does not re-deliver" do
    inst = instance_id()
    seg = seg_identity(inst, "ProcA", "s0")

    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [seg], [], [outbox_item(seg, 0, "held")])

    assert :ok = ProcJournal.flush_pending(inst, self())

    # The hold is taken inside the chain, before the run settles — which is the
    # order the real dispatcher produces.
    assert {"held", meta, %{owner: owner}} = next_output()
    :ok = Outbox.hold(meta)
    :ok = Outbox.settled(meta)

    # Held by a delivery-guaranteed sink: dispatched is not delivered, so the
    # marker must not land and the entry is still owed. The reports are casts
    # to the owning writer; a call to it answers once they are folded.
    {:ok, writer} = Flezha.State.Supervisor.lookup(owner)
    _ = :sys.get_state(writer)
    assert markers(inst) == []
    assert [%{proc_idx: 0}] = ProcJournal.pending_entries(inst)

    # A flush re-entered while the entry is with the chain must not send it twice.
    assert :ok = ProcJournal.flush_pending(inst, self())
    refute_dispatched(100)

    :ok = Outbox.confirm(meta)
    assert [%{proc_idx: 0, payload: %{for_type: :proc_committed}}] = await_markers(inst, 1)
    assert ProcJournal.pending_entries(inst) == []
  end
end
