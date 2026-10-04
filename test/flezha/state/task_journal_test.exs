defmodule FlezhaTest.State.TaskJournalTest do
  @moduledoc """
  `steps-task-journal` controller store: entry codec round-trip, torn-tail
  recovery, oversize rejection, reserved-type tolerance, lifecycle (path
  helper, invisibility to the Cell scanners, delete), and the resume state
  the bind ships back on `AddOperator.task_journal`.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operator.ClassRegistry
  alias Flezha.SDK.Wire
  alias Flezha.Service.Cli
  alias Flezha.State.{Commands, Outbox, Paths, TaskJournal, TestHelpers, WalWriter}
  alias Flezha.State.TaskJournal.Codec

  defp instance_id(), do: "opj_inst_#{System.unique_integer([:positive])}"

  defp seg_identity(rc, sh, pk), do: Wire.encode_cell_id(rc, sh, pk)

  # A Cell a stage ran as its barrier names it: the records it wrote there
  # (`{first, last}`) and the plugin channels they wrote on.
  defp ran(id, records \\ nil, channels \\ [])
  defp ran(id, nil, channels), do: %{cell_id: id, first_seq: nil, seq: nil, channels: channels}

  defp ran(id, {first, last}, channels),
    do: %{cell_id: id, first_seq: first, seq: last, channels: channels}

  defp close_journal_writer(inst) do
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(Paths.journal_id(inst))
    WalWriter.close(pid)
  end

  test "journal path lives with the instance's state files" do
    inst = instance_id()

    assert Paths.journal_path(inst) ==
             Path.join([Paths.state_root(), "_system", inst, inst, "task.journal"])

    assert Paths.wal_path(Paths.journal_id(inst)) == Paths.journal_path(inst)
  end

  test "append/read round-trip with cells and handoff" do
    inst = instance_id()
    seg_a = seg_identity(inst, "TaskA", "s0")
    handoff = [{2, <<1, 2, 3>>}, {2, <<>>}]

    assert :ok = TaskJournal.append_task_committed(inst, 0, 0, [ran(seg_a)], handoff)
    assert :ok = TaskJournal.append_task_committed(inst, 1, 0, [], [{3, "next"}])

    assert {:ok, [first, second]} = TaskJournal.read(inst)

    assert %{seq: 0, type: :task_committed, task_idx: 0, iter: 0} = first
    assert first.payload.cells_ran == [ran(seg_a)]
    assert first.payload.handoff == handoff

    assert %{seq: 1, type: :task_committed, task_idx: 1} = second
    assert second.payload.handoff == [{3, "next"}]

    assert File.exists?(Paths.journal_path(inst))
  end

  test "restore_state folds committed indexes and every in-flight task's own handoff" do
    inst = instance_id()

    assert TaskJournal.restore_state(inst) == nil

    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = TaskJournal.append_task_committed(inst, 1, 0, [], [{2, "b"}])

    # Each committed tasklet's handoff travels under its own task_idx, so a
    # branching tree re-seeds every uncommitted child from ITS parent's edge and
    # never from a sibling's (`steps-branching`). The iteration-level handoff
    # is the previous iteration's terminal batch — none here.
    assert %{
             committed_task_idxs: [0, 1],
             iter: 0,
             handoff: [],
             task_handoffs: [
               %{task_idx: 0, handoff: [{1, "a"}]},
               %{task_idx: 1, handoff: [{2, "b"}]}
             ]
           } = TaskJournal.restore_state(inst)
  end

  test "complete? derives DONE from the declared leaf set" do
    inst = instance_id()

    # A→(B,C): leaves are tasks 1 and 2.
    assert TaskJournal.complete?(inst, [1, 2]) == false, "an empty journal is not complete"
    assert TaskJournal.complete?(inst, nil) == nil, "a class declaring no leaves is not derivable"

    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [{1, "a"}])
    :ok = TaskJournal.append_task_committed(inst, 1, 0, [], [])
    refute TaskJournal.complete?(inst, [1, 2]), "one leaf short"

    :ok = TaskJournal.append_task_committed(inst, 2, 0, [], [])
    assert TaskJournal.complete?(inst, [1, 2]), "every leaf of the in-flight iteration committed"

    # A new iteration starts uncommitted: the leaves must commit again.
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [])
    refute TaskJournal.complete?(inst, [1, 2])
  end

  test "torn tail is discarded; complete prior entries remain readable" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [{1, "kept"}])

    # Kill the live writer so the appended partial frame is observed by a
    # fresh open (mirrors a crash mid-append).
    close_journal_writer(inst)

    path = Paths.journal_path(inst)
    File.write!(path, <<7::64-big, 99::32-big, 1, 2, 3>>, [:append])

    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert entry.payload.handoff == [{1, "kept"}]

    # A re-opened writer truncates the torn tail and appends contiguously.
    :ok = TaskJournal.append_task_committed(inst, 1, 0, [], [])
    assert {:ok, entries} = TaskJournal.read(inst)
    assert Enum.map(entries, & &1.seq) == [0, 1]
  end

  test "oversized handoff is refused with a typed error" do
    on_exit(TestHelpers.override_env(:wal_high_watermark_bytes, 1024))

    inst = instance_id()
    max = TaskJournal.max_entry_bytes()
    fat = :binary.copy(<<0>>, max + 1)

    assert {:error, {:oversized_handoff, size, ^max}} =
             TaskJournal.append_task_committed(inst, 0, 0, [], [{1, fat}])

    assert size > max
    refute File.exists?(Paths.journal_path(inst))
  end

  test "an entry shape this build cannot decode is skipped, not fatal" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])

    close_journal_writer(inst)

    # An entry from a future build (unknown type 77) and a known type carrying a
    # shape this build does not understand: both tolerated, neither fatal.
    unknown = Codec.encode_entry(77, 0, 0, "future")
    malformed = Codec.encode_entry(4, 0, 5, "not-a-deadline")
    path = Paths.journal_path(inst)
    File.write!(path, TestHelpers.build_wal_record(1, <<0::128>>, unknown), [:append])
    File.write!(path, TestHelpers.build_wal_record(2, <<0::128>>, malformed), [:append])

    assert {:ok, [committed]} = TaskJournal.read(inst)
    assert committed.type == :task_committed

    # Undecodable entries do not disturb the fold.
    assert %{committed_task_idxs: [0]} = TaskJournal.restore_state(inst)
  end

  test "iteration commit records the consumed offset and the terminal handoff" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")

    assert TaskJournal.consumed_offset(inst) == nil

    # Iteration 0: two tasks, then the boundary.
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [ran(seg)], [{1, "a"}])
    :ok = TaskJournal.append_task_committed(inst, 1, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 4, [{2, "carry"}])

    assert TaskJournal.consumed_offset(inst) == 4

    resume = TaskJournal.restore_state(inst)

    assert resume.iter == 1, "resume enters the iteration AFTER the last committed one"
    assert resume.committed_task_idxs == [], "iteration 1 has committed no tasklet yet"
    assert resume.handoff == [{2, "carry"}], "a fresh iteration seeds from the terminal handoff"
    assert resume.consumed_offset == 4
    refute Map.has_key?(resume, :parked), "where an instance sleeps is not the journal's"

    # Iteration 1 gets one task in before the crash: resume skips it and seeds
    # from ITS handoff instead.
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [ran(seg)], [{1, "b"}])

    resume = TaskJournal.restore_state(inst)
    assert resume.iter == 1
    assert resume.committed_task_idxs == [0]
    assert resume.task_handoffs == [%{task_idx: 0, handoff: [{1, "b"}]}]

    assert resume.handoff == [{2, "carry"}],
           "the iteration-level handoff stays the previous iteration's terminal batch"

    # Every forward commit is carried back with its iteration — the compensation
    # planner's per-iteration `committed` input.
    assert resume.committed_tasks == [
             %{task_idx: 0, iter: 0, cells_ran: [seg], completion: nil, trigger: nil},
             %{task_idx: 1, iter: 0, cells_ran: [], completion: nil, trigger: nil},
             %{task_idx: 0, iter: 1, cells_ran: [seg], completion: nil, trigger: nil}
           ]
  end

  # ---------- a tasklet's completion ----------

  test "a completion rides both commit entries, and an entry without one reads as not completing" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [{1, "h"}], "3")
    :ok = TaskJournal.append_iter_committed(inst, 0, 2, [], "3")
    :ok = TaskJournal.append_task_committed(inst, 1, 1, [], [])

    close_journal_writer(inst)

    # Written the way a build without completions wrote them: no trailing segment.
    path = Paths.journal_path(inst)

    old_task = Codec.encode_entry(1, 2, 1, [Codec.encode_cells([]), Codec.encode_handoff([])])

    old_iter =
      Codec.encode_entry(3, 0, 1, [
        Codec.encode_offset(5),
        <<0::16-big>>,
        Codec.encode_handoff([])
      ])

    File.write!(path, TestHelpers.build_wal_record(3, <<0::128>>, old_task), [:append])
    File.write!(path, TestHelpers.build_wal_record(4, <<0::128>>, old_iter), [:append])

    assert {:ok, [task, iter, plain, old_p, old_i]} = TaskJournal.read(inst)
    assert task.payload.completion == "3"
    assert task.payload.handoff == [{1, "h"}]
    assert iter.payload.completion == "3"
    assert iter.payload.consumed_offset == 2
    assert plain.payload.completion == nil
    assert {old_p.payload.completion, old_i.payload.completion} == {nil, nil}
    assert old_i.payload.consumed_offset == 5
  end

  test "the root of a timer-fed pass records the firing that opened it, and no other entry does" do
    inst = instance_id()
    trigger = %{cell_key: "a", kind: :processing_time, scheduled_at: 31_000}
    :ok = TaskJournal.append_task_committed(inst, 0, 4, [ran("tick")], [{1, "x"}], nil, trigger)
    :ok = TaskJournal.append_task_committed(inst, 1, 4, [ran("relay")], [], "done")

    assert {:ok, [root, child]} = TaskJournal.read(inst)
    assert root.payload.trigger == trigger
    assert root.payload.completion == nil
    assert {child.payload.trigger, child.payload.completion} == {nil, "done"}

    assert [%{task_idx: 0, trigger: ^trigger}, %{task_idx: 1, trigger: nil}] =
             TaskJournal.restore_state(inst).committed_tasks

    event = %{trigger | kind: :event_time, cell_key: ""}
    :ok = TaskJournal.append_task_committed(inst, 0, 5, [], [], "r", event)
    assert {:ok, [_, _, last]} = TaskJournal.read(inst)
    assert {last.payload.trigger, last.payload.completion} == {event, "r"}
  end

  test "offset 0 is an offset, and a pass that consumed nothing records none" do
    inst = instance_id()
    assert TaskJournal.released_offset(inst) == nil

    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = TaskJournal.append_iter_committed(inst, 1, nil, [{1, "carry"}])

    assert {:ok, [first, second]} = TaskJournal.read(inst)
    assert {first.payload.consumed_offset, second.payload.consumed_offset} == {0, nil}
    assert TaskJournal.consumed_offset(inst) == nil
    assert TaskJournal.released_offset(inst) == 0, "a carry-fed pass lowers no floor"

    assert %RunnerCommands.TaskJournalState{iter: 2, consumed_offset: nil} =
             Commands.task_journal_state(inst)

    rendered =
      ExUnit.CaptureIO.capture_io(fn -> Cli.call("Steps.journal", Jason.encode!([inst]), []) end)

    assert rendered =~ "consumed input-log offset: 0"
    assert rendered =~ "consumed no journaled input"
  end

  test "the journal reports what it released into the input log's watermark" do
    inst = instance_id()
    :ok = TaskJournal.release_input(inst)
    assert Flezha.State.InputLog.watermark(inst) == nil

    :ok = TaskJournal.append_iter_committed(inst, 0, 4, [])
    :ok = TaskJournal.release_input(inst)
    assert Flezha.State.InputLog.watermark(inst) == 4

    # Only forward: a stale or empty report leaves the floor where it is.
    :ok = TaskJournal.release_input(inst, 2)
    :ok = TaskJournal.release_input(inst, nil)
    assert Flezha.State.InputLog.watermark(inst) == 4
  end

  # An input dead-lettered unread before the instance slept is released in
  # the journal, so the release outlives a restart before the next commit.
  test "an input dead-lettered unread is released in the journal" do
    inst = instance_id()
    :ok = TaskJournal.release_unread(inst, nil)
    assert TaskJournal.read(inst) == {:ok, []}

    :ok = TaskJournal.release_unread(inst, 0)

    assert {:ok, [%{type: :input_released, payload: %{consumed_offset: 0}}]} =
             TaskJournal.read(inst)

    assert TaskJournal.released_offset(inst) == 0
    assert TaskJournal.consumed_offset(inst) == nil, "no iteration consumed it"
    assert Flezha.State.InputLog.watermark(inst) == 0

    :ets.match_delete(Flezha.State.InputLog.watermark_table(), {{inst, :_}, :_})
    :ok = TaskJournal.release_input(inst)
    assert Flezha.State.InputLog.watermark(inst) == 0, "the release survives the lost row"

    assert %RunnerCommands.TaskJournalState{released_offset: 0, consumed_offset: nil} =
             Commands.task_journal_state(inst)

    rendered =
      ExUnit.CaptureIO.capture_io(fn -> Cli.call("Steps.journal", Jason.encode!([inst]), []) end)

    assert rendered =~ "input_released"
    assert rendered =~ "released unread input-log offset: 0"
  end

  test "a completion carrying an empty result still completes" do
    inst = instance_id()
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [], "")

    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert entry.payload.completion == ""
    assert %{completion: ""} = TaskJournal.restore_state(inst)
  end

  test "the restore state ships the latest iteration's completion and each tasklet's own" do
    inst = instance_id()

    # Iteration 0 ends without a completion: the resume waits for input.
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [])
    assert %{completion: nil} = TaskJournal.restore_state(inst)

    # Iteration 1's completing tasklet commits and the crash lands before its
    # iteration does: the in-flight commit carries what it asked for.
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [], [{1, "bye"}], "2")
    resume = TaskJournal.restore_state(inst)
    assert %{iter: 1, committed_task_idxs: [0], completion: nil} = resume

    assert List.last(resume.committed_tasks) == %{
             task_idx: 0,
             iter: 1,
             cells_ran: [],
             completion: "2",
             trigger: nil
           }

    journal = Commands.task_journal_state(inst)

    assert [
             %RunnerCommands.TaskCommittedTask{iter: 0, completes: false, result: ""},
             %RunnerCommands.TaskCommittedTask{iter: 1, completes: true, result: "2"}
           ] = journal.committed_tasks

    # The iteration commits with it: the instance has ended.
    :ok = TaskJournal.append_iter_committed(inst, 1, 2, [], "2")
    assert %{iter: 2, completion: "2"} = TaskJournal.restore_state(inst)

    assert %RunnerCommands.TaskJournalState{completed: true, result: "2"} =
             Commands.task_journal_state(inst)
  end

  test "flz state journal names a completing entry and the size of its result" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [], "done")
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [], "done")

    text =
      ExUnit.CaptureIO.capture_io(fn -> Cli.call("Steps.journal", Jason.encode!([inst]), []) end)

    assert length(String.split(text, "completes the instance: result 4 byte(s)")) == 3

    json =
      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Steps.journal", Jason.encode!([inst]), output: "json")
      end)

    assert %{"entries" => [task, iter]} = Jason.decode!(json)
    assert {task["completes_bytes"], iter["completes_bytes"]} == {4, 4}
  end

  test "the journal never registers a phantom Cell" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [{1, "x"}])

    # Scoped to this instance: the shared app's live runners may write
    # unrelated Cell state into the test root concurrently.
    refute Enum.any?(Paths.all_cells(), fn {rc, _, _} -> rc == inst end)
    assert Paths.scan_dir({inst, inst, ""}).wal_files == []
  end

  test "delete_cell removes the journal file" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    assert File.exists?(Paths.journal_path(inst))

    {:ok, %{status: :ok}} = Flezha.State.Supervisor.delete_cell(Paths.journal_id(inst))
    refute File.exists?(Paths.journal_path(inst))
    assert TaskJournal.restore_state(inst) == nil
  end

  # ---------- the records a stage entry names ----------

  test "a stage entry names its Cells' records and channels, and carries no output" do
    inst = instance_id()
    seg_a = seg_identity(inst, "TaskA", "s0")
    seg_b = seg_identity(inst, "TaskB", "")

    :ok =
      TaskJournal.append_task_committed(inst, 0, 0, [ran(seg_a, {3, 4}, ["out"]), ran(seg_b)], [])

    assert {:ok, [entry]} = TaskJournal.read(inst)
    assert entry.payload.cells_ran == [ran(seg_a, {3, 4}, ["out"]), ran(seg_b)]
    refute Map.has_key?(entry.payload, :outbox)

    text =
      ExUnit.CaptureIO.capture_io(fn -> Cli.call("Steps.journal", Jason.encode!([inst]), []) end)

    assert text =~ "ran: #{inst}/TaskA/s0 records=3..4 channels=out"
    assert text =~ "ran: #{inst}/TaskB/\n"
    refute text =~ "outbox"
    refute text =~ "flushes"

    json =
      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Steps.journal", Jason.encode!([inst]), output: "json")
      end)

    assert %{"entries" => [%{"cells_ran" => [a, b]}]} = Jason.decode!(json)

    assert a == %{
             "cell" => "#{inst}/TaskA/s0",
             "first_seq" => 3,
             "seq" => 4,
             "channels" => ["out"]
           }

    assert %{"seq" => nil, "channels" => []} = b
  end

  test "the committed records are the live stage entries' ranges, per Cell" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")
    other = seg_identity(inst, "TaskB", "")

    assert TaskJournal.committed_records(inst) == %{}

    :ok = TaskJournal.append_task_committed(inst, 0, 0, [ran(seg, {0, 1}), ran(other)], [])
    :ok = TaskJournal.append_task_committed(inst, 1, 0, [ran(other, {0, 0})], [])
    :ok = TaskJournal.append_compensated(inst, 0, 0, [ran(seg, {3, 3}, ["undo"])])

    assert TaskJournal.committed_records(inst) == %{seg => [{0, 1}, {3, 3}], other => [{0, 0}]}
    assert TaskJournal.record_ranges([ran(seg, {0, 1}), ran(other)]) == [{seg, 0, 1}]
  end

  test "the rewind guard reads the channels the stage entries name" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")

    for iter <- 0..2 do
      channels = if iter == 0, do: [], else: ["out", "audit"]

      :ok =
        TaskJournal.append_task_committed(inst, 0, iter, [ran(seg, {iter, iter}, channels)], [])

      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [])
    end

    assert Enum.sort(TaskJournal.staged_channels_above(inst, 0)) == [{0, "audit"}, {0, "out"}]
    assert TaskJournal.staged_channels_above(inst, 2) == []
  end

  test "the epoch is 0 at first start and stable across respawns" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")

    assert TaskJournal.current_epoch(inst) == 0
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [ran(seg, {0, 0})], [])

    # A respawn (writer gone, journal on disk) keeps the same epoch.
    close_journal_writer(inst)
    :ok = TaskJournal.append_task_committed(inst, 1, 0, [ran(seg, {1, 1})], [])

    assert TaskJournal.current_epoch(inst) == 0
    assert {:ok, entries} = TaskJournal.read(inst)
    assert Enum.all?(entries, &(&1.epoch == 0))
  end

  # No Cell WAL is seeded: the journal is instance-scoped and rides the bind,
  # so it must assemble for a class that has no per-Cell state at all — the
  # case that ships no `RestoreInitState` and would otherwise resume at 0.
  test "the bind ships the journal state on AddOperator" do
    inst = instance_id()
    seg_id = seg_identity(inst, "TaskA", "s0")

    # Iteration 0 committed one tasklet and finished; iteration 1 has one
    # tasklet in before the crash. The resume must carry BOTH granularities.
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [ran(seg_id, {0, 0})], [{2, "h0"}])
    :ok = TaskJournal.append_iter_committed(inst, 0, 3, [{2, "carry"}])
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [ran(seg_id, {1, 1})], [{2, "h"}])

    journal = Commands.task_journal_state(inst)

    assert %RunnerCommands.TaskJournalState{
             committed_task_idxs: [0],
             iter: 1,
             # The previous iteration's terminal batch (a chained loop's seed) …
             handoff: [%RunnerCommands.TaskHandoffItem{channel_id: 2, payload: "carry"}],
             # … and what the in-flight iteration's committed task released, keyed
             # by task_idx, which is what re-seeds its children.
             task_handoffs: [
               %RunnerCommands.TaskEdgeHandoff{
                 task_idx: 0,
                 handoff: [%RunnerCommands.TaskHandoffItem{channel_id: 2, payload: "h"}]
               }
             ],
             consumed_offset: 3
           } = journal

    # Every forward task commit, with its iteration — the driver's compensation
    # input. Shipping this empty is how a rollback silently undoes nothing.
    assert [
             %RunnerCommands.TaskCommittedTask{task_idx: 0, iter: 0, cells_ran: [^seg_id]},
             %RunnerCommands.TaskCommittedTask{task_idx: 0, iter: 1, cells_ran: [^seg_id]}
           ] = journal.committed_tasks
  end

  test "an instance without a journal ships no journal state" do
    assert Commands.task_journal_state(instance_id()) == nil
  end

  # ---------- steps-loop-rewind ----------

  # Three committed iterations (0, 1, 2), each one task that ran on `seg` and
  # wrote one record, seq `iter`, with an output on `out`: the shape every
  # rewind test below rewinds out of.
  defp three_committed_iterations(inst, seg) do
    for iter <- 0..2 do
      :ok =
        TaskJournal.append_task_committed(inst, 0, iter, [ran(seg, {iter, iter}, ["out"])], [])

      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [{1, "carry#{iter}"}])
    end
  end

  test "a rewind bumps the epoch, and every later append inherits it" do
    inst = instance_id()
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [])
    assert TaskJournal.current_epoch(inst) == 0

    :ok = TaskJournal.append_rewound(inst, 0)
    assert TaskJournal.current_epoch(inst) == 1

    :ok = TaskJournal.append_task_committed(inst, 0, 1, [], [])
    assert TaskJournal.current_epoch(inst) == 1

    :ok = TaskJournal.append_rewound(inst, 0)
    assert TaskJournal.current_epoch(inst) == 2
  end

  test "a rewind folds the resume point, handoff and consumed offset back" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")
    three_committed_iterations(inst, seg)

    assert %{iter: 3, consumed_offset: 3, handoff: [{1, "carry2"}]} =
             TaskJournal.restore_state(inst)

    :ok = TaskJournal.append_rewound(inst, 1)

    resume = TaskJournal.restore_state(inst)
    assert resume.iter == 2
    assert resume.consumed_offset == 2
    assert resume.handoff == [{1, "carry1"}]
    # Iteration 2's task counts as outstanding undo, not committed progress.
    assert Enum.map(resume.committed_tasks, & &1.iter) == [0, 1]
    assert [%{task_idx: 0, iter: 2}] = resume.rewind_undo

    # The fold moves back; the floor in the input log does not, so the rewound
    # turn's input is not delivered again.
    assert TaskJournal.consumed_offset(inst) == 2
    assert TaskJournal.released_offset(inst) == 3
    assert TaskJournal.committed_iterations(inst) == [0, 1]
  end

  test "a rewound iteration's records are no longer committed" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")
    three_committed_iterations(inst, seg)

    assert TaskJournal.committed_records(inst) == %{seg => [{0, 0}, {1, 1}, {2, 2}]}

    :ok = TaskJournal.append_rewound(inst, 0)

    assert TaskJournal.committed_records(inst) == %{seg => [{0, 0}]}
  end

  test "rewind_undo drains as compensations land, and never double-plans" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")
    three_committed_iterations(inst, seg)
    :ok = TaskJournal.append_rewound(inst, 0)

    assert [%{iter: 1}, %{iter: 2}] = TaskJournal.restore_state(inst).rewind_undo

    # A crash after compensating iteration 2 only: iteration 1 is what remains.
    :ok = TaskJournal.append_compensated(inst, 0, 2, [])
    assert [%{task_idx: 0, iter: 1}] = TaskJournal.restore_state(inst).rewind_undo

    :ok = TaskJournal.append_compensated(inst, 0, 1, [])
    assert TaskJournal.restore_state(inst).rewind_undo == []
  end

  test "a re-run of a rewound iteration folds normally, and can be rewound again" do
    inst = instance_id()
    seg = seg_identity(inst, "TaskA", "s0")
    three_committed_iterations(inst, seg)
    :ok = TaskJournal.append_rewound(inst, 0)
    :ok = TaskJournal.append_compensated(inst, 0, 1, [])
    :ok = TaskJournal.append_compensated(inst, 0, 2, [])

    # Iteration 1 re-runs under the bumped epoch and commits.
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [ran(seg, {9, 9}, ["out"])], [])
    :ok = TaskJournal.append_iter_committed(inst, 1, 2, [{1, "redone"}])

    resume = TaskJournal.restore_state(inst)
    assert resume.iter == 2
    assert resume.handoff == [{1, "redone"}]
    assert resume.rewind_undo == []
    assert TaskJournal.committed_iterations(inst) == [0, 1]

    # Only the re-run's record is committed; the unwound run's is gone for good.
    assert TaskJournal.committed_records(inst) == %{seg => [{0, 0}, {9, 9}]}

    # A second rewind unwinds the re-run, not the undone original: the original's
    # `compensated` entry is newer than its commit, the re-run's commit is newer
    # than that compensation.
    :ok = TaskJournal.append_rewound(inst, 0)
    assert [%{task_idx: 0, iter: 1}] = TaskJournal.restore_state(inst).rewind_undo
    assert TaskJournal.current_epoch(inst) == 2
  end

  # ---------- steps-durable-path-throughput: the cut ----------

  defp seqs(inst) do
    {:ok, entries} = TaskJournal.read(inst)
    Enum.map(entries, & &1.seq)
  end

  # The cut is the writer's to run, off the caller; every release rewrites at
  # once here so the file can be read.
  defp cuttable(inst) do
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))
    ClassRegistry.put_compensable_ops(inst, [])
  end

  defp cut_and_await(inst, expected) do
    :ok = TaskJournal.cut(inst)

    assert wait_until(fn -> seqs(inst) == expected end, 2_000),
           "journal holds #{inspect(seqs(inst))}"
  end

  # Two tasks and the boundary per iteration, nothing staged: seqs 3i..3i+2.
  defp commit_iterations(inst, seg, range) do
    for iter <- range do
      :ok = TaskJournal.append_task_committed(inst, 0, iter, [ran(seg)], [{1, "h#{iter}"}])
      :ok = TaskJournal.append_task_committed(inst, 1, iter, [], [])
      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [{2, "carry#{iter}"}])
    end
  end

  test "a cut journal restores the state the full history gave" do
    inst = instance_id()
    cuttable(inst)
    seg = seg_identity(inst, "TaskA", "s0")

    commit_iterations(inst, seg, 0..2)
    # Iteration 3 has one task in before the cut.
    :ok = TaskJournal.append_task_committed(inst, 0, 3, [ran(seg)], [{1, "h3"}])
    assert seqs(inst) == Enum.to_list(0..9)
    full = TaskJournal.restore_state(inst)

    # Only the head — iteration 2's commit — and what followed it remain.
    cut_and_await(inst, [8, 9])
    cut = TaskJournal.restore_state(inst)
    assert Map.drop(cut, [:committed_tasks]) == Map.drop(full, [:committed_tasks])
    assert cut.committed_tasks == Enum.filter(full.committed_tasks, &(&1.iter > 2))

    assert %{iter: 3, consumed_offset: 3, handoff: [{2, "carry2"}], committed_task_idxs: [0]} =
             cut

    assert TaskJournal.consumed_offset(inst) == 3
    assert TaskJournal.current_epoch(inst) == 0

    # Nothing below the head is left to cut.
    cut_and_await(inst, [8, 9])
  end

  # The Cell's persisted flush cursor, as its writer would leave it once the
  # sinks confirmed everything up to `flushed`.
  defp confirm_through(cell, flushed) do
    File.mkdir_p!(Path.dirname(Paths.outbox_dedup_path(cell)))
    :ok = Outbox.write(cell, %{last_persisted_seq: 9, outbox_flushed_seq: flushed, epoch: 0})
  end

  test "an entry whose record is still owed holds the cut, and goes once the cursor passes it" do
    inst = instance_id()
    cuttable(inst)
    cell = {inst, "TaskA", "s0"}
    seg = seg_identity(inst, "TaskA", "s0")

    :ok = TaskJournal.append_task_committed(inst, 0, 0, [ran(seg, {0, 0}, ["out"])], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [])
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [ran(seg, {1, 1})], [])
    :ok = TaskJournal.append_iter_committed(inst, 1, 2, [])

    # Nothing confirmed: the entry naming record 0 holds the cut.
    cut_and_await(inst, [0, 1, 2, 3])
    assert TaskJournal.committed_records(inst) == %{seg => [{0, 0}, {1, 1}]}

    # Record 0 confirmed: the entry naming record 1 holds it next.
    confirm_through(cell, 0)
    cut_and_await(inst, [2, 3])

    confirm_through(cell, 1)
    cut_and_await(inst, [3])
    assert TaskJournal.committed_records(inst) == %{}
    assert %{iter: 2, consumed_offset: 2} = TaskJournal.restore_state(inst)
  end

  test "a rewind marker and its unwound commits survive the cut until they are compensated" do
    inst = instance_id()
    cuttable(inst)
    seg = seg_identity(inst, "TaskA", "s0")

    for iter <- 0..2 do
      :ok = TaskJournal.append_task_committed(inst, 0, iter, [ran(seg)], [])
      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [{1, "carry#{iter}"}])
    end

    :ok = TaskJournal.append_rewound(inst, 0)

    # The head is iteration 0's commit; everything the rewind still has to undo
    # lies above it and stays.
    cut_and_await(inst, [1, 2, 3, 4, 5, 6])
    assert [%{iter: 1}, %{iter: 2}] = TaskJournal.restore_state(inst).rewind_undo
    assert TaskJournal.current_epoch(inst) == 1

    :ok = TaskJournal.append_compensated(inst, 0, 2, [])
    :ok = TaskJournal.append_compensated(inst, 0, 1, [])
    # Iteration 1 re-runs under the bumped epoch and commits: the new head.
    :ok = TaskJournal.append_task_committed(inst, 0, 1, [ran(seg)], [])
    :ok = TaskJournal.append_iter_committed(inst, 1, 2, [{1, "redone"}])

    cut_and_await(inst, [10])

    assert %{
             iter: 2,
             consumed_offset: 2,
             handoff: [{1, "redone"}],
             rewind_undo: []
           } =
             TaskJournal.restore_state(inst)

    assert TaskJournal.current_epoch(inst) == 1
    assert TaskJournal.committed_iterations(inst) == [1]
  end

  test "a class with compensable tasklets, or one that declared nothing, keeps its history" do
    compensable = instance_id()
    ClassRegistry.put_compensable_ops(compensable, [0])
    undeclared = instance_id()

    for inst <- [compensable, undeclared] do
      commit_iterations(inst, seg_identity(inst, "TaskA", "s0"), 0..1)
      :ok = TaskJournal.cut(inst)
      assert seqs(inst) == Enum.to_list(0..5)
    end
  end

  test "a rescale's seal cuts a compensable class to its resume point at once, keeping what is owed" do
    inst = instance_id()
    ClassRegistry.put_compensable_ops(inst, [0])
    seg = seg_identity(inst, "TaskA", "s0")

    # seqs: iteration i's task at 2i, its commit at 2i+1; iteration i's record
    # is seq i, confirmed through 0, so iteration 1's is still owed; the rewind
    # to 1 leaves iteration 2's task to undo.
    confirm_through({inst, "TaskA", "s0"}, 0)

    for iter <- 0..2 do
      :ok = TaskJournal.append_task_committed(inst, 0, iter, [ran(seg, {iter, iter})], [])
      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [])
    end

    :ok = TaskJournal.append_rewound(inst, 1)

    :ok = TaskJournal.cut(inst)
    assert seqs(inst) == Enum.to_list(0..6)

    :ok = TaskJournal.cut(inst, seal: true)
    assert seqs(inst) == [2, 3, 4, 5, 6]
    assert TaskJournal.committed_records(inst) == %{seg => [{1, 1}]}
    assert [%{iter: 2}] = TaskJournal.restore_state(inst).rewind_undo
    assert TaskJournal.committed_iterations(inst) == [1]
    assert %{iter: 2, consumed_offset: 2} = TaskJournal.restore_state(inst)
  end

  describe "a dead-lettered turn" do
    # Iterations 0 and 1 commit; iteration 2's first tasklet commits, its turn
    # fails, and the turn is dead-lettered: seqs 0..4, then the record at 5.
    # `records?: false` names no effect record, so none holds a cut.
    defp dead_letter_iteration_2(inst, seg, records? \\ true) do
      cell = fn iter -> if records?, do: ran(seg, {iter, iter}), else: ran(seg) end

      for iter <- 0..1 do
        :ok = TaskJournal.append_task_committed(inst, 0, iter, [cell.(iter)], [])
        :ok = TaskJournal.append_iter_committed(inst, iter, iter, [])
      end

      :ok = TaskJournal.append_task_committed(inst, 0, 2, [cell.(2)], [{1, "noted"}])
      :ok = TaskJournal.append_dead_lettered(inst, 2, 2, "in", "answer", 3, "c/_turn/e2")
    end

    test "resumes after it with its committed tasklets owed undo" do
      inst = instance_id()
      seg = seg_identity(inst, "TaskA", "s0")
      dead_letter_iteration_2(inst, seg)

      resume = TaskJournal.restore_state(inst)
      assert %{iter: 3, consumed_offset: 2, entry_channel: "in", handoff: []} = resume
      assert resume.committed_task_idxs == []
      assert resume.completion == nil
      assert [%{task_idx: 0, iter: 2}] = resume.rewind_undo

      assert TaskJournal.consumed_offset(inst) == 2
      assert TaskJournal.released_offset(inst) == 2
      assert TaskJournal.committed_iterations(inst) == [0, 1, 2]
      assert TaskJournal.current_epoch(inst) == 0
    end

    test "the owed undo shrinks as each compensated entry lands" do
      inst = instance_id()
      seg = seg_identity(inst, "TaskA", "s0")
      dead_letter_iteration_2(inst, seg)

      :ok = TaskJournal.append_compensated(inst, 0, 2, [ran(seg, {3, 3})])
      assert TaskJournal.restore_state(inst).rewind_undo == []

      # The next turn commits as iteration 3, owing nothing.
      :ok = TaskJournal.append_task_committed(inst, 0, 3, [ran(seg, {4, 4})], [])
      :ok = TaskJournal.append_iter_committed(inst, 3, 3, [])
      assert %{iter: 4, rewind_undo: []} = TaskJournal.restore_state(inst)
    end

    test "counts on every later iteration record, and a rewind over it plans nothing twice" do
      inst = instance_id()
      seg = seg_identity(inst, "TaskA", "s0")
      dead_letter_iteration_2(inst, seg)
      assert TaskJournal.failed_turns(inst) == {1, 2}

      :ok = TaskJournal.append_compensated(inst, 0, 2, [])
      :ok = TaskJournal.append_task_committed(inst, 0, 3, [ran(seg, {4, 4})], [])
      :ok = TaskJournal.append_iter_committed(inst, 3, 3, [])
      assert TaskJournal.failed_turns(inst) == {1, 2}

      {:ok, entries} = TaskJournal.read(inst)

      assert [nil, nil, {1, 2}] =
               for(%{type: :iter_committed, payload: p} <- entries, do: p.failed_turns)

      # The failed turn is a valid target and inside the committed range; the
      # coverage guard skips it, and a rewind below it owes only iteration 3.
      assert TaskJournal.staged_channels_above(inst, 1) == []
      :ok = TaskJournal.append_rewound(inst, 1)
      assert [%{task_idx: 0, iter: 3}] = TaskJournal.restore_state(inst).rewind_undo
      assert TaskJournal.failed_turns(inst) == {1, 2}
    end

    test "survives the cut until its undo is journaled" do
      inst = instance_id()
      cuttable(inst)
      seg = seg_identity(inst, "TaskA", "s0")
      dead_letter_iteration_2(inst, seg, false)

      # The dead-lettered record is the head; its owed task stays with it.
      cut_and_await(inst, [4, 5])
      assert %{iter: 3, rewind_undo: [%{iter: 2}]} = TaskJournal.restore_state(inst)

      :ok = TaskJournal.append_compensated(inst, 0, 2, [])
      cut_and_await(inst, [5, 6])
      assert %{iter: 3, consumed_offset: 2, rewind_undo: []} = TaskJournal.restore_state(inst)
      assert TaskJournal.failed_turns(inst) == {1, 2}
    end

    test "leaves an iteration commit's bytes as they were while no turn failed" do
      inst = instance_id()
      :ok = TaskJournal.append_iter_committed(inst, 0, 4, [{1, "h"}])

      [body] =
        Flezha.State.RecordLog.read(Paths.journal_path(inst), fn _seq, _header, body ->
          {:ok, body}
        end)

      assert body ==
               Codec.encode_entry(3, 0, 0, [
                 <<1, 4::64-big, 0::16-big>>,
                 Codec.encode_handoff([{1, "h"}])
               ])
    end

    test "renders as a journal row naming the tasklet and the entry" do
      inst = instance_id()
      dead_letter_iteration_2(inst, seg_identity(inst, "TaskA", "s0"))

      out =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Steps.journal", Jason.encode!([inst]), [])
        end)

      assert out =~ "dead_lettered task_idx=0 iter=2"
      assert out =~ "consumed input-log offset: 2"
      assert out =~ "turn failed in answer after 3 attempt(s); parked as c/_turn/e2"
    end
  end
end
