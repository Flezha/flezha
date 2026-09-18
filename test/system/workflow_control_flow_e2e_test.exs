defmodule FlezhaTest.System.WorkflowControlFlowE2ETest do
  @moduledoc """
  The durable control flow a structured workflow operative runs, end to end
  against a live reference runner over UDS: a loop that commits per iteration,
  parks when its inbox is empty and rehydrates on the next input; a rewind that
  compensates back to an earlier iteration and bumps the epoch; the entity
  retry ladder and what it does when it exhausts; a branching tree, whose
  fan-out either joins once or unwinds every branch before their parent; and a
  loop a procedure ends with `complete`.

  All but the last prove controller mechanics rather than a runner, so those
  cells pin the reference SDK; the completion is each SDK's own, so its cell
  runs on the selected runtime. Tagged `:system` (boots real subprocesses) — excluded from the
  default `mix test`; run with:

      mix test --only workflow_crash_resume
      mix test test/system/workflow_control_flow_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :workflow

  # The shared tier-e2e mechanics.
  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, kill_overwatch: 2, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.WorkflowResumeHarness

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Replay
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :workflow_crash_resume
  # A loop, a rewind or a retry ladder runs many turns through a live runner.
  @moduletag timeout: 180_000

  describe "durable loop + suspension (reference/uds)" do
    @describetag transport: :uds, cell: :uds_loop, reference_runtime: true

    # `workflow-durable-loop-and-suspension`, end to end on a real runner:
    #
    #   1. two chat turns → two journaled iterations, each with its consumed
    #      inbox offset; the loop then PARKS on the idle inbox and the instance
    #      is evicted from the runner (still registered, `suspended`);
    #   2. a message sent while it is suspended is durably journaled and
    #      REHYDRATES it — processed as the next iteration, on a live runner;
    #   3. the runner is SIGKILLed mid-iteration 3 and a replacement takes
    #      over: the run resumes AT iteration 3 — the committed turns never
    #      re-run — and the undelivered inbox tail is re-forwarded.
    test "loop commits iterations, parks when idle, rehydrates on input, resumes mid-iteration",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_loop")
      operative_id = unique("chat_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      # Park fast so the idle window does not dominate the test.
      {port_a, exec_a} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", "700"}]
        )

      info =
        start_workflow(namespace, operative_id, "chat_loop_workflow",
          plugins: [forward_sink("chat_sink", collector)]
        )

      assert info.runner_id == exec_a

      # --- 1. two turns → two committed iterations, then a park ---
      send_turn(namespace, operative_id, "hello")
      send_turn(namespace, operative_id, "again")

      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 30_000),
             "the loop did not commit one iteration per turn: " <>
               inspect(committed_iters(operative_id))

      assert replies(collector, 2, operative_id) == ["reply:hello", "reply:again"]

      # Each iteration recorded the inbox offset it consumed, and the consumed
      # prefix was released.
      assert consumed_offsets(operative_id) == [1, 2]
      assert ProcInbox.undelivered(operative_id) == []

      # The iteration boundary compacts every Entity of the instance: the
      # controller's CompactEntity must resolve a WORKFLOW Entity (not only a
      # stream one), so a snapshot lands beside the turn WAL.
      assert NodeTestHelper.wait_until(
               fn -> snapshot_count(operative_id, "chat_turn") > 0 end,
               20_000
             ),
             "no boundary snapshot for chat_turn after two committed iterations; dir holds " <>
               inspect(File.ls(Path.join(Paths.class_dir(operative_id), "chat_turn")))

      assert NodeTestHelper.wait_until(
               fn -> ProcJournal.restore_state(operative_id).parked end,
               20_000
             ),
             "the idle loop never parked at its committed boundary"

      # Evicted from the runner, but still registered and addressable — that IS
      # suspension (zero live RUNNER resources, not zero resources).
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 20_000),
             "the parked instance was never suspended controller-side"

      assert registered?(namespace, operative_id)

      # --- 2. a message while suspended rehydrates it ---
      send_turn(namespace, operative_id, "wake-up")

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(operative_id) == [0, 1, 2] end,
               60_000
             ),
             "the suspended instance did not rehydrate and run the new turn as iteration 2"

      assert replies(collector, 3, operative_id) == [
               "reply:hello",
               "reply:again",
               "reply:wake-up"
             ]

      refute suspended?(namespace, operative_id)

      # --- 3. SIGKILL mid-iteration 3 → resume AT iteration 3 ---
      # The class blocks from its third turn on while the release env is unset,
      # so this turn hangs mid-iteration with iterations 0-2 durable.
      send_turn(namespace, operative_id, "mid-crash")

      assert NodeTestHelper.wait_until(
               fn -> ProcInbox.last_offset(operative_id) >= 4 end,
               20_000
             ),
             "the crashing turn never reached the durable inbox"

      # Operator surface, while the turn is still in flight: it is durable and
      # marked undelivered — this is what survives the SIGKILL below.
      pending_inbox =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.state_inbox(operative_id)
        end)

      assert pending_inbox =~ "durable inbox for instance #{operative_id}"
      assert pending_inbox =~ "consumed through 3"
      assert pending_inbox =~ "offset=4 PENDING"

      sigkill(os_pid(port_a))

      {_port_b, exec_b} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", "700"}]
        )

      assert exec_b != exec_a

      assert NodeTestHelper.wait_until(
               fn -> exec_a not in FleetManager.active_runners(namespace) end,
               15_000
             ),
             "dead runner was never evicted after the replacement connected"

      kill_overwatch(namespace, operative_id)

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(operative_id) == [0, 1, 2, 3] end,
               90_000
             ),
             "the resumed loop never committed the in-flight iteration 3"

      assert replies(collector, 4, operative_id) == [
               "reply:hello",
               "reply:again",
               "reply:wake-up",
               "reply:mid-crash"
             ]

      # The committed turns did not re-run: `chat_turn` commits one keyed Change
      # per DISTINCT turn, so a re-run of iteration 0-2 would add a Change.
      # Counted compaction-proof — the boundary snapshots fold the early ones.
      assert change_count(operative_id, "chat_turn") == 4,
             "a committed iteration re-ran on resume (resume-at-iteration broken)"

      # --- operator surface ---
      journal =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.state_journal(operative_id)
        end)

      assert journal =~ "iter_committed"
      assert journal =~ "consumed inbox offset:"
      assert journal =~ "parked"
      assert journal =~ "proc_committed proc_idx=0 iter=3"

      # Every turn is consumed now, so the inbox has released all of them: the
      # journal keeps the offsets, the inbox delivers none. The file itself is
      # rewritten once the released prefix is worth its fsync (the cut
      # threshold); a few tiny turns stay on disk, never re-delivered.
      assert ProcJournal.consumed_offset(operative_id) == 4
      assert ProcJournal.released_offset(operative_id) == 4
      assert ProcInbox.undelivered(operative_id) == []
    end

    # Suspension is durable STATE, not a live process: a controller-side restart
    # of the instance's supervisor while it is suspended must rediscover it from
    # the journal alone — still evicted, still resumable — and a message then
    # sent must rehydrate it exactly as if nothing had happened.
    test "suspension survives a controller-side restart of the instance", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_loop_boot")
      operative_id = unique("chat_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "chat_loop_workflow",
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, operative_id, "one")

      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0] end, 30_000)
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 20_000)

      # Restart the instance's controller-side supervisor while it is suspended.
      # It must come back from durable state as suspended — NOT bind a runner.
      kill_overwatch(namespace, operative_id)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 30_000),
             "a suspended instance was not rediscovered from durable state on restart"

      # And it still wakes on input.
      send_turn(namespace, operative_id, "two")

      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 60_000),
             "the rediscovered suspended instance did not rehydrate on a new message"

      assert replies(collector, 2, operative_id) == ["reply:one", "reply:two"]
    end

    # The same park/rehydrate cycle for a class with NO per-Entity state. It has
    # no Entity WAL, so the controller ships no `RestoreInitState` on the
    # rehydrating bind — the proc-journal reaches the driver only because it rides
    # `AddOperative`. Delivered on the restore frame instead, turn two commits at
    # iteration 0 again, collides with iteration 0's `outbox_flushed`, and its
    # reply is journaled but never delivered.
    test "a loop with no per-Entity state resumes at its iteration and still delivers", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_loop_stateless")
      operative_id = unique("stateless_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "stateless_loop_workflow",
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, operative_id, "one")
      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0] end, 30_000)

      # The park is what makes the next turn a fresh bind.
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 20_000),
             "the idle stateless loop never parked"

      # Nothing on disk to restore — the journal is the only thing carried over.
      assert Replay.resume_entity_count(:workflow, operative_id) == 0

      send_turn(namespace, operative_id, "two")

      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 60_000),
             "the rehydrated turn did not commit as iteration 1: " <>
               inspect(committed_iters(operative_id))

      # The point of the test: the second turn's staged output was delivered.
      assert replies(collector, 2, operative_id) == ["reply:one", "reply:two"]
    end
  end

  describe "a procedure ends the loop (uds)" do
    # Not pinned to the reference SDK: the completion is each runtime's own
    # `ctx.complete`, and every test app's `chat_loop_workflow` ends itself on
    # the turn `bye`, completing with the number of turns its first procedure
    # recorded.
    @describetag transport: :uds, cell: :uds_complete

    test "turns then bye end the instance ok with the turn count, and it does not park after",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_bye")
      operative_id = unique("chat_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      # A short idle window, so a loop that went on waiting after `bye` would
      # park inside this test rather than after it.
      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "chat_loop_workflow",
        plugins: [forward_sink("chat_sink", collector)]
      )

      for turn <- ["one", "two", "three", "bye"], do: send_turn(namespace, operative_id, turn)

      assert NodeTestHelper.wait_until(
               fn -> match?({:ok, %{status: _}}, Completion.read(operative_id)) end,
               60_000
             ),
             "the loop did not end on bye; committed iterations: " <>
               inspect(committed_iters(operative_id))

      assert {:ok, %{status: "ok", result: "3"}} = Completion.read(operative_id)

      # The completing turn still ran its whole iteration, reply included.
      assert replies(collector, 4, operative_id) ==
               ["reply:one", "reply:two", "reply:three", "reply:bye"]

      {:ok, entries} = ProcJournal.read(operative_id)
      assert committed_iters(operative_id) == [0, 1, 2, 3]
      last = entries |> Enum.filter(&(&1.type == :iter_committed)) |> List.last()
      assert last.payload.completion == "3"

      refute entries |> Enum.drop_while(&(&1 != last)) |> Enum.any?(&(&1.type == :parked)),
             "the completed loop parked after its last turn"
    end
  end

  describe "loop rewind (reference/uds)" do
    @describetag transport: :uds, cell: :uds_rewind, reference_runtime: true

    # `workflow-loop-rewind`, end to end on a real runner: three chat turns
    # commit, the operator rewinds to iteration 0, and
    #
    #   * iterations 2 and 1 are compensated in that order — the reply procedure's
    #     undo handle is in Entity State, so it retracts the turns in reverse;
    #   * the loop resumes waiting at iteration 1, and the next message sent is
    #     processed as that iteration (a rewind is undo-and-continue: a committed
    #     turn's input is not retained past its commit);
    #   * everything emitted after the rewind carries a bumped epoch, so a guarded
    #     sink takes the post-rewind output as newer than the unwound run's.
    test "rewind compensates back, resumes at the rewound-to iteration, and bumps the epoch",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_rewind")
      operative_id = unique("chat_rewind_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      {_port, _exec} = boot_runner(namespace, ctx, id_path, release?: false)

      start_workflow(namespace, operative_id, "chat_rewind_workflow",
        plugins: [forward_sink("chat_sink", collector)]
      )

      # --- three turns → three committed iterations ---
      for turn <- ~w(alpha beta gamma), do: send_turn(namespace, operative_id, turn)

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(operative_id) == [0, 1, 2] end,
               60_000
             ),
             "the loop did not commit one iteration per turn: " <>
               inspect(committed_iters(operative_id))

      assert replies(collector, 3, operative_id) == ["reply:alpha", "reply:beta", "reply:gamma"]
      assert ProcJournal.current_epoch(operative_id) == 0
      assert consumed_offsets(operative_id) == [1, 2, 3]

      # --- rewind to iteration 0 ---
      rewind_out =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.operative_rewind(namespace, operative_id, 0)
        end)

      assert rewind_out =~ "rewound to iteration 0"

      # The journal folded back: only iteration 0 is committed, and the epoch is
      # bumped so everything from here on supersedes what the unwound turns wrote.
      assert NodeTestHelper.wait_until(
               fn -> ProcJournal.current_epoch(operative_id) == 1 end,
               20_000
             )

      # --- the loop unwinds the two turns above the target ---
      # Only iteration 0 remains committed; iterations 2 and 1 are undone.
      assert NodeTestHelper.wait_until(
               fn -> ProcJournal.committed_iterations(operative_id) == [0] end,
               60_000
             ),
             "the journal did not fold back to the rewound-to iteration"

      assert NodeTestHelper.wait_until(fn -> length(collected(collector)) >= 5 end, 90_000),
             "the rewound iterations were never compensated"

      # --- the next message is processed as iteration 1 ---
      send_turn(namespace, operative_id, "delta")

      assert NodeTestHelper.wait_until(
               fn -> ProcJournal.committed_iterations(operative_id) == [0, 1] end,
               90_000
             ),
             "the resumed loop did not run the next message as iteration 1"

      outputs = await_outputs(collector, 6, operative_id)

      # Undo runs in reverse iteration order (gamma, then beta), and only then
      # does the loop resume forward. The epoch rule, on the wire: everything the
      # unwound run emitted carries epoch 0; every undo and everything the resumed
      # loop emits carries epoch 1, so a guarded sink takes it as the newer value.
      assert Enum.map(outputs, fn {payload, md} -> {payload, Map.get(md, "flz.epoch")} end) ==
               [
                 {"reply:alpha", "0"},
                 {"reply:beta", "0"},
                 {"reply:gamma", "0"},
                 {"retract:gamma", "1"},
                 {"retract:beta", "1"},
                 {"reply:delta", "1"}
               ]
    end
  end

  describe "entity retry (reference/uds)" do
    @describetag transport: :uds, cell: :uds_retry, reference_runtime: true

    # `workflow-entity-retry` on a live runner: `retry_flaky` fails its first
    # attempt, the per-entity ladder re-runs it in place, and the procedure
    # commits normally — nothing compensates and the run reports `ok`.
    test "a transient entity failure is retried and the run completes", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id = drive_retry_run(ctx, collector, "1", "ok")

      # `res:...` is proc A's forward output; the terminal marker names the attempt
      # the flaky entity finally succeeded on — 2 ⇒ the ladder absorbed the
      # first failure.
      assert [{~s(res:{"seed":"go"}), _}, {marker, _}] = await_outputs(collector, 2, operative_id)
      assert marker == ~s(attempt:2:{"seed":"go"})

      {:ok, entries} = ProcJournal.read(operative_id)
      committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx
      assert committed == [0, 1], "the retried procedure must still commit its boundary"

      refute Enum.any?(entries, &(&1.type == :compensated)),
             "a recovered retry must not compensate"
    end

    # Arming more failures than the ladder has attempts exhausts it: the original
    # error is terminal, proc A rolls back (its undo record is journaled + flushed),
    # and the run reports `error` — the pre-change failure path, unchanged.
    test "a persistent entity failure exhausts the ladder, compensates and errors", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id = drive_retry_run(ctx, collector, "99", "error")

      assert [{~s(res:{"seed":"go"}), _}, {"res:undone", _}] =
               await_outputs(collector, 2, operative_id)

      {:ok, entries} = ProcJournal.read(operative_id)

      assert [%{type: :compensated, proc_idx: 0}] =
               Enum.filter(entries, &(&1.type == :compensated)),
             "the exhausted ladder did not run the saga rollback"

      committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx
      assert committed == [0], "the failing procedure must never commit"
    end
  end

  describe "branching tree (reference/uds)" do
    @describetag transport: :uds, cell: :uds_branch, reference_runtime: true

    # The chain's args are the serialized `init_values` the harness starts with.
    @seed ~s({"seed":"go"})

    # `workflow-branching` on a live runner. BranchA fans out to BranchB and
    # BranchC; BranchB feeds BranchD, which blocks. The SIGKILL therefore lands
    # with A, B and C committed and only D's subtree outstanding — and the
    # respawn must re-run D ALONE, seeded from B's journaled edge.
    test "a fan-out runs to DONE, and a crash re-runs only the uncommitted subtree", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id =
        drive_crash_resume(
          ctx,
          "branch_workflow",
          "ok",
          &branch_fanned_out?/1,
          plugins: [forward_sink("branch_sink", collector)]
        )

      delivered = collector |> await_outputs(4, operative_id) |> Enum.map(&elem(&1, 0))

      assert Enum.sort(delivered) == [
               "a:#{@seed}",
               "b:#{@seed}",
               "c:#{@seed}",
               "d:#{@seed}"
             ]

      assert Enum.count(delivered, &(&1 == "c:#{@seed}")) == 1,
             "the sibling branch had already committed: it must not re-run"

      {:ok, entries} = ProcJournal.read(operative_id)
      committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx

      assert committed == [0, 1, 2, 3],
             "each procedure commits exactly once across the crash: #{inspect(committed)}"

      # The SDK declares the leaf set, so the controller derives DONE from the
      # journal alone — no runner to ask.
      leaf_procs = ClassRegistry.leaf_procs(operative_id)
      assert leaf_procs == [2, 3], "BranchC and BranchD are the tree's leaves"
      assert ProcJournal.complete?(operative_id, leaf_procs)

      rendered =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.state_journal(operative_id)
        end)

      assert rendered =~ "leaves: 2,3  complete: true"
      assert rendered =~ "edge: channel_id="
    end

    # A failure under one branch, after BOTH branches committed: the rollback is
    # reverse-topological — the sibling leaf and the failing branch's committed
    # procedure unwind before the root they share.
    test "a failure under one branch unwinds both branches before their parent", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      namespace = unique("workflow_branch")
      operative_id = unique("branch_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: true,
        extra_env: [{"FLEZHA_BRANCH_FAIL", "1"}]
      )

      start_workflow(namespace, operative_id, "branch_workflow",
        plugins: [forward_sink("branch_sink", collector)]
      )

      await_status(operative_id, "error", 30_000)

      # Forward output of the three completed procedures, then their undo — the
      # deepest first, the shared root last.
      delivered = collector |> await_outputs(6, operative_id) |> Enum.map(&elem(&1, 0))

      assert Enum.drop(delivered, 3) == ["undo-c", "b:undo-b", "a:undo-a"],
             "descendants before ancestors: #{inspect(delivered)}"

      {:ok, entries} = ProcJournal.read(operative_id)
      committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx
      assert committed == [0, 1, 2], "the failing leaf never commits"

      compensated = for %{type: :compensated, proc_idx: idx} <- entries, do: idx
      assert Enum.sort(compensated) == [0, 1, 2]

      refute ProcJournal.complete?(operative_id, ClassRegistry.leaf_procs(operative_id)),
             "a leaf that never committed leaves the operative incomplete"
    end
  end

  # The crash point of `branch_workflow`: the root and BOTH its branches have
  # committed, and only the blocked grandchild is outstanding.
  defp branch_fanned_out?(operative_id) do
    case ProcJournal.restore_state(operative_id) do
      %{committed_proc_idxs: [0, 1, 2]} -> true
      _ -> false
    end
  end

  # Start `retry_workflow` on a fresh runner with `failures` armed on the flaky
  # entity, and wait for the run's terminal status.
  defp drive_retry_run(ctx, collector, failures, terminal_status) do
    namespace = unique("workflow_retry")
    operative_id = unique("retry_workflow")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, _exec} =
      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_WORKFLOW_RETRY_FAILS", failures}]
      )

    start_workflow(namespace, operative_id, "retry_workflow",
      plugins: [forward_sink("retry_sink", collector)]
    )

    await_status(operative_id, terminal_status, 30_000)

    operative_id
  end

  # --- durable-loop helpers ---------------------------------------------------

  # The sink's collector accumulates, so each wait is for the running total.
  defp replies(collector, count, operative_id) do
    collector |> await_outputs(count, operative_id) |> Enum.map(fn {payload, _md} -> payload end)
  end

  defp send_turn(namespace, operative_id, text) do
    :ok = OperativeManager.send_io(operative_id, namespace, text, %{"channel" => "in"})
  end

  defp committed_iters(operative_id) do
    {:ok, entries} = ProcJournal.read(operative_id)
    for %{type: :iter_committed, iter: iter} <- entries, do: iter
  end

  defp consumed_offsets(operative_id) do
    {:ok, entries} = ProcJournal.read(operative_id)
    for %{type: :iter_committed, payload: %{consumed_offset: o}} <- entries, do: o
  end

  defp suspended?(namespace, operative_id) do
    case OperativeManager.get_overwatch_state(namespace, operative_id) do
      {:ok, %{suspended: true}} -> true
      _ -> false
    end
  end

  # Count change-log records in a procedure's retained WAL file(s). The WAL is
  # a stream of `<seq::64><len::32><body::len><crc::32>` records.
  defp snapshot_count(operative_id, procedure),
    do: length(Paths.snapshot_paths({operative_id, procedure, ""}))

  # Distinct Changes applied to the keyless Entity: the seqs still in its WAL
  # plus everything its latest snapshot folded (seq 0..snapshot_seq).
  defp change_count(operative_id, procedure) do
    folded =
      case Paths.snapshot_paths({operative_id, procedure, ""}) do
        [] -> MapSet.new()
        snaps -> snaps |> List.last() |> elem(0) |> then(&MapSet.new(0..&1//1))
      end

    [Paths.class_dir(operative_id), procedure, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.flat_map(fn path -> wal_seqs(File.read!(path), []) end)
    |> MapSet.new()
    |> MapSet.union(folded)
    |> MapSet.size()
  end
end
