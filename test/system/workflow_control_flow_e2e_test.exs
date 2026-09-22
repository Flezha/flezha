defmodule FlezhaTest.System.WorkflowControlFlowE2ETest do
  @moduledoc """
  The durable control flow a structured workflow operative runs, end to end
  against a live runner of the selected SDK runtime over UDS: a loop that commits per iteration,
  parks when its inbox is empty and rehydrates on the next input; a rewind that
  compensates back to an earlier iteration and bumps the epoch; the cell
  retry ladder and what it does when it exhausts; a branching tree, whose
  fan-out either joins once or unwinds every branch before their parent; a
  loop a tasklet ends with `complete`; and an unreadable message a waiting
  instance dead-letters without waking up for it again.

  Every scenario runs on the selected runtime: a workflow's stages run through
  the core's routing, mailboxes and poll core in every SDK, and each test app
  carries the same fixtures. Tagged `:system` (boots real subprocesses) —
  excluded from the default `mix test`; run with:

      mix test --only workflow_crash_resume
      mix test test/system/workflow_control_flow_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :workflow

  # The shared tier-e2e mechanics.
  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, kill_overwatch: 2, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.WorkflowResumeHarness

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Replay
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.Wire
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.InputLog
  alias Flezha.State.TaskJournal
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :workflow_crash_resume
  # A loop, a rewind or a retry ladder runs many turns through a live runner.
  @moduletag timeout: 180_000

  describe "durable loop + suspension (uds)" do
    @describetag transport: :uds, scenario: :uds_loop

    # `workflow-durable-loop-and-suspension`, end to end on a real runner:
    #
    #   1. two chat turns → two journaled iterations, each with its consumed
    #      input-log offset; the loop then PARKS on the idle inbox and the instance
    #      is evicted from the runner (still registered, `suspended`);
    #   2. a message sent while it is suspended is durably journaled and
    #      REHYDRATES it — processed as the next iteration, on a live runner;
    #   3. the runner is SIGKILLed mid-iteration 3 and a replacement takes
    #      over: the run resumes AT iteration 3 — the committed turns never
    #      re-run — and the input log's undelivered tail is re-forwarded.
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
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
        )

      info =
        start_workflow(namespace, operative_id, "chat_loop_workflow",
          first_message: nil,
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

      # Each iteration recorded the input-log offset it consumed, and the
      # consumed prefix was released.
      assert consumed_offsets(operative_id) == [0, 1]
      assert_released(operative_id)

      # The iteration boundary compacts every Cell of the instance: the
      # controller's CompactCell must resolve a WORKFLOW Cell (not only a
      # stream one), so a snapshot lands beside the turn WAL.
      assert NodeTestHelper.wait_until(
               fn -> snapshot_count(operative_id, "chat_turn") > 0 end,
               20_000
             ),
             "no boundary snapshot for chat_turn after two committed iterations; dir holds " <>
               inspect(File.ls(Path.join(Paths.class_dir(operative_id), "chat_turn")))

      assert NodeTestHelper.wait_until(
               fn -> ClassRegistry.parked(operative_id) != nil end,
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
               fn -> match?({:ok, _floor, last} when last >= 3, InputLog.tail(operative_id)) end,
               20_000
             ),
             "the crashing turn never reached the input log"

      # Operator surface, while the turn is still in flight: it is durable and
      # marked undelivered — this is what survives the SIGKILL below.
      pending_inbox =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.state_inbox(operative_id)
        end)

      assert pending_inbox =~ "durable input log for #{operative_id}"
      assert pending_inbox =~ "consumed through 2"
      assert pending_inbox =~ "offset=3 PENDING"

      sigkill(os_pid(port_a))

      {_port_b, exec_b} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
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
      assert journal =~ "consumed input-log offset:"
      refute journal =~ "parked", "where the loop slept is not the journal's"
      assert journal =~ "task_committed task_idx=0 iter=3"

      # Every turn is consumed now, so the log has released all of them: the
      # journal keeps the offsets, the log delivers none. The file itself is
      # rewritten once the released prefix is worth its fsync (the cut
      # threshold); a few tiny turns stay on disk, never re-delivered.
      assert TaskJournal.consumed_offset(operative_id) == 3
      assert TaskJournal.released_offset(operative_id) == 3
      assert_released(operative_id)
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
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "chat_loop_workflow",
        first_message: nil,
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

    # The same park/rehydrate cycle for a class with NO per-Cell state. It has
    # no Cell WAL, so the controller ships no `RestoreInitState` on the
    # rehydrating bind — the task-journal reaches the driver only because it rides
    # `AddOperative`. Delivered on the restore frame instead, turn two commits at
    # iteration 0 again, collides with iteration 0's `outbox_flushed`, and its
    # reply is journaled but never delivered.
    test "a loop with no per-Cell state resumes at its iteration and still delivers", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_loop_stateless")
      operative_id = unique("stateless_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "stateless_loop_workflow",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, operative_id, "one")
      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0] end, 30_000)

      # The park is what makes the next turn a fresh bind.
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 20_000),
             "the idle stateless loop never parked"

      # Nothing on disk to restore — the journal is the only thing carried over.
      assert Replay.resume_cell_count(operative_id) == 0

      send_turn(namespace, operative_id, "two")

      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 60_000),
             "the rehydrated turn did not commit as iteration 1: " <>
               inspect(committed_iters(operative_id))

      # The point of the test: the second turn's staged output was delivered.
      assert replies(collector, 2, operative_id) == ["reply:one", "reply:two"]
    end
  end

  describe "stopping a sleeping loop (uds)" do
    @describetag transport: :uds, scenario: :uds_stop_sleeping

    # A stop answers the same whether the loop is asleep or awake: the sleeper
    # is woken onto a runner, `WorkflowStop` follows the bind, and it ends
    # `cancelled` with its record and directory kept.
    test "a suspended loop is woken and ends cancelled with its directory kept", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_stop_sleeping")
      operative_id = unique("chat_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "chat_loop_workflow",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, operative_id, "hello")
      assert replies(collector, 1, operative_id) == ["reply:hello"]
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 20_000)

      assert {:ok, :cooperative} = OperativeManager.request_stop(namespace, operative_id)
      await_status(operative_id, "cancelled", 30_000)
      assert File.dir?(Paths.class_dir(operative_id))
    end
  end

  describe "a tasklet ends the loop (uds)" do
    # The completion is each runtime's own `ctx.complete`: every test app's
    # `chat_loop_workflow` ends itself on the turn `bye`, completing with the
    # number of turns its first tasklet recorded.
    @describetag transport: :uds, scenario: :uds_complete

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
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "chat_loop_workflow",
        first_message: nil,
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

      {:ok, entries} = TaskJournal.read(operative_id)
      assert committed_iters(operative_id) == [0, 1, 2, 3]
      last = entries |> Enum.filter(&(&1.type == :iter_committed)) |> List.last()
      assert last.payload.completion == "3"

      assert ClassRegistry.parked(operative_id) == nil,
             "the completed loop parked after its last turn"
    end
  end

  describe "an unreadable message (uds)" do
    # Whether the entry reads a message is each runtime's own decoder: every
    # test app's `typed_once_workflow` decodes its `order` entry as a JSON
    # ticket and completes with the ticket's text.
    @describetag transport: :uds, scenario: :uds_unreadable

    test "is dead-lettered and consumed, so the waiting instance stays suspended until a readable one",
         ctx do
      namespace = unique("workflow_unreadable")
      operative_id = unique("typed_once_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      parent = self()
      handler = {__MODULE__, :rehydrating, operative_id}

      :telemetry.attach(
        handler,
        [:flezha, :workflow, :rehydrating],
        fn _e, _meas, meta, _ ->
          if meta.operative_id == operative_id, do: send(parent, :rehydrating)
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_PARK_IDLE_MS", "700"}]
        )

      start_workflow(namespace, operative_id, "typed_once_workflow", first_message: nil)
      send_turn(namespace, operative_id, "not json", "order")

      assert NodeTestHelper.wait_until(fn -> DlqStore.list(operative_id) != [] end, 30_000),
             "the unreadable message was never dead-lettered"

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, operative_id) end, 30_000),
             "the instance waiting for a readable message never suspended"

      # The parked message counts as consumed: the log owes nothing, so the
      # suspension holds instead of rehydrating to deliver it again.
      assert InputLog.undelivered(operative_id) == []
      refute_receive :rehydrating, 3_000
      assert suspended?(namespace, operative_id)
      assert [_one] = DlqStore.list(operative_id)

      send_turn(namespace, operative_id, ~s({"text":"after"}), "order")
      await_status(operative_id, "ok", 60_000)
      assert {:ok, %{result: "after"}} = Completion.read(operative_id)
    end
  end

  describe "loop rewind (uds)" do
    @describetag transport: :uds, scenario: :uds_rewind

    # `workflow-loop-rewind`, end to end on a real runner: three chat turns
    # commit, the operator rewinds to iteration 0, and
    #
    #   * iterations 2 and 1 are compensated in that order — the reply tasklet's
    #     undo handle is in Cell State, so it retracts the turns in reverse;
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
        first_message: nil,
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
      assert TaskJournal.current_epoch(operative_id) == 0
      assert consumed_offsets(operative_id) == [0, 1, 2]

      # --- rewind to iteration 0 ---
      rewind_out =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.operative_rewind(namespace, operative_id, 0)
        end)

      assert rewind_out =~ "rewound to iteration 0"

      # The journal folded back: only iteration 0 is committed, and the epoch is
      # bumped so everything from here on supersedes what the unwound turns wrote.
      assert NodeTestHelper.wait_until(
               fn -> TaskJournal.current_epoch(operative_id) == 1 end,
               20_000
             )

      # --- the loop unwinds the two turns above the target ---
      # Only iteration 0 remains committed; iterations 2 and 1 are undone.
      assert NodeTestHelper.wait_until(
               fn -> TaskJournal.committed_iterations(operative_id) == [0] end,
               60_000
             ),
             "the journal did not fold back to the rewound-to iteration"

      assert NodeTestHelper.wait_until(fn -> length(collected(collector)) >= 5 end, 90_000),
             "the rewound iterations were never compensated"

      # --- the next message is processed as iteration 1 ---
      send_turn(namespace, operative_id, "delta")

      assert NodeTestHelper.wait_until(
               fn -> TaskJournal.committed_iterations(operative_id) == [0, 1] end,
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

  describe "cell retry (uds)" do
    @describetag transport: :uds, scenario: :uds_retry

    # `workflow-cell-retry` on a live runner: `retry_flaky` fails its first
    # attempt, the per-cell ladder re-runs it in place, and the tasklet
    # commits normally — nothing compensates and the run reports `ok`.
    test "a transient cell failure is retried and the run completes", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id = drive_retry_run(ctx, collector, "1", "ok")

      # `res:...` is task A's forward output; the terminal marker names the attempt
      # the flaky cell finally succeeded on — 2 ⇒ the ladder absorbed the
      # first failure.
      assert [{~s(res:{"seed":"go"}), _}, {marker, _}] = await_outputs(collector, 2, operative_id)
      assert marker == ~s(attempt:2:{"seed":"go"})

      {:ok, entries} = TaskJournal.read(operative_id)
      committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx
      assert committed == [0, 1], "the retried tasklet must still commit its boundary"

      refute Enum.any?(entries, &(&1.type == :compensated)),
             "a recovered retry must not compensate"
    end

    # Arming more failures than the ladder has attempts exhausts it: the original
    # error is terminal, task A rolls back (its undo record is journaled + flushed),
    # and the run reports `error` — the pre-change failure path, unchanged.
    test "a persistent cell failure exhausts the ladder, compensates and errors", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id = drive_retry_run(ctx, collector, "99", "error")

      assert [{~s(res:{"seed":"go"}), _}, {"res:undone", _}] =
               await_outputs(collector, 2, operative_id)

      {:ok, entries} = TaskJournal.read(operative_id)

      assert [%{type: :compensated, task_idx: 0}] =
               Enum.filter(entries, &(&1.type == :compensated)),
             "the exhausted ladder did not run the saga rollback"

      committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx
      assert committed == [0], "the failing tasklet must never commit"
    end
  end

  describe "branching tree (uds)" do
    @describetag transport: :uds, scenario: :uds_branch

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

      committed = committed_tasks(operative_id)

      # The branches commit on their own, in whichever order they finish.
      assert hd(committed) == 0 and Enum.sort(committed) == [0, 1, 2, 3],
             "each tasklet commits exactly once across the crash: #{inspect(committed)}"

      # The SDK declares the leaf set, so the controller derives DONE from the
      # journal alone — no runner to ask.
      leaf_tasks = ClassRegistry.leaf_tasks(operative_id)
      assert leaf_tasks == [2, 3], "BranchC and BranchD are the tree's leaves"
      assert TaskJournal.complete?(operative_id, leaf_tasks)

      rendered =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.state_journal(operative_id)
        end)

      assert rendered =~ "leaves: 2,3  complete: true"
      assert rendered =~ "edge: channel_id="
    end

    # A failure under one branch, after BOTH branches committed: the rollback is
    # reverse-topological — the sibling leaf and the failing branch's committed
    # tasklet unwind before the root they share.
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

      # Forward output of the three completed tasklets, then their undo — the
      # branches in reverse of whichever committed first, the shared root last.
      delivered = collector |> await_outputs(6, operative_id) |> Enum.map(&elem(&1, 0))
      {branches, [root]} = delivered |> Enum.drop(3) |> Enum.split(2)

      assert Enum.sort(branches) == ["b:undo-b", "undo-c"] and root == "a:undo-a",
             "descendants before ancestors: #{inspect(delivered)}"

      {:ok, entries} = TaskJournal.read(operative_id)
      committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx

      assert hd(committed) == 0 and Enum.sort(committed) == [0, 1, 2],
             "the failing leaf never commits: #{inspect(committed)}"

      compensated = for %{type: :compensated, task_idx: idx} <- entries, do: idx
      assert Enum.sort(compensated) == [0, 1, 2]

      refute TaskJournal.complete?(operative_id, ClassRegistry.leaf_tasks(operative_id)),
             "a leaf that never committed leaves the operative incomplete"
    end
  end

  describe "a keyed workflow tasklet (uds)" do
    @describetag transport: :uds, scenario: :uds_keyed

    # A `key_by` into `keyed_commit` mints one Cell per key of the entry's list,
    # read on the controller's durable view: the third tasklet blocks (no
    # release), and a live read would wait behind it on a runner whose owner
    # process serves reads and stage pushes in turn.
    test "N keys run N Cells, and the keyed stage's commit names each once", ctx do
      namespace = unique("workflow_keyed")
      operative_id = unique("keyed_block_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: false)
      start_workflow(namespace, operative_id, "keyed_block_workflow", first_message: "a,b,c")

      assert NodeTestHelper.wait_until(fn -> 1 in committed_tasks(operative_id) end, 30_000),
             "the keyed stage never committed"

      assert NodeTestHelper.wait_until(fn -> keyed_cells(operative_id) == ~w(a b c) end, 15_000),
             "keyed Cells on disk: #{inspect(keyed_cells(operative_id))}"

      {:ok, entries} = TaskJournal.read(operative_id)
      [ran] = for %{type: :task_committed, task_idx: 1, payload: p} <- entries, do: p.cells_ran

      assert Enum.sort(ran) == Enum.map(~w(a b c), &keyed_identity(operative_id, &1)),
             "the keyed stage's commit must name each minted Cell once"
    end

    # `keyed_fail` fails after the keyed stage committed: the rollback runs the
    # keyed hook once per key and leaves the hookless first tasklet alone.
    test "a failure compensates the keyed stage once per key", ctx do
      namespace = unique("workflow_keyed_saga")
      operative_id = unique("keyed_saga_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: true)
      start_workflow(namespace, operative_id, "keyed_saga_workflow", first_message: "a,b,c")

      await_status(operative_id, "error", 30_000)

      for key <- ~w(a b c) do
        assert File.exists?(
                 Paths.compensation_marker_path({operative_id, "keyed_commit", key}, :done)
               ),
               "the keyed Cell #{key} was not compensated"
      end

      assert comp_done_count(operative_id, "keyed_commit") == 3
      assert comp_done_count(operative_id, "key_split") == 0

      assert committed_tasks(operative_id) == [0, 1], "the failing tasklet must never commit"

      {:ok, entries} = TaskJournal.read(operative_id)

      refute Enum.any?(entries, &match?(%{type: :compensated, task_idx: 0}, &1)),
             "the hookless first tasklet was compensated"
    end
  end

  defp keyed_cells(operative_id),
    do: Enum.sort(for {_, "keyed_commit", key} <- Paths.cells_of(operative_id), do: key)

  defp keyed_identity(operative_id, key),
    do: Wire.encode_cell_id(operative_id, "keyed_commit", key)

  # The crash point of `branch_workflow`: the root and BOTH its branches have
  # committed, and only the blocked grandchild is outstanding.
  defp branch_fanned_out?(operative_id) do
    case TaskJournal.restore_state(operative_id) do
      %{committed_task_idxs: [0, 1, 2]} -> true
      _ -> false
    end
  end

  # Start `retry_workflow` on a fresh runner with `failures` armed on the flaky
  # cell, and wait for the run's terminal status.
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

  # A commit releases its input after it is acked, so the journal can show
  # the offset consumed a moment before the log stops owing it.
  defp assert_released(operative_id) do
    assert NodeTestHelper.wait_until(fn -> InputLog.undelivered(operative_id) == [] end, 10_000),
           "the log still owes #{inspect(InputLog.undelivered(operative_id))}"
  end

  # Count change-log records in a tasklet's retained WAL file(s). The WAL is
  # a stream of `<seq::64><len::32><body::len><crc::32>` records.
  defp snapshot_count(operative_id, tasklet),
    do: length(Paths.snapshot_paths({operative_id, tasklet, ""}))

  # Distinct Changes applied to the keyless Cell: the seqs still in its WAL
  # plus everything its latest snapshot folded (seq 0..snapshot_seq).
  defp change_count(operative_id, tasklet) do
    folded =
      case Paths.snapshot_paths({operative_id, tasklet, ""}) do
        [] -> MapSet.new()
        snaps -> snaps |> List.last() |> elem(0) |> then(&MapSet.new(0..&1//1))
      end

    [Paths.class_dir(operative_id), tasklet, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.flat_map(fn path -> wal_seqs(File.read!(path), []) end)
    |> MapSet.new()
    |> MapSet.union(folded)
    |> MapSet.size()
  end
end
