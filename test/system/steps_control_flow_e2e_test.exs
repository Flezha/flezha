defmodule FlezhaTest.System.StepsControlFlowE2ETest do
  @moduledoc """
  The durable control flow a structured steps relay runs, end to end
  against a live runner of the selected SDK runtime over UDS: a loop that commits per iteration,
  parks when its inbox is empty and rehydrates on the next input; a rewind that
  compensates back to an earlier iteration and bumps the epoch; the cell
  retry ladder and what it does when it exhausts; a branching tree, whose
  fan-out either joins once or unwinds every branch before their parent; a
  loop a tasklet ends with `complete`; and an unreadable message a waiting
  instance dead-letters without waking up for it again.

  Every scenario runs on the selected runtime: a steps relay's stages run through
  the core's routing, mailboxes and poll core in every SDK, and each test app
  carries the same fixtures. Tagged `:system` (boots real subprocesses) —
  excluded from the default `mix test`; run with:

      mix test --only steps_crash_resume
      mix test test/system/steps_control_flow_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :steps

  # The shared tier-e2e mechanics.
  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, kill_overwatch: 2, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Suspend
  alias Flezha.Runner.FleetManager
  alias Flezha.State.Wire
  alias Flezha.Service.Cli
  alias Flezha.Service.RelayManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.InputLog
  alias Flezha.State.TaskJournal
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @moduletag :steps_crash_resume
  # A loop, a rewind or a retry ladder runs many turns through a live runner.
  @moduletag timeout: 180_000

  describe "durable loop + suspension (uds)" do
    @describetag transport: :uds, scenario: :uds_loop

    # `steps-durable-loop-and-suspension`, end to end on a real runner:
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
      namespace = unique("steps_loop")
      relay_id = unique("chat_loop_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      # Park fast so the idle window does not dominate the test.
      {port_a, exec_a} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      info =
        start_steps(namespace, relay_id, "chat_loop_steps",
          first_message: nil,
          plugins: [forward_sink("chat_sink", collector)]
        )

      assert info.runner_id == exec_a

      # --- 1. two turns → two committed iterations, then a park ---
      send_turn(namespace, relay_id, "hello")
      send_turn(namespace, relay_id, "again")

      assert NodeTestHelper.wait_until(fn -> committed_iters(relay_id) == [0, 1] end, 30_000),
             "the loop did not commit one iteration per turn: " <>
               inspect(committed_iters(relay_id))

      assert replies(collector, 2, relay_id) == ["reply:hello", "reply:again"]

      # Each iteration recorded the input-log offset it consumed, and the
      # consumed prefix was released.
      assert consumed_offsets(relay_id) == [0, 1]
      assert_released(relay_id)

      assert NodeTestHelper.wait_until(
               fn -> Suspend.sleep_record(relay_id) != nil end,
               20_000
             ),
             "the idle loop never parked at its committed boundary"

      # Evicted from the runner, but still registered and addressable — that IS
      # suspension (zero live RUNNER resources, not zero resources).
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, relay_id) end, 20_000),
             "the parked instance was never suspended controller-side"

      assert registered?(namespace, relay_id)

      # --- 2. a message while suspended rehydrates it ---
      send_turn(namespace, relay_id, "wake-up")

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(relay_id) == [0, 1, 2] end,
               60_000
             ),
             "the suspended instance did not rehydrate and run the new turn as iteration 2"

      assert replies(collector, 3, relay_id) == [
               "reply:hello",
               "reply:again",
               "reply:wake-up"
             ]

      refute suspended?(namespace, relay_id)

      # --- 3. SIGKILL mid-iteration 3 → resume AT iteration 3 ---
      # The class blocks from its third turn on while the release env is unset,
      # so this turn hangs mid-iteration with iterations 0-2 durable.
      send_turn(namespace, relay_id, "mid-crash")

      assert NodeTestHelper.wait_until(
               fn -> match?({:ok, _floor, last} when last >= 3, InputLog.tail(relay_id)) end,
               20_000
             ),
             "the crashing turn never reached the input log"

      # Operations surface, while the turn is still in flight: it is durable and
      # marked undelivered — this is what survives the SIGKILL below.
      pending_inbox =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Steps.inbox", Jason.encode!([relay_id]), [])
        end)

      assert pending_inbox =~ "durable input log for #{relay_id}"
      assert pending_inbox =~ "consumed through 2"
      assert pending_inbox =~ "offset=3 PENDING"

      sigkill(os_pid(port_a))

      {_port_b, exec_b} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      assert exec_b != exec_a

      assert NodeTestHelper.wait_until(
               fn -> exec_a not in FleetManager.active_runners(namespace) end,
               15_000
             ),
             "dead runner was never evicted after the replacement connected"

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(relay_id) == [0, 1, 2, 3] end,
               90_000
             ),
             "the resumed loop never committed the in-flight iteration 3"

      assert replies(collector, 4, relay_id) == [
               "reply:hello",
               "reply:again",
               "reply:wake-up",
               "reply:mid-crash"
             ]

      # The committed turns did not re-run: `chat_turn` commits one keyed Change
      # per DISTINCT turn, so a re-run of iteration 0-2 would add a Change.
      # Counted compaction-proof — the boundary snapshots fold the early ones.
      assert change_count(relay_id, "chat_turn") == 4,
             "a committed iteration re-ran on resume (resume-at-iteration broken)"

      # --- operations surface ---
      journal =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Steps.journal", Jason.encode!([relay_id]), [])
        end)

      assert journal =~ "iter_committed"
      assert journal =~ "consumed input-log offset:"
      refute journal =~ "parked", "where the loop slept is not the journal's"
      assert journal =~ "task_committed task_idx=0 iter=3"

      # Every turn is consumed now, so the log has released all of them: the
      # journal keeps the offsets, the log delivers none. The file itself is
      # rewritten once the released prefix is worth its fsync (the cut
      # threshold); a few tiny turns stay on disk, never re-delivered.
      assert TaskJournal.consumed_offset(relay_id) == 3
      assert TaskJournal.released_offset(relay_id) == 3
      assert_released(relay_id)
    end

    # Suspension is durable STATE, not a live process: a controller-side restart
    # of the instance's supervisor while it is suspended must rediscover it from
    # the journal alone — still evicted, still resumable — and a message then
    # sent must rehydrate it exactly as if nothing had happened.
    test "suspension survives a controller-side restart of the instance", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_loop_boot")
      relay_id = unique("chat_loop_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      start_steps(namespace, relay_id, "chat_loop_steps",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, relay_id, "one")

      assert NodeTestHelper.wait_until(fn -> committed_iters(relay_id) == [0] end, 30_000)
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, relay_id) end, 20_000)

      # Restart the instance's controller-side supervisor while it is suspended.
      # It must come back from durable state as suspended — NOT bind a runner.
      kill_overwatch(namespace, relay_id)

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, relay_id) end, 30_000),
             "a suspended instance was not rediscovered from durable state on restart"

      # And it still wakes on input.
      send_turn(namespace, relay_id, "two")

      assert NodeTestHelper.wait_until(fn -> committed_iters(relay_id) == [0, 1] end, 60_000),
             "the rediscovered suspended instance did not rehydrate on a new message"

      assert replies(collector, 2, relay_id) == ["reply:one", "reply:two"]
    end

    # The same park/rehydrate cycle for a class with NO per-Cell state: its
    # Cell's records carry its replies and nothing to restore, so the
    # task-journal reaches the driver only because it rides `AddRelay`.
    # Delivered on a restore frame alone, turn two commits at iteration 0 again
    # and collides with iteration 0's entries.
    test "a loop with no per-Cell state resumes at its iteration and still delivers", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_loop_stateless")
      relay_id = unique("stateless_loop_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: true,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      start_steps(namespace, relay_id, "stateless_loop_steps",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, relay_id, "one")
      assert NodeTestHelper.wait_until(fn -> committed_iters(relay_id) == [0] end, 30_000)

      # The park is what makes the next turn a fresh bind.
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, relay_id) end, 20_000),
             "the idle stateless loop never parked"

      send_turn(namespace, relay_id, "two")

      assert NodeTestHelper.wait_until(fn -> committed_iters(relay_id) == [0, 1] end, 60_000),
             "the rehydrated turn did not commit as iteration 1: " <>
               inspect(committed_iters(relay_id))

      # The point of the test: the second turn's output was delivered.
      assert replies(collector, 2, relay_id) == ["reply:one", "reply:two"]
    end
  end

  describe "stopping a sleeping loop (uds)" do
    @describetag transport: :uds, scenario: :uds_stop_sleeping

    # A stop answers the same whether the loop is asleep or awake: the sleeper
    # is woken onto a runner, `StepsStop` follows the bind, and it ends
    # `cancelled` with its record and directory kept.
    test "a suspended loop is woken and ends cancelled with its directory kept", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_stop_sleeping")
      relay_id = unique("chat_loop_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      start_steps(namespace, relay_id, "chat_loop_steps",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      send_turn(namespace, relay_id, "hello")
      assert replies(collector, 1, relay_id) == ["reply:hello"]
      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, relay_id) end, 20_000)

      assert {:ok, :cooperative} = RelayManager.request_stop(namespace, relay_id)
      await_status(relay_id, "cancelled", 30_000)
      assert File.dir?(Paths.class_dir(relay_id))
    end
  end

  describe "a tasklet ends the loop (uds)" do
    # The completion is each runtime's own `ctx.complete`: every test app's
    # `chat_loop_steps` ends itself on the turn `bye`, completing with the
    # number of turns its first tasklet recorded.
    @describetag transport: :uds, scenario: :uds_complete

    test "turns then bye end the instance ok with the turn count, and it does not park after",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_bye")
      relay_id = unique("chat_loop_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      # A short idle window, so a loop that went on waiting after `bye` would
      # park inside this test rather than after it.
      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      start_steps(namespace, relay_id, "chat_loop_steps",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      for turn <- ["one", "two", "three", "bye"], do: send_turn(namespace, relay_id, turn)

      assert NodeTestHelper.wait_until(
               fn -> match?({:ok, %{status: _}}, Completion.read(relay_id)) end,
               60_000
             ),
             "the loop did not end on bye; committed iterations: " <>
               inspect(committed_iters(relay_id))

      assert {:ok, %{status: "ok", result: "3"}} = Completion.read(relay_id)

      # The completing turn still ran its whole iteration, reply included.
      assert replies(collector, 4, relay_id) ==
               ["reply:one", "reply:two", "reply:three", "reply:bye"]

      {:ok, entries} = TaskJournal.read(relay_id)
      assert committed_iters(relay_id) == [0, 1, 2, 3]
      last = entries |> Enum.filter(&(&1.type == :iter_committed)) |> List.last()
      assert last.payload.completion == "3"

      assert Suspend.sleep_record(relay_id) == nil,
             "the completed loop parked after its last turn"
    end
  end

  describe "an unreadable message (uds)" do
    # Whether the entry reads a message is each runtime's own decoder: every
    # test app's `typed_once_steps` decodes its `order` entry as a JSON
    # ticket and completes with the ticket's text.
    @describetag transport: :uds, scenario: :uds_unreadable

    test "is dead-lettered and consumed, so the waiting instance stays suspended until a readable one",
         ctx do
      namespace = unique("steps_unreadable")
      relay_id = unique("typed_once_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      Telemetry.capture([:flezha, :steps, :rehydrating],
        filter: &(&1.relay_id == relay_id),
        as: fn _e, _meas, _meta -> :rehydrating end
      )

      {_port, _exec} =
        boot_runner(namespace, ctx, id_path,
          release?: false,
          extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
        )

      start_steps(namespace, relay_id, "typed_once_steps", first_message: nil)
      send_turn(namespace, relay_id, "not json", "order")

      assert NodeTestHelper.wait_until(fn -> DlqStore.list(relay_id) != [] end, 30_000),
             "the unreadable message was never dead-lettered"

      assert NodeTestHelper.wait_until(fn -> suspended?(namespace, relay_id) end, 30_000),
             "the instance waiting for a readable message never suspended"

      # The parked message counts as consumed: the log owes nothing, so the
      # suspension holds instead of rehydrating to deliver it again.
      assert InputLog.undelivered(relay_id) == []
      refute_receive :rehydrating, 3_000
      assert suspended?(namespace, relay_id)
      assert [_one] = DlqStore.list(relay_id)

      send_turn(namespace, relay_id, ~s({"text":"after"}), "order")
      await_status(relay_id, "ok", 60_000)
      assert {:ok, %{result: "after"}} = Completion.read(relay_id)
    end
  end

  describe "loop rewind (uds)" do
    @describetag transport: :uds, scenario: :uds_rewind

    # `steps-loop-rewind`, end to end on a real runner: three chat turns
    # commit, the user rewinds to iteration 0, and
    #
    #   * iterations 2 and 1 are compensated in that order — the reply tasklet's
    #     undo handle is in Cell State, so it retracts the turns in reverse;
    #   * the loop resumes waiting at iteration 1, and the next message sent is
    #     processed as that iteration (a rewind is undo-and-continue: a committed
    #     turn's input is not retained past its commit);
    #   * the journal epoch is bumped, so the re-run of iteration 1 is a new
    #     entry rather than a re-send of the unwound one; every output is a
    #     record of the reply Cell, so what is emitted after the rewind carries a
    #     higher `flz.seq` under the same `flz.epoch`, and a guarded sink takes
    #     it as newer than the unwound run's.
    test "rewind compensates back, resumes at the rewound-to iteration, and bumps the epoch",
         ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_rewind")
      relay_id = unique("chat_rewind_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      {_port, _exec} = boot_runner(namespace, ctx, id_path, release?: false)

      start_steps(namespace, relay_id, "chat_rewind_steps",
        first_message: nil,
        plugins: [forward_sink("chat_sink", collector)]
      )

      # --- three turns → three committed iterations ---
      for turn <- ~w(alpha beta gamma), do: send_turn(namespace, relay_id, turn)

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(relay_id) == [0, 1, 2] end,
               60_000
             ),
             "the loop did not commit one iteration per turn: " <>
               inspect(committed_iters(relay_id))

      assert replies(collector, 3, relay_id) == ["reply:alpha", "reply:beta", "reply:gamma"]
      assert TaskJournal.current_epoch(relay_id) == 0
      assert consumed_offsets(relay_id) == [0, 1, 2]

      # --- rewind to iteration 0 ---
      rewind_out =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Steps.rewind", Jason.encode!([namespace, relay_id, 0]), [])
        end)

      assert rewind_out =~ "rewound to iteration 0"

      # The journal folded back: only iteration 0 is committed, and the epoch is
      # bumped so everything from here on supersedes what the unwound turns wrote.
      assert NodeTestHelper.wait_until(
               fn -> TaskJournal.current_epoch(relay_id) == 1 end,
               20_000
             )

      # --- the loop unwinds the two turns above the target ---
      # Only iteration 0 remains committed; iterations 2 and 1 are undone.
      assert NodeTestHelper.wait_until(
               fn -> TaskJournal.committed_iterations(relay_id) == [0] end,
               60_000
             ),
             "the journal did not fold back to the rewound-to iteration"

      assert NodeTestHelper.wait_until(fn -> length(collected(collector)) >= 5 end, 90_000),
             "the rewound iterations were never compensated"

      # --- the next message is processed as iteration 1 ---
      send_turn(namespace, relay_id, "delta")

      assert NodeTestHelper.wait_until(
               fn -> TaskJournal.committed_iterations(relay_id) == [0, 1] end,
               90_000
             ),
             "the resumed loop did not run the next message as iteration 1"

      outputs = await_outputs(collector, 6, relay_id)

      # Undo runs in reverse iteration order (gamma, then beta), and only then
      # does the loop resume forward. On the wire every output is a newer record
      # of the one reply Cell: one `flz.cell` and `flz.epoch`, rising `flz.seq`.
      assert Enum.map(outputs, &elem(&1, 0)) ==
               ~w(reply:alpha reply:beta reply:gamma retract:gamma retract:beta reply:delta)

      assert [{cell, "0"}] =
               outputs
               |> Enum.map(fn {_, md} -> {md["flz.cell"], md["flz.epoch"]} end)
               |> Enum.uniq()

      assert is_binary(cell)
      seqs = Enum.map(outputs, fn {_, md} -> record_seq(md) end)
      assert seqs == Enum.sort(seqs) and seqs == Enum.uniq(seqs), inspect(seqs)

      # The re-run of iteration 1 was appended under the bumped epoch beside
      # the unwound entry, not answered from it.
      {:ok, entries} = TaskJournal.read(relay_id)

      assert [0, 1] =
               for(%{type: :iter_committed, iter: 1, epoch: epoch} <- entries, do: epoch)
    end
  end

  describe "cell retry (uds)" do
    @describetag transport: :uds, scenario: :uds_retry

    # `steps-cell-retry` on a live runner: `retry_flaky` fails its first
    # attempt, the per-cell ladder re-runs it in place, and the tasklet
    # commits normally — nothing compensates and the run reports `ok`.
    test "a transient cell failure is retried and the run completes", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id = drive_retry_run(ctx, collector, "1", "ok")

      # `res:...` is task A's forward output; the terminal marker names the attempt
      # the flaky cell finally succeeded on — 2 ⇒ the ladder absorbed the
      # first failure.
      assert [{~s(res:{"seed":"go"}), _}, {marker, _}] = await_outputs(collector, 2, relay_id)
      assert marker == ~s(attempt:2:{"seed":"go"})

      {:ok, entries} = TaskJournal.read(relay_id)
      committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx
      assert committed == [0, 1], "the retried tasklet must still commit its boundary"

      refute Enum.any?(entries, &(&1.type == :compensated)),
             "a recovered retry must not compensate"
    end

    # Arming more failures than the ladder has attempts exhausts it: the original
    # error is terminal, task A rolls back (its undo record is journaled, then released),
    # and the run reports `error` — the pre-change failure path, unchanged.
    test "a persistent cell failure exhausts the ladder, compensates and errors", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id = drive_retry_run(ctx, collector, "99", "error")

      assert [{~s(res:{"seed":"go"}), _}, {"res:undone", _}] =
               await_outputs(collector, 2, relay_id)

      {:ok, entries} = TaskJournal.read(relay_id)

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

    # `steps-branching` on a live runner. BranchA fans out to BranchB and
    # BranchC; BranchB feeds BranchD, which blocks. The SIGKILL therefore lands
    # with A, B and C committed and only D's subtree outstanding — and the
    # respawn must re-run D ALONE, seeded from B's journaled edge.
    test "a fan-out runs to DONE, and a crash re-runs only the uncommitted subtree", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id =
        drive_crash_resume(
          ctx,
          "branch_steps",
          "ok",
          &branch_fanned_out?/1,
          plugins: [forward_sink("branch_sink", collector)]
        )

      delivered = collector |> await_outputs(4, relay_id) |> Enum.map(&elem(&1, 0))

      assert Enum.sort(delivered) == [
               "a:#{@seed}",
               "b:#{@seed}",
               "c:#{@seed}",
               "d:#{@seed}"
             ]

      assert Enum.count(delivered, &(&1 == "c:#{@seed}")) == 1,
             "the sibling branch had already committed: it must not re-run"

      committed = committed_tasks(relay_id)

      # The branches commit on their own, in whichever order they finish.
      assert hd(committed) == 0 and Enum.sort(committed) == [0, 1, 2, 3],
             "each tasklet commits exactly once across the crash: #{inspect(committed)}"

      # The SDK declares the leaf set, so the controller derives DONE from the
      # journal alone — no runner to ask.
      leaf_tasks = ClassRegistry.leaf_tasks(relay_id)
      assert leaf_tasks == [2, 3], "BranchC and BranchD are the tree's leaves"
      assert TaskJournal.complete?(relay_id, leaf_tasks)

      rendered =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Steps.journal", Jason.encode!([relay_id]), [])
        end)

      assert rendered =~ "leaves: 2,3  complete: true"
      assert rendered =~ "edge: channel_id="
    end

    # A failure under one branch, after BOTH branches committed: the rollback is
    # reverse-topological — the sibling leaf and the failing branch's committed
    # tasklet unwind before the root they share.
    test "a failure under one branch unwinds both branches before their parent", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      namespace = unique("steps_branch")
      relay_id = unique("branch_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: true,
        extra_env: [{"FLEZHA_BRANCH_FAIL", "1"}]
      )

      start_steps(namespace, relay_id, "branch_steps",
        plugins: [forward_sink("branch_sink", collector)]
      )

      await_status(relay_id, "error", 30_000)

      # Forward output of the three completed tasklets, then their undo — the
      # branches in reverse of whichever committed first, the shared root last.
      delivered = collector |> await_outputs(6, relay_id) |> Enum.map(&elem(&1, 0))
      {branches, [root]} = delivered |> Enum.drop(3) |> Enum.split(2)

      assert Enum.sort(branches) == ["b:undo-b", "undo-c"] and root == "a:undo-a",
             "descendants before ancestors: #{inspect(delivered)}"

      {:ok, entries} = TaskJournal.read(relay_id)
      committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx

      assert hd(committed) == 0 and Enum.sort(committed) == [0, 1, 2],
             "the failing leaf never commits: #{inspect(committed)}"

      compensated = for %{type: :compensated, task_idx: idx} <- entries, do: idx
      assert Enum.sort(compensated) == [0, 1, 2]

      refute TaskJournal.complete?(relay_id, ClassRegistry.leaf_tasks(relay_id)),
             "a leaf that never committed leaves the relay incomplete"
    end
  end

  describe "a keyed steps tasklet (uds)" do
    @describetag transport: :uds, scenario: :uds_keyed

    # A `key_by` into `keyed_commit` mints one Cell per key of the entry's list,
    # read on the controller's durable view: the third tasklet blocks (no
    # release), and a live read would wait behind it on a runner whose owner
    # process serves reads and stage pushes in turn.
    test "N keys run N Cells, and the keyed stage's commit names each once", ctx do
      namespace = unique("steps_keyed")
      relay_id = unique("keyed_block_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: false)
      start_steps(namespace, relay_id, "keyed_block_steps", first_message: "a,b,c")

      assert NodeTestHelper.wait_until(fn -> 1 in committed_tasks(relay_id) end, 30_000),
             "the keyed stage never committed"

      assert NodeTestHelper.wait_until(fn -> keyed_cells(relay_id) == ~w(a b c) end, 15_000),
             "keyed Cells on disk: #{inspect(keyed_cells(relay_id))}"

      {:ok, entries} = TaskJournal.read(relay_id)
      [ran] = for %{type: :task_committed, task_idx: 1, payload: p} <- entries, do: p.cells_ran

      assert ran |> Enum.map(& &1.cell_id) |> Enum.sort() ==
               Enum.map(~w(a b c), &keyed_identity(relay_id, &1)),
             "the keyed stage's commit must name each minted Cell once"
    end

    # `keyed_fail` fails after the keyed stage committed: the rollback runs the
    # keyed hook once per key and leaves the hookless first tasklet alone.
    test "a failure compensates the keyed stage once per key", ctx do
      namespace = unique("steps_keyed_saga")
      relay_id = unique("keyed_saga_steps")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: true)
      start_steps(namespace, relay_id, "keyed_saga_steps", first_message: "a,b,c")

      await_status(relay_id, "error", 30_000)

      for key <- ~w(a b c) do
        assert File.exists?(
                 Paths.compensation_marker_path({relay_id, "keyed_commit", key}, :done)
               ),
               "the keyed Cell #{key} was not compensated"
      end

      assert comp_done_count(relay_id, "keyed_commit") == 3
      assert comp_done_count(relay_id, "key_split") == 0

      assert committed_tasks(relay_id) == [0, 1], "the failing tasklet must never commit"

      {:ok, entries} = TaskJournal.read(relay_id)

      refute Enum.any?(entries, &match?(%{type: :compensated, task_idx: 0}, &1)),
             "the hookless first tasklet was compensated"
    end
  end

  # The record a released output came from, the first half of its `flz.seq`.
  defp record_seq(md), do: md["flz.seq"] |> String.split(".") |> hd() |> String.to_integer()

  defp keyed_cells(relay_id),
    do: Enum.sort(for {_, "keyed_commit", key} <- Paths.cells_of(relay_id), do: key)

  defp keyed_identity(relay_id, key),
    do: Wire.encode_cell_id(relay_id, "keyed_commit", key)

  # The crash point of `branch_steps`: the root and BOTH its branches have
  # committed, and only the blocked grandchild is outstanding.
  defp branch_fanned_out?(relay_id) do
    case TaskJournal.restore_state(relay_id) do
      %{committed_task_idxs: [0, 1, 2]} -> true
      _ -> false
    end
  end

  # Start `retry_steps` on a fresh runner with `failures` armed on the flaky
  # cell, and wait for the run's terminal status.
  defp drive_retry_run(ctx, collector, failures, terminal_status) do
    namespace = unique("steps_retry")
    relay_id = unique("retry_steps")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {_port, _exec} =
      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_STEPS_RETRY_FAILS", failures}]
      )

    start_steps(namespace, relay_id, "retry_steps",
      plugins: [forward_sink("retry_sink", collector)]
    )

    await_status(relay_id, terminal_status, 30_000)

    relay_id
  end

  # --- durable-loop helpers ---------------------------------------------------

  # The sink's collector accumulates, so each wait is for the running total.
  defp replies(collector, count, relay_id) do
    collector |> await_outputs(count, relay_id) |> Enum.map(fn {payload, _md} -> payload end)
  end

  # A commit releases its input after it is acked, so the journal can show
  # the offset consumed a moment before the log stops owing it.
  defp assert_released(relay_id) do
    assert NodeTestHelper.wait_until(fn -> InputLog.undelivered(relay_id) == [] end, 10_000),
           "the log still owes #{inspect(InputLog.undelivered(relay_id))}"
  end

  # Distinct Changes applied to the keyless Cell: the seqs still in its WAL
  # plus everything its latest snapshot folded (seq 0..snapshot_seq).
  defp change_count(relay_id, tasklet) do
    folded =
      case Paths.snapshot_paths({relay_id, tasklet, ""}) do
        [] -> MapSet.new()
        snaps -> snaps |> List.last() |> elem(0) |> then(&MapSet.new(0..&1//1))
      end

    [Paths.class_dir(relay_id), tasklet, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.flat_map(fn path -> wal_seqs(File.read!(path), []) end)
    |> MapSet.new()
    |> MapSet.union(folded)
    |> MapSet.size()
  end
end
