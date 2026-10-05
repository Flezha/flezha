defmodule FlezhaTest.System.StepsCrashResumeE2ETest do
  @moduledoc """
  End-to-end crash-resume proof for the durable steps relay model
  (`relay-functionality` §7c.3), against a live runner of the
  selected SDK runtime over both UDS and QUIC.

  Two real-wire scenarios per transport:

    * **resume** — a durable structured steps relay (`commit_block_steps`)
      commits Cell state in tasklet A, then blocks in tasklet B. The
      runner is SIGKILLed mid-run (task A durable, task B never finished), a
      replacement runner is started on the same stable identity, and its
      connect binds the relay onto it. The replacement restores the committed state and runs to completion — proving
      resume-not-restart with a terminal outcome matching a clean run.

    * **compensation** — a durable saga steps relay (`saga_block_steps`)
      completes A and B, then fails in C, triggering reverse-order rollback. B's
      compensation-done marker lands, then A's `compensate` blocks; the runner
      is SIGKILLed mid-compensation. The replacement ships the durable
      compensation markers back and finishes only the outstanding hook (A),
      skipping the already-done B — proving crash-mid-compensation resume.

  ## Crash-resume mechanics (why each step is here)

  `convergence_health_enabled` is `false` in the test config, so nothing
  auto-rebinds a crashed relay — the test drives it:

    * A shared `FLEZHA_RUNNER_ID_PATH` makes the replacement re-present the
      dead runner's durable stable identity with a new incarnation, so its
      connect evicts the dead session and binds the relay onto it with
      `resume_cell_count > 0`, the journal and the input tail, on both
      transports (no reliance on detecting the SIGKILL, which QUIC surfaces
      only after its ~45 s idle timeout).
    * One scenario gives the replacement a new identity instead: the
      convergence loop it starts is then what moves the relay.
    * The replacement is launched WITH `FLEZHA_STEPS_E2E_RELEASE`, so the
      blocking tasklet/compensation that parked on the first runner completes
      on the resume, making the crash window deterministic (no wall-clock races).

  Tagged `:system` (boots real subprocesses) — excluded from the default
  `mix test`; run with:

      mix test --only steps_crash_resume
      mix test test/system/steps_crash_resume_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :steps

  # The shared tier-e2e mechanics.
  import FlezhaTest.Helper.TierE2EHarness,
    only: [
      cleanup: 3,
      kill_overwatch: 2,
      os_pid: 1,
      sigkill: 1,
      unique: 1,
      unique_id_path: 0
    ]

  import FlezhaTest.Helper.StepsResumeHarness

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Replay
  alias Flezha.Runner.FleetManager
  alias Flezha.State.Completion
  alias Flezha.Service.Cli
  alias Flezha.State.Paths
  alias Flezha.SDK.Wire
  alias Flezha.State.Outbox
  alias Flezha.State.TaskJournal
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :steps_crash_resume
  # Two live runner lifecycles + a SIGKILL + a resume per test; QUIC first
  # connects can be slow, so give each scenario a generous budget.
  @moduletag timeout: 180_000

  for transport <- NodeTestHelper.transports() do
    describe "transport=#{transport}" do
      @describetag transport: transport
      @describetag scenario: :"crash_#{transport}"

      test "durable steps resumes from committed Cell state after a SIGKILL", ctx do
        # Mid-run point: task A committed durable Cell state (the spec's
        # precondition); task B is then blocking, so the relay can only
        # complete after resuming on the replacement. On rust the point also
        # requires task A's journaled task-commit — the WAL flush and the TaskCommit
        # ack are independent, and the skip assertion below needs the journal
        # entry durable before the SIGKILL.
        relay_id =
          drive_crash_resume(ctx, "commit_block_steps", "ok", fn oid ->
            Replay.resume_cell_count(oid) > 0 and
              TaskJournal.restore_state(oid) != nil
          end)

        # steps-task-journal: task A's completion was journaled at the task barrier
        # before the crash, so the resume SKIPS it — its WAL keeps exactly the
        # pre-crash record (a redo would append a second one) and task B runs
        # seeded from the journaled handoff. Both SDKs honour this contract.
        assert wal_record_count(relay_id, "commit_a") == 1,
               "journaled task A was re-run on resume (redo-vs-skip broken)"

        committed = committed_tasks(relay_id)

        assert 0 in committed, "task A's pre-crash commit is missing from the journal"
        assert 1 in committed, "resumed task B never committed its boundary"

        # Operations surface: `flz state journal <instance>` renders the entries.
        rendered =
          ExUnit.CaptureIO.capture_io(fn ->
            Cli.call("Steps.journal", Jason.encode!([relay_id]), [])
          end)

        assert rendered =~ "task_committed"
        assert rendered =~ "task_idx=0"
      end

      test "durable saga steps resumes compensation after a SIGKILL mid-rollback", ctx do
        # Mid-run point: A+B completed and C failed → reverse rollback; B's fast
        # compensation recorded a done marker, then A's compensate blocked — the
        # relay is durably mid-compensation.
        relay_id =
          drive_crash_resume(ctx, "saga_block_steps", "error", fn oid ->
            comp_done_count(oid) >= 1
          end)

        # Both compensable stages are undone: A's outstanding hook finished on
        # the resume, joining B's pre-crash done marker.
        assert comp_done_count(relay_id) >= 2,
               "resume did not finish the outstanding compensation hook"
      end

      # The crash lands with the three keyed Cells persisted and the keyed stage
      # journaled; the resume restores them before the pass goes on, and none
      # re-runs: each keyed Cell's WAL keeps its one pre-crash record.
      test "a crash restores the keyed Cells before the pass resumes", ctx do
        relay_id =
          drive_crash_resume(
            ctx,
            "keyed_block_steps",
            "ok",
            fn oid ->
              Replay.resume_cell_count(oid) >= 3 and
                match?(%{committed_task_idxs: [0, 1 | _]}, TaskJournal.restore_state(oid))
            end,
            first_message: "a,b,c"
          )

        for key <- ~w(a b c) do
          path = Paths.wal_path({relay_id, "keyed_commit", key})
          assert length(wal_seqs(File.read!(path), [])) == 1, "keyed Cell #{key} re-ran"
        end
      end

      # `sdk-graph-authoring` fan-in on a live runner: the diamond
      # A→(B1→B2, C)→Join. DiamB2 BLOCKS until the release env — one level
      # below its sibling arm, because siblings commit at one level barrier —
      # so the SIGKILL lands with A, B1 and C committed and B2 mid-run. The
      # respawn re-runs B2 ALONE, and the join runs exactly once — with C's
      # JOURNALED handoff banked and B2's fresh output appended. One contract,
      # both SDKs.
      test "a fan-in diamond joins once, and a crash re-runs only the uncommitted producer",
           ctx do
        collector = spawn_link(fn -> collect_loop([]) end)

        relay_id =
          drive_crash_resume(
            ctx,
            "diamond_steps",
            "ok",
            &diamond_half_committed?/1,
            plugins: [forward_sink("diamond_sink", collector)]
          )

        assert_diamond_resumed(collector, relay_id)
      end

      # The same diamond with NO crash: one runner, the release env set, so
      # nothing resumes and no journal is replayed. It runs beside the crash
      # scenario because the fan-out shape is what the join's payload count
      # proves, and a crash-only test invites the reading that a doubled join
      # is a resume defect — it is not. `java-diamond-resume`: a channel's
      # consumer count SHALL NOT multiply the value its producer released.
      test "a fan-in diamond joins once with no crash at all", ctx do
        collector = spawn_link(fn -> collect_loop([]) end)
        namespace = unique("steps_nocrash")
        relay_id = unique("diamond_steps")
        id_path = unique_id_path()

        on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

        # The release env is what lets DiamB2 through on the FIRST runner, so
        # the chain completes without ever being killed.
        {_port, _exec} = boot_runner(namespace, ctx, id_path, release?: true)

        start_steps(namespace, relay_id, "diamond_steps",
          plugins: [forward_sink("diamond_sink", collector)]
        )

        await_status(relay_id, "ok", 60_000)

        seed = ~s({"seed":"go"})
        delivered = collector |> await_outputs(5, relay_id) |> Enum.map(&elem(&1, 0))

        assert Enum.sort(delivered) ==
                 Enum.sort([
                   "a:#{seed}",
                   "b:#{seed}",
                   "c:#{seed}",
                   "j:b:#{seed}",
                   "j:c:#{seed}"
                 ]),
               "the fan-out multiplied the released batch: #{inspect(delivered)}"

        committed = committed_tasks(relay_id)

        assert Enum.sort(committed) == [0, 1, 2, 3, 4],
               "each node commits exactly once on a clean run: " <> inspect(committed)
      end
    end
  end

  describe "opted-out class (uds)" do
    @describetag transport: :uds, scenario: :uds_optout

    # steps-task-journal: a `durable: false` class skips the
    # task-commit handshake entirely — no journal, no WAL — and a crash-resume
    # keeps the re-run-from-top contract: with nothing restored and nothing
    # journaled, the only way the chain completes on the replacement is a
    # fresh run of task A feeding task B. Task A's sink marker is both the
    # pre-crash mid-run signal and the proof of that re-run: one marker per
    # run, and the second one is owed BEFORE the completed instance is
    # deregistered — the teardown flushes the runner's stream channels, so an
    # output the transport has already delivered is never torn down under.
    test "opted-out steps writes no journal and re-runs from the top", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id =
        drive_crash_resume(
          ctx,
          "optout_commit_block",
          "ok",
          fn _oid ->
            Enum.any?(collected(collector), &String.starts_with?(&1, "a-ran:"))
          end,
          plugins: [forward_sink("optout_sink", collector)]
        )

      # `drive_crash_resume/5` has already seen the instance deregistered, so
      # both runs' markers must be in hand by now: the first run's (the mid-run
      # signal) and the resumed run's.
      markers = Enum.filter(collected(collector), &String.starts_with?(&1, "a-ran:"))

      assert length(markers) == 2,
             "the sink must see task A's marker once per run, both delivered " <>
               "before the teardown: #{inspect(markers)}"

      assert {:ok, []} = TaskJournal.read(relay_id)
      refute File.exists?(Paths.journal_path(relay_id))

      # The class's own state is what an opted-out class never writes.
      assert [Paths.class_dir(relay_id), "**", "*.wal"]
             |> Path.join()
             |> Path.wildcard() == [],
             "an opted-out class must write no WAL"
    end
  end

  describe "a timer-fed pass (uds)" do
    @describetag transport: :uds, scenario: :uds_timer_resume

    # `timer_loop_steps` parks the firing at the gate its first runner was
    # booted with: inside `on_timer`, before `tick` commits, or in `tock`,
    # after it.
    test "a crash before the firing's root commits fires the timer again", ctx do
      assert_fired_once_after_crash(ctx, kill_overwatch?: false)
    end

    test "an Overwatch killed mid-bind fires the restored timer once", ctx do
      assert_fired_once_after_crash(ctx, kill_overwatch?: true)
    end

    defp assert_fired_once_after_crash(ctx, opts) do
      {collector, id, at} = crash_timer_pass(ctx, "timer", &(timer_commits(&1) == []), opts)

      assert NodeTestHelper.wait_until(
               fn -> collected(collector) == ["tock:armed:#{at}", "tock:fired:#{at}"] end,
               60_000
             ),
             "the restored timer did not fire into a fresh pass: #{inspect(collected(collector))}"

      Process.sleep(1_500)
      assert collected(collector) == ["tock:armed:#{at}", "tock:fired:#{at}"]
      assert timer_commits(id) == [at]
    end

    test "a crash after the firing's root committed does not fire it again", ctx do
      {collector, id, at} = crash_timer_pass(ctx, "tock", &(timer_commits(&1) != []))

      assert NodeTestHelper.wait_until(
               fn -> collected(collector) == ["tock:armed:#{at}", "tock:fired:#{at}"] end,
               60_000
             ),
             "`tock` did not resume from `tick`'s commit: #{inspect(collected(collector))}"

      Process.sleep(1_500)
      assert collected(collector) == ["tock:armed:#{at}", "tock:fired:#{at}"]
      assert timer_commits(id) == [at], "the committed firing did not run again"
    end
  end

  describe "a crashed runner's instance reaches its replacement (uds)" do
    @describetag transport: :uds, scenario: :uds_rebind

    # The replacement has an identity of its own, so the convergence loop's
    # rebind is what moves the instance: it must resume, not start again.
    test "a convergence rebind onto another runner resumes, not restarts", ctx do
      ConvergenceSnapshot.reset_recovery_states()
      start_supervised!({Flezha.Convergence.Loop, tick_ms: 1_000})
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id =
        drive_crash_resume(ctx, "diamond_steps", "ok", &diamond_half_committed?/1,
          plugins: [forward_sink("diamond_sink", collector)],
          rebind: :heal
        )

      assert_diamond_resumed(collector, relay_id)
    end
  end

  # The diamond after a resume: the join accumulates both producers, the
  # producer committed before the crash never ran again, and each node
  # committed once.
  defp assert_diamond_resumed(collector, relay_id) do
    seed = ~s({"seed":"go"})
    delivered = collector |> await_outputs(5, relay_id) |> Enum.map(&elem(&1, 0))

    assert Enum.sort(delivered) ==
             Enum.sort([
               "a:#{seed}",
               "b:#{seed}",
               "c:#{seed}",
               "j:b:#{seed}",
               "j:c:#{seed}"
             ]),
           "the join accumulates BOTH producers' contributions: #{inspect(delivered)}"

    assert Enum.count(delivered, &(&1 == "c:#{seed}")) == 1,
           "the committed producer must not re-run"

    committed = committed_tasks(relay_id)

    assert Enum.sort(committed) == [0, 1, 2, 3, 4],
           "each node commits exactly once across the crash — the join never twice: " <>
             inspect(committed)

    assert ClassRegistry.leaf_tasks(relay_id) == [4],
           "the join is the diamond's single leaf"

    assert TaskJournal.complete?(relay_id, [4])
  end

  # Arm a timer on a runner that parks its firing at `hold`, SIGKILL it once
  # `ready?` holds on the journal, and rebind onto a released replacement.
  defp crash_timer_pass(ctx, hold, ready?, opts \\ []) do
    collector = spawn_link(fn -> collect_loop([]) end)
    namespace = unique("timer_resume")
    id = unique("timer_loop_steps")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, id, id_path) end)

    {port_a, runner_a} =
      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_STEPS_E2E_HOLD", hold}]
      )

    start_steps(namespace, id, "timer_loop_steps",
      first_message: nil,
      plugins: [forward_sink("out", collector)]
    )

    send_turn(namespace, id, "after:200")
    assert NodeTestHelper.wait_until(fn -> collected(collector) != [] end, 30_000)
    ["tock:armed:" <> at] = collected(collector)
    at = String.to_integer(at)

    # Past the deadline the firing is parked at the gate.
    assert NodeTestHelper.wait_until(
             fn -> System.system_time(:millisecond) > at + 500 and ready?.(id) end,
             30_000
           ),
           "the firing never reached its crash point"

    sigkill(os_pid(port_a))
    boot_runner(namespace, ctx, id_path, release?: true)
    if opts[:kill_overwatch?], do: kill_overwatch(namespace, id)

    assert NodeTestHelper.wait_until(
             fn -> runner_a not in FleetManager.active_runners(namespace) end,
             15_000
           )

    {collector, id, at}
  end

  # The scheduled timestamps the journal's roots of timer-fed passes name.
  defp timer_commits(id) do
    {:ok, entries} = TaskJournal.read(id)

    for %{type: :task_committed, payload: %{trigger: %{scheduled_at: at}}} <- entries, do: at
  end

  describe "a mid-iteration resume of a message-fed pass (uds)" do
    @describetag transport: :uds, scenario: :uds_message_resume

    # `steps-durable-loop`: the runner dies after the root's `task_committed`
    # and before the pass's `iter_committed`. The controller re-forwards the
    # message that pass had taken, and the resumed pass takes it as its own:
    # the message drives exactly one iteration, sealed with its offset, and
    # the next iteration waits for a new message rather than running over the
    # same one again.
    test "the re-forwarded message drives one iteration after the resume, never two", ctx do
      message_resume(ctx, kill_overwatch?: false)
    end

    # The bind the replacement's connect starts is still restoring and
    # re-forwarding when its Overwatch is killed; the restarted Overwatch binds
    # again, and the tail reaches the driver twice.
    test "an Overwatch killed mid-bind leaves the message one iteration", ctx do
      message_resume(ctx, kill_overwatch?: true)
    end
  end

  defp message_resume(ctx, opts) do
    collector = spawn_link(fn -> collect_loop([]) end)
    namespace = unique("steps_msg_resume")
    relay_id = unique("reply_block_loop_steps")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {port_a, exec_a} = boot_runner(namespace, ctx, id_path, release?: false)

    info =
      start_steps(namespace, relay_id, "reply_block_loop_steps",
        first_message: nil,
        plugins: [forward_sink("reply_sink", collector)]
      )

    assert info.runner_id == exec_a

    # The turn commits at the root, then the reply blocks until the release.
    send_turn(namespace, relay_id, "reply-crash")

    assert NodeTestHelper.wait_until(
             fn -> root_committed_without_iteration?(relay_id) end,
             30_000
           ),
           "the root never committed ahead of the iteration on the first runner"

    refute Completion.exists?(relay_id)

    sigkill(os_pid(port_a))
    {_port_b, exec_b} = boot_runner(namespace, ctx, id_path, release?: true)
    if opts[:kill_overwatch?], do: kill_overwatch(namespace, relay_id)
    assert exec_b != exec_a, "replacement must mint a fresh session token"

    assert NodeTestHelper.wait_until(
             fn -> exec_a not in FleetManager.active_runners(namespace) end,
             15_000
           ),
           "dead runner was never evicted after the replacement connected"

    assert NodeTestHelper.wait_until(fn -> committed_iters(relay_id) == [0] end, 60_000),
           "the resumed pass did not commit iteration 0: " <>
             inspect(committed_iters(relay_id))

    assert consumed_offsets(relay_id) == [0],
           "the resumed pass did not seal with its message's offset"

    # A second message is the next iteration; the crashed turn's reply is
    # seen exactly once.
    send_turn(namespace, relay_id, "next")

    assert NodeTestHelper.wait_until(
             fn -> committed_iters(relay_id) == [0, 1] end,
             60_000
           ),
           "the next message did not run as iteration 1: " <>
             inspect(committed_iters(relay_id))

    assert consumed_offsets(relay_id) == [0, 1]

    assert collector |> await_outputs(2, relay_id) |> Enum.map(&elem(&1, 0)) ==
             ["reply:reply-crash", "reply:next"]
  end

  describe "a steps output's identity (uds)" do
    @describetag transport: :uds, scenario: :uds_outbox

    # The chain's first message, as the harness starts it.
    @a_out ~s(a-out:{"seed":"go"})
    @undo_forward ~s(out:{"seed":"go"})

    # A stage's output rides its Cell's effect record; the stage's journal entry
    # names that record by seq and channel, and the Cell's writer releases the
    # output once the entry is durable, stamped from the record.
    test "a steps output carries its Cell record's identity, delivered once", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id =
        drive_crash_resume(
          ctx,
          "outbox_block_steps",
          "ok",
          &committed_and_delivered?/1,
          plugins: [forward_sink("outbox_sink", collector)]
        )

      [{_seq, record}] = effect_records(relay_id, "commit_a")
      assert record.changes != [], "the change and the output are one record"
      assert [%{channel: "mid", key: "k", metadata: [{"trace", "t1"}]}] = record.outputs

      %{cells_ran: [cell]} = stage_entry(relay_id, 0)
      seq = cell.seq
      assert {cell.first_seq, cell.channels} == {seq, ["mid"]}

      delivered = await_outputs(collector, 1, relay_id)

      assert [{@a_out, md}] = delivered,
             "task A's output must reach the sink exactly once: #{inspect(delivered)}"

      assert md["flz.cell"] == "#{relay_id}/commit_a/"
      assert md["flz.seq"] == "#{seq}.0"
      assert md["flz.epoch"] == "0"
      assert md["flz.key"] == "k"
      assert md["trace"] == "t1"
      assert md["channel"] == "mid"

      rendered =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Steps.journal", Jason.encode!([relay_id]), [])
        end)

      assert rendered =~ "ran: #{relay_id}/commit_a/ record=#{seq} channels=mid"
      refute rendered =~ "outbox:"
      refute rendered =~ "flushes:"
    end

    # The crash-before-flush window, from the Cell's side: the entry is
    # durable, the flush cursor never passed the record. The rebind's writer
    # re-drives it with the identity it had.
    test "a crash before the flush re-drives the output with identical stamps", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id =
        drive_crash_resume(
          ctx,
          "outbox_block_steps",
          "ok",
          &committed_and_delivered?/1,
          plugins: [forward_sink("outbox_sink", collector)],
          before_rebind: &rewind_flush_cursor/1
        )

      delivered = await_outputs(collector, 2, relay_id)

      assert [{@a_out, first}, {@a_out, second}] = delivered,
             "the output was not re-driven on the rebind: #{inspect(delivered)}"

      assert identity(first) == identity(second)
      assert second["flz.key"] == "k"
    end

    # A compensation hook's undo output is the compensating Cell's own record,
    # named by the `compensated` entry. On the first runner the hook blocks, so
    # the SIGKILL lands before it wrote anything; the resumed runner re-runs it
    # (its done marker never landed) and the undo is delivered once, as a newer
    # record of the same Cell.
    test "a compensation's undo output rides its Cell's record and is delivered once", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      relay_id =
        drive_crash_resume(
          ctx,
          "undo_outbox_steps",
          "error",
          &committed_and_delivered?/1,
          plugins: [forward_sink("undo_sink", collector)]
        )

      delivered = await_outputs(collector, 2, relay_id)

      assert [{@undo_forward, forward}, {"out:undone", undo}] = delivered,
             "expected the forward output then exactly one undo: #{inspect(delivered)}"

      assert undo["flz.cell"] == forward["flz.cell"]
      assert seq_of(undo) > seq_of(forward), "the undo is a newer record of the same Cell"

      {:ok, entries} = TaskJournal.read(relay_id)

      assert [%{type: :compensated, task_idx: 0, payload: %{cells_ran: [%{channels: ["mid"]}]}}] =
               Enum.filter(entries, &(&1.type == :compensated)),
             "the undo record was not journaled exactly once"

      assert comp_done_count(relay_id) == 1, "the hook ran for exactly one cell"
    end

    # The window this change closes: with the runner's flush linger raised, a
    # stage's record must still be durable before the entry naming it.
    test "the record precedes the entry naming it under a long flush linger", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_linger")
      relay_id = unique("outbox_block_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      boot_runner(namespace, ctx, id_path,
        release?: true,
        extra_env: [{"FLEZHA_FLUSH_LINGER_MS", "20000"}]
      )

      start_steps(namespace, relay_id, "outbox_block_steps",
        plugins: [forward_sink("outbox_sink", collector)]
      )

      assert NodeTestHelper.wait_until(fn -> 0 in committed_tasks(relay_id) end, 30_000)
      %{cells_ran: [%{seq: seq}]} = stage_entry(relay_id, 0)

      assert seq in Enum.map(effect_records(relay_id, "commit_a"), &elem(&1, 0)),
             "the journal names record #{seq}, which the Cell's WAL does not hold"

      await_status(relay_id, "ok", 30_000)
      assert [{@a_out, _md}] = await_outputs(collector, 1, relay_id)
    end

    # A stage that fails commits nothing: no marker names the record its Cell
    # wrote before the failure, so that record's output never leaves.
    test "a failed stage delivers nothing, though its Cell wrote a record", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("steps_failed_stage")
      relay_id = unique("failed_stage_steps")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

      boot_runner(namespace, ctx, id_path, release?: true)

      start_steps(namespace, relay_id, "failed_stage_steps",
        plugins: [forward_sink("lost_sink", collector)],
        first_message: "a,b"
      )

      await_status(relay_id, "error", 30_000)

      assert [{_seq, %{outputs: [%{channel: "mid"}]}}] =
               effect_records(relay_id, "failed_stage"),
             "the failed stage's Cell never wrote the record whose output must not leave"

      refute 1 in committed_tasks(relay_id)
      assert outboxes_settled?(relay_id)
      assert collected(collector) == [], "a failed stage's output reached the sink"
    end
  end

  # The crash point of `diamond_steps`: the root, the blocked arm's upper
  # half (B1) and the fast arm (C) have committed while the blocked producer
  # (B2, task 3) is mid-run — the join therefore has exactly one journaled and
  # one outstanding contribution.
  defp diamond_half_committed?(relay_id) do
    case TaskJournal.restore_state(relay_id) do
      %{committed_task_idxs: idxs} -> Enum.sort(idxs) == [0, 1, 2]
      _ -> false
    end
  end

  # The durable mid-run point for the output scenarios: task A's entry is
  # journaled and the output its record carries has been confirmed by the sink
  # (its Cell's writer owes nothing and its cursor passed the record).
  defp committed_and_delivered?(relay_id) do
    case TaskJournal.read(relay_id) do
      {:ok, [%{type: :task_committed, payload: %{cells_ran: cells}} | _]} ->
        Enum.all?(cells, fn
          %{seq: nil} -> true
          %{cell_id: id, seq: seq} -> cursor_past?(id, seq)
        end)

      _ ->
        false
    end
  end

  defp cursor_past?(cell_id, seq) do
    {:ok, cell} = Wire.decode_cell_id(cell_id)

    case Flezha.State.WriterRegistry.lookup(cell) do
      {:ok, writer} ->
        match?(
          %{pending: 0, awaiting: 0, outbox_flushed_seq: flushed} when flushed >= seq,
          WalWriter.outbox_status(writer)
        )

      :error ->
        Outbox.read(cell).outbox_flushed_seq >= seq
    end
  end

  # The state a controller crash between the sink's confirmation and the
  # cursor's write leaves: `commit_a`'s cursor back below its record. The live
  # writer is closed first so it does not write the cursor forward again.
  defp rewind_flush_cursor(relay_id) do
    cell = {relay_id, "commit_a", ""}

    with {:ok, writer} <- Flezha.State.WriterRegistry.lookup(cell) do
      WalWriter.close(writer)

      assert NodeTestHelper.wait_until(
               fn -> Flezha.State.WriterRegistry.lookup(cell) == :error end,
               2_000
             )
    end

    dedup = Outbox.read(cell)
    assert dedup.outbox_flushed_seq >= 0, "the output was never confirmed before the crash"
    :ok = Outbox.write(cell, %{dedup | outbox_flushed_seq: -1})
  end

  # The one `task_committed` entry of `task_idx`.
  defp stage_entry(relay_id, task_idx) do
    {:ok, entries} = TaskJournal.read(relay_id)
    [entry] = for %{type: :task_committed, task_idx: ^task_idx} = e <- entries, do: e
    entry.payload
  end

  # Every effect record in `tasklet`'s single Cell WAL, `{seq, record}`.
  defp effect_records(relay_id, tasklet) do
    [Paths.class_dir(relay_id), tasklet, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.flat_map(fn path ->
      {:ok, records} =
        Flezha.SDK.RecordFrame.fold_bytes(File.read!(path), {:log, "effect_records"}, [], fn
          {seq, header, body, _raw}, acc ->
            case Wire.decode_effect_record(header, body) do
              nil -> {:cont, acc}
              record -> {:cont, [{seq, record} | acc]}
            end
        end)

      Enum.reverse(records)
    end)
  end

  # The identity a released output is keyed by.
  defp identity(md), do: Map.take(md, ["flz.cell", "flz.seq", "flz.epoch", "flz.key", "channel"])

  defp seq_of(md), do: md["flz.seq"] |> String.split(".") |> Enum.map(&String.to_integer/1)

  defp wal_record_count(relay_id, tasklet) do
    [Paths.class_dir(relay_id), tasklet, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn path -> length(wal_seqs(File.read!(path), [])) end)
    |> Enum.sum()
  end
end
