defmodule FlezhaTest.System.WorkflowCrashResumeE2ETest do
  @moduledoc """
  End-to-end crash-resume proof for the durable workflow operative model
  (`workflow-operative-functionality` §7c.3), against a live runner of the
  selected SDK runtime over both UDS and QUIC.

  Two real-wire scenarios per transport:

    * **resume** — a durable structured workflow operative (`commit_block_workflow`)
      commits Cell state in tasklet A, then blocks in tasklet B. The
      runner is SIGKILLed mid-run (task A durable, task B never finished), a
      replacement runner is started on the same stable identity, and the
      operative's controller-side supervisor is forced to re-bind onto it. The
      replacement restores the committed state and runs to completion — proving
      resume-not-restart with a terminal outcome matching a clean run.

    * **compensation** — a durable saga workflow operative (`saga_block_workflow`)
      completes A and B, then fails in C, triggering reverse-order rollback. B's
      compensation-done marker lands, then A's `compensate` blocks; the runner
      is SIGKILLed mid-compensation. The replacement ships the durable
      compensation markers back and finishes only the outstanding hook (A),
      skipping the already-done B — proving crash-mid-compensation resume.

  ## Crash-resume mechanics (why each step is here)

  `convergence_health_enabled` is `false` in the test config, so nothing
  auto-rebinds a crashed operative — the test drives it:

    * A shared `FLEZHA_RUNNER_ID_PATH` makes the replacement re-present the
      dead runner's durable stable identity, so its connect swaps the bindings
      and evicts the dead session on both transports (no reliance on detecting
      the SIGKILL, which QUIC surfaces only after its ~45 s idle timeout).
    * `Process.exit(overwatch_pid, :kill)` forces a fresh controller-side bind:
      ProcessHub transient-restarts the supervisor, which re-resolves onto the
      live replacement and re-issues `AddOperative` with `resume_cell_count > 0`
      — the only path that arms the SDK's restore-before-run gate. The hard kill
      bypasses `terminate/2`, so the WAL survives (a graceful stop would delete it).
    * The replacement is launched WITH `FLEZHA_WORKFLOW_E2E_RELEASE`, so the
      blocking tasklet/compensation that parked on the first runner completes
      on the resume, making the crash window deterministic (no wall-clock races).

  Tagged `:system` (boots real subprocesses) — excluded from the default
  `mix test`; run with:

      mix test --only workflow_crash_resume
      mix test test/system/workflow_crash_resume_e2e_test.exs --include system
  """

  use FlezhaTest.SystemCase, capability: :workflow

  # The shared tier-e2e mechanics.
  import FlezhaTest.Helper.TierE2EHarness,
    only: [cleanup: 3, kill_overwatch: 2, os_pid: 1, sigkill: 1, unique: 1, unique_id_path: 0]

  import FlezhaTest.Helper.WorkflowResumeHarness

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Replay
  alias Flezha.Runner.FleetManager
  alias Flezha.State.Completion
  alias Flezha.Service.CliManager
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :workflow_crash_resume
  # Two live runner lifecycles + a SIGKILL + a resume per test; QUIC first
  # connects can be slow, so give each scenario a generous budget.
  @moduletag timeout: 180_000

  for transport <- NodeTestHelper.transports() do
    describe "transport=#{transport}" do
      @describetag transport: transport
      @describetag scenario: :"crash_#{transport}"

      test "durable workflow resumes from committed Cell state after a SIGKILL", ctx do
        # Mid-run point: task A committed durable Cell state (the spec's
        # precondition); task B is then blocking, so the operative can only
        # complete after resuming on the replacement. On rust the point also
        # requires task A's journaled task-commit — the WAL flush and the TaskCommit
        # ack are independent, and the skip assertion below needs the journal
        # entry durable before the SIGKILL.
        operative_id =
          drive_crash_resume(ctx, "commit_block_workflow", "ok", fn oid ->
            Replay.resume_cell_count(:workflow, oid) > 0 and
              TaskJournal.restore_state(oid) != nil
          end)

        # workflow-task-journal: task A's completion was journaled at the task barrier
        # before the crash, so the resume SKIPS it — its WAL keeps exactly the
        # pre-crash record (a redo would append a second one) and task B runs
        # seeded from the journaled handoff. Both SDKs honour this contract.
        assert wal_record_count(operative_id, "commit_a") == 1,
               "journaled task A was re-run on resume (redo-vs-skip broken)"

        {:ok, entries} = TaskJournal.read(operative_id)
        committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx

        assert 0 in committed, "task A's pre-crash commit is missing from the journal"
        assert 1 in committed, "resumed task B never committed its boundary"

        # Operator surface: `flz state journal <instance>` renders the entries.
        rendered =
          ExUnit.CaptureIO.capture_io(fn ->
            CliManager.state_journal(operative_id)
          end)

        assert rendered =~ "task_committed"
        assert rendered =~ "task_idx=0"
      end

      test "durable saga workflow resumes compensation after a SIGKILL mid-rollback", ctx do
        # Mid-run point: A+B completed and C failed → reverse rollback; B's fast
        # compensation recorded a done marker, then A's compensate blocked — the
        # operative is durably mid-compensation.
        operative_id =
          drive_crash_resume(ctx, "saga_block_workflow", "error", fn oid ->
            comp_done_count(oid) >= 1
          end)

        # Both compensable stages are undone: A's outstanding hook finished on
        # the resume, joining B's pre-crash done marker.
        assert comp_done_count(operative_id) >= 2,
               "resume did not finish the outstanding compensation hook"
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

        operative_id =
          drive_crash_resume(
            ctx,
            "diamond_workflow",
            "ok",
            &diamond_half_committed?/1,
            plugins: [forward_sink("diamond_sink", collector)]
          )

        seed = ~s({"seed":"go"})
        delivered = collector |> await_outputs(5, operative_id) |> Enum.map(&elem(&1, 0))

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

        {:ok, entries} = TaskJournal.read(operative_id)
        committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx

        assert Enum.sort(committed) == [0, 1, 2, 3, 4],
               "each node commits exactly once across the crash — the join never twice: " <>
                 inspect(committed)

        assert ClassRegistry.leaf_tasks(operative_id) == [4],
               "the join is the diamond's single leaf"

        assert TaskJournal.complete?(operative_id, [4])
      end

      # The same diamond with NO crash: one runner, the release env set, so
      # nothing resumes and no journal is replayed. It runs beside the crash
      # scenario because the fan-out shape is what the join's payload count
      # proves, and a crash-only test invites the reading that a doubled join
      # is a resume defect — it is not. `java-diamond-resume`: a channel's
      # consumer count SHALL NOT multiply the value its producer released.
      test "a fan-in diamond joins once with no crash at all", ctx do
        collector = spawn_link(fn -> collect_loop([]) end)
        namespace = unique("workflow_nocrash")
        operative_id = unique("diamond_workflow")
        id_path = unique_id_path()

        on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

        # The release env is what lets DiamB2 through on the FIRST runner, so
        # the chain completes without ever being killed.
        {_port, _exec} = boot_runner(namespace, ctx, id_path, release?: true)

        start_workflow(namespace, operative_id, "diamond_workflow",
          plugins: [forward_sink("diamond_sink", collector)]
        )

        await_status(operative_id, "ok", 60_000)

        seed = ~s({"seed":"go"})
        delivered = collector |> await_outputs(5, operative_id) |> Enum.map(&elem(&1, 0))

        assert Enum.sort(delivered) ==
                 Enum.sort([
                   "a:#{seed}",
                   "b:#{seed}",
                   "c:#{seed}",
                   "j:b:#{seed}",
                   "j:c:#{seed}"
                 ]),
               "the fan-out multiplied the released batch: #{inspect(delivered)}"

        {:ok, entries} = TaskJournal.read(operative_id)
        committed = for %{type: :task_committed, task_idx: idx} <- entries, do: idx

        assert Enum.sort(committed) == [0, 1, 2, 3, 4],
               "each node commits exactly once on a clean run: " <> inspect(committed)
      end
    end
  end

  describe "opted-out class (reference/uds)" do
    @describetag transport: :uds, scenario: :uds_optout, reference_runtime: true

    # workflow-task-journal: a `PERSISTENCE_ENABLED = false` class skips the
    # task-commit handshake entirely — no journal, no WAL — and a crash-resume
    # keeps the re-run-from-top contract: with nothing restored and nothing
    # journaled, the only way the chain completes on the replacement is a
    # fresh run of task A feeding task B. Task A's sink marker is both the
    # pre-crash mid-run signal and the proof of that re-run: one marker per
    # run, and the second one is owed BEFORE the completed instance is
    # deregistered — the teardown flushes the runner's stream channels, so an
    # output the transport has already delivered is never torn down under.
    test "opted-out workflow writes no journal and re-runs from the top", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id =
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

      assert {:ok, []} = TaskJournal.read(operative_id)
      refute File.exists?(Paths.journal_path(operative_id))

      # The controller journals the message it forwarded, in the instance's
      # inbox; the class's own state is what an opted-out class never writes.
      assert [Paths.class_dir(operative_id), "**", "*.wal"]
             |> Path.join()
             |> Path.wildcard()
             |> Enum.reject(&(&1 == Paths.inbox_path(operative_id))) == [],
             "an opted-out class must write no WAL"
    end
  end

  describe "a mid-iteration resume of a message-fed pass (reference/uds)" do
    @describetag transport: :uds, scenario: :uds_message_resume, reference_runtime: true

    # `workflow-durable-loop`: the runner dies after the root's `task_committed`
    # and before the pass's `iter_committed`. The controller re-forwards the
    # message that pass had taken, and the resumed pass takes it as its own:
    # the message drives exactly one iteration, sealed with its offset, and
    # the next iteration waits for a new message rather than running over the
    # same one again.
    test "the re-forwarded message drives one iteration after the resume, never two", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)
      namespace = unique("workflow_msg_resume")
      operative_id = unique("reply_block_loop_workflow")
      id_path = unique_id_path()

      on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

      {port_a, exec_a} = boot_runner(namespace, ctx, id_path, release?: false)

      info =
        start_workflow(namespace, operative_id, "reply_block_loop_workflow",
          first_message: nil,
          plugins: [forward_sink("reply_sink", collector)]
        )

      assert info.runner_id == exec_a

      # The turn commits at the root, then the reply blocks until the release.
      send_turn(namespace, operative_id, "reply-crash")

      assert NodeTestHelper.wait_until(
               fn -> root_committed_without_iteration?(operative_id) end,
               30_000
             ),
             "the root never committed ahead of the iteration on the first runner"

      refute Completion.exists?(operative_id)

      sigkill(os_pid(port_a))
      {_port_b, exec_b} = boot_runner(namespace, ctx, id_path, release?: true)
      assert exec_b != exec_a, "replacement must mint a fresh session token"

      assert NodeTestHelper.wait_until(
               fn -> exec_a not in FleetManager.active_runners(namespace) end,
               15_000
             ),
             "dead runner was never evicted after the replacement connected"

      kill_overwatch(namespace, operative_id)

      assert NodeTestHelper.wait_until(fn -> committed_iters(operative_id) == [0] end, 60_000),
             "the resumed pass did not commit iteration 0: " <>
               inspect(committed_iters(operative_id))

      assert consumed_offsets(operative_id) == [1],
             "the resumed pass did not seal with its message's offset"

      # A second message is the next iteration; the crashed turn's reply is
      # seen exactly once.
      send_turn(namespace, operative_id, "next")

      assert NodeTestHelper.wait_until(
               fn -> committed_iters(operative_id) == [0, 1] end,
               60_000
             ),
             "the next message did not run as iteration 1: " <>
               inspect(committed_iters(operative_id))

      assert consumed_offsets(operative_id) == [1, 2]

      assert collector |> await_outputs(2, operative_id) |> Enum.map(&elem(&1, 0)) ==
               ["reply:reply-crash", "reply:next"]
    end
  end

  describe "transactional outbox (reference/uds)" do
    @describetag transport: :uds, scenario: :uds_outbox, reference_runtime: true

    # The chain's args are the serialized `init_values` the harness starts with.
    @a_out ~s(a-out:{"seed":"go"})
    @undo_forward ~s(out:{"seed":"go"})

    # workflow-transactional-outbox: task A's plugin output is STAGED — it reaches
    # the sink only after the controller journaled A's `task_committed` entry (with
    # the output inside it) and fsynced. A crash after that flush must not
    # re-deliver it: the `outbox_flushed` marker gates the restore re-drive.
    test "staged output is flushed once after its commit and never re-delivered", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id =
        drive_crash_resume(
          ctx,
          "outbox_block_workflow",
          "ok",
          &committed_and_flushed?/1,
          plugins: [forward_sink("outbox_sink", collector)]
        )

      delivered = await_outputs(collector, 1, operative_id)

      assert [{@a_out, md}] = delivered,
             "task A's staged output must reach the sink exactly once: #{inspect(delivered)}"

      # Minted from the journal: producing cell, task index + per-task sequence,
      # instance epoch — the effectively-once output-identity shape.
      assert md["flz.cell"] == "#{operative_id}/commit_a/"
      assert md["flz.seq"] == "0.0"
      assert md["flz.epoch"] == "0"
      assert md["channel"] == "mid"

      assert TaskJournal.pending_entries(operative_id) == [],
             "the instance still owes outbox entries after completing"

      {:ok, entries} = TaskJournal.read(operative_id)

      assert Enum.any?(
               entries,
               &match?(
                 %{type: :outbox_flushed, task_idx: 0, payload: %{for_type: :task_committed}},
                 &1
               )
             ),
             "task A's flush was never recorded"

      # Operator surface: the journal renders the outbox and its flush marker.
      rendered =
        ExUnit.CaptureIO.capture_io(fn ->
          CliManager.state_journal(operative_id)
        end)

      assert rendered =~ "outbox: #{operative_id}/commit_a/ seq_in_task=0 channel=mid"
      assert rendered =~ "flushes: task_committed"
    end

    # The crash-before-flush window: the `task_committed` record (output inside)
    # is durable, its `outbox_flushed` marker never landed — exactly what a
    # controller crash between the commit fsync and the flush leaves behind.
    # Restore must re-drive the delivery with the ORIGINAL keys.
    test "a commit whose flush never landed re-drives on restore with identical keys", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id =
        drive_crash_resume(
          ctx,
          "outbox_block_workflow",
          "ok",
          &committed_and_flushed?/1,
          plugins: [forward_sink("outbox_sink", collector)],
          before_rebind: &drop_flush_markers/1
        )

      delivered = await_outputs(collector, 2, operative_id)

      assert [{@a_out, first}, {@a_out, second}] = delivered,
             "the un-flushed commit was not re-driven on restore: #{inspect(delivered)}"

      # The key is a pure function of the journal, so the re-drive is byte-exact:
      # a dedup-capable sink absorbs it, an append sink sees at-least-once.
      assert identity(first) == identity(second)
      assert identity(second)["flz.seq"] == "0.0"

      # The re-drive recorded its own flush marker, so it is owed no more.
      assert TaskJournal.pending_entries(operative_id) == []
    end

    # workflow-operative-saga + outbox: a `compensate` hook's plugin output is
    # staged too. On the first runner the hook emits and then BLOCKS, so the
    # SIGKILL lands with the undo emitted but never journaled — it must not leak.
    # The resumed runner re-runs the outstanding hook (its done marker never
    # landed), journals the undo in a `compensated` entry, and flushes it once
    # under a comp-marked key that cannot collide with the forward output.
    test "a compensation's undo output is staged, journaled, and delivered once", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id =
        drive_crash_resume(
          ctx,
          "undo_outbox_workflow",
          "error",
          &committed_and_flushed?/1,
          plugins: [forward_sink("undo_sink", collector)]
        )

      delivered = await_outputs(collector, 2, operative_id)

      assert [{@undo_forward, forward}, {"out:undone", undo}] = delivered,
             "expected the forward output then exactly one undo: #{inspect(delivered)}"

      # Same cell, same tasklet, same per-task sequence — only the comp
      # marker keeps the undo's key off the forward output it reverses.
      assert identity(forward)["flz.seq"] == "0.0"
      assert identity(undo)["flz.seq"] == "0.0.1"
      assert identity(undo)["flz.cell"] == identity(forward)["flz.cell"]

      {:ok, entries} = TaskJournal.read(operative_id)

      assert [%{type: :compensated, task_idx: 0, payload: %{outbox: [_]}}] =
               Enum.filter(entries, &(&1.type == :compensated)),
             "the undo output was not journaled exactly once"

      assert Enum.any?(
               entries,
               &match?(%{type: :outbox_flushed, payload: %{for_type: :compensated}}, &1)
             ),
             "the undo flush was never recorded"

      assert comp_done_count(operative_id) == 1, "the hook ran for exactly one cell"
      assert TaskJournal.pending_entries(operative_id) == []
    end
  end

  # The crash point of `diamond_workflow`: the root, the blocked arm's upper
  # half (B1) and the fast arm (C) have committed while the blocked producer
  # (B2, task 3) is mid-run — the join therefore has exactly one journaled and
  # one outstanding contribution.
  defp diamond_half_committed?(operative_id) do
    case TaskJournal.restore_state(operative_id) do
      %{committed_task_idxs: idxs} -> Enum.sort(idxs) == [0, 1, 2]
      _ -> false
    end
  end

  # The durable mid-run point for the outbox scenarios: task A's boundary is
  # journaled AND its outbox is recorded flushed (the two are separate records,
  # so waiting on the commit alone would race the flush).

  # The durable mid-run point for the outbox scenarios: task A's boundary is
  # journaled AND its outbox is recorded flushed (the two are separate records,
  # so waiting on the commit alone would race the flush).
  defp committed_and_flushed?(operative_id) do
    TaskJournal.restore_state(operative_id) != nil and
      TaskJournal.pending_entries(operative_id) == []
  end

  # Roll the journal back to the bytes it held before its first `outbox_flushed`
  # record: the commit is durable, the flush marker is not. Truncating at a
  # record boundary is precisely what a crash produces. The live journal writer
  # is closed first so it does not append past the truncation point.
  defp drop_flush_markers(operative_id) do
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(Paths.journal_id(operative_id))
    WalWriter.close(pid)

    path = Paths.journal_path(operative_id)
    bytes = File.read!(path)
    kept = truncate_at_flush_marker(bytes, 0)

    assert kept < byte_size(bytes), "no outbox_flushed record to drop — the flush never happened"
    File.write!(path, binary_part(bytes, 0, kept))
  end

  # Byte offset of the first `outbox_flushed` (entry type 2) record. WAL framing
  # is `<<seq::64, len::32, body::len, crc::32>>`; the body is a 16-byte header
  # followed by the journal entry, whose first byte is the type.
  defp truncate_at_flush_marker(
         <<_seq::64-big, len::32-big, body::binary-size(len), _crc::32-big, rest::binary>>,
         offset
       ) do
    case body do
      <<_header::binary-size(16), 2::8, _::binary>> -> offset
      _ -> truncate_at_flush_marker(rest, offset + 8 + 4 + len + 4)
    end
  end

  defp truncate_at_flush_marker(_, offset), do: offset

  # The identity a flushed outbox entry is keyed by.
  defp identity(md), do: Map.take(md, ["flz.cell", "flz.seq", "flz.epoch", "channel"])

  # Number of per-Cell compensation-done markers the controller has written
  # for this operative (one per compensated Cell).
  defp comp_done_count(operative_id) do
    [Paths.class_dir(operative_id), "**", "*.comp_done"]
    |> Path.join()
    |> Path.wildcard()
    |> length()
  end

  defp wal_record_count(operative_id, tasklet) do
    [Paths.class_dir(operative_id), tasklet, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn path -> length(wal_seqs(File.read!(path), [])) end)
    |> Enum.sum()
  end
end
