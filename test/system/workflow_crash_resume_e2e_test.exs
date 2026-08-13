defmodule FlezhaTest.System.WorkflowCrashResumeE2ETest do
  @moduledoc """
  End-to-end crash-resume proof for the durable workflow operative model
  (`workflow-operative-functionality` §7c.3), against live Rust and Java
  runners over both UDS and QUIC.

  Two real-wire scenarios per (runtime × transport) cell:

    * **resume** — a durable structured workflow operative (`commit_block_workflow`)
      commits Entity state in procedure A, then blocks in procedure B. The
      runner is SIGKILLed mid-run (proc A durable, proc B never finished), a
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
      live replacement and re-issues `AddOperative` with `resume_entity_count > 0`
      — the only path that arms the SDK's restore-before-run gate. The hard kill
      bypasses `terminate/2`, so the WAL survives (a graceful stop would delete it).
    * The replacement is launched WITH `FLEZHA_WORKFLOW_E2E_RELEASE`, so the
      blocking procedure/compensation that parked on the first runner completes
      on the resume, making the crash window deterministic (no wall-clock races).

  Tagged `:system` (boots real subprocesses) — excluded from the default
  `mix test`; run with:

      mix test --only workflow_crash_resume
      mix test test/system/workflow_crash_resume_e2e_test.exs --include system
  """

  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias Flezha.State.Registry
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :system
  @moduletag :workflow_crash_resume
  # Two live runner lifecycles + a SIGKILL + a resume per test; QUIC first
  # connects can be slow, so give each cell a generous budget.
  @moduletag timeout: 180_000

  setup context do
    runtime = context[:runtime]

    if runtime == :java do
      jar = Path.join(File.cwd!(), "test/apps/java_runner/dist/java_runner.jar")

      unless File.exists?(jar) do
        ExUnit.Assertions.flunk(
          "Java compute engine jar missing at #{jar}. Refresh it with `scripts/build-dist.sh`."
        )
      end
    end

    :ok
  end

  for runtime <- [:rust, :java], transport <- [:uds, :quic] do
    describe "runtime=#{runtime}, transport=#{transport}" do
      @describetag runtime: runtime, transport: transport
      @describetag cell: :"#{runtime}_#{transport}"

      test "durable workflow resumes from committed Entity state after a SIGKILL", ctx do
        # Mid-run point: proc A committed durable Entity state (the spec's
        # precondition); proc B is then blocking, so the operative can only
        # complete after resuming on the replacement. On rust the point also
        # requires proc A's journaled proc-commit — the WAL flush and the ProcCommit
        # ack are independent, and the skip assertion below needs the journal
        # entry durable before the SIGKILL.
        operative_id =
          drive_crash_resume(ctx, "commit_block_workflow", "ok", fn oid ->
            Flezha.Operative.Replay.resume_entity_count(:workflow, oid) > 0 and
              Flezha.State.ProcJournal.restore_state(oid) != nil
          end)

        # workflow-proc-journal: proc A's completion was journaled at the proc barrier
        # before the crash, so the resume SKIPS it — its WAL keeps exactly the
        # pre-crash record (a redo would append a second one) and proc B runs
        # seeded from the journaled handoff. Both SDKs honour this contract.
        assert wal_record_count(operative_id, "commit_a") == 1,
               "journaled proc A was re-run on resume (redo-vs-skip broken)"

        {:ok, entries} = Flezha.State.ProcJournal.read(operative_id)
        committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx

        assert 0 in committed, "proc A's pre-crash commit is missing from the journal"
        assert 1 in committed, "resumed proc B never committed its boundary"

        # Operator surface: `flz state journal <instance>` renders the entries.
        rendered =
          ExUnit.CaptureIO.capture_io(fn ->
            Flezha.Service.CliManager.state_journal(operative_id)
          end)

        assert rendered =~ "proc_committed"
        assert rendered =~ "proc_idx=0"
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
        delivered = collector |> await_outputs(5) |> Enum.map(&elem(&1, 0))

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

        {:ok, entries} = ProcJournal.read(operative_id)
        committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx

        assert Enum.sort(committed) == [0, 1, 2, 3, 4],
               "each node commits exactly once across the crash — the join never twice: " <>
                 inspect(committed)

        assert ClassRegistry.leaf_procs(operative_id) == [4],
               "the join is the diamond's single leaf"

        assert ProcJournal.complete?(operative_id, [4])
      end
    end
  end

  describe "effectively-once tier (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_tier

    # §6.2: a tier workflow operative crashes mid-run and resumes; every keyed
    # output reaches the conforming sink with the durable identity stamps, the
    # Model-A re-run re-emits a byte-identical payload under the stable key,
    # and the changelog upsert converges to exactly one value — never lost,
    # never divergent.
    test "tier workflow crash-resume converges the conforming changelog sink", ctx do
      sink = %Flezha.Plugin{
        id: "tier_sink",
        namespace: nil,
        operative_id: nil,
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self(), include_metadata: true},
        subscriptions: [{:on_output, []}]
      }

      _operative_id =
        drive_crash_resume(
          ctx,
          "tier_commit_block_workflow",
          "ok",
          fn oid -> Flezha.Operative.Replay.resume_entity_count(:workflow, oid) > 0 end,
          plugins: [sink]
        )

      # The run is already complete; only the final flush's casts are in flight.
      outputs = await_tier_outputs(1)

      assert outputs != [], "no tier output reached the sink"

      for {msg, md} <- outputs do
        assert md["flz.key"] == "job"
        assert md["flz.epoch"], "tier output missing its epoch stamp"
        assert md["flz.seq"], "tier output missing its durable outbox seq"
        assert String.starts_with?(msg, "committed:")
      end

      # Deterministic re-run: the pre-crash emit and the resume's re-emit are
      # byte-identical, so the changelog upsert converges to one value.
      assert outputs |> Enum.map(fn {msg, _} -> msg end) |> Enum.uniq() |> length() == 1,
             "re-run emitted a divergent payload: #{inspect(outputs)}"

      changelog =
        Enum.reduce(outputs, %{}, fn {msg, md}, acc -> Map.put(acc, md["flz.key"], msg) end)

      assert map_size(changelog) == 1

      # Exact-identity dedup holds within an identity: no identity ever
      # carries two different payloads.
      outputs
      |> Enum.group_by(fn {_m, md} -> {md["flz.entity"], md["flz.seq"], md["flz.epoch"]} end)
      |> Enum.each(fn {identity, group} ->
        payloads = group |> Enum.map(fn {m, _} -> m end) |> Enum.uniq()
        assert length(payloads) == 1, "identity #{inspect(identity)} diverged"
      end)
    end
  end

  describe "opted-out class (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_optout

    # workflow-proc-journal: a `PERSISTENCE_ENABLED = false` class skips the
    # proc-commit handshake entirely — no journal, no WAL — and a crash-resume
    # keeps the re-run-from-top contract: with nothing restored and nothing
    # journaled, the only way the chain completes on the replacement is a
    # fresh run of proc A feeding proc B. (Proc A's sink marker is used only as the
    # pre-crash mid-run signal — plain workflow outputs are best-effort during
    # a rebind, so the resumed run's marker delivery is not asserted.)
    test "opted-out workflow writes no journal and re-runs from the top", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      sink = %Flezha.Plugin{
        id: "optout_sink",
        namespace: nil,
        operative_id: nil,
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: collector, include_metadata: true},
        subscriptions: [{:on_output, []}]
      }

      operative_id =
        drive_crash_resume(
          ctx,
          "optout_commit_block",
          "ok",
          fn _oid ->
            Enum.any?(collected(collector), &String.starts_with?(&1, "a-ran:"))
          end,
          plugins: [sink]
        )

      assert {:ok, []} = Flezha.State.ProcJournal.read(operative_id)
      refute File.exists?(Flezha.State.Registry.journal_path(operative_id))

      assert [Registry.state_root(), operative_id, "**", "*.wal"]
             |> Path.join()
             |> Path.wildcard() == [],
             "an opted-out class must write no WAL"
    end
  end

  describe "transactional outbox (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_outbox

    # The chain's args are the serialized `init_values` the harness starts with.
    @a_out ~s(a-out:{"seed":"go"})
    @undo_forward ~s(out:{"seed":"go"})

    # workflow-transactional-outbox: proc A's plugin output is STAGED — it reaches
    # the sink only after the controller journaled A's `proc_committed` entry (with
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

      delivered = await_outputs(collector, 1)

      assert [{@a_out, md}] = delivered,
             "proc A's staged output must reach the sink exactly once: #{inspect(delivered)}"

      # Minted from the journal: producing entity, proc index + per-proc sequence,
      # instance epoch — the effectively-once output-identity shape.
      assert md["flz.entity"] == "#{operative_id}/commit_a/"
      assert md["flz.seq"] == "0.0"
      assert md["flz.epoch"] == "0"
      assert md["channel"] == "mid"

      assert ProcJournal.pending_entries(operative_id) == [],
             "the instance still owes outbox entries after completing"

      {:ok, entries} = ProcJournal.read(operative_id)

      assert Enum.any?(
               entries,
               &match?(
                 %{type: :outbox_flushed, proc_idx: 0, payload: %{for_type: :proc_committed}},
                 &1
               )
             ),
             "proc A's flush was never recorded"

      # Operator surface: the journal renders the outbox and its flush marker.
      rendered =
        ExUnit.CaptureIO.capture_io(fn ->
          Flezha.Service.CliManager.state_journal(operative_id)
        end)

      assert rendered =~ "outbox: #{operative_id}/commit_a/ seq_in_proc=0 channel=mid"
      assert rendered =~ "flushes: proc_committed"
    end

    # The crash-before-flush window: the `proc_committed` record (output inside)
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

      delivered = await_outputs(collector, 2)

      assert [{@a_out, first}, {@a_out, second}] = delivered,
             "the un-flushed commit was not re-driven on restore: #{inspect(delivered)}"

      # The key is a pure function of the journal, so the re-drive is byte-exact:
      # a dedup-capable sink absorbs it, an append sink sees at-least-once.
      assert identity(first) == identity(second)
      assert identity(second)["flz.seq"] == "0.0"

      # The re-drive recorded its own flush marker, so it is owed no more.
      assert ProcJournal.pending_entries(operative_id) == []
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

      delivered = await_outputs(collector, 2)

      assert [{@undo_forward, forward}, {"out:undone", undo}] = delivered,
             "expected the forward output then exactly one undo: #{inspect(delivered)}"

      # Same entity, same procedure, same per-proc sequence — only the comp
      # marker keeps the undo's key off the forward output it reverses.
      assert identity(forward)["flz.seq"] == "0.0"
      assert identity(undo)["flz.seq"] == "0.0.1"
      assert identity(undo)["flz.entity"] == identity(forward)["flz.entity"]

      {:ok, entries} = ProcJournal.read(operative_id)

      assert [%{type: :compensated, proc_idx: 0, payload: %{outbox: [_]}}] =
               Enum.filter(entries, &(&1.type == :compensated)),
             "the undo output was not journaled exactly once"

      assert Enum.any?(
               entries,
               &match?(%{type: :outbox_flushed, payload: %{for_type: :compensated}}, &1)
             ),
             "the undo flush was never recorded"

      assert comp_done_count(operative_id) == 1, "the hook ran for exactly one entity"
      assert ProcJournal.pending_entries(operative_id) == []
    end
  end

  # The durable mid-run point for the outbox scenarios: proc A's boundary is
  # journaled AND its outbox is recorded flushed (the two are separate records,
  # so waiting on the commit alone would race the flush).
  defp committed_and_flushed?(operative_id) do
    ProcJournal.restore_state(operative_id) != nil and
      ProcJournal.pending_entries(operative_id) == []
  end

  # Roll the journal back to the bytes it held before its first `outbox_flushed`
  # record: the commit is durable, the flush marker is not. Truncating at a
  # record boundary is precisely what a crash produces. The live journal writer
  # is closed first so it does not append past the truncation point.
  defp drop_flush_markers(operative_id) do
    {:ok, pid} = Flezha.State.Supervisor.find_or_start(Registry.journal_id(operative_id))
    Flezha.State.WalWriter.close(pid)

    path = Registry.journal_path(operative_id)
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

  defp collect_loop(acc) do
    receive do
      {:message_forward, msg, md} -> collect_loop([{to_string(msg), md} | acc])
      {:collected?, from} -> send(from, {:collected, acc}) && collect_loop(acc)
    end
  end

  defp collected(collector), do: collector |> collected_with_meta() |> Enum.map(&elem(&1, 0))

  # Everything the sink received, oldest first, with the frame metadata (the
  # `flz.*` identity stamps a flushed outbox entry carries).
  defp collected_with_meta(collector) do
    send(collector, {:collected?, self()})

    receive do
      {:collected, acc} -> Enum.reverse(acc)
    after
      1_000 -> []
    end
  end

  defp forward_sink(id, collector) do
    %Flezha.Plugin{
      id: id,
      namespace: nil,
      operative_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: collector, include_metadata: true},
      subscriptions: [{:on_output, []}]
    }
  end

  # The identity a flushed outbox entry is keyed by.
  defp identity(md), do: Map.take(md, ["flz.entity", "flz.seq", "flz.epoch", "channel"])

  defp drain_tier_outputs(acc) do
    receive do
      {:message_forward, msg, md} -> drain_tier_outputs([{to_string(msg), md} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Wait for the sink to receive at least `count` frames, then settle briefly and
  # take everything. `drive_crash_resume` already waited for the terminal
  # completion, so only the final flush's casts are in flight — but under a loaded
  # box that can take longer than any fixed sleep. The settle window means an
  # unexpected EXTRA delivery still fails the exact-count assertions.
  defp await_outputs(collector, count) do
    wait_until(fn -> length(collected(collector)) >= count end, 30_000)
    Process.sleep(500)
    collected_with_meta(collector)
  end

  # Same deadline-then-settle wait for the tier sink, which forwards into this
  # process's mailbox rather than a collector.
  defp await_tier_outputs(count) do
    deadline = System.monotonic_time(:millisecond) + 30_000
    await_tier_outputs(drain_tier_outputs([]), count, deadline)
  end

  defp await_tier_outputs(acc, count, deadline) do
    cond do
      length(acc) >= count ->
        Process.sleep(500)
        acc ++ drain_tier_outputs([])

      System.monotonic_time(:millisecond) >= deadline ->
        acc

      true ->
        Process.sleep(100)
        await_tier_outputs(acc ++ drain_tier_outputs([]), count, deadline)
    end
  end

  describe "durable loop + suspension (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_loop

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

      assert wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 30_000),
             "the loop did not commit one iteration per turn: " <>
               inspect(committed_iters(operative_id))

      assert replies(collector, 2) == ["reply:hello", "reply:again"]

      # Each iteration recorded the inbox offset it consumed, and the consumed
      # prefix was released.
      assert consumed_offsets(operative_id) == [1, 2]
      assert ProcInbox.undelivered(operative_id) == []

      assert wait_until(fn -> ProcJournal.restore_state(operative_id).parked end, 20_000),
             "the idle loop never parked at its committed boundary"

      # Evicted from the runner, but still registered and addressable — that IS
      # suspension (zero live RUNNER resources, not zero resources).
      assert wait_until(fn -> suspended?(namespace, operative_id) end, 20_000),
             "the parked instance was never suspended controller-side"

      assert registered?(namespace, operative_id)

      # --- 2. a message while suspended rehydrates it ---
      send_turn(namespace, operative_id, "wake-up")

      assert wait_until(fn -> committed_iters(operative_id) == [0, 1, 2] end, 60_000),
             "the suspended instance did not rehydrate and run the new turn as iteration 2"

      assert replies(collector, 3) == ["reply:hello", "reply:again", "reply:wake-up"]
      refute suspended?(namespace, operative_id)

      # --- 3. SIGKILL mid-iteration 3 → resume AT iteration 3 ---
      # The class blocks from its third turn on while the release env is unset,
      # so this turn hangs mid-iteration with iterations 0-2 durable.
      send_turn(namespace, operative_id, "mid-crash")

      assert wait_until(fn -> ProcInbox.last_offset(operative_id) >= 4 end, 20_000),
             "the crashing turn never reached the durable inbox"

      # Operator surface, while the turn is still in flight: it is durable and
      # marked undelivered — this is what survives the SIGKILL below.
      pending_inbox =
        ExUnit.CaptureIO.capture_io(fn ->
          Flezha.Service.CliManager.state_inbox(operative_id)
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

      assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end),
             "dead runner was never evicted after the replacement connected"

      force_fresh_rebind(namespace, operative_id)

      assert wait_until(fn -> committed_iters(operative_id) == [0, 1, 2, 3] end, 90_000),
             "the resumed loop never committed the in-flight iteration 3"

      assert replies(collector, 4) == [
               "reply:hello",
               "reply:again",
               "reply:wake-up",
               "reply:mid-crash"
             ]

      # The committed turns did not re-run: `chat_turn` commits one keyed Change
      # per DISTINCT turn, so a re-run of iteration 0-2 would leave extra WAL
      # records behind. Four turns ran in total, exactly once each.
      assert wal_record_count(operative_id, "chat_turn") == 4,
             "a committed iteration re-ran on resume (resume-at-iteration broken)"

      # --- operator surface ---
      journal =
        ExUnit.CaptureIO.capture_io(fn ->
          Flezha.Service.CliManager.state_journal(operative_id)
        end)

      assert journal =~ "iter_committed"
      assert journal =~ "consumed inbox offset:"
      assert journal =~ "parked"
      assert journal =~ "proc_committed proc_idx=0 iter=3"

      # Every turn is consumed now, so the inbox has released all of them: the
      # journal keeps the offsets, the inbox keeps no payloads.
      assert ProcJournal.consumed_offset(operative_id) == 4
      assert ProcInbox.undelivered(operative_id) == []
      assert ProcInbox.read(operative_id) == []
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

      assert wait_until(fn -> committed_iters(operative_id) == [0] end, 30_000)
      assert wait_until(fn -> suspended?(namespace, operative_id) end, 20_000)

      # Restart the instance's controller-side supervisor while it is suspended.
      # It must come back from durable state as suspended — NOT bind a runner.
      force_fresh_rebind(namespace, operative_id)

      assert wait_until(fn -> suspended?(namespace, operative_id) end, 30_000),
             "a suspended instance was not rediscovered from durable state on restart"

      # And it still wakes on input.
      send_turn(namespace, operative_id, "two")

      assert wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 60_000),
             "the rediscovered suspended instance did not rehydrate on a new message"

      assert replies(collector, 2) == ["reply:one", "reply:two"]
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
      assert wait_until(fn -> committed_iters(operative_id) == [0] end, 30_000)

      # The park is what makes the next turn a fresh bind.
      assert wait_until(fn -> suspended?(namespace, operative_id) end, 20_000),
             "the idle stateless loop never parked"

      # Nothing on disk to restore — the journal is the only thing carried over.
      assert Flezha.Operative.Replay.resume_entity_count(:workflow, operative_id) == 0

      send_turn(namespace, operative_id, "two")

      assert wait_until(fn -> committed_iters(operative_id) == [0, 1] end, 60_000),
             "the rehydrated turn did not commit as iteration 1: " <>
               inspect(committed_iters(operative_id))

      # The point of the test: the second turn's staged output was delivered.
      assert replies(collector, 2) == ["reply:one", "reply:two"]
    end
  end

  describe "loop rewind (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_rewind

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

      assert wait_until(fn -> committed_iters(operative_id) == [0, 1, 2] end, 60_000),
             "the loop did not commit one iteration per turn: " <>
               inspect(committed_iters(operative_id))

      assert replies(collector, 3) == ["reply:alpha", "reply:beta", "reply:gamma"]
      assert ProcJournal.current_epoch(operative_id) == 0
      assert consumed_offsets(operative_id) == [1, 2, 3]

      # --- rewind to iteration 0 ---
      rewind_out =
        ExUnit.CaptureIO.capture_io(fn ->
          Flezha.Service.CliManager.operative_rewind(namespace, operative_id, 0)
        end)

      assert rewind_out =~ "rewound to iteration 0"

      # The journal folded back: only iteration 0 is committed, and the epoch is
      # bumped so everything from here on supersedes what the unwound turns wrote.
      assert wait_until(fn -> ProcJournal.current_epoch(operative_id) == 1 end, 20_000)

      # --- the loop unwinds the two turns above the target ---
      # Only iteration 0 remains committed; iterations 2 and 1 are undone.
      assert wait_until(
               fn -> ProcJournal.committed_iterations(operative_id) == [0] end,
               60_000
             ),
             "the journal did not fold back to the rewound-to iteration"

      assert wait_until(fn -> length(collected(collector)) >= 5 end, 90_000),
             "the rewound iterations were never compensated"

      # --- the next message is processed as iteration 1 ---
      send_turn(namespace, operative_id, "delta")

      assert wait_until(
               fn -> ProcJournal.committed_iterations(operative_id) == [0, 1] end,
               90_000
             ),
             "the resumed loop did not run the next message as iteration 1"

      outputs = await_outputs(collector, 6)

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

  describe "entity retry (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_retry

    # `workflow-entity-retry` on a live runner: `retry_flaky` fails its first
    # attempt, the per-entity ladder re-runs it in place, and the procedure
    # commits normally — nothing compensates and the run reports `ok`.
    test "a transient entity failure is retried and the run completes", ctx do
      collector = spawn_link(fn -> collect_loop([]) end)

      operative_id = drive_retry_run(ctx, collector, "1", "ok")

      # `res:...` is proc A's forward output; the terminal marker names the attempt
      # the flaky entity finally succeeded on — 2 ⇒ the ladder absorbed the
      # first failure.
      assert [{~s(res:{"seed":"go"}), _}, {marker, _}] = await_outputs(collector, 2)
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

      assert [{~s(res:{"seed":"go"}), _}, {"res:undone", _}] = await_outputs(collector, 2)

      {:ok, entries} = ProcJournal.read(operative_id)

      assert [%{type: :compensated, proc_idx: 0}] =
               Enum.filter(entries, &(&1.type == :compensated)),
             "the exhausted ladder did not run the saga rollback"

      committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx
      assert committed == [0], "the failing procedure must never commit"
    end
  end

  describe "branching tree (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_branch

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

      delivered = collector |> await_outputs(4) |> Enum.map(&elem(&1, 0))

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
          Flezha.Service.CliManager.state_journal(operative_id)
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
      delivered = collector |> await_outputs(6) |> Enum.map(&elem(&1, 0))

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

  # The crash point of `diamond_workflow`: the root, the blocked arm's upper
  # half (B1) and the fast arm (C) have committed while the blocked producer
  # (B2, proc 3) is mid-run — the join therefore has exactly one journaled and
  # one outstanding contribution.
  defp diamond_half_committed?(operative_id) do
    case ProcJournal.restore_state(operative_id) do
      %{committed_proc_idxs: idxs} -> Enum.sort(idxs) == [0, 1, 2]
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
  defp replies(collector, count) do
    collector |> await_outputs(count) |> Enum.map(fn {payload, _md} -> payload end)
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

  # --- crash-resume harness ---------------------------------------------------

  # Drive one operative through the full SIGKILL-mid-run → respawn → resume flow
  # and assert the terminal outcome. `mid_run?` is a predicate on the operative
  # id that becomes true once the durable mid-run point is reached on the first
  # runner (which never completes it — the release env is unset there).
  # Returns the operative id for scenario-specific assertions.
  defp drive_crash_resume(ctx, factory_id, terminal_status, mid_run?, opts \\ []) do
    {before_rebind, builder_opts} = Keyword.pop(opts, :before_rebind)
    namespace = unique("workflow_crash")
    operative_id = unique(factory_id)
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    # Runner A (no release env): the operative commits, then blocks mid-chain.
    {port_a, exec_a} = boot_runner(namespace, ctx, id_path, release?: false)

    info = start_workflow(namespace, operative_id, factory_id, builder_opts)
    assert info.runner_id == exec_a

    assert wait_until(fn -> mid_run?.(operative_id) end, 30_000),
           "operative never reached its durable mid-run point on the first runner"

    refute Completion.exists?(operative_id),
           "operative terminated before the crash — it did not block mid-chain"

    assert registered?(namespace, operative_id)

    # SIGKILL A; the replacement (release env set) re-presents A's stable id, so
    # its connect swaps the bindings + evicts the dead session on both transports.
    sigkill(os_pid(port_a))
    {_port_b, exec_b} = boot_runner(namespace, ctx, id_path, release?: true)
    assert exec_b != exec_a, "replacement must mint a fresh session token"

    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end),
           "dead runner was never evicted after the replacement connected"

    # Scenario hook on the durable state the crash left, before the rebind
    # (which is what triggers the restore).
    if before_rebind, do: before_rebind.(operative_id)

    force_fresh_rebind(namespace, operative_id)

    # The resumed operative completes with the expected terminal status.
    await_status(operative_id, terminal_status, 60_000)

    assert wait_until(fn -> not registered?(namespace, operative_id) end),
           "completed operative was never deregistered"

    operative_id
  end

  # --- helpers ----------------------------------------------------------------

  # Start one workflow operative of `factory_id` on `namespace`, returning the
  # start info (every workflow scenario here seeds the same args and differs only
  # in its class and its plugins).
  defp start_workflow(namespace, operative_id, factory_id, opts) do
    builder = %Builder{
      id: operative_id,
      factory_id: factory_id,
      kind: :workflow,
      init_values: %{"seed" => "go"},
      opts: opts
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    info
  end

  # Wait for the run's terminal completion record to carry `status`.
  defp await_status(operative_id, status, timeout_ms) do
    assert wait_until(
             fn -> match?({:ok, %{status: ^status}}, Completion.read(operative_id)) end,
             timeout_ms
           ),
           "run never completed with status #{inspect(status)}"
  end

  defp boot_runner(namespace, ctx, id_path, opts) do
    env =
      [{"FLEZHA_RUNNER_ID_PATH", id_path}] ++
        Keyword.get(opts, :extra_env, []) ++
        if Keyword.fetch!(opts, :release?), do: [{"FLEZHA_WORKFLOW_E2E_RELEASE", "1"}], else: []

    {port, runner_id} =
      NodeTestHelper.start_runner_port(namespace,
        runtime: ctx.runtime,
        transport: ctx.transport,
        env: env,
        connect_timeout: 120_000
      )

    # Close the port at teardown so the runner process exits (it blocks on
    # stdin until then); the SIGKILLed runner is already dead, so this is a
    # no-op for it.
    on_exit(fn -> ensure_port_dead(port) end)
    {port, runner_id}
  end

  defp ensure_port_dead(port) do
    if is_port(port) and Enum.member?(:erlang.ports(), port) do
      try_quietly(fn -> Port.close(port) end)
    end
  end

  # Force a fresh controller-side bind so `AddOperative` carries
  # `resume_entity_count > 0` (Path A). Killing the supervisor bypasses
  # `terminate/2` (WAL survives) and ProcessHub transient-restarts it onto the
  # live replacement runner.
  defp force_fresh_rebind(namespace, operative_id) do
    {:ok, pid} = OperativeManager.get_operative_pid(namespace, operative_id)
    Process.exit(pid, :kill)
  end

  defp os_pid(port) do
    {:os_pid, pid} = :erlang.port_info(port, :os_pid)
    pid
  end

  defp sigkill(os_pid) do
    {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
  end

  defp registered?(namespace, operative_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(namespace, operative_id))
  end

  # Number of per-Entity compensation-done markers the controller has written
  # for this operative (one per compensated Entity).
  defp comp_done_count(operative_id) do
    [Registry.state_root(), operative_id, "**", "*.comp_done"]
    |> Path.join()
    |> Path.wildcard()
    |> length()
  end

  # Count change-log records in a procedure's retained WAL file(s). The WAL is
  # a stream of `<seq::64><len::32><body::len><crc::32>` records.
  defp wal_record_count(operative_id, procedure) do
    [Registry.state_root(), operative_id, procedure, "*.wal"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn path -> count_wal_records(File.read!(path), 0) end)
    |> Enum.sum()
  end

  defp count_wal_records(
         <<_seq::64-big, len::32-big, _body::binary-size(len), _crc::32-big, rest::binary>>,
         acc
       ),
       do: count_wal_records(rest, acc + 1)

  defp count_wal_records(_, acc), do: acc

  defp cleanup(namespace, operative_id, id_path) do
    try_quietly(fn -> OperativeManager.stop_all_operatives(namespace) end)
    try_quietly(fn -> FleetManager.stop_namespace(namespace) end)
    File.rm_rf(id_path)
    File.rm_rf(Path.join(Registry.state_root(), operative_id))
  end

  defp try_quietly(fun) do
    fun.()
  catch
    _, _ -> :ok
  end

  defp unique(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp unique_id_path,
    do: Path.join(System.tmp_dir!(), "flz-crash-id-#{System.unique_integer([:positive])}")

  defp wait_until(fun, timeout_ms \\ 15_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(50)
        do_wait_until(fun, deadline)
    end
  end
end
