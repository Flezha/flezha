defmodule FlezhaTest.System.BoundedCrashResumeE2ETest do
  @moduledoc """
  End-to-end crash-resume proof for the durable bounded operative model
  (`bounded-operative-functionality` §7c.3), against live Rust and Java
  executors over both UDS and QUIC.

  Two real-wire scenarios per (runtime × transport) cell:

    * **resume** — a durable structured bounded operative (`commit_block_bounded`)
      commits Segment state in operation A, then blocks in operation B. The
      executor is SIGKILLed mid-run (op A durable, op B never finished), a
      replacement executor is started on the same stable identity, and the
      operative's controller-side supervisor is forced to re-bind onto it. The
      replacement restores the committed state and runs to completion — proving
      resume-not-restart with a terminal outcome matching a clean run.

    * **compensation** — a durable saga bounded operative (`saga_block_bounded`)
      completes A and B, then fails in C, triggering reverse-order rollback. B's
      compensation-done marker lands, then A's `compensate` blocks; the executor
      is SIGKILLed mid-compensation. The replacement ships the durable
      compensation markers back and finishes only the outstanding hook (A),
      skipping the already-done B — proving crash-mid-compensation resume.

  ## Crash-resume mechanics (why each step is here)

  `convergence_health_enabled` is `false` in the test config, so nothing
  auto-rebinds a crashed operative — the test drives it:

    * A shared `FLEZHA_EXECUTOR_ID_PATH` makes the replacement re-present the
      dead executor's durable stable identity, so its connect swaps the bindings
      and evicts the dead session on both transports (no reliance on detecting
      the SIGKILL, which QUIC surfaces only after its ~45 s idle timeout).
    * `Process.exit(overwatch_pid, :kill)` forces a fresh controller-side bind:
      ProcessHub transient-restarts the supervisor, which re-resolves onto the
      live replacement and re-issues `AddOperative` with `resume_segment_count > 0`
      — the only path that arms the SDK's restore-before-run gate. The hard kill
      bypasses `terminate/2`, so the WAL survives (a graceful stop would delete it).
    * The replacement is launched WITH `FLEZHA_BOUNDED_E2E_RELEASE`, so the
      blocking operation/compensation that parked on the first executor completes
      on the resume, making the crash window deterministic (no wall-clock races).

  Tagged `:system` (boots real subprocesses) — excluded from the default
  `mix test`; run with:

      mix test --only bounded_crash_resume
      mix test test/system/bounded_crash_resume_e2e_test.exs --include system
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Registry
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :system
  @moduletag :bounded_crash_resume
  # Two live executor lifecycles + a SIGKILL + a resume per test; QUIC first
  # connects can be slow, so give each cell a generous budget.
  @moduletag timeout: 180_000

  setup context do
    runtime = context[:runtime]

    if runtime == :java do
      jar = Path.join(File.cwd!(), "test/apps/java_executor/dist/java_executor.jar")

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

      test "durable bounded resumes from committed Segment state after a SIGKILL", ctx do
        # Mid-run point: op A committed durable Segment state (the spec's
        # precondition); op B is then blocking, so the operative can only
        # complete after resuming on the replacement.
        operative_id =
          drive_crash_resume(ctx, "commit_block_bounded", "ok", fn oid ->
            Overwatch.resume_segment_count(:bounded, oid) > 0
          end)

        # Resume-not-restart, directly: op A's WAL carries BOTH the pre-crash
        # commit and the resume re-run's contiguous append (seq terminal+1). A
        # cold restart that lost the committed state would leave only one record.
        assert wal_record_count(operative_id, "commit_a") >= 2,
               "resumed op A did not re-run over restored state (WAL was not resumed)"
      end

      test "durable saga bounded resumes compensation after a SIGKILL mid-rollback", ctx do
        # Mid-run point: A+B completed and C failed → reverse rollback; B's fast
        # compensation recorded a done marker, then A's compensate blocked — the
        # operative is durably mid-compensation.
        operative_id =
          drive_crash_resume(ctx, "saga_block_bounded", "error", fn oid ->
            comp_done_count(oid) >= 1
          end)

        # Both compensable stages are undone: A's outstanding hook finished on
        # the resume, joining B's pre-crash done marker.
        assert comp_done_count(operative_id) >= 2,
               "resume did not finish the outstanding compensation hook"
      end
    end
  end

  describe "effectively-once tier (rust/uds)" do
    @describetag runtime: :rust, transport: :uds, cell: :rust_uds_tier

    # §6.2: a tier bounded operative crashes mid-run and resumes; every keyed
    # output reaches the conforming sink with the durable identity stamps, the
    # Model-A re-run re-emits a byte-identical payload under the stable key,
    # and the changelog upsert converges to exactly one value — never lost,
    # never divergent.
    test "tier bounded crash-resume converges the conforming changelog sink", ctx do
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
          "tier_commit_block_bounded",
          "ok",
          fn oid -> Overwatch.resume_segment_count(:bounded, oid) > 0 end,
          plugins: [sink]
        )

      # Let the resume's final flush land before auditing everything received.
      Process.sleep(1_000)
      outputs = drain_tier_outputs([])

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
      |> Enum.group_by(fn {_m, md} -> {md["flz.segment"], md["flz.seq"], md["flz.epoch"]} end)
      |> Enum.each(fn {identity, group} ->
        payloads = group |> Enum.map(fn {m, _} -> m end) |> Enum.uniq()
        assert length(payloads) == 1, "identity #{inspect(identity)} diverged"
      end)
    end
  end

  defp drain_tier_outputs(acc) do
    receive do
      {:message_forward, msg, md} -> drain_tier_outputs([{to_string(msg), md} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # --- crash-resume harness ---------------------------------------------------

  # Drive one operative through the full SIGKILL-mid-run → respawn → resume flow
  # and assert the terminal outcome. `mid_run?` is a predicate on the operative
  # id that becomes true once the durable mid-run point is reached on the first
  # executor (which never completes it — the release env is unset there).
  # Returns the operative id for scenario-specific assertions.
  defp drive_crash_resume(ctx, factory_id, terminal_status, mid_run?, builder_opts \\ []) do
    namespace = unique("bounded_crash")
    operative_id = unique(factory_id)
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    # Executor A (no release env): the operative commits, then blocks mid-chain.
    {port_a, exec_a} = boot_executor(namespace, ctx, id_path, release?: false)

    builder = %Builder{
      id: operative_id,
      factory_id: factory_id,
      kind: :bounded,
      init_values: %{"seed" => "go"},
      opts: builder_opts
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    assert info.executor_id == exec_a

    assert wait_until(fn -> mid_run?.(operative_id) end, 30_000),
           "operative never reached its durable mid-run point on the first executor"

    refute Completion.exists?(operative_id),
           "operative terminated before the crash — it did not block mid-chain"

    assert registered?(namespace, operative_id)

    # SIGKILL A; the replacement (release env set) re-presents A's stable id, so
    # its connect swaps the bindings + evicts the dead session on both transports.
    sigkill(os_pid(port_a))
    {_port_b, exec_b} = boot_executor(namespace, ctx, id_path, release?: true)
    assert exec_b != exec_a, "replacement must mint a fresh session token"

    assert wait_until(fn -> exec_a not in FleetManager.active_executors(namespace) end),
           "dead executor was never evicted after the replacement connected"

    force_fresh_rebind(namespace, operative_id)

    # The resumed operative completes with the expected terminal status.
    assert wait_until(
             fn -> match?({:ok, %{status: ^terminal_status}}, Completion.read(operative_id)) end,
             60_000
           ),
           "resumed operative never completed with status #{inspect(terminal_status)}"

    assert wait_until(fn -> not registered?(namespace, operative_id) end),
           "completed operative was never deregistered"

    operative_id
  end

  # --- helpers ----------------------------------------------------------------

  defp boot_executor(namespace, ctx, id_path, opts) do
    env =
      [{"FLEZHA_EXECUTOR_ID_PATH", id_path}] ++
        if Keyword.fetch!(opts, :release?), do: [{"FLEZHA_BOUNDED_E2E_RELEASE", "1"}], else: []

    {port, executor_id} =
      NodeTestHelper.start_executor_port(namespace,
        runtime: ctx.runtime,
        transport: ctx.transport,
        env: env,
        connect_timeout: 120_000
      )

    # Close the port at teardown so the executor process exits (it blocks on
    # stdin until then); the SIGKILLed executor is already dead, so this is a
    # no-op for it.
    on_exit(fn -> ensure_port_dead(port) end)
    {port, executor_id}
  end

  defp ensure_port_dead(port) do
    if is_port(port) and Enum.member?(:erlang.ports(), port) do
      try_quietly(fn -> Port.close(port) end)
    end
  end

  # Force a fresh controller-side bind so `AddOperative` carries
  # `resume_segment_count > 0` (Path A). Killing the supervisor bypasses
  # `terminate/2` (WAL survives) and ProcessHub transient-restarts it onto the
  # live replacement executor.
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

  # Number of per-Segment compensation-done markers the controller has written
  # for this operative (one per compensated Segment).
  defp comp_done_count(operative_id) do
    [Registry.state_root(), operative_id, "**", "*.comp_done"]
    |> Path.join()
    |> Path.wildcard()
    |> length()
  end

  # Count change-log records in an operation's retained WAL file(s). The WAL is
  # a stream of `<seq::64><len::32><body::len><crc::32>` records.
  defp wal_record_count(operative_id, operation) do
    [Registry.state_root(), operative_id, operation, "*.wal"]
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
