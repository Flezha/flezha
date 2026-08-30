defmodule FlezhaTest.System.ControlPlaneWedgeE2ETest do
  @moduledoc """
  End-to-end drill for `close-control-plane-silent-wedges`: the chain of
  custody from a real wedged runner to a real remedy.

  A live Rust runner processes one input, then wedges inside user code — its
  socket stays open and its heartbeat thread keeps reporting, while its
  dispatch counter freezes. That is the 2026-06-10 production freeze, and
  before this change nothing in the control plane acted on it: the classifier
  computed the verdict every tick and no one read it.

  The drill asserts the whole path with real components — real runner, real
  liveness counters, real `Convergence.Loop`:

      wedged runner → classifier verdict → Diff → evict_runner
                    → operative misbound → rebind onto a live runner
                    → producing again

  plus the start-authority invariant: exactly one instance exists throughout,
  and no duplicate-binding alarm fires while convergence is churning.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/control_plane_wedge_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :liveness

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness, only: [boot_runner: 3, peek_outputs: 0]

  alias Flezha.Convergence.{Heal, Loop}
  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{HubManager, OperativeManager}

  @factory "wedge_probe"

  # Above the runner's heartbeat cadence below, so every classifier poll sees
  # `hb_count` advance — the "is this runner alive at all" precondition that
  # separates a wedged pool from a dead process.
  @tick_ms 1_500
  @heartbeat_ms 300

  # A fresh fleet per test: every drill evicts or terminates its runner.
  setup do
    namespace = "wedge_e2e_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    %{namespace: namespace}
  end

  # Wedging user code stops dispatch only where dispatch is a shared pool; an
  # SDK that runs a process per Entity cannot produce the signature at all.
  @tag capability: :pool_wedge, skip: FlezhaTest.SystemCase.deferral(:pool_wedge)
  @tag timeout: 300_000
  test "a wedged runner is localized, evicted, and its operative produces again elsewhere",
       %{namespace: namespace} do
    parent = self()
    heals = attach_collector(parent, [:flezha, :convergence, :heal], :heal)
    verdicts = attach_collector(parent, [:flezha, :data_plane, :stall_localized], :verdict)
    dupes = attach_collector(parent, [:flezha, :boot, :reconcile_duplicate], :duplicate)
    on_exit(fn -> Enum.each([heals, verdicts, dupes], &:telemetry.detach/1) end)

    {operative_id, port_a, runner_a} = probe_on_its_own_runner(namespace, "wedged")

    # --- Phase 0: the operative produces on runner A -------------------------
    assert produces?(operative_id, namespace, "one", "ok:one", 60_000),
           "operative never produced before the wedge"

    # --- Phase 1: wedge the pool, then keep feeding it -----------------------
    # `send_frame_calls` must keep advancing (the controller is still
    # delivering) while the SDK's dispatch counter is frozen; that pair, plus a
    # live heartbeat, is the verdict.
    OperativeManager.send_io(operative_id, namespace, "wedge", %{"input_type" => "2"})

    # The runner's heartbeat reports the handout the wedged worker holds.
    assert wait_until(
             fn ->
               Flezha.Transport.LivenessCounters.snapshot_all().channels
               |> Enum.any?(&(&1.runner_id == runner_a and Map.get(&1, :hb_inflight, 0) >= 1))
             end,
             30_000
           ),
           "the wedge input never reached a pool worker"

    # Unlinked on purpose: the test stops it deliberately in phase 2, and a
    # link would make that stop kill the test.
    feeder = start_feeder(operative_id, namespace, parent)
    on_exit(fn -> if Process.alive?(feeder), do: Process.exit(feeder, :kill) end)

    # The replacement a real deployment already has (a second replica, or the
    # one the orchestrator brings back). It is up BEFORE the eviction on
    # purpose: with nowhere to place the operative, every rebind attempt fails
    # and burns a rung of the standard 5 s → 30 s → 60 s → 300 s cooldown, so
    # the drill would be timing the backoff ladder rather than the remedy.
    {port_b, runner_b} = boot_runner(namespace, unique_id_path(), env: heartbeat_env())
    on_exit(fn -> close_port(port_b) end)

    # The Loop runs only in prod config; start one for the drill.
    start_supervised!({Loop, tick_ms: @tick_ms})

    assert_receive {:verdict, %{layer: :runner_pool_wedged, runner_id: ^runner_a}}, 60_000

    assert_receive {:heal, %{primitive: :evict_runner, target: ^runner_a}}, 60_000

    assert wait_until(fn -> runner_a not in FleetManager.active_runners(namespace) end, 30_000),
           "a classifier-proved wedge must evict the runner despite its socket being open"

    # --- Phase 2: the wedged runner goes away; the operative rebinds ---------
    # Its pool is dead for good, so the process is replaced rather than reused.
    send(feeder, :stop)
    close_port(port_a)

    assert wait_until(fn -> bound_runner(namespace, operative_id) == runner_b end, 120_000),
           "the operative never rebound onto the replacement runner"

    # --- Phase 3: producing again -------------------------------------------
    assert produces?(operative_id, namespace, "after", "ok:after", 60_000),
           "the operative never produced again after the wedge was healed " <>
             "(bound=#{inspect(bound_runner(namespace, operative_id))} " <>
             "active=#{inspect(FleetManager.active_runners(namespace))})"

    # --- The start-authority invariant held throughout -----------------------
    ids = namespace |> HubManager.get_all_operatives() |> Enum.map(&elem(&1, 0))
    assert Enum.count(ids, &(&1 == operative_id)) == 1

    refute_received {:duplicate, _},
                    "a duplicate binding alarm fired while convergence was churning"
  end

  # Eviction moves a wedged runner's operatives but leaves the process running
  # and connected, so it becomes a placement candidate again and wedges again —
  # evict, reconnect, take work, wedge. The terminal rung is the only remedy
  # that clears the process, and the exit has to be NON-ZERO: a zero exit reads
  # as a clean stop and the orchestrator would not bring the runner back.
  @tag timeout: 180_000
  test "a terminally disconnected runner exits non-zero and its operatives reroute",
       %{namespace: namespace} do
    {operative_id, doomed_port, doomed} =
      probe_on_its_own_runner(namespace, "restarted", port_opts: [:exit_status])

    assert produces?(operative_id, namespace, "one", "ok:one", 60_000),
           "the operative never produced before the restart " <>
             "(bound=#{inspect(bound_runner(namespace, operative_id))} " <>
             "active=#{inspect(FleetManager.active_runners(namespace))})"

    # The replacement the orchestrator would bring back, up first: the reroute
    # this drill asserts is the existing disconnect path's, not the Loop's.
    {port_b, runner_b} = boot_runner(namespace, unique_id_path(), env: heartbeat_env())
    on_exit(fn -> close_port(port_b) end)

    # The Planner's rung, executed directly: the ladder ordering is proved by
    # the Planner's own tests, what this drill proves is what the SDK does.
    assert Heal.execute(%{
             primitive: :restart_runner,
             namespace: namespace,
             target: doomed,
             args: %{reason: :data_plane_wedge, layer: :runner_pool_wedged}
           }) == :ok

    assert_receive {^doomed_port, {:exit_status, status}}, 60_000

    assert status != 0,
           "a terminal disconnect must exit non-zero, or the orchestrator reads it as a clean stop"

    assert wait_until(fn -> doomed not in FleetManager.active_runners(namespace) end, 60_000)

    assert wait_until(fn -> bound_runner(namespace, operative_id) == runner_b end, 120_000),
           "the operative never rebound onto the replacement runner"

    assert produces?(operative_id, namespace, "after", "ok:after", 60_000)
  end

  # A worker killed mid-dispatch is the OTHER way a runner stops doing work:
  # not a pool that stalls, but one activation whose host died holding it.
  # Per-entity exclusivity is the handout, so without the host answering it —
  # abandoning, replacing the worker, recreating the Entity, whatever that
  # runtime's answer is — the Entity would be retired for the life of the
  # process and nothing would ever say so.
  @tag capability: :pool_wedge, skip: FlezhaTest.SystemCase.deferral(:pool_wedge)
  @tag timeout: 180_000
  test "a worker aborted mid-dispatch recovers without losing its entity",
       %{namespace: namespace} do
    {operative_id, _port, _runner} = probe_on_its_own_runner(namespace, "aborted")

    assert produces?(operative_id, namespace, "one", "ok:one", 60_000),
           "the operative never produced before the abort"

    # The fixture ends this activation as violently as its host allows.
    OperativeManager.send_io(operative_id, namespace, "abort", %{"input_type" => "2"})

    # No operator action, no rebind: a pool that did not come back to size, or
    # an entity left holding an unanswered handout, would never serve this.
    assert produces?(operative_id, namespace, "two", "ok:two", 120_000),
           "the entity was never served again after its worker was aborted"

    # And the aborting item never ran twice: it produced at most once, whether
    # its host re-handed it (abandon / retry ladder) or lost it with the worker.
    aborted = Enum.count(peek_outputs(), fn {msg, _md} -> msg == "ok:abort" end)

    assert aborted <= 1,
           "the aborting item produced #{aborted} outputs — a re-hand must not double-execute"
  end

  # ---------- helpers ----------

  # A fresh runner and a `wedge_probe` operative bound to it, with the outputs
  # forwarded to the calling test. Every drill starts here: the interesting
  # part of each is what happens to that pair afterwards.
  defp probe_on_its_own_runner(namespace, label, opts \\ []) do
    operative_id = "#{label}_#{System.unique_integer([:positive])}"

    {port, runner_id} =
      boot_runner(namespace, unique_id_path(), [env: heartbeat_env()] ++ opts)

    assert {:ok, %{runner_id: ^runner_id}} =
             OperativeManager.start_operative(namespace, builder(operative_id, self()))

    {operative_id, port, runner_id}
  end

  defp builder(operative_id, recv_pid) do
    sink = %Flezha.Plugin{
      id: "wedge_sink",
      namespace: nil,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid, include_metadata: true},
      subscriptions: [{:on_output, []}]
    }

    %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      opts: [plugins: [sink]]
    }
  end

  defp heartbeat_env(), do: [{"FLEZHA_DATA_HEARTBEAT_MS", Integer.to_string(@heartbeat_ms)}]

  # Keeps `send_frame_calls` advancing so the classifier's "is this runner
  # being fed?" precondition holds while its dispatch counter is frozen.
  # Follows the test process so it can never outlive the run.
  defp start_feeder(operative_id, namespace, owner) do
    spawn(fn ->
      Process.monitor(owner)
      feed(operative_id, namespace)
    end)
  end

  defp feed(operative_id, namespace) do
    receive do
      :stop -> :ok
      {:DOWN, _ref, :process, _pid, _reason} -> :ok
    after
      100 ->
        OperativeManager.send_io(operative_id, namespace, "feed", %{"input_type" => "2"})
        feed(operative_id, namespace)
    end
  end

  defp attach_collector(parent, event, tag) do
    id = "wedge-e2e-#{tag}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      event,
      fn _e, _meas, meta, _ -> send(parent, {tag, meta}) end,
      nil
    )

    id
  end

  # Feed `input` until `expected` comes back, or the deadline passes.
  #
  # One frame is not enough to prove an operative cannot produce: a freshly
  # bound operative's sink plugin activates asynchronously, and an emit that
  # beats its subscription is dropped with nothing to re-drive it. Every drill
  # here asserts that the operative CAN produce, never that one particular
  # frame sufficed — so the input is re-sent, exactly as a real producer would.
  defp produces?(operative_id, namespace, input, expected, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    feed_until(operative_id, namespace, input, expected, deadline)
  end

  defp feed_until(operative_id, namespace, input, expected, deadline) do
    OperativeManager.send_io(operative_id, namespace, input, %{"input_type" => "2"})

    cond do
      await_output(expected, 1_000) -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true -> feed_until(operative_id, namespace, input, expected, deadline)
    end
  end

  defp await_output(expected, timeout) do
    wait_until(fn -> Enum.any?(peek_outputs(), fn {msg, _md} -> msg == expected end) end, timeout)
  end

  defp bound_runner(namespace, operative_id) do
    with {:ok, pid} <- OperativeManager.get_operative_pid(namespace, operative_id),
         %{runner_id: runner_id} <- :sys.get_state(pid) do
      runner_id
    else
      _ -> nil
    end
  end

  defp unique_id_path() do
    Path.join(System.tmp_dir!(), "wedge_e2e_#{System.unique_integer([:positive])}.id")
  end

  defp close_port(port) do
    if is_port(port) and Enum.member?(:erlang.ports(), port) do
      try do
        Port.close(port)
      catch
        _, _ -> :ok
      end
    end
  end
end
