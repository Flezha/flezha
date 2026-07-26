defmodule Flezha.Service.HubManagerRecoveryTest do
  @moduledoc """
  Exercises the marker-gated recovery contract Flezha consumes from the
  upstream `recovery-mode-opt-in` change.

  Single-node coverage:

    * a clean shutdown writes the marker → next boot bypasses replay
      and `await_recovery_complete/1` returns immediately;
    * `flz cluster prepare-recovery` deletes the marker on every
      node (here: just the local one) → next boot replays the DETS
      cspecs and `:recovery_complete` telemetry fires;
    * `await_recovery_complete/2` blocks on a missing :complete event
      and times out cleanly.

  The cross-node split-brain detection scenarios (tasks 1.13 / 1.14
  paired with the spec's two-controller cluster) require the live k3s
  smoke test in task 4.19; the marker-file / telemetry contract is
  identical on one node and is what flezha-core owns. The upstream
  process_hub test suite covers the multi-peer init_sync semantics.
  """

  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias ProcessHub.Service.ProcessRegistry

  @namespace "recovery_test_ns"
  # The runner id baked into the durable cspec by the boot that registered it.
  @recorded_runner "recorded_runner_from_previous_boot"
  @seeded_runner "seeded_live_runner"

  setup do
    fleet_state = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

    previous_enabled = Application.get_env(:flezha, :auto_restore_enabled)
    previous_root = Application.get_env(:flezha, :process_hub_registry_root)
    previous_override = Application.get_env(:flezha, :process_hub_recovery_marker_path)
    previous_timeout = Application.get_env(:flezha, :process_hub_recovery_timeout_ms)

    tmp_dir =
      Path.join([System.tmp_dir!(), "flezha_recovery_test_#{System.unique_integer([:positive])}"])

    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)

    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
    Application.delete_env(:flezha, :process_hub_recovery_marker_path)
    Application.put_env(:flezha, :process_hub_recovery_timeout_ms, 3_000)

    on_exit(fn ->
      _ = HubManager.stop_hub(@namespace)
      File.rm_rf(tmp_dir)
      Application.put_env(:flezha, :auto_restore_enabled, previous_enabled)

      if previous_root,
        do: Application.put_env(:flezha, :process_hub_registry_root, previous_root),
        else: Application.delete_env(:flezha, :process_hub_registry_root)

      if previous_override,
        do: Application.put_env(:flezha, :process_hub_recovery_marker_path, previous_override),
        else: Application.delete_env(:flezha, :process_hub_recovery_marker_path)

      if previous_timeout,
        do: Application.put_env(:flezha, :process_hub_recovery_timeout_ms, previous_timeout),
        else: Application.delete_env(:flezha, :process_hub_recovery_timeout_ms)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "fresh boot writes the marker and reaches :normal", %{tmp_dir: tmp_dir} do
    hub_id = Identificator.hub(@namespace)
    marker_path = Path.join([tmp_dir, Atom.to_string(hub_id), "cluster.healthy"])

    {:ok, _} = HubManager.start_hub(@namespace)

    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)
    assert {:ok, state} = HubManager.recovery_state(hub_id)
    assert state in [:normal, :skipped]
    assert File.exists?(marker_path)
  end

  test "prepare_recovery_cluster deletes the marker on the local node",
       %{tmp_dir: tmp_dir} do
    hub_id = Identificator.hub(@namespace)
    marker_path = Path.join([tmp_dir, Atom.to_string(hub_id), "cluster.healthy"])

    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)
    assert File.exists?(marker_path)

    # The CLI verb is a thin wrapper around this upstream call; the
    # operator-facing rendering is covered in cli_manager_cluster_test.
    assert {:ok, _acked} = ProcessHub.Service.Recovery.prepare_recovery_cluster(hub_id)

    refute File.exists?(marker_path)
  end

  test "recovery_state surfaces :unknown_hub when the hub is not started" do
    hub_id = Identificator.hub("never_started_namespace")
    assert {:error, :unknown_hub} = HubManager.recovery_state(hub_id)
  end

  defmodule ReplayStubChild do
    @moduledoc false
    use GenServer

    def start_link({_ns, _operative}), do: GenServer.start_link(__MODULE__, :ok)
    def start_link({_ns, _runner, _operative}), do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, %{}}

    # Stands in for an Overwatch, which absorbs the binding re-assert nudge.
    @impl true
    def handle_info(_msg, state), do: {:noreply, state}
  end

  # fails when: a warm restart leaves a persisted operative listed but
  # `(unplaced)` — bound to the previous boot's dead pid, invisible to the
  # convergence reconciler, producing no data until the operator re-declares it.
  test "recovery replay rebinds a persisted operative even when no runner is placeable" do
    # Zero budget removes every timing dependency: the hook gets exactly one
    # lookup, which cannot succeed because no runner is ever seeded.
    put_env(:hub_runner_assign_wait_ms, 0)

    hub_id = Identificator.hub(@namespace)
    operative_id = "replay_no_runner_op"

    replay_hub(hub_id, [{operative_id, "market"}])

    assert await_bound_node(hub_id, operative_id, 10_000) == node(),
           "recovery replay left #{operative_id} bound to a dead pid — `flz operative list` renders it as (unplaced)"

    # Degraded, not lost: the durable cspec's runner is kept so the row carries a
    # live pid that Overwatch and the convergence loop can rebind.
    assert registry_tag(ProcessRegistry.dump(hub_id), operative_id) == @recorded_runner
  end

  # fails when: one operative whose factory no runner advertises drags the whole
  # replay batch past `cross_node_request_timeout`, so WorkerQueue kills the
  # request and every row in it — placeable or not — keeps its dead pid.
  test "unplaceable operatives do not strand the rest of the replay batch" do
    # 4s × 3 unplaceable children overruns the 10s request bound if the wait is
    # per-child; shared, the whole batch waits 4s once.
    budget = 4_000
    put_env(:hub_runner_assign_wait_ms, budget)

    hub_id = Identificator.hub(@namespace)
    # `start_children/3` folds the batch in registry-dump key order, so the `a_`
    # / `z_` prefixes pin which child is reached with budget left and which is
    # reached after it is spent. Both must end up on a live pid.
    placeable = "a_batch_placeable_op"
    unplaceable = for i <- 1..3, do: "z_batch_unplaceable_op_#{i}"

    seed_runner(@seeded_runner, "market")

    rows =
      [{placeable, "market"}] ++
        Enum.map(unplaceable, &{&1, "factory_no_runner_serves"})

    started_at = System.monotonic_time(:millisecond)
    replay_hub(hub_id, rows)

    for {operative_id, _factory} <- rows do
      assert await_bound_node(hub_id, operative_id, 15_000) == node(),
             "#{operative_id} kept its dead pid — the start request was killed before update_registry"
    end

    elapsed = System.monotonic_time(:millisecond) - started_at

    assert elapsed < budget * 2,
           "the batch spent #{elapsed} ms waiting — the deadline is per-child, not shared, " <>
             "so N unplaceable children scale past cross_node_request_timeout"

    dump = ProcessRegistry.dump(hub_id)
    assert registry_tag(dump, placeable) == @seeded_runner

    for operative_id <- unplaceable do
      assert registry_tag(dump, operative_id) == @recorded_runner
    end
  end

  # fails when: one persisted row whose start args no longer match a known shape
  # raises inside the alter hook and kills the start request, so every sibling in
  # the batch loses its registry update and stays on a dead pid.
  test "a persisted row with unrecognised start args does not strand its batch" do
    hub_id = Identificator.hub(@namespace)
    good = "batch_good_op"
    malformed = "batch_malformed_op"

    seed_runner(@seeded_runner, "market")

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    HubRegistryTestHelper.inject_operative(@namespace, good, [{node(), dead_pid()}], %{
      id: good,
      start:
        {ReplayStubChild, :start_link,
         [{@namespace, @recorded_runner, %{id: good, factory_id: "market"}}]}
    })

    # An arg shape no current clause understands — an older or corrupted row.
    HubRegistryTestHelper.inject_operative(@namespace, malformed, [{node(), dead_pid()}], %{
      id: malformed,
      start: {ReplayStubChild, :start_link, [{@namespace, :not_an_operative_map}]}
    })

    restart_hub_without_marker(hub_id)

    assert await_bound_node(hub_id, good, 10_000) == node(),
           "#{good} kept its dead pid — the malformed sibling killed the whole start request"
  end

  # fails when: await_runner stops iterating and degrades to the single shot it
  # was meant to replace, so a runner that becomes placeable part-way through the
  # budget is missed and the operative is left unplaced.
  test "await_runner polls repeatedly inside one budget" do
    seed_runner(@seeded_runner, "market")

    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

    # Ready only from the fourth attempt: reaching it at all proves the loop ran.
    ready_fun = fn _runner -> Agent.get_and_update(attempts, &{&1 >= 3, &1 + 1}) end

    assert {:ok, @seeded_runner} =
             OperativeManager.await_runner(@namespace, "market", "polling_op", 1_000, ready_fun)

    assert Agent.get(attempts, & &1) >= 4,
           "await_runner gave up after one attempt instead of polling the budget"
  end

  # fails when: children reached after the shared batch budget is spent still
  # block on the FleetManager call, so batch cost grows with N and a large replay
  # walks past cross_node_request_timeout — WorkerQueue kills the request and
  # every row in it keeps its dead pid.
  test "an exhausted budget costs nothing while FleetManager is blocked" do
    :sys.suspend(FleetManager)
    on_exit(fn -> :sys.resume(FleetManager) end)

    started_at = System.monotonic_time(:millisecond)

    for i <- 1..5 do
      # `:timeout`, not `:unavailable` — the budget was gone, the fleet unasked.
      assert OperativeManager.await_runner(@namespace, "market", "spent_budget_op_#{i}", 0) ==
               {:error, :timeout}
    end

    elapsed = System.monotonic_time(:millisecond) - started_at

    assert elapsed < 100,
           "5 zero-budget lookups took #{elapsed} ms — each still blocks, so the batch bound is not real"
  end

  # fails when: the per-attempt call timeout is not clamped to what is left of
  # the caller's budget, so one blocked lookup overruns the deadline it was
  # supposed to respect.
  test "a blocked FleetManager overruns a small budget by at most the attempt floor" do
    :sys.suspend(FleetManager)
    on_exit(fn -> :sys.resume(FleetManager) end)

    budget = 60
    started_at = System.monotonic_time(:millisecond)

    assert {:error, :unavailable} =
             OperativeManager.await_runner(@namespace, "market", "clamped_op", budget)

    elapsed = System.monotonic_time(:millisecond) - started_at

    # Unclamped, the single attempt runs the full 250 ms cap.
    assert elapsed < 150, "await_runner overran a #{budget} ms budget by #{elapsed - budget} ms"
  end

  # fails when: a FleetManager that never answers within the bind window crashes
  # the Overwatch with an unmatched `{:error, :unavailable}` instead of stopping
  # it cleanly, so the operator sees a CaseClauseError rather than the reason the
  # operative did not come up.
  test "Overwatch stops cleanly when FleetManager never answers the bind lookup" do
    put_env(:overwatch_runner_wait_ms, 300)
    :sys.suspend(FleetManager)
    on_exit(fn -> :sys.resume(FleetManager) end)

    Process.flag(:trap_exit, true)

    {:ok, pid} =
      Overwatch.start_link(
        {@namespace, @recorded_runner, %Builder{id: "unanswered_lookup_op", factory_id: "market"}}
      )

    assert_receive {:EXIT, ^pid, reason}, 5_000
    assert reason == :runner_not_ready
  end

  # Registry rows left by a previous boot (dead pids), then a marker-absent hub
  # restart — the warm-restart sequence, with no wall-clock dependency.
  defp replay_hub(hub_id, operatives) do
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    Enum.each(operatives, fn {operative_id, factory_id} ->
      HubRegistryTestHelper.inject_operative(@namespace, operative_id, [{node(), dead_pid()}], %{
        id: operative_id,
        start:
          {ReplayStubChild, :start_link,
           [{@namespace, @recorded_runner, %{id: operative_id, factory_id: factory_id}}]}
      })
    end)

    restart_hub_without_marker(hub_id)
  end

  # The warm restart itself: keep the registry, clear the marker so the next
  # boot replays it, come back up.
  defp restart_hub_without_marker(hub_id) do
    stop_hub_keeping_registry(hub_id)
    File.rm!(marker_path(hub_id))

    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)
  end

  # The owning runner recorded on a registry row (`metadata.tag`).
  defp registry_tag(dump, operative_id) do
    {_cspec, _node_pids, metadata} = Map.fetch!(dump, operative_id)
    metadata[:tag]
  end

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    pid
  end

  defp put_env(key, value) do
    previous = Application.fetch_env(:flezha, key)
    Application.put_env(:flezha, key, value)

    on_exit(fn ->
      case previous do
        {:ok, v} -> Application.put_env(:flezha, key, v)
        :error -> Application.delete_env(:flezha, key)
      end
    end)
  end

  defp marker_path(hub_id) do
    Path.join([
      Application.get_env(:flezha, :process_hub_registry_root),
      Atom.to_string(hub_id),
      "cluster.healthy"
    ])
  end

  # `HubManager.stop_hub/1` wipes the registry dir; a warm restart must keep it.
  defp stop_hub_keeping_registry(hub_id) do
    Supervisor.terminate_child(HubManager, hub_id)
    Supervisor.delete_child(HubManager, hub_id)
  end

  defp seed_runner(runner_id, factory_id) do
    :sys.replace_state(FleetManager, fn state ->
      state
      |> put_in(
        [:runner_namespaces, @namespace],
        %{runner_id => {:active, System.system_time(:millisecond)}}
      )
      |> put_in(
        [:namespace_factories, @namespace],
        %{factory_id => %{description: nil, ce_ids: MapSet.new([runner_id])}}
      )
    end)

    register_control_unit(runner_id)
  end

  # `runner_registered?/1` gates on the CU registration, so a synthetic runner
  # needs one before the hook will hand out its id.
  defp register_control_unit(runner_id) do
    test = self()

    # Unlinked: on_exit kills it, and a kill must not propagate into ExUnit.
    {:ok, cu} =
      Task.start(fn ->
        {:ok, _} = Registry.register(Identificator, "#{runner_id}_cu", nil)
        send(test, :cu_registered)
        Process.sleep(:infinity)
      end)

    assert_receive :cu_registered, 1_000
    on_exit(fn -> Process.exit(cu, :kill) end)
  end

  defp await_bound_node(hub_id, operative_id, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_bound_node(hub_id, operative_id, deadline)
  end

  defp do_await_bound_node(hub_id, operative_id, deadline) do
    bound =
      case Map.get(ProcessRegistry.dump(hub_id), operative_id) do
        {_cspec, node_pids, _meta} -> HubManager.live_node(node_pids)
        _ -> nil
      end

    cond do
      not is_nil(bound) ->
        bound

      System.monotonic_time(:millisecond) >= deadline ->
        nil

      true ->
        Process.sleep(50)
        do_await_bound_node(hub_id, operative_id, deadline)
    end
  end

  test "await_recovery_complete/2 times out when a hub is stuck in :recovering" do
    fake_hub_id = :"fake_stuck_hub_#{System.unique_integer([:positive])}"
    # Simulate a hub observed as :recovering that never transitions.
    # The snapshot treats explicit observations as authoritative.
    Flezha.Convergence.Snapshot.observe_recovery(fake_hub_id, :recovering)

    assert {:error, {:recovery_timeout, [^fake_hub_id]}} =
             HubManager.await_recovery_complete([fake_hub_id], 100)
  end
end
