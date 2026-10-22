defmodule Flezha.Service.HubManagerRecoveryTest do
  @moduledoc """
  Exercises the orphan-reconcile recovery contract Flezha consumes from
  upstream.

  Single-node coverage:

    * every boot runs a reconcile round and reaches `:normal`, so
      `await_recovery_complete/1` returns without user action —
      there is no marker to arm;
    * a warm restart against a surviving registry replays the relay
      list and rebinds it;
    * `await_recovery_complete/2` blocks on a missing :complete event
      and times out cleanly.

  The cross-node split-brain detection scenarios require the live k3s
  smoke test; the reconcile / telemetry contract is identical on one
  node and is what flezha-core owns. The upstream process_hub test
  suite covers the multi-peer init_sync semantics.
  """

  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  import FlezhaTest.Helper.HubRegistryTestHelper, only: [started_plugins: 2, await_bound_node: 3]

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin
  alias Flezha.Process.Cspec
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.DurabilityTier.DeclaredManifest
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.DeclaredChildren
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Storage.RemoteManifest

  @namespace "recovery_test_ns"
  # Every boot in this file pays the grace before the first reconcile round
  # runs. Upstream's floor for the grace is 50 ms — it is a one-shot delay,
  # unlike the interval, whose floor stays at 1 s.
  @reconcile_grace_ms 50
  # The runner id baked into the durable cspec by the boot that registered it.
  @recorded_runner "recorded_runner_from_previous_boot"
  @seeded_runner "seeded_live_runner"

  setup %{tmp: tmp_dir} do
    HubRegistryTestHelper.recovering_hub(@namespace, tmp_dir, @reconcile_grace_ms)
  end

  # fails when: recovery needs user action again. Under the marker
  # design a boot only replayed if someone had armed it first, which is
  # what made a peerless cold start lose the fleet.
  test "a boot reaches :normal on its own, with nothing armed beforehand" do
    hub_id = Identificator.hub(@namespace)

    {:ok, _} = HubManager.start_hub(@namespace)

    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)
    assert {:ok, state} = HubManager.recovery_state(hub_id)
    assert state in [:normal, :skipped]
  end

  test "recovery_state surfaces :unknown_hub when the hub is not started" do
    hub_id = Identificator.hub("never_started_namespace")
    assert {:error, :unknown_hub} = HubManager.recovery_state(hub_id)
  end

  # fails when: a warm restart leaves a persisted relay listed but
  # `(unplaced)` — bound to the previous boot's dead pid, invisible to the
  # convergence reconciler, producing no data until the user re-declares it.
  test "recovery replay rebinds a persisted relay even when no runner is placeable" do
    hub_id = Identificator.hub(@namespace)
    relay_id = "replay_no_runner_op"

    replay_hub(hub_id, [{relay_id, "market"}])

    assert await_bound_node(hub_id, relay_id, 10_000) == node(),
           "recovery replay left #{relay_id} bound to a dead pid — `flz relay list` renders it as (unplaced)"

    # The recorded runner is kept rather than the batch dying. It is not a
    # binding: a real Overwatch re-resolves the runner, finds none, and is held
    # unbound until the convergence loop binds it to one that connects.
    assert registry_tag(ProcessRegistry.dump(hub_id), relay_id) == @recorded_runner
  end

  # fails when: the alter hook resolves a runner for each row of a replay
  # batch, so the batch waits on the rows no runner serves and a large one walks
  # past `cross_node_request_timeout`: WorkerQueue kills the request and every
  # row in it keeps its dead pid. The Overwatch's bind is the one resolution.
  test "a replay batch with unplaceable rows starts every row" do
    hub_id = Identificator.hub(@namespace)
    placeable = "a_batch_placeable_op"
    unplaceable = for i <- 1..3, do: "z_batch_unplaceable_task_#{i}"

    seed_runner(@seeded_runner, "market")

    rows =
      [{placeable, "market"}] ++
        Enum.map(unplaceable, &{&1, "factory_no_runner_serves"})

    replay_hub(hub_id, rows)

    for {relay_id, _factory} <- rows do
      assert await_bound_node(hub_id, relay_id, 15_000) == node(),
             "#{relay_id} kept its dead pid — the start request was killed before update_registry"
    end

    # Every row keeps the runner it recorded, a placeable one too: the hook
    # resolves none, and `StubChild` stands in for the bind that would.
    dump = ProcessRegistry.dump(hub_id)

    for {relay_id, _factory} <- rows do
      assert registry_tag(dump, relay_id) == @recorded_runner
    end
  end

  # fails when: the reconcile replays a declaration whose start args name no
  # runner — the only shape a real relay start ever declares — and the alter
  # hook raises instead of degrading. The raise runs inside the hub's shared
  # WorkerQueue, so it takes the worker down with the whole batch, the
  # coordinator's pending work count never comes back, and the next round does
  # it again.
  test "a declaration that records no runner replays without raising" do
    hub_id = Identificator.hub(@namespace)
    relay_id = "declared_no_recorded_runner_op"

    seed_runner(@seeded_runner, "market")
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    # The shape `RelayManager.Init.start_child/2` submits: no runner
    # position at all. The declared entry commits before the alter hook runs and
    # the hook's rewrite only ever reaches the registry row, so this is the shape
    # every later reconcile replays.
    :ok = HubRegistryTestHelper.start_durable(@namespace, relay_id, recorded_runner: nil)

    assert declared_start_arg(relay_id) ==
             {@namespace, %{id: relay_id, factory_id: "market"}}

    {bound_node, log} =
      ExUnit.CaptureLog.with_log(fn ->
        restart_hub(hub_id)
        await_bound_node(hub_id, relay_id, 10_000)
      end)

    refute log =~ "No active compute engines available",
           "the reconcile replay still raises, which kills the hub's shared WorkerQueue"

    assert bound_node == node(),
           "the replay never placed #{relay_id} — the alter hook took the start request with it"

    # The tag is what the hook wrote, the recorded none. A real
    # Overwatch would then be held unbound until the convergence loop binds it;
    # `StubChild` stands still instead, which is what keeps this test about the
    # hook and the batch.
    assert registry_tag(ProcessRegistry.dump(hub_id), relay_id) == nil

    assert NodeTestHelper.wait_until(
             fn -> :sys.get_state(hub_id).pending_work_count == 0 end,
             5_000
           ),
           "the hub still holds queued work — the WorkerQueue died before reporting it complete"
  end

  # fails when: a binding a user made is erased by the very restart it was
  # written to survive. The bind writes the hub's registry row; the reconcile
  # round starts the relay from the DECLARED entry, whose spec was committed
  # at `flz relay start` and carries no bindings, and that start writes its
  # own spec into the row verbatim.
  test "a restart-driven start does not erase a binding" do
    hub_id = Identificator.hub(@namespace)
    relay_id = "plugin_survives_restart_op"
    name = "plugin_survives_restart_plugin"

    seed_runner(@seeded_runner, "market")
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, relay_id,
        recorded_runner: nil,
        opts: []
      )

    assert {:ok, _} = PluginManager.enable_plugin(name, plugin(relay_id, name))

    assert {:ok, [{^name, %Plugin{}}]} = HubManager.relay_bindings(@namespace, relay_id),
           "the bind did not reach a durable record at all"

    restart_hub(hub_id)
    assert await_bound_node(hub_id, relay_id, 10_000) == node()

    # What a restored Overwatch would bring up: `Lifecycle.complete_init/4`
    # hands `relay.opts[:bindings]` from its own start argument to
    # `Plugins.prepare/2`, and that list IS the extension table it builds.
    refute started_plugins(hub_id, relay_id) == [],
           "binding list empty after restart: the restored relay was started " <>
             "from a declared spec that carries no bindings"

    # The convergence I2 backstop reads this. A fix that repaired only the
    # started spec would leave the backstop blind to any later divergence.
    assert {:ok, [{^name, %Plugin{}}]} = HubManager.relay_bindings(@namespace, relay_id),
           "the bindings the convergence backstop reads are empty after a restart"

    # The plugin itself is declared intent of its own and survives with them.
    assert {:ok, %Plugin{id: ^name}} = HubManager.plugin_entry(@namespace, name)
  end

  # fails when: an unbind reaches only the live relay and the declared entry
  # keeps the binding, so the next restart switches back on what a user
  # deliberately switched off, and nothing short of a second unbind stops it.
  test "a restart-driven start does not resurrect an unbound binding" do
    hub_id = Identificator.hub(@namespace)
    relay_id = "plugin_disabled_before_restart_op"
    name = "plugin_disabled_before_restart_plugin"

    seed_runner(@seeded_runner, "market")
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, relay_id,
        recorded_runner: nil,
        opts: []
      )

    assert {:ok, _} = PluginManager.enable_plugin(name, plugin(relay_id, name))
    assert {:ok, [{^name, %Plugin{}}]} = HubManager.relay_bindings(@namespace, relay_id)

    assert :ok = PluginManager.unbind_plugin(@namespace, name, relay_id)
    assert {:ok, []} = HubManager.relay_bindings(@namespace, relay_id)

    restart_hub(hub_id)
    assert await_bound_node(hub_id, relay_id, 10_000) == node()

    assert started_plugins(hub_id, relay_id) == [],
           "the restart brought back a binding the user removed"

    assert {:ok, []} = HubManager.relay_bindings(@namespace, relay_id)
  end

  # fails when: a plugin enabled with no relay lives anywhere a cold start
  # does not read, so a whole-cluster restart forgets it.
  test "a plugin with no relay survives a cold start" do
    hub_id = Identificator.hub(@namespace)
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    entry = %Plugin{id: "cold_plugin", provider: __MODULE__.NoopProvider, namespace: @namespace}
    assert {:ok, "plugin enabled"} = PluginManager.enable_plugin("cold_plugin", entry)

    HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)
    drop_registry_rows()
    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)

    assert {:ok, %Plugin{id: "cold_plugin", provider: __MODULE__.NoopProvider}} =
             HubManager.plugin_entry(@namespace, "cold_plugin")
  end

  # fails when: one persisted row whose start args match no known shape raises
  # inside the alter hook and kills the start request, so every sibling in the
  # batch loses its registry update and stays on a dead pid.
  test "a persisted row with unrecognised start args does not strand its batch" do
    hub_id = Identificator.hub(@namespace)
    good = "batch_good_op"
    malformed = "batch_malformed_op"

    seed_runner(@seeded_runner, "market")

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    HubRegistryTestHelper.inject_relay(@namespace, good, [{node(), dead_pid()}], %{
      id: good,
      start:
        {HubRegistryTestHelper.StubChild, :start_link,
         [{@namespace, @recorded_runner, %{id: good, factory_id: "market"}}]}
    })

    # An arg shape no current clause understands — an older or corrupted row.
    HubRegistryTestHelper.inject_relay(@namespace, malformed, [{node(), dead_pid()}], %{
      id: malformed,
      start: {HubRegistryTestHelper.StubChild, :start_link, [{@namespace, :not_a_relay_map}]}
    })

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        restart_hub(hub_id, drop_declared: true)

        assert await_bound_node(hub_id, good, 10_000) == node(),
               "#{good} kept its dead pid — the malformed sibling killed the whole start request"
      end)

    # Logged-and-skipped, not raised: the row is named so a user can find it.
    assert log =~ ~s(unrecognised start args for child "#{malformed}")
    assert log =~ "leaving it unaltered"
  end

  # fails when: await_runner stops iterating and degrades to the single shot it
  # was meant to replace, so a runner that becomes placeable part-way through the
  # budget is missed and the relay is left unplaced.
  test "await_runner polls repeatedly inside one budget" do
    seed_runner(@seeded_runner, "market")

    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

    # Ready only from the fourth attempt: reaching it at all proves the loop ran.
    ready_fun = fn _runner -> Agent.get_and_update(attempts, &{&1 >= 3, &1 + 1}) end

    assert {:ok, @seeded_runner} =
             RelayManager.await_runner(@namespace, "market", "polling_op", 1_000, ready_fun)

    assert Agent.get(attempts, & &1) >= 4,
           "await_runner gave up after one attempt instead of polling the budget"
  end

  # fails when: a spent budget still blocks on the FleetManager call, so a bind
  # given none waits on a blocked fleet instead of answering at once.
  test "an exhausted budget costs nothing while FleetManager is blocked" do
    :sys.suspend(FleetManager)
    on_exit(fn -> :sys.resume(FleetManager) end)

    started_at = System.monotonic_time(:millisecond)

    for i <- 1..5 do
      # `:timeout`, not `:unavailable` — the budget was gone, the fleet unasked.
      assert RelayManager.await_runner(@namespace, "market", "spent_budget_task_#{i}", 0) ==
               {:error, :timeout}
    end

    elapsed = System.monotonic_time(:millisecond) - started_at

    assert elapsed < 100,
           "5 zero-budget lookups took #{elapsed} ms — each still blocks on the fleet"
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
             RelayManager.await_runner(@namespace, "market", "clamped_op", budget)

    elapsed = System.monotonic_time(:millisecond) - started_at

    # Unclamped, the single attempt runs the full 250 ms cap.
    assert elapsed < 150, "await_runner overran a #{budget} ms budget by #{elapsed - budget} ms"
  end

  # fails when: a FleetManager that never answers within the bind window crashes
  # the Overwatch with an unmatched `{:error, :unavailable}`, so the user
  # sees a CaseClauseError rather than the reason the relay did not come up.
  test "Overwatch holds unbound when FleetManager never answers the bind lookup" do
    on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 60))
    :sys.suspend(FleetManager)
    on_exit(fn -> :sys.resume(FleetManager) end)

    Process.flag(:trap_exit, true)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, pid} =
          Overwatch.start_link(
            {@namespace, @recorded_runner,
             %Builder{id: "unanswered_lookup_op", factory_id: "market"}}
          )

        on_exit(fn ->
          try do
            GenServer.stop(pid)
          catch
            :exit, _ -> :ok
          end
        end)

        # `:unavailable`, not `:timeout`: the fleet was asked and never answered.
        assert NodeTestHelper.wait_until(
                 fn ->
                   match?(
                     {:runner_not_ready, :unavailable},
                     GenServer.call(pid, :get_overwatch_state).halt_reason
                   )
                 end,
                 5_000,
                 5
               )

        refute_received {:EXIT, ^pid, _}
      end)

    # The reason is read off the state above. What must not appear is a crash —
    # a CaseClauseError is an error-level report, which this suite does capture.
    refute log =~ "CaseClauseError"
  end

  # fails when: a bind that finds no runner stops the Overwatch. The restored
  # relays all wait together, so they all stop together, and a namespace
  # declaring more of them than `dsup_max_restarts: 100` per `dsup_max_seconds:
  # 4` — ProcessHub's defaults, which this hub does not override — exhausts the
  # dist_sup's restart intensity and loses its whole supervision tree.
  @tag timeout: 120_000
  test "a runnerless namespace holding more relays than the restart intensity keeps them all" do
    on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 20))

    hub_id = Identificator.hub(@namespace)
    ids = for i <- 1..101, do: "held_burst_#{i}"

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    Enum.each(ids, fn id ->
      HubRegistryTestHelper.inject_relay(@namespace, id, [{node(), dead_pid()}], %{
        id: id,
        restart: :transient,
        start:
          {Overwatch, :start_link, [{@namespace, nil, %Builder{id: id, factory_id: "market"}}]}
      })
    end)

    restart_hub(hub_id, drop_declared: true)

    # The replay binds each child after `await_recovery_complete/2` returns.
    pids =
      Enum.map(ids, fn id ->
        pid =
          NodeTestHelper.wait_until(
            fn ->
              case HubManager.get_pid(@namespace, id) do
                {:ok, pid} when is_pid(pid) -> pid
                _ -> nil
              end
            end,
            10_000
          )

        assert is_pid(pid), "the replay never started #{id}"
        Process.monitor(pid)
        pid
      end)

    # Alive AND held, not alive-and-still-waiting. Every bind has answered, so
    # every stop one could cause has happened, inside one `dsup_max_seconds` window.
    assert NodeTestHelper.wait_until(
             fn ->
               Enum.all?(pids, fn pid ->
                 match?(
                   {:runner_not_ready, _},
                   GenServer.call(pid, :get_overwatch_state).halt_reason
                 )
               end)
             end,
             10_000,
             5
           )

    refute_received {:DOWN, _, :process, _, _}

    assert Enum.all?(pids, &Process.alive?/1),
           "the namespace lost relays its dist_sup gave up restarting"

    # The supervision tree itself: `is_partitioned?/1` is true once the local
    # distributed supervisor is gone, which is how the intensity is exhausted.
    assert ProcessHub.is_alive?(hub_id)
    refute ProcessHub.is_partitioned?(hub_id)
  end

  describe "declared intent" do
    # fails when: losing the runtime registry loses intent — the declared set
    # must survive row loss and the reconcile must restore from it.
    test "a cold start restores the declared set after registry-row loss" do
      hub_id = Identificator.hub(@namespace)
      relay_id = "declared_cold_start_op"

      {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
      assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

      start_durable_stub(hub_id, relay_id)
      assert relay_id in declared_ids()

      HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)
      drop_registry_rows()

      {:ok, _} = HubManager.start_hub(@namespace)
      assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)

      assert relay_id in declared_ids()

      assert await_bound_node(hub_id, relay_id, 10_000) == node(),
             "the declared set survived row loss but the reconcile did not restore from it"
    end

    # fails when: a stop is only remembered for a window — the entry must be
    # absent from the declared set and stay absent across any later boot,
    # however long the gap (there is no TTL left to expire).
    test "a deliberate stop removes the declared entry and no later boot resurrects it" do
      hub_id = Identificator.hub(@namespace)
      relay_id = "declared_stop_op"

      {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
      assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

      start_durable_stub(hub_id, relay_id)
      assert relay_id in declared_ids()

      # The flezha stop seam: declared removal rides the hub stop path
      # (remove-before-terminate), and the registry row goes with it.
      assert :ok = RelayManager.stop_relay(@namespace, relay_id)
      refute relay_id in declared_ids()
      refute Map.has_key?(ProcessRegistry.dump(hub_id), relay_id)

      restart_hub(hub_id)

      refute relay_id in declared_ids()
      refute Map.has_key?(ProcessRegistry.dump(hub_id), relay_id)
    end

    # fails when: the declared set has no off-cluster copy — losing every
    # cluster disk must still restore intent through the durability tier.
    test "total disk loss restores the declared set from the durability tier" do
      tier_dir =
        Path.join([System.tmp_dir!(), "flezha_tier_#{System.unique_integer([:positive])}"])

      File.mkdir_p!(tier_dir)

      Application.put_env(:flezha, :durability_tier,
        backend: :local_path,
        backend_opts: [path: tier_dir]
      )

      on_exit(fn -> File.rm_rf(tier_dir) end)

      hub_id = Identificator.hub(@namespace)
      relay_id = "declared_disk_loss_op"

      {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
      assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

      start_durable_stub(hub_id, relay_id)
      assert relay_id in declared_ids()

      # The remote ship is asynchronous — wait for the tier to hold a copy
      # that already declares the relay.
      assert NodeTestHelper.wait_until(
               fn -> relay_id in remote_declared_ids(hub_id) end,
               10_000
             ),
             "the declared list never reached the durability tier"

      HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)
      drop_all_registry_files()

      {:ok, _} = HubManager.start_hub(@namespace)
      assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)

      assert relay_id in declared_ids(),
             "the boot did not adopt the declared set from the remote manifest"

      assert await_bound_node(hub_id, relay_id, 10_000) == node()
    end
  end

  # Registry rows left by a previous boot (dead pids) and NO declared list —
  # the on-disk shape of a deployment upgrading from before the
  # declared-children model. The restart's first-enablement seed mints the
  # declared list from these rows, and the same boot's reconcile replays them.
  defp replay_hub(hub_id, relays) do
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    Enum.each(relays, fn {relay_id, factory_id} ->
      HubRegistryTestHelper.inject_relay(@namespace, relay_id, [{node(), dead_pid()}], %{
        id: relay_id,
        start:
          {HubRegistryTestHelper.StubChild, :start_link,
           [{@namespace, @recorded_runner, %{id: relay_id, factory_id: factory_id}}]}
      })
    end)

    restart_hub(hub_id, drop_declared: true)
  end

  # The warm restart itself: keep the registry, come back up. Every boot
  # reconciles now, so nothing has to be armed between the two.
  # `drop_declared: true` removes the declared list and its seeded marker
  # between the stop and the start, so the boot re-seeds from registry rows —
  # the pre-declared-list upgrade shape injected rows rely on.
  defp restart_hub(hub_id, opts \\ []) do
    HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)

    if Keyword.get(opts, :drop_declared, false), do: drop_declared_list()

    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)
  end

  # Must run only while the hub is down: the declared store holds the file
  # open and would recreate it on close.
  defp drop_declared_list do
    Application.get_env(:flezha, :process_hub_registry_root)
    |> Path.join("**/*.declared.dets*")
    |> Path.wildcard()
    |> Enum.each(&File.rm/1)
  end

  # Registry rows lost, declared list kept — the divergence the declared set
  # exists to survive. Hub must be down (same reason as `drop_declared_list/0`).
  defp drop_registry_rows do
    Application.get_env(:flezha, :process_hub_registry_root)
    |> Path.join("**")
    |> Path.wildcard()
    |> Enum.reject(&(File.dir?(&1) or String.contains?(&1, ".declared.dets")))
    |> Enum.each(&File.rm/1)
  end

  # Everything on "the cluster's disks": rows AND the local declared list.
  defp drop_all_registry_files do
    Application.get_env(:flezha, :process_hub_registry_root)
    |> Path.join("**")
    |> Path.wildcard()
    |> Enum.reject(&File.dir?/1)
    |> Enum.each(&File.rm/1)
  end

  # The durable start every real start rides, from the fixture helper.
  defp start_durable_stub(_hub_id, relay_id) do
    :ok = HubRegistryTestHelper.start_durable(@namespace, relay_id)
  end

  # Through flezha's own declared reader — the seam Convergence.Intent uses.
  defp declared_ids do
    HubManager.declared_relays(@namespace) |> Enum.map(& &1.id)
  end

  defp remote_declared_ids(hub_id) do
    with {:ok, {_version, blob}} <-
           DeclaredManifest.fetch(hub_id, []),
         {:ok, %{entries: entries}} <- RemoteManifest.decode(blob) do
      Map.keys(entries)
    else
      _ -> []
    end
  end

  # The single start argument of the hub's declared entry — the manifest the
  # reconcile replays from, which is not the registry row the alter hook rewrote.
  defp declared_start_arg(relay_id) do
    %{children: children} = DeclaredChildren.declared_children(Identificator.hub(@namespace))
    {:ok, arg} = children |> Enum.find(&(&1.id == relay_id)) |> Cspec.start_arg()
    arg
  end

  # The owning runner recorded on a registry row (`metadata.tag`).
  defp registry_tag(dump, relay_id) do
    {_cspec, _node_pids, metadata} = Map.fetch!(dump, relay_id)
    metadata[:tag]
  end

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    pid
  end

  defp seed_runner(runner_id, factory_id),
    do: HubRegistryTestHelper.seed_runner(@namespace, runner_id, factory_id)

  defp plugin(relay_id, plugin_id) do
    Plugin.new(
      id: plugin_id,
      provider: __MODULE__.NoopProvider,
      opts: %{},
      hooks: [{:on_output, []}],
      namespace: @namespace,
      relay_id: relay_id
    )
  end

  defmodule NoopProvider do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_output(ctx, _opts), do: {:ok, ctx}
  end

  test "await_recovery_complete/2 times out when a hub is stuck in :recovering" do
    fake_hub_id = :"fake_stuck_hub_#{System.unique_integer([:positive])}"
    # Simulate a hub observed as :recovering that never transitions.
    # The snapshot treats explicit observations as authoritative.
    ConvergenceSnapshot.observe_recovery(fake_hub_id, :recovering)

    assert {:error, {:recovery_timeout, [^fake_hub_id]}} =
             HubManager.await_recovery_complete([fake_hub_id], 100)
  end
end
