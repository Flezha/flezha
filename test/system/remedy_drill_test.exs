defmodule FlezhaTest.System.RemedyDrillTest do
  @moduledoc """
  Every remedy driven end to end: one row per primitive of `Heal.primitives/0`,
  each injecting the fault the Diff plans that primitive for — through the
  real transport, against the Rust runner fixture, with a real
  `Convergence.Loop` — and asserting that `[:flezha, :convergence, :heal]`
  fires naming THAT primitive and target, that a fault held across the next
  window advances the primitive's key, and that repair resets it. A rebind with
  no runner to place on is the exception: that is an unmet precondition, and it
  arms no rung.

  The row set is read from the primitive table, so a new primitive fails here
  by name until its fault is written; a row whose fault this tier cannot
  inject is a recorded deferral, never a skip of the module. The drain — the
  one remedy-shaped action outside the primitives — has its own row.

  Tagged `:system`; run with
  `mix test --include system test/system/remedy_drill_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle
  use Mimic
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MimicGlobal

  import FlezhaTest.Helper.NodeTestHelper,
    only: [crash_report: 2, wait_until: 2, with_only_logged: 2]

  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [
      boot_runner: 3,
      os_pid: 1,
      produces?: 5,
      sigkill: 1,
      stop_namespace: 1,
      unique_id_path: 1
    ]

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Heal, Loop, Planner}
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Identificator
  alias Flezha.Relay.{Builder, Overwatch}
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.{Ctx, Table}
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{HubManager, RelayManager, PluginManager}
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.{Commands, TestHelpers, WalWriter, Watchdog, WriterRegistry}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.Telemetry
  alias ProcessHub.Service.ProcessRegistry

  setup :set_mimic_global_until_exit

  @tick_ms 1_000
  @grace_ms 30_000
  @heartbeat_ms 300

  # A provider that declares a worker: the table starts it at activation and
  # monitors it, so a killed worker is what a crashed subscriber is.
  defmodule OwnedPidProvider do
    use GenServer
    use Flezha.Plugin.ProviderBehaviour, worker: [module: __MODULE__, restart: :temporary]

    @impl true
    def worker_specs(opts), do: [{opts.source_plugin_id, %{}}]

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}

    def start_link(%{name: name}), do: GenServer.start_link(__MODULE__, :ok, name: name)

    @impl GenServer
    def init(:ok), do: {:ok, nil}
  end

  # Module-wide, so the family outlives each test's namespace teardown, which
  # stops the workers it holds.
  setup_all do
    start_supervised!({WorkerSup, name: WorkerSup.family(OwnedPidProvider)})
    :ok
  end

  setup do
    namespace = "remedy_drill_#{System.unique_integer([:positive])}"

    # BEFORE any hub exists: the cache is cleared so a hub another suite left
    # mid-recovery cannot hold this loop's gate shut, and this namespace's own
    # hub then seeds itself `:normal` as it starts. Clearing it afterwards
    # would drop that seed and the gate would exclude every namespace, so the
    # loop would plan nothing at all.
    ConvergenceSnapshot.reset_recovery_states()
    capture_as([:flezha, :convergence, :heal], :heal)
    capture_as([:flezha, :convergence, :tick], :tick)

    on_exit(fn ->
      Application.delete_env(:flezha, :overwatch_ping_fn)

      stop_namespace(namespace)
    end)

    %{namespace: namespace}
  end

  # The rows this tier cannot drive, each with the reason recorded.
  @deferred %{
    reset_control_channel:
      "needs a relay that is PARKED and still in the hub's declared set: the start " <>
        "authority stops a child whose start fails, and an Overwatch started outside it is " <>
        "not declared, so the loop never sees it. The park-to-cut path is proved by " <>
        "`Flezha.Convergence.DiffTest` (the Planner cuts a dead transport before the rebind, " <>
        "and a timed-out one only after two rungs) and by `lifecycle_test` (the park declares " <>
        "its reason and its transport verdict)"
  }

  # Two rows drive a fixture of another capability, so each declares its own
  # below rather than riding the module's: a row is tagged with the capability
  # whose fixture it needs.
  @own_capability [:resync_runner, :restart_runner]

  for primitive <- Heal.primitives(), primitive not in @own_capability do
    @tag primitive: primitive
    @tag skip: Map.get(@deferred, primitive)
    @tag timeout: 120_000
    test "#{primitive}: the loop plans and executes it from its own fault", %{
      primitive: primitive,
      namespace: namespace
    } do
      row(primitive, namespace)
    end
  end

  # `resync_runner` needs a Cell with persisted state, so it drives the
  # `store_input` fixture.
  describe "resync_runner" do
    @describetag capability: :state_persistence,
                 skip: FlezhaTest.SystemCase.deferral(:state_persistence)
    @describetag timeout: 120_000

    test "the loop plans and executes it from every fault that names a Cell", %{
      namespace: namespace
    } do
      row(:resync_runner, namespace)
    end
  end

  # `restart_runner` needs a runner that can be wedged, so it drives the
  # `wedge_probe` fixture; the wedge itself is a second deferral inside.
  describe "restart_runner" do
    @describetag capability: :liveness, skip: FlezhaTest.SystemCase.deferral(:liveness)
    @describetag timeout: 180_000

    test "the loop plans and executes it from the pool wedge", %{namespace: namespace} do
      row(:restart_runner, namespace)
    end
  end

  @tag timeout: 120_000
  test "the drain declares itself, and the loop plans nothing local while it stands", %{
    namespace: namespace
  } do
    row(:drain, namespace)
  end

  # ---------- the rows ----------

  # A runner dies with no survivor: the declared disconnect plans the rebind.
  # With no runner able to serve the relay the rebind is an unmet
  # precondition, so it arms no rung and is planned again on the very next
  # tick; a runner arriving is the repair.
  defp row(:rebind_relay, namespace) do
    {port_a, runner_a} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "rebound")
    assert bound_runner(namespace, id) == runner_a
    start_loop()

    sigkill(os_pid(port_a))

    for _tick <- 1..2 do
      assert_receive {:heal, %{primitive: :rebind_relay, target: ^id}}, 30_000
    end

    assert none_cooling?(namespace, id), "a rebind with nowhere to place armed a rung"

    {_port_b, runner_b} = boot_runner(namespace, unique_id_path("drill"), [])

    assert wait_until(fn -> bound_runner(namespace, id) == runner_b end, 30_000),
           "the relay never rebound onto the arriving runner"

    # A rebind that answered `:error` on its way here would have armed a rung
    # that the quiet tick below cannot see.
    assert none_cooling?(namespace, id), "the repair left a rung armed"
    assert quiet_tick()
  end

  # The dead runner's eviction is the first op of the same plan.
  defp row(:evict_runner, namespace) do
    {port_a, runner_a} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "evicted")
    assert bound_runner(namespace, id) == runner_a
    start_loop()

    sigkill(os_pid(port_a))
    assert_receive {:heal, %{primitive: :evict_runner, target: ^runner_a}}, 30_000
    refute runner_a in FleetManager.active_runners(namespace)
    assert attempt({:evict_runner, runner_a}) == 0, "an idempotent eviction resets its key"
  end

  # A killed subscriber: the table records it down, the Overwatch declares it,
  # the loop restarts it. The key stays armed until the stable window.
  defp row(:restart_plugin, namespace) do
    {_port, _runner} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "restarted")
    plugin_id = id <> "_owned"
    :ok = enable_plugin(namespace, id, plugin_id)
    {:ok, pid} = RelayManager.get_relay_pid(namespace, id)
    start_loop()

    owned = owned_pid(pid, plugin_id)

    # The kill is the fault, and its supervisor and the table both report it.
    log =
      capture_log(fn ->
        Process.exit(owned, :kill)

        assert wait_until(
                 fn -> Loop.tick_now() == :ok and heal?(:restart_plugin, id) end,
                 30_000
               ),
               "the loop never planned restart_plugin for the crashed subscriber"

        assert wait_until(
                 fn ->
                   new = owned_pid(pid, plugin_id)
                   is_pid(new) and new != owned and Process.alive?(new)
                 end,
                 10_000
               ),
               "the subscriber was never restarted"
      end)

    assert log =~ "subscriber #{plugin_id} crashed: :killed"

    assert attempt({:restart_plugin, {id, plugin_id}}) >= 1,
           "a restart is a ladder: the key stays armed until the stable window"
  end

  # A bind whose ping reports the transport dead: the park is declared, the
  # channel is cut before the rebind, the parked ladder walks; the runner
  # coming back is the repair.
  # A registry write landing after the bind names the old runner: the hook
  # declares it, the probe reads the row beside the binding, and the row is
  # re-published. Inline: the key resets on the publish's own `:ok`.
  defp row(:publish_binding, namespace) do
    {_port, runner} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "republished")
    assert bound_runner(namespace, id) == runner
    start_loop()

    for _ <- 1..2 do
      write_stale_tag(namespace, id, "runner_from_a_previous_boot")
      assert_receive {:heal, %{primitive: :publish_binding, target: ^id}}, 30_000
      assert wait_until(fn -> registry_tag(namespace, id) == runner end, 10_000)
      assert attempt({:publish_binding, id}) == 0
    end

    assert quiet_tick()
  end

  # Three faults, one primitive: a seq gap the WAL declares, a compaction the
  # Watchdog gives up on, and an output stall — each keyed on its own reason.
  defp row(:resync_runner, namespace) do
    {_port, runner} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "resynced", factory: "store_input")
    cell = {id, "store_input_tasklet", ""}
    feed(namespace, id, 5)
    {:ok, writer} = wait_for_writer(cell)
    start_loop()

    # --- a frame dropped on the secondary stream: the WAL sees a gap --------
    last = WalWriter.last_persisted_seq(writer)
    WalWriter.append_async(writer, last + 100, TestHelpers.stub_header(), "gap", runner)
    assert_receive {:heal, %{primitive: :resync_runner, target: ^cell}}, 30_000
    assert wait_until(fn -> attempt({:seq_gap, cell}) >= 1 end, 10_000)
    # The next contiguous append closes the episode.
    feed(namespace, id, 1)
    assert wait_until(fn -> attempt({:seq_gap, cell}) == 0 end, 30_000)

    # --- a compaction request never answered for four sweeps ----------------
    on_exit(TestHelpers.override_env(:compaction_timeout_ms, 50))

    gave_up = ~r/\[watchdog\] compaction GAVE UP after 4 failed attempts relay=#{id} /

    with_only_logged([{gave_up, :always}], fn ->
      for _ <- 1..4 do
        Watchdog.compaction_started(cell, runner)
        age_compaction(cell, 200)
        :ok = Watchdog.sweep_now()
      end
    end)

    assert_receive {:heal, %{primitive: :resync_runner, target: ^cell}}, 30_000
    assert wait_until(fn -> attempt({:compaction_gave_up, cell}) >= 1 end, 10_000)
    # A real compaction completing is the counter-fact.
    {:ok, _ref} = Commands.dispatch_compact_cell(runner, cell)
    assert wait_until(fn -> attempt({:compaction_gave_up, cell}) == 0 end, 30_000)

    # --- the persisted seq stops advancing under input -----------------------
    on_exit(TestHelpers.override_env(:default_output_stall_timeout_ms, 200))
    TestHelpers.age_writer(writer, 1_000)

    :duplicate =
      WalWriter.append(
        writer,
        WalWriter.last_persisted_seq(writer),
        TestHelpers.stub_header(),
        "dup"
      )

    :ok = Watchdog.sweep_now()

    assert_receive {:heal, %{primitive: :resync_runner, target: ^id, namespace: ^namespace}},
                   30_000

    assert wait_until(fn -> attempt({:output_stall, id}) >= 1 end, 10_000)
    # The Watchdog going quiet past the grace is the evidence.
    assert wait_until(fn -> attempt({:output_stall, id}) == 0 end, @grace_ms + 10_000)
    assert quiet_tick()
  end

  # A declared plugin unsubscribed is started again.
  defp row(:start_plugin, namespace) do
    {_port, _runner} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "started")
    plugin_id = id <> "_declared"
    :ok = enable_plugin(namespace, id, plugin_id)
    {:ok, pid} = RelayManager.get_relay_pid(namespace, id)
    start_loop()

    :ok = Overwatch.plugin_unsubscribe(pid, plugin_id)
    refute running?(pid, plugin_id)

    assert wait_until(fn -> Loop.tick_now() == :ok and heal?(:start_plugin, id) end, 30_000),
           "the loop never planned start_plugin for the missing declared plugin"

    assert wait_until(fn -> running?(pid, plugin_id) end, 10_000)
    assert attempt({:start_plugin, {id, plugin_id}}) == 0, "an inline repair resets its key"
  end

  # An undeclared plugin subscribed is torn down.
  defp row(:stop_plugin, namespace) do
    {_port, _runner} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "stopped")
    :ok = enable_plugin(namespace, id, id <> "_declared")
    {:ok, pid} = RelayManager.get_relay_pid(namespace, id)
    start_loop()

    undeclared = id <> "_undeclared"

    :ok =
      Overwatch.plugin_subscribe(pid, undeclared, %Plugin{
        id: undeclared,
        provider: OwnedPidProvider,
        opts: %{},
        hooks: [{:on_input, [position: 1]}],
        namespace: namespace,
        relay_id: id
      })

    assert wait_until(fn -> Loop.tick_now() == :ok and heal?(:stop_plugin, id) end, 30_000),
           "the loop never planned stop_plugin for the undeclared plugin"

    assert wait_until(fn -> not subscribed?(pid, undeclared) end, 10_000)
    assert attempt({:stop_plugin, {id, undeclared}}) == 0
  end

  # The pool wedge: the verdict's first rung is the terminal restart.
  defp row(:restart_runner, namespace) do
    if reason = FlezhaTest.SystemCase.deferral(:pool_wedge) do
      IO.puts("restart_runner: #{reason}")
    else
      capture_as([:flezha, :data_plane, :stall_localized], :verdict)

      {port_a, runner_a} =
        boot_runner(namespace, unique_id_path("drill"),
          env: heartbeat_env(),
          port_opts: [:exit_status]
        )

      id = start_relay(namespace, "wedged", factory: "wedge_probe", plugins: [sink(self())])
      assert produces?(id, namespace, "one", "ok:one", 60_000)

      RelayManager.send_io(id, namespace, "wedge", %{"input_type" => "2"})
      assert wait_until(fn -> inflight?(runner_a) end, 30_000), "the wedge never reached a worker"
      feeder = start_feeder(id, namespace, self())
      {_port_b, runner_b} = boot_runner(namespace, unique_id_path("drill"), env: heartbeat_env())
      start_loop()

      assert_receive {:verdict, %{layer: :runner_pool_wedged, runner_id: ^runner_a}}, 60_000
      assert_receive {:heal, %{primitive: :restart_runner, target: ^runner_a}}, 60_000
      assert_receive {^port_a, {:exit_status, status}}, 60_000
      assert status != 0
      send(feeder, :stop)

      assert wait_until(fn -> bound_runner(namespace, id) == runner_b end, 120_000)
      assert produces?(id, namespace, "after", "ok:after", 60_000)
    end
  end

  # The exception: the drain declares itself; while it stands the loop plans
  # nothing onto the node, and the fact clears at finalise. The rebind AFTER a
  # drain is not asserted here — the drain stops the hub manager, so there is
  # no hub left in this VM to rebind into; `Flezha.ShutdownTest` proves the
  # Planner's suppression against a populated Diff.
  defp row(:drain, namespace) do
    {port_a, runner_a} = boot_runner(namespace, unique_id_path("drill"), [])
    id = start_relay(namespace, "drained")
    assert bound_runner(namespace, id) == runner_a
    start_loop()

    stub(Flezha.State.FsyncBatcher, :flush_all, fn -> :ok end)
    stub(Flezha.Transport.Server, :stop_all, fn -> :ok end)
    on_exit(fn -> Coordinator.__reset_for_tests__() end)

    # The runner dies mid-drain: a disconnect the loop would rebind on any
    # other day. While the drain stands it plans nothing local.
    test_pid = self()

    stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
      sigkill(os_pid(port_a))
      assert wait_until(fn -> draining?() end, 10_000), "the drain was never declared"
      :ok = Loop.tick_now()
      send(test_pid, :ticked_under_the_drain)
      {:error, :no_target_nodes}
    end)

    # Finalise stops the hub manager, which its supervisor reports and restarts;
    # a tick caught calling it meanwhile aborts.
    hub_manager = Process.whereis(HubManager)

    aborted =
      ~r/\[convergence\] tick aborted \(exit\): \{:(normal|noproc), \{GenServer, :call, \[#{inspect(HubManager)},/

    with_only_logged([{crash_report(HubManager, "normal"), :always}, {aborted, :maybe}], fn ->
      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 15_000)
      assert_receive :ticked_under_the_drain, 30_000
      assert wait_until(fn -> Process.whereis(HubManager) not in [nil, hub_manager] end, 10_000)
      # A tick runs in the Loop's own process, so this waits out one in flight.
      _ = :sys.get_state(Loop)
    end)

    refute heal?(:rebind_relay, id)

    # Finalise clears the fact.
    assert wait_until(fn -> not draining?() end, 10_000),
           "the drain fact outlived finalise"
  end

  # ---------- helpers ----------

  defp start_loop() do
    start_supervised!({Loop, tick_ms: @tick_ms, output_stall_grace_ms: @grace_ms})
    assert_receive {:tick, _}, 10_000
  end

  defp start_relay(namespace, label, opts \\ []) do
    id = "#{label}_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: id,
      factory_id: Keyword.get(opts, :factory, "do_nothing"),
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, id, Keyword.get(opts, :plugins, []))]
    }

    assert {:ok, _} = RelayManager.start_relay(namespace, builder)
    id
  end

  # The loop reads the DECLARED plugin set from the durable cspec, which a
  # start's own `plugins` and `PluginManager.enable_plugin/2` both write.
  defp enable_plugin(namespace, relay_id, plugin_id) do
    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(plugin_id, %Plugin{
        id: plugin_id,
        namespace: namespace,
        relay_id: relay_id,
        provider: OwnedPidProvider,
        opts: %{},
        hooks: [{:on_input, [position: 0]}]
      })

    :ok
  end

  defp sink(recv_pid) do
    %Plugin{
      id: "drill_sink",
      namespace: nil,
      relay_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid, include_metadata: true},
      hooks: [{:on_output, []}]
    }
  end

  defp owned_pid(pid, plugin_id), do: Table.owned_pid(:sys.get_state(pid).extensions, plugin_id)

  defp running?(pid, plugin_id),
    do: plugin_id in Table.running_ids(:sys.get_state(pid).extensions)

  defp subscribed?(pid, plugin_id),
    do: plugin_id in Table.subscribed_ids(:sys.get_state(pid).extensions)

  # Every heal event collected so far names `primitive` for `target`. The
  # collector delivers into this process's mailbox, so a row can drive ticks
  # and ask, instead of racing one `assert_receive`.
  defp heal?(primitive, target) do
    Enum.any?(drain_heals(), &(&1.primitive == primitive and &1.target == target))
  end

  defp drain_heals(acc \\ []) do
    receive do
      {:heal, meta} -> drain_heals([meta | acc])
    after
      0 ->
        Enum.each(acc, &send(self(), {:heal, &1}))
        acc
    end
  end

  defp attempt(key), do: Backoff.attempt(:sys.get_state(Loop).backoff, key)

  # Every cooldown key a rebind of `id` can be held on, none of them armed.
  defp none_cooling?(namespace, id),
    do: Enum.all?(Planner.waiting_keys({namespace, id}), &(attempt(&1) == 0))

  defp draining?(), do: Map.has_key?(:sys.get_state(Loop).facts, {:node_draining, node()})

  # The tick after repair plans nothing: drain the collected ticks, take the
  # next fresh one and read its count.
  defp quiet_tick() do
    :ok = Loop.tick_now()
    flush_ticks()
    :ok = Loop.tick_now()
    assert_receive {:tick, %{actions_issued: issued}}, 10_000
    issued == 0
  end

  defp flush_ticks() do
    _ = collect(do: ({:tick, _} -> :ok))
    :ok
  end

  defp bound_runner(namespace, relay_id) do
    with {:ok, pid} <- RelayManager.get_relay_pid(namespace, relay_id),
         %{runner_id: runner_id} <- :sys.get_state(pid) do
      runner_id
    else
      _ -> nil
    end
  end

  defp registry_tag(namespace, id), do: HubManager.relay_runner_tag(namespace, id)

  # Byte-for-byte what a start's late `update_registry` writes when it wins the
  # race: the altered child_data's metadata, carrying the runner alter resolved.
  defp write_stale_tag(namespace, id, stale) do
    hub_id = Identificator.hub(namespace)
    hub = ProcessHub.Hub.get(hub_id)
    {cspec, node_pids, _meta} = ProcessRegistry.lookup(hub_id, id, with_metadata: true)

    :ok =
      ProcessRegistry.insert(hub_id, cspec, node_pids,
        metadata: %{tag: stale},
        hook_storage: hub.storage.hook
      )
  end

  defp feed(namespace, id, n) do
    for i <- 1..n do
      RelayManager.send_io(
        id,
        namespace,
        "line-#{i}-#{System.unique_integer([:positive])}",
        %{
          "input_type" => "2"
        }
      )
    end
  end

  defp wait_for_writer(cell) do
    wait_until(fn -> match?({:ok, _}, WriterRegistry.lookup(cell)) end, 30_000)
    WriterRegistry.lookup(cell)
  end

  defp age_compaction(cell_id, ms) do
    :sys.replace_state(Watchdog, fn state ->
      case Map.get(state.compactions, cell_id) do
        {started_at, runner} ->
          %{state | compactions: Map.put(state.compactions, cell_id, {started_at - ms, runner})}

        nil ->
          state
      end
    end)
  end

  defp heartbeat_env(), do: [{"FLEZHA_DATA_HEARTBEAT_MS", Integer.to_string(@heartbeat_ms)}]

  defp inflight?(runner_id) do
    LivenessCounters.snapshot_all().channels
    |> Enum.any?(&(&1.runner_id == runner_id and Map.get(&1, :hb_inflight, 0) >= 1))
  end

  defp start_feeder(relay_id, namespace, owner) do
    spawn(fn ->
      Process.monitor(owner)
      feed_loop(relay_id, namespace)
    end)
  end

  defp feed_loop(relay_id, namespace) do
    receive do
      :stop -> :ok
      {:DOWN, _ref, :process, _pid, _reason} -> :ok
    after
      100 ->
        RelayManager.send_io(relay_id, namespace, "feed", %{"input_type" => "2"})
        feed_loop(relay_id, namespace)
    end
  end

  defp capture_as(event, tag),
    do: Telemetry.capture(event, as: fn _e, meas, meta -> {tag, Map.merge(meas, meta)} end)
end
