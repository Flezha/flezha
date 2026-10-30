defmodule FlezhaTest.Relay.OverwatchRetryTest do
  @moduledoc """
  The one-attempt bind, the parked-error state and the end-to-end ping. The
  Overwatch's one startup attempt is observed through
  `RelayManager.start_relay`: a ping that answers returns `{:ok, _}`; a
  ping that fails parks the relay with its reason, declares the park to the
  convergence loop, and surfaces `{:error, {:ping_failed, _}}` through the
  failed start's stop. The un-park is the loop's `rebind_relay`, driven here
  through `:rebind`. A process's departure and its shutdown bound close it.

  Uses the `:overwatch_ping_fn` Application-env hook the Overwatch consults
  during a startup attempt.
  """

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  # async: false: writes Application env; the live controller and its fleet; node-global tables.

  use FlezhaTest.Case, fixtures: [:env], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @namespace "default"
  @factory "store_input"

  setup :set_mimic_global_until_exit

  setup do
    # Each relay starts bound to a plugin these tests build whole.
    stub(Flezha.Service.PluginManager, :resolve, fn _ns, plugin, _entry -> {:ok, plugin} end)
    test_pid = self()
    ets = :ets.new(:overwatch_retry_test, [:public, :set])

    # One test seeds the namespace's fleet; the state is process-global.
    fleet_state = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)

    {:ok, test_pid: test_pid, ets: ets, fleet: fleet_state}
  end

  defmodule QuietProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        report_to: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}

    @impl true
    def activate(%{report_to: pid}) do
      send(pid, :activated)
      :ok
    end

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  test "5.4 ping success proceeds to start_relay_sdk and activates plugins",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn runner_id ->
      send(test_pid, {:ping_called, runner_id, ClassRegistry.owner("ping_success_relay")})
      {:ok, :pong}
    end)

    plugin = %Plugin{
      id: "ping_success_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      hooks: [{:on_input, [position: 0]}]
    }

    relay = %Builder{
      id: "ping_success_relay",
      factory_id: @factory,
      opts: [bindings: [plugin]]
    }

    stub(Flezha.State.Adoption, :adopt_newest, fn class, _source, _owner ->
      send(test_pid, {:adopted, class})
      :ok
    end)

    assert {:ok, _} = RelayManager.start_relay(@namespace, relay)

    # On arrival, and again before the start: the class still holds nothing.
    assert_receive {:adopted, "ping_success_relay"}, 2_000
    assert_receive {:adopted, "ping_success_relay"}, 2_000

    # The start's ping is issued after the bind and right before the start, so
    # the ownership record already names the runner the relay starts on.
    assert_receive {:ping_called, runner_id, {:ok, _overwatch, owner}}, 2_000
    assert owner == Marks.identity_of(runner_id)
    assert_receive :activated, 2_000
  end

  test "a remote class is refused at start on a controller with no durability backend" do
    refute Flezha.State.DurabilityTier.enabled?(), "the suite runs with no durability backend"
    relay = %Builder{id: "remote_without_backend_op", factory_id: "sync_durable_steps"}

    assert {:error, {:start_failed, "remote_without_backend_op", reason}} =
             RelayManager.start_relay(@namespace, relay)

    assert reason == {:remote_without_backend, "sync_durable_steps"}
    error = Flezha.Error.from_reason(reason, nil)
    assert error.code == "relay.remote_without_backend"
    assert error.message =~ "FLEZHA_DURABILITY_BACKEND"
    assert error.details == %{"factory" => "sync_durable_steps"}
  end

  test "5.1 a failed attempt parks at once with its reason, declares the park, and is not retried in place",
       %{test_pid: test_pid, ets: ets} do
    become_loop()

    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, attempt ->
        send(test_pid, {:ping_attempt, attempt})
        {:error, :timeout}
      end)
    )

    relay = %Builder{id: "one_attempt_relay", factory_id: @factory, opts: []}

    assert {:error, {:start_failed, _id, {:ping_failed, :timeout}}} =
             RelayManager.start_relay(@namespace, relay)

    # One start attempt after the readiness gate, and no second one of its own.
    assert_receive {:ping_attempt, 1}, 2_000
    refute_received {:ping_attempt, 2}

    assert_receive {:declare, :relay_halted, {@namespace, "one_attempt_relay"},
                    %{reason: {:ping_failed, :timeout}}},
                   2_000
  end

  test "6.4 a halted relay rebinds: pings, starts, clears halt_reason, activates plugins",
       %{test_pid: test_pid, ets: ets} do
    # Start the relay so the readiness gate passes but the START ping fails,
    # parking it (alive, halt_reason set). Started directly (not via
    # RelayManager) so the parked-and-alive GenServer is not stopped as a
    # failed start — this is the cold-restart parked state.
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, _attempt -> {:error, :timeout} end)
    )

    plugin = %Plugin{
      id: "unpark_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      hooks: [{:on_input, [position: 0]}]
    }

    relay = %Builder{id: "unpark_relay", factory_id: @factory, opts: [bindings: [plugin]]}

    {:ok, pid} = Overwatch.start_link({@namespace, "stale_ce", relay})

    on_exit(fn ->
      try do
        NodeTestHelper.ensure_stopped(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    # It parks (halt_reason set) and never activates the plugin while parked.
    assert NodeTestHelper.wait_until(
             fn -> overwatch_state(pid).halt_reason != nil end,
             2_000,
             20
           )

    assert Overwatch.phase(overwatch_state(pid)) == :halted

    refute_received :activated

    # A healthy runner is now reachable — rebind.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)
    runner_id = RelayManager.assign_runner(@namespace, @factory, "unpark_relay")
    assert is_binary(runner_id)

    assert :ok = GenServer.call(pid, {:rebind, runner_id})

    # halt_reason cleared and the plugin activated (resumes producing).
    # `:rebind` is async (the bind runs in a
    # `Task.Supervisor.async_nolink` task), so we poll the state instead of
    # asserting immediately.
    assert_receive :activated, 5_000

    assert NodeTestHelper.wait_until(
             fn -> is_nil(overwatch_state(pid).halt_reason) end,
             2_000,
             20
           )

    state = overwatch_state(pid)
    assert Table.phase(state.extensions, "unpark_plugin") == :running
    assert Overwatch.phase(state) == :running
  end

  defp overwatch_state(pid), do: GenServer.call(pid, :get_overwatch_state)

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the one start attempt. These tests drive the START ping to
  # fail; the readiness gate must still pass or the relay is held
  # unbound instead of exercising the park behaviour.
  # First call = readiness gate (pong); the next = the start attempt (run body).
  defp gate_then(ets, start_fn) do
    fn runner_id ->
      case :ets.update_counter(ets, :calls, {2, 1}, {:calls, 0}) do
        1 -> {:ok, :pong}
        n -> start_fn.(runner_id, n - 1)
      end
    end
  end

  test "5.10 5.5 mocked-ingress regression: failed startup keeps plugins :prepared, no upstream I/O",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, _attempt -> {:error, :timeout} end)
    )

    # QuietProvider's `activate/1` sends `:activated`. If the relay parks
    # without ever calling activate, no :activated message is sent — i.e.
    # the upstream connection an `activate/1` would open never happens.
    plugin = %Plugin{
      id: "cascade_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      hooks: [{:on_input, [position: 0]}]
    }

    relay = %Builder{
      id: "cascade_relay",
      factory_id: @factory,
      opts: [bindings: [plugin]]
    }

    baseline = :erlang.memory(:processes)

    assert {:error, {:start_failed, _id, {:ping_failed, :timeout}}} =
             RelayManager.start_relay(@namespace, relay)

    # Plugins never activate, so the upstream connection (TLS handshake,
    # subscribe handshake) the Websocket provider's `activate/1` would
    # open is never opened. No memory cascade.
    refute_received :activated

    final = :erlang.memory(:processes)
    delta_mib = (final - baseline) / (1024 * 1024)

    assert delta_mib < 50,
           "process memory delta exceeded 50 MiB: #{delta_mib} MiB " <>
             "(baseline=#{baseline}, final=#{final})"
  end

  test "stop_relay returns :ok within 1s while a parked-unpark bind task is in flight",
       %{test_pid: test_pid, ets: ets} do
    # The async migrate path lives on the parked-unpark branch — that's
    # where the whole bind runs. Boot a parked relay (start ping fails
    # after the readiness gate) and then trigger a rebind with a ping that
    # blocks for several seconds.
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _, _ -> {:error, :timeout} end)
    )

    relay = %Builder{id: "stop_during_migrate", factory_id: @factory, opts: []}

    {:ok, overwatch_pid} =
      Overwatch.start_link({@namespace, "stale_ce", relay})

    on_exit(fn ->
      if Process.alive?(overwatch_pid) do
        try do
          GenServer.stop(overwatch_pid, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    # Wait for the relay to park.
    assert NodeTestHelper.wait_until(
             fn ->
               GenServer.call(overwatch_pid, :get_overwatch_state).halt_reason != nil
             end,
             2_000,
             25
           )

    # Replace ping with one that blocks until released, so the rebind's bind
    # task is in flight for as long as the test needs.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id ->
      send(test_pid, {:ping_started, self()})
      NodeTestHelper.park()
      {:ok, :pong}
    end)

    # Fire the rebind. The Overwatch must return `:ok`
    # immediately (the retry task is spawned out-of-band).
    :ok = GenServer.call(overwatch_pid, {:rebind, "fake_new_runner"}, 1_000)

    assert_receive {:ping_started, _}, 500

    # Now tear the relay down the way the hub stop path does (a
    # supervisor :shutdown exit). terminate/2 MUST return within 1 s even
    # though the bind task is blocked inside the ping — it kills the
    # task instead of waiting on it. Trap: the Overwatch is linked to this
    # test, and a :shutdown stop propagates over the link.
    Process.flag(:trap_exit, true)
    started = System.monotonic_time(:millisecond)
    assert :ok = GenServer.stop(overwatch_pid, :shutdown, 1_500)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 1_000,
           "teardown returned in #{elapsed} ms; required < 1 s while bind task is in flight"
  end

  describe "a park is reported, not only logged" do
    test "a park caused by a plugin names the plugin that caused it" do
      id = "park-plugin-#{System.unique_integer([:positive])}"

      reason =
        {:plugin_prepare_failed, "orphan", {:provider_refused, "pkg", "orphan_provider", :bad}}

      Telemetry.capture([:flezha, :relay, :halted],
        as: :halted,
        filter: &(Map.get(&1, :relay_id) == id)
      )

      state = %Overwatch{id: id, namespace: @namespace}

      assert %Overwatch{halt_reason: ^reason} = Lifecycle.halt(state, reason)

      assert_receive {:halted, %{count: 1}, meta}
      assert meta.namespace == @namespace
      assert meta.reason == reason
      assert meta.plugin_id == "orphan"
    end

    test "a park with no plugin behind it is reported too, and claims no plugin id" do
      id = "park-other-#{System.unique_integer([:positive])}"

      Telemetry.capture([:flezha, :relay, :halted],
        as: :halted,
        filter: &(Map.get(&1, :relay_id) == id)
      )

      state = %Overwatch{id: id, namespace: @namespace}

      assert %Overwatch{} = Lifecycle.halt(state, {:restore_refused, :sdk_refused})

      assert_receive {:halted, %{count: 1}, meta}
      assert meta.reason == {:restore_refused, :sdk_refused}
      assert meta.plugin_id == nil
    end

    # fails when: a halt writes `:halted` over a sleeper or a relay holding no
    # binding, so the phase and the judge's state describe one relay differently.
    test "a halt keeps a suspended or unbound phase and halts a bound one" do
      bound = %Overwatch{id: "halt-phase", namespace: @namespace, runner_id: "r", phase: :running}

      for {state, phase} <- [
            {bound, :halted},
            {%{bound | runner_id: nil}, :unbound},
            {%{bound | runner_id: nil, phase: :suspended}, :suspended}
          ] do
        assert %Overwatch{phase: ^phase, halt_reason: :boom} = Lifecycle.halt(state, :boom)
      end
    end
  end

  describe "a bind that finds no runner holds the relay" do
    # fails when: the bind stops the Overwatch. `restart: :transient` then runs a
    # second retry mechanism beside the convergence loop's own rebind ladder, and
    # a runnerless namespace crash-loops every `overwatch_runner_wait_ms` for as
    # long as it stays runnerless.
    test "the relay stays alive past its bind wait, and nothing restarts it" do
      on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 20))

      relay = %Builder{
        id: "held-unbound-#{System.unique_integer([:positive])}",
        factory_id: "factory_no_runner_advertises"
      }

      child = %{
        id: relay.id,
        start: {Overwatch, :start_link, [{@namespace, nil, relay}]},
        restart: :transient
      }

      sup =
        start_supervised!(%{
          id: :held_unbound_sup,
          type: :supervisor,
          start: {Supervisor, :start_link, [[child], [strategy: :one_for_one, max_restarts: 0]]}
        })

      [{_, pid, _, _}] = Supervisor.which_children(sup)
      ref = Process.monitor(pid)

      # The outcome is recorded where the start caller and the convergence loop
      # both read it: no binding, and the bind's own reason.
      assert NodeTestHelper.wait_until(
               fn -> match?({:runner_not_ready, _}, overwatch_state(pid).halt_reason) end,
               2_000,
               5
             )

      # `max_restarts: 0` makes a restart fatal to the supervisor, so one
      # assertion covers both halves: the relay did not die, and nothing
      # brought it back.
      state = overwatch_state(pid)
      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(sup)
      assert [{_, ^pid, _, _}] = Supervisor.which_children(sup)
      assert is_nil(state.runner_id)
      assert Overwatch.phase(state) == :unbound
      assert {:runner_not_ready, reason} = state.halt_reason
      assert reason in [:timeout, :unavailable]
    end

    # fails when: the refusal a waiting user gets depends on the relay
    # dying. It never did — `bind_started/4` reads `:registry_runner_set`, which
    # answers `{:error, _}` from the recorded outcome.
    test "a start whose runner is gone by bind time is still refused" do
      # A zero bind budget is the runner disappearing between admission (which
      # sees a runner for the factory) and the bind.
      on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 0))

      id = "refused-unbound-#{System.unique_integer([:positive])}"

      assert {:error, {:start_failed, ^id, {:runner_not_ready, :timeout}}} =
               RelayManager.start_relay(@namespace, %Builder{
                 id: id,
                 factory_id: @factory
               })

      # Refused means refused: nothing is left running unbound behind the caller.
      assert NodeTestHelper.wait_until(
               fn ->
                 not match?({:ok, pid} when is_pid(pid), HubManager.get_pid(@namespace, id))
               end,
               2_000,
               20
             )
    end

    # fails when: a rebind of a never-bound relay skips the full init.
    test "a rebind of a held relay runs the full init" do
      id = "held-migrate-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :no_runner} end)

      held = %Overwatch{
        id: id,
        namespace: @namespace,
        factory_id: @factory,
        builder: %Builder{id: id, factory_id: @factory},
        extensions: Table.new(Overwatch.__extensions_catalog__()),
        runner_id: nil,
        halt_reason: {:runner_not_ready, :timeout}
      }

      assert {:reply, :ok, %Overwatch{bind_task: %{pid: task}}} =
               Overwatch.handle_call({:rebind, "late_runner"}, {self(), make_ref()}, held)

      # The bind task runs `complete_init`, the full init, which pings the
      # runner.
      assert_receive {:bind_task_result, ^task, {:halt, _, {:ping_failed, :no_runner}}}, 2_000
    end

    # fails when: holding replaces a crash loop with a stall — the relay
    # stays unbound after a runner arrives because nothing rebinds it.
    test "the loop's rebind binds a held relay once a runner connects", %{fleet: fleet} do
      on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 20))

      id = "held-then-bound-#{System.unique_integer([:positive])}"

      # The runner side answers throughout; what the relay lacks is a runner
      # advertising its factory, which is what the bind resolves.
      Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)

      # No runner serves the namespace when the bind runs.
      FleetStateHelper.seed(@namespace, %{})

      {:ok, pid} = Overwatch.start_link({@namespace, nil, %Builder{id: id, factory_id: @factory}})

      on_exit(fn ->
        try do
          NodeTestHelper.ensure_stopped(pid)
        catch
          :exit, _ -> :ok
        end
      end)

      assert NodeTestHelper.wait_until(
               fn ->
                 match?(
                   {:runner_not_ready, _},
                   overwatch_state(pid).halt_reason
                 )
               end,
               3_000,
               20
             )

      # A runner advertising the factory connects.
      :sys.replace_state(FleetManager, fn _ -> fleet end)

      # What the loop plans for an unbound relay, driven the way `Heal`
      # drives it: resolve a target, then `{:rebind, target}`.
      target = RelayManager.assign_runner(@namespace, @factory, id)
      assert is_binary(target)
      assert :ok = GenServer.call(pid, {:rebind, target})

      assert NodeTestHelper.wait_until(
               fn -> is_nil(overwatch_state(pid).halt_reason) end,
               5_000,
               20
             ),
             "the held relay never bound: " <>
               inspect(overwatch_state(pid).halt_reason)

      assert overwatch_state(pid).runner_id == target
    end
  end

  describe "how a process leaves its relay" do
    # fails when: a cause departs other than its row, a handover stops reading
    # as a move, or a process no stop named (the hub's reconcile, a namespace
    # stop) stops reading its declared entry.
    test "departure/1 answers for every cause, with and without a declared entry" do
      named = [{true, :stop, :moved}, {false, :move, :moved}]

      stops =
        for cause <- [:stop, :complete, :rescale, :failed_start], do: {false, cause, :stopped}

      unnamed = [nil, :start, :restart, :arrive, :rebind, :wake, :rewind]

      for declared? <- [true, false] do
        rows = if declared?, do: [%{id: "departing", factory_id: @factory}], else: []

        stub(HubManager, :declared_relays, fn
          "departure-ns" -> rows
          namespace -> Mimic.call_original(HubManager, :declared_relays, [namespace])
        end)

        scanned =
          for cause <- unnamed, do: {false, cause, if(declared?, do: :moved, else: :stopped)}

        for {handed_over?, cause, departs} <- named ++ stops ++ scanned do
          state = %Overwatch{
            id: "departing",
            namespace: "departure-ns",
            handed_over?: handed_over?,
            cause: cause
          }

          assert {cause, declared?, Lifecycle.departure(state)} == {cause, declared?, departs}
        end
      end
    end

    # fails when: a stop whose hub stop failed leaves its cause on a relay that
    # still runs, so a later end the hub drives reads as this stop's.
    test "a failed hub stop leaves the relay running with no cause" do
      Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id -> {:ok, :pong} end)
      id = "hub-stop-refused-#{System.unique_integer([:positive])}"

      assert {:ok, _} =
               RelayManager.start_relay(@namespace, %Builder{id: id, factory_id: @factory})

      {:ok, pid} = RelayManager.get_relay_pid(@namespace, id)

      stub(ProcessHub, :stop_children, fn
        _hub, [^id], _opts -> :refused
        hub, ids, opts -> Mimic.call_original(ProcessHub, :stop_children, [hub, ids, opts])
      end)

      stub(ProcessHub.Future, :await, fn
        :refused -> %ProcessHub.StopResult{status: :error, stopped: [], errors: [{id, :refused}]}
        future -> Mimic.call_original(ProcessHub.Future, :await, [future])
      end)

      assert {:error, {:stop_failed, ^id, :hub_stop, :refused}} =
               RelayManager.stop_relay(@namespace, id)

      assert %{cause: nil, phase: :running} = state = overwatch_state(pid)

      # Durable starts are off under test, so the entry the hub keeps is stubbed:
      # with no cause left, the departure is the declared entry's, not this stop's.
      stub(HubManager, :declared_relays, fn @namespace -> [%{id: id, factory_id: @factory}] end)
      assert Lifecycle.departure(state) == :moved
    end
  end

  describe "a relay declared but running nowhere" do
    setup %{test_pid: test_pid} do
      id = "declared-only-#{System.unique_integer([:positive])}"
      relay = %Builder{id: id, factory_id: @factory}

      stub(HubManager, :declared_relay, fn
        @namespace, ^id -> {:ok, relay}
        namespace, other -> Mimic.call_original(HubManager, :declared_relay, [namespace, other])
      end)

      stub(ProcessHub, :stop_children, fn
        _hub, [^id], _opts -> send(test_pid, {:hub_stop, id}) && :declared_only
        hub, ids, opts -> Mimic.call_original(ProcessHub, :stop_children, [hub, ids, opts])
      end)

      stub(ProcessHub.Future, :await, fn
        :declared_only -> %ProcessHub.StopResult{status: :ok, stopped: [], errors: []}
        future -> Mimic.call_original(ProcessHub.Future, :await, [future])
      end)

      stub(Flezha.Logs.RelayHistory, :ended, fn run, _ends_at ->
        send(test_pid, {:ended, run.builder.id})
        :ok
      end)

      {:ok, id: id}
    end

    # fails when: the stop door answers relay.not_found for a declaration the
    # convergence loop keeps trying to start, so no operator can remove it, or
    # removes it without the record and the announcement any stop makes.
    test "an operator's stop removes the declaration, recorded and announced", %{id: id} do
      Telemetry.capture([:flezha, :relay, :stopped])

      assert :ok = RelayManager.stop_relay(@namespace, id)
      assert_received {:hub_stop, ^id}
      assert_received {:ended, ^id}

      assert_received {:telemetry, [:flezha, :relay, :stopped], _,
                       %{relay: ^id, reason: :stopped}}
    end

    # fails when: a stop lands while the relay's start is on its way and kills
    # the new child with no teardown walk behind it.
    test "a relay whose start is on its way is left to it", %{id: id} do
      misc = ProcessHub.Hub.get(Identificator.hub(@namespace)).storage.misc
      key = {ProcessHub.Constant.StorageKey.sif(), id}
      ProcessHub.Service.Storage.insert(misc, key, [node()])
      on_exit(fn -> ProcessHub.Service.Storage.remove(misc, key) end)

      assert {:error, {:relay_not_found, @namespace, ^id}} =
               RelayManager.stop_relay(@namespace, id)

      refute_received {:hub_stop, ^id}
    end

    # fails when: a rescale, which stops a member it moves, takes the
    # declaration of a member that is not running.
    test "a rescale leaves the declaration of a member that is not running", %{id: id} do
      for cause <- [:rescale, :rescale_leave] do
        assert {:error, {:relay_not_found, @namespace, ^id}} =
                 RelayManager.stop_relay(@namespace, id, cause: cause, preserve_state: true)
      end

      refute_received {:hub_stop, ^id}
      refute_received {:ended, ^id}
    end

    test "with no declaration either, the stop still answers relay_not_found" do
      id = "nowhere-#{System.unique_integer([:positive])}"

      assert {:error, {:relay_not_found, @namespace, ^id}} =
               RelayManager.stop_relay(@namespace, id)
    end
  end

  describe "the Overwatch's shutdown bound" do
    # Adds `run` to the `:process` walk at `order`, for `relay_id`'s process only.
    defp hold_process_walk(relay_id, steps) do
      key = {Flezha.Shutdown, :steps}
      declared = Flezha.Shutdown.all_steps()

      held =
        for {order, run} <- steps do
          %{
            scope: :process,
            order: order,
            name: :"held_#{order}",
            module: __MODULE__,
            run: fn
              {%Overwatch{id: ^relay_id}, _reason}, budget -> run.(budget)
              _other, _budget -> :ok
            end
          }
        end

      :persistent_term.put(key, Enum.sort_by(held ++ declared, &{&1.scope, &1.order}))
      on_exit(fn -> :persistent_term.put(key, declared) end)
    end

    defp start_bound(id) do
      Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id -> {:ok, :pong} end)

      assert {:ok, _} =
               RelayManager.start_relay(@namespace, %Builder{id: id, factory_id: @factory})
    end

    # fails when: the Overwatch's child spec keeps the OTP worker default, so
    # the supervisor kills a teardown walk at 5 s and its later steps never run.
    test "a started relay's supervisor waits the Overwatch's shutdown bound, past 5 s" do
      id = "slow-walk-#{System.unique_integer([:positive])}"
      start_bound(id)

      hub = ProcessHub.Hub.get(Identificator.hub(@namespace))
      assert {:ok, spec} = :supervisor.get_childspec(hub.procs.dist_sup, id)
      assert spec.shutdown == Durations.get(:overwatch_shutdown_ms)
      assert spec.shutdown > 5_000
    end

    # fails when: the hub stop waits no longer than the Overwatch's bound, so a
    # walk that ends inside its own budget still fails the stop and keeps the
    # relay's quota.
    test "a walk held to its budget still ends in a stop that releases the quota",
         %{test_pid: test_pid} do
      # The bounds nest 1 s apart below the release, so 3.6 s leaves the walk a
      # 600 ms budget to end 500 ms inside of.
      on_exit(TestHelpers.override_env(:stop_release_timeout_ms, 3_600))
      id = "budget-walk-#{System.unique_integer([:positive])}"
      hold_process_walk(id, [{1, fn budget -> Process.sleep(budget - 500) end}])

      stub(Flezha.AuthZ.Quotas, :record_deallocation, fn namespace, kind, n ->
        send(test_pid, {:dealloc, namespace, kind})
        Mimic.call_original(Flezha.AuthZ.Quotas, :record_deallocation, [namespace, kind, n])
      end)

      start_bound(id)
      assert :ok = RelayManager.stop_relay(@namespace, id)
      assert_received {:dealloc, @namespace, :max_relays}
    end
  end
end

defmodule FlezhaTest.Relay.OverwatchCorrectionRetentionTest do
  @moduledoc """
  A declared `correction_window` DERIVES the retention it needs rather than being
  refused against it. Retention is not a policy the author and the user
  negotiate — a correction over a 60 s window MEANS holding 60 s of input log and
  60 s of WAL history, which is the definition of the declaration.

  But a one-line declaration can commit the controller to real disk, so a floor
  raised above its configured value is announced.
  """
  # async: false: writes Application env; a shared on-disk root; global telemetry handlers.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.{InputLog, PersistenceResolver}
  alias FlezhaTest.Helper.Telemetry

  setup do
    ClassRegistry.ensure_table()
    :ok
  end

  defp attach(event) do
    ref = make_ref()

    Telemetry.capture(event, as: ref)

    ref
  end

  test "a declared window raises both floors on default config, and says so" do
    class = "corrector-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    # Default config: no hold window at all, and an age floor shorter than the
    # window. This combination raises both floors instead of refusing the
    # relay at start.
    Application.put_env(:flezha, :input_log_hold_window_ms, 0)
    Application.put_env(:flezha, :compact_after_age_ms, 60_000)
    :ok = ClassRegistry.put_correction_window(class, 300_000)

    assert :ok = PersistenceResolver.announce_derived_retention(class, 300_000)

    assert InputLog.hold_window_ms(class) == 300_000
    assert PersistenceResolver.compact_after_age_ms(class) == 300_000

    assert_receive {^ref, %{configured: 0, derived: 300_000},
                    %{floor: :input_log_hold_window_ms, relay: ^class}}

    assert_receive {^ref, %{configured: 60_000, derived: 300_000},
                    %{floor: :compact_after_age_ms, relay: ^class}}
  end

  test "nothing is announced when the user already configured enough" do
    class = "generous-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    Application.put_env(:flezha, :input_log_hold_window_ms, 600_000)
    Application.put_env(:flezha, :compact_after_age_ms, 600_000)
    :ok = ClassRegistry.put_correction_window(class, 60_000)

    assert :ok = PersistenceResolver.announce_derived_retention(class, 60_000)

    assert InputLog.hold_window_ms(class) == 600_000
    assert PersistenceResolver.compact_after_age_ms(class) == 600_000

    refute_received {^ref, _, _}
  end

  test "an age floor of 0 is satisfied, not raised — the polarity trap" do
    class = "stream-hist-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    # 0 disables age compaction: WAL history is retained without an age bound,
    # which already satisfies any window. Raising it would ENABLE compaction and
    # make retention strictly worse.
    Application.put_env(:flezha, :input_log_hold_window_ms, 600_000)
    Application.put_env(:flezha, :compact_after_age_ms, 0)
    :ok = ClassRegistry.put_correction_window(class, 60_000)

    assert :ok = PersistenceResolver.announce_derived_retention(class, 60_000)

    assert PersistenceResolver.compact_after_age_ms(class) == 0
    refute_received {^ref, _, _}
  end
end
