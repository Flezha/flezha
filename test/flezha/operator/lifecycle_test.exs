defmodule FlezhaTest.Operator.OverwatchRetryTest do
  @moduledoc """
  The one-attempt bind, the parked-error state and the end-to-end ping. The
  Overwatch's one startup attempt is observed through
  `OperatorManager.start_operator`: a ping that answers returns `{:ok, _}`; a
  ping that fails parks the operator with its reason, declares the park to the
  convergence loop, and surfaces `{:error, {:ping_failed, _}}` through the
  existing user-action cleanup path (`stop_child_quietly`). The un-park is
  the loop's `rebind_operator`, driven here through `:runner_migrate`.

  Uses the `:overwatch_ping_fn` Application-env hook the Overwatch consults
  during a startup attempt.
  """

  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.Lifecycle
  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  # async: false: writes Application env; the live controller and its fleet; node-global tables.

  use FlezhaTest.Case, fixtures: [:env], async: false

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_retry_test, [:public, :set])

    # One test seeds the namespace's fleet; the state is process-global.
    fleet_state = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

    on_exit(fn -> OperatorManager.stop_all_operators(@namespace) end)

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

  test "5.4 ping success proceeds to start_operator_sdk and activates plugins",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn runner_id ->
      send(test_pid, {:ping_called, runner_id, ClassRegistry.owner("ping_success_operator")})
      {:ok, :pong}
    end)

    plugin = %Plugin{
      id: "ping_success_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      hooks: [{:on_input, [position: 0]}]
    }

    operator = %Builder{
      id: "ping_success_operator",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperatorManager.start_operator(@namespace, operator)

    # The start's ping is issued after the bind and right before the start, so
    # the ownership record already names the runner the operator starts on.
    assert_receive {:ping_called, runner_id, {:ok, _overwatch, owner}}, 2_000
    assert owner == Marks.identity_of(runner_id)
    assert_receive :activated, 2_000
  end

  test "a remote class is refused at start on a controller with no durability backend" do
    refute Flezha.State.DurabilityTier.enabled?(), "the suite runs with no durability backend"
    operator = %Builder{id: "remote_without_backend_op", factory_id: "sync_durable_steps"}

    assert {:error, {:start_failed, "remote_without_backend_op", reason}} =
             OperatorManager.start_operator(@namespace, operator)

    assert reason == {:remote_without_backend, "sync_durable_steps"}
    error = Flezha.Error.from_reason(reason, nil)
    assert error.code == "operator.remote_without_backend"
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

    operator = %Builder{id: "one_attempt_operator", factory_id: @factory, opts: []}

    assert {:error, {:start_failed, _id, {:ping_failed, :timeout}}} =
             OperatorManager.start_operator(@namespace, operator)

    # One start attempt after the readiness gate, and no second one of its own.
    assert_receive {:ping_attempt, 1}, 2_000
    refute_received {:ping_attempt, 2}

    assert_receive {:declare, :operator_parked, {@namespace, "one_attempt_operator"},
                    %{reason: {:ping_failed, :timeout}}},
                   2_000
  end

  test "6.4 a parked operator un-parks via runner_migrate: pings, starts, clears init_error, activates plugins",
       %{test_pid: test_pid, ets: ets} do
    # Start the operator so the readiness gate passes but the START ping fails,
    # parking it (alive, init_error set). Started directly (not via
    # OperatorManager) so the parked-and-alive GenServer is not
    # stop_child_quietly'd — this is the cold-restart parked state.
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

    operator = %Builder{id: "unpark_operator", factory_id: @factory, opts: [plugins: [plugin]]}

    {:ok, pid} = Overwatch.start_link({@namespace, "stale_ce", operator})

    on_exit(fn ->
      try do
        NodeTestHelper.ensure_stopped(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    # It parks (init_error set) and never activates the plugin while parked.
    assert NodeTestHelper.wait_until(
             fn -> overwatch_state(pid).init_error != nil end,
             2_000,
             20
           )

    refute_received :activated

    # A healthy runner is now reachable — un-park via runner_migrate.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)
    runner_id = OperatorManager.assign_runner(@namespace, @factory, "unpark_operator")
    assert is_binary(runner_id)

    assert :ok = GenServer.call(pid, {:runner_migrate, runner_id})

    # init_error cleared and the plugin activated (resumes producing).
    # `:runner_migrate` is async (the bind runs in a
    # `Task.Supervisor.async_nolink` task), so we poll the state instead of
    # asserting immediately.
    assert_receive :activated, 5_000

    assert NodeTestHelper.wait_until(
             fn -> is_nil(overwatch_state(pid).init_error) end,
             2_000,
             20
           )

    state = overwatch_state(pid)
    assert Table.phase(state.extensions, "unpark_plugin") == :running
  end

  defp overwatch_state(pid), do: GenServer.call(pid, :get_overwatch_state)

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the one start attempt. These tests drive the START ping to
  # fail; the readiness gate must still pass or the operator is held
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

    # QuietProvider's `activate/1` sends `:activated`. If the operator parks
    # without ever calling activate, no :activated message is sent — i.e.
    # the upstream connection an `activate/1` would open never happens.
    plugin = %Plugin{
      id: "cascade_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      hooks: [{:on_input, [position: 0]}]
    }

    operator = %Builder{
      id: "cascade_operator",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    baseline = :erlang.memory(:processes)

    assert {:error, {:start_failed, _id, {:ping_failed, :timeout}}} =
             OperatorManager.start_operator(@namespace, operator)

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

  describe "a park is reported, not only logged" do
    test "a park caused by a plugin names the plugin that caused it" do
      id = "park-plugin-#{System.unique_integer([:positive])}"
      reason = {:plugin_prepare_failed, "orphan", {:module_not_loaded, MyOrg.Uninstalled}}

      MessageTestHelper.forward_telemetry(
        [:flezha, :operator, :parked],
        :parked,
        &(Map.get(&1, :operator_id) == id)
      )

      state = %Overwatch{id: id, namespace: @namespace}

      {parked, log} = with_log(fn -> Lifecycle.park(state, reason) end)
      assert %Overwatch{init_error: ^reason} = parked
      assert log =~ "operator #{id} parked: plugin orphan"

      assert_receive {:parked, %{count: 1}, meta}
      assert meta.namespace == @namespace
      assert meta.reason == reason
      assert meta.plugin_id == "orphan"
    end

    test "a park with no plugin behind it is reported too, and claims no plugin id" do
      id = "park-other-#{System.unique_integer([:positive])}"

      MessageTestHelper.forward_telemetry(
        [:flezha, :operator, :parked],
        :parked,
        &(Map.get(&1, :operator_id) == id)
      )

      state = %Overwatch{id: id, namespace: @namespace}

      assert %Overwatch{} = Lifecycle.park(state, {:restore_refused, :sdk_refused})

      assert_receive {:parked, %{count: 1}, meta}
      assert meta.reason == {:restore_refused, :sdk_refused}
      assert meta.plugin_id == nil
    end
  end

  describe "a bind that finds no runner holds the operator" do
    # fails when: the bind stops the Overwatch. `restart: :transient` then runs a
    # second retry mechanism beside the convergence loop's own rebind ladder, and
    # a runnerless namespace crash-loops every `overwatch_runner_wait_ms` for as
    # long as it stays runnerless.
    test "the operator stays alive past its bind wait, and nothing restarts it" do
      on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 200))

      operator = %Builder{
        id: "held-unbound-#{System.unique_integer([:positive])}",
        factory_id: "factory_no_runner_advertises"
      }

      child = %{
        id: operator.id,
        start: {Overwatch, :start_link, [{@namespace, nil, operator}]},
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

      # `max_restarts: 0` makes a restart fatal to the supervisor, so one
      # assertion covers both halves: the operator did not die, and nothing
      # brought it back.
      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 600
      assert Process.alive?(sup)
      assert [{_, ^pid, _, _}] = Supervisor.which_children(sup)

      # The outcome is recorded where the start caller and the convergence loop
      # both read it: no binding, and the bind's own reason.
      state = overwatch_state(pid)
      assert is_nil(state.runner_id)
      assert {:runner_not_ready, reason} = state.init_error
      assert reason in [:timeout, :unavailable]
    end

    # fails when: the refusal a waiting user gets depends on the operator
    # dying. It never did — `bind_started/4` reads `:registry_runner_set`, which
    # answers `{:error, _}` from the recorded outcome.
    test "a start whose runner is gone by bind time is still refused" do
      # A zero bind budget is the runner disappearing between admission (which
      # sees a runner for the factory) and the bind.
      on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 0))

      id = "refused-unbound-#{System.unique_integer([:positive])}"

      assert {:error, {:start_failed, ^id, {:runner_not_ready, :timeout}}} =
               OperatorManager.start_operator(@namespace, %Builder{
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

    # fails when: a rebind of a never-bound operator skips the full init.
    test "a rebind of a held operator runs the full init" do
      id = "held-migrate-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :no_runner} end)

      held = %Overwatch{
        id: id,
        namespace: @namespace,
        factory_id: @factory,
        builder: %Builder{id: id, factory_id: @factory},
        extensions: Table.new(Overwatch.__extensions_catalog__()),
        runner_id: nil,
        init_error: {:runner_not_ready, :timeout}
      }

      assert {:reply, :ok, %Overwatch{migrate_task: %{pid: task}}} =
               Overwatch.handle_call({:runner_migrate, "late_runner"}, {self(), make_ref()}, held)

      # The migrate task runs `complete_init`, the full init, which pings the
      # runner.
      assert_receive {:migrate_task_result, ^task, {:park, _, {:ping_failed, :no_runner}}}, 2_000
    end

    # fails when: holding replaces a crash loop with a stall — the operator
    # stays unbound after a runner arrives because nothing rebinds it.
    test "the loop's rebind binds a held operator once a runner connects", %{fleet: fleet} do
      on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 200))

      id = "held-then-bound-#{System.unique_integer([:positive])}"

      # The runner side answers throughout; what the operator lacks is a runner
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
                   overwatch_state(pid).init_error
                 )
               end,
               3_000,
               20
             )

      # A runner advertising the factory connects.
      :sys.replace_state(FleetManager, fn _ -> fleet end)

      # What the loop plans for an unbound operator, driven the way `Heal`
      # drives it: resolve a target, then `{:runner_migrate, target}`.
      target = OperatorManager.assign_runner(@namespace, @factory, id)
      assert is_binary(target)
      assert :ok = GenServer.call(pid, {:runner_migrate, target})

      assert NodeTestHelper.wait_until(
               fn -> is_nil(overwatch_state(pid).init_error) end,
               5_000,
               20
             ),
             "the held operator never bound: " <>
               inspect(overwatch_state(pid).init_error)

      assert overwatch_state(pid).runner_id == target
    end
  end
end

defmodule FlezhaTest.Operator.OverwatchCorrectionRetentionTest do
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

  alias Flezha.Operator.ClassRegistry
  alias Flezha.State.{InputLog, PersistenceResolver}

  setup do
    ClassRegistry.ensure_table()
    :ok
  end

  defp attach(event) do
    parent = self()
    ref = make_ref()
    handler = {__MODULE__, ref}

    :telemetry.attach(
      handler,
      event,
      fn _e, meas, meta, _ -> send(parent, {ref, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    ref
  end

  test "a declared window raises both floors on default config, and says so" do
    class = "corrector-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    # Default config: no hold window at all, and an age floor shorter than the
    # window. This combination raises both floors instead of refusing the
    # operator at start.
    Application.put_env(:flezha, :input_log_hold_window_ms, 0)
    Application.put_env(:flezha, :compact_after_age_ms, 60_000)
    :ok = ClassRegistry.put_correction_window(class, 300_000)

    assert :ok = PersistenceResolver.announce_derived_retention(class, 300_000)

    assert InputLog.hold_window_ms(class) == 300_000
    assert PersistenceResolver.compact_after_age_ms(class) == 300_000

    assert_receive {^ref, %{configured: 0, derived: 300_000},
                    %{floor: :input_log_hold_window_ms, operator: ^class}}

    assert_receive {^ref, %{configured: 60_000, derived: 300_000},
                    %{floor: :compact_after_age_ms, operator: ^class}}
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
