defmodule FlezhaTest.Operative.OverwatchRetryTest do
  @moduledoc """
  The one-attempt bind, the parked-error state and the end-to-end ping. The
  Overwatch's one startup attempt is observed through
  `OperativeManager.start_operative`: a ping that answers returns `{:ok, _}`; a
  ping that fails parks the operative with its reason, declares the park to the
  convergence loop, and surfaces `{:error, {:ping_failed, _}}` through the
  existing operator-action cleanup path (`stop_child_quietly`). The un-park is
  the loop's `rebind_operative`, driven here through `:runner_migrate`.

  Uses the `:overwatch_ping_fn` Application-env hook the Overwatch consults
  during a startup attempt.
  """

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Lifecycle
  alias Flezha.Operative.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  # async: false: writes Application env; the live controller and its fleet; node-global tables.

  use ExUnit.Case, async: false

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_retry_test, [:public, :set])

    on_exit(fn ->
      OperativeManager.stop_all_operatives(@namespace)
      Application.delete_env(:flezha, :overwatch_ping_fn)
    end)

    {:ok, test_pid: test_pid, ets: ets}
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

  test "5.4 ping success proceeds to start_operative_sdk and activates plugins",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id ->
      send(test_pid, :ping_called)
      {:ok, :pong}
    end)

    plugin = %Plugin{
      id: "ping_success_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{
      id: "ping_success_operative",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert_receive :ping_called, 2_000
    assert_receive :activated, 2_000
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

    operative = %Builder{id: "one_attempt_operative", factory_id: @factory, opts: []}

    assert {:error, %{reason: {:ping_failed, :timeout}}} =
             OperativeManager.start_operative(@namespace, operative)

    # One start attempt after the readiness gate, and no second one of its own.
    assert_receive {:ping_attempt, 1}, 2_000
    refute_received {:ping_attempt, 2}

    assert_receive {:declare, :operative_parked, {@namespace, "one_attempt_operative"},
                    %{reason: {:ping_failed, :timeout}}},
                   2_000
  end

  test "6.4 a parked operative un-parks via runner_migrate: pings, starts, clears init_error, activates plugins",
       %{test_pid: test_pid, ets: ets} do
    # Start the operative so the readiness gate passes but the START ping fails,
    # parking it (alive, init_error set). Started directly (not via
    # OperativeManager) so the parked-and-alive GenServer is not
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
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{id: "unpark_operative", factory_id: @factory, opts: [plugins: [plugin]]}

    {:ok, pid} = Overwatch.start_link({@namespace, "stale_ce", operative})

    on_exit(fn ->
      try do
        NodeTestHelper.ensure_stopped(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    # It parks (init_error set) and never activates the plugin while parked.
    assert NodeTestHelper.wait_until(
             fn -> GenServer.call(pid, :get_overwatch_state).init_error != nil end,
             2_000,
             20
           )

    refute_received :activated

    # A healthy runner is now reachable — un-park via runner_migrate.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)
    runner_id = OperativeManager.assign_runner(@namespace, @factory, "unpark_operative")
    assert is_binary(runner_id)

    assert :ok = GenServer.call(pid, {:runner_migrate, runner_id})

    # init_error cleared and the plugin activated (resumes producing).
    # `:runner_migrate` is async (the bind runs in a
    # `Task.Supervisor.async_nolink` task), so we poll the state instead of
    # asserting immediately.
    assert_receive :activated, 5_000

    assert NodeTestHelper.wait_until(
             fn -> is_nil(GenServer.call(pid, :get_overwatch_state).init_error) end,
             2_000,
             20
           )

    state = GenServer.call(pid, :get_overwatch_state)
    assert Table.phase(state.extensions, "unpark_plugin") == :running
  end

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the one start attempt. These tests drive the START ping to
  # fail; the readiness gate must still pass or the operative stops with
  # `:runner_not_ready` instead of exercising the park behaviour.
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

    # QuietProvider's `activate/1` sends `:activated`. If the operative parks
    # without ever calling activate, no :activated message is sent — i.e.
    # the upstream connection an `activate/1` would open never happens.
    plugin = %Plugin{
      id: "cascade_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{
      id: "cascade_operative",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    baseline = :erlang.memory(:processes)

    assert {:error, %{reason: {:ping_failed, :timeout}}} =
             OperativeManager.start_operative(@namespace, operative)

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
        [:flezha, :operative, :parked],
        :parked,
        &(Map.get(&1, :operative_id) == id)
      )

      state = %Overwatch{id: id, namespace: @namespace}

      assert %Overwatch{init_error: ^reason} = Lifecycle.park(state, reason)

      assert_receive {:parked, %{count: 1}, meta}
      assert meta.namespace == @namespace
      assert meta.reason == reason
      assert meta.plugin_id == "orphan"
    end

    test "a park with no plugin behind it is reported too, and claims no plugin id" do
      id = "park-other-#{System.unique_integer([:positive])}"

      MessageTestHelper.forward_telemetry(
        [:flezha, :operative, :parked],
        :parked,
        &(Map.get(&1, :operative_id) == id)
      )

      state = %Overwatch{id: id, namespace: @namespace}

      assert %Overwatch{} = Lifecycle.park(state, {:restore_refused, :sdk_refused})

      assert_receive {:parked, %{count: 1}, meta}
      assert meta.reason == {:restore_refused, :sdk_refused}
      assert meta.plugin_id == nil
    end
  end
end

defmodule FlezhaTest.Operative.OverwatchCorrectionRetentionTest do
  @moduledoc """
  A declared `correction_window` DERIVES the retention it needs rather than being
  refused against it. Retention is not a policy the author and the operator
  negotiate — a correction over a 60 s window MEANS holding 60 s of input log and
  60 s of WAL history, which is the definition of the declaration.

  But a one-line declaration can commit the controller to real disk, so a floor
  raised above its configured value is announced.
  """
  # async: false: writes Application env; a shared on-disk root; global telemetry handlers.
  use ExUnit.Case, async: false

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{InputLog, PersistenceResolver, TestHelpers}

  setup do
    base = TestHelpers.tmp_app_data_dir()
    ClassRegistry.ensure_table()

    prev_hold = Application.get_env(:flezha, :input_log_hold_window_ms, 0)
    prev_age = Application.get_env(:flezha, :compact_after_age_ms, 0)

    on_exit(fn ->
      Application.put_env(:flezha, :input_log_hold_window_ms, prev_hold)
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(base)
    end)

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
    # operative at start.
    Application.put_env(:flezha, :input_log_hold_window_ms, 0)
    Application.put_env(:flezha, :compact_after_age_ms, 60_000)
    :ok = ClassRegistry.put_correction_window(class, 300_000)

    assert :ok = PersistenceResolver.announce_derived_retention(class, 300_000)

    assert InputLog.hold_window_ms(class) == 300_000
    assert PersistenceResolver.compact_after_age_ms(class) == 300_000

    assert_receive {^ref, %{configured: 0, derived: 300_000},
                    %{floor: :input_log_hold_window_ms, operative: ^class}}

    assert_receive {^ref, %{configured: 60_000, derived: 300_000},
                    %{floor: :compact_after_age_ms, operative: ^class}}
  end

  test "nothing is announced when the operator already configured enough" do
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
