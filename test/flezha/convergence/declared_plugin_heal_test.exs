defmodule FlezhaTest.Convergence.DeclaredPluginHealTest do
  @moduledoc """
  The convergence loop against a live runner keeps a relay's declared
  plugins subscribed: it re-attaches one that went missing and restarts one
  that reports itself unhealthy.
  """
  alias Flezha.Convergence.Heal
  alias Flezha.Convergence.Loop
  alias Flezha.Convergence.Planner
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Relay.Diagnose
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  use ExUnit.Case

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  @temp_namespace "test_temp_namespace"
  @test_store_input_factory "store_input"

  setup _context do
    TestHelpers.reset_all_cells()
    on_exit(fn -> FleetManager.stop_namespace(@temp_namespace) end)
    :ok
  end

  test "pluginless running relay re-attaches its declared plugins (2026-06-12 incident shape)" do
    namespace = @temp_namespace
    ConvergenceSnapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Loop, tick_ms: 60_000})

    NodeTestHelper.start_runner(namespace)

    {:ok, %{relay_id: relay_id}} =
      "self_heal_relay"
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)
      |> RelayFixture.attach_output_msg(namespace)

    plugin_id = relay_id <> "_output_msg_handler"
    {:ok, ow} = HubManager.get_pid(namespace, relay_id)
    assert subscribed_plugin?(ow, plugin_id)

    # Direct Overwatch unsubscribe bypasses PluginManager, so the declared
    # cspec manifest keeps the plugin: running-but-pluginless, exactly the
    # warm-restart incident state.
    :ok = GenServer.call(ow, {:plugin_unsubscribe, plugin_id})
    refute subscribed_plugin?(ow, plugin_id)

    assert NodeTestHelper.wait_until(
             fn ->
               Loop.tick_now()
               subscribed_plugin?(ow, plugin_id)
             end,
             15_000
           ),
           "loop did not re-attach the declared plugin"
  end

  test "declared plugin self-reporting unhealthy is restarted by the loop" do
    namespace = @temp_namespace
    ConvergenceSnapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Loop, tick_ms: 60_000})

    NodeTestHelper.start_runner(namespace)

    {:ok, %{relay_id: relay_id}} =
      "sick_plugin_relay"
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)

    plugin_id = relay_id <> "_sick"
    FlezhaTest.SickProvider.reset()

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(plugin_id, %Flezha.Plugin{
        id: plugin_id,
        namespace: namespace,
        relay_id: relay_id,
        provider: FlezhaTest.SickProvider,
        opts: %{},
        hooks: [{:on_output, []}]
      })

    {:ok, ow} = HubManager.get_pid(namespace, relay_id)
    assert subscribed_plugin?(ow, plugin_id)
    activations_before = FlezhaTest.SickProvider.activations()

    # Healthy plugin: a tick must not restart it.
    Loop.tick_now()
    assert FlezhaTest.SickProvider.activations() == activations_before

    FlezhaTest.SickProvider.set_health({:unhealthy, :test_sick})

    assert NodeTestHelper.wait_until(
             fn ->
               Loop.tick_now()
               FlezhaTest.SickProvider.activations() > activations_before
             end,
             15_000
           ),
           "loop did not restart the unhealthy plugin"

    assert subscribed_plugin?(ow, plugin_id)

    # Verdict cleared: no further restarts once healthy again.
    FlezhaTest.SickProvider.set_health(:healthy)
    Loop.tick_now()
    stable = FlezhaTest.SickProvider.activations()
    Loop.tick_now()
    assert FlezhaTest.SickProvider.activations() == stable
  end

  describe "a binding whose plugin is disabled" do
    setup do
      ConvergenceSnapshot.reset_recovery_states()
      NodeTestHelper.start_runner(@temp_namespace)

      {:ok, %{relay_id: relay_id}} =
        "disabled_binding_relay"
        |> RelayFixture.get(@test_store_input_factory)
        |> RelayFixture.start(@temp_namespace)

      {:ok, ow} = HubManager.get_pid(@temp_namespace, relay_id)
      %{relay_id: relay_id, ow: ow}
    end

    test "start_plugin is unmet and leaves the target's ladder alone", %{relay_id: rid, ow: ow} do
      [name] = disabled_bindings(rid, 1)
      assert {:not_started, {:plugin_not_found, ^name}} = phase(ow, name)

      assert Heal.execute(heal_op(:start_plugin, rid, name)) == :unmet

      # Planned by the loop, on a key with a rung already recorded: the unmet
      # heal neither advances it nor resets it.
      {:ok, _} = start_supervised({Loop, tick_ms: 60_000})
      key = {:start_plugin, {rid, name}}
      # A rung already recorded and past its cooldown, so the tick plans the op.
      eligible = System.monotonic_time(:millisecond) - 1

      Telemetry.capture([:flezha, :convergence, :heal],
        as: fn _event, _measurements, meta -> {:heal, meta.primitive, meta.target} end
      )

      :sys.replace_state(Loop, fn state ->
        %{state | backoff: %{state.backoff | entries: %{key => {2, eligible}}}}
      end)

      Loop.tick_now()
      assert_received {:heal, :start_plugin, ^rid}
      assert :sys.get_state(Loop).backoff.entries[key] == {2, eligible}
      assert {:not_started, _} = phase(ow, name)
    end

    test "restart_plugin is unmet and does not bring the placeholder up", %{
      relay_id: rid,
      ow: ow
    } do
      [name] = disabled_bindings(rid, 1)

      assert Heal.execute(heal_op(:restart_plugin, rid, name)) == :unmet
      assert {:not_started, {:plugin_not_found, ^name}} = phase(ow, name)
    end

    test "more of them than the per-tick cap do not make the loop re-tick", %{relay_id: rid} do
      disabled_bindings(rid, Planner.max_ops() + 1)

      Telemetry.capture([:flezha, :convergence, :heal],
        as: fn _event, _measurements, meta -> {:heal, meta.primitive, meta.target} end
      )

      Telemetry.capture([:flezha, :convergence, :tick],
        as: fn _event, measurements, meta -> {:tick, meta.state, measurements.actions_issued} end
      )

      {:ok, _} = start_supervised({Loop, tick_ms: 60_000})
      assert_receive {:tick, :ticking, 0}, 5_000

      # The tick filled its cap with unmet ops; a spin would have left the next
      # tick already requested.
      assert length(collect(do: ({:heal, :start_plugin, ^rid} -> true))) == Planner.max_ops()
      refute :sys.get_state(Loop).tick_pending?
    end

    test "is a pause: the relay reads healthy and diagnose names it as a note", %{relay_id: rid} do
      [name] = disabled_bindings(rid, 1)
      {:ok, _} = start_supervised({Loop, tick_ms: 60_000})
      Loop.tick_now()

      assert ConvergenceSnapshot.relay_health(@temp_namespace, rid) == :healthy
      plugin = ConvergenceSnapshot.record({:plugin, {@temp_namespace, rid, name}})
      assert {plugin.state, plugin.reason} == {:healthy, :disabled}

      assert {:ok, %{verdict: :ok, findings: findings}} =
               Diagnose.report(@temp_namespace, rid, true)

      assert %{level: :note, message: message} =
               Enum.find(findings, &(&1.code == "diagnose.plugin_disabled"))

      assert message =~ "plugin '#{name}' bound to relay '#{rid}' is disabled"
    end

    test "does not hide a bound plugin that fails", %{relay_id: rid} do
      [_off] = disabled_bindings(rid, 1)
      sick = rid <> "_sick"
      FlezhaTest.SickProvider.reset()
      on_exit(&FlezhaTest.SickProvider.reset/0)
      {:ok, "plugin enabled"} = PluginManager.enable_plugin(sick, sick_plugin(sick, rid))
      {:ok, _} = start_supervised({Loop, tick_ms: 60_000})
      Loop.tick_now()
      assert ConvergenceSnapshot.relay_health(@temp_namespace, rid) == :healthy

      on_exit(NodeTestHelper.override_log_level(:warning))

      # A tick a declared fact requested may be the first to see the plugin
      # unhealthy, so the capture opens before the plugin turns.
      log =
        capture_log([level: :warning], fn ->
          FlezhaTest.SickProvider.set_health({:unhealthy, :test_sick})
          Loop.tick_now()
        end)

      assert ConvergenceSnapshot.relay_health(@temp_namespace, rid) == :degraded

      assert %Verdict{reason: {:plugin_unhealthy, ^sick, {:unhealthy, :test_sick}}} =
               ConvergenceSnapshot.verdict(@temp_namespace, rid)

      assert log =~ ~s([convergence] plugin-degraded relays ["#{rid}"])
    end

    test "enabling the plugin again brings the binding up without the loop", %{
      relay_id: rid,
      ow: ow
    } do
      [name] = disabled_bindings(rid, 1)

      {:ok, "plugin enabled"} = PluginManager.enable_plugin(name, sick_plugin(name, nil))
      assert phase(ow, name) == :running
    end
  end

  # Enable and bind `count` plugins to `relay_id`, then disable each: the
  # bindings stay, recorded not started.
  defp disabled_bindings(relay_id, count) do
    for i <- 1..count do
      name = "#{relay_id}_off_#{i}"
      {:ok, "plugin enabled"} = PluginManager.enable_plugin(name, sick_plugin(name, relay_id))
      {:ok, _} = PluginManager.disable_plugin(@temp_namespace, name)
      name
    end
  end

  defp sick_plugin(name, relay_id),
    do: %Flezha.Plugin{
      id: name,
      namespace: @temp_namespace,
      relay_id: relay_id,
      provider: FlezhaTest.SickProvider,
      opts: %{},
      hooks: [{:on_output, []}]
    }

  defp heal_op(primitive, relay_id, name) do
    {:ok, bindings} = HubManager.relay_bindings(@temp_namespace, relay_id)
    {^name, binding} = List.keyfind(bindings, name, 0)

    %{
      primitive: primitive,
      namespace: @temp_namespace,
      target: relay_id,
      args: %{plugin_id: name, plugin: binding}
    }
  end

  defp phase(ow, plugin_id),
    do: Table.phase(GenServer.call(ow, :get_overwatch_state).extensions, plugin_id)

  defp subscribed_plugin?(ow, plugin_id) do
    case GenServer.call(ow, :get_overwatch_state) do
      %{extensions: %{plugins: plugins}} -> Map.has_key?(plugins, plugin_id)
      _ -> false
    end
  end
end
