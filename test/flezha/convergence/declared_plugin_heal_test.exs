defmodule FlezhaTest.SickProvider do
  @moduledoc false
  # Convergence plugin-health test double: the health verdict and the
  # activation count are controlled through :persistent_term.
  use Flezha.Plugin.ProviderBehaviour

  @impl true
  def on_output(_ctx, _opts), do: :ok

  @impl true
  def activate(_opts) do
    :persistent_term.put({__MODULE__, :activations}, activations() + 1)
    :ok
  end

  @impl true
  def health(_opts), do: :persistent_term.get({__MODULE__, :health}, :healthy)

  def set_health(verdict), do: :persistent_term.put({__MODULE__, :health}, verdict)
  def activations(), do: :persistent_term.get({__MODULE__, :activations}, 0)

  def reset() do
    :persistent_term.put({__MODULE__, :health}, :healthy)
    :persistent_term.put({__MODULE__, :activations}, 0)
  end
end

defmodule FlezhaTest.Convergence.DeclaredPluginHealTest do
  @moduledoc """
  The convergence loop against a live runner keeps an operator's declared
  plugins subscribed: it re-attaches one that went missing and restarts one
  that reports itself unhealthy.
  """
  alias Flezha.Convergence.Loop
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case

  @temp_namespace "test_temp_namespace"
  @test_store_input_factory "store_input"

  setup _context do
    TestHelpers.reset_all_cells()
    on_exit(fn -> FleetManager.stop_namespace(@temp_namespace) end)
    :ok
  end

  test "pluginless running operator re-attaches its declared plugins (2026-06-12 incident shape)" do
    namespace = @temp_namespace
    ConvergenceSnapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Loop, tick_ms: 60_000})

    runner_id = NodeTestHelper.start_runner(namespace)

    {:ok, %{operator_id: operator_id}} =
      "self_heal_operator"
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start(namespace)
      |> OperatorFixture.attach_output_msg(namespace)

    plugin_id = operator_id <> "_output_msg_handler"
    {:ok, ow} = HubManager.get_pid(namespace, operator_id)
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

    NodeTestHelper.stop_runner(namespace, runner_id)
  end

  test "declared plugin self-reporting unhealthy is restarted by the loop" do
    namespace = @temp_namespace
    ConvergenceSnapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Loop, tick_ms: 60_000})

    runner_id = NodeTestHelper.start_runner(namespace)

    {:ok, %{operator_id: operator_id}} =
      "sick_plugin_operator"
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start(namespace)

    plugin_id = operator_id <> "_sick"
    FlezhaTest.SickProvider.reset()

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(plugin_id, %Flezha.Plugin{
        id: plugin_id,
        namespace: namespace,
        operator_id: operator_id,
        provider: FlezhaTest.SickProvider,
        opts: %{},
        hooks: [{:on_output, []}]
      })

    {:ok, ow} = HubManager.get_pid(namespace, operator_id)
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

    NodeTestHelper.stop_runner(namespace, runner_id)
  end

  defp subscribed_plugin?(ow, plugin_id) do
    case GenServer.call(ow, :get_overwatch_state) do
      %{extensions: %{plugins: plugins}} -> Map.has_key?(plugins, plugin_id)
      _ -> false
    end
  end
end
