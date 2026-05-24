defmodule Flezha.Reconciler.LoopSelfHealTest do
  @moduledoc """
  Data-plane / plugin self-healing driven by the reconciler loop
  (self-healing-control-plane, Phase 6.5 + 7.2):

    * a returning executor immediately un-parks its parked relays (no 60 s timer);
    * a persistently-unhealthy plugin is escalated (restarted) under the bounded
      governor.

  Exercised against a real hub with synthetic registry entries + stub relays.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Cluster.Orchestrator
  alias Flezha.Executor.FleetManager
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Reconciler.{Activity, Loop}
  alias Flezha.Relay.Overwatch
  alias Flezha.Service.{HubManager, Identificator, RelayManager}
  alias ProcessHub.Service.ProcessRegistry

  # ---------- stub relays ----------

  defmodule ParkedRelay do
    use GenServer
    def start(test_pid), do: GenServer.start(__MODULE__, test_pid)
    @impl true
    def init(t), do: {:ok, t}

    @impl true
    def handle_call(:get_overwatch_state, _from, t) do
      {:reply, %Overwatch{init_error: :boom, channel: nil, executor_id: "ce_old"}, t}
    end

    def handle_call({:executor_migrate, target}, _from, t) do
      send(t, {:migrated, target})
      {:reply, :ok, t}
    end
  end

  defmodule UnhealthyPluginProvider do
    def health(_opts), do: {:unhealthy, :parked}
  end

  defmodule RelayWithBadPlugin do
    use GenServer
    def start({test_pid, table}), do: GenServer.start(__MODULE__, {test_pid, table})

    @impl true
    def init({test_pid, table}) do
      # channel points at self/0 (replies :bound) so the data stream reads as
      # bound and only the unhealthy plugin drives a convergence action.
      overwatch = %Overwatch{
        init_error: nil,
        channel: self(),
        executor_id: "ce1",
        extensions: table
      }

      {:ok, {test_pid, overwatch}}
    end

    @impl true
    def handle_call(:get_overwatch_state, _from, {_t, overwatch} = s), do: {:reply, overwatch, s}
    def handle_call(:stream_ref_status, _from, s), do: {:reply, :bound, s}

    @impl true
    def handle_info({:retry_plugin_restart, plugin_id}, {t, _} = s) do
      send(t, {:retry_restart, plugin_id})
      {:noreply, s}
    end
  end

  describe "event-driven un-park (6.5)" do
    @ns "reconciler_unpark_test"
    setup :set_mimic_global

    setup do
      assert eventually(fn -> Orchestrator.cluster_ready?() end)
      start_supervised!(Activity)
      start_supervised!({Loop, [sweep_interval_ms: 60_000, debounce_ms: 10]})
      {:ok, _} = ensure_hub(@ns)
      on_exit(fn -> HubManager.stop_hub(@ns) end)
      :ok
    end

    test "a returning executor un-parks its parked relays immediately" do
      attach_telemetry([:flezha, :reconcile, :action], :action)

      {:ok, parked} = ParkedRelay.start(self())
      on_exit(fn -> if Process.alive?(parked), do: GenServer.stop(parked) end)
      inject(@ns, "r_parked", "f1", [{node(), parked}])

      # An executor for the relay's factory is available.
      stub(FleetManager, :active_executors, fn @ns -> ["ce_live"] end)
      stub(FleetManager, :executors_for_factory, fn @ns, "f1" -> ["ce_live"] end)
      stub(RelayManager, :assign_executor, fn @ns, "f1" -> "ce_live" end)

      results = Loop.reconcile_namespace(@ns)

      assert {"r_parked", :degraded, actions} =
               Enum.find(results, fn {id, _, _} -> id == "r_parked" end)

      assert :unpark in actions
      assert_receive {:migrated, "ce_live"}, 2_000
      assert_receive {:telemetry, :action, _m, %{relay_id: "r_parked", action: :unpark}}, 2_000
    end
  end

  describe "plugin escalation (7.2)" do
    @esc_ns "reconciler_escalation_test"

    setup do
      assert eventually(fn -> Orchestrator.cluster_ready?() end)
      start_supervised!(Activity)
      # escalation_threshold: 1 → escalate on the first persistently-unhealthy probe.
      start_supervised!(
        {Loop, [sweep_interval_ms: 60_000, debounce_ms: 10, escalation_threshold: 1]}
      )

      {:ok, _} = ensure_hub(@esc_ns)
      on_exit(fn -> HubManager.stop_hub(@esc_ns) end)
      :ok
    end

    test "a persistently-unhealthy running plugin is restarted under the governor" do
      attach_telemetry([:flezha, :reconcile, :action], :action)

      table = %Table{
        plugins: %{"bad" => %Plugin{id: "bad", provider: UnhealthyPluginProvider, opts: %{}}},
        phases: %{"bad" => :running},
        owned_pids: %{}
      }

      {:ok, relay} = RelayWithBadPlugin.start({self(), table})
      on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
      inject(@esc_ns, "r_esc", "f1", [{node(), relay}])

      results = Loop.reconcile_namespace(@esc_ns)

      assert {"r_esc", :degraded, actions} =
               Enum.find(results, fn {id, _, _} -> id == "r_esc" end)

      assert :restart_plugin in actions
      assert_receive {:retry_restart, "bad"}, 2_000

      assert_receive {:telemetry, :action, _m, %{relay_id: "r_esc", action: :restart_plugin}},
                     2_000
    end
  end

  # ---------- helpers ----------

  defp attach_telemetry(event, tag) do
    test_pid = self()
    id = "self-heal-#{tag}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      event,
      fn _e, m, md, _ -> send(test_pid, {:telemetry, tag, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp ensure_hub(ns) do
    case HubManager.start_hub(ns) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  defp inject(ns, relay_id, factory_id, node_pids) do
    hub_id = Identificator.hub(ns)
    relay = %{id: relay_id, factory_id: factory_id, opts: []}
    cspec = %{id: relay_id, start: {Flezha.Relay.Overwatch, :start_link, [{ns, relay}]}}
    :ok = ProcessRegistry.bulk_insert(hub_id, %{relay_id => {cspec, node_pids, %{}}})
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(50)
        eventually(fun, attempts - 1)
    end
  end
end
