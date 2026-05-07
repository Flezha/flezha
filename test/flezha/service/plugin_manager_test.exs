defmodule Flezha.Service.PluginManagerTest do
  @moduledoc """
  Asserts that `Flezha.Service.PluginManager` writes plugin
  subscriptions through to the ProcessHub child-spec on every enable
  and removes them on every disable, matching the contract in
  openspec/specs/plugin-subscription-persistence/spec.md.

  These tests stub `Flezha.Service.HubManager.update_processor_plugins/3`
  with an in-memory plugin store so we can observe the cspec mutation
  applied by each call. They do NOT spin up a ProcessHub instance —
  the `update_processor_plugins/3` cspec-encoding logic is exercised
  separately in `Flezha.Service.HubManagerTest`.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager

  setup :verify_on_exit!
  setup :set_mimic_global

  @namespace "default"
  @processor_id "pm_test_p1"

  setup do
    {:ok, store} = Agent.start_link(fn -> [] end)
    overwatch = spawn_fake_overwatch(self())

    HubManager
    |> Mimic.stub(:update_processor_plugins, fn ns, pid_id, fun ->
      cond do
        ns != @namespace or pid_id != @processor_id ->
          {:error, "processor not found"}

        true ->
          Agent.update(store, fun)
          :ok
      end
    end)

    ProcessorManager
    |> Mimic.stub(:get_processor_pid, fn ns, pid_id ->
      cond do
        ns != @namespace or pid_id != @processor_id ->
          {:error, "processor not found"}

        true ->
          {:ok, overwatch}
      end
    end)

    on_exit(fn ->
      # Drain any plugin_id residue from the singleton PluginManager so
      # tests stay isolated despite sharing a registered process.
      Map.keys(PluginManager.get_plugins())
      |> Enum.each(&PluginManager.unregister_plugin/1)
    end)

    {:ok, store: store, overwatch: overwatch}
  end

  describe "enable_plugin/2 cspec persistence" do
    # If this fails, plugins enabled via `flz plugin enable` after
    # `flz processor start` will silently disappear on the next
    # Overwatch restart, taking the data plane offline until manually
    # re-enabled (the May 3 flezha_market incident).
    test "writes the plugin to the cspec on success", %{store: store} do
      plugin = build_plugin("kafka_p1_trades", topic: "p1.trades.v1")

      assert {:ok, "plugin enabled"} =
               PluginManager.enable_plugin("kafka_p1_trades", plugin)

      assert [{"kafka_p1_trades", %Plugin{} = persisted}] = Agent.get(store, & &1)
      assert persisted.id == "kafka_p1_trades"
      assert persisted.opts == %{topic: "p1.trades.v1"}
    end

    # If this fails, an Overwatch that rejects the subscribe (e.g.
    # duplicate plugin_id, bad subscription) will still have its cspec
    # rewritten — restart will resurrect the rejected plugin.
    test "does not write to cspec when the Overwatch returns {:error, _}",
         %{store: store, overwatch: overwatch} do
      send(overwatch, {:reject_next, {:error, "boom"}})

      plugin = build_plugin("rejected", topic: "x")

      assert {:error, "boom"} = PluginManager.enable_plugin("rejected", plugin)
      assert [] = Agent.get(store, & &1)
    end

    # If this fails, a transient cspec write error will be surfaced
    # to the caller as a hard failure even though the runtime
    # subscribe succeeded — operators will believe the enable failed
    # and may retry, double-subscribing.
    test "logs at :error and replies :ok when the cspec update fails after a successful subscribe" do
      HubManager
      |> Mimic.stub(:update_processor_plugins, fn _ns, _pid_id, _fun ->
        {:error, :no_response}
      end)

      plugin = build_plugin("transient_fail", topic: "y")

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, "plugin enabled"} =
                   PluginManager.enable_plugin("transient_fail", plugin)
        end)

      assert log =~ "plugin"
      assert log =~ "transient_fail"
      assert log =~ "cspec update failed"
    end

    # If this fails, two concurrent enable_plugin calls for distinct
    # plugin_ids on the same processor will lose one of them on
    # restart, partially restoring the data plane.
    test "two enable_plugin calls for distinct plugin_ids produce a cspec with both",
         %{store: store} do
      plugin_a = build_plugin("plug_a", topic: "a")
      plugin_b = build_plugin("plug_b", topic: "b")

      assert {:ok, _} = PluginManager.enable_plugin("plug_a", plugin_a)
      assert {:ok, _} = PluginManager.enable_plugin("plug_b", plugin_b)

      ids =
        store
        |> Agent.get(& &1)
        |> Enum.map(fn {id, _} -> id end)
        |> Enum.sort()

      assert ids == ["plug_a", "plug_b"]
    end
  end

  describe "disable_plugin/1 cspec persistence" do
    # If this fails, a disabled plugin will be re-subscribed on the
    # next Overwatch restart, leaking back into the data plane.
    test "removes the plugin from the cspec on success", %{store: store} do
      plugin = build_plugin("dis_a", topic: "a")
      assert {:ok, _} = PluginManager.enable_plugin("dis_a", plugin)
      assert [_] = Agent.get(store, & &1)

      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin("dis_a")
      assert [] = Agent.get(store, & &1)
    end

    # If this fails, a cspec-write failure on disable could leave the
    # cspec stale (still listing the plugin) while the runtime
    # unsubscribes — the next restart resurrects the disabled plugin.
    test "does not unsubscribe from the Overwatch if the cspec update fails",
         %{overwatch: overwatch} do
      plugin = build_plugin("dis_persist_fail", topic: "z")
      assert {:ok, _} = PluginManager.enable_plugin("dis_persist_fail", plugin)

      HubManager
      |> Mimic.stub(:update_processor_plugins, fn _ns, _pid_id, _fun ->
        {:error, :no_response}
      end)

      send(overwatch, :record_unsubscribes)
      assert {:error, :no_response} = PluginManager.disable_plugin("dis_persist_fail")
      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, []}, 500
    end

    # If this fails, disabling a plugin that was never enabled
    # surfaces a confusing error instead of the documented "not found"
    # response — mostly affects operator UX.
    test "is a no-op (returns error) when the plugin_id is not in the cspec" do
      assert {:error, "plugin not found"} = PluginManager.disable_plugin("never_enabled")
    end
  end

  defp build_plugin(id, opts) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: Map.new(opts),
      subscriptions: [{:on_output, []}],
      namespace: @namespace,
      processor_id: @processor_id
    )
  end

  defp spawn_fake_overwatch(_test_pid) do
    spawn_link(fn -> fake_overwatch_loop(:queue.new(), []) end)
  end

  defp fake_overwatch_loop(rejects, unsubs) do
    receive do
      {:"$gen_call", from, {:plugin_subscribe, _id, _plugin}} ->
        case :queue.out(rejects) do
          {{:value, reply}, rest} ->
            GenServer.reply(from, reply)
            fake_overwatch_loop(rest, unsubs)

          {:empty, _} ->
            GenServer.reply(from, :ok)
            fake_overwatch_loop(rejects, unsubs)
        end

      {:"$gen_call", from, {:plugin_unsubscribe, id}} ->
        GenServer.reply(from, :ok)
        fake_overwatch_loop(rejects, [id | unsubs])

      {:reject_next, reply} ->
        fake_overwatch_loop(:queue.in(reply, rejects), unsubs)

      :record_unsubscribes ->
        fake_overwatch_loop(rejects, [])

      {:dump_unsubscribes, caller} ->
        send(caller, {:unsubscribes, unsubs})
        fake_overwatch_loop(rejects, unsubs)

      _ ->
        fake_overwatch_loop(rejects, unsubs)
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
