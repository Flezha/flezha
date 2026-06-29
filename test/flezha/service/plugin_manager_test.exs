defmodule Flezha.Service.PluginManagerTest do
  @moduledoc """
  Asserts that `Flezha.Service.PluginManager` writes plugin
  subscriptions through to the ProcessHub child-spec on every enable
  and removes them on every disable.

  The post-`auto-restore-on-boot` ordering is **cspec-first**: the
  durable registry update happens before the runtime subscribe. If the
  cspec write fails, no subscribe is attempted; if the subscribe
  fails, the cspec already records the intent and the next
  `Overwatch.init/1` reconciles. These tests stub
  `ProcessHub.Service.ProcessRegistry.update/3` and
  `ProcessHub.is_alive?/1` to simulate the durable registry without
  spinning up a real hub.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin
  alias Flezha.Service.PluginManager
  alias Flezha.Service.OperativeManager
  alias ProcessHub.Service.ProcessRegistry

  setup :verify_on_exit!
  setup :set_mimic_global

  @namespace "default"
  @operative_id "pm_test_p1"

  setup do
    {:ok, store} = Agent.start_link(fn -> base_cspec([]) end)
    overwatch = spawn_fake_overwatch(self())

    ProcessHub
    |> Mimic.stub(:is_alive?, fn _hub_id -> true end)

    ProcessRegistry
    |> Mimic.stub(:update, fn _hub_id, operative_id, fun, _opts ->
      cond do
        operative_id != @operative_id ->
          {:error, :not_found}

        true ->
          Agent.update(store, fn cspec ->
            {new_cspec, _, _} = fun.(cspec, [], %{})
            new_cspec
          end)

          :ok
      end
    end)

    OperativeManager
    |> Mimic.stub(:get_operative_pid, fn ns, pid_id ->
      cond do
        ns != @namespace or pid_id != @operative_id ->
          {:error, "operative not found"}

        true ->
          {:ok, overwatch}
      end
    end)

    on_exit(fn ->
      Map.keys(PluginManager.get_plugins())
      |> Enum.each(&PluginManager.unregister_plugin/1)
    end)

    {:ok, store: store, overwatch: overwatch}
  end

  describe "enable_plugin/2 cspec persistence (cspec-first ordering)" do
    test "writes the plugin to the cspec on success", %{store: store} do
      plugin = build_plugin("kafka_p1_trades", topic: "p1.trades.v1")

      assert {:ok, "plugin enabled"} =
               PluginManager.enable_plugin("kafka_p1_trades", plugin)

      assert [{"kafka_p1_trades", %Plugin{} = persisted}] = read_plugins(store)
      assert persisted.id == "kafka_p1_trades"
      assert persisted.opts == %{topic: "p1.trades.v1"}
    end

    test "subscribe failure leaves the cspec entry in place (durability precedes runtime)",
         %{store: store, overwatch: overwatch} do
      send(overwatch, {:reject_next, {:error, "boom"}})

      plugin = build_plugin("eager_durable", topic: "x")

      assert {:error, "boom"} = PluginManager.enable_plugin("eager_durable", plugin)
      assert [{"eager_durable", _}] = read_plugins(store)
    end

    test "cspec write failure surfaces :registry_unavailable without touching runtime" do
      ProcessHub
      |> Mimic.stub(:is_alive?, fn _hub_id -> false end)

      plugin = build_plugin("hub_down", topic: "y")

      assert {:error, :registry_unavailable} =
               PluginManager.enable_plugin("hub_down", plugin)
    end

    test "two enable_plugin calls for distinct plugin_ids produce a cspec with both",
         %{store: store} do
      plugin_a = build_plugin("plug_a", topic: "a")
      plugin_b = build_plugin("plug_b", topic: "b")

      assert {:ok, _} = PluginManager.enable_plugin("plug_a", plugin_a)
      assert {:ok, _} = PluginManager.enable_plugin("plug_b", plugin_b)

      ids =
        store
        |> read_plugins()
        |> Enum.map(fn {id, _} -> id end)
        |> Enum.sort()

      assert ids == ["plug_a", "plug_b"]
    end
  end

  describe "disable_plugin/1 cspec persistence" do
    test "removes the plugin from the cspec on success", %{store: store} do
      plugin = build_plugin("dis_a", topic: "a")
      assert {:ok, _} = PluginManager.enable_plugin("dis_a", plugin)
      assert [_] = read_plugins(store)

      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin("dis_a")
      assert [] = read_plugins(store)
    end

    test "does not unsubscribe from the Overwatch if the cspec update fails",
         %{overwatch: overwatch} do
      plugin = build_plugin("dis_persist_fail", topic: "z")
      assert {:ok, _} = PluginManager.enable_plugin("dis_persist_fail", plugin)

      ProcessHub
      |> Mimic.stub(:is_alive?, fn _hub_id -> false end)

      send(overwatch, :record_unsubscribes)

      assert {:error, :registry_unavailable} =
               PluginManager.disable_plugin("dis_persist_fail")

      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, []}, 500
    end

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
      operative_id: @operative_id
    )
  end

  # The cspec shape PluginManager rewrites: %{start: {m, f, [{namespace, %Builder{opts: [plugins: [...]]}}]}}
  defp base_cspec(plugins) do
    %{
      id: @operative_id,
      start:
        {Flezha.Operative.Overwatch, :start_link,
         [
           {@namespace,
            %Flezha.Operative.Builder{
              id: @operative_id,
              factory_id: "do_nothing",
              init_values: %{},
              opts: [plugins: plugins]
            }}
         ]}
    }
  end

  defp read_plugins(store) do
    %{start: {_m, _f, [{_ns, %Flezha.Operative.Builder{opts: opts}}]}} =
      Agent.get(store, & &1)

    Keyword.get(opts, :plugins, [])
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
