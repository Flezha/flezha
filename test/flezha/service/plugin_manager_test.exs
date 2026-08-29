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

  # async: false: Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin
  alias Flezha.Operative.PartitionGroup
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

    test "re-enabling for the same target is a no-op that touches nothing", %{store: store} do
      plugin = build_plugin("idem_a", topic: "a")

      assert {:ok, "plugin enabled"} = PluginManager.enable_plugin("idem_a", plugin)
      cspec_before = read_plugins(store)
      parent = self()

      ProcessRegistry
      |> Mimic.stub(:update, fn _hub_id, _operative_id, _fun, _opts ->
        send(parent, :cspec_write)
        :ok
      end)

      assert {:ok, :already_enabled} = PluginManager.enable_plugin("idem_a", plugin)

      # Neither the cspec nor the live subscription may be touched.
      refute_received :cspec_write
      assert read_plugins(store) == cspec_before
    end

    test "the same plugin id for a different target is refused naming the holder" do
      plugin = build_plugin("dup_x", topic: "a")
      assert {:ok, _} = PluginManager.enable_plugin("dup_x", plugin)

      other = %{build_plugin("dup_x", topic: "a") | operative_id: "someone_else"}
      assert {:error, message} = PluginManager.enable_plugin("dup_x", other)
      assert message =~ "already registered for #{@namespace}/#{@operative_id}"
    end
  end

  describe "a subscription is type-checked before anything durable is written" do
    # Fails when: an ill-typed sub-option is refused only after the cspec write,
    # leaving declared intent for a plugin that can never subscribe — which the
    # convergence loop then tries to heal forever.
    test "an enable refused for an ill-typed sub-option leaves no cspec entry", %{store: store} do
      plugin = %{
        build_plugin("typo_interval", topic: "a")
        | subscriptions: [{"on_interval", %{"interval_ms" => "soon"}}]
      }

      assert {:error, {:invalid_subscription_opts, {:interval_ms, "soon"}}} =
               PluginManager.enable_plugin("typo_interval", plugin)

      assert [] = read_plugins(store)
      refute Map.has_key?(PluginManager.get_plugins(), "typo_interval")
    end

    test "an unknown extension name is refused without being interned", %{store: store} do
      name = "on_never_enabled_#{System.unique_integer([:positive])}"

      plugin = %{
        build_plugin("typo_name", topic: "a")
        | subscriptions: [{name, %{}}]
      }

      assert {:error, {:unknown_extension, ^name}} =
               PluginManager.enable_plugin("typo_name", plugin)

      assert [] = read_plugins(store)
      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "what is persisted holds the coerced value, not the text", %{store: store} do
      plugin = %{
        build_plugin("typed_interval", topic: "a")
        | subscriptions: [{"on_interval", %{"interval_ms" => "5000"}}]
      }

      assert {:ok, "plugin enabled"} = PluginManager.enable_plugin("typed_interval", plugin)

      assert [{"typed_interval", %Plugin{subscriptions: subs}}] = read_plugins(store)
      assert subs == [{:on_interval, [interval_ms: 5_000]}]
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

  describe "a partition group fans enable and disable over its members" do
    @group "ohlc"

    setup %{overwatch: overwatch} do
      members =
        3
        |> Flezha.Partition.ranges()
        |> Enum.with_index()
        |> Enum.map(fn {range, index} ->
          %{
            id: Flezha.Partition.member_id(@group, index),
            partition: %{group: @group, index: index, count: 3, range: range}
          }
        end)

      {:ok, cspecs} = Agent.start_link(fn -> %{} end)
      member_ids = Enum.map(members, & &1.id)

      Mimic.stub(PartitionGroup, :members, fn _ns, id ->
        if id == @group, do: members, else: []
      end)

      OperativeManager
      |> Mimic.stub(:get_operative_pid, fn _ns, id ->
        if id in member_ids, do: {:ok, overwatch}, else: {:error, "operative not found"}
      end)

      ProcessRegistry
      |> Mimic.stub(:update, fn _hub_id, operative_id, fun, _opts ->
        if operative_id in member_ids do
          Agent.update(cspecs, fn store ->
            base = Map.get(store, operative_id, member_cspec(operative_id))
            {updated, _, _} = fun.(base, [], %{})
            Map.put(store, operative_id, updated)
          end)

          :ok
        else
          {:error, :not_found}
        end
      end)

      %{members: members, cspecs: cspecs}
    end

    test "every member gets its own suffixed instance carrying its partition", %{cspecs: cspecs} do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operative_id: @group}

      assert {:ok, message} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert message =~ "3 partitions"

      store = Agent.get(cspecs, & &1)

      for index <- 0..2 do
        member_id = Flezha.Partition.member_id(@group, index)
        plugin_id = "ohlc_ws" <> Flezha.Partition.member_suffix(index)

        assert [{^plugin_id, persisted}] = plugins_of(store[member_id])
        assert persisted.operative_id == member_id
        assert persisted.opts.partition_index == index
        assert persisted.opts.partition_count == 3
        assert persisted.opts.url == "wss://x"

        {from, to} = Enum.at(Flezha.Partition.ranges(3), index)
        assert persisted.opts.key_group_from == from
        assert persisted.opts.key_group_to == to
      end
    end

    test "disable is symmetric — every suffixed instance goes", %{cspecs: cspecs} do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operative_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)

      assert {:ok, message} = PluginManager.disable_plugin_group("ohlc_ws")
      assert message =~ "3 partitions"

      store = Agent.get(cspecs, & &1)
      assert Enum.all?(Map.values(store), &(plugins_of(&1) == []))
      assert Map.keys(PluginManager.get_plugins()) == []
    end

    test "a fully-already-enabled group re-run is a no-op" do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operative_id: @group}

      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert {:ok, :already_enabled} = PluginManager.enable_plugin("ohlc_ws", plugin)
    end

    test "a group re-run heals the member missing its instance" do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operative_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)

      PluginManager.unregister_plugin("ohlc_ws" <> Flezha.Partition.member_suffix(1))

      assert {:ok, message} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert message =~ "enabled on 1 partition(s) (2 already enabled)"
    end

    test "an already-enabled member does not mask a genuine failure elsewhere", %{
      members: members,
      overwatch: overwatch
    } do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operative_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)

      failing = Enum.at(members, 1).id
      PluginManager.unregister_plugin("ohlc_ws" <> Flezha.Partition.member_suffix(1))

      OperativeManager
      |> Mimic.stub(:get_operative_pid, fn _ns, id ->
        if id == failing, do: {:error, "operative not found"}, else: {:ok, overwatch}
      end)

      assert {:error, message} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert message =~ "enabled on 2 of 3 partitions"
      assert message =~ failing
    end

    test "a partial enable names who succeeded and who did not", %{
      members: members,
      overwatch: overwatch
    } do
      failing = Enum.at(members, 1).id

      OperativeManager
      |> Mimic.stub(:get_operative_pid, fn _ns, id ->
        if id == failing, do: {:error, "operative not found"}, else: {:ok, overwatch}
      end)

      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operative_id: @group}

      assert {:error, message} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert message =~ "of 3 partitions"
      assert message =~ failing
    end

    defp member_cspec(member_id) do
      %{
        id: member_id,
        start:
          {Flezha.Operative.Overwatch, :start_link,
           [
             {@namespace,
              %Flezha.Operative.Builder{
                id: member_id,
                factory_id: "do_nothing",
                init_values: %{},
                opts: [plugins: []]
              }}
           ]}
      }
    end

    defp plugins_of(nil), do: []

    defp plugins_of(%{start: {_m, _f, [{_ns, %Flezha.Operative.Builder{opts: opts}}]}}),
      do: Keyword.get(opts, :plugins, [])
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
