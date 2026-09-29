defmodule Flezha.Service.PluginManagerTest do
  @moduledoc """
  Asserts that `Flezha.Service.PluginManager` writes plugins
  and their hooks through to the ProcessHub child-spec on every enable
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
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Directory
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.ProcessRegistry

  setup :verify_on_exit!
  setup :set_mimic_global_until_exit

  @namespace "default"
  @operator_id "pm_test_p1"

  setup do
    {:ok, store} = Agent.start_link(fn -> base_cspec([]) end)
    overwatch = spawn_fake_overwatch(self())

    ProcessHub
    |> Mimic.stub(:is_alive?, fn _hub_id -> true end)

    ProcessRegistry
    |> Mimic.stub(:update, fn _hub_id, operator_id, fun, _opts ->
      if operator_id == @operator_id do
        Agent.update(store, fn cspec ->
          {new_cspec, _, _} = fun.(cspec, [], %{})
          new_cspec
        end)

        :ok
      else
        {:error, :not_found}
      end
    end)

    OperatorManager
    |> Mimic.stub(:get_operator_pid, fn ns, pid_id ->
      if ns == @namespace and pid_id == @operator_id do
        {:ok, overwatch}
      else
        {:error, "operator not found"}
      end
    end)

    # A disable finds its target in the namespaces' registry rows, never in
    # this node's table.
    Mimic.stub(Directory, :namespaces, fn -> [@namespace] end)
    Mimic.stub(Directory, :hub_nodes, fn _ns -> [node()] end)

    Mimic.stub(HubManager, :operator_plugins, fn @namespace ->
      [{@operator_id, read_plugins(store)}]
    end)

    Mimic.stub(HubManager, :declared_plugins, fn @namespace -> [] end)

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

    # Fails when: a failed enable releases its in-memory claim and leaves the
    # durable entry it wrote one line earlier. The plugin is then invisible to
    # `flz plugin list`, which reads the manager's table, while the row still
    # carries it — so the next restart subscribes it
    # and parks the operator if it still cannot start, naming nothing.
    #
    # This is not the crash case `auto-restore-on-boot` requires to self-heal: a
    # controller that dies between the two runs no rollback at all, so its write
    # stands. Rollback covers only a failure the enable itself answered for.
    test "a failed subscribe rolls the durable entry back with the claim",
         %{store: store, overwatch: overwatch} do
      send(overwatch, {:reject_next, {:error, "boom"}})

      plugin = build_plugin("eager_durable", topic: "x")

      assert {:error, "boom"} = PluginManager.enable_plugin("eager_durable", plugin)
      assert [] = read_plugins(store)
      refute Map.has_key?(PluginManager.get_plugins(), "eager_durable")
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
      |> Mimic.stub(:update, fn _hub_id, _operator_id, _fun, _opts ->
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

      other = %{build_plugin("dup_x", topic: "a") | operator_id: "someone_else"}

      assert {:error, {:plugin_id_taken, "dup_x", @namespace, @operator_id}} =
               PluginManager.enable_plugin("dup_x", other)
    end
  end

  describe "a subscription is type-checked before anything durable is written" do
    # Fails when: an ill-typed sub-option is refused only after the cspec write,
    # leaving declared intent for a plugin that can never subscribe — which the
    # convergence loop then tries to heal forever.
    test "an enable refused for an ill-typed sub-option leaves no cspec entry", %{store: store} do
      plugin = %{
        build_plugin("typo_interval", topic: "a")
        | hooks: [{"on_interval", %{"interval_ms" => "soon"}}]
      }

      assert {:error, {:invalid_hook_opts, :interval_ms, "soon"}} =
               PluginManager.enable_plugin("typo_interval", plugin)

      assert [] = read_plugins(store)
      refute Map.has_key?(PluginManager.get_plugins(), "typo_interval")
    end

    test "an unknown extension name is refused without being interned", %{store: store} do
      name = "on_never_enabled_#{System.unique_integer([:positive])}"

      plugin = %{
        build_plugin("typo_name", topic: "a")
        | hooks: [{name, %{}}]
      }

      assert {:error, {:unknown_hook, ^name}} =
               PluginManager.enable_plugin("typo_name", plugin)

      assert [] = read_plugins(store)
      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "what is persisted holds the coerced value, not the text", %{store: store} do
      plugin = %{
        build_plugin("typed_interval", topic: "a")
        | hooks: [{"on_interval", %{"interval_ms" => "5000"}}]
      }

      assert {:ok, "plugin enabled"} = PluginManager.enable_plugin("typed_interval", plugin)

      assert [{"typed_interval", %Plugin{hooks: subs}}] = read_plugins(store)
      assert subs == [{:on_interval, [interval_ms: 5_000]}]
    end
  end

  describe "a CLI enable refuses a plain option it does not read" do
    # Fails when: a word the enable never reads, a removed flag or one a help
    # screen advertised, is dropped, so the plugin is enabled with less than the
    # user wrote and the command reports success.
    test "the option is named and nothing is written", %{store: store} do
      for key <- ["trigger", "position"] do
        err =
          ExUnit.CaptureIO.capture_io(:stderr, fn ->
            assert {:shutdown, 2} = catch_exit(cli_enable("unread_#{key}", %{key => "on_output"}))
          end)

        assert err =~ "--#{key}"
        assert err =~ "--X<key>"
        assert [] = read_plugins(store)
      end

      # The same enable without the unread word is written, so the empty store
      # above is the refusal's doing.
      assert ExUnit.CaptureIO.capture_io(fn -> cli_enable("read_all", %{}) end) =~
               "enabled successfully"

      assert [{"read_all", %Plugin{}}] = read_plugins(store)
    end
  end

  defp cli_enable(plugin_id, extra_opts) do
    scope = %{"namespace" => @namespace, "operator_id" => @operator_id}

    Flezha.Service.CliManager.enable_plugin(
      plugin_id,
      "operator_stdout",
      "{}",
      Jason.encode!(Map.merge(scope, extra_opts))
    )
  end

  describe "disable_plugin/1 cspec persistence" do
    test "removes the plugin from the cspec on success", %{store: store} do
      plugin = build_plugin("dis_a", topic: "a")
      assert {:ok, _} = PluginManager.enable_plugin("dis_a", plugin)
      assert [_] = read_plugins(store)

      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin_group("dis_a")
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
               PluginManager.disable_plugin_group("dis_persist_fail")

      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, []}, 500
    end

    test "is a no-op (returns error) when the plugin_id is not in the cspec" do
      assert {:error, {:plugin_not_found, "never_enabled"}} =
               PluginManager.disable_plugin_group("never_enabled")
    end
  end

  describe "a sink cannot subscribe to previews" do
    test "the Kafka producer on on_preview is refused naming the callback and leaves no row",
         %{store: store, overwatch: overwatch} do
      plugin = %{
        build_plugin("kafka_preview", brokers: "localhost:9092", topic: "t")
        | provider: Flezha.Plugin.Provider.Kafka.Producer,
          hooks: [{:on_preview, []}]
      }

      # The Overwatch's table gives the verdict its subscribe answers.
      catalog = Flezha.Operator.Overwatch.__extensions_catalog__()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, {"k", refusal}} =
                   Table.bulk_subscribe(Table.new(catalog), [{"k", plugin}])

          send(self(), {:refusal, refusal})
        end)

      assert_received {:refusal, refusal}
      assert refusal == {:missing_callback, :on_preview}
      assert log =~ "prepare failed for plugin_id=k"

      send(overwatch, {:reject_next, {:error, refusal}})

      assert {:error, {:missing_callback, :on_preview}} =
               PluginManager.enable_plugin("kafka_preview", plugin)

      assert read_plugins(store) == []
      refute Map.has_key?(PluginManager.get_plugins(), "kafka_preview")
    end
  end

  describe "a disable addressed by its target" do
    # The row carries the plugin, as it does on a node that never ran the
    # enable: the operator moved here, or the enable ran elsewhere.
    setup %{store: store, overwatch: overwatch} do
      plugin = build_plugin("elsewhere", topic: "e")
      Agent.update(store, fn _ -> base_cspec([{"elsewhere", plugin}]) end)
      send(overwatch, :record_unsubscribes)
      refute Map.has_key?(PluginManager.get_plugins(), "elsewhere")
      :ok
    end

    test "detach/3 removes the row and unsubscribes the live operator",
         %{store: store, overwatch: overwatch} do
      assert :ok = PluginManager.detach(@namespace, @operator_id, "elsewhere")
      assert read_plugins(store) == []
      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, ["elsewhere"]}, 500
    end

    test "disable_plugin_group/1 finds it in the rows and disables it",
         %{store: store, overwatch: overwatch} do
      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin_group("elsewhere")
      assert read_plugins(store) == []
      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, ["elsewhere"]}, 500
    end

    test "an operator whose row is between two homes is found in the declared set",
         %{store: store} do
      plugin = build_plugin("elsewhere", topic: "e")
      Mimic.stub(HubManager, :operator_plugins, fn @namespace -> [] end)
      Mimic.stub(HubManager, :declared_plugins, fn @namespace -> [{@operator_id, [plugin]}] end)

      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin_group("elsewhere")
      assert read_plugins(store) == []
    end

    test "the verb is judged in the namespace the rows name" do
      assert PluginManager.target_namespace("elsewhere") == @namespace
      assert PluginManager.target_namespace("nowhere") == "_system"
    end

    test "a detach run on a hub node elsewhere still drops this node's entry" do
      plugin = build_plugin("gateway_listener", [])
      assert :claimed = GenServer.call(PluginManager, {:claim, "gateway_listener", plugin})
      GenServer.cast(PluginManager, {:confirm, "gateway_listener"})
      assert Map.has_key?(PluginManager.get_plugins(), "gateway_listener")

      Mimic.stub(Directory, :hub_nodes, fn _ns -> [:"nohub@127.0.0.1"] end)

      assert {:error, :registry_unavailable} =
               PluginManager.detach(@namespace, @operator_id, "gateway_listener")

      refute Map.has_key?(PluginManager.get_plugins(), "gateway_listener")
    end

    test "a released plugin reads as ended only once its operator is declared no more" do
      Mimic.stub(HubManager, :operator_plugins, fn @namespace, _id ->
        {:error, {:operator_not_found, @namespace, @operator_id}}
      end)

      Mimic.stub(HubManager, :declared_operators, fn @namespace -> [] end)
      assert PluginManager.attachment(@namespace, @operator_id, "l") == :operator_ended

      # Between two homes: declared, with no row yet, for an operator or a
      # group whose members a rescale is restarting.
      Mimic.stub(HubManager, :declared_operators, fn @namespace ->
        [%{id: @operator_id, factory_id: nil}, %{id: "grp#p0", factory_id: nil}]
      end)

      assert PluginManager.attachment(@namespace, @operator_id, "l") == :unknown
      assert PluginManager.attachment(@namespace, "grp", "l") == :unknown
    end

    test "an enable still in flight on this node is refused" do
      assert :claimed =
               GenServer.call(PluginManager, {:claim, "in_flight", build_plugin("in_flight", [])})

      assert {:error, {:plugin_enabling, "in_flight"}} =
               PluginManager.disable_plugin_group("in_flight")

      PluginManager.unregister_plugin("in_flight")
    end
  end

  describe "an operator that does not answer" do
    test "an unsubscribe that never answers leaves the manager alive and names the plugin",
         %{overwatch: overwatch, store: store} do
      plugin = build_plugin("pm_wedged", topic: "w")
      assert {:ok, _} = PluginManager.enable_plugin("pm_wedged", plugin)

      manager = Process.whereis(PluginManager)
      send(overwatch, :go_silent)

      assert {:error, {:undisabled, ["pm_wedged"]}} =
               PluginManager.disable_operator_plugins(@namespace, @operator_id)

      assert Process.whereis(PluginManager) == manager

      # The durable row and the table agree — both say disabled — so a restart
      # cannot read the plugin as disabled and installed at once.
      assert [] = read_plugins(store)
      refute Map.has_key?(PluginManager.get_plugins(), "pm_wedged")
    end

    test "an enable whose subscribe never answers releases its claim",
         %{overwatch: overwatch} do
      # The claim is taken before the subscribe; an exit there would skip the
      # release, and the id then reads as enabled for ever — refused by a later
      # enable and excluded from healing by `Convergence.Loop`.
      send(overwatch, :go_silent)
      plugin = build_plugin("pm_enable_wedged", topic: "w")

      assert {:error, :timeout} = PluginManager.enable_plugin("pm_enable_wedged", plugin)

      assert PluginManager.pending_ids() == MapSet.new()
      refute Map.has_key?(PluginManager.get_plugins(), "pm_enable_wedged")
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

      OperatorManager
      |> Mimic.stub(:get_operator_pid, fn _ns, id ->
        if id in member_ids, do: {:ok, overwatch}, else: {:error, "operator not found"}
      end)

      ProcessRegistry
      |> Mimic.stub(:update, fn _hub_id, operator_id, fun, _opts ->
        if operator_id in member_ids do
          Agent.update(cspecs, fn store ->
            base = Map.get(store, operator_id, member_cspec(operator_id))
            {updated, _, _} = fun.(base, [], %{})
            Map.put(store, operator_id, updated)
          end)

          :ok
        else
          {:error, :not_found}
        end
      end)

      Mimic.stub(HubManager, :operator_plugins, fn @namespace ->
        for {id, cspec} <- Agent.get(cspecs, & &1), do: {id, plugins_of(cspec)}
      end)

      %{members: members, cspecs: cspecs}
    end

    test "every member gets its own suffixed instance carrying its partition", %{cspecs: cspecs} do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}

      assert {:ok, message} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert message =~ "3 partitions"

      store = Agent.get(cspecs, & &1)

      for index <- 0..2 do
        member_id = Flezha.Partition.member_id(@group, index)
        plugin_id = "ohlc_ws" <> Flezha.Partition.member_suffix(index)

        assert [{^plugin_id, persisted}] = plugins_of(store[member_id])
        assert persisted.operator_id == member_id
        assert persisted.opts.partition_index == index
        assert persisted.opts.partition_count == 3
        assert persisted.opts.url == "wss://x"

        {from, to} = Enum.at(Flezha.Partition.ranges(3), index)
        assert persisted.opts.key_group_from == from
        assert persisted.opts.key_group_to == to
      end
    end

    test "disable is symmetric — every suffixed instance goes", %{cspecs: cspecs} do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)

      assert {:ok, message} = PluginManager.disable_plugin_group("ohlc_ws")
      assert message =~ "3 partitions"

      store = Agent.get(cspecs, & &1)
      assert Enum.all?(Map.values(store), &(plugins_of(&1) == []))
      assert Map.keys(PluginManager.get_plugins()) == []
    end

    test "a group whose member rows carry the plugin is detached from this node's empty table",
         %{cspecs: cspecs, overwatch: overwatch} do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)
      Enum.each(Map.keys(PluginManager.get_plugins()), &PluginManager.unregister_plugin/1)
      assert PluginManager.get_plugins() == %{}
      send(overwatch, :record_unsubscribes)

      assert {:ok, message} = PluginManager.disable_plugin_group("ohlc_ws")
      assert message =~ "3 partitions"
      assert Enum.all?(Map.values(Agent.get(cspecs, & &1)), &(plugins_of(&1) == []))

      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, unsubscribed}, 500
      assert Enum.sort(unsubscribed) == ["ohlc_ws#p0", "ohlc_ws#p1", "ohlc_ws#p2"]
    end

    test "detach/3 of a group reaches every current member", %{cspecs: cspecs} do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)
      Enum.each(Map.keys(PluginManager.get_plugins()), &PluginManager.unregister_plugin/1)

      assert :ok = PluginManager.detach(@namespace, @group, "ohlc_ws")
      assert Enum.all?(Map.values(Agent.get(cspecs, & &1)), &(plugins_of(&1) == []))
    end

    test "a fully-already-enabled group re-run is a no-op" do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}

      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert {:ok, :already_enabled} = PluginManager.enable_plugin("ohlc_ws", plugin)
    end

    test "a group re-run heals the member missing its instance" do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)

      PluginManager.unregister_plugin("ohlc_ws" <> Flezha.Partition.member_suffix(1))

      assert {:ok, message} = PluginManager.enable_plugin("ohlc_ws", plugin)
      assert message =~ "enabled on 1 partition(s) (2 already enabled)"
    end

    test "an already-enabled member does not mask a genuine failure elsewhere", %{
      members: members,
      overwatch: overwatch
    } do
      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}
      assert {:ok, _} = PluginManager.enable_plugin("ohlc_ws", plugin)

      failing = Enum.at(members, 1).id
      PluginManager.unregister_plugin("ohlc_ws" <> Flezha.Partition.member_suffix(1))

      OperatorManager
      |> Mimic.stub(:get_operator_pid, fn ns, id ->
        if id == failing, do: {:error, {:operator_not_found, ns, id}}, else: {:ok, overwatch}
      end)

      assert {:error, {:group_enable_partial, 2, 3, [^failing]}} =
               PluginManager.enable_plugin("ohlc_ws", plugin)
    end

    test "a partial enable names who succeeded and who did not", %{
      members: members,
      overwatch: overwatch
    } do
      failing = Enum.at(members, 1).id

      OperatorManager
      |> Mimic.stub(:get_operator_pid, fn ns, id ->
        if id == failing, do: {:error, {:operator_not_found, ns, id}}, else: {:ok, overwatch}
      end)

      plugin = %{build_plugin("ohlc_ws", url: "wss://x") | operator_id: @group}

      assert {:error, {:group_enable_partial, 2, 3, [^failing]}} =
               PluginManager.enable_plugin("ohlc_ws", plugin)
    end

    defp member_cspec(member_id) do
      %{
        id: member_id,
        start:
          {Flezha.Operator.Overwatch, :start_link,
           [
             {@namespace,
              %Flezha.Operator.Builder{
                id: member_id,
                factory_id: "do_nothing",
                init_values: %{},
                opts: [plugins: []]
              }}
           ]}
      }
    end

    defp plugins_of(nil), do: []

    defp plugins_of(%{start: {_m, _f, [{_ns, %Flezha.Operator.Builder{opts: opts}}]}}),
      do: Keyword.get(opts, :plugins, [])
  end

  defp build_plugin(id, opts) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: Map.new(opts),
      hooks: [{:on_output, []}],
      namespace: @namespace,
      operator_id: @operator_id
    )
  end

  # The cspec shape PluginManager rewrites: %{start: {m, f, [{namespace, %Builder{opts: [plugins: [...]]}}]}}
  defp base_cspec(plugins) do
    %{
      id: @operator_id,
      start:
        {Flezha.Operator.Overwatch, :start_link,
         [
           {@namespace,
            %Flezha.Operator.Builder{
              id: @operator_id,
              factory_id: "do_nothing",
              init_values: %{},
              opts: [plugins: plugins]
            }}
         ]}
    }
  end

  defp read_plugins(store) do
    %{start: {_m, _f, [{_ns, %Flezha.Operator.Builder{opts: opts}}]}} =
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

      # A busy Overwatch: it receives, and answers nothing.
      :go_silent ->
        NodeTestHelper.park()

      _ ->
        fake_overwatch_loop(rejects, unsubs)
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
