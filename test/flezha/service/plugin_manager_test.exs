defmodule Flezha.Service.PluginManagerTest do
  @moduledoc """
  Asserts that `Flezha.Service.PluginManager` writes a binding through to its
  operator's ProcessHub child spec on every bind and removes it on every
  unbind, checks it before anything is written, and fans it over a partition
  group's members.

  The order is write first, then the runtime subscribe: a refused binding
  leaves no row, and a subscribe that fails rolls the row back. These tests
  stub `ProcessHub.Service.ProcessRegistry.update/4`, the plugin entries and
  the bound operators' rows, so no hub runs; the live path is
  `FlezhaTest.Plugin.NamedPluginTest`.
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
  alias Flezha.Service.Listener
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.OperatorManager.Teardown
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.ProcessRegistry

  setup :verify_on_exit!
  setup :set_mimic_global_until_exit

  @namespace "default"
  @operator_id "pm_test_p1"

  defmodule NoopProvider do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour,
      options: [topic: [type: :string, doc: "t"], url: [type: :string, doc: "u"]]

    @impl true
    def on_output(ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_interval(_ctx, _opts), do: :ok
  end

  setup do
    {:ok, store} = Agent.start_link(fn -> %{@operator_id => cspec(@operator_id, [])} end)
    overwatch = spawn_fake_overwatch()

    ProcessHub |> Mimic.stub(:is_alive?, fn _hub_id -> true end)

    ProcessRegistry
    |> Mimic.stub(:update, fn _hub_id, operator_id, fun, _opts ->
      if Map.has_key?(Agent.get(store, & &1), operator_id) do
        Agent.update(store, fn rows ->
          {new_cspec, _, _} = fun.(rows[operator_id], [], %{})
          Map.put(rows, operator_id, new_cspec)
        end)
      else
        {:error, :not_found}
      end
    end)

    OperatorManager
    |> Mimic.stub(:get_operator_pid, fn ns, id ->
      if ns == @namespace and Map.has_key?(Agent.get(store, & &1), id),
        do: {:ok, overwatch},
        else: {:error, {:operator_not_found, ns, id}}
    end)

    Mimic.stub(Directory, :hub_nodes, fn _ns -> [node()] end)
    Mimic.stub(Directory, :plugin_hosts, fn _ns, _name -> [] end)
    Mimic.stub(PartitionGroup, :members, fn _ns, _id -> [] end)

    Mimic.stub(HubManager, :plugin_entry, fn @namespace, name ->
      {:ok, %Plugin{id: name, provider: NoopProvider, namespace: @namespace}}
    end)

    Mimic.stub(HubManager, :operator_bindings, fn
      @namespace -> for {id, cspec} <- Agent.get(store, & &1), do: {id, bindings_of(cspec)}
    end)

    Mimic.stub(HubManager, :operator_bindings, fn @namespace, id ->
      case Agent.get(store, & &1[id]) do
        nil -> {:error, {:operator_not_found, @namespace, id}}
        cspec -> {:ok, bindings_of(cspec)}
      end
    end)

    Mimic.stub(HubManager, :declared_bindings, fn @namespace -> [] end)

    # The binds of a plugin that cannot be shared run in its home.
    {:ok, home} =
      Flezha.Plugin.Home.start_link({@namespace, %Plugin{id: "home", provider: NoopProvider}})

    Mimic.stub(ProcessHub, :child_lookup, fn _hub_id, {:plugin, _name} ->
      {%{}, [{node(), home}]}
    end)

    {:ok, store: store, overwatch: overwatch}
  end

  defp read_bindings(store, operator_id \\ @operator_id),
    do: bindings_of(Agent.get(store, & &1[operator_id]))

  describe "bind_plugin/4 writes the binding first" do
    test "the binding reaches the cspec and the operator", %{store: store} do
      assert :ok =
               PluginManager.bind_plugin(@namespace, "sink", @operator_id,
                 hooks: [{:on_output, []}]
               )

      assert [{"sink", %Plugin{} = binding}] = read_bindings(store)
      assert binding.provider == NoopProvider
      assert binding.hooks == [{:on_output, []}]
    end

    # Fails when: a failed bind leaves the durable binding it wrote one line
    # earlier, so the next restart brings up a binding nobody was told about.
    test "a failed subscribe rolls the binding back", %{store: store, overwatch: overwatch} do
      send(overwatch, {:reject_next, {:error, "boom"}})

      assert {:error, "boom"} = PluginManager.bind_plugin(@namespace, "eager", @operator_id)
      assert [] = read_bindings(store)
    end

    test "a hub that is down answers :registry_unavailable" do
      ProcessHub |> Mimic.stub(:is_alive?, fn _hub_id -> false end)

      assert {:error, :registry_unavailable} =
               PluginManager.bind_plugin(@namespace, "hub_down", @operator_id)
    end

    test "two binds of distinct plugins leave both", %{store: store} do
      assert :ok = PluginManager.bind_plugin(@namespace, "plug_a", @operator_id)
      assert :ok = PluginManager.bind_plugin(@namespace, "plug_b", @operator_id)

      assert ["plug_a", "plug_b"] =
               store |> read_bindings() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    end

    test "binding a bound operator again touches nothing", %{store: store} do
      assert :ok = PluginManager.bind_plugin(@namespace, "idem", @operator_id)
      before = read_bindings(store)
      parent = self()

      ProcessRegistry
      |> Mimic.stub(:update, fn _hub_id, _operator_id, _fun, _opts ->
        send(parent, :cspec_write)
        :ok
      end)

      assert :already_bound = PluginManager.bind_plugin(@namespace, "idem", @operator_id)
      refute_received :cspec_write
      assert read_bindings(store) == before
    end

    test "a plugin that is not enabled is plugin.not_found and writes nothing", %{store: store} do
      Mimic.stub(HubManager, :plugin_entry, fn @namespace, _name -> :error end)

      assert {:error, {:plugin_not_found, "nope"}} =
               PluginManager.bind_plugin(@namespace, "nope", @operator_id)

      assert [] = read_bindings(store)
    end
  end

  describe "a binding is type-checked before anything durable is written" do
    # Fails when: an ill-typed sub-option is refused only after the cspec write,
    # leaving a binding that can never come up.
    test "an ill-typed sub-option leaves no binding", %{store: store} do
      assert {:error, {:invalid_hook_opts, :interval_ms, "soon"}} =
               PluginManager.bind_plugin(@namespace, "typo", @operator_id,
                 hooks: [{"on_interval", %{"interval_ms" => "soon"}}]
               )

      assert [] = read_bindings(store)
    end

    test "an unknown hook name is refused without being interned", %{store: store} do
      name = "on_never_enabled_#{System.unique_integer([:positive])}"

      assert {:error, {:unknown_hook, ^name, _allowed}} =
               PluginManager.bind_plugin(@namespace, "typo_name", @operator_id,
                 hooks: [{name, %{}}]
               )

      assert [] = read_bindings(store)
      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "what is written holds the coerced value, not the text", %{store: store} do
      assert :ok =
               PluginManager.bind_plugin(@namespace, "typed", @operator_id,
                 hooks: [{"on_interval", %{"interval_ms" => "5000"}}]
               )

      assert [{"typed", %Plugin{hooks: [{:on_interval, [interval_ms: 5_000]}]}}] =
               read_bindings(store)
    end

    test "a sink on on_preview is refused naming the callback and writes nothing",
         %{store: store, overwatch: overwatch} do
      Mimic.stub(HubManager, :plugin_entry, fn @namespace, name ->
        {:ok,
         %Plugin{
           id: name,
           provider: Flezha.Plugin.Provider.HttpPush,
           namespace: @namespace,
           opts: %{url: "http://127.0.0.1:9/t"}
         }}
      end)

      catalog = Flezha.Operator.Overwatch.__extensions_catalog__()

      plugin = %Plugin{
        id: "k",
        provider: Flezha.Plugin.Provider.HttpPush,
        hooks: [{:on_preview, []}]
      }

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, {"k", {:missing_callback, :on_preview}}} =
                   Table.bulk_subscribe(Table.new(catalog), [{"k", plugin}])
        end)

      assert log =~ "prepare failed for plugin_id=k"
      send(overwatch, {:reject_next, {:error, {:missing_callback, :on_preview}}})

      assert {:error, {:missing_callback, :on_preview}} =
               PluginManager.bind_plugin(@namespace, "sink_preview", @operator_id,
                 hooks: [{:on_preview, []}]
               )

      assert read_bindings(store) == []
    end
  end

  describe "a CLI bind refuses a plain option it does not read" do
    # Fails when: a word the bind never reads is dropped, so the operator is
    # bound with less than the user wrote and the command reports success.
    test "the option is named and nothing is written", %{store: store} do
      err =
        ExUnit.CaptureIO.capture_io(:stderr, fn ->
          assert {:shutdown, 2} = catch_exit(cli_bind("unread", %{"trigger" => "on_output"}))
        end)

      assert err =~ "trigger"
      assert err =~ "plugin.fields_unknown"
      assert [] = read_bindings(store)

      assert ExUnit.CaptureIO.capture_io(fn -> cli_bind("read_all", %{}) end) =~
               "Plugin read_all bound to #{@operator_id}."

      assert [{"read_all", %Plugin{}}] = read_bindings(store)
    end
  end

  defp cli_bind(name, extra_opts) do
    fields = Map.merge(%{"namespace" => @namespace, "operator_id" => @operator_id}, extra_opts)
    Flezha.Service.Cli.call("PluginManager.bind", Jason.encode!([name, fields]), [])
  end

  describe "unbind_plugin/3" do
    test "removes the binding and unsubscribes the operator", %{
      store: store,
      overwatch: overwatch
    } do
      assert :ok = PluginManager.bind_plugin(@namespace, "un_a", @operator_id)
      send(overwatch, :record_unsubscribes)

      assert :ok = PluginManager.unbind_plugin(@namespace, "un_a", @operator_id)

      assert [] = read_bindings(store)
      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, ["un_a"]}, 500
    end

    test "does not unsubscribe when the cspec write fails", %{overwatch: overwatch} do
      assert :ok = PluginManager.bind_plugin(@namespace, "un_fail", @operator_id)
      Mimic.stub(ProcessRegistry, :update, fn _hub, _id, _fun, _opts -> {:error, :boom} end)
      send(overwatch, :record_unsubscribes)

      assert {:error, :boom} = PluginManager.unbind_plugin(@namespace, "un_fail", @operator_id)

      send(overwatch, {:dump_unsubscribes, self()})
      assert_receive {:unsubscribes, []}, 500
    end

    test "a binding the operator does not have is plugin.not_bound" do
      assert {:error, {:plugin_not_bound, "never", @operator_id}} =
               PluginManager.unbind_plugin(@namespace, "never", @operator_id)
    end

    test "an operator that never answers its unsubscribe is still unbound", %{
      store: store,
      overwatch: overwatch
    } do
      assert :ok = PluginManager.bind_plugin(@namespace, "un_wedged", @operator_id)
      send(overwatch, :go_silent)

      assert :ok = PluginManager.unbind_plugin(@namespace, "un_wedged", @operator_id)
      assert [] = read_bindings(store)
    end
  end

  describe "a listener's plugin" do
    test "detach/3 unbinds it and disables it", %{store: store} do
      assert :ok = PluginManager.bind_plugin(@namespace, "listener_x", @operator_id)
      parent = self()

      Mimic.stub(Teardown, :hub_stop_children, fn @namespace, [{:plugin, name}], _timeout ->
        send(parent, {:stopped, name})
        :ok
      end)

      assert :ok = PluginManager.detach(@namespace, @operator_id, "listener_x")
      assert [] = read_bindings(store)
      assert_received {:stopped, "listener_x"}
    end

    test "a released binding reads as ended only once its operator is declared no more" do
      Mimic.stub(HubManager, :declared_operators, fn @namespace -> [] end)
      assert Listener.attachment(@namespace, "gone_op", "l") == :operator_ended

      # Between two homes: declared, with no row yet, for an operator or a
      # group whose members a rescale is restarting.
      Mimic.stub(HubManager, :declared_operators, fn @namespace ->
        [%{id: "gone_op", factory_id: nil}, %{id: "grp#p0", factory_id: nil}]
      end)

      assert Listener.attachment(@namespace, "gone_op", "l") == :unknown
      assert Listener.attachment(@namespace, "grp", "l") == :unknown
    end
  end

  describe "an operator that does not answer" do
    test "a bind whose subscribe never answers writes nothing", %{
      store: store,
      overwatch: overwatch
    } do
      send(overwatch, :go_silent)

      assert {:error, :timeout} =
               PluginManager.bind_plugin(@namespace, "bind_wedged", @operator_id)

      assert [] = read_bindings(store)
    end
  end

  describe "a partition group binds every member" do
    @group "ohlc"

    setup %{store: store} do
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

      Agent.update(store, fn rows ->
        Enum.reduce(members, rows, &Map.put(&2, &1.id, cspec(&1.id, [])))
      end)

      Mimic.stub(PartitionGroup, :members, fn _ns, id ->
        if id == @group, do: members, else: []
      end)

      %{members: members}
    end

    test "every member gets its own suffixed binding carrying its partition", %{store: store} do
      assert :ok = PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)

      for index <- 0..2 do
        member_id = Flezha.Partition.member_id(@group, index)
        id = "ohlc_ws" <> Flezha.Partition.member_suffix(index)

        assert [{^id, binding}] = read_bindings(store, member_id)
        assert binding.operator_id == member_id
        assert binding.opts.partition_index == index
        assert binding.opts.partition_count == 3

        {from, to} = Enum.at(Flezha.Partition.ranges(3), index)
        assert binding.opts.key_group_from == from
        assert binding.opts.key_group_to == to
      end
    end

    test "unbind is symmetric — every member's binding goes", %{store: store, members: members} do
      assert :ok = PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)
      assert :ok = PluginManager.unbind_plugin(@namespace, "ohlc_ws", @group)

      assert Enum.all?(members, &(read_bindings(store, &1.id) == []))
    end

    test "a fully bound group re-run is a no-op" do
      assert :ok = PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)
      assert :already_bound = PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)
    end

    test "a re-run binds the member missing its binding", %{store: store, members: members} do
      assert :ok = PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)
      missing = Enum.at(members, 1).id
      Agent.update(store, &Map.put(&1, missing, cspec(missing, [])))

      assert :ok = PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)
      assert [{"ohlc_ws#p1", _}] = read_bindings(store, missing)
    end

    test "a partial bind names who succeeded and who did not", %{
      members: members,
      overwatch: overwatch,
      store: store
    } do
      failing = Enum.at(members, 1).id

      OperatorManager
      |> Mimic.stub(:get_operator_pid, fn ns, id ->
        if id == failing, do: {:error, {:operator_not_found, ns, id}}, else: {:ok, overwatch}
      end)

      assert {:error, {:group_enable_partial, 2, 3, [^failing]}} =
               PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)

      assert read_bindings(store, failing) == []
    end

    test "a bind every member refuses answers a member's own reason" do
      OperatorManager
      |> Mimic.stub(:get_operator_pid, fn ns, id -> {:error, {:operator_not_found, ns, id}} end)

      assert {:error, {:operator_not_found, @namespace, member}} =
               PluginManager.bind_plugin(@namespace, "ohlc_ws", @group)

      assert String.starts_with?(member, @group <> "#p")
    end

    test "a shareable plugin binds every member", %{store: store, members: members} do
      Mimic.stub(HubManager, :plugin_entry, fn @namespace, name ->
        {:ok, %Plugin{id: name, provider: Flezha.Plugin.Provider.StdOut, namespace: @namespace}}
      end)

      assert :ok = PluginManager.bind_plugin(@namespace, "out", @group)

      for %{id: member_id} <- members do
        assert [{"out#p" <> _, %Plugin{operator_id: ^member_id}}] =
                 read_bindings(store, member_id)
      end
    end
  end

  defp cspec(operator_id, bindings) do
    %{
      id: operator_id,
      start:
        {Flezha.Operator.Overwatch, :start_link,
         [
           {@namespace,
            %Flezha.Operator.Builder{
              id: operator_id,
              factory_id: "do_nothing",
              init_values: %{},
              opts: [bindings: bindings]
            }}
         ]}
    }
  end

  defp bindings_of(nil), do: []

  defp bindings_of(%{start: {_m, _f, [{_ns, %Flezha.Operator.Builder{opts: opts}}]}}),
    do: Keyword.get(opts, :bindings, [])

  defp spawn_fake_overwatch do
    spawn_link(fn -> fake_overwatch_loop(:queue.new(), []) end)
  end

  defp fake_overwatch_loop(rejects, unsubs) do
    receive do
      {:"$gen_call", from, {:plugin_subscribe, _id, _binding, _entry}} ->
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
end
