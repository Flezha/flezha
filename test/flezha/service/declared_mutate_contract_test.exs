defmodule Flezha.Service.DeclaredMutateContractTest do
  @moduledoc """
  Pins the one piece of `process_hub` Flezha reaches for that is not part of its
  public API: the coordinator's `{:declared_mutate, {:add, [spec]}}` call, which
  `Flezha.Service.OperatorManager.Init.redeclare/2` uses to rewrite an
  operator's declared child spec in place.

  Flezha needs it because the declared set is written at start and at stop and
  has no update path, while an operator is bound to a plugin between the two. The registry
  row cannot stand in: an auto-recovery hub opens its backend with
  `recovery_replay: false`, so its live registry starts empty after a restart
  and a row written before one is never read back.

  Without this test a release of `process_hub` that renames or removes that
  message breaks binding durability silently: every bind would still answer
  success, and the loss would only show on the next restart. If this fails after
  a dependency bump, the fix is to adopt whatever public declare-update function
  that release ships, not to reach for another internal.
  """

  # async: false: writes Application env, and owns a hub on a shared on-disk root.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Process.Cspec
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias ProcessHub.Service.DeclaredChildren

  @namespace "declared_mutate_contract_ns"
  @operator_id "declared_mutate_op"

  setup %{tmp: tmp_dir} do
    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
    Application.put_env(:flezha, :process_hub_reconcile_grace_ms, 50)
    Application.put_env(:flezha, :process_hub_reconcile_interval_ms, 60_000)
    # No runner is ever seeded, so the start's placement could only wait out its budget.
    Application.put_env(:flezha, :hub_runner_assign_wait_ms, 0)
    on_exit(fn -> HubManager.stop_hub(@namespace) end)

    hub_id = Identificator.hub(@namespace)
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, @operator_id,
        recorded_runner: nil,
        opts: []
      )

    {:ok, hub_id: hub_id}
  end

  # fails when: an enable is kept anywhere but the declared set, so a cold
  # start forgets the plugin, or a disable leaves an entry a restart revives.
  test "a plugin is its own declared entry from its enable to its disable", %{hub_id: hub_id} do
    enable("own_entry")
    assert {:plugin, "own_entry"} in declared_ids(hub_id)

    refute Enum.any?(
             HubManager.declared_operators(@namespace),
             &(&1.id == {:plugin, "own_entry"})
           )

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "own_entry")
    refute {:plugin, "own_entry"} in declared_ids(hub_id)
  end

  # fails when: a process_hub release moves the leader-side declared mutation, so
  # the re-declare answers without writing and every bind becomes a silent
  # no-op that only shows up on the next restart.
  test "a bind reaches the declared entry, starting and stopping nothing", %{hub_id: hub_id} do
    before_pid = HubManager.get_pid(@namespace, @operator_id)
    bind("contract_plugin")

    assert ["contract_plugin"] = declared_binding_ids(hub_id)

    assert declared_operator_ids(hub_id) == [@operator_id],
           "the upsert added or removed an entry instead of replacing one"

    assert HubManager.get_pid(@namespace, @operator_id) == before_pid,
           "re-declaring restarted the operator; it must touch no process"
  end

  # fails when: a re-declare merges instead of replacing, so an unbind can never
  # remove a binding from the record a restart reads.
  test "an unbind removes the binding from the declared entry", %{hub_id: hub_id} do
    bind("to_be_unbound")
    assert ["to_be_unbound"] = declared_binding_ids(hub_id)

    assert :ok = PluginManager.unbind_plugin(@namespace, "to_be_unbound", @operator_id)
    assert [] = declared_binding_ids(hub_id)
  end

  # fails when: the declared list is built in the caller from a read taken before
  # a concurrent write landed, so two binds race and one is silently dropped
  # from the record a restart rebuilds from. The registry row merges concurrent
  # transforms inside the registry; the declared write must copy from it, and
  # re-check, rather than compute its own list.
  test "concurrent binds on one operator all reach the declared entry", %{hub_id: hub_id} do
    ids = for i <- 1..8, do: "racing_plugin_#{i}"
    Enum.each(ids, &enable/1)

    ids
    |> Task.async_stream(&bind_only/1, max_concurrency: 8, timeout: 30_000)
    |> Stream.run()

    assert declared_binding_ids(hub_id) == Enum.sort(ids),
           "a concurrent bind was lost from the declared entry"

    assert {:ok, row} = HubManager.operator_bindings(@namespace, @operator_id)
    assert row |> Enum.map(&elem(&1, 0)) |> Enum.sort() == Enum.sort(ids)
  end

  # fails when: an undeclared operator gets a declaration invented for it, which
  # would let a bind resurrect an operator a stop removed for good.
  test "an operator with no declared entry is refused rather than declared" do
    assert {:error, :operator_not_declared} =
             OperatorManager.redeclare(@namespace, "never_declared_op")
  end

  # fails when: a controller running with auto-restore off is told its binding
  # is durable. That mode declares nothing at operator start either, so the
  # write must persist nothing and still succeed.
  test "durable starts disabled: the re-declare writes nothing and still succeeds",
       %{hub_id: hub_id} do
    Application.put_env(:flezha, :auto_restore_enabled, false)
    on_exit(fn -> Application.put_env(:flezha, :auto_restore_enabled, true) end)

    assert :ok = OperatorManager.redeclare(@namespace, @operator_id)
    assert [] = declared_binding_ids(hub_id)
  end

  defp enable(name) do
    plugin = Plugin.new(id: name, provider: __MODULE__.NoopProvider, namespace: @namespace)
    assert {:ok, _} = PluginManager.enable_plugin(name, plugin)
  end

  defp bind(name) do
    enable(name)
    bind_only(name)
  end

  defp bind_only(name),
    do:
      assert(
        :ok = PluginManager.bind_plugin(@namespace, name, @operator_id, hooks: [{:on_output, []}])
      )

  defp declared_binding_ids(hub_id) do
    %{children: children} = DeclaredChildren.declared_children(hub_id)
    {:ok, arg} = children |> Enum.find(&(&1.id == @operator_id)) |> Cspec.start_arg()

    operator =
      case arg do
        {_ns, operator} -> operator
        {_ns, _runner, operator} -> operator
      end

    operator.opts |> Keyword.get(:bindings, []) |> Enum.map(&elem(&1, 0)) |> Enum.sort()
  end

  defp declared_ids(hub_id) do
    %{children: children} = DeclaredChildren.declared_children(hub_id)
    Enum.map(children, & &1.id)
  end

  defp declared_operator_ids(hub_id),
    do: hub_id |> declared_ids() |> Enum.filter(&is_binary/1) |> Enum.sort()

  defmodule NoopProvider do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_output(ctx, _opts), do: {:ok, ctx}
  end
end
