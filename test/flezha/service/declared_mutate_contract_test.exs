defmodule Flezha.Service.DeclaredMutateContractTest do
  @moduledoc """
  Pins the one piece of `process_hub` Flezha reaches for that is not part of its
  public API: the coordinator's `{:declared_mutate, {:add, [spec]}}` call, which
  `Flezha.Service.OperativeManager.Init.redeclare_plugins/2` uses to rewrite an
  operative's declared child spec in place.

  Flezha needs it because the declared set is written at start and at stop and
  has no update path, while a plugin is enabled between the two. The registry
  row cannot stand in: an auto-recovery hub opens its backend with
  `recovery_replay: false`, so its live registry starts empty after a restart
  and a row written before one is never read back.

  Without this test a release of `process_hub` that renames or removes that
  message breaks plugin durability silently: every enable would still answer
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
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias ProcessHub.Service.DeclaredChildren

  @namespace "declared_mutate_contract_ns"
  @operative_id "declared_mutate_op"

  setup %{tmp: tmp_dir} do
    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
    Application.put_env(:flezha, :process_hub_reconcile_grace_ms, 50)
    Application.put_env(:flezha, :process_hub_reconcile_interval_ms, 60_000)
    # No runner is ever seeded, so the start's placement could only wait out its budget.
    Application.put_env(:flezha, :hub_runner_assign_wait_ms, 0)

    on_exit(fn ->
      # Not per-enable: `enable/1` runs inside a Task in the concurrency case,
      # and `on_exit/2` may only be called from the test process.
      PluginManager.get_plugins() |> Map.keys() |> Enum.each(&PluginManager.unregister_plugin/1)
      HubManager.stop_hub(@namespace)
    end)

    hub_id = Identificator.hub(@namespace)
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, @operative_id,
        recorded_runner: nil,
        opts: []
      )

    {:ok, hub_id: hub_id}
  end

  # fails when: a process_hub release moves the leader-side declared mutation, so
  # the re-declare answers without writing and every plugin enable becomes a
  # silent no-op that only shows up on the next restart.
  test "an enable reaches the declared entry, starting and stopping nothing",
       %{hub_id: hub_id} do
    before_pid = HubManager.get_pid(@namespace, @operative_id)
    enable("contract_plugin")

    assert ["contract_plugin"] = declared_plugin_ids(hub_id)

    assert declared_ids(hub_id) == [@operative_id],
           "the upsert added or removed an entry instead of replacing one"

    assert HubManager.get_pid(@namespace, @operative_id) == before_pid,
           "re-declaring restarted the operative; it must touch no process"
  end

  # fails when: a re-declare merges instead of replacing, so a disable can never
  # remove a plugin from the record a restart reads.
  test "a disable removes the plugin from the declared entry", %{hub_id: hub_id} do
    enable("to_be_disabled")
    assert ["to_be_disabled"] = declared_plugin_ids(hub_id)

    assert {:ok, _} = PluginManager.disable_plugin("to_be_disabled")
    assert [] = declared_plugin_ids(hub_id)
  end

  # fails when: the declared list is built in the caller from a read taken before
  # a concurrent write landed, so two enables race and one is silently dropped
  # from the record a restart rebuilds from. The registry row merges concurrent
  # transforms inside the registry; the declared write must copy from it, and
  # re-check, rather than compute its own list.
  test "concurrent enables on one operative all reach the declared entry",
       %{hub_id: hub_id} do
    ids = for i <- 1..8, do: "racing_plugin_#{i}"

    ids
    |> Task.async_stream(&enable/1, max_concurrency: 8, timeout: 30_000)
    |> Stream.run()

    assert declared_plugin_ids(hub_id) == Enum.sort(ids),
           "a concurrent enable was lost from the declared entry"

    assert {:ok, row} = HubManager.operative_plugins(@namespace, @operative_id)
    assert row |> Enum.map(&elem(&1, 0)) |> Enum.sort() == Enum.sort(ids)
  end

  # fails when: an undeclared operative gets a declaration invented for it, which
  # would let a plugin write resurrect an operative a stop removed for good.
  test "an operative with no declared entry is refused rather than declared" do
    assert {:error, :operative_not_declared} =
             OperativeManager.redeclare_plugins(@namespace, "never_declared_op")
  end

  # fails when: a controller running with auto-restore off is told its plugin is
  # durable. That mode declares nothing at operative start either, so the write
  # must persist nothing and still succeed, rather than fail or promise a
  # durability the deployment has switched off.
  test "durable starts disabled: the re-declare writes nothing and still succeeds",
       %{hub_id: hub_id} do
    Application.put_env(:flezha, :auto_restore_enabled, false)
    on_exit(fn -> Application.put_env(:flezha, :auto_restore_enabled, true) end)

    assert :ok = OperativeManager.redeclare_plugins(@namespace, @operative_id)
    assert [] = declared_plugin_ids(hub_id)
  end

  defp enable(plugin_id) do
    plugin =
      Plugin.new(
        id: plugin_id,
        provider: __MODULE__.NoopProvider,
        opts: %{},
        hooks: [{:on_output, []}],
        namespace: @namespace,
        operative_id: @operative_id
      )

    assert {:ok, _} = PluginManager.enable_plugin(plugin_id, plugin)
  end

  defp declared_plugin_ids(hub_id) do
    %{children: children} = DeclaredChildren.declared_children(hub_id)
    {:ok, arg} = children |> Enum.find(&(&1.id == @operative_id)) |> Cspec.start_arg()

    operative =
      case arg do
        {_ns, operative} -> operative
        {_ns, _runner, operative} -> operative
      end

    operative.opts |> Keyword.get(:plugins, []) |> Enum.map(&elem(&1, 0)) |> Enum.sort()
  end

  defp declared_ids(hub_id) do
    %{children: children} = DeclaredChildren.declared_children(hub_id)
    children |> Enum.map(& &1.id) |> Enum.sort()
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
