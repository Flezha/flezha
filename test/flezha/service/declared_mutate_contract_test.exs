defmodule Flezha.Service.DeclaredMutateContractTest do
  @moduledoc """
  Pins the one piece of `process_hub` Flezha reaches for that is not part of its
  public API: the coordinator's `{:declared_mutate, {:add, [spec]}}` call, which
  `Flezha.Service.RelayManager.Init.redeclare/2` uses to rewrite a
  relay's declared child spec in place: its bindings, and when its run
  window ends.

  Flezha needs it because the declared set is written at start and at stop and
  has no update path, while a relay is bound to a plugin between the two. The registry
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
  use Mimic

  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Process.Cspec
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RegistryUpdate
  alias Flezha.Service.RelayManager.Init
  alias Flezha.Service.RelayManager.Teardown
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.State.TestHelpers
  alias ProcessHub.Service.DeclaredChildren

  @namespace "declared_mutate_contract_ns"
  @relay_id "declared_mutate_op"

  setup %{tmp: tmp_dir} do
    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
    Application.put_env(:flezha, :process_hub_reconcile_grace_ms, 50)
    Application.put_env(:flezha, :process_hub_reconcile_interval_ms, 60_000)
    on_exit(fn -> HubManager.stop_hub(@namespace) end)

    hub_id = Identificator.hub(@namespace)
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, @relay_id,
        recorded_runner: nil,
        opts: []
      )

    {:ok, hub_id: hub_id}
  end

  # fails when: an enable whose entry never becomes readable here goes on to
  # its bind, which answers that the plugin it just enabled is not enabled.
  # The relays bound to it are subscribed all the same: the home runs, and a
  # retry answers already enabled, which subscribes nothing.
  test "an enable whose entry never shows here is refused as an unanswered registry" do
    test_pid = self()
    stub(HubManager, :plugin_entry, fn _namespace, _name -> :error end)
    stub(Flezha.Config.Backoff, :wait, fn _ladder, _attempt, _target -> :ok end)

    stub(Flezha.Service.PluginRecords, :bindings, fn @namespace, "unreadable" ->
      [{@relay_id, "unreadable", %Plugin{id: "unreadable", provider: nil, namespace: @namespace}}]
    end)

    stub(Flezha.Relay.Overwatch, :plugin_subscribe, fn _pid, id, _binding, _entry ->
      send(test_pid, {:subscribed, id})
      :ok
    end)

    plugin =
      Plugin.new(id: "unreadable", provider: __MODULE__.NoopProvider, namespace: @namespace)

    assert {:error, :registry_unavailable} = PluginManager.enable_plugin("unreadable", plugin)
    assert_received {:subscribed, "unreadable"}
  end

  # fails when: an enable is kept anywhere but the declared set, so a cold
  # start forgets the plugin, or a disable leaves an entry a restart revives.
  test "a plugin is its own declared entry from its enable to its disable", %{hub_id: hub_id} do
    enable("own_entry")
    assert {:plugin, "own_entry"} in declared_ids(hub_id)

    refute Enum.any?(
             HubManager.declared_relays(@namespace),
             &(&1.id == {:plugin, "own_entry"})
           )

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "own_entry")
    refute {:plugin, "own_entry"} in declared_ids(hub_id)
  end

  # fails when: a process_hub release moves the leader-side declared mutation, so
  # the re-declare answers without writing and every bind becomes a silent
  # no-op that only shows up on the next restart.
  test "a bind reaches the declared entry, starting and stopping nothing", %{hub_id: hub_id} do
    before_pid = HubManager.get_pid(@namespace, @relay_id)
    bind("contract_plugin")

    assert ["contract_plugin"] = declared_binding_ids(hub_id)

    assert declared_relay_ids(hub_id) == [@relay_id],
           "the upsert added or removed an entry instead of replacing one"

    assert HubManager.get_pid(@namespace, @relay_id) == before_pid,
           "re-declaring restarted the relay; it must touch no process"
  end

  # fails when: a re-declare merges instead of replacing, so an unbind can never
  # remove a binding from the record a restart reads.
  test "an unbind removes the binding from the declared entry", %{hub_id: hub_id} do
    bind("to_be_unbound")
    assert ["to_be_unbound"] = declared_binding_ids(hub_id)

    assert :ok = PluginManager.unbind_plugin(@namespace, "to_be_unbound", @relay_id)
    assert [] = declared_binding_ids(hub_id)
  end

  # fails when: the declared list is built in the caller from a read taken before
  # a concurrent write landed, so two binds race and one is silently dropped
  # from the record a restart rebuilds from. The registry row merges concurrent
  # transforms inside the registry; the declared write must copy from it, and
  # re-check, rather than compute its own list.
  test "concurrent binds on one relay all reach the declared entry", %{hub_id: hub_id} do
    ids = for i <- 1..8, do: "racing_plugin_#{i}"
    Enum.each(ids, &enable/1)

    ids
    |> Task.async_stream(&bind_only/1, max_concurrency: 8, timeout: 30_000)
    |> Stream.run()

    assert declared_binding_ids(hub_id) == Enum.sort(ids),
           "a concurrent bind was lost from the declared entry"

    assert {:ok, row} = HubManager.relay_bindings(@namespace, @relay_id)
    assert row |> Enum.map(&elem(&1, 0)) |> Enum.sort() == Enum.sort(ids)
  end

  # fails when: an undeclared relay gets a declaration invented for it, which
  # would let a bind resurrect a relay a stop removed for good.
  test "a relay with no declared entry is refused rather than declared" do
    assert {:error, :relay_not_declared} =
             RelayManager.redeclare(@namespace, "never_declared_op")
  end

  # fails when: a leader whose hub is not running, a peer still starting or
  # back inside the node-down grace, fails the write at once with noproc.
  test "a leader whose hub is down is asked again, and refused as transient past the budget",
       %{hub_id: hub_id} do
    on_exit(TestHelpers.override_env(:hub_declared_mutate_ms, 300))
    stub(DeclaredChildren, :leader, fn _hub -> :ghost@nowhere end)
    ends_at = System.os_time(:millisecond) + 60_000

    assert {:error, :hub_unavailable} =
             Init.write_plugins(@namespace, @relay_id, RegistryUpdate.put_window_end(ends_at))

    assert declared_opt(hub_id, :max_duration_ends_at) == nil

    asks = :counters.new(1, [])

    stub(DeclaredChildren, :leader, fn _hub ->
      :counters.add(asks, 1, 1)
      if :counters.get(asks, 1) == 1, do: :ghost@nowhere, else: node()
    end)

    assert :ok = RelayManager.redeclare(@namespace, @relay_id)
    assert declared_opt(hub_id, :max_duration_ends_at) == ends_at
    assert :counters.get(asks, 1) >= 2
  end

  # fails when: a controller running with auto-restore off is told its binding
  # is durable. That mode declares nothing at relay start either, so the
  # write must persist nothing and still succeed.
  test "durable starts disabled: the re-declare writes nothing and still succeeds",
       %{hub_id: hub_id} do
    Application.put_env(:flezha, :auto_restore_enabled, false)
    on_exit(fn -> Application.put_env(:flezha, :auto_restore_enabled, true) end)

    assert :ok = RelayManager.redeclare(@namespace, @relay_id)
    assert [] = declared_binding_ids(hub_id)
  end

  # Fails when: the run window's end lives anywhere a move, a restart or a
  # second bind does not read, or a later write replaces it, so the window
  # restarts and the run never reaches its `max_duration_ms`.
  test "the first bind's window end is kept by the row and the declared entry",
       %{hub_id: hub_id} do
    assert :ok = Init.stamp_window_end(@namespace, unstamped(), 2_000)
    ends_at = await_declared_end(hub_id)
    assert Init.window_end(@namespace, @relay_id) == ends_at

    later = RegistryUpdate.put_window_end(ends_at + 60_000)
    assert :ok = Init.write_plugins(@namespace, @relay_id, later)
    assert Init.window_end(@namespace, @relay_id) == ends_at, "a second end replaced the first"
    assert declared_opt(hub_id, :max_duration_ends_at) == ends_at

    bind("window_plugin")
    assert declared_opt(hub_id, :max_duration_ends_at) == ends_at, "a bind dropped the end"
  end

  test "a class with no window records no end", %{hub_id: hub_id} do
    assert :ok = Init.stamp_window_end(@namespace, unstamped(), nil)
    assert Init.window_end(@namespace, @relay_id) == nil
    assert declared_opt(hub_id, :max_duration_ends_at) == nil
  end

  # Fails when: a stop leaves the end behind, so a new run of the same id
  # starts with its predecessor's window already spent.
  test "a stop then a start of the same id opens a new window", %{hub_id: hub_id} do
    assert :ok = Init.stamp_window_end(@namespace, unstamped(), 1)
    first = await_declared_end(hub_id)

    assert :ok = Teardown.hub_stop_children(@namespace, [@relay_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, @relay_id, recorded_runner: nil, opts: [])

    assert Init.window_end(@namespace, @relay_id) == nil

    assert :ok = Init.stamp_window_end(@namespace, unstamped(), 60_000)
    assert await_declared_end(hub_id) > first
  end

  # The stamp is written off the caller.
  defp await_declared_end(hub_id) do
    assert NodeTestHelper.wait_until(
             fn -> declared_opt(hub_id, :max_duration_ends_at) != nil end,
             5_000
           ),
           "no window end reached the declared entry"

    declared_opt(hub_id, :max_duration_ends_at)
  end

  defp unstamped, do: %{id: @relay_id, opts: [max_duration_ends_at: nil]}

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
        :ok = PluginManager.bind_plugin(@namespace, name, @relay_id, hooks: [{:on_output, []}])
      )

  defp declared_binding_ids(hub_id),
    do: hub_id |> declared_opt(:bindings) |> List.wrap() |> Enum.map(&elem(&1, 0)) |> Enum.sort()

  defp declared_opt(hub_id, key) do
    %{children: children} = DeclaredChildren.declared_children(hub_id)
    {:ok, arg} = children |> Enum.find(&(&1.id == @relay_id)) |> Cspec.start_arg()

    relay =
      case arg do
        {_ns, relay} -> relay
        {_ns, _runner, relay} -> relay
      end

    Keyword.get(relay.opts, key)
  end

  defp declared_ids(hub_id) do
    %{children: children} = DeclaredChildren.declared_children(hub_id)
    Enum.map(children, & &1.id)
  end

  defp declared_relay_ids(hub_id),
    do: hub_id |> declared_ids() |> Enum.filter(&is_binary/1) |> Enum.sort()

  defmodule NoopProvider do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_output(ctx, _opts), do: {:ok, ctx}
  end
end
