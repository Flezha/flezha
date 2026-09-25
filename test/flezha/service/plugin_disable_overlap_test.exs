defmodule Flezha.Service.PluginDisableOverlapTest do
  @moduledoc """
  M unreachable operators cost one bound, not M.

  The teardown walk used to be `Enum.each` over the operator ids on the
  CALLER's side. Every item funnels into one registered `PluginManager`, which
  serves one message at a time, and the per-operator `Overwatch` unsubscribe
  wait happens INSIDE its `handle_call` — so a caller-side fan-out only fills
  its mailbox: the waits still run end to end and every caller queued past the
  outer bound reports a reachable operator as undisabled. The overlap has to
  be created where the serialisation is, and this drill is what says so.

  Red before the change: `disable_operators/2` did not exist and the walk
  took M × the inner bound.
  """

  # async: false: Mimic global mode; Mimic stubs a registered manager.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Config.Durations
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias ProcessHub.Service.ProcessRegistry

  setup :set_mimic_global_until_exit

  @namespace "disable_overlap"
  @members 6

  setup do
    ids = for n <- 1..@members, do: "overlap_op_#{n}"

    ProcessHub |> Mimic.stub(:is_alive?, fn _hub_id -> true end)
    ProcessRegistry |> Mimic.stub(:update, fn _hub, _id, _fun, _opts -> :ok end)

    # Every operator resolves to a pid that never answers, so the real
    # `Overwatch.plugin_unsubscribe` runs its full 2 s bound for each of them.
    # No stub: the wait under test is the production one.
    wedged = spawn(fn -> receive do: (:never -> :never) end)
    OperatorManager |> Mimic.stub(:get_operator_pid, fn _ns, _id -> {:ok, wedged} end)

    for id <- ids, do: PluginManager.register_plugin(plugin_id(id), {@namespace, id})
    on_exit(fn -> for id <- ids, do: PluginManager.unregister_plugin(plugin_id(id)) end)

    {:ok, ids: ids}
  end

  defp plugin_id(operator_id), do: "overlap_plugin_#{operator_id}"

  test "M unreachable operators cost one inner bound, not M", %{ids: ids} do
    inner_ms = Durations.get(:plugin_call_timeout_ms)

    {elapsed_us, reply} =
      :timer.tc(fn -> PluginManager.disable_operators(@namespace, ids) end)

    elapsed_ms = div(elapsed_us, 1_000)

    # One bound plus slack, never `@members` of them. The serial walk took
    # `@members * inner_ms`; the assertion is the midpoint, so it cannot pass
    # by accident on a slow box.
    assert elapsed_ms < @members * inner_ms / 2,
           "the walk took #{elapsed_ms} ms — #{@members} bounds of #{inner_ms} ms, not one"

    # The contract survives the overlap: every one that could not be disabled
    # is NAMED, and none is a reason to abandon the ones behind it.
    assert {:error, {:undisabled, undisabled}} = reply
    assert Enum.sort(undisabled) == Enum.sort(Enum.map(ids, &plugin_id/1))

    # Every registration is dropped either way — a plugin left registered
    # against a torn-down operator is what the walk exists to prevent.
    registered = Map.keys(PluginManager.get_plugins())
    assert Enum.all?(ids, &(plugin_id(&1) not in registered))
  end

  test "the fan-out bound sits strictly inside the service call bound" do
    # `stop-shared-services-dying-on-their-own-input`: an inner bound equal to
    # the outer one is no bound at all — both give up at the same instant and
    # the outer reports a reachable target as unreachable.
    assert Durations.get(:plugin_call_timeout_ms) <
             Durations.get(:plugin_disable_fanout_ms)

    assert Durations.get(:plugin_disable_fanout_ms) <
             Durations.get(:plugin_service_call_ms)
  end
end
