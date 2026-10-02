defmodule Flezha.Service.PluginDisableOverlapTest do
  @moduledoc """
  M unreachable operators cost one bound, not M: the walk that takes every
  binding off a rescaled group's members (`PluginManager.unbind_all/2`) runs
  the per-operator unsubscribe waits side by side, and names every operator it
  could not reach.
  """

  # async: false: Mimic global mode.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Config.Durations
  alias Flezha.Plugin
  alias Flezha.Service.HubManager
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

    Mimic.stub(HubManager, :operator_bindings, fn @namespace, id ->
      {:ok, [{"overlap_plugin", %Plugin{id: "overlap_plugin", provider: nil, operator_id: id}}]}
    end)

    # Every operator resolves to a pid that never answers, so the real
    # `Overwatch.plugin_unsubscribe` runs its full bound for each of them.
    wedged = spawn(fn -> receive do: (:never -> :never) end)
    OperatorManager |> Mimic.stub(:get_operator_pid, fn _ns, _id -> {:ok, wedged} end)

    {:ok, ids: ids}
  end

  test "M unreachable operators cost one inner bound, not M", %{ids: ids} do
    inner_ms = Durations.get(:plugin_call_timeout_ms)

    {elapsed_us, reply} = :timer.tc(fn -> PluginManager.unbind_all(@namespace, ids) end)
    elapsed_ms = div(elapsed_us, 1_000)

    # One bound plus slack, never `@members` of them; the assertion is the
    # midpoint, so it cannot pass by accident on a slow box.
    assert elapsed_ms < @members * inner_ms / 2,
           "the walk took #{elapsed_ms} ms — #{@members} bounds of #{inner_ms} ms, not one"

    assert {:error, {:undisabled, undisabled}} = reply
    assert Enum.sort(undisabled) == Enum.sort(ids)
  end

  test "the fan-out bound sits strictly between the call bound and the route bound" do
    assert Durations.get(:plugin_call_timeout_ms) < Durations.get(:plugin_disable_fanout_ms)
    assert Durations.get(:plugin_disable_fanout_ms) < Durations.get(:route_call_timeout_ms)
  end
end
