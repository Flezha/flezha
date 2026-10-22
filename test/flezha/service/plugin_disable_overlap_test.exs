defmodule Flezha.Service.PluginDisableOverlapTest do
  @moduledoc """
  M unreachable relays cost one bound, not M: the walk that takes every
  binding off a rescaled group's members (`PluginManager.unbind_all/2`) runs
  the per-relay unsubscribe waits side by side, and names every relay it
  could not reach.
  """

  # async: false: Mimic global mode.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Config.Durations
  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias ProcessHub.Service.ProcessRegistry

  setup :set_mimic_global_until_exit

  @namespace "disable_overlap"
  @members 6

  setup do
    ids = for n <- 1..@members, do: "overlap_op_#{n}"

    ProcessHub |> Mimic.stub(:is_alive?, fn _hub_id -> true end)
    ProcessRegistry |> Mimic.stub(:update, fn _hub, _id, _fun, _opts -> :ok end)

    Mimic.stub(HubManager, :relay_bindings, fn @namespace, id ->
      {:ok, [{"overlap_plugin", %Plugin{id: "overlap_plugin", provider: nil, relay_id: id}}]}
    end)

    # Every relay resolves to a pid that never answers, so the real
    # `Overwatch.plugin_unsubscribe` runs its full bound for each of them.
    wedged = spawn(fn -> receive do: (:never -> :never) end)
    RelayManager |> Mimic.stub(:get_relay_pid, fn _ns, _id -> {:ok, wedged} end)

    {:ok, ids: ids}
  end

  test "M unreachable relays cost one inner bound, not M", %{ids: ids} do
    on_exit(TestHelpers.override_env(:plugin_call_timeout_ms, 200))
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
