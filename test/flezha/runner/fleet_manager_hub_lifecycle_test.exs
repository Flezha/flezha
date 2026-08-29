defmodule Flezha.Runner.FleetManagerHubLifecycleTest do
  @moduledoc """
  A namespace hub outlives its runner while it still holds registered
  (durably-suspended) operatives. `runner_cleanup` reaps the dead runner ref but
  must NOT `stop_hub` while operatives remain — otherwise a parked fleet whose
  sole runner restarts loses every Overwatch and its durable registry, breaking
  the "still addressable" promise (workflow_operative.md). When the registry is
  empty, cleanup still tears the hub down.

  Drives `FleetManager` directly with a synthetic `:inactive` runner against a
  real (but operative-less) ProcessHub, injecting a registry row the way
  `HubManagerRegistryTest` does — no SDK / no runner process needed.
  """
  # async: false: writes Application env; the live controller and its fleet.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.HubRegistryTestHelper

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager

  setup do
    original = :sys.get_state(FleetManager)
    keepalive = Application.get_env(:flezha, :runner_keepalive)

    on_exit(fn ->
      :sys.replace_state(FleetManager, fn _ -> original end)
      restore_env(:runner_keepalive, keepalive)
    end)

    :ok
  end

  test "cleanup preserves a hub that still holds a registered operative" do
    ns = fresh_ns("keep")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    inject_operative(ns, "parked-1")
    assert HubManager.registered_operatives?(ns)

    seed_inactive(ns, "dead_runner")
    cleanup_now()

    assert HubManager.hub_alive?(ns) === true, "hub torn down despite a registered operative"
    refute Map.has_key?(runner_map(ns), "dead_runner"), "dead runner ref must still be reaped"
  end

  test "cleanup tears down a hub whose registry is empty" do
    ns = fresh_ns("empty")
    {:ok, _} = ensure_hub(ns)
    on_exit(fn -> HubManager.stop_hub(ns) end)

    refute HubManager.registered_operatives?(ns)

    seed_inactive(ns, "dead_runner")
    cleanup_now()

    assert HubManager.hub_alive?(ns) === false, "an empty hub should still be reclaimed"
  end

  test "get_pid returns a clean error (never raises) when the hub is gone" do
    ns = fresh_ns("nohub")
    assert HubManager.get_pid(ns, "whoever") == {:error, "hub not alive"}
  end

  # --- helpers -----------------------------------------------------------

  defp fresh_ns(tag), do: "hub_lifecycle_#{tag}_#{System.unique_integer([:positive])}"

  defp cleanup_now() do
    Application.put_env(:flezha, :runner_keepalive, "0")
    :ok = GenServer.call(FleetManager, :runner_cleanup)
  end

  defp seed_inactive(ns, runner_id) do
    entry = {runner_id, {:inactive, System.system_time(:millisecond) - 1}}

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, ns], Map.new([entry]))
    end)
  end

  defp runner_map(ns), do: :sys.get_state(FleetManager).runner_namespaces[ns] || %{}

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, val), do: Application.put_env(:flezha, key, val)
end
