defmodule FlezhaTest.Executor.FleetManagerGhostTest do
  @moduledoc """
  Regression coverage for the unbounded ghost-executor accumulation that
  appeared under rapid repeated executor kill/restart (chaos soak): a dead
  executor whose control channel closed was never removed from the active
  set when the disconnect handler's relay-reroute side effects raised
  (`:noproc`/`ArgumentError` against a hub mid-restart), so
  `active_executors/1` kept reporting corpses and relays got assigned to a
  dead node.

  These tests drive `Flezha.Executor.FleetManager` directly with synthetic
  active-executor state (no real SDK / no app_data) so they can run in the
  presence of a live chaos cluster.
  """

  alias Flezha.Executor.FleetManager

  use ExUnit.Case, async: false

  # A namespace with no running ProcessHub so the reroute side effects
  # (`HubManager.executor_relays/2` → ETS match on an undefined table) raise,
  # exercising the crash-proof eviction path.
  @namespace "ghost_test_namespace_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  defp seed_active(executor_ids) do
    ts = System.system_time(:millisecond)
    entries = Map.new(executor_ids, fn id -> {id, {:active, ts}} end)

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:executor_namespaces, @namespace], entries)
    end)
  end

  test "a closed executor is removed from the active set even when reroute side effects fail" do
    seed_active(["ghost_a", "ghost_b"])
    assert "ghost_a" in FleetManager.active_executors(@namespace)
    assert "ghost_b" in FleetManager.active_executors(@namespace)

    # Mirrors ControlChannel.handle_connection_closed/1.
    :ok = GenServer.call(FleetManager, {:executor_disconnected, @namespace, "ghost_a"})

    active = FleetManager.active_executors(@namespace)
    refute "ghost_a" in active, "dead executor must not linger as active"
    assert "ghost_b" in active, "live peer must survive the disconnect"
  end

  test "active set does not accumulate ghosts under repeated kill/restart churn" do
    # Simulate N rounds of: fresh executor connects (added to active set by a
    # peer), prior one's control channel closes. The active set must never
    # grow past the live executors, regardless of reroute failures.
    seed_active(["live_peer", "round_0"])

    Enum.each(1..8, fn round ->
      :sys.replace_state(FleetManager, fn state ->
        update_in(state, [:executor_namespaces, @namespace], fn ns ->
          Map.put(ns, "round_#{round}", {:active, System.system_time(:millisecond)})
        end)
      end)

      :ok =
        GenServer.call(FleetManager, {:executor_disconnected, @namespace, "round_#{round - 1}"})
    end)

    # Drain the final outstanding id; "live_peer" + "round_8" remain.
    :ok = GenServer.call(FleetManager, {:executor_disconnected, @namespace, "round_8"})

    active = FleetManager.active_executors(@namespace)
    assert "live_peer" in active
    assert Enum.all?(active, fn id -> id == "live_peer" end),
           "no dead round_* ghosts may remain, got: #{inspect(active)}"
  end
end
