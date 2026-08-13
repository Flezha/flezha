defmodule FlezhaTest.Runner.FleetManagerGhostTest do
  @moduledoc """
  Regression coverage for the stream ghost-runner accumulation that
  appeared under rapid repeated runner kill/restart (chaos soak): a dead
  runner whose control channel closed was never removed from the active
  set when the disconnect handler's operative-reroute side effects raised
  (`:noproc`/`ArgumentError` against a hub mid-restart), so
  `active_runners/1` kept reporting corpses and operatives got assigned to a
  dead node.

  These tests drive `Flezha.Runner.FleetManager` directly with synthetic
  active-runner state (no real SDK / no app_data) so they can run in the
  presence of a live chaos cluster.
  """

  alias Flezha.Runner.FleetManager

  use ExUnit.Case, async: false

  # A namespace with no running ProcessHub so the reroute side effects
  # (`HubManager.runner_operatives/2` → ETS match on an undefined table) raise,
  # exercising the crash-proof eviction path.
  @namespace "ghost_test_namespace_#{System.unique_integer([:positive])}"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  defp seed_active(runner_ids) do
    ts = System.system_time(:millisecond)
    entries = Map.new(runner_ids, fn id -> {id, {:active, ts}} end)

    :sys.replace_state(FleetManager, fn state ->
      put_in(state, [:runner_namespaces, @namespace], entries)
    end)
  end

  test "superseded-session marks are pruned past their TTL on the cleanup tick" do
    fresh = "fence_fresh_#{System.unique_integer([:positive])}"
    aged = "fence_aged_#{System.unique_integer([:positive])}"

    FleetManager.mark_session_superseded(fresh)
    FleetManager.mark_session_superseded(aged)

    # Age one mark past the 10-minute TTL, then drive the periodic cleanup.
    true =
      :ets.update_element(
        :flezha_superseded_sessions,
        aged,
        {2, :erlang.monotonic_time(:millisecond) - 700_000}
      )

    :ok = GenServer.call(FleetManager, :runner_cleanup)

    refute FleetManager.superseded_session?(aged)
    assert FleetManager.superseded_session?(fresh)
    :ets.delete(:flezha_superseded_sessions, fresh)
  end

  test "a closed runner is removed from the active set even when reroute side effects fail" do
    seed_active(["ghost_a", "ghost_b"])
    assert "ghost_a" in FleetManager.active_runners(@namespace)
    assert "ghost_b" in FleetManager.active_runners(@namespace)

    # Mirrors ControlChannel.handle_connection_closed/1.
    :ok = GenServer.call(FleetManager, {:runner_disconnected, @namespace, "ghost_a"})

    active = FleetManager.active_runners(@namespace)
    refute "ghost_a" in active, "dead runner must not linger as active"
    assert "ghost_b" in active, "live peer must survive the disconnect"
  end

  test "active set does not accumulate ghosts under repeated kill/restart churn" do
    # Simulate N rounds of: fresh runner connects (added to active set by a
    # peer), prior one's control channel closes. The active set must never
    # grow past the live runners, regardless of reroute failures.
    seed_active(["live_peer", "round_0"])

    Enum.each(1..8, fn round ->
      :sys.replace_state(FleetManager, fn state ->
        update_in(state, [:runner_namespaces, @namespace], fn ns ->
          Map.put(ns, "round_#{round}", {:active, System.system_time(:millisecond)})
        end)
      end)

      :ok =
        GenServer.call(FleetManager, {:runner_disconnected, @namespace, "round_#{round - 1}"})
    end)

    # Drain the final outstanding id; "live_peer" + "round_8" remain.
    :ok = GenServer.call(FleetManager, {:runner_disconnected, @namespace, "round_8"})

    active = FleetManager.active_runners(@namespace)
    assert "live_peer" in active

    assert Enum.all?(active, fn id -> id == "live_peer" end),
           "no dead round_* ghosts may remain, got: #{inspect(active)}"
  end
end
