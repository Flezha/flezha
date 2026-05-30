defmodule FlezhaTest.Executor.FleetManagerZombieTest do
  @moduledoc """
  Regression coverage for the zombie-executor failure class observed under
  the chaos soak: an executor container is SIGKILL'd, but its controller-side
  ControlUnit GenServer stays alive AND its id stays in the `:active` set
  (its `{:flz_closed}` disconnect signal never arrived). Such a "zombie" still
  looks active, so a relay round-robin-assigned to it silently never produces.

  Two complementary fixes are exercised:

    * Eviction completeness — `{:evict_unreachable, ids}` drops a zombie from
      the active set (and `do_evict_executor` stops its supervisor, taking the
      orphaned cu down with it). The periodic liveness sweep feeds this call
      the ids whose end-to-end ping failed.

    * Assignment safety — `RelayManager.assign_executor/2` and
      `FleetManager.executor_reachable?/1` skip any executor that does not
      answer a fresh ping, so a relay is never bound to a lingering zombie.

  Drives `Flezha.Executor.FleetManager` directly with synthetic active-executor
  state (no real SDK / no app_data) so it runs alongside a live chaos cluster.
  """

  alias Flezha.Executor.FleetManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.RelayManager

  use ExUnit.Case, async: false

  @namespace "zombie_test_namespace_#{System.unique_integer([:positive])}"

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

  # A stand-in for a *reachable* executor: registers under the cu via-name that
  # `ControlUnit.ping_executor/1` looks up and answers `{:ping_executor, _}`
  # with `{:ok, :pong}`. Torn down on test exit.
  defp register_live_cu(executor_id) do
    name = Identificator.control_unit(executor_id)
    {:ok, responder} = FakeCu.start_link(name)
    on_exit(fn -> if Process.alive?(responder), do: GenServer.stop(responder) end)
    responder
  end

  test "evict_unreachable drops the listed zombie and keeps live peers" do
    seed_active(["zombie_a", "live_b"])
    assert "zombie_a" in FleetManager.active_executors(@namespace)
    assert "live_b" in FleetManager.active_executors(@namespace)

    :ok = GenServer.call(FleetManager, {:evict_unreachable, ["zombie_a"]})

    active = FleetManager.active_executors(@namespace)
    refute "zombie_a" in active, "evicted zombie must not linger as active"
    assert "live_b" in active, "untouched peer must survive eviction"
  end

  test "executor_reachable?/1 is false for an id whose control unit is absent" do
    # A SIGKILL'd executor whose cu is gone (or a never-registered id) is by
    # definition unreachable — no cu means no ping round-trip.
    refute FleetManager.executor_reachable?("ghost_no_cu_#{System.unique_integer([:positive])}")
  end

  test "executor_reachable?/1 is true only when the cu answers a ping" do
    eid = "live_cu_#{System.unique_integer([:positive])}"
    _responder = register_live_cu(eid)

    assert FleetManager.executor_reachable?(eid)
  end

  test "assign_executor prefers a reachable executor over a lingering zombie" do
    live = "live_cu_#{System.unique_integer([:positive])}"
    zombie = "zombie_no_cu_#{System.unique_integer([:positive])}"

    _responder = register_live_cu(live)
    seed_active([live, zombie])

    # Ping filtering is on by default; with one reachable and one zombie in the
    # pool, every assignment must land on the reachable one.
    for _ <- 1..20 do
      assert RelayManager.assign_executor(@namespace, nil) == live
    end
  end

  test "assign_executor falls back to the raw pool when none answer (no starvation)" do
    a = "init_a_#{System.unique_integer([:positive])}"
    b = "init_b_#{System.unique_integer([:positive])}"
    seed_active([a, b])

    # Neither has a cu, so neither pings; rather than starve a relay (a real
    # executor may still be initialising), one of the pool is returned.
    picked = RelayManager.assign_executor(@namespace, nil)
    assert picked in [a, b]
  end
end

defmodule FakeCu do
  @moduledoc false
  # Minimal control-unit stand-in: answers `{:ping_executor, _}` with
  # `{:ok, :pong}` so `ControlUnit.ping_executor/1` sees a reachable executor.
  use GenServer

  def start_link(name), do: GenServer.start_link(__MODULE__, nil, name: name)
  @impl true
  def init(_), do: {:ok, %{}}
  @impl true
  def handle_call({:ping_executor, _timeout}, _from, state), do: {:reply, {:ok, :pong}, state}
end
