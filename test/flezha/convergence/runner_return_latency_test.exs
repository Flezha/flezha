defmodule Flezha.Convergence.RunnerReturnLatencyTest do
  @moduledoc """
  The acceptance check driven live: a real runner over the real transport, a
  real operative, and a convergence Loop whose periodic tick is far outside the
  assertion budget — so only the release a connecting runner performs, and the
  immediate tick it triggers, can rebind the operative in time.
  """

  # async: false: real runners, the global EventManager, a shared on-disk root.
  use ExUnit.Case, async: false

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Loop, Planner}
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.OperativeFixture
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "test_temp_namespace"
  @factory "store_input"
  @operative "returning_op"

  # Far outside the budget below. A periodic tick that could bind the operative
  # on its own would make the assertion prove nothing.
  @periodic_tick_ms 120_000
  @budget_ms 5_000

  setup do
    TestHelpers.reset_all_entities()
    ConvergenceSnapshot.reset_recovery_states()

    on_exit(fn ->
      OperativeManager.stop_all_operatives(@namespace)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  @tag :runner_return_latency
  test "an operative waiting out a rung binds within seconds of a capable runner returning" do
    {:ok, _} = start_supervised({Loop, tick_ms: @periodic_tick_ms})

    first = NodeTestHelper.start_runner(@namespace)

    {:ok, _info} =
      @operative
      |> OperativeFixture.get(@factory)
      |> OperativeFixture.start(@namespace)

    assert bound_to?(first), "the operative never bound to the first runner"

    NodeTestHelper.stop_runner(@namespace, first)
    :ok = Loop.tick_now()
    :ok = Loop.tick_now()

    # The outage has been going a while: every cooldown key a waiting operative
    # can be planned on sits on the top rung, which is what a connecting runner
    # has to clear. Seeded rather than waited for, because that rung is five
    # minutes and waiting it out is the defect, not the test.
    seed_top_rung()

    second = NodeTestHelper.start_runner(@namespace)

    assert bound_to?(second),
           "the operative did not bind within #{@budget_ms} ms of a capable runner returning"
  end

  defp bound_to?(runner_id) do
    NodeTestHelper.wait_until(
      fn ->
        case OperativeManager.get_overwatch_state(@namespace, @operative) do
          {:ok, %{runner_id: ^runner_id}} -> true
          _other -> false
        end
      end,
      @budget_ms,
      25
    )
  end

  defp seed_top_rung() do
    top = length(Backoff.ladder(:heal).rungs)
    cooling_until = System.monotonic_time(:millisecond) + List.last(Backoff.ladder(:heal).rungs)

    :sys.replace_state(Loop, fn state ->
      entries =
        Enum.reduce(
          Planner.waiting_keys({@namespace, @operative}),
          state.backoff.entries,
          &Map.put(&2, &1, {top, cooling_until})
        )

      %{state | backoff: %{state.backoff | entries: entries}}
    end)
  end
end
