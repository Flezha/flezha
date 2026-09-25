defmodule Flezha.Convergence.RunnerReturnLatencyTest do
  @moduledoc """
  The acceptance check driven live: a real runner over the real transport, a
  real operator, and a convergence Loop whose periodic tick is far outside the
  assertion budget — so only the release a connecting runner performs, and the
  immediate tick it triggers, can rebind the operator in time. The same fleet
  then checks the health a rebound operator reports.
  """

  # async: false: real runners, the global EventManager, a shared on-disk root.
  use ExUnit.Case, async: false

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Loop, Planner}
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "test_temp_namespace"
  @factory "store_input"
  @operator "returning_op"

  # Far outside the budget below. A periodic tick that could bind the operator
  # on its own would make the assertion prove nothing.
  @periodic_tick_ms 120_000
  @budget_ms 5_000

  setup do
    TestHelpers.reset_all_cells()
    ConvergenceSnapshot.reset_recovery_states()

    on_exit(fn ->
      OperatorManager.stop_all_operators(@namespace)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  @tag :runner_return_latency
  test "an operator waiting out a rung binds within seconds of a capable runner returning" do
    {:ok, _} = start_supervised({Loop, tick_ms: @periodic_tick_ms})

    first = NodeTestHelper.start_runner(@namespace)

    {:ok, _info} =
      @operator
      |> OperatorFixture.get(@factory)
      |> OperatorFixture.start(@namespace)

    assert bound_to?(first), "the operator never bound to the first runner"

    NodeTestHelper.stop_runner(@namespace, first)
    :ok = Loop.tick_now()
    :ok = Loop.tick_now()

    # The outage has been going a while: every cooldown key a waiting operator
    # can be planned on sits on the top rung, which is what a connecting runner
    # has to clear. Seeded rather than waited for, because that rung is five
    # minutes and waiting it out is the defect, not the test.
    seed_top_rung()

    second = NodeTestHelper.start_runner(@namespace)

    assert bound_to?(second),
           "the operator did not bind within #{@budget_ms} ms of a capable runner returning"
  end

  @tag :runner_return_latency
  test "an operator rebound after its runner died reads healthy within seconds, not after the stall grace" do
    {:ok, _} = start_supervised({Loop, tick_ms: @periodic_tick_ms})

    {first_port, first} = NodeTestHelper.start_runner_port(@namespace, port_opts: [:exit_status])
    on_exit(fn -> NodeTestHelper.ensure_port_dead(first_port) end)

    {:ok, _info} =
      @operator
      |> OperatorFixture.get(@factory)
      |> OperatorFixture.start(@namespace)

    assert bound_to?(first), "the operator never bound to the first runner"

    # A data stream names only the operators that have taken an input on it,
    # and only those are flagged when it closes.
    OperatorManager.send_io(@operator, @namespace, "before the stop", %{"input_type" => "2"})

    assert NodeTestHelper.wait_until(
             fn -> Enum.any?(data_stream_channels(first), &carries_operator?/1) end,
             @budget_ms
           ),
           "no data stream of the first runner ever carried the operator"

    # A runner that dies closes its data streams and its control session in no
    # fixed order, and only a data stream the controller sees close first
    # declares the stall. The close is delivered first here, then the runner is
    # signalled the way a user stops one.
    Enum.each(data_stream_channels(first), &send(&1, {:flz_closed, :test_close, :eof}))
    {:os_pid, os_pid} = :erlang.port_info(first_port, :os_pid)
    {_, 0} = System.cmd("kill", ["-TERM", Integer.to_string(os_pid)])
    assert_receive {^first_port, {:exit_status, _}}, @budget_ms

    second = NodeTestHelper.start_runner(@namespace)
    assert bound_to?(second), "the operator never bound to the second runner"

    assert NodeTestHelper.wait_until(
             fn ->
               :ok = Loop.tick_now()
               ConvergenceSnapshot.operator_health(@namespace, @operator) == :healthy
             end,
             @budget_ms,
             100
           ),
           "the rebound operator still read " <>
             "#{ConvergenceSnapshot.operator_health(@namespace, @operator)} after #{@budget_ms} ms"
  end

  defp bound_to?(runner_id) do
    NodeTestHelper.wait_until(
      fn ->
        case OperatorManager.get_overwatch_state(@namespace, @operator) do
          {:ok, %{runner_id: ^runner_id}} -> true
          _other -> false
        end
      end,
      @budget_ms,
      25
    )
  end

  # Every primary and secondary data stream channel `runner_id` holds, by the
  # `<runner>_scp_<n>` and `<runner>_scs_<n>` names they register under.
  defp data_stream_channels(runner_id) do
    pattern = ~r/^#{Regex.escape(runner_id)}_sc[ps]_\d+$/

    for {key, pid} <-
          Registry.select(Flezha.Identificator, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}]),
        is_binary(key) and key =~ pattern,
        do: pid
  end

  defp carries_operator?(channel) do
    Map.has_key?(:sys.get_state(channel).operator_mapping, @operator)
  end

  defp seed_top_rung() do
    top = length(Backoff.ladder(:heal).rungs)
    cooling_until = System.monotonic_time(:millisecond) + List.last(Backoff.ladder(:heal).rungs)

    :sys.replace_state(Loop, fn state ->
      entries =
        Enum.reduce(
          Planner.waiting_keys({@namespace, @operator}),
          state.backoff.entries,
          &Map.put(&2, &1, {top, cooling_until})
        )

      %{state | backoff: %{state.backoff | entries: entries}}
    end)
  end
end
