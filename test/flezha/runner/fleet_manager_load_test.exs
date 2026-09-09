defmodule FlezhaTest.Runner.FleetManagerLoadTest do
  @moduledoc """
  Runner selection: the fairness cap that narrows the rendezvous ranking, the
  per-runner count it reads, and the two placement paths that reach it — one
  operative at a time, and the recovery batch that places many before any of
  them is recorded.
  """

  # async: false: drives the live FleetManager's state and the node-wide
  # Identificator registry.

  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Load
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.TestHelpers, as: StateTestHelpers
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.HubRegistryTestHelper

  @factory "spread_factory"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  defp namespace(), do: "spread_ns_#{System.unique_integer([:positive])}"

  defp seed(ns, runner_ids) do
    HubRegistryTestHelper.seed_runners(ns, runner_ids, @factory)
    on_exit(fn -> FleetStateHelper.forget_langs(ns, [@factory]) end)
    runner_ids
  end

  defp counts(runner_ids, assigned) do
    tally = Enum.frequencies(assigned)
    Map.new(runner_ids, &{&1, Map.get(tally, &1, 0)})
  end

  # The recovery batch's own placement call, with the child data shape
  # `StartChildrenRequest` hands the hook. Returns the runner the hook wrote
  # onto the row.
  defp place_in_batch(ns, operative_id) do
    child_data = %{
      child_spec: %{
        id: operative_id,
        start:
          {HubRegistryTestHelper.StubChild, :start_link,
           [{ns, %{id: operative_id, factory_id: @factory}}]}
      },
      metadata: %{}
    }

    HubManager.alter_child_data(:ignored_hub_id, child_data).metadata.tag
  end

  describe "the spread across a namespace's runners" do
    test "eight runners and eight operatives leave no runner idle" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..8, do: "spread_runner_#{i}"))

      assigned =
        for i <- 1..8, do: OperativeManager.assign_runner(ns, @factory, "spread_op_#{i}")

      assert counts(runners, assigned) == Map.new(runners, &{&1, 1})
    end

    test "five runners and twenty-five operatives land five each" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..5, do: "spread_runner_#{i}"))

      assigned =
        for i <- 1..25, do: OperativeManager.assign_runner(ns, @factory, "spread_op_#{i}")

      assert counts(runners, assigned) == Map.new(runners, &{&1, 5})
    end

    test "a recovery batch spreads before any of its rows is recorded" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..4, do: "batch_runner_#{i}"))

      assigned = for i <- 1..8, do: place_in_batch(ns, "batch_op_#{i}")

      assert counts(runners, assigned) == Map.new(runners, &{&1, 2})
    end
  end

  describe "select/3" do
    test "an empty load map is plain rendezvous hashing" do
      candidates = for i <- 1..8, do: "runner_#{i}"

      for i <- 1..50 do
        operative_id = "op_#{i}"

        assert Load.select(candidates, operative_id, %{}) ==
                 Enum.max_by(candidates, &:erlang.phash2({operative_id, &1}))
      end
    end

    test "an accurate load map spreads perfectly" do
      candidates = for i <- 1..8, do: "runner_#{i}"

      {assigned, _load} =
        Enum.map_reduce(1..24, Map.new(candidates, &{&1, 0}), fn i, load ->
          chosen = Load.select(candidates, "op_#{i}", load)
          {chosen, Map.update!(load, chosen, &(&1 + 1))}
        end)

      assert counts(candidates, assigned) == Map.new(candidates, &{&1, 3})
    end

    test "every candidate at cap still places, on the ranking head" do
      candidates = for i <- 1..4, do: "runner_#{i}"
      at_cap = Map.new(candidates, &{&1, 1_000})

      assert Load.select(candidates, "op", at_cap) ==
               Enum.max_by(candidates, &:erlang.phash2({"op", &1}))
    end

    test "a single candidate is selected whatever its count" do
      assert Load.select(["only"], "op", %{"only" => 9_999}) == "only"
    end

    test "no candidate is no selection" do
      assert Load.select([], "op", %{}) == nil
    end

    test "a percentage high enough to clear every count restores plain rendezvous" do
      candidates = for i <- 1..8, do: "runner_#{i}"
      on_exit(StateTestHelpers.override_env(:placement_load_cap_percent, 100_000))

      {assigned, _load} =
        Enum.map_reduce(1..8, Map.new(candidates, &{&1, 0}), fn i, load ->
          chosen = Load.select(candidates, "spread_op_#{i}", load)
          {chosen, Map.update!(load, chosen, &(&1 + 1))}
        end)

      plain =
        for i <- 1..8,
            do: Enum.max_by(candidates, &:erlang.phash2({"spread_op_#{i}", &1}))

      assert assigned == plain
    end
  end

  describe "node selection" do
    # `Heal.replacement_node/3` picks a cluster NODE through this same function.
    # A node has no operative count in the fleet's map, and two convergence
    # ticks that disagreed on the destination would fight over one operative.
    test "the same operative and node set answer the same node twice, whatever the runner counts" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..4, do: "node_runner_#{i}"))
      nodes = [:a@host, :b@host, :c@host]

      first = OperativeManager.pick_runner(nodes, "node_op")

      # Fill the fleet's load map, so a node choice that consulted a runner
      # count would have a different one to consult on the second read.
      for i <- 1..12, do: OperativeManager.assign_runner(ns, @factory, "node_fill_#{i}")
      refute FleetStateHelper.loads(ns) == Map.new(runners, &{&1, 0})

      assert OperativeManager.pick_runner(nodes, "node_op") == first
      assert first == Enum.max_by(nodes, &:erlang.phash2({"node_op", &1}))
    end
  end

  describe "the load map" do
    test "a handout is counted before the next placement reads it" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..4, do: "count_runner_#{i}"))

      first = OperativeManager.assign_runner(ns, @factory, "count_op_1")
      assert FleetStateHelper.loads(ns)[first] == 1

      second = OperativeManager.assign_runner(ns, @factory, "count_op_2")
      refute second == first
      assert FleetStateHelper.loads(ns) == counts(runners, [first, second])
    end

    # `Lifecycle.bind/3` awaits a runner and `complete_init` re-resolves before
    # its one attempt, so one operative asks twice. Two answers would send it to
    # a runner its own planner did not choose, and charge both.
    test "resolving one operative twice answers the same runner and counts once" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..4, do: "twice_runner_#{i}"))

      first = OperativeManager.assign_runner(ns, @factory, "twice_op")

      assert OperativeManager.assign_runner(ns, @factory, "twice_op") == first
      assert FleetStateHelper.loads(ns) == counts(runners, [first])
    end

    test "a rebuild corrects a count inflated by a placement that never started" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..4, do: "rebuild_runner_#{i}"))

      for i <- 1..4, do: OperativeManager.assign_runner(ns, @factory, "rebuild_op_#{i}")
      assert FleetStateHelper.loads(ns) == Map.new(runners, &{&1, 1})

      # Nothing started, so the registry records none of them and only a rebuild
      # can say so. Ageing the declared interval to nothing is what makes the
      # next placement rebuild rather than read the four phantoms.
      on_exit(StateTestHelpers.override_env(:placement_load_refresh_ms, 0))
      last = OperativeManager.assign_runner(ns, @factory, "rebuild_op_5")

      assert FleetStateHelper.loads(ns) == counts(runners, [last])
    end

    test "a count is read from the registry's runner tags" do
      ns = namespace()
      runners = seed(ns, for(i <- 1..3, do: "tagged_runner_#{i}"))
      [first, second, _third] = runners

      dump = %{
        "op_a" => {%{}, [{node(), self()}], %{tag: first}},
        "op_b" => {%{}, [{node(), self()}], %{tag: first}},
        "op_c" => {%{}, [{node(), self()}], %{tag: second}},
        "op_unbound" => {%{}, [{node(), self()}], %{}}
      }

      assert Load.counts(dump, runners) == counts(runners, [first, first, second])
    end
  end
end
