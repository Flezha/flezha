defmodule Flezha.Process.ParallelAndCspecTest do
  @moduledoc """
  The two seams a caller declares INTO rather than around: a fan-out that can
  take a bound, and a child spec built from a restart kind.

  `Flezha.Parallel` had no test at all while three callers used it and ten
  more built their own pipeline beside it — because it hard-coded
  `timeout: :infinity` and a caller needing a bound had nowhere to put one.
  """

  use ExUnit.Case, async: true

  alias Flezha.Parallel
  alias Flezha.Process.Cspec
  alias FlezhaTest.Helper.NodeTestHelper

  # ---------- Parallel ----------

  test "map/3 with no bound still runs every item to completion" do
    assert Parallel.map([1, 2, 3], &(&1 * 2)) == [2, 4, 6]
  end

  test "map/3 keeps input order however the items finish" do
    # Each item finishes only after the one behind it, so they finish in reverse.
    finished = :atomics.new(4, [])

    assert Parallel.map([1, 2, 3], fn n ->
             assert NodeTestHelper.wait_until(
                      fn -> n == 3 or :atomics.get(finished, n + 1) == 1 end,
                      1_000,
                      5
                    )

             :atomics.put(finished, n, 1)
             n
           end) == [1, 2, 3]
  end

  test "each/3 runs every item for its side effect" do
    parent = self()
    assert Parallel.each(1..5, &send(parent, {:did, &1})) == :ok
    for n <- 1..5, do: assert_received({:did, ^n})
  end

  test "results/3 pairs every item with its own outcome" do
    outcomes = Parallel.results([1, 2], &(&1 * 10), timeout: 1_000)
    assert outcomes == [{1, {:ok, 10}}, {2, {:ok, 20}}]
  end

  test "one wedged item costs one bound, and the rest still answer" do
    # fails when: a caller has to build its own `Task.async_stream` because the
    # seam cannot take a bound — which is why probe and the hub drain each had
    # a pipeline of their own.
    {elapsed_us, outcomes} =
      :timer.tc(fn ->
        Parallel.results(
          [:fast, :wedged, :fast],
          fn
            :wedged -> receive(do: (:never -> :never))
            :fast -> :done
          end,
          timeout: 200
        )
      end)

    assert [{:fast, {:ok, :done}}, {:wedged, {:exit, :timeout}}, {:fast, {:ok, :done}}] = outcomes
    assert elapsed_us < 2_000_000, "the wedged item was waited for more than once"
  end

  test "map_nodes/3 answers per node and survives an unreachable one" do
    outcomes =
      Parallel.map_nodes([node(), :nobody@nowhere], &:erpc.call(&1, Kernel, :node, []),
        timeout: 2_000
      )

    assert {node(), {:ok, node()}} in outcomes
    assert {:nobody@nowhere, {:exit, _reason}} = List.keyfind(outcomes, :nobody@nowhere, 0)
  end

  test "a duplicate node is asked once" do
    assert length(Parallel.map_nodes([node(), node()], & &1, timeout: 1_000)) == 1
  end

  # ---------- Cspec ----------

  test "build/4 turns a kind into the six keys a supervisor reads" do
    spec = Cspec.build(:w, {M, :start_link, [1]}, :worker_transient)

    assert spec == %{
             id: :w,
             start: {M, :start_link, [1]},
             restart: :transient,
             type: :worker,
             shutdown: 5_000
           }
  end

  test "a supervisor kind holds the tree open, a one-shot does not" do
    assert Cspec.policy(:supervisor_permanent).shutdown == :infinity
    assert Cspec.policy(:one_shot_temporary).shutdown == :brutal_kill
  end

  test "opts override only the row keys a child may vary" do
    spec = Cspec.build(:w, {M, :start_link, []}, :worker_permanent, shutdown: 500, id: :ignored)
    assert spec.shutdown == 500
    assert spec.id == :w
  end

  test "a kind no row names is refused at the call" do
    assert_raise FunctionClauseError, fn ->
      Cspec.build(:w, {M, :start_link, []}, :worker_whenever)
    end
  end

  test "start_arg/1 reads a persisted spec's single argument and nothing else" do
    assert Cspec.start_arg(%{start: {M, :start_link, [{"ns", %{id: "op"}}]}}) ==
             {:ok, {"ns", %{id: "op"}}}

    assert Cspec.start_arg(%{start: {M, :start_link, [1, 2]}}) == :error
    assert Cspec.start_arg(%{no: :start}) == :error
    assert Cspec.start_arg(start: {M, :f, [:a]}) == {:ok, :a}
  end
end
