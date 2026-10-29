defmodule Flezha.Cluster.ReadsTest do
  @moduledoc """
  The cluster read core on this node alone: a local answer, a raising local
  half, the `node` a row is given, and the lookup that keeps the node that has
  what was asked for. The cross-node bound is pinned by
  `Flezha.Cluster.ReadsMultiNodeTest`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Cluster.Reads

  @doc false
  def answer(value), do: value

  @doc false
  def raise_here, do: raise("the local half failed")

  test "this node is the first controller" do
    assert hd(Reads.controllers()) == node()
  end

  test "this node answers in place" do
    assert Reads.ask([node()], __MODULE__, :answer, [:here]) ==
             %{answers: [{node(), :here}], unreachable: []}
  end

  test "a local half that raises is unreachable, not a failed read" do
    assert Reads.ask([node()], __MODULE__, :raise_here, []) ==
             %{answers: [], unreachable: [node()]}
  end

  test "a node asked twice answers once" do
    assert %{answers: [_one]} = Reads.ask([node(), node()], __MODULE__, :answer, [1])
  end

  test "a map row is given the node that answered it unless it names one" do
    rows = [%{id: 1}, %{id: 2, node: :elsewhere}, {:tuple, 3}, "bare"]

    assert Reads.rows([node()], __MODULE__, :answer, [rows]) == %{
             rows: [%{id: 1, node: node()}, %{id: 2, node: :elsewhere}, {:tuple, 3}, "bare"],
             unreachable: []
           }
  end

  test "find keeps the node that has it, or names the nodes that did not answer" do
    assert Reads.find([node()], __MODULE__, :answer, [:found]) == {:ok, node(), :found}
    assert Reads.find([node()], __MODULE__, :answer, [nil]) == {:error, []}
    assert Reads.find([node()], __MODULE__, :raise_here, []) == {:error, [node()]}
  end

  test "find reads a plain lookup's :error and {:error, _} as not held" do
    for absent <- [false, :error, {:error, :not_found}] do
      assert Reads.find([node()], __MODULE__, :answer, [absent]) == {:error, []}
    end

    assert Reads.find([node()], __MODULE__, :answer, [{:ok, 1}]) == {:ok, node(), {:ok, 1}}
  end

  test "a node answering one map gives one row" do
    assert Reads.rows([node()], __MODULE__, :answer, [%{up: true}]) ==
             %{rows: [%{up: true, node: node()}], unreachable: []}
  end

  test "newest merges oldest first and keeps the newest limit" do
    answer = %{rows: [%{at: 3}, %{at: 1}, %{at: 2}], unreachable: [:gone]}

    assert Reads.newest(answer, & &1.at, 2) == %{rows: [%{at: 2}, %{at: 3}], unreachable: [:gone]}
    assert Reads.newest(answer, & &1.at, nil).rows == [%{at: 1}, %{at: 2}, %{at: 3}]
  end

  test "grouped joins the rows of one key into one, sorted by key" do
    answer = %{rows: [%{id: :b, n: 1}, %{id: :a, n: 2}, %{id: :b, n: 3}], unreachable: []}

    merged =
      Reads.grouped(
        answer,
        & &1.id,
        &%{id: hd(&1).id, n: Enum.sum(Enum.map(&1, fn r -> r.n end))}
      )

    assert merged.rows == [%{id: :a, n: 2}, %{id: :b, n: 4}]
  end

  test "on runs in place here, and a peer that cannot be reached goes through on_error" do
    assert Reads.on(node(), __MODULE__, :answer, [:here]) == :here

    assert {:error, {:routed_call_failed, :"nowhere@127.0.0.1", :error, _}} =
             Reads.on(:"nowhere@127.0.0.1", __MODULE__, :answer, [:here])

    assert Reads.on(:"nowhere@127.0.0.1", __MODULE__, :answer, [:here],
             on_error: fn _ -> :mapped end
           ) ==
             :mapped
  end
end
