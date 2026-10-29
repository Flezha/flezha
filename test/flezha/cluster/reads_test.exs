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
end
