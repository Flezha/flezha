defmodule Flezha.Cluster.Reads do
  @moduledoc """
  Ask the cluster: one read answered by several controllers at once, under one
  bound, naming every controller that did not answer.

  This node answers in place and every peer over `:erpc`, all at the same time,
  so a peer that does not answer costs the read one bound, not one per peer,
  and never fails it. The function asked is a public `@doc false` local half,
  called by name on each node: closures are never shipped over distribution.

  `controllers/0` is every controller the cluster directory lists, so a
  connected node that is not a controller (a console) is never asked.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Parallel

  @type answer :: %{answers: [{node(), term()}], unreachable: [node()]}
  @type rows :: %{rows: [term()], unreachable: [node()]}

  @doc "Every controller the directory lists, this node first."
  @spec controllers() :: [node()]
  def controllers(), do: [node() | Enum.sort(List.delete(Directory.controller_nodes(), node()))]

  @doc """
  Each node's answer to `apply(module, fun, args)`, in node order. A node that
  raises, exits or overruns the bound (`:timeout`, default `peer_read_ms`) has
  no answer and is listed in `unreachable`.
  """
  @spec ask([node()], module(), atom(), [term()], keyword()) :: answer()
  def ask(nodes, module, fun, args, opts \\ []) do
    timeout = Keyword.get_lazy(opts, :timeout, fn -> Durations.get(:peer_read_ms) end)

    nodes
    |> Enum.uniq()
    |> Parallel.results(&call(&1, module, fun, args, timeout), timeout: timeout)
    |> Enum.reduce(%{answers: [], unreachable: []}, fn
      {node, {:ok, value}}, acc -> %{acc | answers: [{node, value} | acc.answers]}
      {node, {:exit, _reason}}, acc -> %{acc | unreachable: [node | acc.unreachable]}
    end)
    |> Map.new(fn {key, list} -> {key, Enum.reverse(list)} end)
  end

  @doc """
  `ask/5` for a function answering a list: the lists joined in node order. A
  map row is given the `node` that answered it unless it already names one;
  any other row (a tuple, a bare id) is joined as it is.
  """
  @spec rows([node()], module(), atom(), [term()], keyword()) :: rows()
  def rows(nodes, module, fun, args, opts \\ []) do
    %{answers: answers, unreachable: unreachable} = ask(nodes, module, fun, args, opts)

    rows =
      for {node, list} <- answers, is_list(list), row <- list do
        if is_map(row), do: Map.put_new(row, :node, node), else: row
      end

    %{rows: rows, unreachable: unreachable}
  end

  @doc """
  Ask every node and keep the first, in node order, whose answer is neither
  `nil` nor `false`: the one that has what was looked for. `{:ok, node,
  answer}`, or `{:error, unreachable}` when no node that answered has it.
  """
  @spec find([node()], module(), atom(), [term()], keyword()) ::
          {:ok, node(), term()} | {:error, [node()]}
  def find(nodes, module, fun, args, opts \\ []) do
    %{answers: answers, unreachable: unreachable} = ask(nodes, module, fun, args, opts)

    case Enum.find(answers, fn {_node, answer} -> answer not in [nil, false] end) do
      {node, answer} -> {:ok, node, answer}
      nil -> {:error, unreachable}
    end
  end

  defp call(node, module, fun, args, _timeout) when node == node(), do: apply(module, fun, args)
  defp call(node, module, fun, args, timeout), do: :erpc.call(node, module, fun, args, timeout)
end
