defmodule Flezha.Cluster.Reads do
  @moduledoc """
  Ask the cluster: one read answered by several controllers at once, under one
  bound, naming every controller that did not answer.

  This node answers in place and every peer over `:erpc`, all at the same time,
  so a peer that does not answer costs the read one bound, not one per peer,
  and never fails it. The function asked is a public `@doc false` local half,
  called by name on each node: closures are never shipped over distribution.

  `controllers/0` is every controller the cluster directory lists, so a
  connected node that is not a controller (the `flz` CLI) is never asked.
  `newest/3` and `grouped/3` merge the rows of a read, and `on/5` acts on the
  one controller that holds what a call names.
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
  node answering one map, a controller's own facts, gives that one row. A map
  row is given the `node` that answered it unless it already names one; any
  other row (a tuple, a bare id) is joined as it is.
  """
  @spec rows([node()], module(), atom(), [term()], keyword()) :: rows()
  def rows(nodes, module, fun, args, opts \\ []) do
    %{answers: answers, unreachable: unreachable} = ask(nodes, module, fun, args, opts)

    rows =
      for {node, answer} <- answers,
          is_list(answer) or is_map(answer),
          row <- List.wrap(answer) do
        if is_map(row), do: Map.put_new(row, :node, node), else: row
      end

    %{rows: rows, unreachable: unreachable}
  end

  @doc """
  Ask every node and keep the first, in node order, that has what was looked
  for: whose answer is none of `nil`, `false`, `:error` and `{:error, _}`, so a
  plain lookup is asked as it is. `{:ok, node, answer}`, or `{:error,
  unreachable}` when no node that answered has it.
  """
  @spec find([node()], module(), atom(), [term()], keyword()) ::
          {:ok, node(), term()} | {:error, [node()]}
  def find(nodes, module, fun, args, opts \\ []) do
    %{answers: answers, unreachable: unreachable} = ask(nodes, module, fun, args, opts)

    case Enum.find(answers, fn {_node, answer} -> held?(answer) end) do
      {node, answer} -> {:ok, node, answer}
      nil -> {:error, unreachable}
    end
  end

  @doc """
  Rows merged across controllers in the order of `by` (a function of a row),
  oldest first, keeping the newest `limit` of them; `nil` keeps every row. Each
  node trims to `limit` before answering, so a merge moves at most `limit` rows
  per controller.
  """
  @spec newest(rows(), (term() -> term()), non_neg_integer() | nil) :: rows()
  def newest(%{rows: rows} = answer, by, limit) do
    sorted = Enum.sort_by(rows, by)
    %{answer | rows: if(limit, do: Enum.take(sorted, -limit), else: sorted)}
  end

  @doc """
  Rows joined into one per `key` (a function of a row), sorted by it: `merge`
  makes one row of the rows the controllers answered for that key.
  """
  @spec grouped(rows(), (term() -> term()), ([term()] -> term())) :: rows()
  def grouped(%{rows: rows} = answer, key, merge) do
    merged = rows |> Enum.group_by(key) |> Enum.map(fn {_key, group} -> merge.(group) end)
    %{answer | rows: Enum.sort_by(merged, key)}
  end

  @doc """
  `apply(module, fun, args)` on the one `node` that holds what it acts on: in
  place on this node, else over `:erpc` under `route_call_timeout_ms`, which
  exceeds a routed relay start's own budget (ProcessHub's wait plus its retry
  ladder). A call that raises, exits or overruns
  answers `on_error.({:routed_call_failed, node, kind, reason})`; `:on_error`
  defaults to `&{:error, &1}`.
  """
  @spec on(node(), module(), atom(), [term()], keyword()) :: term()
  def on(node, module, fun, args, opts \\ []) do
    call(node, module, fun, args, Durations.get(:route_call_timeout_ms))
  catch
    kind, reason ->
      Keyword.get(opts, :on_error, &{:error, &1}).({:routed_call_failed, node, kind, reason})
  end

  defp held?(answer), do: answer not in [nil, false, :error] and not match?({:error, _}, answer)

  defp call(node, module, fun, args, _timeout) when node == node(), do: apply(module, fun, args)
  defp call(node, module, fun, args, timeout), do: :erpc.call(node, module, fun, args, timeout)
end
