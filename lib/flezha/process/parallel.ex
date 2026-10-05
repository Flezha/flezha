defmodule Flezha.Parallel do
  @moduledoc """
  Work over independent items, run concurrently: a partition group's members, a
  plugin fanned over them, a bench's pipelines, a relay's Cells.

  `map/3` keeps every element in flight and returns results in input order.
  `each/3` is the fire-and-collect-nothing shape, bounded — for work whose
  result is a side effect and whose fan-out is unbounded in size.
  `results/3` is the shape a caller that must survive a wedged item needs:
  each item paired with its own outcome, `{:ok, value}` or `{:exit, reason}`,
  so one item that never answers costs one bound rather than the whole fan-out.

  A bound is per call. `map/3` and `each/3` still run to `:infinity` when no
  `:timeout` is given — a caller that already sized its fan-out and needs
  every answer is the reason this module exists — and `results/3` requires
  one, because pairing an item with `{:exit, :timeout}` is the only thing it
  is for.
  """

  @typedoc "One item's outcome: its value, or the reason its task went away."
  @type outcome :: {:ok, term()} | {:exit, term()}

  @spec map(Enumerable.t(), (term() -> term()), keyword()) :: [term()]
  def map(enum, fun, opts \\ []) do
    enum
    |> stream(fun, opts, ordered: true, max_concurrency: bound(enum, opts))
    |> Enum.map(fn
      {:ok, value} -> value
      # `map/3` answers one value per item, so an item that has none is the
      # caller's exit — the same failure an unbounded crashing task already is.
      {:exit, reason} -> exit(reason)
    end)
  end

  @doc """
  Run `fun` over every item for its side effect, at most `max_concurrency` at a
  time, in whatever order they finish. A caller with a large or unknown fan-out
  wants this rather than `map/3`: each item holds a process (and, for a control
  round trip, a pending reply) until it resolves.
  """
  @spec each(Enumerable.t(), (term() -> term()), keyword()) :: :ok
  def each(enum, fun, opts \\ []) do
    enum
    |> stream(fun, opts, ordered: false, max_concurrency: Keyword.get(opts, :max_concurrency, 16))
    |> Stream.run()
  end

  @doc """
  Every item paired with its own outcome, in input order, under a per-item
  `:timeout` (required). An item whose work exceeds the bound, raises, throws
  or exits is `{:exit, reason}`; the rest still answer.

  The guard is the point: `Task.async_stream` links its tasks, so one item
  that raises takes the caller with it — which is why every caller that had to
  survive a wedged or unreachable item wrote its own `try` inside its own
  pipeline. Here it is written once.
  """
  @spec results(Enumerable.t(), (term() -> term()), keyword()) :: [{term(), outcome()}]
  def results(enum, fun, opts) do
    items = Enum.to_list(enum)

    items
    |> stream(guarded(fun), Keyword.put_new(opts, :on_timeout, :kill_task),
      ordered: true,
      max_concurrency: bound(items, opts),
      timeout: Keyword.fetch!(opts, :timeout)
    )
    |> Enum.zip(items)
    |> Enum.map(fn
      # `guarded/1` already tagged the item's own outcome; a bare `{:exit, …}`
      # from the stream is the bound expiring or the task being killed.
      {{:ok, outcome}, item} -> {item, outcome}
      {{:exit, reason}, item} -> {item, {:exit, reason}}
    end)
  end

  defp guarded(fun) do
    fn item ->
      try do
        {:ok, fun.(item)}
      catch
        :exit, reason -> {:exit, reason}
        kind, reason -> {:exit, {kind, reason}}
      end
    end
  end

  @doc """
  Run `fun` on every node concurrently and pair each with its outcome. An
  unreachable peer is one `{:exit, reason}` rather than a failed read for
  everybody, and the slowest node costs one `:timeout`, not one per node.
  """
  @spec map_nodes([node()], (node() -> term()), keyword()) :: [{node(), outcome()}]
  def map_nodes(nodes, fun, opts) do
    nodes |> Enum.uniq() |> results(fun, opts)
  end

  # `map/3` with no explicit bound runs every item at once: its callers fan out
  # over a set they already sized. Passing one opts into the same per-item
  # budget `each/3` defaults to.
  defp bound(enum, opts) do
    Keyword.get_lazy(opts, :max_concurrency, fn -> max(Enum.count(enum), 1) end)
  end

  # A caller's own `:timeout` and `:on_timeout` reach `Task.async_stream`
  # unchanged. `fixed` is what each entry point decides for itself (order, and
  # the concurrency it derived); the unbounded `:timeout` default is what keeps
  # a caller that declares neither running exactly as it did.
  defp stream(enum, fun, opts, fixed) do
    passed = Keyword.take(opts, [:timeout, :on_timeout])
    Task.async_stream(enum, fun, Keyword.merge([timeout: :infinity], passed ++ fixed))
  end
end
