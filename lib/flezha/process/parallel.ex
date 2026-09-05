defmodule Flezha.Parallel do
  @moduledoc """
  Work over independent items, run concurrently: a partition group's members, a
  plugin fanned over them, a bench's pipelines, an operative's Entities.

  `map/2` keeps every element in flight and returns results in input order.
  `each/3` is the fire-and-collect-nothing shape, bounded — for work whose
  result is a side effect and whose fan-out is unbounded in size.
  """

  @spec map(Enumerable.t(), (term() -> term()), keyword()) :: [term()]
  def map(enum, fun, opts \\ []) do
    enum
    |> stream(fun, ordered: true, max_concurrency: bound(enum, opts))
    |> Enum.map(fn {:ok, value} -> value end)
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
    |> stream(fun, ordered: false, max_concurrency: Keyword.get(opts, :max_concurrency, 16))
    |> Stream.run()
  end

  # `map/3` with no explicit bound runs every item at once: its callers fan out
  # over a set they already sized. Passing one opts into the same per-item
  # budget `each/3` defaults to.
  defp bound(enum, opts) do
    Keyword.get_lazy(opts, :max_concurrency, fn -> max(Enum.count(enum), 1) end)
  end

  defp stream(enum, fun, opts) do
    Task.async_stream(enum, fun, Keyword.put(opts, :timeout, :infinity))
  end
end
