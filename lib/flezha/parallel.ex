defmodule Flezha.Parallel do
  @moduledoc """
  Work over independent items, run concurrently: a partition group's members, a
  plugin fanned over them, a bench's pipelines, an operative's Entities.

  `map/2` keeps every element in flight and returns results in input order.
  `each/3` is the fire-and-collect-nothing shape, bounded — for work whose
  result is a side effect and whose fan-out is unbounded in size.
  """

  @spec map(Enumerable.t(), (term() -> term())) :: [term()]
  def map(enum, fun) do
    enum
    |> Task.async_stream(fun,
      ordered: true,
      timeout: :infinity,
      max_concurrency: max(Enum.count(enum), 1)
    )
    |> Enum.map(fn {:ok, value} -> value end)
  end

  @doc """
  Run `fun` over every item for its side effect, at most `max_concurrency` at a
  time, in whatever order they finish. A caller with a large or unknown fan-out
  wants this rather than `map/2`: each item holds a process (and, for a control
  round trip, a pending reply) until it resolves.
  """
  @spec each(Enumerable.t(), (term() -> term()), keyword()) :: :ok
  def each(enum, fun, opts \\ []) do
    enum
    |> Task.async_stream(fun,
      ordered: false,
      timeout: :infinity,
      max_concurrency: Keyword.get(opts, :max_concurrency, 16)
    )
    |> Stream.run()
  end
end
