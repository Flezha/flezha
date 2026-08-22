defmodule Flezha.Parallel do
  @moduledoc """
  `Enum.map/2` with every element in flight at once, results in input order —
  the shape of work over a group whose members are independent: a partition
  group's members, a plugin fanned over them, a bench's pipelines.
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
end
