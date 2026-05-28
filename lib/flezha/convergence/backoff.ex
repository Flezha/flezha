defmodule Flezha.Convergence.Backoff do
  @moduledoc """
  Pure per-`{primitive, target}` exponential cooldown table shared by
  every heal primitive. Schedule: 5 s → 30 s → 60 s → 300 s, then held
  at 300 s. A successful heal resets the entry.

  No process, no side effects: the `Loop` owns one `%Backoff{}` in its
  state, calls `filter/3` to drop planned ops still in cooldown, and
  `update/4` to record the outcome of the ops it executed.
  """

  @schedule_ms [5_000, 30_000, 60_000, 300_000]

  @type key :: {atom(), term()}
  @type t :: %__MODULE__{entries: %{key() => {non_neg_integer(), integer()}}}

  defstruct entries: %{}

  @spec new() :: t()
  def new(), do: %__MODULE__{}

  @spec schedule_ms() :: [pos_integer()]
  def schedule_ms(), do: @schedule_ms

  @doc """
  Keep only the ops whose `{primitive, target}` key is past its cooldown
  deadline. `key_fn` extracts the backoff key from an op. Ops come first
  so this composes in a pipe.
  """
  @spec filter([op], t(), (op -> key())) :: [op] when op: term()
  def filter(ops, %__MODULE__{} = b, key_fn, now \\ monotonic_ms()) when is_function(key_fn, 1) do
    Enum.filter(ops, fn op -> eligible?(b, key_fn.(op), now) end)
  end

  @spec eligible?(t(), key(), integer()) :: boolean()
  def eligible?(%__MODULE__{entries: entries}, key, now \\ monotonic_ms()) do
    case Map.get(entries, key) do
      nil -> true
      {_attempt, next_eligible} -> now >= next_eligible
    end
  end

  @doc """
  Record the result of executing the op keyed by `key`. `:ok` resets the
  entry; `:error` advances the attempt count and arms the next cooldown.
  """
  @spec update(t(), key(), :ok | :error, integer()) :: t()
  def update(backoff, key, result, now \\ monotonic_ms())

  def update(%__MODULE__{entries: entries} = b, key, :ok, _now) do
    %{b | entries: Map.delete(entries, key)}
  end

  def update(%__MODULE__{entries: entries} = b, key, :error, now) do
    {attempt, _next} = Map.get(entries, key, {0, 0})
    delay = Enum.at(@schedule_ms, attempt) || List.last(@schedule_ms)
    %{b | entries: Map.put(entries, key, {attempt + 1, now + delay})}
  end

  defp monotonic_ms(), do: System.monotonic_time(:millisecond)
end
