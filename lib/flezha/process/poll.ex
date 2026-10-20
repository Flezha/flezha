defmodule Flezha.Process.Poll do
  @moduledoc """
  The one wait-until loop: ask a condition again, a pause apart, until it holds
  or a deadline passes.

  The condition answers `{:ok, value}` once it holds, or `{:error, reason}` to be
  asked again. At the deadline the last refusal is the answer, so a condition
  with nothing to say answers `{:error, :timeout}`, and one that can tell why it
  has not held yet says so. It is asked at once, then after each pause, and a
  pause never runs past the deadline: the last ask is at it.
  """

  @typedoc "A `System.monotonic_time(:millisecond)` instant."
  @type deadline() :: integer()

  @doc """
  Ask `fun` every `interval_ms` until it answers `{:ok, value}` or `deadline_ms`
  passes. `pause` waits between asks (`Process.sleep/1` unless a caller must
  stay answerable to its own messages meanwhile).
  """
  @spec until(
          deadline(),
          pos_integer(),
          (-> {:ok, value} | {:error, reason}),
          (non_neg_integer() -> term())
        ) :: {:ok, value} | {:error, reason}
        when value: term(), reason: term()
  def until(deadline_ms, interval_ms, fun, pause \\ &Process.sleep/1) do
    case fun.() do
      {:ok, _value} = held ->
        held

      {:error, _reason} = refused ->
        case deadline_ms - System.monotonic_time(:millisecond) do
          left when left > 0 ->
            pause.(min(left, interval_ms))
            until(deadline_ms, interval_ms, fun, pause)

          _passed ->
            refused
        end
    end
  end
end
