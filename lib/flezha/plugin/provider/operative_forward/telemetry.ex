defmodule Flezha.Plugin.Provider.OperativeForward.Telemetry do
  @moduledoc """
  Wrappers around `:telemetry.execute/3` for the operative-forward plugin,
  following the `[:flezha, :plugin, <provider>, _]` shape the Kafka producer
  established.

  Every event carries the source operative, the target operative, and the plugin
  id, so a saturating cycle between two operatives is visible to an operator
  without parsing log lines.
  """

  @prefix [:flezha, :plugin, :operative_forward]

  @doc "One payload reached the target. `outcome` is the verdict's terminal tag."
  @spec delivered(atom(), map()) :: :ok
  def delivered(outcome, labels) do
    :telemetry.execute(@prefix ++ [:delivered], %{count: 1}, Map.put(labels, :outcome, outcome))
  end

  @doc "A delivery attempt failed and the payload is retained for retry."
  @spec retry(pos_integer(), non_neg_integer(), term(), map()) :: :ok
  def retry(attempt, backoff_ms, reason, labels) do
    :telemetry.execute(
      @prefix ++ [:retry],
      %{attempt: attempt, backoff_ms: backoff_ms},
      Map.put(labels, :reason, reason)
    )
  end

  @doc "Current queue depth, emitted on every enqueue."
  @spec queue_depth(non_neg_integer(), map()) :: :ok
  def queue_depth(depth, labels) do
    :telemetry.execute(@prefix ++ [:queue_depth], %{depth: depth}, labels)
  end

  @doc "The queue reached `:queue_max` and the enqueueing caller is parked."
  @spec saturated(non_neg_integer(), map()) :: :ok
  def saturated(depth, labels) do
    :telemetry.execute(@prefix ++ [:saturated], %{depth: depth}, labels)
  end
end
