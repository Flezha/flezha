defmodule Flezha.Plugin.Provider.Kafka.Telemetry do
  @moduledoc """
  Wrappers around `:telemetry.execute/3` for the hardened Kafka producer
  plugin. Every produce attempt, retry, drop, failure, success, and
  metadata refresh emits exactly one event.

  Events follow `[:flezha, :plugin, :kafka, _, _]` to match the existing
  `[:flezha, :plugin, _, _]` namespace in this codebase.

  All events fire-and-forget — `:telemetry.execute` runs handlers
  synchronously in the caller's process, so handlers should themselves not
  block. The producer worker spawns a fresh process for non-trivial work,
  but per-event handlers are still expected to be cheap.
  """

  @prefix [:flezha, :plugin, :kafka]

  @spec produce_ok(non_neg_integer(), non_neg_integer(), map()) :: :ok
  def produce_ok(batch_size, latency_ms, metadata) do
    :telemetry.execute(
      @prefix ++ [:produce, :ok],
      %{batch_size: batch_size, latency_ms: latency_ms},
      metadata
    )
  end

  @spec retry(pos_integer(), non_neg_integer(), map()) :: :ok
  def retry(attempt, backoff_ms, metadata) do
    :telemetry.execute(
      @prefix ++ [:produce, :retry],
      %{attempt: attempt, backoff_ms: backoff_ms},
      metadata
    )
  end

  @spec dropped(pos_integer(), map()) :: :ok
  def dropped(count, metadata) do
    :telemetry.execute(
      @prefix ++ [:produce, :dropped],
      %{count: count},
      metadata
    )
  end

  @spec failed(pos_integer(), non_neg_integer(), map()) :: :ok
  def failed(attempts, total_ms, metadata) do
    :telemetry.execute(
      @prefix ++ [:produce, :failed],
      %{attempts: attempts, total_ms: total_ms},
      metadata
    )
  end

  @spec metadata_refresh(non_neg_integer(), map()) :: :ok
  def metadata_refresh(latency_ms, metadata) do
    :telemetry.execute(
      @prefix ++ [:metadata, :refresh],
      %{latency_ms: latency_ms},
      metadata
    )
  end

  @spec topic_created(integer(), integer(), keyword() | map()) :: :ok
  def topic_created(partitions, replication_factor, metadata) do
    :telemetry.execute(
      @prefix ++ [:topic, :created],
      %{partitions: partitions, replication_factor: replication_factor},
      Map.new(metadata)
    )
  end

  @spec worker_swapped(map(), map()) :: :ok
  def worker_swapped(measurements, metadata) do
    :telemetry.execute(
      @prefix ++ [:worker, :swapped],
      measurements,
      metadata
    )
  end
end
