defmodule Flezha.Reconciler.Activity do
  @moduledoc """
  Per-relay egress-activity tracker for the reconciler.

  Owns the `:flezha_relay_activity` ETS table, mapping
  `relay_id -> last_output_monotonic_ms`. It is written once per outbound frame
  from `Flezha.Executor.StreamChannel` (a single O(1) named insert, gated on
  `:reconciler_enabled` and off the GenServer path) and read by
  `Flezha.Reconciler.Probe` during reconcile.

  This is the relay -> executor -> Kafka **egress** signal, distinct from
  `Flezha.State.Watchdog`'s per-Partition **persistence** output-staleness (which
  has its own `[:flezha, :state, :output_stall]` signal).

  A second, **cold-path** entry tracks when an outbound frame was *dropped*
  because the data channel was unbound (`record_drop/1`, keyed `{:drop, relay_id}`).
  A dropped frame is positive evidence that ingest is live yet output is broken,
  so the probe (Phase 6.2) flags a throughput stall only when a recent drop
  coincides with a stale last-output — "data in, nothing out" — never flagging a
  legitimately idle relay. The drop also emits `[:flezha, :stream, :drop]` so the
  previously-silent seam is observable.
  """

  use GenServer

  @table :flezha_relay_activity

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc """
  Record an outbound frame for `relay_id`. Hot-path: a single named ETS insert.

  The caller gates on `:reconciler_enabled` (so the table exists), but the write
  is wrapped against the narrow window where the owner is mid-restart so a frame
  send can never crash the StreamChannel.
  """
  @spec record_output(binary()) :: :ok
  def record_output(relay_id) do
    :ets.insert(@table, {relay_id, System.monotonic_time(:millisecond)})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Record that an outbound frame for `relay_id` was dropped because the data
  channel was unbound. Always emits `[:flezha, :stream, :drop]` (so the seam is
  observable regardless of `:reconciler_enabled`); the timestamp is recorded only
  when the table exists (the reconciler's ingest-live evidence for Phase 6.2).
  Cold path — only reached while a relay's stream is unbound, never on the
  healthy hot path.
  """
  @spec record_drop(binary()) :: :ok
  def record_drop(relay_id) do
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{relay_id: relay_id})
    insert(drop_key(relay_id), System.monotonic_time(:millisecond))
  end

  @doc """
  Monotonic-ms timestamp of the last recorded output for `relay_id`, or `nil`
  when none has been recorded (or the table is absent because the reconciler is
  disabled). Safe to call from any process.
  """
  @spec last_output_ms(binary()) :: integer() | nil
  def last_output_ms(relay_id), do: lookup(relay_id)

  @doc """
  Monotonic-ms timestamp of the last unbound-stream drop for `relay_id`, or
  `nil` when none has been recorded. Safe to call from any process.
  """
  @spec last_drop_ms(binary()) :: integer() | nil
  def last_drop_ms(relay_id), do: lookup(drop_key(relay_id))

  @doc "Drop the activity entries (output + drop) for a relay that is gone."
  @spec forget(binary()) :: :ok
  def forget(relay_id) do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      tid ->
        :ets.delete(tid, relay_id)
        :ets.delete(tid, drop_key(relay_id))
        :ok
    end
  end

  defp drop_key(relay_id), do: {:drop, relay_id}

  defp lookup(key) do
    case :ets.whereis(@table) do
      :undefined ->
        nil

      tid ->
        case :ets.lookup(tid, key) do
          [{_, ts}] -> ts
          [] -> nil
        end
    end
  end

  defp insert(key, ts) do
    :ets.insert(@table, {key, ts})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    {:ok, %{}}
  end
end
