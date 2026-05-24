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
  has its own `[:flezha, :state, :output_stall]` signal). Phase 5 records and reads
  the timestamp; the ingest-gated throughput-stall escalation that consumes it is
  Phase 6.
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
  Monotonic-ms timestamp of the last recorded output for `relay_id`, or `nil`
  when none has been recorded (or the table is absent because the reconciler is
  disabled). Safe to call from any process.
  """
  @spec last_output_ms(binary()) :: integer() | nil
  def last_output_ms(relay_id) do
    case :ets.whereis(@table) do
      :undefined ->
        nil

      tid ->
        case :ets.lookup(tid, relay_id) do
          [{_, ts}] -> ts
          [] -> nil
        end
    end
  end

  @doc "Drop the activity entry for a relay that is gone."
  @spec forget(binary()) :: :ok
  def forget(relay_id) do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      tid ->
        :ets.delete(tid, relay_id)
        :ok
    end
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
