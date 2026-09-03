defmodule Flezha.Transport.LivenessCounters do
  @moduledoc """
  Always-on, lock-free liveness counters for the controller data plane.

  Each `StreamChannel` and `Operative.Overwatch` owns an `:atomics`-backed
  `:counters` array and bumps slots locally (single-process, no contention).
  The ref is registered in a public ETS table so the `:flezha_transport`
  observability source and the convergence stall classifier can read every
  channel/operative's counters in one lock-free scan.

  Cardinality is bounded: one entry per channel (runner + stream name) and
  one per operative instance — never per frame. Entries whose owner process has
  died are pruned on the next `snapshot_all/0`.

  Counters mirror the `Flezha.Observability.Counters` pattern (sub-100ns
  increment) and add **zero** data-plane behaviour — they are pure
  instrumentation.
  """

  @table :flezha_liveness_counters

  # Slot layouts — the single source of truth for both the `:counters` index of
  # each slot and its key in `snapshot_all/0`. Position (1-based) is the index;
  # appending a slot is the only edit needed. `last_send_ms` holds a monotonic-ms
  # timestamp (set, not incremented); the rest are monotonic counts.
  # `hb_count` counts heartbeat frames; `hb_dispatches` mirrors the runner's
  # self-reported dispatch-progress counter carried in heartbeat metadata —
  # heartbeats advancing while hb_dispatches is frozen means the runner
  # process is alive but its dispatch pool is wedged.
  # `hb_inflight` and `hb_inflight_oldest_ms` mirror the runner's handout
  # self-report (set, not incremented): activations underway and the age of the
  # oldest. `hb_dispatches` freezing says the WHOLE pool stopped; these two are
  # what make ONE wedged Entity visible while the runner keeps completing work.
  # `credit_active` is 0/1 (set), `credit_ledger_level` is the stream's
  # remaining credit bytes (set), `credit_overrun_total` counts frames a
  # credit-speaking sender pushed past an exhausted ledger (protocol
  # violations — any non-zero value is an anomaly, never load response).
  # `hb_blocked_parks` mirrors the runner's DropNothing block counter and
  # `hb_drops_newest`/`hb_drops_oldest` its shed counts (all set, not
  # incremented): dispatches frozen with blocked_parks climbing is healthy
  # backpressure, dispatches frozen with it flat is a wedge, and the two drop
  # counters are the only outside handle on a shedding operative's loss.
  # `hb_pending_pushes` and `hb_pending_push_oldest_ms` mirror the runner's
  # deferred-push self-report (set, not incremented): Entities whose producer
  # is being withheld work because a consumer is full. A gated producer holds
  # no handout, so `hb_inflight` is blind to it and backpressure that
  # withholds work would otherwise read as a runner doing nothing.
  # `stale_writer_refused` counts durable-write forwards refused on a fenced
  # (takeover-superseded) channel.
  # `send_gate_held` counts senders held on this stream's send-buffer gate —
  # the send-side mirror of `hb_credit_parked`: advancing while `nil_drops`
  # stays flat is congestion being absorbed, not data being lost.
  @channel_slots [
    :io_in_received,
    :send_frame_calls,
    :nil_drops,
    :flz_data_received,
    :last_send_ms,
    :hb_count,
    :hb_dispatches,
    :hb_credit_parked,
    :hb_inflight,
    :hb_inflight_oldest_ms,
    :hb_blocked_parks,
    :hb_drops_newest,
    :hb_drops_oldest,
    :hb_pending_pushes,
    :hb_pending_push_oldest_ms,
    :credit_active,
    :credit_ledger_level,
    :credit_overrun_total,
    :stale_writer_refused,
    :send_gate_held
  ]
  @operative_slots [:io_in_received, :forwarded, :filtered_nil, :parked_drops]
  @channel_size length(@channel_slots)
  @operative_size length(@operative_slots)

  @doc """
  Whether the ~1 Hz driver poll + stall classifier should run. The counters
  themselves are always-on; only the polling/classification is gated.
  """
  @spec poll_enabled?() :: boolean()
  def poll_enabled?() do
    Application.get_env(:flezha, :observability, [])
    |> Keyword.get(:data_plane_liveness, true)
  end

  # ---------- table lifecycle ----------

  @doc "Create the registry table if absent. Idempotent and race-tolerant."
  @spec ensure_table() :: :ok
  def ensure_table() do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [
            :set,
            :public,
            :named_table,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  # ---------- registration ----------

  @doc """
  Register a channel's counter array, keyed by `{runner_id, name}`. The
  caller (the channel process) owns the entry; pass `self()`. Returns the
  `:counters` ref to bump on the hot path.
  """
  @spec register_channel(term(), term()) :: :counters.counters_ref()
  def register_channel(runner_id, name) do
    ensure_table()
    ref = :counters.new(@channel_size, [:atomics])
    :ets.insert(@table, {{:channel, runner_id, name}, ref, self()})
    ref
  end

  @doc """
  Register a operative's counter array, keyed by `operative_id`. Pass `self()` as the
  owner. Returns the `:counters` ref.
  """
  @spec register_operative(term()) :: :counters.counters_ref()
  def register_operative(operative_id) do
    ensure_table()
    ref = :counters.new(@operative_size, [:atomics])
    :ets.insert(@table, {{:operative, operative_id}, ref, self()})
    ref
  end

  @doc """
  Register a per-runner data-stream NIF server so the stall classifier can
  poll its driver self-report. Without this the classifier only sees the
  control-channel driver, and the data-plane drivers are the blind spot that
  leaves. Pass `self()` (the owning `StreamAcceptor`).
  """
  @spec register_driver(term(), atom(), reference()) :: :ok
  def register_driver(runner_id, type, server_ref) do
    ensure_table()
    :ets.insert(@table, {{:driver, runner_id, type}, server_ref, self()})
    :ok
  end

  @doc """
  Live data-stream driver refs as `{{runner_id, type}, server_ref}` pairs,
  pruning entries whose owner has died.
  """
  @spec driver_refs() :: [{{term(), atom()}, reference()}]
  def driver_refs() do
    case :ets.whereis(@table) do
      :undefined ->
        []

      _ ->
        :ets.match_object(@table, {{:driver, :_, :_}, :_, :_})
        |> Enum.flat_map(fn {{:driver, runner_id, type} = key, ref, owner} ->
          if Process.alive?(owner) do
            [{{runner_id, type}, ref}]
          else
            :ets.delete(@table, key)
            []
          end
        end)
    end
  end

  # ---------- hot-path mutation ----------

  @doc "Increment a channel counter slot by one."
  @spec bump_channel(:counters.counters_ref() | nil, atom()) :: :ok
  def bump_channel(nil, _slot), do: :ok
  def bump_channel(ref, slot), do: :counters.add(ref, channel_index(slot), 1)

  @doc "Set a channel timestamp slot (e.g. `:last_send_ms`)."
  @spec set_channel(:counters.counters_ref() | nil, atom(), integer()) :: :ok
  def set_channel(nil, _slot, _value), do: :ok
  def set_channel(ref, slot, value), do: :counters.put(ref, channel_index(slot), value)

  @doc "Increment a operative counter slot by one."
  @spec bump_operative(:counters.counters_ref() | nil, atom()) :: :ok
  def bump_operative(nil, _slot), do: :ok
  def bump_operative(ref, slot), do: :counters.add(ref, operative_index(slot), 1)

  # Compile each slot atom to its 1-based `:counters` index (same perf as
  # hand-written clauses, derived from the layout so the two never drift).
  for {slot, idx} <- Enum.with_index(@channel_slots, 1) do
    defp channel_index(unquote(slot)), do: unquote(idx)
  end

  for {slot, idx} <- Enum.with_index(@operative_slots, 1) do
    defp operative_index(unquote(slot)), do: unquote(idx)
  end

  # ---------- read side ----------

  @doc """
  Snapshot every live channel and operative counter array, pruning entries whose
  owner has died. Returns `%{channels: [...], operatives: [...]}`.
  """
  @spec snapshot_all() :: %{channels: [map()], operatives: [map()]}
  def snapshot_all() do
    case :ets.whereis(@table) do
      :undefined ->
        %{channels: [], operatives: []}

      _ ->
        entries = :ets.tab2list(@table)

        {channels, operatives} =
          Enum.reduce(entries, {[], []}, fn {key, ref, owner}, {chans, rels} ->
            if Process.alive?(owner) do
              case key do
                {:channel, runner_id, name} ->
                  {[channel_snapshot(runner_id, name, ref) | chans], rels}

                {:operative, operative_id} ->
                  {chans, [operative_snapshot(operative_id, ref) | rels]}

                # Driver server refs are polled via `driver_refs/0`, not here.
                {:driver, _runner_id, _type} ->
                  {chans, rels}
              end
            else
              :ets.delete(@table, key)
              {chans, rels}
            end
          end)

        %{channels: channels, operatives: operatives}
    end
  end

  defp channel_snapshot(runner_id, name, ref) do
    snapshot(@channel_slots, ref, %{runner_id: runner_id, name: name})
  end

  defp operative_snapshot(operative_id, ref) do
    snapshot(@operative_slots, ref, %{operative_id: operative_id})
  end

  # Read every slot of `ref` into `base`, keyed by the layout's slot atoms.
  defp snapshot(slots, ref, base) do
    slots
    |> Enum.with_index(1)
    |> Enum.reduce(base, fn {slot, idx}, acc -> Map.put(acc, slot, :counters.get(ref, idx)) end)
  end
end
