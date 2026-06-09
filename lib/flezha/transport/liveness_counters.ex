defmodule Flezha.Transport.LivenessCounters do
  @moduledoc """
  Always-on, lock-free liveness counters for the controller data plane.

  Each `StreamChannel` and `Relay.Overwatch` owns an `:atomics`-backed
  `:counters` array and bumps slots locally (single-process, no contention).
  The ref is registered in a public ETS table so the `:flezha_transport`
  observability source and the convergence stall classifier can read every
  channel/relay's counters in one lock-free scan.

  Cardinality is bounded: one entry per channel (executor + stream name) and
  one per relay instance — never per frame. Entries whose owner process has
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
  @channel_slots [:io_in_received, :send_frame_calls, :nil_drops, :flz_data_received, :last_send_ms]
  @relay_slots [:io_in_received, :forwarded, :filtered_nil]
  @channel_size length(@channel_slots)
  @relay_size length(@relay_slots)

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
  Register a channel's counter array, keyed by `{executor_id, name}`. The
  caller (the channel process) owns the entry; pass `self()`. Returns the
  `:counters` ref to bump on the hot path.
  """
  @spec register_channel(term(), term()) :: :counters.counters_ref()
  def register_channel(executor_id, name) do
    ensure_table()
    ref = :counters.new(@channel_size, [:atomics])
    :ets.insert(@table, {{:channel, executor_id, name}, ref, self()})
    ref
  end

  @doc """
  Register a relay's counter array, keyed by `relay_id`. Pass `self()` as the
  owner. Returns the `:counters` ref.
  """
  @spec register_relay(term()) :: :counters.counters_ref()
  def register_relay(relay_id) do
    ensure_table()
    ref = :counters.new(@relay_size, [:atomics])
    :ets.insert(@table, {{:relay, relay_id}, ref, self()})
    ref
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

  @doc "Increment a relay counter slot by one."
  @spec bump_relay(:counters.counters_ref() | nil, atom()) :: :ok
  def bump_relay(nil, _slot), do: :ok
  def bump_relay(ref, slot), do: :counters.add(ref, relay_index(slot), 1)

  # Compile each slot atom to its 1-based `:counters` index (same perf as
  # hand-written clauses, derived from the layout so the two never drift).
  for {slot, idx} <- Enum.with_index(@channel_slots, 1) do
    defp channel_index(unquote(slot)), do: unquote(idx)
  end

  for {slot, idx} <- Enum.with_index(@relay_slots, 1) do
    defp relay_index(unquote(slot)), do: unquote(idx)
  end

  # ---------- read side ----------

  @doc """
  Snapshot every live channel and relay counter array, pruning entries whose
  owner has died. Returns `%{channels: [...], relays: [...]}`.
  """
  @spec snapshot_all() :: %{channels: [map()], relays: [map()]}
  def snapshot_all() do
    case :ets.whereis(@table) do
      :undefined ->
        %{channels: [], relays: []}

      _ ->
        entries = :ets.tab2list(@table)

        {channels, relays} =
          Enum.reduce(entries, {[], []}, fn {key, ref, owner}, {chans, rels} ->
            if Process.alive?(owner) do
              case key do
                {:channel, executor_id, name} ->
                  {[channel_snapshot(executor_id, name, ref) | chans], rels}

                {:relay, relay_id} ->
                  {chans, [relay_snapshot(relay_id, ref) | rels]}
              end
            else
              :ets.delete(@table, key)
              {chans, rels}
            end
          end)

        %{channels: channels, relays: relays}
    end
  end

  defp channel_snapshot(executor_id, name, ref) do
    snapshot(@channel_slots, ref, %{executor_id: executor_id, name: name})
  end

  defp relay_snapshot(relay_id, ref) do
    snapshot(@relay_slots, ref, %{relay_id: relay_id})
  end

  # Read every slot of `ref` into `base`, keyed by the layout's slot atoms.
  defp snapshot(slots, ref, base) do
    slots
    |> Enum.with_index(1)
    |> Enum.reduce(base, fn {slot, idx}, acc -> Map.put(acc, slot, :counters.get(ref, idx)) end)
  end
end
