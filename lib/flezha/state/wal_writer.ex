defmodule Flezha.State.WalWriter do
  @moduledoc """
  Per-Partition `GenServer` wrapping a single open WAL file handle.

  Each appended record is `<<seq::64-big, len::32-big, header::binary,
  change_bytes::binary, crc32::32-big>>` where `len = 16 + byte_size(change_bytes)`
  and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`.

  fsync is decoupled from append: the writer registers as dirty with
  `Flezha.State.FsyncBatcher` which drives the actual `:file.datasync/1`
  on its tick cadence. High-watermark crossings emit a telemetry event
  (`[:flezha, :state, :wal, :backpressure]`) that section-9 wiring picks
  up to ship the wire-level `PartitionMailboxStatus`.
  """

  use GenServer, restart: :transient

  alias Flezha.State.{FsyncBatcher, Metrics, Outbox, Registry}

  @header_size 16
  @reservoir_capacity 1024

  defstruct [
    :partition_id,
    :file_handle,
    :wal_path,
    :last_persisted_seq,
    :last_advance_at,
    :last_change_seen_at,
    :pending_bytes,
    :pending_since_us,
    :high_watermark,
    :low_watermark,
    :metrics,
    :latency_reservoir,
    :backpressured?
  ]

  @type t :: %__MODULE__{
          partition_id: Registry.partition_id(),
          file_handle: :file.io_device(),
          wal_path: binary(),
          last_persisted_seq: non_neg_integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          pending_bytes: non_neg_integer(),
          pending_since_us: non_neg_integer() | nil,
          high_watermark: pos_integer(),
          low_watermark: pos_integer(),
          metrics: :counters.counters_ref(),
          latency_reservoir: :queue.queue(),
          backpressured?: boolean()
        }

  @typedoc """
  Liveness view consumed by `Flezha.State.Watchdog`. `last_advance_at`
  moves only when `last_persisted_seq` increases; `last_change_seen_at`
  moves on every received Change frame (append, duplicate, or gap). A
  Partition is *stalled* (vs idle) when Changes are still arriving but
  the persisted seq has stopped advancing.
  """
  @type stall_status :: %{
          last_persisted_seq: non_neg_integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer()
        }

  # ---------- Public API ----------

  def start_link(partition_id) do
    GenServer.start_link(__MODULE__, partition_id, name: via(partition_id))
  end

  @spec append(GenServer.server(), non_neg_integer(), binary(), binary()) ::
          :ok | :duplicate | {:error, term()}
  def append(server, seq, header, change_bytes) do
    GenServer.call(server, {:append, seq, header, change_bytes})
  end

  @spec fsync(GenServer.server()) :: :ok
  def fsync(server), do: GenServer.call(server, :fsync)

  @spec truncate_up_to(GenServer.server(), non_neg_integer()) :: :ok
  def truncate_up_to(server, seq), do: GenServer.call(server, {:truncate_up_to, seq})

  @spec last_persisted_seq(GenServer.server()) :: non_neg_integer()
  def last_persisted_seq(server), do: GenServer.call(server, :last_persisted_seq)

  @spec stall_status(GenServer.server()) :: stall_status()
  def stall_status(server), do: GenServer.call(server, :stall_status)

  @spec close(GenServer.server()) :: :ok
  def close(server), do: GenServer.stop(server, :normal)

  defp via(partition_id) do
    {:via, Elixir.Registry, {Flezha.State.Supervisor.registry_name(), partition_id}}
  end

  # ---------- Encode / decode ----------

  @doc """
  Frame a record for on-disk storage.
  """
  @spec encode_record(non_neg_integer(), binary(), binary()) :: binary()
  def encode_record(seq, header, change_bytes)
      when is_integer(seq) and seq >= 0 and is_binary(header) and is_binary(change_bytes) do
    if byte_size(header) != @header_size do
      raise ArgumentError, "header must be #{@header_size} bytes, got #{byte_size(header)}"
    end

    len = @header_size + byte_size(change_bytes)
    crc = :erlang.crc32(<<header::binary, change_bytes::binary>>)
    <<seq::64-big, len::32-big, header::binary, change_bytes::binary, crc::32-big>>
  end

  @doc """
  Read one record at `position` from `device`. Returns the decoded
  `{seq, header, change_bytes}` triple plus the byte length consumed,
  or an error tag.
  """
  @spec decode_record(:file.io_device(), non_neg_integer()) ::
          {:ok, non_neg_integer(), binary(), binary(), non_neg_integer()}
          | {:error, :crc_mismatch | :short_read | :bad_len}
  def decode_record(device, position) do
    with {:ok, <<seq::64-big, len::32-big>>} <- pread(device, position, 12),
         true <- len >= @header_size or {:error, :bad_len},
         {:ok, payload_and_crc} <- pread(device, position + 12, len + 4) do
      payload_size = len

      <<header::binary-size(@header_size), change_bytes::binary-size(payload_size - @header_size),
        crc::32-big>> = payload_and_crc

      computed = :erlang.crc32(<<header::binary, change_bytes::binary>>)

      if computed == crc do
        {:ok, seq, header, change_bytes, 12 + len + 4}
      else
        {:error, :crc_mismatch}
      end
    else
      :eof -> {:error, :short_read}
      {:error, _} = err -> err
      false -> {:error, :bad_len}
    end
  end

  defp pread(device, position, count) do
    case :file.pread(device, position, count) do
      {:ok, bin} when byte_size(bin) == count -> {:ok, bin}
      {:ok, _} -> :eof
      :eof -> :eof
      {:error, _} = err -> err
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(partition_id) do
    Process.flag(:trap_exit, true)
    wal_path = Registry.wal_path(partition_id)
    File.mkdir_p!(Path.dirname(wal_path))
    {:ok, file} = :file.open(wal_path, [:raw, :read, :write, :binary])

    last_seq = scan_for_last_valid_seq(file, wal_path)

    metrics = Metrics.for_partition(partition_id)
    Metrics.put(metrics, :wal_seq, last_seq)
    Metrics.put(metrics, :wal_size_bytes, file_size(wal_path))

    now = :erlang.monotonic_time(:millisecond)

    state = %__MODULE__{
      partition_id: partition_id,
      file_handle: file,
      wal_path: wal_path,
      last_persisted_seq: last_seq,
      last_advance_at: now,
      last_change_seen_at: now,
      pending_bytes: 0,
      pending_since_us: nil,
      high_watermark: high_watermark(),
      low_watermark: low_watermark(),
      metrics: metrics,
      latency_reservoir: :queue.new(),
      backpressured?: false
    }

    # Reposition for appends.
    {:ok, _} = :file.position(file, :eof)

    {:ok, state}
  end

  @impl true
  def handle_call({:append, seq, header, change_bytes}, _from, state) do
    # Every received Change frame — accepted, deduped, or rejected as a
    # gap — proves the SDK is still emitting, which distinguishes a
    # stalled Partition from a genuinely idle one for the watchdog.
    now = :erlang.monotonic_time(:millisecond)
    state = %{state | last_change_seen_at: now}

    cond do
      seq <= state.last_persisted_seq ->
        Metrics.add(state.metrics, :dedup_drops_total, 1)
        {:reply, :duplicate, state}

      seq != state.last_persisted_seq + 1 ->
        {:reply, {:error, {:gap, expected: state.last_persisted_seq + 1, got: seq}}, state}

      true ->
        record = encode_record(seq, header, change_bytes)
        :ok = :file.write(state.file_handle, record)
        size = byte_size(record)

        Metrics.add(state.metrics, :persisted_changes_total, 1)
        Metrics.add(state.metrics, :persisted_bytes_total, size)
        Metrics.put(state.metrics, :wal_seq, seq)
        Metrics.add(state.metrics, :wal_size_bytes, size)

        # Update outbox dedup file. A failure here is non-fatal: on restart
        # `scan_for_last_valid_seq/2` recomputes from the WAL itself; the
        # dedup file is a fast-path cache.
        _ = Outbox.write_last_seq(state.partition_id, seq)

        FsyncBatcher.mark_dirty(state.partition_id)

        new_pending = state.pending_bytes + size

        new_state = %{
          state
          | last_persisted_seq: seq,
            last_advance_at: now,
            pending_bytes: new_pending,
            pending_since_us: state.pending_since_us || System.monotonic_time(:microsecond)
        }

        new_state = maybe_signal_backpressure(new_state, new_pending)

        {:reply, :ok, new_state}
    end
  end

  def handle_call(:fsync, _from, state) do
    t0 = System.monotonic_time(:microsecond)
    :ok = :file.datasync(state.file_handle)
    t1 = System.monotonic_time(:microsecond)

    record_latency_sample(state.metrics, state.latency_reservoir, div(t1 - t0, 1000))
    |> case do
      {p99_ms, new_reservoir} ->
        Metrics.put(state.metrics, :flush_latency_ms_p99, p99_ms)

        Metrics.put(
          state.metrics,
          :fsync_durability_lag_ms,
          fsync_lag_ms(state.pending_since_us, t1)
        )

        new_state = %{
          state
          | pending_bytes: 0,
            pending_since_us: nil,
            latency_reservoir: new_reservoir
        }

        new_state = maybe_signal_backpressure(new_state, 0)
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:truncate_up_to, seq}, _from, state) do
    :ok = :file.close(state.file_handle)
    new_path = rewrite_keep_seq_gt(state.wal_path, seq)
    :ok = File.rename(new_path, state.wal_path)
    {:ok, file} = :file.open(state.wal_path, [:raw, :read, :write, :binary])
    {:ok, _} = :file.position(file, :eof)

    Metrics.put(state.metrics, :wal_size_bytes, file_size(state.wal_path))

    {:reply, :ok, %{state | file_handle: file, pending_bytes: 0, pending_since_us: nil}}
  end

  def handle_call(:last_persisted_seq, _from, state) do
    {:reply, state.last_persisted_seq, state}
  end

  def handle_call(:stall_status, _from, state) do
    status = %{
      last_persisted_seq: state.last_persisted_seq,
      last_advance_at: state.last_advance_at,
      last_change_seen_at: state.last_change_seen_at
    }

    {:reply, status, state}
  end

  @impl true
  def terminate(_reason, %{file_handle: file}) when not is_nil(file) do
    _ = :file.datasync(file)
    _ = :file.close(file)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  # ---------- Scan / rewrite helpers ----------

  @doc """
  Walk the WAL from offset 0, returning the seq of the last record
  whose crc validates. Truncates any partial-write tail past that point.

  Backward scan would require a second copy of the length field at the
  record tail; we use forward scan and rely on monotonic seqs for the
  "last valid record" answer. The cost is O(records-since-snapshot)
  at boot; acceptable because compaction keeps WALs short.
  """
  @spec scan_for_last_valid_seq(:file.io_device(), binary()) :: non_neg_integer()
  def scan_for_last_valid_seq(device, _wal_path) do
    do_scan(device, 0, 0, 0)
  end

  defp do_scan(device, position, last_seq, last_end) do
    case decode_record(device, position) do
      {:ok, seq, _header, _bytes, consumed} ->
        do_scan(device, position + consumed, seq, position + consumed)

      {:error, _} ->
        # Truncate any partial-write tail past the last valid record.
        case :file.position(device, last_end) do
          {:ok, _} ->
            _ = :file.truncate(device)
            last_seq

          _ ->
            last_seq
        end
    end
  end

  defp rewrite_keep_seq_gt(wal_path, keep_above_seq) do
    {:ok, src} = :file.open(wal_path, [:raw, :read, :binary])
    new_path = wal_path <> ".rewriting"
    {:ok, dst} = :file.open(new_path, [:raw, :write, :binary])

    try do
      copy_keep_seq_gt(src, dst, 0, keep_above_seq)
      :ok = :file.sync(dst)
    after
      _ = :file.close(src)
      _ = :file.close(dst)
    end

    new_path
  end

  defp copy_keep_seq_gt(src, dst, position, keep_above_seq) do
    case decode_record(src, position) do
      {:ok, seq, header, change_bytes, consumed} ->
        if seq > keep_above_seq do
          :ok = :file.write(dst, encode_record(seq, header, change_bytes))
        end

        copy_keep_seq_gt(src, dst, position + consumed, keep_above_seq)

      {:error, _} ->
        :ok
    end
  end

  # ---------- Backpressure ----------

  defp maybe_signal_backpressure(%{backpressured?: false} = state, pending)
       when pending >= state.high_watermark do
    emit_backpressure(state.partition_id, :full, pending)
    %{state | backpressured?: true}
  end

  defp maybe_signal_backpressure(%{backpressured?: true} = state, pending)
       when pending <= state.low_watermark do
    emit_backpressure(state.partition_id, :open, pending)
    %{state | backpressured?: false}
  end

  defp maybe_signal_backpressure(state, _), do: state

  # Emit the telemetry event AND ship the wire signal. The telemetry
  # event is for in-process observers (metrics, debug); the wire signal
  # routes through the producing Shard's `OverflowPolicy` machinery on
  # the SDK side.
  defp emit_backpressure(partition_id, status, pending) do
    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{pending_bytes: pending},
      %{partition_id: partition_id, status: status}
    )

    _ = Flezha.Relay.Overwatch.notify_partition_backpressure(partition_id, status)
    :ok
  end

  # ---------- Metrics helpers ----------

  defp record_latency_sample(_metrics, reservoir, sample_ms) do
    capped =
      if :queue.len(reservoir) >= @reservoir_capacity do
        {_, q} = :queue.out(reservoir)
        q
      else
        reservoir
      end

    new_reservoir = :queue.in(sample_ms, capped)
    {p99(new_reservoir), new_reservoir}
  end

  defp p99(reservoir) do
    list = :queue.to_list(reservoir)

    case length(list) do
      0 ->
        0

      n ->
        sorted = Enum.sort(list)
        idx = max(0, ceil(n * 0.99) - 1)
        Enum.at(sorted, idx, 0)
    end
  end

  defp fsync_lag_ms(nil, _now_us), do: 0

  defp fsync_lag_ms(pending_since_us, now_us) do
    div(max(now_us - pending_since_us, 0), 1000)
  end

  defp file_size(path) do
    case :file.read_file_info(path) do
      {:ok, info} -> elem(info, 1)
      _ -> 0
    end
  end

  defp high_watermark() do
    Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)
  end

  defp low_watermark() do
    Application.get_env(
      :flezha,
      :wal_low_watermark_bytes,
      div(high_watermark(), 2)
    )
  end
end
