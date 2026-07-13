defmodule Flezha.State.WalWriter do
  @moduledoc """
  Per-Segment `GenServer` wrapping a single open WAL file handle.

  Each appended record is `<<seq::64-big, len::32-big, header::binary,
  change_bytes::binary, crc32::32-big>>` where `len = 16 + byte_size(change_bytes)`
  and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`.

  fsync is decoupled from append: the writer registers as dirty with
  `Flezha.State.FsyncBatcher` which drives the actual `:file.datasync/1`
  on its tick cadence. High-watermark crossings emit a telemetry event
  (`[:flezha, :state, :wal, :backpressure]`) that section-9 wiring picks
  up to ship the wire-level `SegmentMailboxStatus`.
  """

  use GenServer, restart: :transient

  require Logger

  alias Flezha.Executor.FleetManager
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{FsyncBatcher, InputLog, Metrics, Outbox, Recovery, Registry, Wire}

  @header_size 16
  @reservoir_capacity 1024
  @reserved_segment_keys Registry.reserved_segment_keys()

  defstruct [
    :segment_id,
    :file_handle,
    :wal_path,
    :last_persisted_seq,
    :last_advance_at,
    :last_change_seen_at,
    :owning_executor,
    :pending_bytes,
    :pending_since_us,
    :high_watermark,
    :low_watermark,
    :hard_watermark,
    :pending_msg_cap,
    :metrics,
    :latency_reservoir,
    :backpressured?,
    :shedding?,
    :awaiting_baseline?,
    :oldest_unsnapshotted_at,
    :credit_pending,
    :credit_active?,
    :refused_sessions,
    # Effectively-once outbox: FIFO of effect-record entries awaiting the
    # fsync-gated `:on_output` dispatch (`%{seq, outputs, consumed_offset}`,
    # dispatchable once `seq <= outbox_durable_seq`), the flush cursor, the
    # realign epoch, and the retry timer armed while the operative's
    # Overwatch is not yet resolvable.
    :pending_outbox,
    :outbox_durable_seq,
    :outbox_flushed_seq,
    :outbox_epoch,
    :outbox_retry_timer,
    # Highest seq of an OUTPUT-BEARING effect record queued. Producing-liveness
    # compares it against `outbox_flushed_seq` to see an outbox that accumulates
    # without draining. It must count only output-bearing records:
    # `outbox_durable_seq` advances on every fsynced record, so a Segment that
    # persists state but emits nothing would read as a frozen outbox forever.
    :outbox_last_seq
  ]

  @type t :: %__MODULE__{
          segment_id: Registry.segment_id(),
          file_handle: :file.io_device(),
          wal_path: binary(),
          last_persisted_seq: integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          owning_executor: binary() | nil,
          pending_bytes: non_neg_integer(),
          pending_since_us: non_neg_integer() | nil,
          high_watermark: pos_integer(),
          low_watermark: pos_integer(),
          hard_watermark: pos_integer(),
          pending_msg_cap: pos_integer(),
          metrics: :counters.counters_ref(),
          latency_reservoir: :queue.queue(),
          backpressured?: boolean(),
          shedding?: boolean(),
          awaiting_baseline?: boolean(),
          oldest_unsnapshotted_at: integer() | nil,
          credit_pending: %{optional({pid(), binary()}) => pos_integer()},
          credit_active?: boolean(),
          refused_sessions: MapSet.t(binary())
        }

  @typedoc """
  Liveness view consumed by `Flezha.State.Watchdog`. `last_advance_at`
  moves only when `last_persisted_seq` increases; `last_change_seen_at`
  moves on every received Change frame (append, duplicate, or gap). A
  Segment is *stalled* (vs idle) when Changes are still arriving but
  the persisted seq has stopped advancing. `wal_size_bytes` /
  `oldest_unsnapshotted_at` / `owning_executor` feed the Watchdog's
  size+age compaction triggers without a second disk scan.
  """
  @type stall_status :: %{
          last_persisted_seq: integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          wal_size_bytes: non_neg_integer(),
          oldest_unsnapshotted_at: integer() | nil,
          owning_executor: binary() | nil,
          out_pending_seq: integer(),
          outbox_flushed_seq: integer()
        }

  # ---------- Public API ----------

  def start_link(segment_id) do
    GenServer.start_link(__MODULE__, segment_id, name: via(segment_id))
  end

  @doc """
  Append a Change at `seq`. `executor_id` identifies the executor that emitted
  it; a Change from a *different* executor than the one that wrote the current
  epoch realigns the WAL to the new executor's seq base (executor-restart
  recovery — see `handle_call/3`) — unless a takeover superseded that session
  (`Flezha.Executor.FleetManager.superseded_session?/1`), in which case the
  frame is refused with `{:error, :stale_writer}` instead of realigning. Pass
  `nil` (the default) to disable epoch tracking — the single-executor / test
  path keeps the prior dedup/gap/append semantics.
  """
  @spec append(GenServer.server(), non_neg_integer(), binary(), binary(), binary() | nil) ::
          :ok | :duplicate | :awaiting_baseline | {:error, term()}
  def append(server, seq, header, change_bytes, executor_id \\ nil) do
    GenServer.call(server, {:append, seq, header, change_bytes, executor_id})
  end

  @doc """
  Non-blocking append. The caller (a `StreamChannel`) hands off the Change and
  returns immediately, so WAL/recovery latency never stalls the data-plane
  channel. On a seq gap the writer emits `[:flezha, :state, :seq_gap]` and
  drives resync itself. Past the hard in-memory bound (un-fsynced bytes or
  mailbox depth) the Change is shed (dropped, `wal_shed_total` incremented),
  and the resulting gap drives resync — back-pressure sheds, never blocks.

  `credit_opts` carries the credit-grant ledger attribution for a
  credit-speaking stream: `%{channel: pid, session: id, wire_size: bytes}`.
  The writer reports every attributed byte back to
  `channel` exactly once as a `{:credit_drained, session, bytes}` message —
  on fsync for written records (durable drain, mints credit), or immediately
  for a Change that never occupies the un-fsynced buffer (dedup drop, gap
  rejection, shed, stale-writer refusal, epoch realign, truncation). The
  session tag lets the
  channel drop reports for bytes debited under a pre-reconnect session. An
  empty map (the default) is the legacy path with zero accounting.
  """
  @spec append_async(
          GenServer.server(),
          non_neg_integer(),
          binary(),
          binary(),
          binary() | nil,
          map()
        ) :: :ok
  def append_async(server, seq, header, change_bytes, executor_id \\ nil, credit_opts \\ %{}) do
    GenServer.cast(server, {:append_async, seq, header, change_bytes, executor_id, credit_opts})
  end

  @spec fsync(GenServer.server()) :: :ok
  def fsync(server), do: GenServer.call(server, :fsync)

  @spec truncate_up_to(GenServer.server(), non_neg_integer()) :: :ok
  def truncate_up_to(server, seq), do: GenServer.call(server, {:truncate_up_to, seq})

  @doc """
  Rewind the Segment to `seq` under `epoch`: keep the records at or below it,
  drop the snapshots above it, and re-seat the outbox on the survivors.

  The non-destructive sibling of the realign wipe. A realign discards a stale
  executor's whole lineage; a rewind discards only the history above a boundary
  the controller means to rebuild, which is what a late-input correction needs.

  `epoch` is explicit rather than `current + 1` so that re-running an
  interrupted correction re-asserts the epoch it already committed to. A
  correction that crashed after this call and before its replay finished resumes
  by rewinding to the same boundary under the same epoch — a no-op on the WAL,
  and crucially not a second bump, which would make the replay's outputs miss
  the rows the first attempt already wrote.
  """
  @spec rewind_to(GenServer.server(), non_neg_integer(), non_neg_integer()) :: :ok
  def rewind_to(server, seq, epoch), do: GenServer.call(server, {:rewind_to, seq, epoch})

  @doc "Current outbox epoch — the base a deliberate correction bumps from."
  @spec outbox_epoch(GenServer.server()) :: non_neg_integer()
  def outbox_epoch(server), do: GenServer.call(server, :outbox_epoch)

  @doc """
  Where a correction for `target_event_time` must rewind to: the last seq whose
  history entirely precedes that event time, plus the input offset consumed as
  of that seq.

  The boundary is the record *before the first one at or after the target* — not
  the last record whose own event time precedes it. The difference matters
  exactly when the WAL already holds an out-of-order straggler (a beyond-window
  input processed as an arrival): scanning backwards for the first earlier event
  time would stop at the straggler and leave the records that overtook the late
  input in place, correcting nothing. Scanning forward folds the straggler into
  the replay too, which is what re-ordering it means.

  `{:error, :nothing_to_correct}` when no record is at or after the target —
  the input is not actually late for this Segment.
  """
  @spec correction_baseline(GenServer.server(), non_neg_integer()) ::
          {:ok, %{seq: integer(), consumed_offset: integer()}} | {:error, :nothing_to_correct}
  def correction_baseline(server, target_event_time),
    do: GenServer.call(server, {:correction_baseline, target_event_time})

  @doc """
  Highest input-log offset consumed at or below `seq` (`-1` when none) — the
  point an operator replay from that baseline must re-forward the input log
  from.
  """
  @spec consumed_offset_at(GenServer.server(), non_neg_integer()) :: integer()
  def consumed_offset_at(server, seq), do: GenServer.call(server, {:consumed_offset_at, seq})

  @spec last_persisted_seq(GenServer.server()) :: integer()
  def last_persisted_seq(server), do: GenServer.call(server, :last_persisted_seq)

  @doc "Live effectively-once outbox status (`flz state outbox`)."
  @spec outbox_status(GenServer.server()) :: map()
  def outbox_status(server), do: GenServer.call(server, :outbox_status)

  @spec stall_status(GenServer.server()) :: stall_status()
  def stall_status(server), do: GenServer.call(server, :stall_status)

  @spec close(GenServer.server()) :: :ok
  def close(server), do: GenServer.stop(server, :normal)

  defp via(segment_id) do
    {:via, Elixir.Registry, {Flezha.State.Supervisor.registry_name(), segment_id}}
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
      change_len = len - @header_size

      <<header::binary-size(@header_size), change_bytes::binary-size(^change_len), crc::32-big>> =
        payload_and_crc

      computed = :erlang.crc32(<<header::binary, change_bytes::binary>>)

      if computed == crc do
        {:ok, seq, header, change_bytes, 12 + len + 4}
      else
        {:error, :crc_mismatch}
      end
    else
      # `len >= @header_size or {:error, :bad_len}` yields `true` or the
      # error tuple, so the tuple clause carries :bad_len too.
      :eof -> {:error, :short_read}
      {:error, _} = err -> err
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
  def init(segment_id) do
    Process.flag(:trap_exit, true)
    now = :erlang.monotonic_time(:millisecond)

    # Disk I/O (mkdir/open/scan/seek) is deferred to `handle_continue/2` so this
    # `init` returns instantly: the process is already Registry-registered, so a
    # `Supervisor.find_or_start/1` from a StreamChannel never blocks on disk. The
    # continue runs before any queued append, so appends queue in this writer's
    # mailbox — never the channel's. `-1` is the "no records yet" seq sentinel.
    state = %__MODULE__{
      segment_id: segment_id,
      file_handle: nil,
      wal_path: Registry.wal_path(segment_id),
      last_persisted_seq: -1,
      last_advance_at: now,
      last_change_seen_at: now,
      owning_executor: nil,
      pending_bytes: 0,
      pending_since_us: nil,
      high_watermark: high_watermark(),
      low_watermark: low_watermark(),
      hard_watermark: hard_watermark(),
      pending_msg_cap: pending_msg_cap(),
      metrics: Metrics.for_segment(segment_id),
      latency_reservoir: :queue.new(),
      backpressured?: false,
      shedding?: false,
      awaiting_baseline?: false,
      oldest_unsnapshotted_at: nil,
      credit_pending: %{},
      credit_active?: false,
      refused_sessions: MapSet.new(),
      pending_outbox: :queue.new(),
      outbox_durable_seq: -1,
      outbox_flushed_seq: -1,
      outbox_epoch: 0,
      outbox_retry_timer: nil,
      outbox_last_seq: -1
    }

    {:ok, state, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, state) do
    File.mkdir_p!(Path.dirname(state.wal_path))
    {:ok, file} = :file.open(state.wal_path, [:raw, :read, :write, :binary])

    last_seq = scan_for_last_valid_seq(file, state.wal_path)
    size = file_size(state.wal_path)

    Metrics.put(state.metrics, :wal_seq, last_seq)
    Metrics.put(state.metrics, :wal_size_bytes, size)

    # Reposition for appends.
    {:ok, _} = :file.position(file, :eof)

    # Boot-time approximation: surviving records are at least this old, so
    # the age trigger under-fires by at most one boot-to-sweep interval.
    oldest = if size > 0, do: :erlang.monotonic_time(:millisecond)

    # Restart re-drive (effectively-once): every durable effect record above
    # the flush cursor re-enters the outbox queue with identical identity —
    # a stale cursor widens the re-drive (sink dedup absorbs), never skips.
    dedup = Outbox.read(state.segment_id)
    {redrive, max_consumed} = scan_effect_records_above(file, dedup.outbox_flushed_seq)

    if is_integer(max_consumed) do
      {rc, _sh, _pk} = state.segment_id
      InputLog.record_consumed(rc, state.segment_id, max_consumed)
    end

    state = %{
      state
      | file_handle: file,
        last_persisted_seq: last_seq,
        oldest_unsnapshotted_at: oldest,
        outbox_flushed_seq: dedup.outbox_flushed_seq,
        outbox_epoch: dedup.epoch,
        outbox_durable_seq: last_seq,
        pending_outbox: :queue.from_list(redrive),
        outbox_last_seq: redrive_last_seq(redrive, dedup.outbox_flushed_seq)
    }

    {:noreply, try_dispatch_outbox(state)}
  end

  # Seed the producing-liveness cursor from what the re-drive owes. Starting it at
  # -1 would make the first re-driven record read as a jump from nothing, which the
  # classifier would see as an outbox that suddenly grew.
  defp redrive_last_seq(redrive, flushed_seq) do
    redrive
    |> Enum.filter(&(&1.outputs != []))
    |> Enum.reduce(flushed_seq, fn entry, acc -> max(acc, entry.seq) end)
  end

  @impl true
  def handle_call({:append, seq, header, change_bytes, executor_id}, _from, state) do
    # Every received Change frame — accepted, deduped, realigned, or rejected
    # as a gap — proves the SDK is still emitting, which distinguishes a
    # stalled Segment from a genuinely idle one for the watchdog.
    now = :erlang.monotonic_time(:millisecond)
    state = %{state | last_change_seen_at: now}

    cond do
      stale_writer?(state, executor_id) ->
        # A takeover superseded this session — its channels were unbound
        # before the new owner was bound, so this frame is late in-flight
        # residue (typically a wedged channel flushing after the swap).
        # Refuse instead of realigning onto the stale lineage; the owner's
        # restore/resync handshake re-covers anything durable.
        {:reply, {:error, :stale_writer}, refuse_stale(state, executor_id)}

      realign?(state, executor_id, seq) ->
        # A *different* executor than the one that wrote the current epoch is
        # emitting a non-contiguous seq — the signature of an executor pod
        # restart whose post-rebuild seq base has diverged from our WAL. The
        # rebuilt executor is the live source of truth, so adopt its epoch
        # instead of dedup-dropping its Changes as stale retransmits, which
        # would freeze the WAL while the executor keeps producing.
        state
        |> realign_epoch(seq, executor_id, now)
        |> write_record(seq, header, change_bytes, executor_id, now)

      seq <= state.last_persisted_seq ->
        Metrics.add(state.metrics, :dedup_drops_total, 1)
        {:reply, :duplicate, state}

      seq != state.last_persisted_seq + 1 and state.last_persisted_seq == -1 ->
        # Empty-WAL gap: a cold takeover — this controller holds no baseline
        # for a Segment the live executor already has state for. Not a
        # protocol violation: shed the frame and pull a full-state baseline
        # via CompactSegment instead of disconnecting the SDK.
        {:reply, :awaiting_baseline, await_baseline(state, seq, executor_id)}

      seq != state.last_persisted_seq + 1 ->
        {:reply, {:error, {:gap, expected: state.last_persisted_seq + 1, got: seq}}, state}

      true ->
        write_record(state, seq, header, change_bytes, executor_id, now)
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

        # Durable drain: report the fsynced wire bytes back to each owning
        # stream's ledger — the only point that mints credit for written
        # records (append alone never does).
        new_state = flush_credit_pending(new_state)
        new_state = maybe_signal_backpressure(new_state, 0)

        # Effectively-once: everything appended is now durable — dispatch
        # the gated `:on_output` flush and advance the dedup file once per
        # batch (never per record).
        new_state = %{new_state | outbox_durable_seq: new_state.last_persisted_seq}
        new_state = try_dispatch_outbox(new_state)
        new_state = persist_dedup(new_state)
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:rewind_to, seq, epoch}, _from, state) do
    :ok = :file.close(state.file_handle)
    new_path = rewrite_keep_seq_lte(state.wal_path, seq)
    :ok = File.rename(new_path, state.wal_path)
    {:ok, file} = :file.open(state.wal_path, [:raw, :read, :write, :binary])
    {:ok, _} = :file.position(file, :eof)

    # Only the snapshots ABOVE the boundary die. The ones at or below it are
    # exactly the baselines a restore to `seq` needs — dropping them all, as a
    # realign does, would be a wipe, not a rewind.
    Enum.each(Registry.snapshot_paths(state.segment_id), fn {snap_seq, path} ->
      if snap_seq > seq, do: File.rm(path)
    end)

    state = flush_credit_pending(state)

    # Records at or below the boundary are untouched history: those already
    # flushed stay flushed (re-emitting them under the new epoch would be churn
    # that overwrites each row with itself). Only what the boundary left
    # un-flushed is still owed. Re-derive it from the rewritten file rather than
    # filtering the in-memory queue — the file is the authority on what survived,
    # and the same scan yields the consumed-offset the input-log watermark must
    # fall back to.
    flushed = min(state.outbox_flushed_seq, seq)
    {surviving, max_consumed} = scan_effect_records_above(file, flushed)
    rewind_input_watermark(state, max_consumed)

    size = file_size(state.wal_path)
    now = :erlang.monotonic_time(:millisecond)
    Metrics.put(state.metrics, :wal_size_bytes, size)
    Metrics.put(state.metrics, :wal_seq, seq)
    Metrics.put(state.metrics, :snapshot_seq, highest_snapshot_seq(state.segment_id))
    emit_rewind(state.segment_id, state.last_persisted_seq, seq, epoch)

    state = %{
      state
      | file_handle: file,
        last_persisted_seq: seq,
        last_advance_at: now,
        pending_bytes: 0,
        pending_since_us: nil,
        oldest_unsnapshotted_at: if(size > 0, do: now)
    }

    {:reply, :ok,
     bump_epoch(state, %{
       pending: :queue.from_list(surviving),
       durable_seq: seq,
       flushed_seq: flushed,
       epoch: epoch
     })}
  end

  def handle_call({:truncate_up_to, seq}, _from, state) do
    :ok = :file.close(state.file_handle)
    new_path = rewrite_keep_seq_gt(state.wal_path, seq)
    :ok = File.rename(new_path, state.wal_path)
    {:ok, file} = :file.open(state.wal_path, [:raw, :read, :write, :binary])
    {:ok, _} = :file.position(file, :eof)

    size = file_size(state.wal_path)
    Metrics.put(state.metrics, :wal_size_bytes, size)

    # Surviving tail records post-date the snapshot's baseline by at most the
    # compaction round-trip, so "now" is a safe age floor for the next trigger.
    oldest = if size > 0, do: :erlang.monotonic_time(:millisecond)

    state = flush_credit_pending(state)
    state = establish_baseline(state, seq)

    state = %{
      state
      | file_handle: file,
        pending_bytes: 0,
        pending_since_us: nil,
        oldest_unsnapshotted_at: oldest
    }

    # Rotation/compaction is one of the dedup file's advance points.
    {:reply, :ok, persist_dedup(state)}
  end

  def handle_call(:last_persisted_seq, _from, state) do
    {:reply, state.last_persisted_seq, state}
  end

  def handle_call(:outbox_epoch, _from, state) do
    {:reply, state.outbox_epoch, state}
  end

  def handle_call({:correction_baseline, target}, _from, state) do
    {:reply, scan_correction_baseline(state.file_handle, target), state}
  end

  def handle_call({:consumed_offset_at, seq}, _from, state) do
    {:reply, scan_consumed_at(state.file_handle, seq), state}
  end

  def handle_call(:outbox_status, _from, state) do
    status = %{
      last_persisted_seq: state.last_persisted_seq,
      outbox_flushed_seq: state.outbox_flushed_seq,
      epoch: state.outbox_epoch,
      pending: :queue.len(state.pending_outbox)
    }

    {:reply, status, state}
  end

  def handle_call(:stall_status, _from, state) do
    status = %{
      last_persisted_seq: state.last_persisted_seq,
      last_advance_at: state.last_advance_at,
      last_change_seen_at: state.last_change_seen_at,
      wal_size_bytes: Metrics.get(state.metrics, :wal_size_bytes),
      oldest_unsnapshotted_at: state.oldest_unsnapshotted_at,
      owning_executor: state.owning_executor,
      out_pending_seq: state.outbox_last_seq,
      outbox_flushed_seq: state.outbox_flushed_seq
    }

    {:reply, status, state}
  end

  @impl true
  def handle_cast({:append_async, seq, header, change_bytes, executor_id, credit_opts}, state) do
    state = %{state | credit_active?: Map.has_key?(credit_opts, :channel)}

    if over_hard_bound?(state) do
      release_credit(credit_opts)
      {:noreply, shed(state, seq, executor_id)}
    else
      do_append_async(
        seq,
        header,
        change_bytes,
        executor_id,
        %{state | shedding?: false},
        credit_opts
      )
    end
  end

  defp do_append_async(seq, header, change_bytes, executor_id, state, credit_opts) do
    case handle_call({:append, seq, header, change_bytes, executor_id}, nil, state) do
      {:reply, {:error, {:gap, expected: expected, got: got}}, new_state} ->
        release_credit(credit_opts)
        {rc, sh, pk} = new_state.segment_id

        :telemetry.execute(
          [:flezha, :state, :seq_gap],
          %{expected: expected, got: got},
          %{operative: rc, operation: sh, key: pk}
        )

        # Resync (a RestoreInitState handshake / WAL replay) runs in a separate
        # process so it never blocks this writer's append queue — and so the
        # test resync fun may re-enter `append/5` without self-deadlock.
        segment_id = new_state.segment_id

        spawn(fn ->
          Recovery.resync(segment_id, executor_id: executor_id, reason: :seq_gap)
        end)

        {:noreply, new_state}

      {:reply, :ok, new_state} ->
        {:noreply, track_credit(new_state, credit_opts)}

      {:reply, _result, new_state} ->
        release_credit(credit_opts)
        {:noreply, new_state}
    end
  end

  # Overwatch was not yet resolvable when durable outbox entries arrived
  # (cold-boot re-drive before the operative bound): retry the gated flush.
  @impl true
  def handle_info(:outbox_retry, state) do
    before = state.outbox_flushed_seq
    state = try_dispatch_outbox(%{state | outbox_retry_timer: nil})
    state = if state.outbox_flushed_seq != before, do: persist_dedup(state), else: state
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{file_handle: file} = state) when not is_nil(file) do
    _ = :file.datasync(file)
    _ = :file.close(file)
    _ = flush_credit_pending(state)
    _ = persist_dedup(state)
    :ok
  end

  def terminate(_reason, state) do
    _ = flush_credit_pending(state)
    :ok
  end

  # ---------- Append / realign ----------

  defp write_record(state, seq, header, change_bytes, executor_id, now) do
    record = encode_record(seq, header, change_bytes)
    :ok = :file.write(state.file_handle, record)
    size = byte_size(record)

    Metrics.add(state.metrics, :persisted_changes_total, 1)
    Metrics.add(state.metrics, :persisted_bytes_total, size)
    Metrics.put(state.metrics, :wal_seq, seq)
    Metrics.add(state.metrics, :wal_size_bytes, size)

    # The WAL append is the only per-record durable write; the dedup file
    # is a rebuildable cache advanced at fsync-batch cadence (and repaired
    # from the WAL scan on boot).
    state = maybe_queue_effect(state, seq, header, change_bytes)

    FsyncBatcher.mark_dirty(state.segment_id)

    new_pending = state.pending_bytes + size

    new_state = %{
      state
      | last_persisted_seq: seq,
        last_advance_at: now,
        owning_executor: executor_id || state.owning_executor,
        pending_bytes: new_pending,
        pending_since_us: state.pending_since_us || System.monotonic_time(:microsecond),
        oldest_unsnapshotted_at: state.oldest_unsnapshotted_at || now
    }

    new_state = maybe_signal_backpressure(new_state, new_pending)

    {:reply, :ok, new_state}
  end

  # ---------- Effectively-once outbox (fsync-gated flush) ----------

  # Queue an effect record's outputs for the post-fsync dispatch. Plain
  # change records pay one header-byte check and nothing else.
  defp maybe_queue_effect(state, seq, header, body) do
    if Wire.effect_record_header?(header) do
      case Wire.decode_effect_body(body) do
        {:ok, decoded} ->
          entry = %{
            seq: seq,
            outputs: decoded.outputs,
            consumed_offset: decoded.consumed_offset,
            dlq: decoded.dlq
          }

          last = if decoded.outputs == [], do: state.outbox_last_seq, else: seq

          %{
            state
            | pending_outbox: :queue.in(entry, state.pending_outbox),
              outbox_last_seq: max(state.outbox_last_seq, last)
          }

        {:error, reason} ->
          {rc, sh, pk} = state.segment_id

          Logger.warning(
            "[wal] effect record body undecodable segment=#{rc}/#{sh}/#{pk} " <>
              "seq=#{seq}: #{inspect(reason)} — outputs not flushed"
          )

          state
      end
    else
      state
    end
  end

  # Dispatch every leading durable entry, in seq order, to the operative's
  # `:on_output` chain with the durable identity `(flz.segment, outbox_seq,
  # epoch)`. Undispatchable (Overwatch not yet bound) → bounded retry timer;
  # the queue is bounded by the credit window like the un-fsynced buffer.
  defp try_dispatch_outbox(state) do
    case :queue.peek(state.pending_outbox) do
      :empty ->
        state

      {:value, %{seq: seq}} when seq > state.outbox_durable_seq ->
        state

      {:value, _entry} ->
        {rc, _sh, _pk} = state.segment_id

        case ClassRegistry.overwatch_pid(rc) do
          {:ok, pid} -> dispatch_outbox_entries(state, pid)
          :error -> schedule_outbox_retry(state)
        end
    end
  end

  defp dispatch_outbox_entries(state, pid) do
    case :queue.out(state.pending_outbox) do
      {{:value, %{seq: seq} = entry}, rest} when seq <= state.outbox_durable_seq ->
        dispatch_one_effect(state, pid, entry)

        state = %{
          state
          | pending_outbox: rest,
            outbox_flushed_seq: entry.seq
        }

        dispatch_outbox_entries(state, pid)

      _ ->
        state
    end
  end

  defp dispatch_one_effect(state, pid, entry) do
    {rc, sh, pk} = state.segment_id

    entry.outputs
    |> Enum.with_index()
    |> Enum.each(fn {output, ordinal} ->
      stamps =
        Wire.output_stamps(%{
          segment: state.segment_id,
          seq: "#{entry.seq}.#{ordinal}",
          epoch: state.outbox_epoch,
          channel: output.channel,
          key: output.key
        })

      metadata = Map.merge(Map.new(output.metadata), stamps)
      GenServer.cast(pid, {:io_out, output.payload, metadata})
    end)

    # Materialize a dead-letter park BEFORE the consumed-offset advance:
    # the watermark move can truncate the poison input out of the
    # input-log, so the DLQ store entry must be durable first (synchronous
    # — the append is fsynced and idempotent, so a boot re-drive of the
    # same record is a no-op). Parks are rare; the hot path pays one nil
    # check.
    case Map.get(entry, :dlq) do
      nil ->
        :ok

      dlq ->
        {:ok, _ref} =
          Flezha.Dlq.Store.append(%{
            operative_class: rc,
            operation_id: sh,
            segment_key: pk,
            channel: dlq.channel,
            origin_offset: entry.consumed_offset,
            error: dlq.error,
            attempts: dlq.attempts,
            payload: dlq.payload
          })
    end

    # Durable consumed-offset: advances the input-log min-watermark.
    if is_integer(entry.consumed_offset) do
      InputLog.record_consumed(rc, state.segment_id, entry.consumed_offset)
    end
  end

  defp schedule_outbox_retry(%{outbox_retry_timer: nil} = state) do
    ref = Process.send_after(self(), :outbox_retry, 1_000)
    %{state | outbox_retry_timer: ref}
  end

  defp schedule_outbox_retry(state), do: state

  # Advance the dedup file (persistence watermark + flush cursor + epoch)
  # with one atomic tmp+rename — at most once per fsync batch. The reserved
  # per-instance identities (op-journal, durable inbox) skip it: neither carries
  # effect records, and the dedup side-file would register a phantom Segment
  # with `Registry.scan_dir/1`.
  defp persist_dedup(%{segment_id: {_, _, key}} = state) when key in @reserved_segment_keys,
    do: state

  defp persist_dedup(state) do
    _ =
      Outbox.write(state.segment_id, %{
        last_persisted_seq: max(state.last_persisted_seq, 0),
        outbox_flushed_seq: state.outbox_flushed_seq,
        epoch: state.outbox_epoch
      })

    state
  end

  # Only a frame from a *different* session than the epoch owner consults the
  # takeover fence; the owning session short-circuits on the first comparison
  # (already made today for realign), so its append path gains no per-record
  # work.
  defp stale_writer?(state, executor_id) do
    is_binary(executor_id) and executor_id != state.owning_executor and
      FleetManager.superseded_session?(executor_id)
  end

  # One `stale_writer_refused` event per superseded session per writer
  # lifetime (sessions are never reused, so the episode never re-opens);
  # every refusal increments the counter.
  defp refuse_stale(state, executor_id) do
    Metrics.add(state.metrics, :stale_writer_refused_total, 1)

    if MapSet.member?(state.refused_sessions, executor_id) do
      state
    else
      {rc, sh, pk} = state.segment_id

      :telemetry.execute(
        [:flezha, :state, :stale_writer_refused],
        %{count: 1},
        %{operative: rc, operation: sh, key: pk, session: executor_id}
      )

      %{state | refused_sessions: MapSet.put(state.refused_sessions, executor_id)}
    end
  end

  # An executor restart is the only way a Change's seq can diverge under a *new*
  # executor id: within one executor's lifetime the seq is monotonic, so a low
  # seq from the same executor is a legitimate retransmit (deduped, not
  # realigned). `owning_executor == nil` (no Change seen since boot) defers to
  # the normal dedup/gap/append path so a controller restart still re-seeds the
  # alive executor via `RestoreInitState` instead of discarding the WAL.
  defp realign?(state, executor_id, seq) do
    is_binary(executor_id) and is_binary(state.owning_executor) and
      executor_id != state.owning_executor and seq != state.last_persisted_seq + 1
  end

  # Discard the stale pre-restart epoch so a later `RestoreInitState` ships only
  # the new executor's history: empty the WAL file and drop its snapshots, then
  # reset the seq baseline so the incoming `seq` appends contiguously.
  defp realign_epoch(state, seq, executor_id, now) do
    # The truncated epoch's un-fsynced bytes no longer occupy the buffer;
    # release them to the stream ledger so the window is not leaked.
    state = flush_credit_pending(state)
    {:ok, _} = :file.position(state.file_handle, 0)
    :ok = :file.truncate(state.file_handle)

    Enum.each(Registry.snapshot_paths(state.segment_id), fn {_seq, path} -> File.rm(path) end)

    emit_realign(state.segment_id, state.last_persisted_seq, seq, executor_id)

    Metrics.put(state.metrics, :wal_size_bytes, 0)
    Metrics.put(state.metrics, :snapshot_seq, 0)

    # The stale lineage's undispatched outputs die with its records — the
    # realigned executor's replay regenerates them under the new epoch.
    state = %{
      state
      | last_persisted_seq: seq - 1,
        last_advance_at: now,
        owning_executor: executor_id,
        pending_bytes: 0,
        pending_since_us: nil,
        oldest_unsnapshotted_at: nil
    }

    # A realign discards the whole lineage, so nothing in the new one has been
    # flushed and nothing is queued.
    bump_epoch(state, %{
      pending: :queue.new(),
      durable_seq: seq - 1,
      flushed_seq: -1,
      epoch: state.outbox_epoch + 1
    })
  end

  # The half a realign and a rewind share: re-seat the outbox cursors on the new
  # lineage and persist the epoch bump **synchronously**. That `persist_dedup`
  # is the commit point of both operations — it is what stops an output
  # regenerated above the boundary from colliding with its own pre-boundary
  # delivery, because the two now key under different epochs.
  #
  # `epoch` is passed rather than derived so a caller resuming an interrupted
  # boundary re-asserts the epoch it already committed to instead of bumping a
  # second time.
  defp bump_epoch(state, %{pending: pending, durable_seq: durable, flushed_seq: flushed, epoch: e}) do
    state = %{
      state
      | pending_outbox: pending,
        outbox_durable_seq: durable,
        outbox_flushed_seq: flushed,
        outbox_epoch: e
    }

    persist_dedup(state)
  end

  # Cold-takeover shed: no baseline on this controller, so the frame can't
  # append. Count it and, once per episode, drive a CompactSegment pull via
  # `Recovery.resync` (routes to the pull because the Segment has no
  # persisted state). The pulled snapshot encodes the shed deltas' effects.
  defp await_baseline(state, seq, executor_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    if state.awaiting_baseline? do
      state
    else
      {rc, sh, pk} = state.segment_id

      :telemetry.execute(
        [:flezha, :state, :baseline_seed],
        %{first_seq: seq},
        %{operative: rc, operation: sh, key: pk, executor: executor_id}
      )

      segment_id = state.segment_id

      spawn(fn ->
        Recovery.resync(segment_id, executor_id: executor_id, reason: :baseline_seed)
      end)

      %{state | awaiting_baseline?: true}
    end
  end

  # Baseline established: the pulled snapshot's seq supersedes the shed
  # deltas, so adopt it and let the executor's next delta append contiguously.
  defp establish_baseline(%{awaiting_baseline?: true, last_persisted_seq: last} = state, seq)
       when seq > last do
    now = :erlang.monotonic_time(:millisecond)
    Metrics.put(state.metrics, :wal_seq, seq)
    state = %{state | awaiting_baseline?: false, last_persisted_seq: seq, last_advance_at: now}
    persist_dedup(state)
  end

  defp establish_baseline(state, _seq), do: %{state | awaiting_baseline?: false}

  defp emit_realign({rc, sh, pk}, from_seq, to_seq, executor_id) do
    :telemetry.execute(
      [:flezha, :state, :wal_realign],
      %{from_seq: from_seq, to_seq: to_seq},
      %{operative: rc, operation: sh, key: pk, executor: executor_id}
    )
  end

  # Walk forward to the first record at or after `target`; everything before it
  # is the baseline. A record with no event time (a plain change record from
  # pre-tier history) belongs to the baseline — it cannot be placed on the event
  # -time axis, so it cannot be said to come after the target.
  defp scan_correction_baseline(device, target) do
    # `{-1, -1}` = no baseline seen yet; a first record already at or after the
    # target means the rewind would have to go below the retained history.
    fold_records(device, {-1, -1}, fn {seq, header, body}, {seq_acc, offset_acc} ->
      case Wire.decode_effect_record(header, body) do
        %{event_time: event_time} when event_time >= target ->
          {:halt, baseline_result(seq_acc, offset_acc)}

        effect ->
          {:cont, {seq, max(offset_acc, consumed_offset_of(effect))}}
      end
    end)
    |> case do
      {:ok, _} = ok -> ok
      {:error, _} = err -> err
      # The fold ran off the end: nothing is at or after the target, so the input
      # is not actually late for this Segment.
      {_seq, _offset} -> {:error, :nothing_to_correct}
    end
  end

  defp baseline_result(seq, _offset) when seq < 0, do: {:error, :nothing_to_correct}
  defp baseline_result(seq, offset), do: {:ok, %{seq: seq, consumed_offset: offset}}

  defp scan_consumed_at(device, upto) do
    fold_records(device, -1, fn {seq, header, body}, acc ->
      if seq > upto,
        do: {:halt, acc},
        else: {:cont, max(acc, consumed_offset_of(Wire.decode_effect_record(header, body)))}
    end)
  end

  # `-1` for a plain change record, or an effect record that consumed nothing.
  defp consumed_offset_of(nil), do: -1
  defp consumed_offset_of(%{consumed_offset: nil}), do: -1
  defp consumed_offset_of(%{consumed_offset: offset}), do: offset

  defp emit_rewind({rc, sh, pk}, from_seq, to_seq, epoch) do
    :telemetry.execute(
      [:flezha, :state, :wal_rewind],
      %{from_seq: from_seq, to_seq: to_seq},
      %{operative: rc, operation: sh, key: pk, epoch: epoch}
    )
  end

  defp highest_snapshot_seq(segment_id) do
    case Registry.snapshot_paths(segment_id) do
      [] -> 0
      paths -> paths |> List.last() |> elem(0)
    end
  end

  # The rewound records are the ones that reported this Segment's consumed
  # offsets, so the input-log watermark it advanced is now a claim about inputs
  # nothing has consumed. Lower it, or the next retention sweep truncates away
  # the very inputs the correction is about to replay.
  defp rewind_input_watermark(state, max_consumed) do
    {operative_class, _, _} = state.segment_id
    InputLog.rewind_consumed(operative_class, state.segment_id, max_consumed || -1)
  end

  # ---------- Scan / rewrite helpers ----------

  @doc """
  Reduce over a WAL file's framed records, oldest first.

  `fun.({seq, header, body}, acc)` returns `{:cont, acc}` or `{:halt, acc}`; the
  walk also stops at the first undecodable record, which is the torn-tail
  contract every reader here shares.

  The one place the `decode_record/2` recursion lives. The framing is shared by
  the Segment WAL, the input log, the op-journal and the retention rewriter, and
  each of them used to carry its own copy of this loop.
  """
  @spec fold_records(:file.io_device(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                                {:cont, acc} | {:halt, acc})) :: acc
        when acc: term()
  def fold_records(device, acc, fun), do: fold_records(device, 0, acc, fun)

  defp fold_records(device, position, acc, fun) do
    case decode_record(device, position) do
      {:ok, seq, header, body, consumed} ->
        case fun.({seq, header, body}, acc) do
          {:cont, acc} -> fold_records(device, position + consumed, acc, fun)
          {:halt, acc} -> acc
        end

      {:error, _} ->
        acc
    end
  end

  @doc """
  Walk the WAL from offset 0, returning the seq of the last record
  whose crc validates. Truncates any partial-write tail past that point.

  Backward scan would require a second copy of the length field at the
  record tail; we use forward scan and rely on monotonic seqs for the
  "last valid record" answer. The cost is O(records-since-snapshot)
  at boot; acceptable because compaction keeps WALs short.
  """
  @spec scan_for_last_valid_seq(:file.io_device(), binary()) :: integer()
  def scan_for_last_valid_seq(device, _wal_path) do
    # `-1`, not `0`, for an empty WAL: the SDK's per-Segment seq is 0-based
    # (`change_seq` starts at 0), so a `0` floor makes the dedup guard
    # (`seq <= last_persisted_seq`) drop the very first change (seq 0) of every
    # operative. `-1` is the same "no records yet" sentinel `realign_epoch` uses.
    do_scan(device, 0, -1, 0)
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

  # Effect records above the flush cursor, oldest first — the restart
  # re-drive set (already durable, dispatched once the Overwatch resolves).
  # One pass over the WAL's effect records: the re-drive set (records above
  # the flush cursor, oldest first, already durable) plus the highest
  # consumed-offset across ALL effect records — the Segment's durable
  # input-log watermark, repopulated at open (the ETS is a rebuildable cache).
  defp scan_effect_records_above(device, cursor) do
    {acc, max_consumed} =
      fold_records(device, {[], nil}, fn {seq, header, body}, {acc, max_consumed} = unchanged ->
        case Wire.decode_effect_record(header, body) do
          nil ->
            {:cont, unchanged}

          effect ->
            entry = %{
              seq: seq,
              outputs: effect.outputs,
              consumed_offset: effect.consumed_offset,
              dlq: effect.dlq
            }

            acc = if seq > cursor, do: [entry | acc], else: acc
            {:cont, {acc, max_offset(max_consumed, effect.consumed_offset)}}
        end
      end)

    {Enum.reverse(acc), max_consumed}
  end

  defp max_offset(current, nil), do: current
  defp max_offset(nil, offset), do: offset
  defp max_offset(current, offset), do: max(current, offset)

  # Also reused by `Flezha.State.InputLog` for watermark truncation —
  # the input-log shares this record framing.
  @doc false
  def rewrite_keep_seq_gt(wal_path, keep_above_seq) do
    rewrite_keep(wal_path, &(&1 > keep_above_seq))
  end

  # Suffix-drop: keep the records at or below `seq`. The mirror image of
  # `rewrite_keep_seq_gt/2` — compaction drops the prefix a snapshot subsumes,
  # a correction drops the suffix a rewind invalidates.
  @doc false
  def rewrite_keep_seq_lte(wal_path, keep_below_seq) do
    rewrite_keep(wal_path, &(&1 <= keep_below_seq))
  end

  # Copy the records `keep?` accepts into `<path>.rewriting`, fsync, and return
  # the new path. The caller renames — the two-step is what makes a crash
  # mid-rewrite leave the original intact (`Registry.scan_dir/1` skips
  # `.rewriting` residue).
  defp rewrite_keep(wal_path, keep?) do
    {:ok, src} = :file.open(wal_path, [:raw, :read, :binary])
    new_path = wal_path <> ".rewriting"
    {:ok, dst} = :file.open(new_path, [:raw, :write, :binary])

    try do
      copy_keep(src, dst, keep?)
      :ok = :file.sync(dst)
    after
      _ = :file.close(src)
      _ = :file.close(dst)
    end

    new_path
  end

  defp copy_keep(src, dst, keep?) do
    fold_records(src, :ok, fn {seq, header, change_bytes}, :ok ->
      if keep?.(seq), do: :ok = :file.write(dst, encode_record(seq, header, change_bytes))
      {:cont, :ok}
    end)
  end

  # ---------- Hard bound (shed) ----------

  # Last-resort safety net above the soft watermark: cooperative SDKs throttle on
  # `SegmentMailboxStatus` first; only a slow disk or a non-cooperating SDK
  # reaches here. Covers un-fsynced bytes AND mailbox depth — a cast flood grows
  # the mailbox even when bytes-per-record is small.
  defp over_hard_bound?(state) do
    state.pending_bytes >= state.hard_watermark or
      message_queue_len() >= state.pending_msg_cap
  end

  defp message_queue_len() do
    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, n} -> n
      nil -> 0
    end
  end

  # Drop the Change without writing; the resulting seq gap drives the existing
  # resync handshake so the SDK (authoritative) re-sends. Resync is spawned once
  # per shed episode (on the false->true transition) to avoid a resync storm
  # under a sustained flood.
  defp shed(state, seq, executor_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    # A conformant credit-speaking sender can never reach the hard bound
    # (the stream ledger refuses the overrun first) — a shed here is a
    # protocol anomaly, not back-pressure.
    if state.credit_active? do
      {rc, sh, pk} = state.segment_id

      :telemetry.execute(
        [:flezha, :state, :credit_anomaly],
        %{count: 1},
        %{kind: :shed, operative: rc, operation: sh, key: pk, executor: executor_id}
      )
    end

    if state.shedding? do
      state
    else
      {rc, sh, pk} = state.segment_id

      :telemetry.execute(
        [:flezha, :state, :seq_gap],
        %{expected: state.last_persisted_seq + 1, got: seq},
        %{operative: rc, operation: sh, key: pk}
      )

      segment_id = state.segment_id
      spawn(fn -> Recovery.resync(segment_id, executor_id: executor_id, reason: :shed) end)
      %{state | shedding?: true}
    end
  end

  # ---------- Backpressure ----------

  defp maybe_signal_backpressure(%{backpressured?: false} = state, pending)
       when pending >= state.high_watermark do
    emit_backpressure(state, :full, pending)
    %{state | backpressured?: true}
  end

  defp maybe_signal_backpressure(%{backpressured?: true} = state, pending)
       when pending <= state.low_watermark do
    emit_backpressure(state, :open, pending)
    %{state | backpressured?: false}
  end

  defp maybe_signal_backpressure(state, _), do: state

  # Emit the telemetry event AND ship the wire signal. The telemetry
  # event is for in-process observers (metrics, debug); the wire signal
  # routes through the producing Operation's `OverflowPolicy` machinery on
  # the SDK side. A credit-speaking session gets no wire signal: its
  # back-pressure is carried entirely by the credit ledger (the sender parks
  # on window exhaustion), so only the telemetry event is emitted.
  defp emit_backpressure(state, status, pending) do
    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{pending_bytes: pending},
      %{segment_id: state.segment_id, status: status}
    )

    # Routing needs the owning executor (`nil` until the first Change lands,
    # in which case there is no session to signal).
    with false <- state.credit_active?,
         {rc, sh, pk} <- state.segment_id,
         executor_id when is_binary(executor_id) <- state.owning_executor do
      _ =
        Flezha.Operative.Overwatch.notify_segment_backpressure(
          {executor_id, rc, sh, pk},
          status
        )
    end

    :ok
  end

  # ---------- Credit-ledger attribution ----------

  # Accumulate a written record's wire bytes against the stream channel that
  # debited them; reported back on the next durable drain (fsync/truncate).
  defp track_credit(state, %{channel: ch, session: session, wire_size: n})
       when is_pid(ch) and n > 0 do
    %{state | credit_pending: Map.update(state.credit_pending, {ch, session}, n, &(&1 + n))}
  end

  defp track_credit(state, _), do: state

  # A Change that will never occupy the un-fsynced buffer (dedup drop, gap
  # rejection, shed): its debit is released immediately.
  defp release_credit(%{channel: ch, session: session, wire_size: n})
       when is_pid(ch) and n > 0 do
    send(ch, {:credit_drained, session, n})
    :ok
  end

  defp release_credit(_), do: :ok

  defp flush_credit_pending(%{credit_pending: cp} = state) when map_size(cp) == 0, do: state

  defp flush_credit_pending(state) do
    Enum.each(state.credit_pending, fn {{ch, session}, bytes} ->
      send(ch, {:credit_drained, session, bytes})
    end)

    %{state | credit_pending: %{}}
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

  # Hard in-memory bound, strictly above the soft watermark (default 4x) so
  # cooperative back-pressure takes effect before anything is shed.
  defp hard_watermark() do
    Application.get_env(:flezha, :wal_hard_watermark_bytes, high_watermark() * 4)
  end

  # Pending mailbox-depth cap: a cast flood can grow the mailbox even when each
  # record is small.
  defp pending_msg_cap() do
    Application.get_env(:flezha, :wal_pending_msg_cap, 5_000)
  end
end
