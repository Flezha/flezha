defmodule Flezha.State.WalWriter do
  @moduledoc """
  Per-Entity `GenServer` wrapping a single open WAL file handle.

  Each appended record is `<<seq::64-big, len::32-big, header::binary,
  change_bytes::binary, crc32::32-big>>` where `len = 16 + byte_size(change_bytes)`
  and `crc32 = :erlang.crc32(<<header::binary, change_bytes::binary>>)`.

  fsync is decoupled from append: the writer registers as dirty with
  `Flezha.State.FsyncBatcher` which drives the actual `:file.datasync/1`
  on its tick cadence. High-watermark crossings emit a telemetry event
  (`[:flezha, :state, :wal, :backpressure]`) that section-9 wiring picks
  up to ship the wire-level `EntityMailboxStatus`.
  """

  use GenServer, restart: :transient

  require Logger

  alias Flezha.Runner.FleetManager
  alias Flezha.Operative.ClassRegistry

  alias Flezha.State.{
    DurabilityTier,
    FsyncBatcher,
    InputLog,
    Metrics,
    Outbox,
    Paths,
    PersistenceResolver,
    Recovery,
    SeqIndex,
    Wire
  }

  @header_size 16
  @reservoir_capacity 1024
  @reserved_entity_keys Paths.reserved_entity_keys()

  defstruct [
    :entity_id,
    :file_handle,
    :wal_path,
    :last_persisted_seq,
    :last_advance_at,
    :last_change_seen_at,
    :owning_runner,
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
    :last_resync_at,
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
    # Dispatched but not yet confirmed by the sinks: the per-entry tallies
    # (`Flezha.State.Outbox` keys) and, for a stream writer, the dispatch-order
    # queue of `{seq, at}` the cursor drains in. The cursor may only reach a seq
    # once every earlier one has left this queue, which is what stops an
    # out-of-order confirmation from advancing it past unconfirmed output.
    :outbox_awaiting,
    :outbox_inflight,
    :outbox_inflight_max,
    :outbox_resettle_ms,
    # Highest seq of an OUTPUT-BEARING effect record queued. Producing-liveness
    # compares it against `outbox_flushed_seq` to see an outbox that accumulates
    # without draining. It must count only output-bearing records:
    # `outbox_durable_seq` advances on every fsynced record, so an Entity that
    # persists state but emits nothing would read as a frozen outbox forever.
    :outbox_last_seq,
    # A `Flezha.State.SeqIndex` over the file: a cut at either end finds its
    # boundary within one sample span and copies only the kept side.
    :seq_index,
    # Batched appends owed their reply: `{from, reply}` newest first, answered
    # in append order once the records are synced.
    :sync_waiters,
    # `header => seq` of every record still in the file, for a log whose
    # appends dedup on their header (a record log's barrier or inbox entry).
    # Built from the file on the first keyed append, never for a stream WAL.
    :header_index
  ]

  @type t :: %__MODULE__{
          entity_id: Paths.entity_id(),
          file_handle: :file.io_device(),
          wal_path: binary(),
          last_persisted_seq: integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          owning_runner: binary() | nil,
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
          last_resync_at: integer() | nil,
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
  Entity is *stalled* (vs idle) when Changes are still arriving but
  the persisted seq has stopped advancing. `wal_size_bytes` /
  `oldest_unsnapshotted_at` / `owning_runner` feed the Watchdog's
  size+age compaction triggers without a second disk scan.
  """
  @type stall_status :: %{
          last_persisted_seq: integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          wal_size_bytes: non_neg_integer(),
          oldest_unsnapshotted_at: integer() | nil,
          owning_runner: binary() | nil,
          out_pending_seq: integer(),
          outbox_flushed_seq: integer(),
          outbox_unconfirmed_ms: non_neg_integer()
        }

  # ---------- Public API ----------

  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id, name: via(entity_id))
  end

  @doc """
  Append a Change at `seq`. `runner_id` identifies the runner that emitted
  it; a Change from a *different* runner than the one that wrote the current
  epoch realigns the WAL to the new runner's seq base (runner-restart
  recovery — see `handle_call/3`) — unless a takeover superseded that session
  (`Flezha.Runner.FleetManager.superseded_session?/1`), in which case the
  frame is refused with `{:error, :stale_writer}` instead of realigning. Pass
  `nil` (the default) to disable epoch tracking — the single-runner / test
  path keeps the prior dedup/gap/append semantics.
  """
  @spec append(GenServer.server(), non_neg_integer(), binary(), binary(), binary() | nil) ::
          :ok | :duplicate | :awaiting_baseline | {:error, term()}
  def append(server, seq, header, change_bytes, runner_id \\ nil) do
    GenServer.call(server, {:append, seq, header, change_bytes, runner_id})
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
  def append_async(server, seq, header, change_bytes, runner_id \\ nil, credit_opts \\ %{}) do
    GenServer.cast(server, {:append_async, seq, header, change_bytes, runner_id, credit_opts})
  end

  @spec fsync(GenServer.server()) :: :ok
  def fsync(server), do: GenServer.call(server, :fsync)

  @doc "Fire-and-forget `fsync/1`: how the batcher's tick syncs every dirty writer at once."
  @spec fsync_async(GenServer.server()) :: :ok
  def fsync_async(server), do: GenServer.cast(server, :fsync)

  @doc """
  Append one record at the log's own next seq, answering the seq it was
  assigned once it is durable. Seq assignment and write are one call because
  they are one decision: two processes appending to the same log would
  otherwise read the same tail and collide, and the loser's record would be
  refused as a duplicate.

  `sync: :sync` (the default) fsyncs before answering. `sync: :batched` marks
  the writer dirty and answers when the batch that holds the record has synced
  — the `FsyncBatcher` tick, a prefix cut, or a clean close — so every append
  that lands in one tick shares one fsync, and the reply is never earlier than
  the sync. A writer that dies in between never answers, which fails the
  caller's call (or its `send_request`, see `append_next_async/6`).

  `dedup: true` refuses a record whose 16-byte header matches one still in the
  file, answering `{:duplicate, seq}` with the existing record's seq — the
  idempotency a re-sent barrier or a re-sent inbox message needs. The index is
  built from the file on the first keyed append and trimmed by every prefix
  cut, so a log that never dedups pays nothing.
  """
  @spec append_next(GenServer.server(), binary(), binary(), keyword()) ::
          {:ok, non_neg_integer()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def append_next(server, header, body, opts \\ []),
    do: GenServer.call(server, {:append_next, header, body, opts})

  @doc """
  `append_next/4` issued with `:gen_server.send_request/4` under `label` into
  `reqids`: the caller carries on and collects the answer — the same reply, or
  the writer's exit — with `:gen_server.check_response/3`. With
  `sync: :batched` the answer IS the durability token: it arrives when the
  batch has synced, never before.
  """
  @spec append_next_async(
          GenServer.server(),
          binary(),
          binary(),
          keyword(),
          :gen_server.request_id_collection(),
          term()
        ) :: :gen_server.request_id_collection()
  def append_next_async(server, header, body, opts, reqids, label),
    do: :gen_server.send_request(server, {:append_next, header, body, opts}, label, reqids)

  @doc """
  Drop the records at or below `seq`. `min_bytes: n` defers the file rewrite
  while fewer than `n` bytes lie below the cut — the records stay on disk until
  a later request finds enough of them, so a log cut at every turn pays its
  fsync'd rewrite once per `n` bytes rather than once per turn. A keyed log's
  dedup window releases the records either way.
  """
  @spec truncate_up_to(GenServer.server(), non_neg_integer(), keyword()) :: :ok
  def truncate_up_to(server, seq, opts \\ []),
    do: GenServer.call(server, {:truncate_up_to, seq, opts})

  @doc "Fire-and-forget `truncate_up_to/3`, for a cut whose caller has no use for the answer."
  @spec truncate_up_to_async(GenServer.server(), non_neg_integer(), keyword()) :: :ok
  def truncate_up_to_async(server, seq, opts \\ []),
    do: GenServer.cast(server, {:truncate_up_to, seq, opts})

  @doc """
  Rewind the Entity to `seq` under `epoch`: keep the records at or below it,
  drop the snapshots above it, and re-seat the outbox on the survivors.

  The non-destructive sibling of the realign wipe. A realign discards a stale
  runner's whole lineage; a rewind discards only the history above a boundary
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
  the input is not actually late for this Entity.
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

  @doc """
  Deliver one workflow transactional-outbox entry — its stamped `outputs`, to
  `overwatch` — under `key`, registering the entry as owed to the sinks so its
  `outbox_flushed` marker is appended once they confirm; `telemetry` is the
  `{event, measurements, metadata}` emitted with the delivery. An entry a
  previous flush already handed to the chain is not delivered again: the
  writer owns the awaiting set, so the check and the dispatch are one step in
  its loop. Fire-and-forget — the flush is journal-driven and re-entrant, so
  a caller never waits on this writer's sync.
  """
  @spec flush_outbox(GenServer.server(), Outbox.key(), [term()], pid(), {[atom()], map(), map()}) ::
          :ok
  def flush_outbox(server, key, outputs, overwatch, telemetry),
    do: GenServer.cast(server, {:outbox_flush, key, outputs, overwatch, telemetry})

  @doc "Live effectively-once outbox status (`flz state outbox`)."
  @spec outbox_status(GenServer.server(), timeout()) :: map()
  def outbox_status(server, timeout \\ 5_000),
    do: GenServer.call(server, :outbox_status, timeout)

  @spec stall_status(GenServer.server()) :: stall_status()
  def stall_status(server), do: GenServer.call(server, :stall_status)

  @spec close(GenServer.server()) :: :ok
  def close(server), do: GenServer.stop(server, :normal)

  defp via(entity_id) do
    {:via, Elixir.Registry, {Flezha.State.Supervisor.registry_name(), entity_id}}
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
  def init(entity_id) do
    Process.flag(:trap_exit, true)
    now = now_ms()

    # Disk I/O (mkdir/open/scan/seek) is deferred to `handle_continue/2` so this
    # `init` returns instantly: the process is already Registry-registered, so a
    # `Supervisor.find_or_start/1` from a StreamChannel never blocks on disk. The
    # continue runs before any queued append, so appends queue in this writer's
    # mailbox — never the channel's. `-1` is the "no records yet" seq sentinel.
    state = %__MODULE__{
      entity_id: entity_id,
      file_handle: nil,
      wal_path: Paths.wal_path(entity_id),
      last_persisted_seq: -1,
      last_advance_at: now,
      last_change_seen_at: now,
      owning_runner: nil,
      pending_bytes: 0,
      pending_since_us: nil,
      high_watermark: high_watermark(),
      low_watermark: low_watermark(),
      hard_watermark: hard_watermark(),
      pending_msg_cap: pending_msg_cap(),
      metrics: Metrics.for_entity(entity_id),
      latency_reservoir: :queue.new(),
      backpressured?: false,
      shedding?: false,
      last_resync_at: nil,
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
      outbox_awaiting: %{},
      outbox_inflight: :queue.new(),
      outbox_inflight_max: outbox_inflight_max(),
      outbox_resettle_ms: outbox_resettle_ms(),
      outbox_last_seq: -1,
      seq_index: SeqIndex.new(),
      sync_waiters: [],
      header_index: nil
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
    oldest = if size > 0, do: now_ms()

    # Restart re-drive (effectively-once): every durable effect record above
    # the flush cursor re-enters the outbox queue with identical identity —
    # a stale cursor widens the re-drive (sink dedup absorbs), never skips.
    dedup = Outbox.read(state.entity_id)
    {redrive, max_consumed} = scan_effect_records_above(file, dedup.outbox_flushed_seq)

    if is_integer(max_consumed) do
      {rc, _sh, _pk} = state.entity_id
      InputLog.record_consumed(rc, state.entity_id, max_consumed)
    end

    state = %{
      state
      | file_handle: file,
        seq_index: SeqIndex.new(size),
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
  def handle_call({:append, seq, header, change_bytes, runner_id}, _from, state) do
    # Every received Change frame — accepted, deduped, realigned, or rejected
    # as a gap — proves the SDK is still emitting, which distinguishes a
    # stalled Entity from a genuinely idle one for the watchdog.
    now = now_ms()
    state = %{state | last_change_seen_at: now}

    cond do
      stale_writer?(state, runner_id) ->
        # A takeover superseded this session — its channels were unbound
        # before the new owner was bound, so this frame is late in-flight
        # residue (typically a wedged channel flushing after the swap).
        # Refuse instead of realigning onto the stale lineage; the owner's
        # restore/resync handshake re-covers anything durable.
        {:reply, {:error, :stale_writer}, refuse_stale(state, runner_id)}

      realign?(state, runner_id, seq) ->
        # A *different* runner than the one that wrote the current epoch is
        # emitting a non-contiguous seq — the signature of a runner pod
        # restart whose post-rebuild seq base has diverged from our WAL. The
        # rebuilt runner is the live source of truth, so adopt its epoch
        # instead of dedup-dropping its Changes as stale retransmits, which
        # would freeze the WAL while the runner keeps producing.
        state
        |> realign_epoch(seq, runner_id, now)
        |> write_record(seq, header, change_bytes, runner_id, now)

      seq <= state.last_persisted_seq ->
        Metrics.add(state.metrics, :dedup_drops_total, 1)
        {:reply, :duplicate, state}

      seq != state.last_persisted_seq + 1 and state.last_persisted_seq == -1 ->
        # Empty-WAL gap: a cold takeover — this controller holds no baseline
        # for an Entity the live runner already has state for. Not a
        # protocol violation: shed the frame and pull a full-state baseline
        # via CompactEntity instead of disconnecting the SDK.
        {:reply, :awaiting_baseline, await_baseline(state, seq, runner_id)}

      seq != state.last_persisted_seq + 1 ->
        {:reply, {:error, {:gap, expected: state.last_persisted_seq + 1, got: seq}}, state}

      true ->
        write_record(state, seq, header, change_bytes, runner_id, now)
    end
  end

  def handle_call({:append_next, header, body, opts}, from, state) do
    dedup? = Keyword.get(opts, :dedup, false)
    state = if dedup?, do: ensure_header_index(state), else: state

    case dedup? and Map.fetch(state.header_index, header) do
      {:ok, seq} ->
        answer(state, from, {:duplicate, seq}, opts)

      _ ->
        seq = state.last_persisted_seq + 1

        case handle_call({:append, seq, header, body, nil}, from, state) do
          {:reply, :ok, state} -> answer(state, from, {:ok, seq}, opts)
          {:reply, error, state} -> {:reply, error, state}
        end
    end
  end

  def handle_call(:fsync, _from, state), do: {:reply, :ok, do_fsync(state)}

  def handle_call({:rewind_to, seq, epoch}, _from, state) do
    {:ok, file, seq_index} =
      cut_above(state.wal_path, state.file_handle, state.seq_index, seq + 1)

    # Only the snapshots ABOVE the boundary die. The ones at or below it are
    # exactly the baselines a restore to `seq` needs — dropping them all, as a
    # realign does, would be a wipe, not a rewind.
    Enum.each(Paths.snapshot_paths(state.entity_id), fn {snap_seq, path} ->
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
    now = now_ms()
    Metrics.put(state.metrics, :wal_size_bytes, size)
    Metrics.put(state.metrics, :wal_seq, seq)
    Metrics.put(state.metrics, :snapshot_seq, highest_snapshot_seq(state.entity_id))
    emit_rewind(state.entity_id, state.last_persisted_seq, seq, epoch)

    state = %{
      state
      | file_handle: file,
        seq_index: seq_index,
        header_index: nil,
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

  # Drop the records at or below `seq` (compaction's prefix, a retention
  # cut). The cut runs in this loop — under the effectively-once tier every
  # output rides it — so it costs the tail, never the file. A cut with nothing
  # below it (a retention tick on a writer compaction already cut), or with too
  # little below it to be worth a rewrite yet, answers at once; only a writer
  # awaiting its baseline still records the seq, which is what establishes the
  # baseline. The dedup window releases the records in every case.
  def handle_call({:truncate_up_to, seq, opts}, _from, state) do
    min_bytes = Keyword.get(opts, :min_bytes, 0)
    state = trim_header_index(state, seq)

    case cut_below(state.wal_path, state.file_handle, state.seq_index, seq + 1, min_bytes) do
      {:ok, _cut, file, seq_index} ->
        {:reply, :ok, after_prefix_cut(%{state | file_handle: file, seq_index: seq_index}, seq)}

      :none when state.awaiting_baseline? ->
        {:reply, :ok, persist_dedup(establish_baseline(state, seq))}

      :none ->
        {:reply, :ok, state}
    end
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
      pending: :queue.len(state.pending_outbox),
      awaiting: map_size(state.outbox_awaiting)
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
      owning_runner: state.owning_runner,
      out_pending_seq: state.outbox_last_seq,
      outbox_flushed_seq: state.outbox_flushed_seq,
      outbox_unconfirmed_ms: unconfirmed_age_ms(state)
    }

    {:reply, status, state}
  end

  # The append's reply under its sync mode. Synchronous: fsync now. Batched: owed
  # until the next sync — unless nothing is un-synced (a duplicate against a
  # clean writer), in which case the record it names is durable already.
  defp answer(state, from, reply, opts) do
    case Keyword.get(opts, :sync, :sync) do
      :sync -> {:reply, reply, do_fsync(state)}
      :batched when state.pending_since_us == nil -> {:reply, reply, state}
      :batched -> {:noreply, %{state | sync_waiters: [{from, reply} | state.sync_waiters]}}
    end
  end

  @impl true
  def handle_cast({:append_async, seq, header, change_bytes, runner_id, credit_opts}, state) do
    state = %{state | credit_active?: Map.has_key?(credit_opts, :channel)}

    if over_hard_bound?(state) do
      release_credit(credit_opts)
      {:noreply, shed(state, seq, runner_id)}
    else
      do_append_async(
        seq,
        header,
        change_bytes,
        runner_id,
        %{state | shedding?: false},
        credit_opts
      )
    end
  end

  def handle_cast(:fsync, state), do: {:noreply, do_fsync(state)}

  def handle_cast({:truncate_up_to, seq, opts}, state) do
    {:reply, :ok, state} = handle_call({:truncate_up_to, seq, opts}, nil, state)
    {:noreply, state}
  end

  # The workflow flush: the entry is registered here, in the writer that owns
  # the awaiting set, and dispatched from here (a stream writer registers its
  # own as it dispatches). An entry already awaiting is one a previous flush
  # handed to the chain and whose marker is merely waiting on the sinks.
  def handle_cast({:outbox_flush, key, outputs, overwatch, {event, measurements, meta}}, state) do
    if Map.has_key?(state.outbox_awaiting, key) do
      {:noreply, state}
    else
      state = register_entry(state, key, length(outputs), nil)
      Outbox.dispatch(overwatch, outputs)
      :telemetry.execute(event, measurements, meta)
      {:noreply, state}
    end
  end

  # Delivery confirmation (`Flezha.State.Outbox`): sinks report against the entry
  # the flush registered, whichever flush registered it.

  def handle_cast({:outbox_hold, key}, state),
    do: {:noreply, tally(state, key, &%{&1 | holds: &1.holds + 1})}

  def handle_cast({:outbox_settled, key, holds}, state),
    do: {:noreply, settled(state, {key, holds})}

  def handle_cast({:outbox_settled_many, entries}, state),
    do: {:noreply, Enum.reduce(entries, state, &settled(&2, &1))}

  def handle_cast({:outbox_confirmed, key}, state),
    do: {:noreply, confirmed(state, key)}

  def handle_cast({:outbox_confirmed_many, keys}, state),
    do: {:noreply, Enum.reduce(keys, state, &confirmed(&2, &1))}

  defp do_append_async(seq, header, change_bytes, runner_id, state, credit_opts) do
    case handle_call({:append, seq, header, change_bytes, runner_id}, nil, state) do
      {:reply, {:error, {:gap, expected: expected, got: got}}, new_state} ->
        release_credit(credit_opts)
        emit_seq_gap(new_state.entity_id, expected, got)
        {:noreply, resync_rate_limited(new_state, runner_id, :seq_gap)}

      {:reply, :ok, new_state} ->
        # A contiguous append is evidence the gap healed, so the episode ends
        # and the next gap may resync immediately.
        {:noreply, track_credit(%{new_state | last_resync_at: nil}, credit_opts)}

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
    state = resettle_outbox(%{state | outbox_retry_timer: nil})
    state = try_dispatch_outbox(state)
    state = if state.outbox_flushed_seq != before, do: persist_dedup(state), else: state
    state = if state.outbox_awaiting == %{}, do: state, else: schedule_outbox_retry(state)
    {:noreply, state}
  end

  def handle_info({:outbox_marked, key}, state),
    do: {:noreply, %{state | outbox_awaiting: Map.delete(state.outbox_awaiting, key)}}

  def handle_info(_msg, state), do: {:noreply, state}

  # A clean close syncs what it holds and answers the batched appends it owes;
  # a crash answers nobody, so a waiter fails instead of reading as durable.
  @impl true
  def terminate(reason, %{file_handle: file} = state) when not is_nil(file) do
    synced = :file.datasync(file)
    _ = :file.close(file)
    _ = flush_credit_pending(state)
    _ = persist_dedup(state)
    if synced == :ok and clean_exit?(reason), do: reply_synced(state)
    :ok
  end

  def terminate(_reason, state) do
    _ = flush_credit_pending(state)
    :ok
  end

  defp clean_exit?(reason), do: reason in [:normal, :shutdown] or match?({:shutdown, _}, reason)

  defp do_fsync(state) do
    t0 = System.monotonic_time(:microsecond)
    :ok = :file.datasync(state.file_handle)
    t1 = System.monotonic_time(:microsecond)
    Metrics.add(state.metrics, :fsync_total, 1)

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
        new_state |> persist_dedup() |> reply_synced()
    end
  end

  # Everything appended is durable: answer the batched appends, oldest first.
  defp reply_synced(%{sync_waiters: []} = state), do: state

  defp reply_synced(state) do
    state.sync_waiters
    |> Enum.reverse()
    |> Enum.each(fn {from, reply} -> GenServer.reply(from, reply) end)

    %{state | sync_waiters: []}
  end

  # ---------- Header dedup ----------

  # `header => seq` for every record in the file, built once per writer
  # lifetime by the first keyed append. The file is the set a keyed log dedups
  # against: a prefix cut trims it, a rewrite of the lineage drops it for a
  # rebuild.
  defp ensure_header_index(%{header_index: nil} = state) do
    index =
      fold_records(state.file_handle, %{}, fn {seq, header, _body}, acc ->
        {:cont, Map.put(acc, header, seq)}
      end)

    %{state | header_index: index}
  end

  defp ensure_header_index(state), do: state

  defp note_header(%{header_index: nil} = state, _header, _seq), do: state

  defp note_header(state, header, seq),
    do: %{state | header_index: Map.put(state.header_index, header, seq)}

  defp trim_header_index(%{header_index: nil} = state, _seq), do: state

  defp trim_header_index(state, seq),
    do: %{state | header_index: Map.reject(state.header_index, fn {_, s} -> s <= seq end)}

  # The bookkeeping a prefix cut owes once the file is shorter: the rewrite
  # fsynced the kept tail, so everything pending is durable.
  defp after_prefix_cut(state, seq) do
    size = file_size(state.wal_path)
    Metrics.put(state.metrics, :wal_size_bytes, size)

    # Surviving tail records post-date the snapshot's baseline by at most the
    # compaction round-trip, so "now" is a safe age floor for the next trigger.
    oldest = if size > 0, do: now_ms()

    state = flush_credit_pending(state)
    state = establish_baseline(state, seq)

    state = %{state | pending_bytes: 0, pending_since_us: nil, oldest_unsnapshotted_at: oldest}

    # Rotation/compaction is one of the dedup file's advance points.
    state |> persist_dedup() |> reply_synced()
  end

  # ---------- Append / realign ----------

  defp write_record(state, seq, header, change_bytes, runner_id, now) do
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

    # One mark per fsync window: `pending_since_us` is nil exactly when nothing
    # written since the last sync is waiting on the batcher.
    if state.pending_since_us == nil, do: FsyncBatcher.mark_dirty(state.entity_id)

    new_pending = state.pending_bytes + size

    new_state = %{
      note_header(state, header, seq)
      | last_persisted_seq: seq,
        seq_index: SeqIndex.note(state.seq_index, seq, size),
        last_advance_at: now,
        owning_runner: runner_id || state.owning_runner,
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
          {rc, sh, pk} = state.entity_id

          Logger.warning(
            "[wal] effect record body undecodable entity=#{rc}/#{sh}/#{pk} " <>
              "seq=#{seq}: #{inspect(reason)} — outputs not flushed"
          )

          state
      end
    else
      state
    end
  end

  # Dispatch every leading durable entry, in seq order, to the operative's
  # `:on_output` chain with the durable identity `(flz.entity, outbox_seq,
  # epoch)`. Undispatchable (Overwatch not yet bound) → bounded retry timer;
  # the queue is bounded by the credit window like the un-fsynced buffer.
  #
  # Runs on every event that can open the window: an fsync (new durable
  # entries) and a completed confirmation (a free slot). A confirmation that did
  # not refill would leave the tail of a burst to the 1 s retry tick, one window
  # per tick.
  defp try_dispatch_outbox(state) do
    case :queue.peek(state.pending_outbox) do
      :empty ->
        state

      {:value, %{seq: seq}} when seq > state.outbox_durable_seq ->
        state

      {:value, _entry} ->
        if window_full?(state) do
          state
        else
          {rc, _sh, _pk} = state.entity_id

          case ClassRegistry.overwatch_pid(rc) do
            {:ok, pid} -> dispatch_outbox_window(state, pid)
            :error -> schedule_outbox_retry(state)
          end
        end
    end
  end

  # One window, one batch: every entry the window admits reaches the chain in one
  # cast, and the furthest consumed-offset the window carried advances the
  # watermark once. Every entry is registered before the cast leaves, so no
  # report can name an entry that does not exist yet.
  defp dispatch_outbox_window(state, pid) do
    {state, chunks, consumed} = take_outbox_window(state, [], -1)

    Outbox.dispatch(pid, in_dispatch_order(chunks))
    advance_consumed(state, consumed)

    state
  end

  # Each entry contributed its outputs as one chunk, newest first.
  defp in_dispatch_order(chunks), do: chunks |> Enum.reverse() |> Enum.concat()

  # Back-pressure, not loss: with the confirmation window full the durable
  # entries simply stay queued (and on disk) until a sink answers. Ingest and
  # the effect-record fsync never wait on this.
  defp take_outbox_window(state, chunks, consumed) do
    case :queue.out(state.pending_outbox) do
      {{:value, %{seq: seq} = entry}, rest} when seq <= state.outbox_durable_seq ->
        if window_full?(state) do
          {state, chunks, consumed}
        else
          key = {:cursor, seq}
          outputs = entry_outputs(state, key, entry)
          state = await_entry(%{state | pending_outbox: rest}, key, entry, length(outputs))

          apply_effect(state, entry)

          take_outbox_window(state, [outputs | chunks], furthest_consumed(consumed, entry))
        end

      _ ->
        {state, chunks, consumed}
    end
  end

  # The window counts entries inside the plugin chain unconfirmed — exactly the
  # awaiting set. `:queue.len/1` on the inflight queue is O(n), which made every
  # refill of a 1 000-deep window quadratic.
  defp window_full?(state),
    do: map_size(state.outbox_awaiting) >= state.outbox_inflight_max

  # The stamped outputs alone — what a re-dispatch owes, and how many the
  # entry's tally waits on before the cursor may pass it.
  defp entry_outputs(state, key, entry) do
    # The token names the entry, and a stream Entity's writer owns its own
    # output — so one token covers every output of the entry.
    ack = Outbox.token(state.entity_id, key)

    entry.outputs
    |> Enum.with_index()
    |> Enum.map(fn {output, ordinal} ->
      Outbox.stamp(output.payload, %{
        owner: state.entity_id,
        entry: key,
        entity: state.entity_id,
        ack: ack,
        seq: "#{entry.seq}.#{ordinal}",
        epoch: state.outbox_epoch,
        channel: output.channel,
        key: output.key,
        metadata: output.metadata
      })
    end)
  end

  # Consumed-offsets only move forward, so the window's furthest offset is the
  # same watermark every entry's own advance would have reached — at one ETS
  # write and one credit recompute instead of one per record.
  defp furthest_consumed(consumed, %{consumed_offset: offset}) when is_integer(offset),
    do: max(consumed, offset)

  defp furthest_consumed(consumed, _entry), do: consumed

  # -1 is the window that acknowledged nothing: mid-interval records carry no
  # offset, and a whole window of them must not touch the watermark.
  defp advance_consumed(_state, -1), do: :ok

  defp advance_consumed(state, offset) do
    {rc, _sh, _pk} = state.entity_id
    InputLog.record_consumed(rc, state.entity_id, offset)
  end

  # The durable side effects the record carries beyond its outputs. They belong
  # to the record, not to a delivery attempt, so a re-dispatch does not repeat
  # them.
  defp apply_effect(state, entry) do
    {rc, sh, pk} = state.entity_id

    # Materialize a dead-letter park BEFORE the pass's consumed-offset advance:
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
            procedure_id: sh,
            entity_key: pk,
            channel: dlq.channel,
            origin_offset: entry.consumed_offset,
            error: dlq.error,
            attempts: dlq.attempts,
            payload: dlq.payload
          })
    end

    # An integer offset on a checkpoint-ack class means an interval just closed:
    # state and source position are consistent at exactly this cut, which is the
    # cut the remote durability tier wants. Every other class reaches the tier
    # through compaction, a commit barrier, or the backstop sweep.
    DurabilityTier.on_effect_flushed(state.entity_id, entry.consumed_offset)
  end

  defp schedule_outbox_retry(%{outbox_retry_timer: nil} = state) do
    ref = Process.send_after(self(), :outbox_retry, 1_000)
    %{state | outbox_retry_timer: ref}
  end

  defp schedule_outbox_retry(state), do: state

  # One entry's outstanding delivery: `pending` counts outputs still inside the
  # plugin chain, `holds` the sinks that took ownership of one, `confirms` the
  # answers they owe. Complete once every output has settled and every hold has
  # answered — an unheld output is complete the moment its chain run settles,
  # which is what keeps a chain with no delivery-guaranteed sink behaving as it
  # always did.
  #
  # Registration always precedes the reports (both the stream dispatch and the
  # journal flush register inside the handler that casts), so a report against
  # an unknown entry is one a resettle superseded — dropped, because the live
  # registration owns the entry now. `entry` is retained for exactly that
  # resettle; the journal flush keeps `nil` and re-drives from its own journal.
  defp register_entry(state, key, outputs, entry) do
    tally = %{pending: outputs, holds: 0, confirms: 0, done?: false, entry: entry, at: now_ms()}

    %{state | outbox_awaiting: Map.put(state.outbox_awaiting, key, tally)}
    |> tally(key, & &1)
  end

  defp await_entry(state, {:cursor, seq} = key, entry, outputs) do
    %{state | outbox_inflight: :queue.in({seq, now_ms()}, state.outbox_inflight)}
    |> register_entry(key, outputs, entry)
    |> schedule_outbox_retry()
  end

  # A batch report carries the same per-entry facts as the single-key reports
  # it replaces, so both shapes fold through the one tally.
  defp settled(state, {key, holds}),
    do: tally(state, key, &%{&1 | pending: &1.pending - 1, holds: &1.holds + holds})

  defp confirmed(state, key), do: tally(state, key, &%{&1 | confirms: &1.confirms + 1})

  defp tally(state, key, fun) do
    case Map.fetch(state.outbox_awaiting, key) do
      # A report for an entry that is gone (a resettle superseded it) or already
      # completing changes nothing.
      :error ->
        state

      {:ok, %{done?: true}} ->
        state

      {:ok, tally} ->
        tally = fun.(tally)

        if complete?(tally) do
          complete_entry(
            %{state | outbox_awaiting: Map.put(state.outbox_awaiting, key, tally)},
            key
          )
        else
          %{state | outbox_awaiting: Map.put(state.outbox_awaiting, key, tally)}
        end
    end
  end

  # `pending` may go negative when a superseded chain run settles late; its
  # outputs were delivered under the same identity, so it is still complete.
  defp complete?(%{pending: pending, holds: holds, confirms: confirms}),
    do: pending <= 0 and confirms >= holds

  # A chain run that never reported back — the Overwatch died between the cast
  # and the settlement — leaves the entry owed with nothing in flight to answer
  # it. Re-dispatch its outputs under their identical identity (conforming sinks
  # dedup; the record's own side effects already landed) and
  # start the tally over: a hold the lost run took would otherwise never be
  # answered, wedging the cursor for good. The journal's entries carry no copy —
  # dropping the tally is what lets the next journal-driven flush re-drive them.
  defp resettle_outbox(state) do
    if resettle_due?(state) do
      {rc, _sh, _pk} = state.entity_id
      overwatch = ClassRegistry.overwatch_pid(rc)

      {state, chunks} =
        Enum.reduce(state.outbox_awaiting, {state, []}, fn {key, tally}, {acc, chunks} ->
          if tally.pending > 0 and now_ms() - tally.at >= acc.outbox_resettle_ms,
            do: resettle_entry(overwatch, acc, chunks, key, tally),
            else: {acc, chunks}
        end)

      with {:ok, pid} <- overwatch, do: Outbox.dispatch(pid, in_dispatch_order(chunks))

      state
    else
      state
    end
  end

  # The inflight head is the oldest dispatch, so one comparison rules the whole
  # sweep out on a healthy stream writer.
  defp resettle_due?(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {_seq, at}} -> now_ms() - at >= state.outbox_resettle_ms
      :empty -> map_size(state.outbox_awaiting) > 0
    end
  end

  defp resettle_entry({:ok, _pid}, state, chunks, key, %{entry: entry}) when entry != nil do
    outputs = entry_outputs(state, key, entry)
    {register_entry(state, key, length(outputs), entry), [outputs | chunks]}
  end

  # Nothing to re-dispatch to: the entry stays owed with its tally untouched, so
  # the next tick tries again instead of restarting the clock against a chain
  # that cannot run.
  defp resettle_entry(:error, state, chunks, _key, %{entry: entry}) when entry != nil,
    do: {state, chunks}

  defp resettle_entry(_overwatch, state, chunks, key, _tally),
    do: {%{state | outbox_awaiting: Map.delete(state.outbox_awaiting, key)}, chunks}

  # The entry stays registered until its marker is durable: a journal-driven
  # re-flush reads the journal, so dropping it any sooner would let the entry be
  # delivered a second time. The append itself must not run here — it is a call
  # back into this very process — so it runs off-process and reports back.
  defp complete_entry(state, {:marker, _, _, _, _} = key) do
    writer = self()
    entity_id = state.entity_id

    spawn(fn ->
      Outbox.complete(entity_id, key)
      send(writer, {:outbox_marked, key})
    end)

    put_in(state.outbox_awaiting[key].done?, true)
  end

  defp complete_entry(state, {:cursor, _seq} = key) do
    %{state | outbox_awaiting: Map.delete(state.outbox_awaiting, key)}
    |> drain_confirmed()
    |> try_dispatch_outbox()
  end

  # The cursor advances over the leading run of confirmed entries only: an entry
  # still in `outbox_awaiting` stops the drain, so a later confirmation can
  # never carry the cursor past output no sink has taken.
  defp drain_confirmed(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {seq, _at}} ->
        if Map.has_key?(state.outbox_awaiting, {:cursor, seq}) do
          state
        else
          {_, rest} = :queue.out(state.outbox_inflight)
          drain_confirmed(%{state | outbox_inflight: rest, outbox_flushed_seq: seq})
        end

      :empty ->
        state
    end
  end

  # Age of the oldest output no sink has confirmed — the producing-liveness
  # evidence for a sink that stopped answering while nothing new is emitted.
  defp unconfirmed_age_ms(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {_seq, at}} -> now_ms() - at
      :empty -> 0
    end
  end

  # Advance the dedup file (persistence watermark + flush cursor + epoch)
  # with one atomic tmp+rename — at most once per fsync batch. The reserved
  # per-instance identities (proc-journal, durable inbox) skip it: neither carries
  # effect records, and the dedup side-file would register a phantom Entity
  # with `Paths.scan_dir/1`.
  defp persist_dedup(%{entity_id: {_, _, key}} = state) when key in @reserved_entity_keys,
    do: state

  defp persist_dedup(state) do
    _ =
      Outbox.write(state.entity_id, %{
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
  defp stale_writer?(state, runner_id) do
    is_binary(runner_id) and runner_id != state.owning_runner and
      FleetManager.superseded_session?(runner_id)
  end

  # One `stale_writer_refused` event per superseded session per writer
  # lifetime (sessions are never reused, so the episode never re-opens);
  # every refusal increments the counter.
  defp refuse_stale(state, runner_id) do
    Metrics.add(state.metrics, :stale_writer_refused_total, 1)

    if MapSet.member?(state.refused_sessions, runner_id) do
      state
    else
      {rc, sh, pk} = state.entity_id

      :telemetry.execute(
        [:flezha, :state, :stale_writer_refused],
        %{count: 1},
        %{operative: rc, procedure: sh, key: pk, session: runner_id}
      )

      %{state | refused_sessions: MapSet.put(state.refused_sessions, runner_id)}
    end
  end

  # A runner restart is the only way a Change's seq can diverge under a *new*
  # runner id: within one runner's lifetime the seq is monotonic, so a low
  # seq from the same runner is a legitimate retransmit (deduped, not
  # realigned). `owning_runner == nil` (no Change seen since boot) defers to
  # the normal dedup/gap/append path so a controller restart still re-seeds the
  # alive runner via `RestoreInitState` instead of discarding the WAL.
  defp realign?(state, runner_id, seq) do
    is_binary(runner_id) and is_binary(state.owning_runner) and
      runner_id != state.owning_runner and seq != state.last_persisted_seq + 1
  end

  # Discard the stale pre-restart epoch so a later `RestoreInitState` ships only
  # the new runner's history: empty the WAL file and drop its snapshots, then
  # reset the seq baseline so the incoming `seq` appends contiguously.
  defp realign_epoch(state, seq, runner_id, now) do
    # The truncated epoch's un-fsynced bytes no longer occupy the buffer;
    # release them to the stream ledger so the window is not leaked.
    state = flush_credit_pending(state)
    {:ok, _} = :file.position(state.file_handle, 0)
    :ok = :file.truncate(state.file_handle)

    Enum.each(Paths.snapshot_paths(state.entity_id), fn {_seq, path} -> File.rm(path) end)

    emit_realign(state.entity_id, state.last_persisted_seq, seq, runner_id)

    Metrics.put(state.metrics, :wal_size_bytes, 0)
    Metrics.put(state.metrics, :snapshot_seq, 0)

    # The stale lineage's undispatched outputs die with its records — the
    # realigned runner's replay regenerates them under the new epoch.
    state = %{
      state
      | last_persisted_seq: seq - 1,
        last_advance_at: now,
        owning_runner: runner_id,
        pending_bytes: 0,
        pending_since_us: nil,
        oldest_unsnapshotted_at: nil,
        header_index: nil
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
  # is the commit point of both procedures — it is what stops an output
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
        outbox_awaiting: %{},
        outbox_inflight: :queue.new(),
        outbox_epoch: e
    }

    persist_dedup(state)
  end

  # Cold-takeover shed: no baseline on this controller, so the frame can't
  # append. Count it and drive a CompactEntity pull via `Recovery.resync`
  # (routes to the pull because the Entity has no persisted state). The pulled
  # snapshot encodes the shed deltas' effects. The pull is retried on the
  # resync interval for as long as the Entity waits: only that snapshot clears
  # this state, so a pull that failed to dispatch must not be the last one.
  defp await_baseline(state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    unless state.awaiting_baseline? do
      {rc, sh, pk} = state.entity_id

      :telemetry.execute(
        [:flezha, :state, :baseline_seed],
        %{first_seq: seq},
        %{operative: rc, procedure: sh, key: pk, runner: runner_id}
      )
    end

    resync_rate_limited(%{state | awaiting_baseline?: true}, runner_id, :baseline_seed)
  end

  # Baseline established: the pulled snapshot's seq supersedes the shed
  # deltas, so adopt it and let the runner's next delta append contiguously.
  defp establish_baseline(%{awaiting_baseline?: true, last_persisted_seq: last} = state, seq)
       when seq > last do
    now = now_ms()
    Metrics.put(state.metrics, :wal_seq, seq)

    state = %{
      state
      | awaiting_baseline?: false,
        last_resync_at: nil,
        last_persisted_seq: seq,
        last_advance_at: now
    }

    persist_dedup(state)
  end

  defp establish_baseline(state, _seq), do: %{state | awaiting_baseline?: false}

  defp emit_seq_gap({rc, sh, pk}, expected, got) do
    :telemetry.execute(
      [:flezha, :state, :seq_gap],
      %{expected: expected, got: got},
      %{operative: rc, procedure: sh, key: pk}
    )
  end

  defp emit_realign({rc, sh, pk}, from_seq, to_seq, runner_id) do
    :telemetry.execute(
      [:flezha, :state, :wal_realign],
      %{from_seq: from_seq, to_seq: to_seq},
      %{operative: rc, procedure: sh, key: pk, runner: runner_id}
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
      # is not actually late for this Entity.
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
      %{operative: rc, procedure: sh, key: pk, epoch: epoch}
    )
  end

  defp highest_snapshot_seq(entity_id) do
    case Paths.snapshot_paths(entity_id) do
      [] -> 0
      paths -> paths |> List.last() |> elem(0)
    end
  end

  # The rewound records are the ones that reported this Entity's consumed
  # offsets, so the input-log watermark it advanced is now a claim about inputs
  # nothing has consumed. Lower it, or the next retention sweep truncates away
  # the very inputs the correction is about to replay.
  defp rewind_input_watermark(state, max_consumed) do
    {operative_class, _, _} = state.entity_id
    InputLog.rewind_consumed(operative_class, state.entity_id, max_consumed || -1)
  end

  # ---------- Scan / rewrite helpers ----------

  @doc """
  Reduce over a WAL file's framed records, oldest first.

  `fun.({seq, header, body}, acc)` returns `{:cont, acc}` or `{:halt, acc}`; the
  walk also stops at the first undecodable record, which is the torn-tail
  contract every reader here shares.

  The one place the `decode_record/2` recursion lives. The framing is shared by
  the Entity WAL, the input log, the proc-journal and the retention rewriter, and
  each of them used to carry its own copy of this loop.
  """
  @spec fold_records(:file.io_device(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                                {:cont, acc} | {:halt, acc})) :: acc
        when acc: term()
  def fold_records(device, acc, fun), do: fold_records(device, 0, acc, fun)

  @doc """
  `fold_records/3` over a path: open read-only, fold, close. A file that is
  missing or unreadable folds to `acc` unchanged — absent history is no records,
  which is what every reader of this framing already means by it.

  The one definition of the open-fold-close dance, shared by the record-log
  reader, time travel, and the remote durability tier's ship.
  """
  @spec fold_path(binary(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                    {:cont, acc} | {:halt, acc})) :: acc
        when acc: term()
  def fold_path(path, acc, fun) do
    case :file.open(path, [:raw, :read, :binary]) do
      {:ok, device} ->
        try do
          fold_records(device, acc, fun)
        after
          :file.close(device)
        end

      {:error, _} ->
        acc
    end
  end

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
    # `-1`, not `0`, for an empty WAL: the SDK's per-Entity seq is 0-based
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
  # consumed-offset across ALL effect records — the Entity's durable
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

  @doc """
  Drop the records below `floor` from the record log at `path`, whose live
  handle is `device` and whose index is `index`. The boundary is found within
  one sample span, the kept tail is copied as bytes into `<path>.rewriting`,
  fsynced and renamed into place, and the handle is reopened on the new file
  at its end. Answers `{:ok, cut_bytes, device, index}` with the index
  rebased; `:none` when nothing is below the floor, or fewer than `min_bytes`
  are (the rewrite is not worth its fsync yet); `{:error, reason}` when the
  file is gone (a cut queued behind its directory's removal), with the
  caller's handle untouched. Both record logs cut through this, so a cut costs
  the tail, never the file.
  """
  @spec cut_below(Path.t(), :file.io_device(), SeqIndex.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer(), :file.io_device(), SeqIndex.t()} | :none | {:error, term()}
  def cut_below(path, device, index, floor, min_bytes \\ 0) do
    case SeqIndex.position_of(index, device, floor) do
      cut when cut == 0 or cut < min_bytes ->
        :none

      cut ->
        with {:ok, file} <- replace_with_range(path, device, cut, :eof) do
          {:ok, cut, file, SeqIndex.after_cut(index, floor, cut)}
        end
    end
  end

  @doc """
  Drop the records at or above `floor` — the mirror of `cut_below/4`: a
  correction's rewind drops the suffix it invalidated, keeping the prefix as
  bytes. The samples below the boundary stay valid. Answers `{:ok, device,
  index}`.
  """
  @spec cut_above(Path.t(), :file.io_device(), SeqIndex.t(), non_neg_integer()) ::
          {:ok, :file.io_device(), SeqIndex.t()}
  def cut_above(path, device, index, floor) do
    bound = SeqIndex.position_of(index, device, floor)
    {:ok, file} = replace_with_range(path, device, 0, bound)
    {:ok, file, SeqIndex.cut_above(index, floor, bound)}
  end

  # Copy the bytes `[from, to)` of `path` into `<path>.rewriting`, fsync it,
  # rename it into place and reopen `device` on it at its end. The two-step is
  # what makes a crash mid-rewrite leave the original intact
  # (`Paths.scan_dir/1` skips `.rewriting` residue); the rename happens
  # before the old handle closes, so a failure before it leaves that handle
  # valid. Records are self-delimiting and CRC-framed, so a record-aligned
  # byte range is the same log the records would re-encode to.
  defp replace_with_range(path, device, from, to) do
    new_path = path <> ".rewriting"

    with {:ok, src} <- :file.open(path, [:raw, :read, :binary]),
         {:ok, dst} <- open_or_close(new_path, src),
         :ok <- copy_range(src, dst, from, to),
         :ok <- :file.rename(new_path, path),
         :ok = :file.close(device),
         {:ok, file} <- :file.open(path, [:raw, :read, :write, :binary]),
         {:ok, _} <- :file.position(file, :eof) do
      {:ok, file}
    end
  end

  defp copy_range(src, dst, from, to) do
    try do
      with {:ok, _} <- :file.position(src, from),
           {:ok, _} <-
             if(to == :eof, do: :file.copy(src, dst), else: :file.copy(src, dst, to - from)),
           :ok <- :file.sync(dst) do
        :ok
      end
    after
      _ = :file.close(src)
      _ = :file.close(dst)
    end
  end

  defp open_or_close(path, src) do
    case :file.open(path, [:raw, :write, :binary]) do
      {:ok, dst} ->
        {:ok, dst}

      {:error, _} = error ->
        _ = :file.close(src)
        error
    end
  end

  # ---------- Hard bound (shed) ----------

  # Last-resort safety net above the soft watermark: cooperative SDKs throttle on
  # `EntityMailboxStatus` first; only a slow disk or a non-cooperating SDK
  # reaches here.
  #
  # The un-fsynced byte bound always applies. The mailbox-depth cap applies only
  # to an un-credited stream, where nothing else stops a cast flood: it counts
  # MESSAGES while credit counts BYTES, so on a credit-speaking stream it fires
  # far below the window it is meant to backstop — at 128-byte Changes, 5_000
  # messages is ~0.6 MB against a 16 MB window, shedding a conformant sender's
  # Changes 25x early. Credit already bounds mailbox + un-fsynced bytes to the
  # window (the channel debits at admit, before the cast).
  defp over_hard_bound?(state) do
    state.pending_bytes >= state.hard_watermark or
      (not state.credit_active? and message_queue_len() >= state.pending_msg_cap)
  end

  defp message_queue_len() do
    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, n} -> n
      nil -> 0
    end
  end

  # Drive a resync for this Entity off-process. Captures `entity_id` only —
  # closing over `state` would retain the writer's file handle, metrics and
  # outbox in the spawned process — and never blocks the writer's append queue.
  defp spawn_resync(state, runner_id, reason) do
    entity_id = state.entity_id
    spawn(fn -> Recovery.resync(entity_id, runner_id: runner_id, reason: reason) end)
  end

  # At most one resync per Entity per interval, shared by every gap-driven
  # trigger: a sustained flood produces a gap per frame, and one unguarded spawn
  # each piles up resync processes before any completes. A per-episode latch
  # would be simpler but cannot heal — a resync can fail (a torn control channel
  # answers `:no_control_channel`) and the gap itself suppresses the contiguous
  # append that would re-arm the latch.
  defp resync_rate_limited(state, runner_id, reason) do
    now = now_ms()

    if is_integer(state.last_resync_at) and
         now - state.last_resync_at < resync_retry_interval_ms() do
      state
    else
      spawn_resync(state, runner_id, reason)
      %{state | last_resync_at: now}
    end
  end

  # Drop the Change without writing; the resulting seq gap drives the existing
  # resync handshake so the SDK (authoritative) re-sends. Resync is spawned once
  # per shed episode (on the false->true transition) to avoid a resync storm
  # under a sustained flood. A latch is enough here, unlike the baseline pull:
  # `append_async` clears it on any successful append, so relieved
  # back-pressure re-arms it without the resync having to succeed.
  defp shed(state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)
    {rc, sh, pk} = state.entity_id

    # A conformant credit-speaking sender can never reach the hard bound
    # (the stream ledger refuses the overrun first) — a shed here is a
    # protocol anomaly, not back-pressure.
    if state.credit_active? do
      :telemetry.execute(
        [:flezha, :state, :credit_anomaly],
        %{count: 1},
        %{kind: :shed, operative: rc, procedure: sh, key: pk, runner: runner_id}
      )
    end

    if state.shedding? do
      state
    else
      emit_seq_gap(state.entity_id, state.last_persisted_seq + 1, seq)
      spawn_resync(state, runner_id, :shed)
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
  # routes through the producing Procedure's `OverflowPolicy` machinery on
  # the SDK side. A credit-speaking session gets no wire signal: its
  # back-pressure is carried entirely by the credit ledger (the sender parks
  # on window exhaustion), so only the telemetry event is emitted.
  defp emit_backpressure(state, status, pending) do
    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{pending_bytes: pending},
      %{entity_id: state.entity_id, status: status}
    )

    # Routing needs the owning runner (`nil` until the first Change lands,
    # in which case there is no session to signal).
    with false <- state.credit_active?,
         {rc, sh, pk} <- state.entity_id,
         runner_id when is_binary(runner_id) <- state.owning_runner do
      _ =
        Flezha.State.Commands.notify_entity_backpressure(
          {runner_id, rc, sh, pk},
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

  defp high_watermark(), do: PersistenceResolver.wal_high_watermark_bytes()

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

  # How many effect records may be inside the plugin chain unconfirmed before
  # the flush stops handing over more. The durable entries keep queueing behind
  # it; only the flush waits.
  defp outbox_inflight_max() do
    Application.get_env(:flezha, :outbox_inflight_max, 1_000)
  end

  # How long a dispatched entry may go without its chain run reporting back
  # before it is re-dispatched. Long enough that a merely backlogged Overwatch
  # is not duplicated into; a re-dispatch is safe either way.
  defp outbox_resettle_ms() do
    Application.get_env(:flezha, :outbox_resettle_ms, 60_000)
  end

  # Floor between resync attempts for one Entity. Low enough that a genuine gap
  # heals promptly, high enough that a per-frame gap flood cannot fan out.
  defp resync_retry_interval_ms() do
    Application.get_env(:flezha, :wal_resync_retry_interval_ms, 1_000)
  end

  defp now_ms(), do: :erlang.monotonic_time(:millisecond)
end
