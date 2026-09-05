defmodule Flezha.State.WalWriter.Boot do
  @moduledoc """
  How a writer comes up: build the struct, then open the file it names.

  The two halves are separate because the first must not touch disk.
  `new/1` returns instantly so the process is Registry-registered before any
  I/O, and `open/1` does the mkdir, the open, the torn-tail scan and the
  restart re-drive — every durable effect record above the flush cursor
  re-entering the outbox with identical identity, so a stale cursor widens the
  re-drive and never skips it.
  """

  alias Flezha.State.{InputLog, Metrics, Outbox, Paths, RecordFrame, SeqIndex}
  alias Flezha.State.WalWriter.{Admission, Epoch, Settlement, State}

  @doc "The struct a writer starts from, before any disk I/O."
  def new(entity_id) do
    Process.flag(:trap_exit, true)
    now = State.now_ms()

    # Disk I/O (mkdir/open/scan/seek) is deferred to `handle_continue/2` so this
    # `init` returns instantly: the process is already Registry-registered, so a
    # `Supervisor.find_or_start/1` from a StreamChannel never blocks on disk. The
    # continue runs before any queued append, so appends queue in this writer's
    # mailbox — never the channel's. `-1` is the "no records yet" seq sentinel.
    state = %State{
      entity_id: entity_id,
      file_handle: nil,
      wal_path: Paths.wal_path(entity_id),
      last_persisted_seq: -1,
      last_advance_at: now,
      last_change_seen_at: now,
      owning_runner: nil,
      pending_bytes: 0,
      pending_since_us: nil,
      limits: Admission.limits(),
      metrics: Metrics.for_entity(entity_id),
      latency_reservoir: :queue.new(),
      backpressured?: false,
      shedding?: false,
      gap_open?: false,
      awaiting_baseline?: false,
      oldest_unsnapshotted_at: nil,
      credit_pending: %{},
      credit_active?: false,
      refused_sessions: MapSet.new(),
      pending_outbox: :queue.new(),
      outbox_durable_seq: -1,
      outbox_flushed_seq: -1,
      outbox_epoch: 0,
      timers: Flezha.Process.Timer.new(),
      outbox_awaiting: %{},
      outbox_inflight: :queue.new(),
      outbox_last_seq: -1,
      seq_index: SeqIndex.new(),
      sync_waiters: [],
      header_index: nil,
      refusal: nil
    }

    state
  end

  @doc """
  Open the WAL, repair a torn tail, and re-drive every durable effect record
  above the flush cursor. Deferred out of `new/1` so the process is
  Registry-registered before it touches disk: a `Supervisor.find_or_start/1`
  from a StreamChannel never blocks, and appends queue in this writer's mailbox
  rather than the channel's.
  """
  def open(state) do
    File.mkdir_p!(Path.dirname(state.wal_path))
    {:ok, file} = :file.open(state.wal_path, [:raw, :read, :write, :binary])

    case RecordFrame.scan_for_last_valid_seq(file, state.wal_path) do
      {:ok, last_seq} -> opened(%{state | file_handle: file}, file, last_seq)
      {:error, refusal} -> refused(%{state | file_handle: file}, refusal)
    end
  end

  # The scan refused: the file holds a record that failed its crc with complete
  # records behind it. Nothing here reports a number derived from a walk that
  # stopped early — not the seq metric, and not the consumed offset the outbox
  # re-drive would report, which only ever moves forward.
  defp refused(state, refusal) do
    Metrics.put(state.metrics, :wal_size_bytes, RecordFrame.file_size(state.wal_path))
    %{state | refusal: refusal}
  end

  defp opened(state, file, last_seq) do
    size = RecordFrame.file_size(state.wal_path)

    Metrics.put(state.metrics, :wal_seq, last_seq)
    Metrics.put(state.metrics, :wal_size_bytes, size)

    # Reposition for appends.
    {:ok, _} = :file.position(file, :eof)

    # Boot-time approximation: surviving records are at least this old, so
    # the age trigger under-fires by at most one boot-to-sweep interval.
    oldest = if size > 0, do: State.now_ms()

    # Restart re-drive (effectively-once): every durable effect record above
    # the flush cursor re-enters the outbox queue with identical identity —
    # a stale cursor widens the re-drive (sink dedup absorbs), never skips.
    dedup = Outbox.read(state.entity_id)

    {redrive, max_consumed} =
      Epoch.scan_effect_records_above(file, state.wal_path, dedup.outbox_flushed_seq)

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

    Settlement.try_dispatch_outbox(state)
  end

  # Seed the producing-liveness cursor from what the re-drive owes. Starting it at
  # -1 would make the first re-driven record read as a jump from nothing, which the
  # classifier would see as an outbox that suddenly grew.
  defp redrive_last_seq(redrive, flushed_seq) do
    redrive
    |> Enum.filter(&(&1.outputs != []))
    |> Enum.reduce(flushed_seq, fn entry, acc -> max(acc, entry.seq) end)
  end
end
