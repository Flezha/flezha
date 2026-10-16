defmodule Flezha.State.WalWriter.Boot do
  @moduledoc """
  How a writer comes up: build the struct, then open the file it names.

  The two halves are separate because the first must not touch disk.
  `new/1` returns instantly so the process is Registry-registered before any
  I/O, and `open/1` does the mkdir, the open, the torn-tail scan and the
  restart re-drive — every durable effect record above the flush cursor
  re-entering the outbox with identical identity, so a stale cursor widens the
  re-drive and never skips it. A durable steps relay Cell's writer holds each
  re-driven entry until its instance's journal names it, and drops the ones
  the journal says never committed.
  """

  alias Flezha.Relay.ClassRegistry

  alias Flezha.State.{
    InputLog,
    Metrics,
    Outbox,
    Paths,
    RecordFrame,
    SeqIndex,
    Snapshot,
    TaskJournal
  }

  alias Flezha.State.WalWriter.{Admission, Epoch, RemoteGate, Settlement, State}

  @doc "The struct a writer starts from, before any disk I/O."
  def new(cell_id) do
    Process.flag(:trap_exit, true)
    now = State.now_ms()

    # Disk I/O (mkdir/open/scan/seek) is deferred to `handle_continue/2` so this
    # `init` returns instantly: the process is already Registry-registered, so a
    # `Supervisor.find_or_start/1` from a StreamChannel never blocks on disk. The
    # continue runs before any queued append, so appends queue in this writer's
    # mailbox — never the channel's. `-1` is the "no records yet" seq sentinel.
    state = %State{
      cell_id: cell_id,
      file_handle: nil,
      wal_path: Paths.wal_path(cell_id),
      last_persisted_seq: -1,
      last_advance_at: now,
      last_change_seen_at: now,
      owning_runner: nil,
      pending_bytes: 0,
      pending_since_us: nil,
      limits: Admission.limits(),
      metrics: Metrics.for_cell(cell_id),
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
      commit_unit: :record,
      remote: nil,
      outbox_durable_seq: -1,
      outbox_flushed_seq: -1,
      outbox_epoch: 0,
      timers: Flezha.Process.Timer.new(),
      outbox_awaiting: %{},
      outbox_inflight: :queue.new(),
      outbox_last_seq: -1,
      seq_index: SeqIndex.new(),
      sync_waiters: [],
      durable_waiters: [],
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
    unit = commit_unit(state.cell_id)
    state = %{state | commit_unit: unit, remote: RemoteGate.new(state.cell_id, unit)}
    File.mkdir_p!(Path.dirname(state.wal_path))
    {:ok, file} = :file.open(state.wal_path, [:raw, :read, :write, :binary])

    case RecordFrame.scan_for_last_valid_seq(file, state.wal_path) do
      {:ok, last_seq} ->
        opened(%{state | file_handle: file}, file, max(last_seq, snapshot_seq(state.cell_id)))

      {:error, refusal} ->
        refused(%{state | file_handle: file}, refusal)
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

  # A compaction can leave the WAL empty with every Change folded into the
  # newest snapshot; the writer resumes from whichever reaches further, which
  # is the seq a restore ships and the runner resumes after.
  defp snapshot_seq(cell_id) do
    case Snapshot.latest_snapshot(cell_id) do
      {seq, _path} -> seq
      nil -> -1
    end
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
    dedup = Outbox.read(state.cell_id)
    durable = RemoteGate.durable_through(state, last_seq, dedup.outbox_flushed_seq)

    # Records a snapshot folded away report no consumed offset.
    {redrive, max_consumed} =
      Epoch.scan_effect_records_above(state, file, dedup.outbox_flushed_seq, durable)

    {rc, _sh, _pk} = state.cell_id
    if max_consumed >= 0, do: InputLog.record_consumed(rc, state.cell_id, max_consumed)

    state = %{
      state
      | file_handle: file,
        seq_index: SeqIndex.new(size),
        last_persisted_seq: last_seq,
        oldest_unsnapshotted_at: oldest,
        outbox_flushed_seq: dedup.outbox_flushed_seq,
        outbox_epoch: dedup.epoch,
        outbox_durable_seq: durable,
        pending_outbox: :queue.from_list(redrive),
        outbox_last_seq: redrive_last_seq(redrive, dedup.outbox_flushed_seq)
    }

    # What a closed writer left owed rides this writer's first acknowledgement.
    case state.commit_unit do
      :record -> RemoteGate.synced(%{state | credit_pending: RemoteGate.take_owed(state.cell_id)})
      :stage -> Settlement.seed_commits(state, TaskJournal.committed_records(rc), :hold)
    end
  end

  # A Cell of a steps class commits at its stage marker; every other Cell at
  # its record. How it runs outlives the instance's registration on disk, so a
  # writer that boots before its instance binds still reads it.
  defp commit_unit({relay_class, _sh, key}) do
    if key not in Paths.reserved_cell_keys() and ClassRegistry.journals?(relay_class),
      do: :stage,
      else: :record
  end

  # Seed the producing-liveness cursor from what the re-drive owes. Starting it at
  # -1 would make the first re-driven record read as a jump from nothing, which the
  # classifier would see as an outbox that suddenly grew.
  defp redrive_last_seq(redrive, flushed_seq) do
    redrive
    |> Enum.filter(&(&1.committed? and &1.outputs != []))
    |> Enum.reduce(flushed_seq, fn entry, acc -> max(acc, entry.seq) end)
  end
end
