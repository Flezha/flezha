defmodule Flezha.State.WalWriter.Epoch do
  @moduledoc """
  Which session owns the WAL, and where its history starts.

  The owner is the runner the last bind re-seated (`reseat/2`): a frame from
  any other, or from a session a takeover superseded, is refused, so no stale
  instance rewrites the log. A correction's `rewind/3` cuts above a boundary,
  keeps the baselines below it and bumps the epoch, which is what makes a
  re-run's output supersede the run it replaces at a guarded sink. A refusing
  disk cuts what was written since the last sync and keeps the epoch
  (`drop_unsynced/2`).
  """

  require Logger

  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.State.{Frame, Wire}
  alias Flezha.State.{FsyncBatcher, InputLog, Lineage, Metrics, Paths, RecordFrame, SeqIndex}
  alias Flezha.State.WalWriter.{Admission, RemoteGate, Settlement, State}

  # A frame from a session a takeover superseded, or from a runner that is not
  # the owner. The owning session short-circuits on the first comparison, so
  # its append path gains no per-record work.
  def stale_writer?(state, runner_id) do
    is_binary(runner_id) and runner_id != state.owning_runner and
      (Marks.superseded_session?(runner_id) or
         (is_binary(state.owning_runner) and not owner?(state, runner_id)))
  end

  # One `stale_writer_refused` event per refused session per writer lifetime
  # (sessions are never reused, so the episode never re-opens); every refusal
  # increments the counter.
  def refuse_stale(state, runner_id) do
    Metrics.add(state.metrics, :stale_writer_refused_total, 1)

    if MapSet.member?(state.refused_sessions, runner_id) do
      state
    else
      {rc, sh, pk} = state.cell_id

      :telemetry.execute(
        [:flezha, :state, :stale_writer_refused],
        %{count: 1},
        %{relay: rc, tasklet: sh, key: pk, session: runner_id}
      )

      %{state | refused_sessions: MapSet.put(state.refused_sessions, runner_id)}
    end
  end

  # By the stable identity a runner's sessions share: a reconnect swaps the
  # session without a bind.
  def owner?(%{owning_runner: owner}, runner_id) when is_binary(owner) and is_binary(runner_id),
    do: Marks.identity_of(owner) == Marks.identity_of(runner_id)

  def owner?(_state, _runner_id), do: false

  # A bind re-seated the runner's Cell from this log, so the runner continues
  # from here: no pull is owed. A task journal is the controller's own log.
  def reseat(%{cell_id: {_, _, key}} = state, runner_id) do
    owner = if key not in Paths.reserved_cell_keys(), do: runner_id
    Admission.close_gap(%{state | owning_runner: owner})
  end

  # The first record of a window marks the Cell dirty; while `mint?` (a landed
  # copy, or no lineage id yet) a new id is installed first. A failed install raises:
  # a refusal would be a result on every append; the restart mints at the resent record.
  # A directory gone is the Cell deleted under this writer, which is `:dropped`.
  def open_window(%{mint?: true, lineage: lineage} = state) do
    fork = %{lineage | epoch: state.outbox_epoch, folded_offset: state.folded_offset}

    case Settlement.put_lineage(state, Lineage.mint(fork, state.last_persisted_seq)) do
      {:ok, state} -> open_window(%{state | mint?: false})
      {:error, :enoent} -> :dropped
    end
  end

  def open_window(state), do: {:ok, tap(state, &FsyncBatcher.mark_dirty(&1.cell_id))}

  # A rewind's last step: re-seat the outbox cursors on the lineage record's
  # epoch, which keys an output regenerated above the boundary apart from its
  # own pre-boundary delivery, then end the cut and write the dedup file
  # **synchronously**.
  defp bump_epoch(state, pending, durable, flushed) do
    state = %{
      state
      | pending_outbox: pending,
        outbox_durable_seq: durable,
        outbox_flushed_seq: flushed,
        outbox_awaiting: %{},
        outbox_inflight: :queue.new(),
        outbox_epoch: state.lineage.epoch
    }

    Settlement.persist_dedup(state)
  end

  # A frame above the next seq is refused and the gap declared, so the loop
  # pulls the runner's state. With no log here (a cold takeover) it is shed,
  # and the episode's first names the baseline it seeds; only the pulled
  # snapshot ends it.
  def refuse_gap(%{last_persisted_seq: -1} = state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    unless state.gap_open? do
      {rc, sh, pk} = state.cell_id

      :telemetry.execute(
        [:flezha, :state, :baseline_seed],
        %{first_seq: seq},
        %{relay: rc, tasklet: sh, key: pk, runner: runner_id}
      )
    end

    Admission.open_gap(state, runner_id, :baseline_seed)
  end

  def refuse_gap(state, seq, runner_id) do
    Admission.emit_seq_gap(state.cell_id, state.last_persisted_seq + 1, seq)
    Admission.open_gap(state, runner_id, :seq_gap)
  end

  # Baseline established: a snapshot ahead of the WAL supersedes what the WAL
  # lacks (the deltas a cold takeover shed, or those a gap refused), so adopt
  # its seq and let the runner's next delta append contiguously. A held Change
  # above it keeps the gap open for the writer to append.
  def establish_baseline(%{last_persisted_seq: last} = state, seq) when seq > last do
    now = State.now_ms()
    Metrics.put(state.metrics, :wal_seq, seq)
    state = %{state | last_persisted_seq: seq, last_advance_at: now}

    if match?({{held, _, _, _}, _} when held > seq, state.held),
      do: state,
      else: state |> Admission.close_gap() |> Settlement.persist_dedup()
  end

  def establish_baseline(state, _seq), do: state

  # While a gap is open the newest refused Change is held, its credit with it:
  # the runner restates exactly one record above a pulled snapshot, for the
  # baseline to append.
  def hold(state, record, credit), do: %{Admission.release_held(state) | held: {record, credit}}

  # A pull refused for the runner's credit, which the held Change may hold.
  def pull_refused(state, runner_id),
    do: if(owner?(state, runner_id), do: Admission.release_held(state), else: state)

  # Walk forward to the first record at or after `target`; everything before it
  # is the baseline. A record with no event time (a plain change record from
  # pre-tier history) belongs to the baseline — it cannot be placed on the event
  # -time axis, so it cannot be said to come after the target.
  def scan_correction_baseline(state, target) do
    # Seq `-1` = no baseline seen yet, and the offset starts from the folded
    # one; a first record already at or after the target means the rewind
    # would have to go below the retained history.
    RecordFrame.fold_records(state.file_handle, state.wal_path, {-1, state.folded_offset}, fn
      {seq, header, body}, {seq_acc, offset_acc} ->
        case Wire.decode_effect_record(header, body) do
          %{event_time: event_time} when event_time >= target ->
            {:halt, baseline_result(seq_acc, offset_acc)}

          effect ->
            {:cont, {seq, max(offset_acc, consumed_offset_of(effect))}}
        end
    end)
    |> case do
      {:ok, {:ok, _} = ok} -> ok
      {:ok, {:error, _} = err} -> err
      # The fold ran off the end: nothing is at or after the target, so the input
      # is not actually late for this Cell.
      {:ok, {_seq, _offset}} -> {:error, :nothing_to_correct}
      # A record on the way to the baseline was refused; there is no baseline.
      {:error, _refusal} = refused -> refused
    end
  end

  defp baseline_result(seq, _offset) when seq < 0, do: {:error, :nothing_to_correct}
  defp baseline_result(seq, offset), do: {:ok, %{seq: seq, consumed_offset: offset}}

  @doc """
  The highest input offset `cell_id` consumed through seq `upto`, read from disk
  (`-1` for none): the folded offset once `upto` reaches the newest snapshot, and
  the WAL records through `upto`. `:latest` is every durable record: behind the
  remote gate those at or below the persisted flush cursor; a writer passes its own.
  """
  @spec floor(Paths.cell_id(), :latest | integer()) :: integer() | {:error, Frame.refusal()}
  def floor(cell_id, upto) do
    %{outbox_flushed_seq: flushed, folded_offset: folded} = Lineage.progress(cell_id)
    # An atom sorts above every integer: `:latest` passes every snapshot and record.
    seed = folded_through(cell_id, folded, upto)
    upto = if upto == :latest and RemoteGate.new(cell_id, :record), do: flushed, else: upto

    fold = fn
      {seq, _header, _body}, acc when seq > upto -> {:halt, acc}
      {_seq, header, body}, acc -> {:cont, max(acc, consumed(header, body))}
    end

    with {:ok, offset} <- RecordFrame.fold_path(Paths.wal_path(cell_id), seed, fold), do: offset
  end

  defp consumed(header, body), do: consumed_offset_of(Wire.decode_effect_record(header, body))

  @doc """
  Fold what `holder`'s records through its gate's bound consumed into its lineage
  record, then cut its WAL below `seq + 1`. `holder` is a writer's state, or an
  offline cut's with the persisted flush cursor as `outbox_durable_seq`. Answers
  `:ok`, `:none` or `{:error, reason}` (nothing cut) with the holder.
  """
  @spec cut_below(map(), non_neg_integer(), non_neg_integer()) :: {term(), map()}
  def cut_below(%{cell_id: {_, _, key} = cell_id} = holder, seq, min_bytes) do
    bound = RemoteGate.durable_through(holder, seq, holder.outbox_durable_seq)

    folded =
      case key not in Paths.reserved_cell_keys() and floor(cell_id, bound) do
        offset when is_integer(offset) and offset > holder.folded_offset ->
          Settlement.put_lineage(holder, %{epoch: holder.outbox_epoch, folded_offset: offset})

        _reserved_refused_or_folded ->
          {:ok, holder}
      end

    with {:ok, %{wal_path: path, file_handle: device, seq_index: index} = holder} <- folded do
      case RecordFrame.cut_below(path, device, index, seq + 1, min_bytes) do
        {:ok, _cut, file, index} -> {:ok, %{holder | file_handle: file, seq_index: index}}
        not_cut -> {not_cut, holder}
      end
    else
      failed -> {failed, holder}
    end
  end

  @doc """
  The inter-tasklet values `cell_id`'s WAL records kept for inputs above
  `floor`, in record order, read from the file alone.
  """
  @spec edges_above(Paths.cell_id(), integer()) :: [Wire.edge()]
  def edges_above(cell_id, floor) do
    case RecordFrame.fold_path(Paths.wal_path(cell_id), [], fn {_seq, header, body}, acc ->
           case Wire.decode_effect_record(header, body) do
             %{edges: [_ | _] = edges} ->
               {:cont, Enum.reverse(Enum.filter(edges, &above?(&1, floor)), acc)}

             _no_edges ->
               {:cont, acc}
           end
         end) do
      {:ok, edges} -> Enum.reverse(edges)
      {:error, _refusal} -> []
    end
  end

  defp above?(%{origin: origin}, floor), do: is_integer(origin) and origin > floor

  # `-1` for a plain change record, or an effect record that consumed nothing.
  defp consumed_offset_of(nil), do: -1
  defp consumed_offset_of(%{consumed_offset: nil}), do: -1
  defp consumed_offset_of(%{consumed_offset: offset}), do: offset

  defp emit_rewind({rc, sh, pk}, from_seq, to_seq, epoch) do
    :telemetry.execute(
      [:flezha, :state, :wal_rewind],
      %{from_seq: from_seq, to_seq: to_seq},
      %{relay: rc, tasklet: sh, key: pk, epoch: epoch}
    )
  end

  # The folded offset as far as `seq`: a cut is always at a snapshot's seq, so
  # below the newest snapshot it may claim records above `seq`, and counts nothing.
  defp folded_through(cell_id, folded, seq),
    do: if(seq >= highest_snapshot_seq(cell_id), do: folded, else: -1)

  defp highest_snapshot_seq(cell_id) do
    case Paths.snapshot_paths(cell_id) do
      [] -> 0
      paths -> paths |> List.last() |> elem(0)
    end
  end

  # ---------- Effect-record scan ----------

  @doc """
  The effect records above `cursor`, as outbox entries in record order, and
  the highest consumed offset the records at or below `durable` carry, or the
  writer's `folded_offset` (`-1` when neither holds one). Behind the remote
  gate a record not yet acknowledged reports its offset when its ship lands,
  through its dispatch, so `durable` is the acknowledged seq there and the last
  record otherwise.
  """
  def scan_effect_records_above(state, device, cursor, durable) do
    # Both callers walked this file to its end immediately before, so a record
    # that does not decode here is impossible, not a case to answer.
    {:ok, {acc, max_consumed}} =
      RecordFrame.fold_records(device, state.wal_path, {[], state.folded_offset}, fn
        {seq, header, body}, {acc, max_consumed} = unchanged ->
          case Wire.decode_effect_record(header, body) do
            nil ->
              {:cont, unchanged}

            effect ->
              entry = Settlement.entry(state, seq, effect)
              acc = if seq > cursor, do: [entry | acc], else: acc
              offset = if seq <= durable, do: consumed_offset_of(effect), else: -1
              {:cont, {acc, max(max_consumed, offset)}}
          end
      end)

    {Enum.reverse(acc), max_consumed}
  end

  @doc """
  Cut the WAL and the snapshots above `seq`, re-derive what the boundary left
  un-flushed from the rewritten file, and open the next epoch on it.

  Answers `{:ok, state}`, `{:error, reason, state}` with nothing cut when the
  lineage install fails, or `{:error, refusal, state}` when locating the
  boundary refused a record: the cut keeps `[0, bound)` and renames it into
  place, so a boundary taken at a record that did not decode would delete the
  valid records between it and the seq the caller asked to rewind to. The
  writer that owns the file then refuses to serve rather than cutting.
  """
  def rewind(state, seq, epoch) do
    changes = %{
      epoch: epoch,
      folded_offset: folded_through(state.cell_id, state.folded_offset, seq),
      cut: {:above, seq}
    }

    case Settlement.put_lineage(state, changes) do
      {:ok, state} -> rewind_disk(state, seq)
      {:error, reason} -> {:error, reason, state}
    end
  end

  # A rewind's disk step, run live and by a boot completing one.
  def rewind_disk(state, seq) do
    case RecordFrame.cut_above(state.wal_path, state.file_handle, state.seq_index, seq + 1) do
      {:ok, file, seq_index} -> {:ok, do_rewind(state, seq, file, seq_index)}
      {:error, refusal} -> {:error, refusal, %{state | refusal: refusal}}
    end
  end

  # The disk refused a write or a sync: nothing written since the last sync is
  # known durable, so it is cut, its credit released and its waiters refused,
  # retryably; a runner's Cell opens a gap, which pulls its state.
  def drop_unsynced(state, reason) do
    bound = state.seq_index.end - state.pending_bytes

    # Read through a fresh handle: the one the disk refused may not read.
    {:ok, reader} = :file.open(state.wal_path, [:raw, :read, :binary])

    floor =
      case RecordFrame.decode_record(reader, bound) do
        {:ok, seq, _header, _body, _consumed} -> seq
        _none -> state.last_persisted_seq + 1
      end

    _ = :file.close(reader)

    {:ok, file} = RecordFrame.cut_in_place(state.wal_path, state.file_handle, bound)

    unless state.disk_refused,
      do: Logger.warning("[wal] #{inspect(state.cell_id)}: disk refused #{inspect(reason)}")

    refusal = {:error, {:wal_writer_unavailable, reason}}
    Enum.each(state.sync_waiters, fn {from, _reply} -> GenServer.reply(from, refusal) end)
    Metrics.put(state.metrics, :wal_seq, floor - 1)
    Metrics.put(state.metrics, :wal_size_bytes, bound)
    state = RemoteGate.reset(state, floor - 1)

    state = %{
      state
      | file_handle: file,
        last_persisted_seq: floor - 1,
        seq_index: SeqIndex.cut_above(state.seq_index, floor, bound),
        header_index: nil,
        pending_bytes: 0,
        pending_since_us: nil,
        sync_waiters: [],
        pending_outbox: :queue.filter(&(&1.seq < floor), state.pending_outbox),
        disk_refused: reason
    }

    if state.owning_runner,
      do: Admission.open_gap(state, state.owning_runner, :unsynced),
      else: state
  end

  # A sync with a record written since the disk refused one ends the episode.
  def disk_back(%{disk_refused: refused, pending_bytes: bytes} = state)
      when refused != nil and bytes > 0 do
    Logger.warning("[wal] cell=#{inspect(state.cell_id)}: the disk takes writes again")
    %{state | disk_refused: nil}
  end

  def disk_back(state), do: state

  defp do_rewind(state, seq, file, seq_index) do
    # Only the snapshots ABOVE the boundary die. The ones at or below it are
    # exactly the baselines a restore to `seq` needs.
    Enum.each(Paths.snapshot_paths(state.cell_id), fn {snap_seq, path} ->
      if snap_seq > seq, do: File.rm(path)
    end)

    state = RemoteGate.reset(state, seq)

    # Records at or below the boundary are untouched history: those already
    # flushed stay flushed (re-emitting them under the new epoch would be churn
    # that overwrites each row with itself). Only what the boundary left
    # un-flushed is still owed. Re-derive it from the rewritten file rather than
    # filtering the in-memory queue — the file is the authority on what survived,
    # and the same scan yields the consumed-offset the input-log watermark must
    # fall back to: the rewound records reported this Cell's offsets, so the
    # watermark they advanced now claims inputs nothing has consumed, and the
    # next retention sweep would truncate the inputs the correction replays.
    flushed = min(state.outbox_flushed_seq, seq)
    durable = RemoteGate.durable_through(state, seq, state.outbox_durable_seq)
    {surviving, max_consumed} = scan_effect_records_above(state, file, flushed, durable)
    {relay_class, _, _} = state.cell_id
    InputLog.rewind_consumed(relay_class, state.cell_id, max_consumed)

    size = RecordFrame.file_size(state.wal_path)
    now = State.now_ms()
    Metrics.put(state.metrics, :wal_size_bytes, size)
    Metrics.put(state.metrics, :wal_seq, seq)
    Metrics.put(state.metrics, :snapshot_seq, highest_snapshot_seq(state.cell_id))
    emit_rewind(state.cell_id, state.last_persisted_seq, seq, state.lineage.epoch)

    state = %{
      state
      | file_handle: file,
        seq_index: seq_index,
        header_index: nil,
        last_persisted_seq: seq,
        last_advance_at: now,
        pending_bytes: 0,
        pending_since_us: nil,
        oldest_unsnapshotted_at: if(size > 0, do: now),
        # The correction re-seats the runner's Cell; its next record names the owner.
        owning_runner: nil
    }

    bump_epoch(state, :queue.from_list(surviving), durable, flushed)
  end
end
