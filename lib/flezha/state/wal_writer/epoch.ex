defmodule Flezha.State.WalWriter.Epoch do
  @moduledoc """
  Which session owns the WAL, and where its history starts.

  A runner restart, a takeover and a correction all say the same thing in
  different words: the records above some seq belong to a session that has
  stopped writing here. `realign_epoch/4` discards a stale pre-restart epoch whole,
  `rewind/3` cuts only above a boundary and keeps the baselines below it, and
  the takeover fence refuses a frame from a superseded session outright. Every
  one of them bumps the epoch, which is what makes a re-run's output supersede
  the run it replaces at a guarded sink. A disk that refused a write or a sync
  cuts what was written since the last sync (`drop_unsynced/2`) and keeps the
  epoch: the runner's current state, pulled again, becomes the baseline, and
  the Changes refused meanwhile are appended after it (`append_kept/2`).
  """

  require Logger

  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.State.{Frame, Wire}
  alias Flezha.State.{FsyncBatcher, InputLog, Lineage, Metrics, Paths, RecordFrame, SeqIndex}
  alias Flezha.State.WalWriter.{Admission, RemoteGate, Settlement, State}

  # Only a frame from a *different* session than the epoch owner consults the
  # takeover fence; the owning session short-circuits on the first comparison
  # (already made today for realign), so its append path gains no per-record
  # work.
  def stale_writer?(state, runner_id) do
    is_binary(runner_id) and runner_id != state.owning_runner and
      Marks.superseded_session?(runner_id)
  end

  # One `stale_writer_refused` event per superseded session per writer
  # lifetime (sessions are never reused, so the episode never re-opens);
  # every refusal increments the counter.
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

  # A runner restart is the only way a Change's seq can diverge under a *new*
  # runner id: within one runner's lifetime the seq is monotonic, so a low
  # seq from the same runner is a legitimate retransmit (deduped, not
  # realigned). `owning_runner == nil` (no Change seen since boot) defers to
  # the normal dedup/gap/append path so a controller restart still re-seeds the
  # alive runner via `RestoreInitState` instead of discarding the WAL.
  def realign?(state, runner_id, seq) do
    is_binary(runner_id) and is_binary(state.owning_runner) and
      runner_id != state.owning_runner and seq != state.last_persisted_seq + 1
  end

  # Discard the stale pre-restart epoch so a later `RestoreInitState` ships only
  # the new runner's history: empty the WAL file and drop its snapshots, then
  # reset the seq baseline so the incoming `seq` appends contiguously.
  def realign_epoch(%{outbox_epoch: epoch} = state, seq, runner_id, now) do
    with {:ok, state} <-
           Settlement.put_lineage(state, %{epoch: epoch + 1, folded_offset: -1, cut: :all}) do
      emit_realign(state.cell_id, state.last_persisted_seq, seq, runner_id)
      state = realign_disk(%{state | last_persisted_seq: seq - 1, owning_runner: runner_id})
      # The new runner's log starts here: nothing kept for a baseline applies.
      state = %{release_kept(state) | kept: {0, [], -1}, awaiting_baseline?: false}
      {:ok, %{state | last_advance_at: now}}
    end
  end

  # A realign's disk step, run live and by a boot completing one.
  def realign_disk(state) do
    # The truncated epoch's un-fsynced bytes have left the buffer;
    # release them to the stream ledger so the window is not leaked.
    state = RemoteGate.reset(state)
    {:ok, _} = :file.position(state.file_handle, 0)
    :ok = :file.truncate(state.file_handle)

    Enum.each(Paths.snapshot_paths(state.cell_id), fn {_seq, path} -> File.rm(path) end)

    Metrics.put(state.metrics, :wal_size_bytes, 0)
    Metrics.put(state.metrics, :snapshot_seq, 0)

    state = %{
      state
      | pending_bytes: 0,
        pending_since_us: nil,
        oldest_unsnapshotted_at: nil,
        header_index: nil,
        seq_index: SeqIndex.new()
    }

    # The stale lineage's undispatched outputs die with its records, so nothing
    # is flushed or queued; the realigned runner's replay regenerates them.
    bump_epoch(state, :queue.new(), state.last_persisted_seq, -1)
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

  # The half a realign and a rewind share: re-seat the outbox cursors on the
  # lineage record's epoch, which keys an output regenerated above the boundary
  # apart from its own pre-boundary delivery, then end the cut and write the
  # dedup file **synchronously**.
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

  # Cold-takeover shed: no baseline on this controller, so the frame can't
  # append. Count it and declare the gap, so the loop drives a CompactCell
  # pull (the resync routes to the pull because the Cell has no persisted
  # state). The pulled snapshot encodes the shed deltas' effects; only that
  # snapshot clears this state, and the loop retries the pull on its table
  # until it does.
  def await_baseline(state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    if state.awaiting_baseline? do
      state
    else
      {rc, sh, pk} = state.cell_id

      :telemetry.execute(
        [:flezha, :state, :baseline_seed],
        %{first_seq: seq},
        %{relay: rc, tasklet: sh, key: pk, runner: runner_id}
      )

      # The log lacks every seq below the first one shipped to it.
      pull_baseline(state, runner_id, :baseline_seed, seq - 1)
    end
  end

  # The log lacks changes the runner applied, up to `top` at least: declare
  # the gap and pull its state as the baseline (`Flezha.State.Recovery` never
  # pushes meanwhile). The episode remembers the highest seq it lost, so only
  # a baseline at or above it ends it (`Admission.close_gap/1`).
  defp pull_baseline(%{kept: {bytes, kept, lost}} = state, runner_id, trigger, top \\ -1) do
    state = %{state | awaiting_baseline?: true, kept: {bytes, kept, max(lost, top)}}
    Admission.open_gap(state, runner_id, trigger)
  end

  # Baseline established: a snapshot ahead of the WAL supersedes what the WAL
  # lacks (the deltas a cold takeover shed, or those a gap refused), so adopt
  # its seq and let the runner's next delta append contiguously. A pulled
  # baseline ends its episode in `append_kept/2` instead.
  def establish_baseline(%{last_persisted_seq: last} = state, seq) when seq > last do
    Metrics.put(state.metrics, :wal_seq, seq)
    state = %{state | last_persisted_seq: seq, last_advance_at: State.now_ms()}

    if state.awaiting_baseline?,
      do: state,
      else: state |> Admission.close_gap() |> Settlement.persist_dedup()
  end

  def establish_baseline(state, _seq), do: state

  # The bookkeeping a prefix cut owes once the file is shorter: the rewrite
  # fsynced the kept tail, so everything pending is durable.
  def after_prefix_cut(state, seq) do
    size = RecordFrame.file_size(state.wal_path)
    Metrics.put(state.metrics, :wal_size_bytes, size)

    # Surviving tail records post-date the snapshot's baseline by at most the
    # compaction round-trip, so "now" is a safe age floor for the next trigger.
    oldest = if size > 0, do: State.now_ms()

    state = RemoteGate.drained(state)
    state = establish_baseline(state, seq)

    state = %{state | pending_bytes: 0, pending_since_us: nil, oldest_unsnapshotted_at: oldest}

    # Rotation/compaction is one of the dedup file's advance points.
    state |> Settlement.persist_dedup() |> Admission.reply_synced()
  end

  # A Change refused while a baseline is pulled is kept, in order, for that
  # baseline to append: the runner shipped it after the state the snapshot
  # holds, or with it (the re-stated record at `seq + 1` that precedes the
  # snapshot frame). Its credit waits with it, counted in the bytes the stream
  # window debited, under `limits.kept_cap`: past it the older kept Changes are
  # released, and the newest too if it alone does not fit, so the runner's
  # window always has room for the pull's snapshot. Any other gap drains at once.
  def refused(%{awaiting_baseline?: true, kept: {bytes, kept, lost}} = state, record, credit) do
    {_seq, header, body, _runner_id} = record
    size = Map.get(credit, :wire_size, byte_size(header) + byte_size(body))

    cond do
      bytes + size <= state.limits.kept_cap ->
        %{state | kept: {bytes + size, [{record, credit} | kept], lost}}

      kept != [] ->
        refused(release_kept(state), record, credit)

      true ->
        release_kept(%{state | kept: {size, [{record, credit}], lost}})
    end
  end

  def refused(state, _record, credit), do: RemoteGate.released(state, credit)

  # After a baseline lands: append the kept Changes above it, oldest first,
  # each through `write` (the writer's own append), its credit drained by the
  # sync that follows. One at or below the baseline is covered by it and drains
  # now. A hole, or a disk that refuses again, releases the rest and pulls
  # again. A baseline below the highest seq the episode lost (a retention cut,
  # an older snapshot) is not the pulled one: everything stays as it is.
  def append_kept(%{awaiting_baseline?: false} = state, _write), do: state

  def append_kept(%{kept: {_bytes, _kept, lost}} = state, _write)
      when state.last_persisted_seq < lost,
      do: state

  def append_kept(%{kept: {_bytes, kept, lost}} = state, write) do
    case write_kept(%{state | kept: {0, [], lost}}, Enum.reverse(kept), write) do
      {:ok, state} ->
        state |> Admission.close_gap() |> Settlement.persist_dedup()

      {:hole, runner_id, rest, state} ->
        state = release_kept(%{state | kept: put_elem(state.kept, 1, rest)})
        pull_baseline(state, runner_id, :baseline_seed)
    end
  end

  defp write_kept(state, [], _write), do: {:ok, state}

  defp write_kept(state, [{{seq, _, _, _}, credit} | rest], write)
       when seq <= state.last_persisted_seq,
       do: write_kept(RemoteGate.released(state, credit), rest, write)

  defp write_kept(state, [{{seq, header, body, runner_id}, credit} | rest] = kept, write)
       when seq == state.last_persisted_seq + 1 do
    case write.(state, seq, header, body, runner_id, State.now_ms()) do
      {:reply, :ok, state} -> write_kept(Admission.track_credit(state, credit), rest, write)
      {:reply, _refused, state} -> {:hole, runner_id, kept, state}
      {:stop, _reason, _dropped, state} -> {:hole, runner_id, kept, state}
    end
  end

  defp write_kept(state, [{{_seq, _, _, runner_id}, _credit} | _] = kept, _write),
    do: {:hole, runner_id, kept, state}

  # A runner refused a pull for credit, which the Changes kept from it may
  # hold: they are let go, and the baseline the next pull brings must still
  # reach the seqs let go.
  def pull_refused(%{kept: {_bytes, [{{_, _, _, runner_id}, _} | _], _}} = state, runner_id),
    do: release_kept(state)

  def pull_refused(state, _runner_id), do: state

  @doc """
  Lets go of every kept Change, its credit released so no marker confirms the
  input behind it, and remembers the highest seq let go as lost to the episode.
  """
  def release_kept(%{kept: {_bytes, kept, lost}} = state) do
    top =
      Enum.reduce(kept, lost, fn {{seq, _, _, _}, credit}, top ->
        Admission.release_credit(state, credit, :credit_released)
        max(seq, top)
      end)

    %{state | kept: {0, [], top}}
  end

  defp emit_realign({rc, sh, pk}, from_seq, to_seq, runner_id) do
    :telemetry.execute(
      [:flezha, :state, :wal_realign],
      %{from_seq: from_seq, to_seq: to_seq},
      %{relay: rc, tasklet: sh, key: pk, runner: runner_id}
    )
  end

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

  # The disk refused a write or a sync. Nothing written since the last sync
  # was answered or drained, and none of it is known durable, so it goes: cut
  # from the file in place, its credit released, its waiters refused,
  # retryably, and, for a runner's Cell, a baseline pulled from its runner
  # (`append_kept/2`), since a push of this log would rewind the live Cell.
  # The gate lets go of it before the seq falls back to the last synced one.
  # `refused_seq`, a write the disk refused, is lost with them.
  def drop_unsynced(state, reason, refused_seq \\ -1) do
    lost = max(state.last_persisted_seq, refused_seq)
    bound = state.seq_index.end - state.pending_bytes
    locate = fn file -> {:ok, bound, first_seq(file, bound, state.last_persisted_seq + 1)} end
    {:ok, file, floor} = RecordFrame.cut_in_place(state.wal_path, state.file_handle, locate)

    if state.disk_refused == nil do
      Logger.warning(
        "[wal] cell=#{inspect(state.cell_id)}: the disk refused a write or a sync " <>
          "(#{inspect(reason)}); records from seq #{floor} on are dropped, to be sent again"
      )
    end

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
      do: pull_baseline(state, state.owning_runner, :unsynced, lost),
      else: state
  end

  # A sync with a record written since the disk refused one ends the episode.
  def disk_back(state) do
    Logger.warning("[wal] cell=#{inspect(state.cell_id)}: the disk takes writes again")
    %{state | disk_refused: nil}
  end

  # The seq of the first whole record at `position`, else `default`.
  defp first_seq(file, position, default) do
    case RecordFrame.decode_record(file, position) do
      {:ok, seq, _header, _body, _consumed} -> seq
      _none -> default
    end
  end

  defp do_rewind(state, seq, file, seq_index) do
    # Only the snapshots ABOVE the boundary die. The ones at or below it are
    # exactly the baselines a restore to `seq` needs — dropping them all, as a
    # realign does, would be a wipe, not a rewind.
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
        oldest_unsnapshotted_at: if(size > 0, do: now)
    }

    bump_epoch(state, :queue.from_list(surviving), durable, flushed)
  end
end
