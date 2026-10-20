defmodule Flezha.State.WalWriter.Epoch do
  @moduledoc """
  Which session owns the WAL, and where its history starts.

  A runner restart, a takeover and a correction all say the same thing in
  different words: the records above some seq belong to a session that has
  stopped writing here. `realign_epoch/4` discards a stale pre-restart epoch whole,
  `rewind/3` cuts only above a boundary and keeps the baselines below it, and
  the takeover fence refuses a frame from a superseded session outright. Every
  one of them bumps the epoch, which is what makes a re-run's output supersede
  the run it replaces at a guarded sink.
  """

  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.State.Wire
  alias Flezha.State.{InputLog, Lineage, Metrics, Paths, RecordFrame, SeqIndex}
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
    with {:ok, state} <- bump(state, %{epoch: epoch + 1, folded_offset: -1, cut: :all}) do
      emit_realign(state.cell_id, state.last_persisted_seq, seq, runner_id)
      state = realign_disk(%{state | last_persisted_seq: seq - 1, owning_runner: runner_id})
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

  # The lineage record a bump or a prefix cut writes before its disk step; a
  # failed install refuses the request with the writer as it was. A bump's cut
  # stays until the dedup write after the disk step clears it in this record
  # first: the dedup file is not synced, so only a synced record ends the cut.
  def bump(state, changes) do
    lineage = Map.merge(state.lineage, changes)

    with :ok <- Lineage.put(state.cell_id, lineage),
         do: {:ok, %{state | lineage: lineage, folded_offset: lineage.folded_offset}}
  end

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

    unless state.awaiting_baseline? do
      {rc, sh, pk} = state.cell_id

      :telemetry.execute(
        [:flezha, :state, :baseline_seed],
        %{first_seq: seq},
        %{relay: rc, tasklet: sh, key: pk, runner: runner_id}
      )
    end

    Admission.open_gap(%{state | awaiting_baseline?: true}, runner_id, :baseline_seed)
  end

  # Baseline established: a snapshot ahead of the WAL supersedes what the WAL
  # lacks (the deltas a cold takeover shed, or those a gap refused), so adopt
  # its seq and let the runner's next delta append contiguously.
  def establish_baseline(%{last_persisted_seq: last} = state, seq) when seq > last do
    now = State.now_ms()
    Metrics.put(state.metrics, :wal_seq, seq)

    state =
      Admission.close_gap(%{
        state
        | awaiting_baseline?: false,
          last_persisted_seq: seq,
          last_advance_at: now
      })

    Settlement.persist_dedup(state)
  end

  def establish_baseline(state, _seq), do: %{state | awaiting_baseline?: false}

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

  def scan_consumed_at(state, upto) do
    seed = folded_through(state, upto)

    case RecordFrame.fold_records(state.file_handle, state.wal_path, seed, consumed_through(upto)) do
      {:ok, offset} -> offset
      {:error, _refusal} = refused -> refused
    end
  end

  @doc """
  The highest consumed offset `cell_id`'s WAL records carry, or its
  `folded_offset` (`Flezha.State.Lineage`), read from disk alone; `nil` when neither holds one.
  Behind the remote gate only the records at or below the persisted flush
  cursor count.
  """
  @spec consumed_offset_on_disk(Paths.cell_id()) :: non_neg_integer() | nil
  def consumed_offset_on_disk(cell_id) do
    dedup = Lineage.progress(cell_id)
    upto = if RemoteGate.new(cell_id, :record), do: dedup.outbox_flushed_seq, else: :infinity
    fold = consumed_through(upto)

    case RecordFrame.fold_path(Paths.wal_path(cell_id), dedup.folded_offset, fold) do
      {:ok, offset} when offset >= 0 -> offset
      _none_or_refused -> nil
    end
  end

  # The fold of the highest consumed offset the records up to seq `upto` carry.
  defp consumed_through(upto) do
    fn {seq, header, body}, acc ->
      if seq > upto,
        do: {:halt, acc},
        else: {:cont, max(acc, consumed_offset_of(Wire.decode_effect_record(header, body)))}
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
  defp folded_through(state, seq),
    do: if(seq >= highest_snapshot_seq(state.cell_id), do: state.folded_offset, else: -1)

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
    changes = %{epoch: epoch, folded_offset: folded_through(state, seq), cut: {:above, seq}}

    case bump(state, changes) do
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
