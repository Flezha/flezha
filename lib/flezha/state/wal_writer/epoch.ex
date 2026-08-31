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

  alias Flezha.Runner.FleetManager
  alias Flezha.State.{InputLog, Metrics, Paths, RecordFrame, Wire}
  alias Flezha.State.WalWriter.{Admission, Settlement, State}

  # Only a frame from a *different* session than the epoch owner consults the
  # takeover fence; the owning session short-circuits on the first comparison
  # (already made today for realign), so its append path gains no per-record
  # work.
  def stale_writer?(state, runner_id) do
    is_binary(runner_id) and runner_id != state.owning_runner and
      FleetManager.superseded_session?(runner_id)
  end

  # One `stale_writer_refused` event per superseded session per writer
  # lifetime (sessions are never reused, so the episode never re-opens);
  # every refusal increments the counter.
  def refuse_stale(state, runner_id) do
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
  def realign?(state, runner_id, seq) do
    is_binary(runner_id) and is_binary(state.owning_runner) and
      runner_id != state.owning_runner and seq != state.last_persisted_seq + 1
  end

  # Discard the stale pre-restart epoch so a later `RestoreInitState` ships only
  # the new runner's history: empty the WAL file and drop its snapshots, then
  # reset the seq baseline so the incoming `seq` appends contiguously.
  def realign_epoch(state, seq, runner_id, now) do
    # The truncated epoch's un-fsynced bytes have left the buffer;
    # release them to the stream ledger so the window is not leaked.
    state = Admission.flush_credit_pending(state)
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

    Settlement.persist_dedup(state)
  end

  # Cold-takeover shed: no baseline on this controller, so the frame can't
  # append. Count it and drive a CompactEntity pull via `Recovery.resync`
  # (routes to the pull because the Entity has no persisted state). The pulled
  # snapshot encodes the shed deltas' effects. The pull is retried on the
  # resync interval for as long as the Entity waits: only that snapshot clears
  # this state, so a pull that failed to dispatch must not be the last one.
  def await_baseline(state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    unless state.awaiting_baseline? do
      {rc, sh, pk} = state.entity_id

      :telemetry.execute(
        [:flezha, :state, :baseline_seed],
        %{first_seq: seq},
        %{operative: rc, procedure: sh, key: pk, runner: runner_id}
      )
    end

    Admission.resync_rate_limited(%{state | awaiting_baseline?: true}, runner_id, :baseline_seed)
  end

  # Baseline established: the pulled snapshot's seq supersedes the shed
  # deltas, so adopt it and let the runner's next delta append contiguously.
  def establish_baseline(%{awaiting_baseline?: true, last_persisted_seq: last} = state, seq)
      when seq > last do
    now = State.now_ms()
    Metrics.put(state.metrics, :wal_seq, seq)

    state = %{
      state
      | awaiting_baseline?: false,
        last_resync_at: nil,
        last_persisted_seq: seq,
        last_advance_at: now
    }

    Settlement.persist_dedup(state)
  end

  def establish_baseline(state, _seq), do: %{state | awaiting_baseline?: false}

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
  def scan_correction_baseline(device, target) do
    # `{-1, -1}` = no baseline seen yet; a first record already at or after the
    # target means the rewind would have to go below the retained history.
    RecordFrame.fold_records(device, {-1, -1}, fn {seq, header, body}, {seq_acc, offset_acc} ->
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

  def scan_consumed_at(device, upto) do
    RecordFrame.fold_records(device, -1, fn {seq, header, body}, acc ->
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

  # ---------- Effect-record scan ----------

  def scan_effect_records_above(device, cursor) do
    {acc, max_consumed} =
      RecordFrame.fold_records(device, {[], nil}, fn {seq, header, body},
                                                     {acc, max_consumed} = unchanged ->
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

  # Cut the WAL and the snapshots above `seq`, re-derive what the boundary left
  # un-flushed from the rewritten file, and open the next epoch on it.
  def rewind(state, seq, epoch) do
    {:ok, file, seq_index} =
      RecordFrame.cut_above(state.wal_path, state.file_handle, state.seq_index, seq + 1)

    # Only the snapshots ABOVE the boundary die. The ones at or below it are
    # exactly the baselines a restore to `seq` needs — dropping them all, as a
    # realign does, would be a wipe, not a rewind.
    Enum.each(Paths.snapshot_paths(state.entity_id), fn {snap_seq, path} ->
      if snap_seq > seq, do: File.rm(path)
    end)

    state = Admission.flush_credit_pending(state)

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

    size = RecordFrame.file_size(state.wal_path)
    now = State.now_ms()
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

    bump_epoch(state, %{
      pending: :queue.from_list(surviving),
      durable_seq: seq,
      flushed_seq: flushed,
      epoch: epoch
    })
  end
end
