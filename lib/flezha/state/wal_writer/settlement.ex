defmodule Flezha.State.WalWriter.Settlement do
  @moduledoc """
  The writer's effectively-once outbox: which durable entries are owed, which
  are in flight, and when the cursor may pass one.

  An effect record's outputs queue on append and dispatch only once the record
  is durable. An entry completes when every output has settled and every hold
  has answered; the cursor then drains over the leading run of completed
  entries, so an out-of-order confirmation can never carry it past output no
  sink has taken. Back-pressure here is queueing, never loss — with the window
  full the entries stay on disk until a sink answers.
  """

  require Logger

  alias Flezha.Dlq.Entry, as: DlqEntry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Config.Durations
  alias Flezha.Operative.Lookup
  alias Flezha.SDK.Wire
  alias Flezha.State.{DurabilityTier, InputLog, Outbox, Paths}
  alias Flezha.Process.Timer
  alias Flezha.State.WalWriter.State

  @reserved_entity_keys Paths.reserved_entity_keys()

  # ---------- Effectively-once outbox (fsync-gated flush) ----------

  # Queue an effect record's outputs for the post-fsync dispatch. Plain
  # change records pay one header-byte check and nothing else.
  def maybe_queue_effect(state, seq, header, body) do
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
  def try_dispatch_outbox(state) do
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

          case Lookup.overwatch(rc) do
            {:ok, pid} -> dispatch_outbox_window(state, pid)
            {:error, _reason} -> schedule_outbox_retry(state)
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

          # An entry with no outputs completes inside `await_entry/4`, which
          # re-enters dispatch: the entry's own durable effects have to be
          # written before a nested window can advance the watermark past them.
          apply_effect(state, entry)
          state = await_entry(%{state | pending_outbox: rest}, key, entry, length(outputs))

          take_outbox_window(state, [outputs | chunks], furthest_consumed(consumed, entry))
        end

      _ ->
        {state, chunks, consumed}
    end
  end

  # The window counts entries inside the plugin chain unconfirmed — exactly the
  # awaiting set. `:queue.len/1` on the inflight queue is O(n), which made every
  # refill of a 1 000-deep window quadratic.
  def window_full?(state),
    do: map_size(state.outbox_awaiting) >= state.limits.outbox_inflight_max

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
          DlqStore.append(
            DlqEntry.new!(%{
              operative_class: rc,
              procedure_id: sh,
              entity_key: pk,
              channel: dlq.channel,
              origin_offset: entry.consumed_offset,
              error: dlq.error,
              attempts: dlq.attempts,
              payload: dlq.payload
            })
          )
    end

    # An integer offset on a checkpoint-ack class means an interval just closed:
    # state and source position are consistent at exactly this cut, which is the
    # cut the remote durability tier wants. Every other class reaches the tier
    # through compaction, a commit barrier, or the backstop sweep.
    DurabilityTier.on_effect_flushed(state.entity_id, entry.consumed_offset)
  end

  def schedule_outbox_retry(state) do
    if Timer.armed?(state.timers, :outbox_retry) do
      state
    else
      %{
        state
        | timers:
            Timer.arm(state.timers, :outbox_retry, Durations.get(:outbox_retry_ms), :outbox_retry)
      }
    end
  end

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
  def register_entry(state, key, outputs, entry) do
    tally = %{
      pending: outputs,
      holds: 0,
      confirms: 0,
      done?: false,
      entry: entry,
      at: State.now_ms()
    }

    %{state | outbox_awaiting: Map.put(state.outbox_awaiting, key, tally)}
    |> tally(key, & &1)
  end

  defp await_entry(state, {:cursor, seq} = key, entry, outputs) do
    %{state | outbox_inflight: :queue.in({seq, State.now_ms()}, state.outbox_inflight)}
    |> register_entry(key, outputs, entry)
    |> schedule_outbox_retry()
  end

  # A batch report carries the same per-entry facts as the single-key reports
  # it replaces, so both shapes fold through the one tally.
  def settled(state, {key, holds}),
    do: tally(state, key, &%{&1 | pending: &1.pending - 1, holds: &1.holds + holds})

  def confirmed(state, key), do: tally(state, key, &%{&1 | confirms: &1.confirms + 1})

  def tally(state, key, fun) do
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
  def complete?(%{pending: pending, holds: holds, confirms: confirms}),
    do: pending <= 0 and confirms >= holds

  # A chain run that never reported back — the Overwatch died between the cast
  # and the settlement — leaves the entry owed with nothing in flight to answer
  # it. Re-dispatch its outputs under their identical identity (conforming sinks
  # dedup; the record's own side effects already landed) and
  # start the tally over: a hold the lost run took would otherwise never be
  # answered, wedging the cursor for good. The journal's entries carry no copy —
  # dropping the tally is what lets the next journal-driven flush re-drive them.
  def resettle_outbox(state) do
    if resettle_due?(state) do
      {rc, _sh, _pk} = state.entity_id
      overwatch = Lookup.overwatch(rc)

      {state, chunks} =
        Enum.reduce(state.outbox_awaiting, {state, []}, fn {key, tally}, {acc, chunks} ->
          if tally.pending > 0 and State.now_ms() - tally.at >= acc.limits.outbox_resettle_ms,
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
  def resettle_due?(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {_seq, at}} -> State.now_ms() - at >= state.limits.outbox_resettle_ms
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
  def unconfirmed_age_ms(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {_seq, at}} -> State.now_ms() - at
      :empty -> 0
    end
  end

  # Advance the dedup file (persistence watermark + flush cursor + epoch)
  # with one atomic tmp+rename — at most once per fsync batch. The reserved
  # per-instance identities (proc-journal, durable inbox) skip it: neither carries
  # effect records, and the dedup side-file would register a phantom Entity
  # with `Paths.scan_dir/1`.
  def persist_dedup(%{entity_id: {_, _, key}} = state) when key in @reserved_entity_keys,
    do: state

  def persist_dedup(state) do
    # The cursors as one record; only the persistence watermark is clamped,
    # because the dedup file's own guard has no room for the empty-log `-1`.
    progress = %{State.progress(state) | last_persisted_seq: max(state.last_persisted_seq, 0)}

    # An out-of-order record is not written. The cursor may lag and never lead
    # (`Flezha.State.DurabilityTier.Hydrator`), so keeping the older, consistent
    # record re-drives work the sinks dedup — writing a flush cursor ahead of
    # what is durable would drop those outputs for good.
    if State.ordered?(progress) do
      _ = Outbox.write(state.entity_id, progress)
    else
      Logger.error(
        "[state] refusing to persist an out-of-order cursor record for " <>
          "#{inspect(state.entity_id)}: #{inspect(progress)}"
      )
    end

    state
  end
end
