defmodule Flezha.State.WalWriter.Settlement do
  @moduledoc """
  The writer's outbox, for a flow relay Cell under the effectively-once tier and a
  durable steps relay Cell alike: which durable entries are owed, which are in
  flight, and when the cursor may pass one.

  An effect record's outputs queue on append and dispatch only once the record
  is durable and its kind has committed it: a flow relay's record is its own
  commit; a steps relay Cell's entry is held until the stage marker naming it is
  durable (`commit_records/3`), and one that no marker will ever name (an
  interrupted, failed or unwound attempt's) is dropped without reaching the
  chain. An entry completes when every output has settled and every hold has
  answered; the cursor then drains over the leading run of completed entries,
  so an out-of-order confirmation can never carry it past output no sink has
  taken. Back-pressure here is queueing, never loss: with the window full the
  entries stay on disk until a sink answers.
  """

  require Logger

  alias Flezha.Dlq.Entry, as: DlqEntry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.Token
  alias Flezha.Relay.Lookup
  alias Flezha.State.Wire
  alias Flezha.State.{DurabilityTier, InputLog, Lineage, Outbox, Paths}
  alias Flezha.Process.Timer
  alias Flezha.State.WalWriter.State

  @reserved_cell_keys Paths.reserved_cell_keys()
  @delivery_hold Outbox.delivery_hold()
  @reply_to Token.reply_to_key()
  @call Token.call_key()

  # ---------- The outbox (fsync- and commit-gated flush) ----------

  @doc """
  One outbox entry for the effect record at `seq`: committed on arrival for a
  writer whose record is its own commit, held for a stage-committing one.
  """
  def entry(state, seq, decoded),
    do: %{
      seq: seq,
      outputs: decoded.outputs,
      consumed_offset: decoded.consumed_offset,
      dlq: decoded.dlq,
      committed?: state.commit_unit == :record
    }

  # The one gate: an entry leaves once its record is durable and its kind has
  # committed it.
  defguardp dispatchable(entry, state)
            when entry.committed? and entry.seq <= state.outbox_durable_seq

  # Queue an effect record's outputs for the post-fsync dispatch. Plain
  # change records pay one header-byte check and nothing else.
  def maybe_queue_effect(state, seq, header, body) do
    if Wire.effect_record_header?(header) do
      case Wire.decode_effect_body(body) do
        {:ok, decoded} ->
          entry = entry(state, seq, decoded)

          %{
            state
            | pending_outbox: :queue.in(entry, state.pending_outbox),
              outbox_last_seq: owed_last_seq(state.outbox_last_seq, entry)
          }

        {:error, reason} ->
          {rc, sh, pk} = state.cell_id

          Logger.warning(
            "[wal] effect record body undecodable cell=#{rc}/#{sh}/#{pk} " <>
              "seq=#{seq}: #{inspect(reason)} — outputs not flushed"
          )

          state
      end
    else
      state
    end
  end

  # Dispatch every leading durable entry, in seq order, to the relay's
  # `:on_output` chain with the durable identity `(flz.cell, outbox_seq,
  # epoch)`. Undispatchable (Overwatch not yet bound) → bounded retry timer;
  # the queue is bounded by the credit window like the un-fsynced buffer.
  #
  # Runs on every event that can open the window: an fsync (new durable
  # entries) and a completed confirmation (a free slot). A confirmation that did
  # not refill would leave the tail of a burst to the 1 s retry tick, one window
  # per tick.
  def try_dispatch_outbox(state) do
    case :queue.peek(state.pending_outbox) do
      {:value, entry} when dispatchable(entry, state) ->
        if window_full?(state) do
          state
        else
          {rc, _sh, _pk} = state.cell_id

          case Lookup.overwatch(rc) do
            {:ok, pid} -> dispatch_outbox_window(state, pid)
            {:error, _reason} -> schedule_outbox_retry(state)
          end
        end

      _empty_or_held ->
        state
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
      {{:value, entry}, rest} when dispatchable(entry, state) ->
        if window_full?(state) do
          {state, chunks, consumed}
        else
          key = {:cursor, entry.seq}
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
    # The token names the entry, and a Cell's writer owns its own output, so
    # one token covers every output of the entry.
    ack = Outbox.token(state.cell_id, key)

    entry.outputs
    |> Enum.with_index()
    |> Enum.map(fn {output, ordinal} ->
      Outbox.stamp(output.payload, %{
        owner: state.cell_id,
        entry: key,
        cell: state.cell_id,
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
    {rc, _sh, _pk} = state.cell_id
    InputLog.record_consumed(rc, state.cell_id, offset)
  end

  # The durable side effects the record carries beyond its outputs. They belong
  # to the record, not to a delivery attempt, so a re-dispatch does not repeat
  # them.
  defp apply_effect(state, entry) do
    {rc, sh, pk} = state.cell_id

    # Materialize a dead-letter park BEFORE the pass's consumed-offset advance:
    # the watermark move can truncate the poison input out of the
    # input-log, so the DLQ store entry must be durable first (synchronous
    # — the append is fsynced and idempotent, so a boot re-drive of the
    # same record lands on the existing entry and tells no listener). Parks
    # are rare; the hot path pays one nil check.
    case Map.get(entry, :dlq) do
      nil ->
        :ok

      dlq ->
        parked =
          DlqStore.append(
            DlqEntry.new!(%{
              relay_class: rc,
              tasklet_id: sh,
              cell_key: pk,
              channel: dlq.channel,
              origin_offset: entry.consumed_offset,
              error: dlq.error,
              attempts: dlq.attempts,
              payload: dlq.payload
            })
          )

        # A caller waiting on the parked input rides its input-log record.
        token =
          if is_integer(entry.consumed_offset), do: InputLog.reply_to(rc, entry.consumed_offset)

        DlqStore.announce(rc, parked, %{
          token: token,
          error: dlq.error,
          channel: dlq.channel,
          offset: entry.consumed_offset,
          tasklet: sh,
          attempts: dlq.attempts
        })
    end

    # An integer offset on a checkpoint-ack class means an interval just closed:
    # state and source position are consistent at exactly this cut, which is the
    # cut the remote durability tier wants. Every other class reaches the tier
    # through compaction, a commit barrier, or the backstop sweep; a Cell behind
    # the remote gate shipped this cut before it was dispatched.
    if is_nil(state.remote),
      do: DurabilityTier.on_effect_flushed(state.cell_id, entry.consumed_offset)
  end

  @doc """
  A steps relay Cell's records `first..last` are named by a durable stage marker:
  release them. A held entry below `first` belongs to an attempt of an earlier
  stage that never committed (a stage's attempts on one Cell run one after
  another, and each commit names the attempt that succeeded), so it is dropped
  without reaching the chain.
  """
  def commit_records(state, first, last) do
    state
    |> settle_held(fn
      %{seq: seq} when seq >= first and seq <= last -> :commit
      %{seq: seq} when seq < first -> :drop
      _entry -> :hold
    end)
    |> try_dispatch_outbox()
  end

  @doc """
  Re-derive which held entries are committed from the journal's live entries,
  `committed` being the record ranges they name per Cell
  (`TaskJournal.committed_records/1`). An entry outside every range of this
  Cell and below the last is an attempt that never committed, or one a rewind
  unwound, and is dropped. One above the last belongs to an attempt still
  running when a writer boots (it stays held) or, at a bind, to the attempt
  the rebound runner interrupted, and it is dropped.
  """
  def seed_commits(%{cell_id: {rc, sh, pk}} = state, committed, above)
      when above in [:hold, :drop] do
    ranges = Map.get(committed, Wire.encode_cell_id(rc, sh, pk), [])
    last = ranges |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> -1 end)

    state
    |> settle_held(fn %{seq: seq} ->
      cond do
        Enum.any?(ranges, fn {f, l} -> seq >= f and seq <= l end) -> :commit
        seq < last -> :drop
        true -> above
      end
    end)
    |> try_dispatch_outbox()
  end

  # Every held entry's fate by `decide`; a committed one keeps its place, a
  # dropped one leaves the queue, and the producing-liveness cursor counts the
  # newly owed.
  defp settle_held(state, decide) do
    {kept, last} =
      state.pending_outbox
      |> :queue.to_list()
      |> Enum.flat_map_reduce(state.outbox_last_seq, fn
        %{committed?: true} = entry, last ->
          {[entry], last}

        entry, last ->
          case decide.(entry) do
            :commit ->
              committed = %{entry | committed?: true}
              {[committed], owed_last_seq(last, committed)}

            :drop ->
              {[], last}

            :hold ->
              {[entry], last}
          end
      end)

    %{state | pending_outbox: :queue.from_list(kept), outbox_last_seq: last}
  end

  # The highest committed, output-bearing entry: what the classifier compares
  # against the flush cursor to see an outbox that grows without draining. A
  # held entry is not owed yet, however long its stage takes.
  defp owed_last_seq(last, %{committed?: true, outputs: [_ | _], seq: seq}), do: max(last, seq)
  defp owed_last_seq(last, _entry), do: last

  @doc "Entries owed a dispatch now, and entries held for a stage marker."
  def queued(state) do
    Enum.reduce(:queue.to_list(state.pending_outbox), {0, 0}, fn
      %{committed?: true}, {owed, held} -> {owed + 1, held}
      _held, {owed, held} -> {owed, held + 1}
    end)
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
  # Registration always precedes the reports (the dispatch registers inside
  # the handler that casts), so a report against an unknown entry is one a
  # resettle superseded, dropped because the live registration owns the entry
  # now. `entry` is retained for exactly that resettle.
  defp register_entry(state, key, outputs, entry) do
    tally = %{
      pending: outputs,
      holds: 0,
      confirms: 0,
      entry: entry,
      at: State.now_ms()
    }

    %{state | outbox_awaiting: Map.put(state.outbox_awaiting, key, tally)}
    |> tally(key, & &1)
  end

  defp await_entry(state, key, entry, outputs) do
    %{state | outbox_inflight: :queue.in({key, State.now_ms()}, state.outbox_inflight)}
    |> register_entry(key, outputs, entry)
    |> schedule_outbox_retry()
  end

  # A batch report carries the same per-entry facts as the single-key reports
  # it replaces, so both shapes fold through the one tally.
  def settled(state, {key, holds}),
    do: tally(state, key, &%{&1 | pending: &1.pending - 1, holds: &1.holds + holds})

  def confirmed(state, key), do: tally(state, key, &%{&1 | confirms: &1.confirms + 1})

  @doc """
  A relay's call or answer took a hold outside a dispatcher run, or its
  delivery answered one. Either weighs `Outbox.delivery_hold/0`, as a delivery
  hold inside a run does.
  """
  def delivery(state, key, :held),
    do: tally(state, key, &%{&1 | holds: &1.holds + @delivery_hold})

  def delivery(state, key, :confirmed), do: tally(state, key, &delivered/1)

  # A confirmation no delivery hold still waits on (a second one of one copy)
  # is dropped: its weight would answer the sinks' holds. Until the run settles
  # its holds are not in, so the entry's call and answer outputs bound them.
  defp delivered(tally) do
    if delivery_owed?(tally),
      do: %{tally | confirms: tally.confirms + @delivery_hold},
      else: tally
  end

  defp delivery_owed?(%{pending: pending, confirms: confirms, entry: entry}) when pending > 0,
    do: div(confirms, @delivery_hold) < Enum.count(entry.outputs, &delivery_output?(&1.metadata))

  defp delivery_owed?(tally), do: deliveries(tally) > 0

  # Delivery holds still unanswered, read off the two weighted counts.
  defp deliveries(%{holds: holds, confirms: confirms}),
    do: div(holds, @delivery_hold) - div(confirms, @delivery_hold)

  def tally(state, key, fun) do
    case Map.fetch(state.outbox_awaiting, key) do
      # A report for an entry that is gone (a resettle superseded it) changes
      # nothing.
      :error ->
        state

      {:ok, tally} ->
        tally = fun.(tally)

        if complete?(tally),
          do: complete_entry(state, key),
          else: %{state | outbox_awaiting: Map.put(state.outbox_awaiting, key, tally)}
    end
  end

  def complete?(%{pending: pending, holds: holds, confirms: confirms}),
    do: pending <= 0 and confirms >= holds

  # A chain run that never reported back — the Overwatch died between the cast
  # and the settlement — leaves the entry owed with nothing in flight to answer
  # it. Re-dispatch its outputs under their identical identity (conforming sinks
  # dedup; the record's own side effects already landed) and start the tally
  # over under the entry's next key: a hold the lost run took would otherwise
  # never be answered, wedging the cursor for good.
  def resettle_outbox(state) do
    if resettle_due?(state) do
      resettle(state, fn tally ->
        unanswered?(tally) and State.now_ms() - tally.at >= state.limits.outbox_resettle_ms
      end)
    else
      state
    end
  end

  @doc """
  Re-dispatch now, not at the resettle tick, every call this writer dispatched
  by `started`: its caller's Overwatch, started then, got an answer for a call
  it has no row for. A later dispatch went to that Overwatch and is its own.
  How many entries hold call `id` with a chain run yet to report, so its
  answer waits; an answer no entry holds is the caller's to absorb.
  """
  @spec redrive_calls(State.t(), integer(), binary()) :: {non_neg_integer(), State.t()}
  def redrive_calls(state, started, id) do
    call? = &Enum.any?(&1.entry.outputs, fn output -> call_output?(output.metadata) end)
    state = resettle(state, &(call?.(&1) and &1.at <= started and unanswered?(&1)))

    {Enum.count(state.outbox_awaiting, fn {key, tally} ->
       tally.pending > 0 and call?.(tally) and
         Enum.any?(entry_outputs(state, key, tally.entry), &(Token.durable_id(elem(&1, 1)) == id))
     end), state}
  end

  defp resettle(state, due?) do
    {rc, _sh, _pk} = state.cell_id
    overwatch = Lookup.overwatch(rc)

    {state, chunks, renamed} =
      Enum.reduce(state.outbox_awaiting, {state, [], %{}}, fn {key, tally}, acc ->
        if due?.(tally), do: resettle_entry(overwatch, acc, key, tally), else: acc
      end)

    with {:ok, pid} <- overwatch, do: Outbox.dispatch(pid, in_dispatch_order(chunks))

    rename_inflight(state, renamed)
  end

  # A chain run that never reported, or a delivery hold of a relay's call or
  # answer still open: the caller's Overwatch keeps a call in memory and the
  # delivery keeps an answer, so either one lost to a restart comes back only
  # from this re-dispatch, and a live one re-takes its hold and changes nothing.
  # A sink's hold waits for its sink.
  defp unanswered?(%{pending: pending}) when pending > 0, do: true
  defp unanswered?(tally), do: deliveries(tally) > 0

  defp delivery_output?(metadata), do: call_output?(metadata) or answer_output?(metadata)

  defp call_output?(metadata), do: List.keymember?(metadata, @call, 0)

  defp answer_output?(metadata),
    do: Token.relay_token?(elem(List.keyfind(metadata, @reply_to, 0, {@reply_to, nil}), 1))

  # The inflight head is the oldest dispatch, so one comparison rules the whole
  # sweep out on a healthy stream writer.
  def resettle_due?(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {_key, at}} -> State.now_ms() - at >= state.limits.outbox_resettle_ms
      :empty -> map_size(state.outbox_awaiting) > 0
    end
  end

  defp resettle_entry({:ok, _pid}, {state, chunks, renamed}, {n, seq} = key, %{entry: entry}) do
    next = {if(n == :cursor, do: 1, else: n + 1), seq}
    outputs = entry_outputs(state, next, entry)
    state = %{state | outbox_awaiting: Map.delete(state.outbox_awaiting, key)}

    {register_entry(state, next, length(outputs), entry), [outputs | chunks],
     Map.put(renamed, key, next)}
  end

  # Nothing to re-dispatch to: the entry stays owed with its tally untouched, so
  # the next tick tries again instead of restarting the clock against a chain
  # that cannot run.
  defp resettle_entry({:error, _reason}, acc, _key, _tally), do: acc

  defp rename_inflight(state, renamed) do
    inflight =
      :queue.filtermap(
        fn {key, at} -> {true, {Map.get(renamed, key, key), at}} end,
        state.outbox_inflight
      )

    %{state | outbox_inflight: inflight}
  end

  defp complete_entry(state, key) do
    %{state | outbox_awaiting: Map.delete(state.outbox_awaiting, key)}
    |> drain_confirmed()
    |> try_dispatch_outbox()
  end

  # The cursor advances over the leading run of confirmed entries only: an entry
  # still in `outbox_awaiting` stops the drain, so a later confirmation can
  # never carry the cursor past output no sink has taken.
  defp drain_confirmed(state) do
    case :queue.peek(state.outbox_inflight) do
      {:value, {{_n, seq} = key, _at}} ->
        if Map.has_key?(state.outbox_awaiting, key) do
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
      {:value, {_key, at}} -> State.now_ms() - at
      :empty -> 0
    end
  end

  # Advance the dedup file with one atomic tmp+rename, at most once per fsync
  # batch. The reserved per-instance identities (the task-journal, the input
  # log) skip it: they carry no effect records, and the dedup side-file would
  # register a phantom Cell with `Paths.scan_dir/1`. A writer whose open never
  # loaded the Cell's files (`lineage: nil`) would overwrite them with defaults.
  def persist_dedup(%{cell_id: {_, _, key}, lineage: lineage} = state)
      when key in @reserved_cell_keys or lineage == nil,
      do: state

  def persist_dedup(%{folded_offset: folded} = state) do
    # The cursors as one record; only the persistence watermark is clamped,
    # because the dedup file's own guard has no room for the empty-log `-1`.
    progress = %{State.progress(state) | last_persisted_seq: max(state.last_persisted_seq, 0)}
    lineage = %{epoch: progress.epoch, folded_offset: folded, cut: nil}

    # An out-of-order record is not written. The cursor may lag and never lead
    # (`Flezha.State.DurabilityTier.Hydrator`), so keeping the older, consistent
    # record re-drives work the sinks dedup — writing a flush cursor ahead of
    # what is durable would drop those outputs for good.
    with true <- State.ordered?(progress) || out_of_order(state, progress),
         {:ok, state} <- put_lineage(state, lineage) do
      _ = Lineage.put_dedup(state.cell_id, Map.put(progress, :folded_offset, folded))
      state
    else
      _not_written -> state
    end
  end

  defp out_of_order(state, progress) do
    Logger.error(
      "[state] refusing to persist an out-of-order cursor record for " <>
        "#{inspect(state.cell_id)}: #{inspect(progress)}"
    )
  end

  # The lineage record, synced, goes before the dedup file or disk step that relies
  # on it; a failed install leaves the writer as it was. A bump's cut stays until
  # the dedup write after its disk step clears it: only a synced record ends it.
  def put_lineage(%{lineage: lineage} = state, changes) do
    record = Map.merge(lineage, changes)

    with :ok <- if(record == lineage, do: :ok, else: Lineage.put(state.cell_id, record)),
         do: {:ok, %{state | lineage: record, folded_offset: record.folded_offset}}
  end
end
