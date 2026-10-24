defmodule Flezha.State.InputLog.Server do
  @moduledoc "The process behind `Flezha.State.InputLog`, one per instance."

  use GenServer

  require Logger

  alias Flezha.Config
  alias Flezha.Process.Cspec
  alias Flezha.State.{CreditLedger, Frame, FsyncBatcher, InputLog, Paths, PersistenceResolver}
  alias Flezha.State.{RecordFrame, SeqIndex, Supervisor}

  alias Flezha.State.InputLog.{Effects, MsgIndex, Syncer}

  # Input-log records reuse the WalWriter framing, so they carry its 16-byte
  # header slot. The wire never fills it, which leaves it free for the append
  # wall-time — the column that turns `hold_window` into a real retention
  # window and lets a correction find the offset a point in time sits at.
  #
  #   0..7   ts_ms  u64 BE — append wall-time
  #   8      version u8    — 0 = pre-timestamp record ("unknown time")
  #   9..15  zero
  #
  # Byte 9 stays zero on purpose: it is the cell WAL's `format_id`, so a
  # zero there means an input record can never decode as an effect record if
  # one is ever fed to the wrong reader.
  @header_version 1

  defp record_header(ts_ms), do: <<ts_ms::64-big, @header_version::8, 0::8, 0::48>>

  # `nil` for a pre-timestamp record — it predates the hold window's existence,
  # so it can only be held by the watermark, never by time.
  defp header_time(<<ts_ms::64-big, @header_version::8, _::binary>>), do: ts_ms
  defp header_time(_), do: nil

  # Source-ingestion credit (`credit-grant-flow-control` §source): appends
  # debit the per-Relay ledger; min-watermark advances drain it (batched
  # by the low watermark); at zero credit the append parks via a deferred
  # reply, which the Overwatch holds open on the source's call
  # (`Flezha.Relay.Ingest`) — the websocket stops reading its socket until
  # credit mints, while the host keeps running the chains that mint it.
  # `consumed` is the log's own copy of its reporters' durable consumed-offsets:
  # the min-watermark that drains source credit is taken from it, so an advance
  # never scans the shared table. The table stays the published view.
  # `positions` (a `Flezha.State.SeqIndex`) lets a truncation find its floor
  # within a few records and copy only the tail — the work is the tail, never
  # the log.
  defstruct [
    :relay_class,
    :path,
    :file_handle,
    :next_offset,
    :ledger,
    :offset_sizes,
    :waiters,
    :consumed,
    :syncer,
    :positions,
    # The record this class's log refused (`Flezha.State.RecordFrame`), or
    # `nil`. Set, the log stays alive and refuses to serve: appending would
    # hand out offsets the file already holds past the failure, and reading
    # would serve the prefix before it as the whole log.
    :refusal,
    # The retained records' message ids and live calls
    # (`Flezha.State.InputLog.MsgIndex`), `nil` until the first append that
    # carries an id builds it from the file.
    msg_ids: nil,
    # Answers owed until the records they name are synced, newest first, and
    # whether anything was written since the companion was last handed them.
    owed: [],
    dirty: false,
    # A multi-stream processed-prefix marker's confirmations so far, by offset.
    prefix_votes: %{},
    # The effect log (`Flezha.State.InputLog.Effects`), `nil` until the first
    # result, and the largest result it keeps.
    effects: nil,
    effects_cap: 0
  ]

  # Cap on how many queued messages of one kind a handler folds into one pass
  # (appends into one write syscall, consumed-offsets into one watermark
  # recompute). High enough to amortize a burst, low enough that nothing queued
  # behind them waits on an unbounded amount of other work.
  @max_append_batch 256

  # The results one input may keep: a bound on what a runner can make the
  # controller hold, and ship on every bind, for one input.
  @max_effects_per_offset 1024

  def child_spec(relay_class) do
    Cspec.build(
      {__MODULE__, relay_class},
      {__MODULE__, :start_link, [relay_class]},
      :worker_transient
    )
  end

  def start_link(relay_class) do
    GenServer.start_link(__MODULE__, relay_class, name: via(relay_class))
  end

  @impl true
  def init(relay_class) do
    Process.flag(:trap_exit, true)

    # Same deferred-IO pattern as WalWriter: register instantly, open on
    # continue so a concurrent find_or_start never blocks on disk.
    window = Config.get(:source_credit_window_bytes)

    state = %__MODULE__{
      relay_class: relay_class,
      path: Paths.input_log_path(relay_class),
      file_handle: nil,
      next_offset: 0,
      ledger: CreditLedger.new("source:" <> relay_class, window, max(div(window, 2), 1)),
      offset_sizes: :queue.new(),
      waiters: :queue.new(),
      consumed: %{},
      positions: SeqIndex.new(),
      effects_cap: Flezha.Config.get(:effect_max_bytes)
    }

    {:ok, state, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, state) do
    File.mkdir_p!(Path.dirname(state.path))
    {:ok, file} = :file.open(state.path, [:raw, :read, :write, :binary])

    # Torn-tail repair + last-offset recovery, shared with the WAL.
    case RecordFrame.scan_for_last_valid_seq(file, state.path) do
      {:ok, last} -> {:noreply, opened(state, file, last)}
      {:error, refusal} -> {:noreply, %{state | file_handle: file, refusal: refusal}}
    end
  end

  defp opened(state, file, last) do
    {:ok, size} = :file.position(file, :eof)

    # Offsets reported before this log (re)started are in the published table.
    consumed = Map.new(InputLog.cell_offsets(state.relay_class))

    # Records written before this (re)start are not indexed: the first
    # truncation after a restart locates its floor from the start of the file,
    # once.
    #
    # The fsync companion is started here and nowhere else, so a refused open
    # returns before it exists rather than leaving one linked to a log that
    # will never sync.
    %{
      state
      | file_handle: file,
        next_offset: last + 1,
        consumed: consumed,
        syncer: Syncer.start(state.path),
        positions: SeqIndex.new(size)
    }
    |> open_effects(File.exists?(Paths.effects_log_path(state.relay_class)))
  end

  # A log whose file refused a record refuses to serve the class, and stays
  # alive doing so: it has no companion to sync, no offset it can hand out
  # without colliding with the records past the failure, and no range it can
  # answer that would not be the prefix before it.
  @impl true
  def handle_call(_request, _from, %{refusal: {_, _, _} = refusal} = state) do
    {:reply, {:error, refusal}, state}
  end

  def handle_call({:append, body, msg_id}, from, state) do
    # Fold in the appends already queued behind this one so a burst costs one
    # write syscall instead of N. Under light load the batch is a single
    # record and this is the old path with one extra `receive` that returns
    # immediately.
    batch = [{from, body, msg_id} | collect_appends(@max_append_batch - 1, [])]
    {:noreply, write_queued_appends(state, batch)}
  end

  def handle_call(:fsync, _from, state) do
    {:reply, Syncer.call(state.syncer, &{:sync, Enum.reverse(state.owed), &1, &2}),
     %{state | owed: [], dirty: false}}
  end

  def handle_call(:last_offset, _from, state) do
    {:reply, state.next_offset - 1, state}
  end

  def handle_call({:read_range, from, to}, _from, state) do
    {:reply, collect_range(state.file_handle, state.path, from, to), state}
  end

  # The lowest offset a live call holds, which the sweep's floor may not pass.
  def handle_call(:call_floor, _from, state) do
    {floor, index} = MsgIndex.floor(ensure_ids(state).msg_ids)
    {:reply, floor, %{state | msg_ids: index}}
  end

  def handle_call({:msg_offset, msg_id}, _from, state) do
    state = ensure_ids(state)

    case retained(state, msg_id) do
      {:duplicate, offset} -> {:reply, offset, state}
      :none -> {:reply, nil, state}
    end
  end

  def handle_call({:offset_before_time, ts_ms}, _from, state) do
    {:reply, scan_offset_before(state.file_handle, state.path, ts_ms), state}
  end

  # The first payload kept for an identity wins. Either answer leaves through
  # the companion behind a sync of the effect log, so it never names a result
  # that is not on disk.
  def handle_call({:keep_effect, offset, _identity, _cell, payload}, _from, state)
      when offset >= state.next_offset or byte_size(payload) > state.effects_cap,
      do: {:reply, {:error, {:effect_refused, offset, byte_size(payload)}}, state}

  def handle_call({:keep_effect, offset, identity, cell, payload}, from, state) do
    case Effects.judge(state.effects, offset, identity, payload, @max_effects_per_offset) do
      :kept ->
        {:noreply, owe_effect(state, from, :kept)}

      {:first, first} ->
        {:noreply, owe_effect(state, from, {:first, first})}

      :full ->
        {:reply, {:error, {:effect_refused, offset, byte_size(payload)}}, state}

      :new ->
        case open_effects(state, true) do
          %{refusal: nil} = state ->
            result = {offset, identity, cell, payload}
            effects = Effects.append(state.effects, result, now_header())
            {:noreply, owe_effect(%{state | effects: effects}, from, :kept)}

          state ->
            {:reply, {:error, state.refusal}, state}
        end
    end
  end

  def handle_call({:effect_results, above}, _from, state),
    do: {:reply, Effects.above(state.effects, above), state}

  # A refused log has no companion to sync, no cut to make and no watermark to
  # advance: every cast is dropped rather than reaching a `nil` companion.
  @impl true
  def handle_cast(_request, %{refusal: {_, _, _}} = state), do: {:noreply, state}

  # The retention sweep's truncation. It runs in this loop — ingest waits
  # behind it — so it costs the retained tail, never the log. A prefix below
  # the record-log cut threshold is left for a later sweep, so a loop that
  # releases one input per turn rewrites the file once per threshold.
  def handle_cast({:truncate_below, floor}, state) do
    min_bytes = PersistenceResolver.record_log_cut_after_bytes()

    case RecordFrame.cut_below(state.path, state.file_handle, state.positions, floor, min_bytes) do
      {:ok, _cut, file, positions} ->
        # The companion's descriptor still names the file the rename replaced.
        # Awaited: a sync the companion merged across the swap would answer
        # appends written to the new file by syncing the old one.
        _ = Syncer.call(state.syncer, &{:reopen, state.path, &1, &2})

        {:noreply,
         %{state | file_handle: file, positions: positions, msg_ids: trim_ids(state, floor)}
         |> drop_effects_below(floor)}

      # Too little below the floor to cut the inputs yet; the results of inputs
      # below it are dropped all the same, so a log of small inputs with large
      # results does not keep them past their inputs.
      :none ->
        {:noreply, drop_effects_below(state, floor)}

      # The file is gone from under a queued truncation (its class directory
      # removed): nothing to cut.
      _ ->
        {:noreply, state}
    end
  end

  # The batcher's tick: the companion syncs, then answers every append the
  # sync made durable, while this loop keeps appending.
  def handle_cast(:fsync, state) do
    send(state.syncer, {:sync, Enum.reverse(state.owed)})
    {:noreply, %{state | owed: [], dirty: false}}
  end

  # One stream of a multi-stream marker confirmed `offset`: it is the prefix
  # once `streams` distinct streams have, and a later offset confirmed so
  # supersedes every earlier one still counting.
  def handle_cast({:prefix_vote, offset, streams, stream}, state) do
    votes = Map.update(state.prefix_votes, offset, MapSet.new([stream]), &MapSet.put(&1, stream))

    if MapSet.size(votes[offset]) >= streams do
      InputLog.confirm_prefix(state.relay_class, offset)
      {:noreply, %{state | prefix_votes: Map.reject(votes, fn {o, _} -> o <= offset end)}}
    else
      {:noreply, %{state | prefix_votes: votes}}
    end
  end

  # A reporter's durable consumed-offset moved (an effect record or an
  # iteration commit became durable, a correction rewound it, or the Cell was
  # deleted: `-1`). Fold in every advance already queued, then drain the freed
  # bytes from the source ledger once and, when the batched grant lands, unpark
  # waiting appends in arrival order.
  def handle_cast({:consumed, cell_id, offset}, state) do
    state =
      state
      |> note_consumed(cell_id, offset)
      |> collect_consumed(@max_append_batch - 1)

    {:noreply, advance_watermark(state)}
  end

  @impl true
  # The sync companion is this log's only link and holds the descriptor every
  # append's durability rides on, so its death is a durability failure and must
  # be loud. Stopping is what makes it loud AND recoverable: `terminate/2`
  # datasyncs the window the companion owed, and the `:transient` restart opens
  # a fresh pair — `handle_continue(:open)` creates the class directory, so a
  # log whose directory went away heals on the way back up.
  def handle_info({:EXIT, syncer, reason}, %{syncer: syncer} = state) do
    :telemetry.execute(
      [:flezha, :state, :input_log_syncer_died],
      %{count: 1},
      %{relay: state.relay_class, reason: reason}
    )

    {:stop, {:input_log_syncer_died, reason}, state}
  end

  # Defining any `handle_info/2` replaces `GenServer`'s default one, which is
  # what names an unrouted message. This says the same thing, at the same level.
  def handle_info(message, state) do
    Logger.error(
      "#{inspect(__MODULE__)} #{inspect(self())} received unexpected message " <>
        "in handle_info/2: #{inspect(message)}"
    )

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.file_handle do
      settle_owed(state)
      _ = :file.close(state.file_handle)
    end

    Effects.close(state.effects)

    # Linked, but a `:normal` exit would not take it along.
    if state.syncer, do: send(state.syncer, :stop)
    :ok
  end

  # The answers still owed name records a sync makes durable. The companion
  # gives them behind the effect answers it already holds, so every caller is
  # answered before this process is gone; with no live companion, this loop
  # syncs and answers its own.
  defp settle_owed(%{syncer: syncer} = state) do
    owed = Enum.reverse(state.owed)
    synced? = is_pid(syncer) and Syncer.call(syncer, &{:sync, owed, &1, &2}) == :ok
    if not synced? and :file.datasync(state.file_handle) == :ok, do: Syncer.answer(owed)
  end

  # ---------- Internals ----------

  defp note_consumed(state, cell_id, offset) when offset < 0,
    do: %{state | consumed: Map.delete(state.consumed, cell_id)}

  defp note_consumed(state, cell_id, offset),
    do: %{state | consumed: Map.put(state.consumed, cell_id, offset)}

  defp collect_consumed(state, 0), do: state

  defp collect_consumed(state, remaining) do
    receive do
      {:"$gen_cast", {:consumed, cell_id, offset}} ->
        collect_consumed(note_consumed(state, cell_id, offset), remaining - 1)
    after
      0 -> state
    end
  end

  defp advance_watermark(%{consumed: consumed} = state) when map_size(consumed) == 0, do: state

  defp advance_watermark(state) do
    wm = InputLog.floor_of(state.consumed)

    {freed, offset_sizes} = pop_freed(state.offset_sizes, wm)
    {grant, ledger} = CreditLedger.drain(state.ledger, freed)
    state = %{state | ledger: ledger, offset_sizes: offset_sizes}
    if grant > 0, do: unpark_waiters(state), else: state
  end

  # Drain queued `{:append, body, msg_id}` calls without disturbing the relative
  # order of anything else in the mailbox — a selective receive leaves
  # non-matching messages (`:fsync`, `{:consumed, ...}`, ...) queued where they
  # were.
  defp collect_appends(0, acc), do: Enum.reverse(acc)

  defp collect_appends(remaining, acc) do
    receive do
      {:"$gen_call", from, {:append, body, msg_id}} ->
        collect_appends(remaining - 1, [{from, body, msg_id} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Admit in arrival order, encode into one iolist, write once, answer all once
  # synced. An append whose id the log retains is answered with that record and
  # writes nothing. The first record that overruns credit parks, and so does
  # everything behind it: once one has parked, the rest of the batch parks
  # without re-testing the ledger, so a later smaller record can never be
  # admitted ahead of an earlier parked one. That whole-tail park is what
  # preserves the source's order — the same order one-append-per-message got
  # for free from the mailbox.
  defp write_queued_appends(state, batch) do
    state = if Enum.any?(batch, &elem(&1, 2)), do: ensure_ids(state), else: state

    {state, iodata, answers, _parking?} =
      Enum.reduce(batch, {state, [], [], false}, fn
        {from, _body, _id} = append, {st, io, answers, false} ->
          case admit(st, append) do
            {:ok, offset, record, st} ->
              {st, [io, record], [{from, {:ok, offset}} | answers], false}

            {:duplicate, _offset} = dup ->
              {st, io, [{from, dup} | answers], false}

            {:overrun, st} ->
              {park(st, append), io, answers, true}
          end

        append, {st, io, answers, true} ->
          {park(st, append), io, answers, true}
      end)

    state |> flush_records(iodata) |> owe(answers)
  end

  # One append's admission: absorbed by an id the log retains, held back by the
  # credit window (the ledger still records the overrun), or encoded at the
  # next offset. The caller decides when the record reaches the file.
  defp admit(state, {_from, body, id}) do
    with :none <- retained(state, id),
         {:ok, ledger} <- CreditLedger.admit(state.ledger, byte_size(body)) do
      {offset, record, state} = encode_append(%{state | ledger: ledger}, body, id)
      {:ok, offset, record, state}
    else
      {:duplicate, _offset} = dup -> dup
      {:overrun, ledger} -> {:overrun, %{state | ledger: ledger}}
    end
  end

  defp park(state, append), do: %{state | waiters: :queue.in(append, state.waiters)}

  # An answer leaves once the records it names are synced: owed to the next tick
  # while anything written is unsynced, else handed to the companion, which
  # sends it behind the sync already in flight. `answers` are newest first.
  defp owe(state, []), do: state
  defp owe(%{dirty: true} = state, answers), do: %{state | owed: answers ++ state.owed}

  defp owe(state, answers) do
    send(state.syncer, {:sync, Enum.reverse(answers)})
    state
  end

  defp flush_records(state, []), do: state

  defp flush_records(state, iodata) do
    :ok = :file.write(state.file_handle, iodata)
    FsyncBatcher.mark_dirty({state.relay_class, :input_log})
    %{state | dirty: true}
  end

  defp encode_append(state, body, id) do
    offset = state.next_offset
    header = record_header(System.system_time(:millisecond))
    record = Frame.encode_record(offset, header, body)

    {offset, record,
     %{
       state
       | next_offset: offset + 1,
         offset_sizes: :queue.in({offset, byte_size(body)}, state.offset_sizes),
         positions: SeqIndex.note(state.positions, offset, byte_size(record)),
         msg_ids: MsgIndex.note(state.msg_ids, id, offset)
     }}
  end

  # ---------- Effect results ----------

  # The effect log, opened once it is wanted: on start when the file exists, or
  # on the first result. The companion takes a handle of its own to sync it.
  defp open_effects(%{effects: nil} = state, true) do
    path = Paths.effects_log_path(state.relay_class)

    case Effects.open(path) do
      {:ok, effects} ->
        _ = Syncer.call(state.syncer, &{:reopen_effects, path, &1, &2})
        %{state | effects: effects}

      {:error, refusal} ->
        %{state | refusal: refusal}
    end
  end

  defp open_effects(state, _wanted), do: state

  defp owe_effect(state, from, reply) do
    send(state.syncer, {:sync_effects, [{from, reply}]})
    state
  end

  # A truncation drops the results of the inputs it passed; a rewritten file
  # needs the companion's descriptor moved onto it.
  defp drop_effects_below(state, floor) do
    effects = Effects.drop_below(state.effects, floor, now_header())

    if effects && effects.file != state.effects.file,
      do: _ = Syncer.call(state.syncer, &{:reopen_effects, effects.path, &1, &2})

    %{state | effects: effects}
  end

  defp now_header, do: record_header(System.system_time(:millisecond))

  # ---------- Message-id index ----------

  # Walked once per log lifetime, by the first request that needs it. The open
  # walked this file to its end, so a record that does not decode is
  # impossible, not a case.
  defp ensure_ids(%{msg_ids: nil} = state),
    do: %{state | msg_ids: MsgIndex.build(state.file_handle, state.path)}

  defp ensure_ids(state), do: state

  defp retained(_state, nil), do: :none
  defp retained(state, key), do: MsgIndex.retained(state.msg_ids, key)

  defp trim_ids(%{msg_ids: nil}, _floor), do: nil
  defp trim_ids(state, floor), do: MsgIndex.trim(state.msg_ids, floor)

  # Free the tracked bytes of every journaled offset at or below the
  # watermark (its consumption is durable; the log slot is reclaimable).
  # Offsets are assigned monotonically, so the freed set is always a prefix:
  # popping it costs the number freed, not the number still in flight.
  defp pop_freed(offset_sizes, watermark, freed \\ 0) do
    case :queue.peek(offset_sizes) do
      {:value, {offset, size}} when offset <= watermark ->
        pop_freed(:queue.drop(offset_sizes), watermark, freed + size)

      _ ->
        {freed, offset_sizes}
    end
  end

  defp unpark_waiters(state) do
    case :queue.out(state.waiters) do
      {:empty, _} ->
        state

      {{:value, {from, _body, _id} = append}, rest} ->
        case admit(state, append) do
          {:ok, offset, record, state} ->
            %{state | waiters: rest}
            |> flush_records([record])
            |> owe([{from, {:ok, offset}}])
            |> unpark_waiters()

          {:duplicate, _offset} = dup ->
            unpark_waiters(owe(%{state | waiters: rest}, [{from, dup}]))

          {:overrun, state} ->
            state
        end
    end
  end

  defp via(relay_class) do
    {:via, Elixir.Registry, {Supervisor.registry_name(), {relay_class, :input_log}}}
  end

  # Offsets are strictly monotonic, so the walk is done the moment it passes
  # `to` — without that halt this ran to EOF on every call, inside the log's own
  # GenServer, blocking every append behind it.
  defp collect_range(device, path, from, to) do
    device
    |> RecordFrame.fold_records(path, [], fn {seq, _header, body}, acc ->
      cond do
        seq > to ->
          {:halt, acc}

        seq >= from ->
          {message, metadata} = :erlang.binary_to_term(body)
          {:cont, [{seq, message, metadata} | acc]}

        true ->
          {:cont, acc}
      end
    end)
    |> case do
      {:ok, records} -> Enum.reverse(records)
      {:error, _refusal} = refused -> refused
    end
  end

  # Highest offset whose append-time is strictly before `ts_ms`, or `nil` when
  # every retained record is at or after it (nothing has aged out). Records with
  # no timestamp are never eligible — an unknown age cannot be judged against a
  # window.
  defp scan_offset_before(device, path, ts_ms) do
    case RecordFrame.fold_records(device, path, nil, fn {seq, header, _body}, found ->
           case header_time(header) do
             ts when is_integer(ts) and ts < ts_ms -> {:cont, seq}
             _ -> {:cont, found}
           end
         end) do
      {:ok, offset} -> offset
      {:error, _refusal} = refused -> refused
    end
  end
end
