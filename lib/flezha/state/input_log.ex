defmodule Flezha.State.InputLog do
  @moduledoc """
  Per-instance offset-addressable journal of durable input.

  An instance has durable input when it is an unbounded operator on the
  effectively-once tier or one that sleeps, or a durable bounded operator; all
  journal through this one log. One GenServer
  per instance journals every inbound payload (post-`:on_input`) at
  `<state_root>/<namespace>/<state key>/input.log` before the Overwatch
  forwards it, at strictly monotonic offsets, and answers once the record is
  synced. Records reuse the `WalWriter` framing, and durability rides the shared
  `FsyncBatcher`: the server registers in the `WriterRegistry` under
  `{key, :input_log}` and answers the same `:fsync` call, so the batcher drains
  it unchanged. A payload whose `flz.msg_id` the log still retains is absorbed
  rather than journaled twice.

  Retention is watermark-driven: each reporter's durable consumed-offset (an
  unbounded Cell's record, a bounded operator's iteration commit or released
  input under its journal's identity) is reported into a public ETS table
  (`record_consumed/3`), an unbounded operator's runner-reported processed
  prefix is confirmed beside them (`confirm_prefix/2`), and the sweep truncates
  strictly below `min(watermark, now - hold_window)`. A reporter that consumed
  nothing holds the watermark at the log start; a frozen watermark over a
  growing log emits `[:flezha, :state, :input_log_frozen_watermark]` naming the
  lagging reporter instead of silently accumulating.
  """

  use GenServer

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Process.Cspec
  alias Flezha.SDK.RecordFrame, as: Frame
  alias Flezha.SDK.SafeCall

  alias Flezha.State.{
    CreditLedger,
    FsyncBatcher,
    Paths,
    PersistenceResolver,
    RecordFrame,
    SeqIndex,
    Supervisor
  }

  alias Flezha.State.InputLog.MsgIndex

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

  @watermarks :flezha_input_log_watermarks

  # The confirmed processed prefix's row beside the reporters' Cell rows; an
  # atom, so `cell_offsets/1` never takes it for a reporter.
  @prefix_key :processed_prefix

  # Source-ingestion credit (`credit-grant-flow-control` §source): appends
  # debit the per-Operator ledger; min-watermark advances drain it (batched
  # by the low watermark); at zero credit the append parks via a deferred
  # reply, which the Overwatch holds open on the source's call
  # (`Flezha.Operator.Ingest`) — the websocket stops reading its socket until
  # credit mints, while the host keeps running the chains that mint it.
  # `consumed` is the log's own copy of its reporters' durable consumed-offsets:
  # the min-watermark that drains source credit is taken from it, so an advance
  # never scans the shared table. The table stays the published view.
  # `positions` (a `Flezha.State.SeqIndex`) lets a truncation find its floor
  # within a few records and copy only the tail — the work is the tail, never
  # the log.
  defstruct [
    :operator_class,
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
    prefix_votes: %{}
  ]

  # Cap on how many queued messages of one kind a handler folds into one pass
  # (appends into one write syscall, consumed-offsets into one watermark
  # recompute). High enough to amortize a burst, low enough that nothing queued
  # behind them waits on an unbounded amount of other work.
  @max_append_batch 256

  # ---------- Public API ----------

  def child_spec(operator_class) do
    Cspec.build(
      {__MODULE__, operator_class},
      {__MODULE__, :start_link, [operator_class]},
      :worker_transient
    )
  end

  def start_link(operator_class) do
    GenServer.start_link(__MODULE__, operator_class, name: via(operator_class))
  end

  @doc """
  Journal one inbound payload, returning its assigned offset once the record is
  synced, so the caller forwards only durable payloads; the sync is the shared
  `FsyncBatcher` tick, so every append of one tick shares one fsync. Starts the
  log server on first use. A payload whose metadata carries a `flz.msg_id` the
  log still retains is absorbed: `{:duplicate, offset}` names the retained
  record and nothing is written. At zero source credit the call parks (deferred
  reply) until the min-watermark advance mints credit — the intended source
  back-pressure, hence `:infinity`.

  The waiting form of `append_async/5`, for a caller with nothing else to do
  meanwhile; the Overwatch's ingest issues the request and carries on.
  """
  @spec append(binary(), term(), map()) ::
          {:ok, non_neg_integer()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def append(operator_class, message, metadata),
    do: do_append(operator_class, message, metadata, 2)

  defp do_append(operator_class, message, metadata, attempts) do
    reqids = append_async(operator_class, message, metadata, :gen_server.reqids_new(), :append)

    case :gen_server.wait_response(reqids, :infinity, true) do
      {{:reply, reply}, :append, _} ->
        reply

      # A restarting log's registry entry can briefly point at the dead pid.
      {{:error, {:noproc, _}}, :append, _} when attempts > 1 ->
        do_append(operator_class, message, metadata, attempts - 1)

      {{:error, {reason, log}}, :append, _} ->
        exit({reason, {__MODULE__, :append, [operator_class, log]}})
    end
  end

  @doc """
  Journal one inbound payload without waiting: the `{:append, body, msg_id}`
  request is issued with `:gen_server.send_request/4` under `label` into the
  caller's request collection. The answer — `append/3`'s, or the log's exit,
  which the monitored request reports as `{:error, {reason, log}}` instead of
  leaving the caller owed — arrives as a message the caller hands to
  `:gen_server.check_response/3`, once the record is synced. A zero-credit park
  is then the caller's to hold, not its loop's to wait on.
  """
  @spec append_async(binary(), term(), map(), :gen_server.request_id_collection(), term()) ::
          :gen_server.request_id_collection()
  def append_async(operator_class, message, metadata, reqids, label) do
    {:ok, pid} = Supervisor.find_or_start({operator_class, :input_log})
    request = {:append, encode_body(message, metadata), MsgIndex.key(metadata)}
    :gen_server.send_request(pid, request, label, reqids)
  end

  # Encode in the caller rather than in the server: every ingest process pays
  # its own serialization in parallel instead of queueing behind the one
  # journal process that every operator of this class shares.
  defp encode_body(message, metadata), do: :erlang.term_to_binary({message, metadata})

  @doc "Highest journaled offset, `-1` when the log is empty or absent."
  @spec last_offset(binary()) :: integer() | {:error, term()}
  def last_offset(operator_class) do
    {:ok, pid} = Supervisor.find_or_start({operator_class, :input_log})
    call(pid, :last_offset)
  end

  @doc """
  `{:ok, floor, last}` for a state key: the instance's floor (`watermark/1`,
  `-1` when no reporter has reported) and its log's last offset (`-1` when
  there is none). The instance owes undelivered input exactly when
  `last > floor`.

  Reads no record: a running log answers from memory, and a file whose log is
  not running is opened (the scan its next append needs anyway). A key with
  no file starts no log: asking whether an instance owes input must not create
  a log for one that never had any.
  """
  @spec tail(binary()) :: {:ok, integer(), integer()} | {:error, term()}
  def tail(operator_class) do
    case retained_last_offset(operator_class) do
      last when is_integer(last) -> {:ok, watermark(operator_class) || -1, last}
      {:error, _refusal} = refused -> refused
    end
  end

  @doc """
  The retained records above the instance's floor, in offset order, as
  `read_range/3` answers them: what the next bind re-forwards. `[]` for a key
  with no log.
  """
  @spec undelivered(binary()) :: [{non_neg_integer(), term(), map()}] | {:error, term()}
  def undelivered(operator_class) do
    case tail(operator_class) do
      {:ok, floor, last} when last > floor -> read_range(operator_class, floor + 1, last)
      {:ok, _floor, _last} -> []
      {:error, _refusal} = refused -> refused
    end
  end

  @doc """
  The offset of the retained record whose `flz.msg_id` is `msg_id`, or `nil`
  when none is retained. A key with no log starts none.
  """
  @spec msg_offset(binary(), binary()) :: non_neg_integer() | nil
  def msg_offset(operator_class, msg_id) do
    if File.exists?(Paths.input_log_path(operator_class)) do
      {:ok, pid} = Supervisor.find_or_start({operator_class, :input_log})

      case call(pid, {:msg_offset, msg_id}) do
        offset when is_integer(offset) -> offset
        _none_or_refused -> nil
      end
    end
  end

  @doc """
  The lowest offset a live call's record holds in this log, or `nil`: the
  retention sweep never truncates at or above it.
  """
  @spec call_floor(binary()) :: non_neg_integer() | nil
  def call_floor(operator_class) do
    with {:ok, pid} <- Supervisor.lookup({operator_class, :input_log}),
         floor when is_integer(floor) <- call(pid, :call_floor),
         do: floor,
         else: (_none_or_down -> nil)
  end

  defp retained_last_offset(operator_class) do
    case Supervisor.lookup({operator_class, :input_log}) do
      {:ok, pid} ->
        call(pid, :last_offset)

      :error ->
        if File.exists?(Paths.input_log_path(operator_class)),
          do: last_offset(operator_class),
          else: -1
    end
  end

  @doc """
  Ordered read of the retained offsets in `from..to` (inclusive). Returns
  `{offset, message, metadata}` triples; offsets already truncated or not
  yet journaled are absent from the result.
  """
  @spec read_range(binary(), non_neg_integer(), non_neg_integer()) ::
          [{non_neg_integer(), term(), map()}] | {:error, term()}
  def read_range(operator_class, from, to) when from <= to do
    {:ok, pid} = Supervisor.find_or_start({operator_class, :input_log})
    call(pid, {:read_range, from, to})
  end

  @doc """
  The tokens of the outside calls the retained records in `from..to` carry
  (`flz.reply_to`), by offset; a record that carries none is absent.
  """
  @spec calls(binary(), non_neg_integer(), non_neg_integer()) :: %{non_neg_integer() => binary()}
  def calls(operator_class, from, to) do
    case read_range(operator_class, from, to) do
      records when is_list(records) ->
        for {offset, _message, %{"flz.reply_to" => token}} <- records,
            into: %{},
            do: {offset, token}

      {:error, _reason} ->
        %{}
    end
  end

  @doc "The token of the outside call the record at `offset` carries, `nil` when none."
  @spec reply_to(binary(), non_neg_integer()) :: binary() | nil
  def reply_to(operator_class, offset), do: Map.get(calls(operator_class, offset, offset), offset)

  # ---------- Min-watermark tracker (public ETS, rebuildable cache) ----------

  @doc """
  Report a reporter's durable consumed-offset: an unbounded Cell's when an effect
  record carrying `consumed_offset` becomes durable (and on boot repopulation
  from the WAL tail), a durable bounded operator's under `Paths.journal_id/1` when an
  iteration commit or a park carrying one becomes durable (and at every bind,
  from the journal).

  A consumed-offset only ever moves forward: the report takes effect when it is
  strictly greater than the stored one and is otherwise ignored, so a reporter
  holding a stale view cannot walk the watermark back over a higher offset
  another already made durable. `rewind_consumed/3` is the only way down.

  The rule lives with the value rather than at the call sites because no caller
  is in a position to hold it. An outbox window folds its own entries forward
  with `max/2` and knows nothing of a window that ran nested inside its own
  dispatch; a hydration knows nothing of a window closing beside it. Only the
  value they all report into sees both.
  """
  @spec record_consumed(binary(), tuple(), non_neg_integer()) :: :ok
  def record_consumed(operator_class, cell_id, offset) do
    ensure_watermark_table()
    stored = advance_stored_offset({operator_class, cell_id}, offset)

    # A watermark advance may free source credit; the log server drains the
    # ledger and unparks waiting appends. Async — this runs on the WalWriter's
    # fsync path. Every report notifies, including one the guard ignored, or
    # which reporter arrived last would decide whether parked appends are
    # released; it carries the offset now STORED, so the log's own copy of the
    # value tracks the value rather than the request.
    notify_consumed(operator_class, cell_id, stored)
  end

  # The offset now stored for `key` after applying `offset` to it. Reporters are
  # separate `WalWriter` processes, so the comparison and the write have to be
  # one operation or two of them interleaving lose an advance: the match spec's
  # guard IS the monotonic rule, and `select_replace/2` is what makes deciding
  # it and acting on it indivisible.
  defp advance_stored_offset(key, offset) do
    advance = [{{key, :"$1"}, [{:<, :"$1", offset}], [{:const, {key, offset}}]}]

    case :ets.select_replace(@watermarks, advance) do
      1 ->
        offset

      # Nothing matched: either a row already stands at or past `offset`, and
      # whoever wrote it is the authority its report drains against, or there
      # is no row for this Cell yet.
      0 ->
        case :ets.lookup(@watermarks, key) do
          [{_, stored}] -> stored
          [] -> claim_stored_offset(key, offset)
        end
    end
  end

  # First offset for a Cell: claim the row, or re-decide against whoever
  # claimed it first.
  defp claim_stored_offset(key, offset) do
    if :ets.insert_new(@watermarks, {key, offset}),
      do: offset,
      else: advance_stored_offset(key, offset)
  end

  defp notify_consumed(operator_class, cell_id, offset) do
    case Supervisor.lookup({operator_class, :input_log}) do
      {:ok, pid} -> GenServer.cast(pid, {:consumed, cell_id, offset})
      :error -> :ok
    end
  end

  @doc """
  Move a Cell's consumed-offset *backwards* after a correction rewound its
  WAL — the records that reported the higher offset are gone.

  Distinct from `record_consumed/3` only in intent, and that is the point: a
  watermark that could silently move either way would be a bug waiting to
  happen, while a rewind that quietly left it high would let the next sweep
  truncate the very inputs the correction is about to replay.
  """
  @spec rewind_consumed(binary(), tuple(), non_neg_integer() | -1) :: :ok
  def rewind_consumed(operator_class, cell_id, offset) do
    ensure_watermark_table()
    # The confirmed prefix covers inputs the correction is about to replay.
    :ets.delete(@watermarks, {operator_class, @prefix_key})
    notify_consumed(operator_class, @prefix_key, -1)

    if offset < 0 do
      :ets.delete(@watermarks, {operator_class, cell_id})
    else
      :ets.insert(@watermarks, {{operator_class, cell_id}, offset})
    end

    notify_consumed(operator_class, cell_id, offset)
  end

  @doc """
  Highest offset appended strictly before `ts_ms`, or `nil` when no retained
  record is that old. Pre-timestamp records are never eligible — an unknown age
  cannot be judged against a window.
  """
  @spec offset_before_time(binary(), integer()) :: non_neg_integer() | nil | {:error, term()}
  def offset_before_time(operator_class, ts_ms) do
    {:ok, pid} = Supervisor.find_or_start({operator_class, :input_log})
    call(pid, {:offset_before_time, ts_ms})
  end

  @doc "Drop a hard-deleted Cell's watermark entry so it stops holding truncation."
  @spec forget_cell(binary(), tuple()) :: :ok
  def forget_cell(operator_class, cell_id) do
    ensure_watermark_table()
    :ets.delete(@watermarks, {operator_class, cell_id})
    notify_consumed(operator_class, cell_id, -1)
  end

  @doc """
  The instance's floor: the processed prefix its runner reported and the
  controller confirmed durable (`confirm_prefix/2`), or before one is
  confirmed, and after a restart, the minimum durable consumed-offset across
  its reporters. `nil` when neither exists (the watermark then holds at the log
  start and nothing truncates).
  """
  @spec watermark(binary()) :: non_neg_integer() | nil
  def watermark(operator_class) do
    reporters = Map.new(cell_offsets(operator_class))

    case :ets.lookup(@watermarks, {operator_class, @prefix_key}) do
      [{_, offset}] -> floor_of(Map.put(reporters, @prefix_key, offset))
      [] -> floor_of(reporters)
    end
  end

  # The confirmed prefix when there is one, else the slowest reporter; `nil`
  # for none.
  defp floor_of(%{@prefix_key => prefix}), do: prefix
  defp floor_of(consumed) when map_size(consumed) == 0, do: nil
  defp floor_of(consumed), do: consumed |> Map.values() |> Enum.min()

  @doc """
  Confirm the instance's processed prefix: every input at or below `offset`
  was processed and every record it produced is fsynced. It only moves
  forward; a correction rewind clears it (`rewind_consumed/3`).

  A runner that spread the instance's records over several streams sent the
  marker on each (`streams:`); `stream:` names the one confirming here, and
  the prefix moves once every one of them has.
  """
  @spec confirm_prefix(binary(), non_neg_integer(), keyword()) :: :ok
  def confirm_prefix(operator_class, offset, opts \\ []) do
    case Keyword.get(opts, :streams, 1) do
      1 -> store_prefix(operator_class, offset)
      streams -> vote_prefix(operator_class, offset, streams, Keyword.fetch!(opts, :stream))
    end
  end

  defp store_prefix(operator_class, offset) do
    ensure_watermark_table()
    stored = advance_stored_offset({operator_class, @prefix_key}, offset)
    notify_consumed(operator_class, @prefix_key, stored)
  end

  defp vote_prefix(operator_class, offset, streams, stream) do
    case Supervisor.lookup({operator_class, :input_log}) do
      {:ok, pid} -> GenServer.cast(pid, {:prefix_vote, offset, streams, stream})
      :error -> :ok
    end
  end

  @doc """
  Producing-liveness cursors: the log's last offset and the durable consumed
  offset of every reporter that has reported one. `nil` when no log is running
  for the key — an instance without durable input has none.

  Lookup-only on purpose. `last_offset/1` goes through `find_or_start/1`, and
  polling THAT from the convergence tick would create an input log — a file, a
  process, and a permanently frozen watermark the retention sweep would then warn
  about — for every operator that never asked for one.
  """
  @spec cursors(binary()) ::
          %{tail: integer(), consumed: %{tuple() => non_neg_integer()}} | nil
  def cursors(operator_class) do
    case Supervisor.lookup({operator_class, :input_log}) do
      {:ok, pid} ->
        # A refused log has no tail to report — the prefix that verified is not
        # the log's end — so it degrades to the same WAL-seq-only classification
        # a tier-off operator gets, rather than reporting a cursor that lies.
        case call(pid, :last_offset) do
          tail when is_integer(tail) ->
            %{tail: tail, consumed: Map.new(cell_offsets(operator_class))}

          {:error, _refusal} ->
            nil
        end

      :error ->
        nil
    end
  end

  # ---------- GenServer ----------

  # The log for a class is found or started per call, so `pid` can be a writer
  # that is mid-restart or already gone. One declared refusal for all four
  # reads; every caller's spec already carries `{:error, term()}`.
  defp call(pid, request) do
    SafeCall.bounded(pid, request, Durations.get(:input_log_call_ms),
      on_error: &{:error, {:input_log_unavailable, &1}}
    )
  end

  @impl true
  def init(operator_class) do
    Process.flag(:trap_exit, true)

    # Same deferred-IO pattern as WalWriter: register instantly, open on
    # continue so a concurrent find_or_start never blocks on disk.
    window = source_window_bytes()

    state = %__MODULE__{
      operator_class: operator_class,
      path: Paths.input_log_path(operator_class),
      file_handle: nil,
      next_offset: 0,
      ledger: CreditLedger.new("source:" <> operator_class, window, max(div(window, 2), 1)),
      offset_sizes: :queue.new(),
      waiters: :queue.new(),
      consumed: %{},
      positions: SeqIndex.new()
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
    consumed = Map.new(cell_offsets(state.operator_class))

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
        syncer: start_syncer(state.path),
        positions: SeqIndex.new(size)
    }
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
    {:reply, companion_call(state.syncer, &{:sync, Enum.reverse(state.owed), &1, &2}),
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
        _ = companion_call(state.syncer, &{:reopen, state.path, &1, &2})

        {:noreply,
         %{state | file_handle: file, positions: positions, msg_ids: trim_ids(state, floor)}}

      # Nothing (or too little) below the floor, or the file is gone from under
      # a queued truncation (its class directory removed): nothing to cut.
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
      store_prefix(state.operator_class, offset)
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
      %{operator: state.operator_class, reason: reason}
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
      # The answers still owed name records this sync makes durable.
      if :file.datasync(state.file_handle) == :ok, do: answer(Enum.reverse(state.owed))
      _ = :file.close(state.file_handle)
    end

    # Linked, but a `:normal` exit would not take it along.
    if state.syncer, do: send(state.syncer, :stop)
    :ok
  end

  # ---------- Sync companion ----------

  # A second raw handle on the same file, in its own process: fdatasync syncs
  # the inode, not the descriptor, so the ~ms a datasync takes is spent here
  # while the log's loop keeps answering appends — which every durable-input
  # source is waiting on, one record at a time.
  defp start_syncer(path) do
    spawn_link(fn -> syncer_loop(open_raw(path)) end)
  end

  defp open_raw(path) do
    {:ok, fh} = :file.open(path, [:raw, :read, :write, :binary])
    fh
  end

  defp syncer_loop(fh) do
    receive do
      {:sync, answers} ->
        # A backlog of ticks is one sync: datasync covers every write before it.
        answers = drain_syncs([answers])
        :ok = :file.datasync(fh)
        answer(answers)
        syncer_loop(fh)

      {:sync, answers, from, ref} ->
        :ok = :file.datasync(fh)
        answer(answers)
        send(from, {ref, :ok})
        syncer_loop(fh)

      {:reopen, path, from, ref} ->
        _ = :file.close(fh)
        fh = open_raw(path)
        send(from, {ref, :ok})
        syncer_loop(fh)

      :stop ->
        _ = :file.close(fh)
        :ok
    end
  end

  defp drain_syncs(batches) do
    receive do
      {:sync, answers} -> drain_syncs([answers | batches])
    after
      0 -> batches |> Enum.reverse() |> Enum.concat()
    end
  end

  defp answer(answers),
    do: Enum.each(answers, fn {from, reply} -> GenServer.reply(from, reply) end)

  # A request only the companion can answer: the synchronous flush
  # (`flush_now`, shutdown) or the swap onto a rewritten file. The monitor is
  # the bound: a caller's call timeout bounds the caller, never this loop, and
  # an unbounded wait here parks every append the log owes for the rest of its
  # life. `:error` is what the `FsyncBatcher` already reads as "still dirty,
  # sync it on the next drain", so a lost sync is retried rather than reported
  # durable.
  defp companion_call(syncer, request) do
    ref = Process.monitor(syncer)
    send(syncer, request.(self(), ref))

    receive do
      {^ref, :ok} ->
        Process.demonitor(ref, [:flush])
        :ok

      {:DOWN, ^ref, :process, _pid, _reason} ->
        :error
    end
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
    wm = floor_of(state.consumed)

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
    FsyncBatcher.mark_dirty({state.operator_class, :input_log})
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

  defp source_window_bytes() do
    Application.get_env(
      :flezha,
      :source_credit_window_bytes,
      PersistenceResolver.wal_high_watermark_bytes()
    )
  end

  defp via(operator_class) do
    {:via, Elixir.Registry, {Supervisor.registry_name(), {operator_class, :input_log}}}
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

  @doc """
  The operator's input-log hold window: the greater of the user-configured
  floor and the operator's own declared correction window.

  A correction over a 60 s window MEANS retaining 60 s of input log — that is the
  definition of the declaration, not a policy to be negotiated with a user.
  So the declaration derives it, and a user who configures a longer floor
  still gets theirs.
  """
  @spec hold_window_ms(binary()) :: non_neg_integer()
  def hold_window_ms(operator_class) when is_binary(operator_class) do
    configured = Durations.get(:input_log_hold_window_ms)
    declared = ClassRegistry.correction_window_ms(operator_class) || 0
    max(configured, declared)
  end

  @doc false
  @spec cell_offsets(binary()) :: [{tuple(), integer()}]
  def cell_offsets(operator_class) do
    ensure_watermark_table()

    @watermarks
    |> :ets.match({{operator_class, :"$1"}, :"$2"})
    |> Enum.filter(fn [cell_id, _offset] -> is_tuple(cell_id) end)
    |> Enum.map(fn [cell_id, offset] -> {cell_id, offset} end)
  end

  @doc false
  @spec watermark_table() :: atom()
  def watermark_table(), do: @watermarks

  # Created by `Flezha.State.Supervisor` so it outlives any one reporter; the
  # fallback here only serves callers running without the state subsystem.
  @doc false
  def ensure_watermark_table() do
    case :ets.whereis(@watermarks) do
      :undefined ->
        try do
          :ets.new(@watermarks, [:set, :public, :named_table, write_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end
end
