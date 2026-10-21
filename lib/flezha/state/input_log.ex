defmodule Flezha.State.InputLog do
  @moduledoc """
  Per-instance offset-addressable journal of durable input.

  An instance has durable input when it is a flow relay on the
  effectively-once tier or one that sleeps, or a durable steps relay; all
  journal through this one log. One process per instance
  (`Flezha.State.InputLog.Server`) journals every inbound payload (post-`:on_input`)
  at `<state_root>/<namespace>/<state key>/input.log` before the Overwatch
  forwards it, at strictly monotonic offsets, and answers once the record is
  synced. Records reuse the `WalWriter` framing, and durability rides the shared
  `FsyncBatcher`: the server registers in the `WriterRegistry` under
  `{key, :input_log}` and answers the same `:fsync` call, so the batcher drains
  it unchanged. A payload whose `flz.msg_id` the log still retains is absorbed
  rather than journaled twice.

  Retention is watermark-driven: each reporter's durable consumed-offset (an
  flow Cell's record, a steps relay's iteration commit or released
  input under its journal's identity) is reported into a public ETS table
  (`record_consumed/3`), a flow relay's runner-reported processed
  prefix is confirmed beside them (`confirm_prefix/2`), and the sweep truncates
  strictly below `min(watermark, now - hold_window)`. A reporter that consumed
  nothing holds the watermark at the log start; a frozen watermark over a
  growing log emits `[:flezha, :state, :input_log_frozen_watermark]` naming the
  lagging reporter instead of silently accumulating.

  The same server keeps the instance's effect results (`keep_effect/5`) in a
  second file, `effects.log`, created on the first one: the results a
  tasklet's `ctx.effect` calls returned, each kept under its identity for as
  long as the input whose offset it names, and dropped by the truncation that
  drops that input. An append is answered once the sync companion has synced
  it; the input appends never wait on it.
  """

  alias Flezha.Config.Durations
  alias Flezha.Relay.Calls.Token
  alias Flezha.Relay.ClassRegistry
  alias Flezha.SDK.SafeCall
  alias Flezha.State.{Paths, Supervisor}
  alias Flezha.State.InputLog.MsgIndex

  @watermarks :flezha_input_log_watermarks
  @reply_to Token.reply_to_key()

  # The confirmed processed prefix's row beside the reporters' Cell rows; an
  # atom, so `cell_offsets/1` never takes it for a reporter.
  @prefix_key :processed_prefix

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
  def append(relay_class, message, metadata),
    do: do_append(relay_class, message, metadata, 2)

  defp do_append(relay_class, message, metadata, attempts) do
    reqids = append_async(relay_class, message, metadata, :gen_server.reqids_new(), :append)

    case :gen_server.wait_response(reqids, :infinity, true) do
      {{:reply, reply}, :append, _} ->
        reply

      # A restarting log's registry entry can briefly point at the dead pid.
      {{:error, {:noproc, _}}, :append, _} when attempts > 1 ->
        do_append(relay_class, message, metadata, attempts - 1)

      {{:error, {reason, log}}, :append, _} ->
        exit({reason, {__MODULE__, :append, [relay_class, log]}})
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
  def append_async(relay_class, message, metadata, reqids, label) do
    {:ok, pid} = Supervisor.find_or_start({relay_class, :input_log})
    request = {:append, encode_body(message, metadata), MsgIndex.key(metadata)}
    :gen_server.send_request(pid, request, label, reqids)
  end

  # Encode in the caller rather than in the server: every ingest process pays
  # its own serialization in parallel instead of queueing behind the one
  # journal process that every relay of this class shares.
  defp encode_body(message, metadata), do: :erlang.term_to_binary({message, metadata})

  @doc "Highest journaled offset, `-1` when the log is empty or absent."
  @spec last_offset(binary()) :: integer() | {:error, term()}
  def last_offset(relay_class) do
    {:ok, pid} = Supervisor.find_or_start({relay_class, :input_log})
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
  def tail(relay_class) do
    case retained_last_offset(relay_class) do
      last when is_integer(last) -> {:ok, watermark(relay_class) || -1, last}
      {:error, _refusal} = refused -> refused
    end
  end

  @doc """
  The retained records above the instance's floor and above offset `above`,
  in offset order, as `read_range/3` answers them: what the next bind
  re-forwards. `[]` for a key with no log.
  """
  @spec undelivered(binary(), integer()) ::
          [{non_neg_integer(), term(), map()}] | {:error, term()}
  def undelivered(relay_class, above \\ -1) do
    with {:ok, floor, last} <- tail(relay_class) do
      from = max(floor, above) + 1
      if last >= from, do: read_range(relay_class, from, last), else: []
    end
  end

  @doc """
  The offset of the retained record whose `flz.msg_id` is `msg_id`, or `nil`
  when none is retained. A key with no log starts none.
  """
  @spec msg_offset(binary(), binary()) :: non_neg_integer() | nil
  def msg_offset(relay_class, msg_id) do
    if File.exists?(Paths.input_log_path(relay_class)) do
      {:ok, pid} = Supervisor.find_or_start({relay_class, :input_log})

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
  def call_floor(relay_class) do
    with {:ok, pid} <- Supervisor.lookup({relay_class, :input_log}),
         floor when is_integer(floor) <- call(pid, :call_floor),
         do: floor,
         else: (_none_or_down -> nil)
  end

  defp retained_last_offset(relay_class) do
    case Supervisor.lookup({relay_class, :input_log}) do
      {:ok, pid} ->
        call(pid, :last_offset)

      :error ->
        if File.exists?(Paths.input_log_path(relay_class)),
          do: last_offset(relay_class),
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
  def read_range(relay_class, from, to) when from <= to do
    {:ok, pid} = Supervisor.find_or_start({relay_class, :input_log})
    call(pid, {:read_range, from, to})
  end

  @doc """
  The tokens of the outside calls the retained records in `from..to` carry
  (`flz.reply_to`), by offset; a record that carries none is absent.
  """
  @spec calls(binary(), non_neg_integer(), non_neg_integer()) :: %{non_neg_integer() => binary()}
  def calls(relay_class, from, to) do
    case read_range(relay_class, from, to) do
      records when is_list(records) ->
        for {offset, _message, %{@reply_to => token}} <- records,
            into: %{},
            do: {offset, token}

      {:error, _reason} ->
        %{}
    end
  end

  @doc "The token of the outside call the record at `offset` carries, `nil` when none."
  @spec reply_to(binary(), non_neg_integer()) :: binary() | nil
  def reply_to(relay_class, offset), do: Map.get(calls(relay_class, offset, offset), offset)

  # ---------- Min-watermark tracker (public ETS, rebuildable cache) ----------

  @doc """
  Report a reporter's durable consumed-offset: a flow Cell's when an effect
  record carrying `consumed_offset` becomes durable (and on boot repopulation
  from the WAL tail), a durable steps relay's under `Paths.journal_id/1` when an
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
  def record_consumed(relay_class, cell_id, offset) do
    ensure_watermark_table()
    stored = advance_stored_offset({relay_class, cell_id}, offset)

    # A watermark advance may free source credit; the log server drains the
    # ledger and unparks waiting appends. Async — this runs on the WalWriter's
    # fsync path. Every report notifies, including one the guard ignored, or
    # which reporter arrived last would decide whether parked appends are
    # released; it carries the offset now STORED, so the log's own copy of the
    # value tracks the value rather than the request.
    notify_consumed(relay_class, cell_id, stored)
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

  defp notify_consumed(relay_class, cell_id, offset) do
    case Supervisor.lookup({relay_class, :input_log}) do
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
  def rewind_consumed(relay_class, cell_id, offset) do
    ensure_watermark_table()
    # The confirmed prefix covers inputs the correction is about to replay.
    :ets.delete(@watermarks, {relay_class, @prefix_key})
    notify_consumed(relay_class, @prefix_key, -1)

    if offset < 0 do
      :ets.delete(@watermarks, {relay_class, cell_id})
    else
      :ets.insert(@watermarks, {{relay_class, cell_id}, offset})
    end

    notify_consumed(relay_class, cell_id, offset)
  end

  @doc """
  Highest offset appended strictly before `ts_ms`, or `nil` when no retained
  record is that old or the log is not open here (the retention sweep's
  question, which never opens a log). Pre-timestamp records are never
  eligible — an unknown age cannot be judged against a window.
  """
  @spec offset_before_time(binary(), integer()) :: non_neg_integer() | nil | {:error, term()}
  def offset_before_time(relay_class, ts_ms) do
    with {:ok, pid} <- Supervisor.lookup({relay_class, :input_log}),
         do: call(pid, {:offset_before_time, ts_ms}),
         else: (:error -> nil)
  end

  @doc "Drop a hard-deleted Cell's watermark entry so it stops holding truncation."
  @spec forget_cell(binary(), tuple()) :: :ok
  def forget_cell(relay_class, cell_id) do
    ensure_watermark_table()
    :ets.delete(@watermarks, {relay_class, cell_id})
    notify_consumed(relay_class, cell_id, -1)
  end

  @doc """
  The instance's floor: the processed prefix its runner reported and the
  controller confirmed durable (`confirm_prefix/2`), or before one is
  confirmed, and after a restart, the minimum durable consumed-offset across
  its reporters. `nil` when neither exists (the watermark then holds at the log
  start and nothing truncates).
  """
  @spec watermark(binary()) :: non_neg_integer() | nil
  def watermark(relay_class) do
    reporters = Map.new(cell_offsets(relay_class))

    case :ets.lookup(@watermarks, {relay_class, @prefix_key}) do
      [{_, offset}] -> floor_of(Map.put(reporters, @prefix_key, offset))
      [] -> floor_of(reporters)
    end
  end

  # The confirmed prefix when there is one, else the slowest reporter; `nil`
  # for none. The log folds its own copy of the reporters by it too.
  @doc false
  def floor_of(%{@prefix_key => prefix}), do: prefix
  def floor_of(consumed) when map_size(consumed) == 0, do: nil
  def floor_of(consumed), do: consumed |> Map.values() |> Enum.min()

  @doc """
  Confirm the instance's processed prefix: every input at or below `offset`
  was processed and every record it produced is fsynced. It only moves
  forward; a correction rewind clears it (`rewind_consumed/3`).

  A runner that spread the instance's records over several streams sent the
  marker on each (`streams:`); `stream:` names the one confirming here, and
  the prefix moves once every one of them has.
  """
  @spec confirm_prefix(binary(), non_neg_integer(), keyword()) :: :ok
  def confirm_prefix(relay_class, offset, opts \\ []) do
    case Keyword.get(opts, :streams, 1) do
      1 -> store_prefix(relay_class, offset)
      streams -> vote_prefix(relay_class, offset, streams, Keyword.fetch!(opts, :stream))
    end
  end

  defp store_prefix(relay_class, offset) do
    ensure_watermark_table()
    stored = advance_stored_offset({relay_class, @prefix_key}, offset)
    notify_consumed(relay_class, @prefix_key, stored)
  end

  defp vote_prefix(relay_class, offset, streams, stream) do
    case Supervisor.lookup({relay_class, :input_log}) do
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
  about — for every relay that never asked for one.
  """
  @spec cursors(binary()) ::
          %{tail: integer(), consumed: %{tuple() => non_neg_integer()}} | nil
  def cursors(relay_class) do
    case Supervisor.lookup({relay_class, :input_log}) do
      {:ok, pid} ->
        # A refused log has no tail to report — the prefix that verified is not
        # the log's end — so it degrades to the same WAL-seq-only classification
        # a tier-off relay gets, rather than reporting a cursor that lies.
        case call(pid, :last_offset) do
          tail when is_integer(tail) ->
            %{tail: tail, consumed: Map.new(cell_offsets(relay_class))}

          {:error, _refusal} ->
            nil
        end

      :error ->
        nil
    end
  end

  @doc """
  Keep a `ctx.effect` result under `identity` for the input at `offset`,
  answering once it is synced: `:kept` when this payload was kept, or
  `{:first, payload}` when the identity already held another, which the call
  then returns. Starts the log server on first use.
  """
  @spec keep_effect(binary(), non_neg_integer(), binary(), binary(), binary()) ::
          :kept | {:first, binary()} | {:error, term()}
  def keep_effect(relay_class, offset, identity, cell, payload) do
    {:ok, pid} = Supervisor.find_or_start({relay_class, :input_log})
    call(pid, {:keep_effect, offset, identity, cell, payload})
  end

  @doc """
  The kept effect results for inputs above `above`, in offset order, as
  `{offset, identity, cell, payload}`: what a bind ships. `[]` for a key with
  no effect log, which starts no log.
  """
  @spec effect_results(binary(), integer()) ::
          [{non_neg_integer(), binary(), binary(), binary()}] | {:error, term()}
  def effect_results(relay_class, above) do
    if File.exists?(Paths.effects_log_path(relay_class)) do
      {:ok, pid} = Supervisor.find_or_start({relay_class, :input_log})
      call(pid, {:effect_results, above})
    else
      []
    end
  end

  # The log for a class is found or started per call, so `pid` can be a writer
  # that is mid-restart or already gone. One declared refusal for all four
  # reads; every caller's spec already carries `{:error, term()}`.
  defp call(pid, request) do
    SafeCall.bounded(pid, request, Durations.get(:input_log_call_ms),
      on_error: &{:error, {:input_log_unavailable, &1}}
    )
  end

  @doc """
  The relay's input-log hold window: the greater of the user-configured
  floor and the relay's own declared correction window.

  A correction over a 60 s window MEANS retaining 60 s of input log — that is the
  definition of the declaration, not a policy to be negotiated with a user.
  So the declaration derives it, and a user who configures a longer floor
  still gets theirs.
  """
  @spec hold_window_ms(binary()) :: non_neg_integer()
  def hold_window_ms(relay_class) when is_binary(relay_class) do
    configured = Durations.get(:input_log_hold_window_ms)
    declared = ClassRegistry.correction_window_ms(relay_class) || 0
    max(configured, declared)
  end

  @doc false
  @spec cell_offsets(binary()) :: [{tuple(), integer()}]
  def cell_offsets(relay_class) do
    ensure_watermark_table()

    @watermarks
    |> :ets.match({{relay_class, :"$1"}, :"$2"})
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
