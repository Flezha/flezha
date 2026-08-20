defmodule Flezha.State.InputLog do
  @moduledoc """
  Per-operative offset-addressable ingest journal for the effectively-once
  tier (`durable-input-log`).

  One GenServer per tier-enabled operative journals every inbound payload
  (post-`:on_input`) at `<state_root>/<operative_class>/input.log` before the
  Overwatch forwards it, assigning strictly monotonic offsets. Records reuse
  the `WalWriter` framing (`encode_record`/`decode_record`, crc-protected,
  torn tail discarded on open) with a zeroed header, and durability rides the
  shared `FsyncBatcher`: the server registers in the `WriterRegistry` under
  `{operative_class, :input_log}` and answers the same `:fsync` call a
  `WalWriter` does, so the batcher drains it unchanged.

  Retention is watermark-driven: each Entity's durable consumed-offset is
  reported into a public ETS table (`record_consumed/3`, repopulated from the
  WAL after a restart — the table is a rebuildable cache, owned by
  `Flezha.State.Supervisor`) and `sweep/1`
  truncates strictly below `min(watermark, now - hold_window)` with
  `hold_window` defaulting to zero. An Entity that has consumed nothing holds
  the watermark at the log start; a frozen watermark with a growing log emits
  `[:flezha, :state, :input_log_frozen_watermark]` naming the lagging Entity
  instead of silently accumulating.

  Operatives without the tier never start an InputLog and never touch this
  module — the cost-free-default contract.
  """

  use GenServer

  alias Flezha.Operative.ClassRegistry

  alias Flezha.State.{
    CreditLedger,
    FsyncBatcher,
    PersistenceResolver,
    Registry,
    Supervisor,
    WalWriter
  }

  # Input-log records reuse the WalWriter framing, so they carry its 16-byte
  # header slot. The wire never fills it, which leaves it free for the append
  # wall-time — the column that turns `hold_window` into a real retention
  # window and lets a correction find the offset a point in time sits at.
  #
  #   0..7   ts_ms  u64 BE — append wall-time
  #   8      version u8    — 0 = pre-timestamp record ("unknown time")
  #   9..15  zero
  #
  # Byte 9 stays zero on purpose: it is the entity WAL's `format_id`, so a
  # zero there means an input record can never decode as an effect record if
  # one is ever fed to the wrong reader.
  @header_version 1

  defp record_header(ts_ms), do: <<ts_ms::64-big, @header_version::8, 0::8, 0::48>>

  # `nil` for a pre-timestamp record — it predates the hold window's existence,
  # so it can only be held by the watermark, never by time.
  defp header_time(<<ts_ms::64-big, @header_version::8, _::binary>>), do: ts_ms
  defp header_time(_), do: nil

  @watermarks :flezha_input_log_watermarks

  # Source-ingestion credit (`credit-grant-flow-control` §source): appends
  # debit the per-Operative ledger; min-watermark advances drain it (batched
  # by the low watermark); at zero credit the append parks via a deferred
  # reply, which blocks the source adapter's synchronous ingest call — the
  # websocket stops reading its socket until credit mints.
  # `consumed` is the log's own copy of its Entities' durable consumed-offsets:
  # the min-watermark that drains source credit is taken from it, so an advance
  # never scans the shared table. The table stays the published view.
  defstruct [
    :operative_class,
    :path,
    :file_handle,
    :next_offset,
    :ledger,
    :offset_sizes,
    :waiters,
    :consumed
  ]

  # Cap on how many queued messages of one kind a handler folds into one pass
  # (appends into one write syscall, consumed-offsets into one watermark
  # recompute). High enough to amortize a burst, low enough that nothing queued
  # behind them waits on an unbounded amount of other work.
  @max_append_batch 256

  # ---------- Public API ----------

  def child_spec(operative_class) do
    %{
      id: {__MODULE__, operative_class},
      start: {__MODULE__, :start_link, [operative_class]},
      restart: :transient
    }
  end

  def start_link(operative_class) do
    GenServer.start_link(__MODULE__, operative_class, name: via(operative_class))
  end

  @doc """
  Journal one inbound payload, returning its assigned offset. Starts the
  operative's log server on first use. The append (write syscall) completes
  before this returns, so the caller forwards only journaled payloads;
  durability is fsync-batched like the per-Entity WAL. At zero source
  credit the call parks (deferred reply) until the min-watermark advance
  mints credit — the intended source back-pressure, hence `:infinity`.
  """
  @spec append(binary(), term(), map()) :: {:ok, non_neg_integer()}
  def append(operative_class, message, metadata),
    do: do_append(operative_class, message, metadata, 2)

  defp do_append(operative_class, message, metadata, attempts) do
    {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
    # Encode here rather than in the server: every source process pays its own
    # serialization in parallel instead of queueing behind the one journal
    # process that every operative of this class shares.
    body = :erlang.term_to_binary({message, metadata})
    GenServer.call(pid, {:append, body}, :infinity)
  catch
    # A restarting log's registry entry can briefly point at the dead pid.
    :exit, {:noproc, _} when attempts > 1 ->
      do_append(operative_class, message, metadata, attempts - 1)
  end

  @doc "Highest journaled offset, `-1` when the log is empty or absent."
  @spec last_offset(binary()) :: integer()
  def last_offset(operative_class) do
    {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
    GenServer.call(pid, :last_offset)
  end

  @doc """
  Ordered read of the retained offsets in `from..to` (inclusive). Returns
  `{offset, message, metadata}` triples; offsets already truncated or not
  yet journaled are absent from the result.
  """
  @spec read_range(binary(), non_neg_integer(), non_neg_integer()) ::
          [{non_neg_integer(), term(), map()}]
  def read_range(operative_class, from, to) when from <= to do
    {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
    GenServer.call(pid, {:read_range, from, to})
  end

  # ---------- Min-watermark tracker (public ETS, rebuildable cache) ----------

  @doc """
  Report an Entity's durable consumed-offset (called when an effect record
  carrying `consumed_offset` becomes durable, and on boot repopulation from
  the WAL tail).
  """
  @spec record_consumed(binary(), tuple(), non_neg_integer()) :: :ok
  def record_consumed(operative_class, entity_id, offset) do
    ensure_watermark_table()
    :ets.insert(@watermarks, {{operative_class, entity_id}, offset})

    # A watermark advance may free source credit; the log server drains
    # the ledger and unparks waiting appends. Async — this runs on the
    # WalWriter's fsync path.
    notify_consumed(operative_class, entity_id, offset)
  end

  defp notify_consumed(operative_class, entity_id, offset) do
    case Supervisor.lookup({operative_class, :input_log}) do
      {:ok, pid} -> GenServer.cast(pid, {:consumed, entity_id, offset})
      :error -> :ok
    end
  end

  @doc """
  Move an Entity's consumed-offset *backwards* after a correction rewound its
  WAL — the records that reported the old, higher offset no longer exist.

  Distinct from `record_consumed/3` only in intent, and that is the point: a
  watermark that could silently move either way would be a bug waiting to
  happen, while a rewind that quietly left it high would let the next sweep
  truncate the very inputs the correction is about to replay.
  """
  @spec rewind_consumed(binary(), tuple(), non_neg_integer() | -1) :: :ok
  def rewind_consumed(operative_class, entity_id, offset) do
    ensure_watermark_table()

    if offset < 0 do
      :ets.delete(@watermarks, {operative_class, entity_id})
    else
      :ets.insert(@watermarks, {{operative_class, entity_id}, offset})
    end

    notify_consumed(operative_class, entity_id, offset)
  end

  @doc """
  Highest offset appended strictly before `ts_ms`, or `nil` when no retained
  record is that old. Pre-timestamp records are never eligible — an unknown age
  cannot be judged against a window.
  """
  @spec offset_before_time(binary(), integer()) :: non_neg_integer() | nil
  def offset_before_time(operative_class, ts_ms) do
    {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
    GenServer.call(pid, {:offset_before_time, ts_ms})
  end

  @doc "Drop a hard-deleted Entity's watermark entry so it stops holding truncation."
  @spec forget_entity(binary(), tuple()) :: :ok
  def forget_entity(operative_class, entity_id) do
    ensure_watermark_table()
    :ets.delete(@watermarks, {operative_class, entity_id})
    notify_consumed(operative_class, entity_id, -1)
  end

  @doc """
  Minimum durable consumed-offset across the operative's reporting Entities,
  or `nil` when no Entity has reported (the watermark then holds at the log
  start and nothing truncates).
  """
  @spec watermark(binary()) :: non_neg_integer() | nil
  def watermark(operative_class) do
    case entity_offsets(operative_class) do
      [] -> nil
      offsets -> offsets |> Enum.map(&elem(&1, 1)) |> Enum.min()
    end
  end

  @doc """
  Producing-liveness cursors: the log's last offset and the durable consumed
  offset of every Entity that has reported one. `nil` when no log is live —
  the operative is not on the effectively-once tier and has none.

  Lookup-only on purpose. `last_offset/1` goes through `find_or_start/1`, and
  polling THAT from the convergence tick would create an input log — a file, a
  process, and a permanently frozen watermark the retention sweep would then warn
  about — for every operative that never asked for one.
  """
  @spec cursors(binary()) ::
          %{tail: integer(), consumed: %{tuple() => non_neg_integer()}} | nil
  def cursors(operative_class) do
    case Supervisor.lookup({operative_class, :input_log}) do
      {:ok, pid} ->
        %{
          tail: GenServer.call(pid, :last_offset),
          consumed: Map.new(entity_offsets(operative_class))
        }

      :error ->
        nil
    end
  end

  @doc """
  Retention-sweep entry point: truncate the operative's input-log strictly
  below `min(watermark, now - hold_window)` and emit the frozen-watermark
  warning when the log grows past the size threshold without the watermark
  advancing. No-op when no input-log file exists.
  """
  @spec sweep(binary()) :: :ok
  def sweep(operative_class) do
    path = Registry.input_log_path(operative_class)

    if File.exists?(path) do
      do_sweep(operative_class, path)
    else
      :ok
    end
  end

  @doc "Every operative_class with an input-log on disk (for the retention sweep)."
  @spec all_classes() :: [binary()]
  def all_classes() do
    Enum.filter(Registry.operative_classes(), &File.regular?(Registry.input_log_path(&1)))
  end

  # ---------- GenServer ----------

  @impl true
  def init(operative_class) do
    Process.flag(:trap_exit, true)

    # Same deferred-IO pattern as WalWriter: register instantly, open on
    # continue so a concurrent find_or_start never blocks on disk.
    window = source_window_bytes()

    state = %__MODULE__{
      operative_class: operative_class,
      path: Registry.input_log_path(operative_class),
      file_handle: nil,
      next_offset: 0,
      ledger: CreditLedger.new("source:" <> operative_class, window, max(div(window, 2), 1)),
      offset_sizes: :queue.new(),
      waiters: :queue.new(),
      consumed: %{}
    }

    {:ok, state, {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, state) do
    File.mkdir_p!(Path.dirname(state.path))
    {:ok, file} = :file.open(state.path, [:raw, :read, :write, :binary])

    # Torn-tail discard + last-offset recovery, shared with the WAL.
    last = WalWriter.scan_for_last_valid_seq(file, state.path)
    {:ok, _} = :file.position(file, :eof)

    # Offsets reported before this log (re)started are in the published table.
    consumed = Map.new(entity_offsets(state.operative_class))
    {:noreply, %{state | file_handle: file, next_offset: last + 1, consumed: consumed}}
  end

  @impl true
  def handle_call({:append, body}, from, state) do
    # Fold in the appends already queued behind this one so a burst costs one
    # write syscall instead of N. Under light load the batch is a single
    # record and this is the old path with one extra `receive` that returns
    # immediately.
    {:noreply,
     write_queued_appends(state, [{from, body} | collect_appends(@max_append_batch - 1, [])])}
  end

  def handle_call(:fsync, _from, state) do
    :ok = :file.datasync(state.file_handle)
    {:reply, :ok, state}
  end

  def handle_call(:last_offset, _from, state) do
    {:reply, state.next_offset - 1, state}
  end

  def handle_call({:read_range, from, to}, _from, state) do
    {:reply, collect_range(state.file_handle, from, to), state}
  end

  def handle_call({:offset_before_time, ts_ms}, _from, state) do
    {:reply, scan_offset_before(state.file_handle, ts_ms), state}
  end

  def handle_call({:truncate_below, floor}, _from, state) do
    :ok = :file.close(state.file_handle)
    new_path = WalWriter.rewrite_keep_seq_gt(state.path, floor - 1)
    :ok = File.rename(new_path, state.path)
    {:ok, file} = :file.open(state.path, [:raw, :read, :write, :binary])
    {:ok, _} = :file.position(file, :eof)
    {:reply, :ok, %{state | file_handle: file}}
  end

  @impl true
  def handle_cast(:fsync, state) do
    :ok = :file.datasync(state.file_handle)
    {:noreply, state}
  end

  # An Entity's durable consumed-offset moved (an effect record became durable,
  # a correction rewound it, or the Entity was deleted: `-1`). Fold in every
  # advance already queued, then drain the freed bytes from the source ledger
  # once and, when the batched grant lands, unpark waiting appends in arrival
  # order.
  def handle_cast({:consumed, entity_id, offset}, state) do
    state =
      state
      |> note_consumed(entity_id, offset)
      |> collect_consumed(@max_append_batch - 1)

    {:noreply, advance_watermark(state)}
  end

  @impl true
  def terminate(_reason, state) do
    if state.file_handle do
      _ = :file.datasync(state.file_handle)
      _ = :file.close(state.file_handle)
    end

    :ok
  end

  # ---------- Internals ----------

  defp note_consumed(state, entity_id, offset) when offset < 0,
    do: %{state | consumed: Map.delete(state.consumed, entity_id)}

  defp note_consumed(state, entity_id, offset),
    do: %{state | consumed: Map.put(state.consumed, entity_id, offset)}

  defp collect_consumed(state, 0), do: state

  defp collect_consumed(state, remaining) do
    receive do
      {:"$gen_cast", {:consumed, entity_id, offset}} ->
        collect_consumed(note_consumed(state, entity_id, offset), remaining - 1)
    after
      0 -> state
    end
  end

  defp advance_watermark(%{consumed: consumed} = state) when map_size(consumed) == 0, do: state

  defp advance_watermark(state) do
    wm = state.consumed |> Map.values() |> Enum.min()
    {freed, offset_sizes} = pop_freed(state.offset_sizes, wm)
    {grant, ledger} = CreditLedger.drain(state.ledger, freed)
    state = %{state | ledger: ledger, offset_sizes: offset_sizes}
    if grant > 0, do: unpark_waiters(state), else: state
  end

  # Drain queued `{:append, body}` calls without disturbing the relative order
  # of anything else in the mailbox — a selective receive leaves non-matching
  # messages (`:fsync`, `{:consumed, ...}`, ...) queued where they were.
  defp collect_appends(0, acc), do: Enum.reverse(acc)

  defp collect_appends(remaining, acc) do
    receive do
      {:"$gen_call", from, {:append, body}} ->
        collect_appends(remaining - 1, [{from, body} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Admit in arrival order, encode into one iolist, write once, reply to all.
  # The first record that overruns credit parks, and so does everything behind
  # it — parking the whole tail is what preserves ordering.
  defp write_queued_appends(state, batch) do
    {state, iodata, replies} =
      Enum.reduce(batch, {state, [], []}, fn {from, body}, {st, io, replies} ->
        case CreditLedger.admit(st.ledger, byte_size(body)) do
          {:ok, ledger} ->
            {offset, record, st} = encode_append(%{st | ledger: ledger}, body)
            {st, [io, record], [{from, offset} | replies]}

          {:overrun, ledger} ->
            {park(%{st | ledger: ledger}, from, body), io, replies}
        end
      end)

    state = flush_records(state, iodata)

    Enum.each(Enum.reverse(replies), fn {from, offset} -> GenServer.reply(from, {:ok, offset}) end)

    state
  end

  defp park(state, from, body),
    do: %{state | waiters: :queue.in({from, body}, state.waiters)}

  defp flush_records(state, []), do: state

  defp flush_records(state, iodata) do
    :ok = :file.write(state.file_handle, iodata)
    FsyncBatcher.mark_dirty({state.operative_class, :input_log})
    state
  end

  # Assign the next offset and encode the record; the caller decides when the
  # bytes actually reach the file.
  defp encode_append(state, body) do
    offset = state.next_offset
    header = record_header(System.system_time(:millisecond))

    {offset, WalWriter.encode_record(offset, header, body),
     %{
       state
       | next_offset: offset + 1,
         offset_sizes: :queue.in({offset, byte_size(body)}, state.offset_sizes)
     }}
  end

  defp write_journal(state, body) do
    {offset, record, state} = encode_append(state, body)
    {offset, flush_records(state, [record])}
  end

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

      {{:value, {from, body}}, rest} ->
        case CreditLedger.admit(state.ledger, byte_size(body)) do
          {:ok, ledger} ->
            {offset, state} = write_journal(%{state | ledger: ledger, waiters: rest}, body)
            GenServer.reply(from, {:ok, offset})
            unpark_waiters(state)

          {:overrun, ledger} ->
            %{state | ledger: ledger}
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

  defp via(operative_class) do
    {:via, Elixir.Registry, {Supervisor.registry_name(), {operative_class, :input_log}}}
  end

  # Offsets are strictly monotonic, so the walk is done the moment it passes
  # `to` — without that halt this ran to EOF on every call, inside the log's own
  # GenServer, blocking every append behind it.
  defp collect_range(device, from, to) do
    device
    |> WalWriter.fold_records([], fn {seq, _header, body}, acc ->
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
    |> Enum.reverse()
  end

  # Highest offset whose append-time is strictly before `ts_ms`, or `nil` when
  # every retained record is at or after it (nothing has aged out). Records with
  # no timestamp are never eligible — an unknown age cannot be judged against a
  # window.
  defp scan_offset_before(device, ts_ms) do
    WalWriter.fold_records(device, nil, fn {seq, header, _body}, found ->
      case header_time(header) do
        ts when is_integer(ts) and ts < ts_ms -> {:cont, seq}
        _ -> {:cont, found}
      end
    end)
  end

  defp do_sweep(operative_class, path) do
    watermark = watermark(operative_class)
    floor = truncation_floor(operative_class, watermark)

    # The *floor* decides whether there is work, not the watermark: under a hold
    # window the floor advances with the clock even while every Entity's
    # consumed-offset stands still, and gating on the watermark would mean a
    # fully-consumed, fully-aged-out log is never reclaimed.
    advanced? = floor != last_sweep_floor(operative_class)

    if is_integer(floor) and floor > 0 and advanced? do
      {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
      :ok = GenServer.call(pid, {:truncate_below, floor})
    else
      maybe_warn_frozen(operative_class, path, watermark)
    end

    remember_sweep(operative_class, watermark, floor)
    :ok
  end

  defp last_sweep_floor(operative_class), do: last_sweep(operative_class, :last_sweep_floor)

  defp last_sweep_watermark(operative_class),
    do: last_sweep(operative_class, :last_sweep_watermark)

  defp last_sweep(operative_class, key) do
    ensure_watermark_table()

    case :ets.lookup(@watermarks, {operative_class, key}) do
      [{_, value}] -> value
      [] -> :none
    end
  end

  # `min(watermark, now - hold_window)` in offset terms — a consumed record is
  # only reclaimable once it has also aged out of the hold window, because a
  # correction replays consumed inputs.
  #
  # With the default `hold_window = 0` the time bound is "now", which every
  # retained record predates, so the watermark alone decides. A raised window
  # holds the tail: `nil` from the scan means nothing has aged out yet, which is
  # "hold everything" — but now as a workflow, self-clearing state rather than
  # the permanent one it used to be.
  defp truncation_floor(_operative_class, nil), do: nil

  defp truncation_floor(operative_class, watermark) do
    case hold_window_ms(operative_class) do
      0 ->
        watermark

      hold_window ->
        case offset_before_time(operative_class, System.system_time(:millisecond) - hold_window) do
          nil -> nil
          aged_out -> min(watermark, aged_out)
        end
    end
  end

  @doc """
  The operative's input-log hold window: the greater of the operator-configured
  floor and the operative's own declared correction window.

  A correction over a 60 s window MEANS retaining 60 s of input log — that is the
  definition of the declaration, not a policy to be negotiated with an operator.
  So the declaration derives it, and an operator who configures a longer floor
  still gets theirs.
  """
  @spec hold_window_ms(binary()) :: non_neg_integer()
  def hold_window_ms(operative_class) when is_binary(operative_class) do
    configured = Application.get_env(:flezha, :input_log_hold_window_ms, 0)
    declared = ClassRegistry.correction_window_ms(operative_class) || 0
    max(configured, declared)
  end

  defp frozen_size_threshold(), do: PersistenceResolver.wal_high_watermark_bytes()

  defp maybe_warn_frozen(operative_class, path, watermark) do
    size =
      case File.stat(path) do
        {:ok, %{size: size}} -> size
        {:error, _} -> 0
      end

    frozen? = last_sweep_watermark(operative_class) == watermark

    if frozen? and size >= frozen_size_threshold() do
      :telemetry.execute(
        [:flezha, :state, :input_log_frozen_watermark],
        %{size_bytes: size},
        %{
          operative: operative_class,
          watermark: watermark,
          lagging_entity: lagging_entity(operative_class)
        }
      )
    end

    :ok
  end

  # Two memos, two questions: the floor answers "is there anything left to
  # reclaim?" (it moves with the clock under a hold window); the watermark
  # answers "is an Entity refusing to consume?" (the frozen-watermark warning).
  defp remember_sweep(operative_class, watermark, floor) do
    ensure_watermark_table()
    :ets.insert(@watermarks, {{operative_class, :last_sweep_floor}, floor})
    :ets.insert(@watermarks, {{operative_class, :last_sweep_watermark}, watermark})
  end

  defp lagging_entity(operative_class) do
    case entity_offsets(operative_class) do
      [] -> nil
      offsets -> offsets |> Enum.min_by(&elem(&1, 1)) |> elem(0)
    end
  end

  defp entity_offsets(operative_class) do
    ensure_watermark_table()

    @watermarks
    |> :ets.match({{operative_class, :"$1"}, :"$2"})
    |> Enum.filter(fn [entity_id, _offset] -> is_tuple(entity_id) end)
    |> Enum.map(fn [entity_id, offset] -> {entity_id, offset} end)
  end

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
