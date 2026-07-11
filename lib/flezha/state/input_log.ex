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

  Retention is watermark-driven: each Segment's durable consumed-offset is
  reported into a public ETS table (`record_consumed/3`, repopulated from the
  WAL after a restart — the table is a rebuildable cache) and `sweep/1`
  truncates strictly below `min(watermark, now - hold_window)` with
  `hold_window` defaulting to zero. A Segment that has consumed nothing holds
  the watermark at the log start; a frozen watermark with a growing log emits
  `[:flezha, :state, :input_log_frozen_watermark]` naming the lagging Segment
  instead of silently accumulating.

  Operatives without the tier never start an InputLog and never touch this
  module — the cost-free-default contract.
  """

  use GenServer

  alias Flezha.State.{CreditLedger, FsyncBatcher, Registry, Supervisor, WalWriter}

  # Input-log records carry no wire header; the 16 zero bytes keep the
  # WalWriter record framing (and its scan/rewrite helpers) reusable as-is.
  @zero_header <<0::unit(8)-size(16)>>

  @watermarks :flezha_input_log_watermarks

  # Source-ingestion credit (`credit-grant-flow-control` §source): appends
  # debit the per-Operative ledger; min-watermark advances drain it (batched
  # by the low watermark); at zero credit the append parks via a deferred
  # reply, which blocks the source adapter's synchronous ingest call — the
  # websocket stops reading its socket until credit mints.
  defstruct [
    :operative_class,
    :path,
    :file_handle,
    :next_offset,
    :ledger,
    :offset_sizes,
    :waiters
  ]

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
  durability is fsync-batched like the per-Segment WAL. At zero source
  credit the call parks (deferred reply) until the min-watermark advance
  mints credit — the intended source back-pressure, hence `:infinity`.
  """
  @spec append(binary(), term(), map()) :: {:ok, non_neg_integer()}
  def append(operative_class, message, metadata),
    do: do_append(operative_class, message, metadata, 2)

  defp do_append(operative_class, message, metadata, attempts) do
    {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
    GenServer.call(pid, {:append, message, metadata}, :infinity)
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
  Report a Segment's durable consumed-offset (called when an effect record
  carrying `consumed_offset` becomes durable, and on boot repopulation from
  the WAL tail).
  """
  @spec record_consumed(binary(), tuple(), non_neg_integer()) :: :ok
  def record_consumed(operative_class, segment_id, offset) do
    ensure_watermark_table()
    :ets.insert(@watermarks, {{operative_class, segment_id}, offset})

    # A watermark advance may free source credit; the log server drains
    # the ledger and unparks waiting appends. Async — this runs on the
    # WalWriter's fsync path.
    case Supervisor.lookup({operative_class, :input_log}) do
      {:ok, pid} -> GenServer.cast(pid, :watermark_advance)
      :error -> :ok
    end

    :ok
  end

  @doc "Drop a hard-deleted Segment's watermark entry so it stops holding truncation."
  @spec forget_segment(binary(), tuple()) :: :ok
  def forget_segment(operative_class, segment_id) do
    ensure_watermark_table()
    :ets.delete(@watermarks, {operative_class, segment_id})
    :ok
  end

  @doc """
  Minimum durable consumed-offset across the operative's reporting Segments,
  or `nil` when no Segment has reported (the watermark then holds at the log
  start and nothing truncates).
  """
  @spec watermark(binary()) :: non_neg_integer() | nil
  def watermark(operative_class) do
    case segment_offsets(operative_class) do
      [] -> nil
      offsets -> offsets |> Enum.map(&elem(&1, 1)) |> Enum.min()
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
    root = Registry.state_root()

    case File.ls(root) do
      {:ok, names} ->
        Enum.filter(names, &File.regular?(Path.join([root, &1, "input.log"])))

      {:error, _} ->
        []
    end
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
      offset_sizes: %{},
      waiters: :queue.new()
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

    {:noreply, %{state | file_handle: file, next_offset: last + 1}}
  end

  @impl true
  def handle_call({:append, message, metadata}, from, state) do
    body = :erlang.term_to_binary({message, metadata})

    case CreditLedger.admit(state.ledger, byte_size(body)) do
      {:ok, ledger} ->
        {offset, state} = write_journal(%{state | ledger: ledger}, body)
        {:reply, {:ok, offset}, state}

      {:overrun, ledger} ->
        # Zero source credit: park the caller (deferred reply) until the
        # min-watermark advance drains the ledger. The blocked ingest call
        # is the back-pressure that stops the source reading.
        {:noreply, %{state | ledger: ledger, waiters: :queue.in({from, body}, state.waiters)}}
    end
  end

  def handle_call(:fsync, _from, state) do
    :ok = :file.datasync(state.file_handle)
    {:reply, :ok, state}
  end

  def handle_call(:last_offset, _from, state) do
    {:reply, state.next_offset - 1, state}
  end

  def handle_call({:read_range, from, to}, _from, state) do
    {:reply, collect_range(state.file_handle, 0, from, to, []), state}
  end

  def handle_call({:truncate_below, floor}, _from, state) do
    :ok = :file.close(state.file_handle)
    new_path = WalWriter.rewrite_keep_seq_gt(state.path, floor - 1)
    :ok = File.rename(new_path, state.path)
    {:ok, file} = :file.open(state.path, [:raw, :read, :write, :binary])
    {:ok, _} = :file.position(file, :eof)
    {:reply, :ok, %{state | file_handle: file}}
  end

  # Min-watermark advance (an effect record's consumed-offset became
  # durable): drain the freed bytes from the source ledger and, when the
  # batched grant lands, unpark waiting appends in arrival order.
  @impl true
  def handle_cast(:watermark_advance, state) do
    case watermark(state.operative_class) do
      nil ->
        {:noreply, state}

      wm ->
        {freed, offset_sizes} = pop_freed(state.offset_sizes, wm)
        {grant, ledger} = CreditLedger.drain(state.ledger, freed)
        state = %{state | ledger: ledger, offset_sizes: offset_sizes}
        state = if grant > 0, do: unpark_waiters(state), else: state
        {:noreply, state}
    end
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

  defp write_journal(state, body) do
    offset = state.next_offset
    :ok = :file.write(state.file_handle, WalWriter.encode_record(offset, @zero_header, body))
    FsyncBatcher.mark_dirty({state.operative_class, :input_log})

    {offset,
     %{
       state
       | next_offset: offset + 1,
         offset_sizes: Map.put(state.offset_sizes, offset, byte_size(body))
     }}
  end

  # Free the tracked bytes of every journaled offset at or below the
  # watermark (its consumption is durable; the log slot is reclaimable).
  defp pop_freed(offset_sizes, watermark) do
    Enum.reduce(offset_sizes, {0, offset_sizes}, fn
      {offset, size}, {freed, sizes} when offset <= watermark ->
        {freed + size, Map.delete(sizes, offset)}

      _, acc ->
        acc
    end)
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
      Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)
    )
  end

  defp via(operative_class) do
    {:via, Elixir.Registry, {Supervisor.registry_name(), {operative_class, :input_log}}}
  end

  defp collect_range(device, position, from, to, acc) do
    case WalWriter.decode_record(device, position) do
      {:ok, seq, _header, body, consumed} ->
        acc =
          if seq >= from and seq <= to do
            {message, metadata} = :erlang.binary_to_term(body)
            [{seq, message, metadata} | acc]
          else
            acc
          end

        collect_range(device, position + consumed, from, to, acc)

      {:error, _} ->
        Enum.reverse(acc)
    end
  end

  defp do_sweep(operative_class, path) do
    watermark = watermark(operative_class)
    floor = truncation_floor(watermark)
    advanced? = watermark != last_sweep_watermark(operative_class)

    if is_integer(floor) and floor > 0 and advanced? do
      {:ok, pid} = Supervisor.find_or_start({operative_class, :input_log})
      :ok = GenServer.call(pid, {:truncate_below, floor})
    else
      maybe_warn_frozen(operative_class, path, watermark)
    end

    remember_sweep(operative_class, watermark)
    :ok
  end

  defp last_sweep_watermark(operative_class) do
    ensure_watermark_table()

    case :ets.lookup(@watermarks, {operative_class, :last_sweep_watermark}) do
      [{_, watermark}] -> watermark
      [] -> :none
    end
  end

  # `min(watermark, now - hold_window)` in offset terms. With the default
  # `hold_window = 0` the time bound is "now", which every retained record
  # predates — the watermark alone decides. A raised hold window (the
  # rewind/correction hook) needs a time→offset mapping the log does not
  # carry yet, so it conservatively holds everything.
  defp truncation_floor(nil), do: nil

  defp truncation_floor(watermark) do
    if hold_window_ms() > 0, do: nil, else: watermark
  end

  defp hold_window_ms(), do: Application.get_env(:flezha, :input_log_hold_window_ms, 0)

  defp frozen_size_threshold(),
    do: Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)

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
          lagging_segment: lagging_segment(operative_class)
        }
      )
    end

    :ok
  end

  defp remember_sweep(operative_class, watermark) do
    ensure_watermark_table()
    :ets.insert(@watermarks, {{operative_class, :last_sweep_watermark}, watermark})
  end

  defp lagging_segment(operative_class) do
    case segment_offsets(operative_class) do
      [] -> nil
      offsets -> offsets |> Enum.min_by(&elem(&1, 1)) |> elem(0)
    end
  end

  defp segment_offsets(operative_class) do
    ensure_watermark_table()

    @watermarks
    |> :ets.match({{operative_class, :"$1"}, :"$2"})
    |> Enum.filter(fn [segment_id, _offset] -> is_tuple(segment_id) end)
    |> Enum.map(fn [segment_id, offset] -> {segment_id, offset} end)
  end

  defp ensure_watermark_table() do
    case :ets.whereis(@watermarks) do
      :undefined ->
        try do
          :ets.new(@watermarks, [:set, :public, :named_table, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end
end
