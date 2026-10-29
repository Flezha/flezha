defmodule Flezha.Logs.Writer do
  @moduledoc """
  Owns one kind's active append-only file (`<prefix>-current.jsonl`).

  Appends are group-committed: each one queues behind the others that arrived
  with it, and once the mailbox holds no more (or 512 are queued) the batch is
  written as one write. Under `:per_record` and `:group_commit` the batch is
  synced once before every caller in it is answered, so an answer means the
  record is on disk; under `:interval` (`:batched` is its older spelling) the
  callers are answered after the write and a timer bounds the unsynced window.

  A direct kind's append is bounded by its caller: at `max_pending` queued
  appends it is refused at once (`{:error, :overloaded}`), and one that waits
  past `append_ms` answers `{:error, :timeout}`, while the record is still
  written when the writer reaches it. Both are counted on the kind's counters,
  read by `counters/1`, and an overload is announced at most once a second.

  Each committed batch is broadcast on `Flezha.Observability.PubSub`, topic
  `"flezha:logs:<kind>"`, which every controller's tail hears. The running
  SHA-256 of the active file is kept so a rotation never re-reads it, and the
  on-disk `INDEX` is rewritten atomically as it rotates.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:fsync]
  require Logger

  alias Flezha.Process.Cspec
  alias Flezha.Config.Durations
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Logs.{Index, Kind, Kinds, Reader}
  alias Flezha.Observability.Bus
  alias Flezha.SDK.SafeCall

  @batch_max 512
  @dropped 1
  @timeout 2
  @overload_second 3
  @post_close 4

  @doc "The PubSub topic each committed batch of `kind` is broadcast on."
  @spec topic(Kind.t() | atom()) :: String.t()
  def topic(%Kind{id: id}), do: topic(id)
  def topic(id) when is_atom(id), do: "flezha:logs:#{id}"

  def child_spec(%Kind{} = kind),
    do: Cspec.build({__MODULE__, kind.id}, {__MODULE__, :start_link, [kind]}, :worker_permanent)

  def start_link(%Kind{} = kind, opts \\ []),
    do: GenServer.start_link(__MODULE__, {kind, opts}, name: Kind.name(kind, :writer))

  @doc """
  Append a pre-encoded record line and wait for it: `:ok` once it is written
  (and synced, unless the kind syncs on an interval), else the refusal.
  """
  @spec append(Kind.t(), binary()) :: :ok | {:error, term()}
  def append(%Kind{} = kind, line) when is_binary(line) do
    with {:ok, pid, counters} <- whereis(kind),
         :ok <- under_bound(kind, pid, counters) do
      GenServer.call(pid, {:append, line}, Kind.get(kind, :append_ms))
    end
  catch
    :exit, {:timeout, _} -> count(kind, @timeout, {:error, :timeout})
    :exit, _gone -> {:error, :closed}
  end

  @doc """
  Hand a line to the writer for the caller behind `from`, which the writer
  answers as it answers an `append/2`; `nil` answers nobody.
  """
  @spec forward(Kind.t(), GenServer.from() | nil, binary()) :: :ok
  def forward(%Kind{} = kind, from, line) do
    case whereis(kind) do
      {:ok, pid, _counters} -> GenServer.cast(pid, {:append, from, line})
      {:error, _} = closed -> reply(from, closed)
    end
  end

  @doc """
  Drain pending appends, sync, and close. After return the writer refuses
  further appends with `{:error, :closed}` and counts them. Idempotent.
  """
  @spec close(Kind.t(), timeout()) :: :ok
  def close(%Kind{} = kind, timeout \\ Durations.get(:audit_log_writer_call_ms)),
    do: SafeCall.bounded(Kind.name(kind, :writer), :close, timeout, on_error: :ok)

  @doc "The node stop's row: every running kind's writer closed after the flush, before the hubs stop."
  def shutdown_steps,
    do: [%{scope: :node, order: 30, name: :logs, run: &__MODULE__.close_step/2}]

  @doc false
  def close_step(_target, budget) do
    for kind <- Kinds.all(),
        match?({:ok, _, _}, whereis(kind)),
        do: close(kind, min(budget, Durations.get(:shutdown_finalise_call_ms)))

    :ok
  end

  @doc "Rotate the active file. Returns `{:ok, %{rotated_to, size_bytes, record_count, sha256}}`."
  @spec rotate_now(Kind.t(), integer() | nil) :: {:ok, map()} | {:error, term()}
  def rotate_now(%Kind{} = kind, ts_us \\ nil),
    do:
      GenServer.call(
        Kind.name(kind, :writer),
        {:rotate_now, ts_us},
        Durations.get(:audit_log_rotate_ms)
      )

  @doc "A snapshot of the writer's state and counters."
  @spec stats(Kind.t()) :: map()
  def stats(%Kind{} = kind), do: call(kind, :stats)

  @doc "The rotated files' catalog."
  @spec catalog(Kind.t()) :: [Index.entry()]
  def catalog(%Kind{} = kind), do: call(kind, :catalog)

  @doc "True when the writer is closed (during shutdown) or not running."
  @spec closed?(Kind.t()) :: boolean()
  def closed?(%Kind{} = kind),
    do:
      SafeCall.bounded(
        Kind.name(kind, :writer),
        :closed?,
        Durations.get(:audit_log_writer_call_ms),
        on_error: true
      )

  @doc "The kind's refusal counters on this controller; zero when it is not running."
  @spec counters(Kind.t()) :: %{atom() => non_neg_integer()}
  def counters(%Kind{} = kind) do
    case whereis(kind) do
      {:ok, _pid, ref} -> read_counters(ref)
      {:error, _} -> %{dropped_count: 0, timeout_count: 0, post_close_dropped: 0}
    end
  end

  defp read_counters(ref) do
    %{
      dropped_count: :atomics.get(ref, @dropped),
      timeout_count: :atomics.get(ref, @timeout),
      post_close_dropped: :atomics.get(ref, @post_close)
    }
  end

  # ---------- caller side ----------

  defp whereis(kind) do
    case Registry.lookup(Flezha.Logs.Registry, {kind.id, :writer}) do
      [{pid, counters}] when counters != nil -> {:ok, pid, counters}
      _ -> {:error, :closed}
    end
  rescue
    ArgumentError -> {:error, :closed}
  end

  defp under_bound(kind, pid, counters) do
    case Kind.get(kind, :max_pending) do
      nil ->
        :ok

      bound ->
        if Bus.mailbox_full?(pid, bound), do: overloaded(kind, counters), else: :ok
    end
  end

  defp overloaded(kind, counters) do
    :atomics.add(counters, @dropped, 1)
    second = System.monotonic_time(:second)
    last = :atomics.get(counters, @overload_second)

    if last != second and
         :atomics.compare_exchange(counters, @overload_second, last, second) == :ok,
       do: Kind.emit(kind, :overloaded, %{count: 1}, %{kind: kind.id, node: node()})

    {:error, :overloaded}
  end

  defp count(kind, index, answer) do
    with {:ok, _pid, counters} <- whereis(kind), do: :atomics.add(counters, index, 1)
    answer
  end

  # ---------- GenServer ----------

  @impl true
  def init({kind, opts}) do
    dir = Keyword.get(opts, :dir) || Kind.dir(kind)
    :ok = Layout.claim(kind.layout, dir)
    counters = :atomics.new(4, signed: true)

    {_new, _old} =
      Registry.update_value(Flezha.Logs.Registry, {kind.id, :writer}, fn _ -> counters end)

    path = Path.join(dir, Kind.active(kind))
    {fd, size, hash_ctx, record_count} = open_active(path)

    {:ok,
     %{
       kind: kind,
       dir: dir,
       fd: fd,
       path: path,
       opened_at_ms: System.system_time(:millisecond),
       size_bytes: size,
       record_count: record_count,
       hash_ctx: hash_ctx,
       fsync_mode: fsync_mode(Kind.get(kind, :fsync_mode)),
       fsync_interval_ms: Kind.get(kind, :fsync_interval_ms),
       catalog: load_catalog(kind, dir),
       counters: counters,
       batch: [],
       batched: 0,
       closed?: false
     }}
  end

  @impl true
  def handle_call({:append, line}, from, state), do: {:noreply, queue(state, from, line)}

  def handle_call(:close, _from, %{closed?: true} = state), do: {:reply, :ok, state}

  def handle_call(:close, _from, state) do
    state = state |> commit() |> cancel_timer(:fsync)
    safe_sync(state.fd)
    if state.fd, do: :file.close(state.fd)
    {:reply, :ok, %{state | fd: nil, closed?: true}}
  end

  def handle_call({:rotate_now, _ts_us}, _from, %{closed?: true} = state),
    do: {:reply, {:error, :closed}, state}

  def handle_call({:rotate_now, ts_us}, _from, state) do
    case state |> commit() |> do_rotate(ts_us || System.system_time(:microsecond)) do
      {:ok, info, state} -> {:reply, {:ok, info}, state}
      {:error, reason, restored} -> {:reply, {:error, reason}, restored}
    end
  end

  def handle_call(:stats, _from, state) do
    snapshot = %{
      current_file: state.path,
      current_file_bytes: state.size_bytes,
      current_record_count: state.record_count,
      opened_at_ms: state.opened_at_ms,
      catalog_count: length(state.catalog),
      catalog_total_bytes: Enum.reduce(state.catalog, 0, &(&1.size_bytes + &2)),
      oldest_ts_us: oldest_ts(state.catalog),
      fsync_mode: state.fsync_mode,
      closed: state.closed?
    }

    {:reply, Map.merge(snapshot, read_counters(state.counters)), state}
  end

  def handle_call(:catalog, _from, state), do: {:reply, state.catalog, state}
  def handle_call(:closed?, _from, state), do: {:reply, state.closed?, state}

  @impl true
  def handle_cast({:append, from, line}, state), do: {:noreply, queue(state, from, line)}

  def handle_cast(:catalog_refresh, state),
    do: {:noreply, %{state | catalog: load_catalog(state.kind, state.dir, state.catalog)}}

  def handle_cast(_other, state), do: {:noreply, state}

  @impl true
  def handle_info(:commit, state), do: {:noreply, commit(state)}

  def handle_info(:fsync_now, state) do
    safe_sync(state.fd)
    {:noreply, state}
  end

  def handle_info({:emit_rotated_event, info}, state) do
    # Emitted from its own process, so the bus's call into a sink that writes
    # this very log never waits on this writer's mailbox.
    kind = state.kind
    meta = %{rotated_to: info.rotated_to, sha256: info.sha256}
    measured = %{size_bytes: info.size_bytes, record_count: info.record_count}
    _ = spawn(fn -> Kind.emit(kind, :rotated, measured, meta) end)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # ---------- group commit ----------

  defp queue(%{closed?: true} = state, from, _line) do
    :atomics.add(state.counters, @post_close, 1)
    reply(from, {:error, :closed})
    state
  end

  defp queue(state, from, line) do
    state = %{state | batch: [{from, line} | state.batch], batched: state.batched + 1}

    cond do
      state.batched >= @batch_max or mailbox_empty?() -> commit(state)
      state.batched == 1 -> tap(state, fn _ -> send(self(), :commit) end)
      true -> state
    end
  end

  defp mailbox_empty?(), do: Process.info(self(), :message_queue_len) == {:message_queue_len, 0}

  defp commit(%{batch: []} = state), do: state

  defp commit(state) do
    entries = Enum.reverse(state.batch)
    lines = Enum.map(entries, &elem(&1, 1))
    state = %{state | batch: [], batched: 0}

    {answer, state} =
      case :file.write(state.fd, lines) do
        :ok ->
          {:ok, state |> wrote(lines, length(entries)) |> settle()}

        {:error, reason} = err ->
          Logger.error("[logs.writer] #{state.kind.id} write error: #{inspect(reason)}")
          {err, state}
      end

    Enum.each(entries, fn {from, _line} -> reply(from, answer) end)
    if answer == :ok, do: broadcast(state.kind, lines)
    state
  end

  defp wrote(state, lines, count) do
    %{
      state
      | size_bytes: state.size_bytes + IO.iodata_length(lines),
        hash_ctx: :crypto.hash_update(state.hash_ctx, lines),
        record_count: state.record_count + count
    }
  end

  defp settle(%{fsync_mode: :interval} = state) do
    if timer_armed?(state, :fsync),
      do: state,
      else: arm_timer(state, :fsync, state.fsync_interval_ms, :fsync_now)
  end

  defp settle(state) do
    safe_sync(state.fd)
    state
  end

  defp reply(nil, _answer), do: :ok
  defp reply(from, answer), do: GenServer.reply(from, answer)

  # A tail on any controller hears the batch; a node whose PubSub is not up
  # (a writer started on its own in a test) has nobody to tell.
  defp broadcast(kind, lines) do
    Phoenix.PubSub.broadcast(
      Flezha.Observability.PubSub,
      topic(kind),
      {:log_lines, kind.id, lines}
    )
  rescue
    ArgumentError -> :ok
  end

  defp fsync_mode(:batched), do: :interval
  defp fsync_mode(mode) when mode in [:per_record, :group_commit, :interval], do: mode

  # ---------- open / rotate ----------

  defp load_catalog(kind, dir, fallback \\ nil) do
    case Index.load(dir) do
      {:ok, list} ->
        list

      {:error, _reason} when fallback != nil ->
        fallback

      {:error, reason} ->
        if File.exists?(Index.path(dir)),
          do:
            Logger.warning(
              "[logs.writer] #{kind.id} INDEX unreadable (#{inspect(reason)}); rebuilding"
            )

        case Index.rebuild(kind, dir) do
          {:ok, list} -> list
          _ -> []
        end
    end
  end

  defp open_active(path) do
    size =
      case File.stat(path) do
        {:ok, %File.Stat{size: s}} -> s
        _ -> 0
      end

    {hash_ctx, record_count} =
      if size > 0, do: scan_existing(path), else: {:crypto.hash_init(:sha256), 0}

    {:ok, fd} = :file.open(path, [:write, :append, :raw, :binary])
    {fd, size, hash_ctx, record_count}
  end

  defp scan_existing(path) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          do_scan(fd, :crypto.hash_init(:sha256), 0)
        after
          :file.close(fd)
        end

      _ ->
        {:crypto.hash_init(:sha256), 0}
    end
  end

  defp do_scan(fd, ctx, count) do
    case :file.read(fd, 65_536) do
      {:ok, chunk} ->
        newlines = length(:binary.matches(chunk, "\n"))
        do_scan(fd, :crypto.hash_update(ctx, chunk), count + newlines)

      _eof_or_error ->
        {ctx, count}
    end
  end

  defp do_rotate(state, ts_us) do
    state = cancel_timer(state, :fsync)
    safe_sync(state.fd)
    if state.fd, do: :file.close(state.fd)

    sha = state.hash_ctx |> :crypto.hash_final() |> Base.encode16(case: :lower)
    rotated_name = rotated_filename(state.kind, ts_us, state.catalog)
    rotated_path = Path.join(state.dir, rotated_name)

    case File.rename(state.path, rotated_path) do
      :ok ->
        commit_rotation(state, rotated_name, rotated_path, sha)

      {:error, reason} ->
        # Rotation failed: re-open the active file so appends keep landing.
        {fd, size, hash_ctx, count} = open_active(state.path)

        {:error, reason,
         %{state | fd: fd, size_bytes: size, hash_ctx: hash_ctx, record_count: count}}
    end
  end

  # The rename landed: catalogue what moved, open a fresh active file, and
  # report the entry the rotated event carries.
  defp commit_rotation(state, rotated_name, rotated_path, sha) do
    {first_ts, last_ts, _n} = Reader.head_metadata(rotated_path)

    entry = %{
      filename: rotated_name,
      first_ts_us: first_ts,
      last_ts_us: last_ts,
      record_count: state.record_count,
      size_bytes: state.size_bytes,
      sha256: sha,
      clock: :wall
    }

    catalog = state.catalog ++ [entry]
    _ = Index.write(state.dir, catalog)
    {fd, _, hash_ctx, count} = open_active(state.path)

    info = %{
      rotated_to: rotated_name,
      size_bytes: entry.size_bytes,
      record_count: entry.record_count,
      sha256: sha
    }

    send(self(), {:emit_rotated_event, info})

    {:ok, info,
     %{
       state
       | fd: fd,
         opened_at_ms: System.system_time(:millisecond),
         size_bytes: 0,
         record_count: count,
         hash_ctx: hash_ctx,
         catalog: catalog
     }}
  end

  defp safe_sync(nil), do: :ok

  defp safe_sync(fd) do
    :file.sync(fd)
  catch
    _, _ -> :ok
  end

  defp oldest_ts(catalog) do
    catalog |> Enum.map(& &1.first_ts_us) |> Enum.reject(&is_nil/1) |> Enum.min(fn -> nil end)
  end

  defp rotated_filename(kind, ts_us, catalog) do
    {{y, mo, d}, {h, mi, s}} = :calendar.system_time_to_universal_time(ts_us, :microsecond)

    stamp =
      "~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ"
      |> :io_lib.format([y, mo, d, h, mi, s])
      |> IO.iodata_to_binary()

    prefix = "#{kind.prefix}-#{stamp}-"

    used =
      for %{filename: name} <- catalog, String.starts_with?(name, prefix) do
        case Integer.parse(String.replace_leading(name, prefix, "")) do
          {n, _} -> n
          _ -> 0
        end
      end

    "#{prefix}#{Enum.max([0 | used]) + 1}.jsonl"
  end

  defp call(kind, request),
    do:
      GenServer.call(Kind.name(kind, :writer), request, Durations.get(:audit_log_writer_call_ms))
end
