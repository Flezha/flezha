defmodule Flezha.Observability.AuditLog.Writer do
  @moduledoc """
  Owns the active append-only audit file (`audit-current.jsonl`).

  Synchronously accepts `{:write, record_bin}` GenServer calls from
  the sink. In `:per_record` fsync mode the writer fsyncs after every
  write before replying — so the sink's producer-side `GenServer.call`
  observes durable acceptance. In `:batched` mode a timer schedules
  `:fsync_now` no later than `:fsync_interval_ms` after the first
  buffered write.

  The Writer also computes the running SHA-256 of the active file so
  rotation does not have to re-read it.

  Catalog updates: on rotation, the writer atomically rewrites the
  on-disk `INDEX` via `Flezha.Observability.AuditLog.Index.write/2`
  with the rotated entry appended.

  A best-effort post-write fan-out to local in-process tail
  subscribers (registered via `subscribe_tail/0` / `unsubscribe_tail/1`)
  is fired after every successful write so `Flezha.Service.AuditLog.tail/2`
  can stream live records without going through Phoenix.PubSub.
  """

  use GenServer
  require Logger

  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Observability.AuditLog.Index

  @active_filename "audit-current.jsonl"
  @audit_source :audit_log

  @type state :: %{
          dir: String.t(),
          fd: :file.io_device() | nil,
          path: String.t(),
          opened_at_ms: integer(),
          size_bytes: non_neg_integer(),
          record_count: non_neg_integer(),
          hash_ctx: term(),
          fsync_mode: :per_record | :batched,
          fsync_interval_ms: pos_integer(),
          pending_fsync_timer: reference() | nil,
          catalog: [Index.entry()],
          subscribers: %{reference() => pid()},
          closed?: boolean()
        }

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Synchronously append a pre-encoded record line."
  @spec write(binary(), timeout()) :: :ok | {:error, term()}
  def write(record_bin, timeout \\ 30_000) when is_binary(record_bin) do
    GenServer.call(__MODULE__, {:write, record_bin}, timeout)
  end

  @doc """
  Drain pending writes, fsync, and close. After return the writer
  rejects further writes with `{:error, :closed}` and `closed?/0`
  returns `true`. Idempotent.
  """
  @spec close(timeout()) :: :ok
  def close(timeout \\ 5_000) do
    try do
      GenServer.call(__MODULE__, :close, timeout)
    catch
      :exit, _ -> :ok
    end
  end

  @doc "Rotate the active file. Returns `{:ok, %{rotated_to, size, count, sha256}}`."
  @spec rotate_now(integer() | nil, timeout()) :: {:ok, map()} | {:error, term()}
  def rotate_now(ts_us \\ nil, timeout \\ 10_000) do
    GenServer.call(__MODULE__, {:rotate_now, ts_us}, timeout)
  end

  @doc "Snapshot of writer state for `Flezha.Service.AuditLog.stats/1`."
  @spec stats() :: map()
  def stats() do
    GenServer.call(__MODULE__, :stats)
  end

  @doc "Current catalog (rotated files), used by `list_files/1`."
  @spec catalog() :: [Index.entry()]
  def catalog() do
    GenServer.call(__MODULE__, :catalog)
  end

  @doc "Subscribe the calling pid to live record messages."
  @spec subscribe_tail() :: {:ok, reference()}
  def subscribe_tail() do
    GenServer.call(__MODULE__, {:subscribe_tail, self()})
  end

  @spec unsubscribe_tail(reference()) :: :ok
  def unsubscribe_tail(ref) when is_reference(ref) do
    GenServer.call(__MODULE__, {:unsubscribe_tail, ref})
  end

  @doc "True if the writer has been closed (e.g. during shutdown)."
  @spec closed?() :: boolean()
  def closed?() do
    try do
      GenServer.call(__MODULE__, :closed?)
    catch
      :exit, _ -> true
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    config = Application.get_env(:flezha, :audit_log, [])
    dir = Keyword.get(opts, :dir) || Keyword.get(config, :dir, "app_data/audit")
    File.mkdir_p!(dir)

    fsync_mode = Keyword.get(config, :fsync_mode, :per_record)
    fsync_interval_ms = Keyword.get(config, :fsync_interval_ms, 100)

    catalog =
      case Index.load(dir) do
        {:ok, list} ->
          list

        {:error, reason} ->
          if File.exists?(Index.path(dir)) do
            Logger.warning("[audit_log.writer] INDEX unreadable (#{inspect(reason)}); rebuilding")
          end

          case Index.rebuild(dir) do
            {:ok, list} -> list
            _ -> []
          end
      end

    path = Path.join(dir, @active_filename)
    {fd, size, hash_ctx, record_count} = open_active(path)

    {:ok,
     %{
       dir: dir,
       fd: fd,
       path: path,
       opened_at_ms: System.system_time(:millisecond),
       size_bytes: size,
       record_count: record_count,
       hash_ctx: hash_ctx,
       fsync_mode: fsync_mode,
       fsync_interval_ms: fsync_interval_ms,
       pending_fsync_timer: nil,
       catalog: catalog,
       subscribers: %{},
       closed?: false
     }}
  end

  @impl true
  def handle_call({:write, _}, _from, %{closed?: true} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:write, record_bin}, _from, state) do
    case do_write(state, record_bin) do
      {:ok, state} ->
        broadcast_record(state, record_bin)
        {:reply, :ok, state}

      {:error, reason} = err ->
        Logger.error("[audit_log.writer] write error: #{inspect(reason)}")
        {:reply, err, state}
    end
  end

  def handle_call(:close, _from, %{closed?: true} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:close, _from, state) do
    state = cancel_fsync_timer(state)
    safe_sync(state.fd)
    if state.fd, do: :file.close(state.fd)

    # Notify subscribers so any in-flight tails terminate cleanly.
    Enum.each(state.subscribers, fn {ref, pid} ->
      send(pid, {:audit_writer_closed, ref})
    end)

    {:reply, :ok, %{state | fd: nil, closed?: true, pending_fsync_timer: nil}}
  end

  def handle_call({:rotate_now, ts_us}, _from, %{closed?: true} = state) do
    _ = ts_us
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:rotate_now, ts_us}, _from, state) do
    ts_us = ts_us || System.system_time(:microsecond)

    case do_rotate(state, ts_us) do
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

    {:reply, snapshot, state}
  end

  def handle_call(:catalog, _from, state) do
    {:reply, state.catalog, state}
  end

  def handle_call({:subscribe_tail, pid}, _from, state) do
    ref = Process.monitor(pid)
    {:reply, {:ok, ref}, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  def handle_call({:unsubscribe_tail, ref}, _from, state) do
    case Map.pop(state.subscribers, ref) do
      {nil, _} ->
        {:reply, :ok, state}

      {_pid, rest} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{state | subscribers: rest}}
    end
  end

  def handle_call(:closed?, _from, state), do: {:reply, state.closed?, state}

  @impl true
  def handle_cast(:catalog_refresh, state) do
    catalog =
      case Index.load(state.dir) do
        {:ok, list} -> list
        _ -> state.catalog
      end

    {:noreply, %{state | catalog: catalog}}
  end

  def handle_cast(_other, state), do: {:noreply, state}

  @impl true
  def handle_info(:fsync_now, %{fd: nil} = state) do
    {:noreply, %{state | pending_fsync_timer: nil}}
  end

  def handle_info(:fsync_now, state) do
    safe_sync(state.fd)
    {:noreply, %{state | pending_fsync_timer: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
  end

  def handle_info({:emit_rotated_event, info}, state) do
    # Emit from a spawned process so the Bus → Sink → Writer call
    # chain triggered by this event does not block this Writer's
    # mailbox.
    _ = spawn(fn -> emit_rotated_event(info) end)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # ---------- Internal: open / write / rotate ----------

  defp open_active(path) do
    initial_size =
      case File.stat(path) do
        {:ok, %File.Stat{size: s}} -> s
        _ -> 0
      end

    {hash_ctx, record_count} =
      if initial_size > 0 do
        scan_existing(path)
      else
        {:crypto.hash_init(:sha256), 0}
      end

    {:ok, fd} = :file.open(path, [:write, :append, :raw, :binary])
    {fd, initial_size, hash_ctx, record_count}
  end

  defp scan_existing(path) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          do_scan(fd, :crypto.hash_init(:sha256), 0, "")
        after
          :file.close(fd)
        end

      _ ->
        {:crypto.hash_init(:sha256), 0}
    end
  end

  defp do_scan(fd, ctx, count, leftover) do
    case :file.read(fd, 65_536) do
      {:ok, chunk} ->
        ctx = :crypto.hash_update(ctx, chunk)
        buf = leftover <> chunk
        {newlines, rest} = count_newlines(buf, 0)
        do_scan(fd, ctx, count + newlines, rest)

      :eof ->
        {ctx, count}

      {:error, _} ->
        {ctx, count}
    end
  end

  defp count_newlines(<<>>, n), do: {n, ""}

  defp count_newlines(bin, n) do
    case :binary.split(bin, "\n") do
      [_left, rest] -> count_newlines(rest, n + 1)
      [tail] -> {n, tail}
    end
  end

  defp do_write(state, record_bin) do
    case :file.write(state.fd, record_bin) do
      :ok ->
        size = state.size_bytes + byte_size(record_bin)
        ctx = :crypto.hash_update(state.hash_ctx, record_bin)
        count = state.record_count + 1

        state = %{
          state
          | size_bytes: size,
            hash_ctx: ctx,
            record_count: count
        }

        state = maybe_sync(state)
        {:ok, state}

      {:error, _} = err ->
        err
    end
  end

  defp maybe_sync(%{fsync_mode: :per_record} = state) do
    safe_sync(state.fd)
    state
  end

  defp maybe_sync(%{fsync_mode: :batched, pending_fsync_timer: nil} = state) do
    timer = Process.send_after(self(), :fsync_now, state.fsync_interval_ms)
    %{state | pending_fsync_timer: timer}
  end

  defp maybe_sync(state), do: state

  defp do_rotate(state, ts_us) do
    state = cancel_fsync_timer(state)
    safe_sync(state.fd)
    if state.fd, do: :file.close(state.fd)

    sha = :crypto.hash_final(state.hash_ctx) |> Base.encode16(case: :lower)

    rotated_name = rotated_filename(ts_us, state.catalog)
    rotated_path = Path.join(state.dir, rotated_name)

    case File.rename(state.path, rotated_path) do
      :ok ->
        {first_ts, last_ts, _n} = file_ts_range(rotated_path)

        entry = %{
          filename: rotated_name,
          first_ts_us: first_ts,
          last_ts_us: last_ts,
          record_count: state.record_count,
          size_bytes: state.size_bytes,
          sha256: sha
        }

        catalog = state.catalog ++ [entry]
        _ = Index.write(state.dir, catalog)

        {fd, _, hash_ctx, count} = open_active(state.path)

        new_state = %{
          state
          | fd: fd,
            opened_at_ms: System.system_time(:millisecond),
            size_bytes: 0,
            record_count: count,
            hash_ctx: hash_ctx,
            catalog: catalog
        }

        info = %{
          rotated_to: rotated_name,
          size_bytes: entry.size_bytes,
          record_count: entry.record_count,
          sha256: sha
        }

        send(self(), {:emit_rotated_event, info})

        {:ok, info, new_state}

      {:error, reason} ->
        # Rotation failed — re-open the active file so we can keep accepting writes.
        {fd, size, hash_ctx, count} = open_active(state.path)

        restored = %{
          state
          | fd: fd,
            size_bytes: size,
            hash_ctx: hash_ctx,
            record_count: count
        }

        {:error, reason, restored}
    end
  end

  defp file_ts_range(path) do
    Flezha.Observability.AuditLog.Reader.head_metadata(path)
  end

  defp cancel_fsync_timer(%{pending_fsync_timer: nil} = state), do: state

  defp cancel_fsync_timer(%{pending_fsync_timer: ref} = state) when is_reference(ref) do
    Process.cancel_timer(ref)
    %{state | pending_fsync_timer: nil}
  end

  defp safe_sync(nil), do: :ok

  defp safe_sync(fd) do
    try do
      :file.sync(fd)
    catch
      _, _ -> :ok
    end
  end

  defp broadcast_record(%{subscribers: subs}, _record_bin) when map_size(subs) == 0, do: :ok

  defp broadcast_record(%{subscribers: subs}, record_bin) do
    case Flezha.Observability.AuditLog.Codec.decode(record_bin) do
      {:ok, record} ->
        Enum.each(subs, fn {ref, pid} ->
          send(pid, {:audit_record, ref, record})
        end)

      _ ->
        :ok
    end
  end

  defp emit_rotated_event(info) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @audit_source,
        name: :audit_rotated,
        measurements: %{size_bytes: info.size_bytes, record_count: info.record_count},
        metadata: %{rotated_to: info.rotated_to, sha256: info.sha256}
      })
    )
  end

  defp oldest_ts([]), do: nil

  defp oldest_ts(catalog) do
    catalog
    |> Enum.map(& &1.first_ts_us)
    |> Enum.reject(&is_nil/1)
    |> Enum.min(fn -> nil end)
  end

  # ---------- Rotation filename ----------

  defp rotated_filename(ts_us, catalog) do
    {{y, mo, d}, {h, mi, s}} =
      :calendar.system_time_to_universal_time(ts_us, :microsecond)

    stamp =
      :io_lib.format(
        "~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ",
        [y, mo, d, h, mi, s]
      )
      |> IO.iodata_to_binary()

    n = next_suffix(stamp, catalog)
    "audit-#{stamp}-#{n}.jsonl"
  end

  defp next_suffix(stamp, catalog) do
    prefix = "audit-" <> stamp <> "-"

    used =
      catalog
      |> Enum.map(& &1.filename)
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.map(fn name ->
        rest = String.replace_leading(name, prefix, "")

        case Integer.parse(rest) do
          {n, _} -> n
          _ -> 0
        end
      end)

    case used do
      [] -> 1
      list -> Enum.max(list) + 1
    end
  end
end
