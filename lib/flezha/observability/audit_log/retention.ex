defmodule Flezha.Observability.AuditLog.Retention do
  @moduledoc """
  Periodic retention sweep.

  Every `:retention_check_interval_ms` (default `3_600_000`) the
  worker walks the audit directory, deletes rotated files older than
  `:retention_seconds`, and — if the total bytes on disk still exceed
  `:max_total_bytes` — deletes the oldest in-window rotated files
  until the cap is satisfied. The active file is NEVER deleted.

  After deletions the on-disk `INDEX` is rewritten (drop deleted
  entries) and an `:audit_retention_sweep` event is emitted on the
  `:audit_log` source.
  """

  use GenServer

  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Observability.AuditLog.{Index, Writer}

  @audit_source :audit_log

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

  @doc "Force an immediate sweep — primarily for tests and operator-driven cleanup."
  @spec sweep_now() :: {:ok, map()}
  def sweep_now() do
    GenServer.call(__MODULE__, :sweep_now, 30_000)
  end

  @impl true
  def init(_opts) do
    config = Application.get_env(:flezha, :audit_log, [])

    state = %{
      dir: Keyword.get(config, :dir, "app_data/audit"),
      interval_ms: Keyword.get(config, :retention_check_interval_ms, 3_600_000),
      retention_seconds: Keyword.get(config, :retention_seconds, 30 * 86_400),
      max_total_bytes: Keyword.get(config, :max_total_bytes, 4_294_967_296)
    }

    schedule(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    {:reply, do_sweep(state), state}
  end

  @impl true
  def handle_info(:tick, state) do
    _ = do_sweep(state)
    schedule(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp do_sweep(state) do
    now_ms = System.system_time(:millisecond)
    cutoff_ms = now_ms - state.retention_seconds * 1_000

    rotated =
      Index.list_rotated_files(state.dir)
      |> Enum.map(fn name ->
        path = Path.join(state.dir, name)

        case File.stat(path, time: :posix) do
          {:ok, %File.Stat{size: size, mtime: mtime}} ->
            %{name: name, path: path, size: size, mtime_ms: mtime * 1_000}

          _ ->
            nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    {too_old, in_window} = Enum.split_with(rotated, &(&1.mtime_ms < cutoff_ms))

    {deleted_count, deleted_bytes, surviving} = delete_files(too_old, 0, 0, in_window)

    surviving_sorted = Enum.sort_by(surviving, & &1.mtime_ms)
    total_bytes = Enum.reduce(surviving_sorted, 0, &(&1.size + &2))

    {extra_deleted, extra_bytes, remaining} =
      shrink_to_cap(surviving_sorted, total_bytes, state.max_total_bytes, 0, 0)

    total_deleted = deleted_count + extra_deleted
    total_deleted_bytes = deleted_bytes + extra_bytes
    remaining_count = length(remaining)
    remaining_bytes = Enum.reduce(remaining, 0, &(&1.size + &2))

    if total_deleted > 0 do
      kept = MapSet.new(remaining, & &1.name)
      rewrite_index(state.dir, kept)
    end

    result = %{
      deleted_count: total_deleted,
      deleted_bytes: total_deleted_bytes,
      remaining_count: remaining_count,
      total_bytes: remaining_bytes
    }

    emit_sweep(result)

    {:ok, result}
  end

  defp delete_files([], dc, db, surviving), do: {dc, db, surviving}

  defp delete_files([f | rest], dc, db, surviving) do
    case File.rm(f.path) do
      :ok -> delete_files(rest, dc + 1, db + f.size, surviving)
      _ -> delete_files(rest, dc, db, [f | surviving])
    end
  end

  defp shrink_to_cap(list, total, cap, dc, db) when total <= cap do
    {dc, db, list}
  end

  defp shrink_to_cap([], _total, _cap, dc, db) do
    {dc, db, []}
  end

  defp shrink_to_cap([oldest | rest], total, cap, dc, db) do
    case File.rm(oldest.path) do
      :ok -> shrink_to_cap(rest, total - oldest.size, cap, dc + 1, db + oldest.size)
      _ -> {dc, db, [oldest | rest]}
    end
  end

  defp rewrite_index(dir, kept) do
    case Index.load(dir) do
      {:ok, entries} ->
        kept_entries = Enum.filter(entries, &MapSet.member?(kept, &1.filename))
        _ = Index.write(dir, kept_entries)
        sync_writer_catalog(kept_entries)

      _ ->
        :ok
    end
  end

  defp sync_writer_catalog(_kept_entries) do
    # The Writer caches the catalog in memory. Best-effort cast so
    # subsequent stats() reflect the deletions; if the Writer is not
    # alive (boot/teardown), this is a no-op.
    if Process.whereis(Writer) do
      try do
        GenServer.cast(Writer, :catalog_refresh)
      catch
        _, _ -> :ok
      end
    end

    :ok
  end

  defp emit_sweep(result) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @audit_source,
        name: :audit_retention_sweep,
        measurements: %{
          deleted_count: result.deleted_count,
          deleted_bytes: result.deleted_bytes,
          remaining_count: result.remaining_count,
          total_bytes: result.total_bytes
        },
        metadata: %{}
      })
    )
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
