defmodule Flezha.Logs.Retention do
  @moduledoc """
  One kind's periodic retention sweep.

  Every `retention_check_interval_ms` the worker walks the kind's directory,
  deletes rotated files older than `retention_seconds`, and while the total
  bytes on disk still exceed `max_total_bytes` deletes the oldest remaining
  rotated files. The active file is never deleted.

  After deletions the on-disk `INDEX` is rewritten without the deleted entries
  and the kind's sweep event is emitted.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:tick]

  alias Flezha.Process.Cspec
  alias Flezha.Config.Durations
  alias Flezha.Logs.{Index, Kind}

  def child_spec(%Kind{} = kind),
    do: Cspec.build({__MODULE__, kind.id}, {__MODULE__, :start_link, [kind]}, :worker_permanent)

  def start_link(%Kind{} = kind),
    do: GenServer.start_link(__MODULE__, kind, name: Kind.name(kind, :retention))

  @doc "Sweep `kind` now, for tests and user-driven cleanup."
  @spec sweep_now(Kind.t()) :: {:ok, map()}
  def sweep_now(%Kind{} = kind),
    do:
      GenServer.call(
        Kind.name(kind, :retention),
        :sweep_now,
        Durations.get(:audit_log_sweep_call_ms)
      )

  @impl true
  def init(kind), do: {:ok, arm_tick(%{kind: kind, dir: Kind.dir(kind)})}

  @impl true
  def handle_call(:sweep_now, _from, state), do: {:reply, do_sweep(state), state}

  @impl true
  def handle_info(:tick, state) do
    _ = do_sweep(state)
    {:noreply, arm_tick(state)}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp do_sweep(%{kind: kind, dir: dir}) do
    cutoff_ms = System.system_time(:millisecond) - Kind.get(kind, :retention_seconds) * 1_000

    rotated =
      for name <- Index.list_rotated_files(kind, dir),
          path = Path.join(dir, name),
          {:ok, %File.Stat{size: size, mtime: mtime}} <- [File.stat(path, time: :posix)],
          do: %{name: name, path: path, size: size, mtime_ms: mtime * 1_000}

    {too_old, in_window} = Enum.split_with(rotated, &(&1.mtime_ms < cutoff_ms))
    {deleted_count, deleted_bytes, surviving} = delete_files(too_old, 0, 0, in_window)

    surviving = Enum.sort_by(surviving, & &1.mtime_ms)
    total_bytes = Enum.reduce(surviving, 0, &(&1.size + &2))

    {extra_deleted, extra_bytes, remaining} =
      shrink_to_cap(surviving, total_bytes, Kind.get(kind, :max_total_bytes), 0, 0)

    if deleted_count + extra_deleted > 0,
      do: rewrite_index(kind, dir, MapSet.new(remaining, & &1.name))

    result = %{
      deleted_count: deleted_count + extra_deleted,
      deleted_bytes: deleted_bytes + extra_bytes,
      remaining_count: length(remaining),
      total_bytes: Enum.reduce(remaining, 0, &(&1.size + &2))
    }

    Kind.emit(kind, :swept, result)
    {:ok, result}
  end

  defp delete_files([], dc, db, surviving), do: {dc, db, surviving}

  defp delete_files([f | rest], dc, db, surviving) do
    case File.rm(f.path) do
      :ok -> delete_files(rest, dc + 1, db + f.size, surviving)
      _ -> delete_files(rest, dc, db, [f | surviving])
    end
  end

  defp shrink_to_cap(list, total, cap, dc, db) when total <= cap, do: {dc, db, list}
  defp shrink_to_cap([], _total, _cap, dc, db), do: {dc, db, []}

  defp shrink_to_cap([oldest | rest], total, cap, dc, db) do
    case File.rm(oldest.path) do
      :ok -> shrink_to_cap(rest, total - oldest.size, cap, dc + 1, db + oldest.size)
      _ -> {dc, db, [oldest | rest]}
    end
  end

  # The writer caches the catalog: told to reload it, so its stats show the
  # deletions; a writer not running (boot, teardown) has nothing to reload.
  defp rewrite_index(kind, dir, kept) do
    with {:ok, entries} <- Index.load(dir) do
      _ = Index.write(dir, Enum.filter(entries, &MapSet.member?(kept, &1.filename)))
      GenServer.cast(Kind.name(kind, :writer), :catalog_refresh)
    end
  end

  defp arm_tick(state),
    do: arm_timer(state, :tick, Kind.get(state.kind, :retention_check_interval_ms), :tick)
end
