defmodule Flezha.State.Retention do
  @moduledoc """
  Sweeper enforcing `time_travel_retention_hours` per Operative class.

  On each pass, walks every Segment under `Registry.state_root/0` and
  prunes:

    * Snapshots strictly older than `now - retention_hours`, keeping at
      minimum the latest snapshot.
    * WAL records whose `seq <= kept_snapshot_seq` (chunk-rewrite via
      `segment.wal.rewriting → rename`).

  `retention_hours = 0` disables time-travel for that Operative class:
  the WAL is truncated to the latest snapshot's seq immediately on
  every pass ("compact and forget").

  On the same tick it also runs the **finished-job** sweep: a finished
  bounded operative's retained state + completion record
  (`Flezha.State.Completion`) is removed once `finished_at +
  bounded_completed_retention_hours` is in the past.

  Holds its own sweep timer (`:time_travel_retention_check_interval_ms`,
  default 60 000 ms) after `consume-recovery-mode-and-self-heal` §4
  removed the `Flezha.Reconciler.Loop` periodic sweep that previously
  drove it.
  """

  use GenServer

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{Completion, Metrics, PersistenceResolver, Registry, Supervisor, WalWriter}

  defstruct []

  # ---------- Public API ----------

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run a sweep tick synchronously. Used by tests and `flz state` ops."
  @spec sweep_now() :: :ok
  def sweep_now(), do: GenServer.call(__MODULE__, :sweep_now)

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    # Own the operative_class→kind table: Retention is always-on and is the
    # process-less consumer that most needs the durable-kind resolution.
    ClassRegistry.ensure_table()
    schedule_sweep()
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    sweep_all()
    sweep_completed()
    sweep_input_logs()
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:retention_tick, state) do
    schedule_sweep()
    sweep_all()
    sweep_completed()
    sweep_input_logs()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule_sweep() do
    interval = Application.get_env(:flezha, :time_travel_retention_check_interval_ms, 60_000)
    if interval > 0, do: Process.send_after(self(), :retention_tick, interval)
  end

  # ---------- Sweep ----------

  defp sweep_all() do
    Enum.each(Registry.all_segments(), fn {operative_class, _, _} = pid ->
      hours =
        if ClassRegistry.capable?(operative_class, :time_travel?) do
          PersistenceResolver.retention_hours(operative_class)
        else
          # No time-travel for this kind (a durable :bounded operative): keep
          # only the latest snapshot, never retain history for point-in-time.
          0
        end

      sweep_segment(pid, hours)
    end)
  end

  # Finished-job retention: reclaim a finished bounded operative's retained
  # state + completion record once `finished_at + bounded_completed_retention_hours`
  # is in the past. A finished operative is fully torn down (no live writer), so
  # the whole class dir is safe to remove wholesale.
  defp sweep_completed() do
    now = System.system_time(:second)
    root = Registry.state_root()

    Enum.each(class_dirs(root), fn operative_class ->
      with {:ok, %{finished_at: finished_at}} when is_integer(finished_at) <-
             Completion.read(operative_class),
           window = PersistenceResolver.bounded_completed_retention_hours(operative_class),
           true <- finished_at + window * 3600 < now do
        _ = File.rm_rf(Path.join(root, operative_class))
      else
        _ -> :ok
      end
    end)
  end

  defp class_dirs(root) do
    case File.ls(root) do
      {:ok, names} -> Enum.filter(names, &File.dir?(Path.join(root, &1)))
      {:error, _} -> []
    end
  end

  # Effectively-once tier: truncate each operative's input-log below its
  # min durable consumed-offset watermark (`durable-input-log`).
  defp sweep_input_logs() do
    Enum.each(Flezha.State.InputLog.all_classes(), &Flezha.State.InputLog.sweep/1)
  end

  @doc """
  Sweep a single Segment. Public for `flz state` debugging.
  """
  @spec sweep_segment(Registry.segment_id(), non_neg_integer()) :: :ok
  def sweep_segment(segment_id, retention_hours)
      when is_integer(retention_hours) and retention_hours >= 0 do
    snaps = Registry.snapshot_paths(segment_id)

    case snaps do
      [] ->
        :ok

      _ ->
        kept_seq = oldest_kept_snapshot_seq(snaps, retention_hours)

        Enum.each(snaps, fn {seq, path} ->
          if seq < kept_seq, do: File.rm(path)
        end)

        rewrite_wal_excluding_pre_seq(segment_id, kept_seq)
    end

    :ok
  end

  defp oldest_kept_snapshot_seq(snaps, 0) do
    # retention disabled — keep only the latest.
    {seq, _} = List.last(snaps)
    seq
  end

  defp oldest_kept_snapshot_seq(snaps, retention_hours) do
    cutoff_ts = System.system_time(:second) - retention_hours * 3600

    {latest_seq, _} = List.last(snaps)

    older_than_cutoff =
      snaps
      |> Enum.filter(fn {_, path} -> mtime_seconds(path) <= cutoff_ts end)

    case older_than_cutoff do
      [] ->
        latest_seq

      _ ->
        # Keep all snapshots from the newest of the older-than-cutoff set
        # forward; older ones are reclaimable.
        {seq, _} = List.last(older_than_cutoff)
        seq
    end
  end

  @doc """
  Rewrite the WAL keeping only records with `seq > keep_above_seq`.
  Atomic via `segment.wal.rewriting → rename`.
  """
  @spec rewrite_wal_excluding_pre_seq(Registry.segment_id(), non_neg_integer()) :: :ok
  def rewrite_wal_excluding_pre_seq(segment_id, keep_above_seq) do
    case Supervisor.lookup(segment_id) do
      {:ok, pid} ->
        # Delegate through the writer so it controls its own file handle.
        WalWriter.truncate_up_to(pid, keep_above_seq)

      :error ->
        # No live writer — perform the rewrite in-process.
        rewrite_offline(segment_id, keep_above_seq)
    end
  end

  # No live writer, so the rewrite happens in-process — but through the same
  # primitive the writer uses, not a second copy of it.
  defp rewrite_offline(segment_id, keep_above_seq) do
    wal_path = Registry.wal_path(segment_id)

    if File.exists?(wal_path) do
      tmp_path = WalWriter.rewrite_keep_seq_gt(wal_path, keep_above_seq)
      :ok = File.rename(tmp_path, wal_path)
      Metrics.put(Metrics.for_segment(segment_id), :wal_size_bytes, file_size(wal_path))
    end

    :ok
  end

  defp mtime_seconds(path) do
    case :file.read_file_info(path, time: :posix) do
      {:ok, info} -> elem(info, 5)
      _ -> 0
    end
  end

  defp file_size(path) do
    case :file.read_file_info(path) do
      {:ok, info} -> elem(info, 1)
      _ -> 0
    end
  end
end
