defmodule Flezha.State.Retention do
  @moduledoc """
  Sweeper enforcing `time_travel_retention_hours` per Operative class.

  On each pass, walks every Entity under `Paths.state_root/0` and prunes
  snapshots strictly older than `now - retention_hours` (keeping at minimum the
  latest snapshot) and WAL records whose `seq <= kept_snapshot_seq`, by chunk
  rewrite through `entity.wal.rewriting` -> rename.

  `retention_hours = 0` disables time-travel for that Operative class: the WAL
  is truncated to the latest snapshot's seq on every pass — compact and forget.

  On the same tick it also runs the **finished-job** sweep: a finished workflow
  operative's retained state and completion record (`Flezha.State.Completion`)
  are removed once `finished_at + completed_retention_hours` is past.

  Holds its own sweep timer, `:time_travel_retention_check_interval_ms`.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:sweep]

  alias Flezha.Config.Durations
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.InputLog.Sweep

  alias Flezha.State.{
    Completion,
    Metrics,
    Paths,
    PersistenceResolver,
    RecordFrame,
    SeqIndex,
    WalWriter,
    WriterRegistry
  }

  defstruct timers: %{}

  # ---------- Public API ----------

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run a sweep tick synchronously. Used by tests and `flz state` procs."
  @spec sweep_now() :: :ok
  def sweep_now(), do: GenServer.call(__MODULE__, :sweep_now)

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    {:ok, arm_sweep(%__MODULE__{})}
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
    state = arm_sweep(state)
    sweep_all()
    sweep_completed()
    sweep_input_logs()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # A non-positive interval turns the sweep off: nothing is armed and the key
  # simply holds no timer.
  defp arm_sweep(state) do
    interval = Durations.get(:time_travel_retention_check_interval_ms)
    if interval > 0, do: arm_timer(state, :sweep, interval, :retention_tick), else: state
  end

  # ---------- Sweep ----------

  defp sweep_all() do
    Enum.each(Paths.all_entities(), fn {operative_class, _, _} = pid ->
      hours =
        if ClassRegistry.capable?(operative_class, :time_travel?) do
          PersistenceResolver.retention_hours(operative_class)
        else
          # No time-travel for this kind (a durable :workflow operative): keep
          # only the latest snapshot, never retain history for point-in-time.
          0
        end

      sweep_entity(pid, hours)
    end)
  end

  # Finished-job retention: reclaim a finished workflow operative's retained
  # state + completion record once `finished_at + completed_retention_hours`
  # is in the past. A finished operative is fully torn down (no live writer), so
  # the whole class dir goes, through the one guard every state deletion passes.
  defp sweep_completed() do
    now = System.system_time(:second)

    Enum.each(Paths.operative_classes(), fn operative_class ->
      with {:ok, %{finished_at: finished_at}} when is_integer(finished_at) <-
             Completion.read(operative_class),
           window = PersistenceResolver.completed_retention_hours(operative_class),
           true <- finished_at + window * 3600 < now do
        Paths.delete_class_dir(operative_class)
      else
        _ -> :ok
      end
    end)
  end

  # Effectively-once tier: truncate each operative's input-log below its
  # min durable consumed-offset watermark (`durable-input-log`).
  defp sweep_input_logs() do
    Enum.each(Sweep.all_classes(), &Sweep.run/1)
  end

  @doc """
  Sweep a single Entity. Public for `flz state` debugging.
  """
  @spec sweep_entity(Paths.entity_id(), non_neg_integer()) :: :ok
  def sweep_entity(entity_id, retention_hours)
      when is_integer(retention_hours) and retention_hours >= 0 do
    snaps = Paths.snapshot_paths(entity_id)

    case snaps do
      [] ->
        :ok

      _ ->
        kept_seq = oldest_kept_snapshot_seq(snaps, retention_hours)

        Enum.each(snaps, fn {seq, path} ->
          if seq < kept_seq, do: File.rm(path)
        end)

        rewrite_wal_excluding_pre_seq(entity_id, kept_seq)
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
  Atomic via `entity.wal.rewriting → rename`.
  """
  @spec rewrite_wal_excluding_pre_seq(Paths.entity_id(), non_neg_integer()) :: :ok
  def rewrite_wal_excluding_pre_seq(entity_id, keep_above_seq) do
    case WriterRegistry.lookup(entity_id) do
      {:ok, pid} ->
        # Delegate through the writer so it controls its own file handle.
        WalWriter.truncate_up_to(pid, keep_above_seq)

      :error ->
        # No live writer — perform the rewrite in-process.
        rewrite_offline(entity_id, keep_above_seq)
    end
  end

  # No live writer, so the rewrite happens in-process — but through the same
  # primitive the writer uses, not a second copy of it.
  defp rewrite_offline(entity_id, keep_above_seq) do
    wal_path = Paths.wal_path(entity_id)

    case :file.open(wal_path, [:raw, :read, :write, :binary]) do
      {:ok, device} ->
        case RecordFrame.cut_below(
               wal_path,
               device,
               SeqIndex.new(file_size(wal_path)),
               keep_above_seq + 1
             ) do
          {:ok, _cut, file, _index} ->
            _ = :file.close(file)
            Metrics.put(Metrics.for_entity(entity_id), :wal_size_bytes, file_size(wal_path))

          _ ->
            _ = :file.close(device)
        end

      {:error, _} ->
        :ok
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
