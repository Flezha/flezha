defmodule Flezha.State.Retention do
  @moduledoc """
  Periodic sweeper enforcing `time_travel_retention_hours` per Relay class.

  On each tick, walks every Partition under `Registry.state_root/0` and
  prunes:

    * Snapshots strictly older than `now - retention_hours`, keeping at
      minimum the latest snapshot.
    * WAL records whose `seq <= kept_snapshot_seq` (chunk-rewrite via
      `partition.wal.rewriting → rename`).

  `retention_hours = 0` disables time-travel for that Relay class:
  the WAL is truncated to the latest snapshot's seq immediately on
  every tick ("compact and forget").

  Tick cadence is `:flezha, :time_travel_retention_check_interval_ms`,
  default 60 000 ms. Validated at `init/1`; out-of-range values fail
  startup loudly.
  """

  use GenServer

  alias Flezha.State.{Metrics, PersistenceResolver, Registry, Supervisor, WalWriter}

  @default_interval_ms 60_000
  @min_interval_ms 100
  @max_interval_ms 24 * 60 * 60 * 1000

  defstruct [:tick_ref, :tick_interval_ms]

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
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, configured_interval_ms())

    cond do
      not is_integer(interval) ->
        {:stop, {:invalid_interval, interval}}

      interval < @min_interval_ms or interval > @max_interval_ms ->
        {:stop, {:interval_out_of_range, interval, [@min_interval_ms, @max_interval_ms]}}

      true ->
        ref = Process.send_after(self(), :tick, interval)
        {:ok, %__MODULE__{tick_ref: ref, tick_interval_ms: interval}}
    end
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    sweep_all()
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    sweep_all()
    ref = Process.send_after(self(), :tick, state.tick_interval_ms)
    {:noreply, %{state | tick_ref: ref}}
  end

  # ---------- Sweep ----------

  defp sweep_all() do
    Enum.each(Registry.all_partitions(), fn {_, relay_class, _, _} = pid ->
      hours = PersistenceResolver.retention_hours(relay_class)
      sweep_partition(pid, hours)
    end)
  end

  @doc """
  Sweep a single Partition. Public for `flz state` debugging.
  """
  @spec sweep_partition(Registry.partition_id(), non_neg_integer()) :: :ok
  def sweep_partition(partition_id, retention_hours)
      when is_integer(retention_hours) and retention_hours >= 0 do
    snaps = Registry.snapshot_paths(partition_id)

    case snaps do
      [] ->
        :ok

      _ ->
        kept_seq = oldest_kept_snapshot_seq(snaps, retention_hours)

        Enum.each(snaps, fn {seq, path} ->
          if seq < kept_seq, do: File.rm(path)
        end)

        rewrite_wal_excluding_pre_seq(partition_id, kept_seq)
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
  Atomic via `partition.wal.rewriting → rename`.
  """
  @spec rewrite_wal_excluding_pre_seq(Registry.partition_id(), non_neg_integer()) :: :ok
  def rewrite_wal_excluding_pre_seq(partition_id, keep_above_seq) do
    case Supervisor.lookup(partition_id) do
      {:ok, pid} ->
        # Delegate through the writer so it controls its own file handle.
        WalWriter.truncate_up_to(pid, keep_above_seq)

      :error ->
        # No live writer — perform the rewrite in-process.
        rewrite_offline(partition_id, keep_above_seq)
    end
  end

  defp rewrite_offline(partition_id, keep_above_seq) do
    wal_path = Registry.wal_path(partition_id)

    if File.exists?(wal_path) do
      tmp_path = wal_path <> ".rewriting"
      {:ok, src} = :file.open(wal_path, [:raw, :read, :binary])
      {:ok, dst} = :file.open(tmp_path, [:raw, :write, :binary])

      try do
        copy_keep_seq_gt(src, dst, 0, keep_above_seq)
        :ok = :file.sync(dst)
      after
        _ = :file.close(src)
        _ = :file.close(dst)
      end

      :ok = File.rename(tmp_path, wal_path)
      Metrics.put(Metrics.for_partition(partition_id), :wal_size_bytes, file_size(wal_path))
    end

    :ok
  end

  defp copy_keep_seq_gt(src, dst, position, keep_above_seq) do
    case WalWriter.decode_record(src, position) do
      {:ok, seq, header, change_bytes, consumed} ->
        if seq > keep_above_seq do
          :ok = :file.write(dst, WalWriter.encode_record(seq, header, change_bytes))
        end

        copy_keep_seq_gt(src, dst, position + consumed, keep_above_seq)

      {:error, _} ->
        :ok
    end
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

  defp configured_interval_ms() do
    Application.get_env(:flezha, :time_travel_retention_check_interval_ms, @default_interval_ms)
  end
end
