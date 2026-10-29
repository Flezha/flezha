defmodule Flezha.State.Retention do
  @moduledoc """
  Sweeper enforcing `time_travel_retention_hours` per Relay class.

  On each pass, walks every Cell under `Paths.state_root/0` and prunes
  snapshots strictly older than `now - retention_hours` (keeping at minimum the
  latest snapshot) and WAL records whose `seq <= kept_snapshot_seq`, by chunk
  rewrite through `cell.wal.rewriting` -> rename.

  `retention_hours = 0` disables time-travel for that Relay class: the WAL
  is truncated to the latest snapshot's seq on every pass — compact and forget.

  On the same tick it also runs the **finished-relay** sweep: a kept finished
  relay's state and completion record (`Flezha.State.Completion`) are removed
  once its window (`PersistenceResolver.kept_until/2`) is past, and a class directory
  whose delete a write in flight held back (`Paths.deleting?/1`) is deleted.

  Holds its own sweep timer, `:time_travel_retention_check_interval_ms`.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:sweep]

  alias Flezha.Config.Durations
  alias Flezha.Relay.Lookup
  alias Flezha.State.InputLog.Sweep
  alias Flezha.State.Supervisor, as: StateSupervisor

  alias Flezha.State.WalWriter.{Epoch, RemoteGate}

  alias Flezha.State.{
    Completion,
    Lineage,
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

  @doc "Run a sweep tick synchronously. Used by tests and `flz state` ops."
  @spec sweep_now() :: :ok
  def sweep_now(), do: GenServer.call(__MODULE__, :sweep_now, Durations.get(:state_sweep_call_ms))

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
    Enum.each(Paths.all_cells(), fn {relay_class, _, _} = pid ->
      sweep_cell(pid, PersistenceResolver.retention_hours(relay_class))
    end)
  end

  # Finished-relay retention: reclaim a kept relay's state and completion
  # record once `finished_at` plus its window is in the past, and finish a
  # delete a write in flight held back. A finished relay is fully torn down (no
  # live writer), so the whole class dir goes, through the one guard every
  # state deletion passes.
  defp sweep_completed() do
    now = System.system_time(:second)

    Enum.each(Paths.class_dirs(), fn {relay_class, dir} ->
      if unfinished_delete?(relay_class, dir) or expired?(relay_class, dir, now),
        do: StateSupervisor.delete_class(relay_class)
    end)
  end

  # A relay started again under the id since owns the directory now.
  defp unfinished_delete?(relay_class, dir),
    do: Paths.deleting?(dir) and match?({:error, _}, Lookup.overwatch(relay_class))

  defp expired?(relay_class, dir, now) do
    case Completion.read_in(dir) do
      {:ok, %{finished_at: finished_at}} when is_integer(finished_at) ->
        PersistenceResolver.kept_until(relay_class, finished_at) < now

      _not_finished ->
        false
    end
  end

  # Effectively-once tier: truncate each relay's input-log below its
  # min durable consumed-offset watermark (`durable-input-log`).
  defp sweep_input_logs() do
    Enum.each(Sweep.all_classes(), &Sweep.run/1)
  end

  @doc """
  Sweep a single Cell. Public for `flz state` debugging.
  """
  @spec sweep_cell(Paths.cell_id(), non_neg_integer()) :: :ok
  def sweep_cell(cell_id, retention_hours)
      when is_integer(retention_hours) and retention_hours >= 0 do
    snaps = Paths.snapshot_paths(cell_id)

    case snaps do
      [] ->
        :ok

      _ ->
        kept_seq = oldest_kept_snapshot_seq(snaps, retention_hours)

        Enum.each(snaps, fn {seq, path} ->
          if seq < kept_seq, do: File.rm(path)
        end)

        cut(cell_id, kept_seq)
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

  # The WAL keeps the records above `seq`. Under the Cell's file lock no writer
  # opens; one already up cuts its own file, asked once the lock is let go, since
  # its boot may be waiting on that lock.
  defp cut(cell_id, seq) do
    case WriterRegistry.with_files(cell_id, fn -> cut_offline(cell_id, seq) end) do
      {:ok, writer} -> WalWriter.truncate_up_to(writer, seq)
      _cut_or_no_wal -> :ok
    end
  end

  defp cut_offline(cell_id, seq) do
    path = Paths.wal_path(cell_id)

    # A bump still pending is the writer's to finish at its boot; a fold before it
    # would count records it drops. A cut reads the writer's state from disk, gated
    # by the persisted flush cursor.
    with :error <- WriterRegistry.lookup(cell_id),
         %{cut: nil, record: record, progress: progress} <- Lineage.read(cell_id),
         {:ok, device} <- :file.open(path, [:raw, :read, :write, :binary]) do
      holder = %{
        cell_id: cell_id,
        lineage: record,
        folded_offset: progress.folded_offset,
        outbox_epoch: progress.epoch,
        outbox_durable_seq: progress.outbox_flushed_seq,
        remote: RemoteGate.new(cell_id, :record),
        wal_path: path,
        file_handle: device,
        seq_index: SeqIndex.new(RecordFrame.file_size(path))
      }

      {cut, %{file_handle: file}} = Epoch.cut_below(holder, seq, 0)
      _ = :file.close(file)

      if cut == :ok,
        do: Metrics.put(Metrics.for_cell(cell_id), :wal_size_bytes, RecordFrame.file_size(path))
    end
  end

  defp mtime_seconds(path) do
    case :file.read_file_info(path, time: :posix) do
      {:ok, info} -> elem(info, 5)
      _ -> 0
    end
  end
end
