defmodule Flezha.State.Ref.Cell do
  @moduledoc "`{:cell, cell_id}`: a Cell's file set, or a steps instance's task journal."

  use Flezha.State.Ref

  alias Flezha.State.{InputLog, Lineage, Paths, Restore, TaskJournal, WalWriter}

  def wal_path({:cell, cell_id}), do: Paths.wal_path(cell_id)
  def files({:cell, cell_id}), do: Paths.cell_files(cell_id)
  def snapshots({:cell, cell_id}), do: Paths.snapshot_paths(cell_id)
  def snapshot_path({:cell, cell_id}, seq), do: Paths.snapshot_path(cell_id, seq)
  def epoch({:cell, cell_id}), do: Lineage.progress(cell_id).epoch
  def markers({:cell, cell_id}), do: Paths.compensation_markers(cell_id)

  # A snapshot not held yet can be asked for, so its name resolves by its seq.
  def file_path({:cell, cell_id} = ref, name),
    do: super(ref, name) || Paths.snapshot_file_path(cell_id, name)

  def lineage({:cell, cell_id}) do
    %{record: record} = Lineage.read(cell_id)
    if record.id, do: Map.take(record, [:id, :trail])
  end

  # The bind-time restore's predicate, so the tier and the restore agree.
  def has_local_state?({:cell, cell_id}), do: Restore.has_persisted_state?(cell_id)

  # The small files no record implies land after the data (`Lineage.land/3`).
  def land({:cell, cell_id}, arrival, fetch), do: Lineage.land(cell_id, arrival, fetch)

  # The input log's floor for a Cell, and a task journal's consumed input, or the
  # bind would re-deliver every retained input the instance had consumed.
  def seed_floor({:cell, {class, _, _} = cell_id}) do
    with offset when is_integer(offset) and offset >= 0 <-
           WalWriter.consumed_offset_on_disk(cell_id),
         do: InputLog.record_consumed(class, cell_id, offset)

    if cell_id == Paths.journal_id(class), do: TaskJournal.release_input(class), else: :ok
  end
end
