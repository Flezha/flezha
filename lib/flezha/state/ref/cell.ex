defmodule Flezha.State.Ref.Cell do
  @moduledoc "`{:cell, cell_id}`: a Cell's file set, or a steps instance's task journal."

  use Flezha.State.Ref

  alias Flezha.State.{Lineage, Paths, Restore}

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
end
