defmodule Flezha.State.Lineage do
  @moduledoc """
  A Cell's two small durable records. `<key>.outbox.dedup` holds, in a 32-byte
  layout a rollback build still reads, the persistence watermark, the flush
  cursor (both a cache the WAL scan repairs; they may lag, never lead), the
  epoch and the folded offset (the highest consumed offset a prefix cut
  dropped, `-1` for none). `<key>.lineage` is the home of the last two: it is
  installed with a directory fsync before the dedup file carries a new copy of
  either. `read/1` takes the higher epoch, with that file's folded offset (the
  lineage file's on a tie), and a pending cut only while the dedup file's epoch
  is below the lineage file's. An unreadable lineage file reads as none.
  """

  require Logger

  alias Flezha.State.{Frame, Paths}

  @type cut :: nil | :all | {:above, non_neg_integer()}
  @type stored :: %{
          id: binary() | nil,
          trail: [{binary(), non_neg_integer()}],
          landed: boolean(),
          epoch: non_neg_integer() | nil,
          folded_offset: integer(),
          cut: cut()
        }
  @type progress :: %{atom() => integer()}
  @type t :: %{record: stored(), progress: progress(), cut: cut()}

  # Magic and version; the crc covers them with the body.
  @head <<"FLZL", 1>>
  @none %{id: nil, trail: [], landed: false, epoch: nil, folded_offset: -1, cut: nil}
  @keys Map.keys(@none)

  @doc "Both files merged: the lineage record, the winning progress, the pending cut."
  @spec read(Paths.cell_id()) :: t()
  def read(cell_id) do
    record = record(cell_id)
    dedup = dedup(cell_id)

    if is_integer(record.epoch) and record.epoch >= dedup.epoch do
      progress = %{dedup | epoch: record.epoch, folded_offset: record.folded_offset}
      %{record: record, progress: progress, cut: if(record.epoch > dedup.epoch, do: record.cut)}
    else
      %{record: record, progress: dedup, cut: nil}
    end
  end

  @spec progress(Paths.cell_id()) :: progress()
  def progress(cell_id), do: read(cell_id).progress

  @doc "Install the lineage record, fsynced with its directory before this returns."
  @spec put(Paths.cell_id(), stored()) :: :ok | {:error, term()}
  def put(cell_id, %{epoch: epoch} = record) when is_integer(epoch) and epoch >= 0 do
    body = :erlang.term_to_binary(Map.take(record, @keys))
    bytes = [@head, <<Frame.crc32(@head, body)::32>>, body]
    Paths.atomic_install(Paths.lineage_path(cell_id), &:file.write(&1, bytes), :lineage)
  end

  defp record(cell_id) do
    path = Paths.lineage_path(cell_id)

    case File.read(path) do
      {:error, _} ->
        @none

      {:ok, bytes} ->
        decode(bytes) ||
          (Logger.warning("[state] unreadable #{path}: reading the dedup file alone") && @none)
    end
  end

  defp decode(<<@head::binary, crc::32, body::binary>>) do
    with ^crc <- Frame.crc32(@head, body),
         %{epoch: epoch} = record when is_integer(epoch) <- :erlang.binary_to_term(body, [:safe]),
         [] <- @keys -- Map.keys(record),
         do: record,
         else: (_ -> nil)
  rescue
    ArgumentError -> nil
  end

  defp decode(_bytes), do: nil

  @doc "The dedup file alone."
  @spec dedup(Paths.cell_id()) :: progress()
  def dedup(cell_id) do
    {seq, flushed, epoch, folded} =
      case File.read(Paths.outbox_dedup_path(cell_id)) do
        {:ok, <<s::64, f::64-signed, e::64, o::64-signed>>} -> {s, f, e, o}
        {:ok, <<s::64, f::64-signed, e::64>>} -> {s, f, e, -1}
        {:ok, <<s::64>>} -> {s, -1, 0, -1}
        _ -> {0, -1, 0, -1}
      end

    %{last_persisted_seq: seq, outbox_flushed_seq: flushed, epoch: epoch, folded_offset: folded}
  end

  @doc "Write the dedup file; an absent `folded_offset` writes `-1`."
  @spec put_dedup(Paths.cell_id(), map()) :: :ok | {:error, term()}
  def put_dedup(
        cell_id,
        %{last_persisted_seq: seq, outbox_flushed_seq: flushed, epoch: epoch} = record
      )
      when is_integer(seq) and seq >= 0 and is_integer(flushed) and flushed >= -1 and
             is_integer(epoch) and epoch >= 0 do
    folded = Map.get(record, :folded_offset, -1)
    bytes = <<seq::64-big, flushed::64-big-signed, epoch::64-big, folded::64-big-signed>>

    # No directory is created (the WAL open owns that, so a write cannot
    # resurrect a deleted Cell dir) and no fsync owed: the writer's batch syncs.
    Paths.atomic_install(Paths.outbox_dedup_path(cell_id), &:file.write(&1, bytes), :outbox_dedup)
  end
end
