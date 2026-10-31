defmodule Flezha.State.Lineage do
  @moduledoc """
  A Cell's two small durable records. `<key>.outbox.dedup` keeps, in the 32-byte
  layout a rollback build reads, the persistence watermark and flush cursor (a
  cache that may lag, never lead), the epoch and the folded offset (what a prefix
  cut dropped consumed, `-1` for none). `<key>.lineage` is the home of the last
  two and is installed, directory fsynced, before the dedup file carries either.
  """

  require Logger

  alias Flezha.State.{Frame, Paths, WriterRegistry}

  @type cut :: nil | {:above, non_neg_integer()}
  @type stored :: %{
          id: binary() | nil,
          trail: [{binary(), integer()}],
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
  # The dedup cursors that re-drive every effect record.
  @fresh_cursors %{last_persisted_seq: 0, outbox_flushed_seq: -1}

  @doc """
  Both files merged: the higher epoch with its file's folded offset (the lineage
  file's on a tie), the cut pending only while the dedup epoch is below it. A
  dedup file behind the lineage record holds an older epoch's cursors, so they
  read as an empty log's: every record re-drives, which the sinks dedup.
  """
  @spec read(Paths.cell_id()) :: t()
  def read(cell_id) do
    record = record(cell_id)
    dedup = dedup(cell_id)

    if is_integer(record.epoch) and record.epoch >= dedup.epoch do
      behind? = record.epoch > dedup.epoch
      progress = %{dedup | epoch: record.epoch, folded_offset: record.folded_offset}
      progress = if behind?, do: Map.merge(progress, @fresh_cursors), else: progress
      %{record: record, progress: progress, cut: if(behind?, do: record.cut)}
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

  @doc """
  Land a Cell's files from elsewhere: `fetch` under the Cell's file lock, then a
  pull's lineage record marked landed, or a hydration's `facts` installed with a
  dedup file whose cursors `{0, -1}` re-drive every effect record, then its markers.
  """
  @spec land(Paths.cell_id(), :pulled | map(), (-> result)) :: result when result: term()
  def land(cell_id, facts, fetch) do
    WriterRegistry.with_files(cell_id, fn ->
      with ok when ok == :ok or elem(ok, 0) == :ok <- fetch.(),
           :ok <- arrived(cell_id, facts),
           do: ok
    end)
  end

  defp arrived(cell_id, :pulled) do
    case record(cell_id) do
      %{epoch: nil} -> :ok
      record -> put(cell_id, %{record | landed: true})
    end
  end

  defp arrived(cell_id, facts) do
    with :ok <- put(cell_id, Map.merge(@none, facts)),
         :ok <-
           put_dedup(cell_id, Map.merge(facts, @fresh_cursors)) do
      for {kind, true} <- facts.markers || %{}, do: Paths.write_compensation_marker(cell_id, kind)
      :ok
    end
  end

  @doc """
  How lineage `theirs` stands to `ours` (`%{id, trail}` or `nil`): `:descendant`,
  `{:ancestor, fork}` when ours continues theirs from seq `fork`, `:other`, or
  `:same` when no id tells them apart.
  """
  @spec relation(map() | nil, map() | nil) ::
          :same | :descendant | {:ancestor, integer()} | :other
  def relation(%{id: theirs, trail: their_trail}, %{id: ours, trail: our_trail})
      when is_binary(theirs) and is_binary(ours) and theirs != ours do
    case {List.keymember?(their_trail, ours, 0), List.keyfind(our_trail, theirs, 0)} do
      {true, _} -> :descendant
      {false, {_, fork}} -> {:ancestor, fork}
      {false, nil} -> :other
    end
  end

  def relation(_theirs, _ours), do: :same

  @doc "`record` under a new id, its old one pushed on the trail at `fork_seq` (16 kept)."
  @spec mint(stored(), integer()) :: stored()
  def mint(%{id: id, trail: trail} = record, fork_seq) do
    trail = if id, do: Enum.take([{id, fork_seq} | trail], 16), else: trail
    %{record | id: :crypto.strong_rand_bytes(16), trail: trail, landed: false}
  end

  defp record(cell_id) do
    path = Paths.lineage_path(cell_id)

    with {:ok, <<@head::binary, crc::32, body::binary>>} <- File.read(path),
         ^crc <- Frame.crc32(@head, body),
         %{epoch: epoch} = record when is_integer(epoch) <- :erlang.binary_to_term(body, [:safe]),
         [] <- @keys -- Map.keys(record) do
      record
    else
      {:error, _absent} -> @none
      _unreadable -> unreadable(path)
    end
  rescue
    ArgumentError -> unreadable(Paths.lineage_path(cell_id))
  end

  defp unreadable(path) do
    Logger.warning("[state] unreadable #{path}: reading the dedup file alone")
    @none
  end

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
  def put_dedup(cell_id, %{last_persisted_seq: s, outbox_flushed_seq: f, epoch: e} = record)
      when is_integer(s) and s >= 0 and is_integer(f) and f >= -1 and is_integer(e) and e >= 0 do
    bytes = <<s::64, f::64-signed, e::64, Map.get(record, :folded_offset, -1)::64-signed>>

    # No directory is created (the WAL open owns that, so a write cannot
    # resurrect a deleted Cell dir) and no fsync owed: the writer's batch syncs.
    Paths.atomic_install(Paths.outbox_dedup_path(cell_id), &:file.write(&1, bytes), :outbox_dedup)
  end
end
