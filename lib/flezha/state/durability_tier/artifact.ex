defmodule Flezha.State.DurabilityTier.Artifact do
  @moduledoc """
  The local side of the remote durability tier: what a shippable reference maps
  onto on disk, how a seq range is read out of it, how a fetched one is written
  back.

  **Seq-addressed** references (a kind with a WAL, `Flezha.State.Ref`) are read
  as a range of WAL records. **Whole-payload** ones are the files they name,
  packed by `pack/1`, each keyed by its writer or claim-fenced because their
  replicas are per-node.

  Record bundles are read at ship time and re-framed, never copied as file byte
  ranges: compaction and correction rewrite the live WAL in place, so a byte
  offset moves where a seq range does not.
  """

  alias Flezha.Dlq
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.State.Frame
  alias Flezha.State.Wire
  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{Lineage, Paths, RecordFrame, Ref}

  @type ref :: Manifest.ref()

  @doc """
  Whether `ref` is packed whole rather than read as a seq range — the one place
  the two artifact families are told apart.
  """
  @spec whole_payload?(ref()) :: boolean()
  def whole_payload?(ref) when is_tuple(ref), do: Ref.wal_path(ref) == nil

  @doc """
  Every hub whose registry cspec exists on this node, stamped with `writer`.

  The writer is part of the identity rather than something to arbitrate: each
  controller holds its own replica of one gossiped registry, so they are
  separate lineages rather than writers competing for one.
  """
  @spec cspec_refs(binary()) :: [ref()]
  def cspec_refs(writer) when is_binary(writer) do
    case File.ls(cspec_root()) do
      {:ok, entries} ->
        entries |> Enum.map(&{:cspec, {&1, writer}}) |> Enum.filter(&Ref.has_local_state?/1)

      {:error, _} ->
        []
    end
  end

  @doc """
  Every parked-failure set of `relay_class` with a directory on this node.

  Deliberately not filtered by `Ref.has_local_state?/1`: an emptied set has to stay
  in the sweep so its remote entry stops naming a blob whose entries the user
  has since resolved. `Flezha.Dlq.Store` removes its directory only once that
  has landed.
  """
  @spec dlq_refs(binary()) :: [ref()]
  def dlq_refs(relay_class) when is_binary(relay_class),
    do: relay_class |> Dlq.Store.tasklets() |> Enum.map(&{:dlq, &1})

  @doc """
  Every relay class with parked failures on this node.

  Unioned with the classes that have Cell state, because the two sets differ:
  a stopped relay keeps its parked failures, and losing them on a disk death
  is exactly what shipping them prevents.
  """
  @spec dlq_classes() :: [binary()]
  def dlq_classes(),
    do: Dlq.Store.tasklets() |> Enum.map(fn {class, _tasklet} -> class end) |> Enum.uniq()

  @doc """
  Every control-plane store with local files, stamped with `writer`.

  Like the cspec these are per-writer replicas rather than one arbitrated
  lineage: each controller's copy is its own truth, and hydration takes the
  newest.
  """
  @spec control_refs(binary()) :: [ref()]
  def control_refs(writer) when is_binary(writer) do
    Ref.Control.stores()
    |> Enum.map(&{:control, {&1, writer}})
    |> Enum.filter(&Ref.has_local_state?/1)
  end

  defp cspec_root(), do: Layout.dir(:process_hub)

  @doc """
  How new `ref`'s local copy is: its WAL-owner epoch, then the highest seq it
  holds (`-1` for none), or `:unreadable` when its WAL refuses a record. A
  rewind or a realign bumps the epoch and every write raises the seq, so of
  two copies of one lineage the greater marker is the newer;
  `Manifest.freshness/1` reads a shipped cut the same way.
  """
  @spec freshness(ref()) :: {non_neg_integer(), integer()} | :unreadable
  def freshness(ref) do
    {Ref.epoch(ref), top_seq(Ref.snapshots(ref), seq_bounds(ref))}
  rescue
    RuntimeError -> :unreadable
  end

  @doc "`freshness/1` of every durable reference `relay_class` holds on this node."
  @spec freshness_of(binary()) :: %{ref() => term()}
  def freshness_of(relay_class),
    do: Map.new(Paths.durable_refs(relay_class), &{&1, freshness(&1)})

  @doc "`Ref.lineage/1` of every durable reference `relay_class` holds on this node."
  @spec lineage_of(binary()) :: %{ref() => map() | nil}
  def lineage_of(relay_class),
    do: Map.new(Paths.durable_refs(relay_class), &{&1, Ref.lineage(&1)})

  @doc """
  Whether one relay's copy marked `theirs` replaces the local copy marked
  `ours` (both `%{ref => marker}`, lineages `%{ref => Ref.lineage/1}`): each of its
  markers can be read, it shares a reference with ours unless ours is empty, and
  on each shared one it continues ours, is unrelated, moved past the fork ours
  continues it from, or, of one lineage, is at least as new (a tie goes to it).
  A reference only ours holds stays (`Flezha.State.Adoption`).
  """
  @spec copy_supersedes?(%{ref() => term()}, %{ref() => term()}, map(), map()) :: boolean()
  def copy_supersedes?(theirs, ours, their_lineage \\ %{}, our_lineage \\ %{}) do
    shared = Map.take(ours, Map.keys(theirs))

    theirs != %{} and Enum.all?(Map.values(theirs), &match?({_, _}, &1)) and
      (ours == %{} or shared != %{}) and
      Enum.all?(shared, fn {ref, mine} ->
        relation = Lineage.relation(their_lineage[ref], our_lineage[ref])
        supersedes?(relation, Map.fetch!(theirs, ref), mine)
      end)
  end

  defp supersedes?(_relation, _theirs, :unreadable), do: false
  defp supersedes?(:same, theirs, ours), do: theirs >= ours
  defp supersedes?({:ancestor, fork}, {_epoch, top}, _ours), do: top > fork
  defp supersedes?(_descendant_or_other, _theirs, _ours), do: true

  @doc "The highest seq a snapshot set and WAL bounds cover, `-1` for neither."
  @spec top_seq([{non_neg_integer(), binary()}], {non_neg_integer(), non_neg_integer()} | :empty) ::
          integer()
  def top_seq(snapshots, bounds) do
    top_snapshot = Enum.reduce(snapshots, -1, fn {seq, _path}, acc -> max(seq, acc) end)

    case bounds do
      {_lo, hi} -> max(hi, top_snapshot)
      :empty -> top_snapshot
    end
  end

  @doc """
  Lowest and highest record seq currently in `ref`'s WAL, or `:empty`.

  The low bound is what compaction left behind, and it is the evidence that a
  shipped range below it is now covered by a snapshot rather than lost.
  """
  @spec seq_bounds(ref()) :: {non_neg_integer(), non_neg_integer()} | :empty
  def seq_bounds(ref) do
    fold_wal(ref, :empty, fn {seq, _header, _body}, acc ->
      case acc do
        :empty -> {:cont, {seq, seq}}
        {lo, _hi} -> {:cont, {lo, seq}}
      end
    end)
  end

  @doc """
  The records of `ref` with `from <= seq <= to`, re-framed verbatim, plus the
  highest source position they acknowledge.

  `source_offsets` carries `consumed_offset` — the input-log position the
  shipped state cut is consistent with, read out of the effect records in the
  bundle itself. That is what lets a hydrated Cell resume replay at the cut
  instead of from the beginning.
  """
  @spec read_range(ref(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary(), %{count: non_neg_integer(), source_offsets: map()}}
  def read_range(ref, from, to) do
    {acc, count, offset} =
      fold_wal(ref, {[], 0, nil}, fn {seq, header, body}, {acc, count, offset} = unchanged ->
        cond do
          seq > to ->
            {:halt, unchanged}

          seq < from ->
            {:cont, unchanged}

          true ->
            {:cont,
             {[Frame.encode_record(seq, header, body) | acc], count + 1,
              max_consumed(offset, header, body)}}
        end
      end)

    offsets = if offset == nil, do: %{}, else: %{consumed_offset: offset}

    {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary(),
     %{count: count, source_offsets: offsets}}
  end

  defp max_consumed(current, header, body) do
    case Wire.decode_effect_record(header, body) do
      %{consumed_offset: offset} when is_integer(offset) -> max(current || offset, offset)
      _ -> current
    end
  end

  # `fold_path!/3` owns the open-fold-close and the torn-tail contract; a
  # whole-file reference has no records at all. A refused record raises rather
  # than shipping the prefix before it as the whole WAL.
  defp fold_wal(ref, acc, fun) do
    case Ref.wal_path(ref) do
      nil -> acc
      path -> RecordFrame.fold_path!(path, acc, fun)
    end
  end

  # ---------- hydration writes ----------

  @doc """
  Write fetched WAL bytes as `ref`'s local WAL, and fetched snapshot bytes as
  its snapshot files.

  Written through `Paths.atomic_install/3` — the same `.transferring` →
  fsync → rename the cross-controller pull uses, so a failure leaves the
  destination cold rather than half-restored.
  """
  @spec write_wal(ref(), binary()) :: :ok | {:error, term()}
  def write_wal(_ref, <<>>), do: :ok

  def write_wal(ref, bytes) do
    case Ref.wal_path(ref) do
      nil -> {:error, :not_seq_addressed}
      path -> atomic_write(path, bytes)
    end
  end

  @spec write_snapshot(ref(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def write_snapshot(ref, seq, bytes) do
    case Ref.snapshot_path(ref, seq) do
      nil -> {:error, :not_snapshotted}
      path -> atomic_write(path, bytes)
    end
  end

  @doc """
  A whole-payload artifact's bytes: every file it names, in one blob.

  One blob rather than one per file because the set is the unit of consistency —
  a parked record's index line and its payload must land together or not at all.
  Term format for the same reason the manifest uses it: both ends are the same
  controller build. `:empty` means the artifact has nothing on disk right now,
  which for a parked set is the ordinary end state after the last entry is
  resolved.
  """
  @spec pack(ref()) :: {:ok, binary()} | :empty | {:error, term()}
  def pack(ref) do
    case Ref.payload_files(ref) do
      nil -> {:error, :not_whole_payload}
      {_root, []} -> :empty
      {root, names} -> read_all(root, names)
    end
  end

  @doc """
  Write a packed artifact back, file by file, each through the same atomic
  install every other hydrated file uses.

  A name is only ever a basename — `pack/1` writes them that way — so anything
  else in a fetched blob is a corrupt or hostile entry and is refused rather
  than allowed to address a path outside the artifact's root.
  """
  @spec unpack(ref(), binary()) :: :ok | {:error, term()}
  def unpack(ref, bytes) do
    with {root, _names} <- Ref.payload_files(ref),
         {:ok, files} when is_map(files) <- decode_payload(bytes) do
      Enum.reduce_while(files, :ok, fn {name, content}, :ok ->
        if Path.basename(name) == name and name not in [".", ".."] do
          case atomic_write(Path.join(root, name), content) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
          end
        else
          {:halt, {:error, {:unsafe_artifact_name, name}}}
        end
      end)
    else
      nil -> {:error, :not_whole_payload}
      _ -> {:error, :malformed_payload}
    end
  end

  defp read_all(root, names) do
    Enum.reduce_while(names, {:ok, %{}}, fn name, {:ok, acc} ->
      case File.read(Path.join(root, name)) do
        {:ok, bytes} -> {:cont, {:ok, Map.put(acc, name, bytes)}}
        # Deleted between the listing and the read — it drops out of this cut
        # rather than failing it, exactly as a retention-deleted snapshot does.
        {:error, :enoent} -> {:cont, {:ok, acc}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, files} -> {:ok, :erlang.term_to_binary(files, compressed: 6)}
      {:error, _} = err -> err
    end
  end

  defp decode_payload(bytes) do
    {:ok, :erlang.binary_to_term(bytes, [:safe])}
  rescue
    _ -> :error
  end

  defp atomic_write(path, bytes),
    do: Paths.atomic_install(path, &:file.write(&1, bytes), :tier_artifact)
end
