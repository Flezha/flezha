defmodule Flezha.State.DurabilityTier.Manifest do
  @moduledoc """
  What a shipped artifact set *is*, and the naming that addresses it.

  A manifest entry describes one shippable unit — an Entity (its WAL and
  snapshots), a workflow instance's proc-journal or durable inbox (which ride
  the same substrate under reserved entity keys), an operative's input log, its
  parked failures, the ProcessHub registry cspec, or a control-plane store — as
  of one shipped cut. It is written **last**
  in every ship, so a crash mid-upload leaves the previous entry authoritative
  and the half-uploaded blobs invisible (`docs/operations/remote_durability_tier.md`).

  The algebra here is pure and is the whole recovery contract:

    * `put_snapshots/2` mirrors the local snapshot set — a snapshot that
      retention deleted locally stops being referenced remotely.
    * `prune_above/2` is the correction case: `WalWriter.rewind_to/3` dropped
      the local records above a seq, so the ranges covering them stop being
      truth. The next ship re-covers whatever remains.
    * `prune_below/2` is the compaction case: a snapshot subsumes every range
      fully below the WAL's new floor.
    * `put_range/2` appends the cut just shipped.

  Ranges are half-open on the low side (`from` is the first seq *in* the
  bundle) and stay disjoint and ordered, so hydration concatenates them into a
  WAL file verbatim.
  """

  alias Flezha.State.Registry

  @magic "FLZM"
  @version 1

  # What makes two entries the same shipped cut. Everything the manifest asserts
  # about the Entity, and nothing about when it was asserted.
  #
  # `owner`/`claim` are here, not merely alongside: a takeover changes no data,
  # so without them `same_cut?/2` would report "nothing to write" and the new
  # owner's claim would never land.
  @cut_keys [:epoch, :snapshots, :ranges, :file, :source_offsets, :markers, :owner, :claim]

  @type ref ::
          {:entity, Registry.entity_id()}
          | {:input_log, binary()}
          | {:cspec, {binary(), binary()}}
          | {:dlq, {binary(), binary()}}
          | {:control, {binary(), binary()}}

  @type range :: %{
          from: non_neg_integer(),
          to: non_neg_integer(),
          epoch: non_neg_integer(),
          blob: binary()
        }
  @type snapshot :: %{seq: non_neg_integer(), blob: binary()}
  @type markers :: %{intent: boolean(), done: boolean()}

  @type t :: %__MODULE__{
          version: pos_integer(),
          ref: ref(),
          epoch: non_neg_integer(),
          snapshots: [snapshot()],
          ranges: [range()],
          file: nil | %{blob: binary(), size: non_neg_integer(), sha: binary()},
          source_offsets: %{optional(atom()) => integer()},
          markers: markers(),
          owner: nil | binary(),
          claim: non_neg_integer(),
          shipped_at_ms: integer()
        }

  defstruct version: @version,
            ref: nil,
            epoch: 0,
            snapshots: [],
            ranges: [],
            file: nil,
            source_offsets: %{},
            markers: %{intent: false, done: false},
            owner: nil,
            claim: 0,
            shipped_at_ms: 0

  @spec new(ref()) :: t()
  def new(ref), do: %__MODULE__{ref: ref}

  # ---------- codec ----------

  @doc """
  Encode for the backend. Term format rather than a hand-rolled codec: both
  ends of this blob are the same controller build, and the whole point of the
  manifest-last write is that a decode failure is never a partial read.
  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = manifest) do
    body = :erlang.term_to_binary(Map.from_struct(manifest), compressed: 6)
    <<@magic, @version::32-big, body::binary>>
  end

  @doc """
  Decode a manifest blob. `:error` for anything this build does not recognise —
  a caller treats that exactly like "no manifest", which is the safe direction:
  it hydrates nothing and ships a fresh entry.

  A field this build knows and the stored entry lacks takes its struct default,
  so a manifest written before a field existed decodes as "that fact was never
  asserted" rather than failing.
  """
  @spec decode(binary()) :: {:ok, t()} | :error
  def decode(<<@magic, @version::32-big, body::binary>>) do
    fields = :erlang.binary_to_term(body, [:safe])
    {:ok, struct(__MODULE__, fields)}
  rescue
    _ -> :error
  end

  def decode(_other), do: :error

  # ---------- algebra ----------

  @doc "Highest seq this entry covers, or `-1` when it covers nothing."
  @spec shipped_through(t()) :: integer()
  def shipped_through(%__MODULE__{snapshots: snaps, ranges: ranges}) do
    Enum.reduce(snaps, -1, &max(&1.seq, &2))
    |> then(fn acc -> Enum.reduce(ranges, acc, &max(&1.to, &2)) end)
  end

  @doc """
  Drop everything above `seq` — the correction case. A range straddling `seq`
  goes entirely: the records at or below it are still local, so the next ship
  re-covers them, whereas keeping a bundle that also holds rewound records
  would resurrect them on hydration.
  """
  @spec prune_above(t(), integer()) :: t()
  def prune_above(%__MODULE__{} = manifest, seq) do
    %{
      manifest
      | snapshots: Enum.reject(manifest.snapshots, &(&1.seq > seq)),
        ranges: Enum.reject(manifest.ranges, &(&1.to > seq))
    }
  end

  @doc """
  Drop every range reaching at or below `floor` — the compaction case, where
  the local WAL was truncated and a snapshot at or above `floor` covers them.

  A range *straddling* the floor goes too, rather than being kept for its tail:
  keeping it would pin the pre-compaction records in the live set forever, so
  the manifest could only grow. The tail is still local, so the same ship
  re-covers it from the truncated WAL. Called only with a floor a retained
  snapshot actually covers, so a range is never dropped without a baseline
  standing in for it.
  """
  @spec prune_below(t(), integer()) :: t()
  def prune_below(%__MODULE__{} = manifest, floor) do
    %{manifest | ranges: Enum.reject(manifest.ranges, &(&1.from <= floor))}
  end

  @doc "Mirror the local snapshot set: `seqs` is what is on disk right now."
  @spec put_snapshots(t(), [{non_neg_integer(), binary()}]) :: t()
  def put_snapshots(%__MODULE__{} = manifest, seq_blobs) do
    snapshots =
      seq_blobs
      |> Enum.map(fn {seq, blob} -> %{seq: seq, blob: blob} end)
      |> Enum.sort_by(& &1.seq)

    %{manifest | snapshots: snapshots}
  end

  @spec put_range(t(), range()) :: t()
  def put_range(%__MODULE__{} = manifest, range) do
    ranges =
      (manifest.ranges ++ [range])
      |> Enum.uniq_by(&{&1.from, &1.to})
      |> Enum.sort_by(& &1.from)

    %{manifest | ranges: ranges}
  end

  @doc """
  Whether two entries describe the same shipped cut. `shipped_at_ms` is
  deliberately ignored: a re-ship that found nothing new must not rewrite the
  manifest, or a quiet entity would churn the backend once per sweep forever.

  `markers` IS compared, and has to be: a compensation marker appears after the
  state change it belongs to, so an entry that is otherwise identical must still
  be rewritten or the marker never ships.
  """
  @spec same_cut?(t(), t()) :: boolean()
  def same_cut?(%__MODULE__{} = a, %__MODULE__{} = b) do
    Map.take(a, @cut_keys) == Map.take(b, @cut_keys)
  end

  @doc """
  The conditional-write guard `writer` must present to update this entry.

  The owner extends its own lineage at the claim it already holds; anyone else
  bids one above it. The bid is a strict compare at the backend, which is what
  makes it a compare-and-set: two nodes bidding the same number cannot both
  win, and "whoever wrote last" — the way the cspec used to behave — stops
  being reachable.

  This replaces the WAL epoch as the guard. The epoch is copied with the state
  it protects (`WalTransfer` carries the dedup file, `Hydrator` restores it
  verbatim), so it can order one writer's own writes but can never tell two
  writers apart. It stays in the entry as the supersession fact `prune_above/2`
  reads; it is no longer the gate.
  """
  @spec guard_for(t(), binary()) :: {:extend, non_neg_integer()} | {:claim, non_neg_integer()}
  def guard_for(%__MODULE__{owner: owner, claim: claim}, writer) when owner == writer,
    do: {:extend, claim}

  def guard_for(%__MODULE__{claim: claim}, _writer), do: {:claim, claim + 1}

  @doc """
  Stamp `writer`'s ownership onto the cut being shipped, and report the guard its
  write must carry.

  One call rather than a `guard_for/2` and a separate stamp: the two must agree
  about which writer they are for, and taking the writer once is what makes
  disagreeing impossible.
  """
  @spec claim_as(t(), t(), binary()) :: {t(), {:extend | :claim, non_neg_integer()}}
  def claim_as(%__MODULE__{} = shipped, %__MODULE__{} = previous, writer) do
    {_mode, claim} = guard = guard_for(previous, writer)
    {%{shipped | owner: writer, claim: claim}, guard}
  end

  @doc "Every blob key this entry references — the GC's live set."
  @spec blob_keys(t()) :: [binary()]
  def blob_keys(%__MODULE__{} = manifest) do
    Enum.map(manifest.snapshots, & &1.blob) ++
      Enum.map(manifest.ranges, & &1.blob) ++
      case manifest.file do
        nil -> []
        %{blob: blob} -> [blob]
      end
  end

  # ---------- keys ----------

  # Every artifact class, as `tag => {kind, identity segments}`. The tag is the
  # first path segment of every key the class owns; the count is how many
  # segments its identity takes, so `path_of/1` and `ref_of_blob_key/1` are two
  # readings of one table rather than hand-written inverses that can drift. A
  # new artifact class is a row here plus an `Artifact.payload_files/1` clause.
  #
  # Which classes carry a writer in their identity is the ownership model in
  # one line: the cspec and the control-plane stores are per-writer replicas of
  # something no node replicates for another, while an Entity's state and an
  # operative's parked failures belong to whichever node runs it and are
  # arbitrated by the claim counter instead.
  @classes %{
    "e" => {:entity, 3},
    "i" => {:input_log, 1},
    "c" => {:cspec, 2},
    "d" => {:dlq, 2},
    "p" => {:control, 2}
  }

  @tags Map.new(@classes, fn {tag, {kind, _segments}} -> {kind, tag} end)

  @doc "Storage key of `ref`'s manifest entry."
  @spec key(ref()) :: binary()
  def key(ref), do: "manifest/" <> path_of(ref)

  @doc "Storage key of one blob belonging to `ref`."
  @spec blob_key(ref(), binary()) :: binary()
  def blob_key(ref, name), do: "blob/" <> path_of(ref) <> "/" <> name

  @doc """
  The reference a blob key belongs to — the declared inverse of `blob_key/2`,
  and what lets collection ask "is this mine?" without a second naming scheme.

  `:error` for anything this build did not write. A caller treats that as "not
  mine", which is the safe direction: an unrecognised key is never collected on
  the scoping rule, only ever on age.
  """
  @spec ref_of_blob_key(binary()) :: {:ok, ref()} | :error
  def ref_of_blob_key("blob/" <> path) do
    # The blob name is the last segment and no segment can contain "/" —
    # `Registry.encode_entity_key/1` emits url-base64, which has none.
    [tag | rest] = String.split(path, "/")

    with {:ok, {kind, segments}} <- Map.fetch(@classes, tag),
         ^segments <- length(rest) - 1 do
      {:ok, {kind, rest |> Enum.take(segments) |> Enum.map(&unsegment/1) |> identity()}}
    else
      _ -> :error
    end
  end

  def ref_of_blob_key(_other), do: :error

  @doc """
  Manifest-key prefix covering every artifact of `kind`, narrowed by the leading
  identity segments in `parts`.

  `prefix(:entity, [class])` is every Entity of one operative class;
  `prefix(:cspec)` is every registry replica there is.
  """
  @spec prefix(atom(), [binary()]) :: binary()
  def prefix(kind, parts \\ []),
    do: "manifest/" <> Enum.join([tag!(kind) | Enum.map(parts, &segment/1)], "/") <> "/"

  @doc "Key prefix under which every blob lives — the GC's candidate set."
  @spec blob_prefix() :: binary()
  def blob_prefix(), do: "blob/"

  @doc """
  Name of a record-bundle blob covering `(from..to)` written at `epoch`.

  The content hash is part of the name, and that is load-bearing rather than
  cosmetic: position alone (`epoch`, `from`, `to`) is derived from the artifact's
  identity and says nothing about who wrote it, so two controllers holding one
  reference — which an ordinary migration produces, since the source keeps its
  copy — would address the same key with different records and silently
  overwrite each other. Different bytes now mean a different key. Identical
  bytes still mean the same key, so a re-ship stays idempotent.
  """
  @spec range_name(non_neg_integer(), non_neg_integer(), non_neg_integer(), binary()) :: binary()
  def range_name(epoch, from, to, sha_hex), do: "r-#{epoch}-#{from}-#{to}-#{sha_hex}"

  @doc "Name of a snapshot blob at `seq`, content-addressed for the same reason."
  @spec snapshot_name(non_neg_integer(), binary()) :: binary()
  def snapshot_name(seq, sha_hex), do: "s-#{seq}-#{sha_hex}"

  @doc "Name of a whole-payload blob, content-addressed so a re-ship is idempotent."
  @spec file_name(binary()) :: binary()
  def file_name(sha_hex), do: "f-" <> sha_hex

  defp path_of({kind, identity}),
    do: Enum.join([tag!(kind) | Enum.map(parts_of(identity), &segment/1)], "/")

  defp tag!(kind), do: Map.fetch!(@tags, kind)

  # An identity is a tuple of segments, or a bare binary where one segment says
  # everything — `parts_of/1` and `identity/1` are the two directions of that.
  defp parts_of(identity) when is_tuple(identity), do: Tuple.to_list(identity)
  defp parts_of(identity) when is_binary(identity), do: [identity]

  defp identity([one]), do: one
  defp identity(parts), do: List.to_tuple(parts)

  # Reuses the Entity-key filesystem encoding: the same "safe chars or
  # url-base64" rule that keeps a key legal as a filename keeps it legal as a
  # storage key segment, so one identity maps to one name on both sides.
  defp segment(part), do: Registry.encode_entity_key(part)

  defp unsegment(part), do: Registry.decode_entity_key(part)
end
