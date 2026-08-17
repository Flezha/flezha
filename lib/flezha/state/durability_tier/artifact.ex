defmodule Flezha.State.DurabilityTier.Artifact do
  @moduledoc """
  The local side of the remote durability tier: what a shippable reference maps
  onto on disk, how a seq range is read out of it, and how a fetched artifact is
  written back.

  Three reference shapes cover every durable byte the tier owns, and two of them
  are the same substrate:

    * `{:entity, entity_id}` — an Entity's WAL plus its snapshots. A workflow
      instance's proc-journal and durable inbox are *also* this shape: they ride
      the Entity persistence substrate under `Flezha.State.Registry`'s reserved
      entity keys, so they need no separate machinery here.
    * `{:input_log, operative_class}` — the effectively-once tier's durable
      input log, which shares the WAL record framing and so shares the
      seq-range reader.
    * `{:cspec, {hub_id, writer}}` — the ProcessHub durable registry, a whole
      file. Every node in a namespace holds its own replica, so the writer is
      part of the identity rather than something to arbitrate.

  Record bundles are read **at ship time** through `WalWriter.fold_records/3`
  and re-framed with `WalWriter.encode_record/3`, never copied as file byte
  ranges: compaction (`rewrite_keep_seq_gt`) and correction (`rewind_to`)
  rewrite the live WAL in place, so an offset recorded yesterday addresses
  different records today. A seq range does not move.
  """

  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{Outbox, Registry, Restore, WalWriter, Wire}

  @type ref :: Manifest.ref()

  @doc """
  Every Entity reference of `operative_class` that has local durable files —
  Entities, and (for a workflow instance) its reserved proc-journal and inbox
  identities, which `Registry.all_entities/0` does not surface.
  """
  @spec entity_refs(binary()) :: [ref()]
  def entity_refs(operative_class) do
    reserved =
      Enum.map(Registry.reserved_entity_keys(), &{operative_class, operative_class, &1})

    (Registry.entities_of(operative_class) ++ reserved)
    |> Enum.uniq()
    |> Enum.filter(&has_local_state?({:entity, &1}))
    |> Enum.map(&{:entity, &1})
  end

  @doc "Local WAL path of `ref`, or `nil` for a whole-file reference."
  @spec wal_path(ref()) :: binary() | nil
  def wal_path({:entity, entity_id}), do: Registry.wal_path(entity_id)
  def wal_path({:input_log, operative_class}), do: Registry.input_log_path(operative_class)
  def wal_path({:cspec, _}), do: nil

  @doc "Local whole-file path of `ref`, or `nil` for a seq-addressed reference."
  @spec file_path(ref()) :: binary() | nil
  def file_path({:cspec, {hub_id, _writer}}),
    do: Path.join([cspec_root(), hub_id, "registry.dets"])

  def file_path(_ref), do: nil

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
        entries |> Enum.map(&{:cspec, {&1, writer}}) |> Enum.filter(&has_local_state?/1)

      {:error, _} ->
        []
    end
  end

  @doc """
  The on-disk state key `ref` lives under — the directory name that
  `DurabilityTier.refs_of_state_key/1` enumerates from, and the unit an operative is
  adopted as.
  """
  @spec state_key(ref()) :: binary()
  def state_key({:entity, {class, _procedure, _entity_key}}), do: class
  def state_key({:input_log, class}), do: class
  def state_key({:cspec, {hub_id, _writer}}), do: hub_id

  defp cspec_root(),
    do: Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")

  @doc "Local snapshot files of `ref`, oldest first. Only Entities have them."
  @spec snapshots(ref()) :: [{non_neg_integer(), binary()}]
  def snapshots({:entity, entity_id}), do: Registry.snapshot_files(entity_id)
  def snapshots(_ref), do: []

  @doc """
  The WAL-owner epoch a manifest write is conditioned on.

  This is the existing local fencing epoch (`Flezha.State.Outbox`), read without
  a live writer so a ship never depends on one. References with no dedup file —
  the reserved journal/inbox identities, the input log, the cspec — report `0`;
  their remote lineage is then ordered by write order rather than fenced, which
  is exactly the reach the local epoch machinery already has.
  """
  @spec epoch(ref()) :: non_neg_integer()
  def epoch({:entity, entity_id}), do: Outbox.read(entity_id).epoch
  def epoch(_ref), do: 0

  @doc """
  `ref`'s saga compensation markers — which of its undo hooks have been
  intended and which have already run.

  They are presence-only files that no state change implies, and the resumed
  workflow driver reads them off `RestoreInitState` to decide whether to
  compensate or resume forward. A recovery that restored the data without them
  resumes forward past a rollback it owes, so they travel on the manifest
  rather than as blobs.
  """
  @spec markers(ref()) :: Manifest.markers()
  def markers({:entity, entity_id}), do: Registry.compensation_markers(entity_id)
  def markers(_ref), do: %{intent: false, done: false}

  @doc """
  Whether `ref` has anything durable on disk right now. For an Entity this is
  `Restore.has_persisted_state?/1` — the same predicate the bind-time restore
  filters on, so the tier and the restore path can never disagree about which
  Entities exist.
  """
  @spec has_local_state?(ref()) :: boolean()
  def has_local_state?({:entity, entity_id}), do: Restore.has_persisted_state?(entity_id)
  def has_local_state?(ref), do: non_empty?(wal_path(ref)) or non_empty?(file_path(ref))

  defp non_empty?(nil), do: false

  defp non_empty?(path) do
    match?({:ok, %{size: size}} when size > 0, File.stat(path))
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
  bundle itself. That is what lets a hydrated Entity resume replay at the cut
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
             {[WalWriter.encode_record(seq, header, body) | acc], count + 1,
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

  # `fold_path/3` owns the open-fold-close and the torn-tail contract; a
  # whole-file reference has no records at all.
  defp fold_wal(ref, acc, fun) do
    case wal_path(ref) do
      nil -> acc
      path -> WalWriter.fold_path(path, acc, fun)
    end
  end

  # ---------- hydration writes ----------

  @doc """
  Write fetched WAL bytes as `ref`'s local WAL, and fetched snapshot bytes as
  its snapshot files.

  Written through `Registry.atomic_install/2` — the same `.transferring` →
  fsync → rename the cross-controller pull uses, so a failure leaves the
  destination cold rather than half-restored.
  """
  @spec write_wal(ref(), binary()) :: :ok | {:error, term()}
  def write_wal(_ref, <<>>), do: :ok

  def write_wal(ref, bytes) do
    case wal_path(ref) do
      nil -> {:error, :not_seq_addressed}
      path -> atomic_write(path, bytes)
    end
  end

  @spec write_snapshot(ref(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def write_snapshot({:entity, entity_id}, seq, bytes),
    do: atomic_write(Registry.snapshot_path(entity_id, seq), bytes)

  def write_snapshot(_ref, _seq, _bytes), do: {:error, :not_snapshotted}

  @spec write_file(ref(), binary()) :: :ok | {:error, term()}
  def write_file(ref, bytes) do
    case file_path(ref) do
      nil -> {:error, :not_whole_file}
      path -> atomic_write(path, bytes)
    end
  end

  defp atomic_write(path, bytes),
    do: Registry.atomic_install(path, &:file.write(&1, bytes))
end
