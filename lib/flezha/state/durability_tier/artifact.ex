defmodule Flezha.State.DurabilityTier.Artifact do
  @moduledoc """
  The local side of the remote durability tier: what a shippable reference maps
  onto on disk, how a seq range is read out of it, how a fetched one is written
  back.

  **Seq-addressed** references are read as a range of WAL records —
  `{:entity, entity_id}` (an Entity's WAL and snapshots; a workflow instance's
  proc-journal and inbox are this shape too, under `Paths`'s reserved entity
  keys) and `{:input_log, operative_class}`. **Whole-payload** ones are the
  files they name, packed by `pack/1`: `{:cspec, {hub_id, writer}}`, `{:dlq,
  {operative_class, procedure_id}}` and `{:control, {store, writer}}`, each
  keyed by its writer or claim-fenced because their replicas are per-node.

  Record bundles are read at ship time and re-framed, never copied as file byte
  ranges: compaction and correction rewrite the live WAL in place, so a byte
  offset moves where a seq range does not.
  """

  alias Flezha.AuthZ.Config
  alias Flezha.Dlq
  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{Outbox, Paths, RecordFrame, Restore, Wire}

  @type ref :: Manifest.ref()

  @whole_payload_kinds [:cspec, :dlq, :control]

  # The control-plane stores that ship, as `store => files`. Each is a
  # node-local DETS registry a replacement controller needs in order to come
  # back as itself rather than as a blank cluster.
  #
  # The file list is explicit rather than a directory scan, and that is the
  # security boundary: `bootstrap.token` lives beside these and is a plaintext
  # one-shot secret that must never leave the node. A resurrected controller
  # reads "already claimed" out of `state.dets`, so it never needs the token
  # back.
  @control_stores %{
    "authz" => ~w(state.dets roles.dets namespaces.dets tokens.dets quota_counters.dets),
    "schedules" => ~w(schedules.dets)
  }

  @doc """
  Every Entity reference of `operative_class` that has local durable files —
  Entities, and (for a workflow instance) its reserved proc-journal and inbox
  identities, which `Paths.all_entities/0` does not surface.
  """
  @spec entity_refs(binary()) :: [ref()]
  def entity_refs(operative_class) do
    reserved =
      Enum.map(Paths.reserved_entity_keys(), &{operative_class, operative_class, &1})

    (Paths.entities_of(operative_class) ++ reserved)
    |> Enum.uniq()
    |> Enum.filter(&has_local_state?({:entity, &1}))
    |> Enum.map(&{:entity, &1})
  end

  @doc "Local WAL path of `ref`, or `nil` for a whole-file reference."
  @spec wal_path(ref()) :: binary() | nil
  def wal_path({:entity, entity_id}), do: Paths.wal_path(entity_id)
  def wal_path({:input_log, operative_class}), do: Paths.input_log_path(operative_class)
  def wal_path(_ref), do: nil

  @doc """
  Whether `ref` is packed whole rather than read as a seq range — the one place
  the two artifact families are told apart.
  """
  @spec whole_payload?(ref()) :: boolean()
  def whole_payload?(ref) when is_tuple(ref), do: elem(ref, 0) in @whole_payload_kinds

  @doc """
  The files making up a whole-payload artifact as `{root, names}` — only the
  names that exist and hold bytes right now — or `nil` for a seq-addressed one.

  This is the only per-artifact knowledge in the packing path: adding a
  whole-payload artifact is a clause here, not a mechanism.
  """
  @spec payload_files(ref()) :: {binary(), [binary()]} | nil
  def payload_files({:cspec, {hub_id, _writer}}),
    do: present(Path.join(cspec_root(), hub_id), ["registry.dets"])

  def payload_files({:dlq, {operative_class, procedure_id}}) do
    root = Dlq.Store.proc_dir(operative_class, procedure_id)

    case File.ls(root) do
      {:ok, names} -> present(root, names)
      {:error, _} -> {root, []}
    end
  end

  def payload_files({:control, {store, _writer}}),
    do: present(control_root(store), Map.fetch!(@control_stores, store))

  def payload_files(_ref), do: nil

  defp control_root("authz"), do: Config.auth_dir()
  defp control_root("schedules"), do: Flezha.Schedule.Store.dir()

  defp present(root, names) do
    {root, Enum.filter(names, &non_empty?(Path.join(root, &1)))}
  end

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
  Every parked-failure set of `operative_class` with a directory on this node.

  Deliberately not filtered by `has_local_state?/1`: an emptied set has to stay
  in the sweep so its remote entry stops naming a blob whose entries the operator
  has since resolved.
  """
  @spec dlq_refs(binary()) :: [ref()]
  def dlq_refs(operative_class) when is_binary(operative_class),
    do: operative_class |> Dlq.Store.procedures() |> Enum.map(&{:dlq, &1})

  @doc """
  Every operative class with parked failures on this node.

  Unioned with the classes that have Entity state, because the two sets differ:
  a stopped operative keeps its parked failures, and losing them on a disk death
  is exactly what shipping them prevents.
  """
  @spec dlq_classes() :: [binary()]
  def dlq_classes(),
    do: Dlq.Store.procedures() |> Enum.map(fn {class, _procedure} -> class end) |> Enum.uniq()

  @doc """
  Every control-plane store with local files, stamped with `writer`.

  Like the cspec these are per-writer replicas rather than one arbitrated
  lineage: each controller's copy is its own truth, and hydration takes the
  newest.
  """
  @spec control_refs(binary()) :: [ref()]
  def control_refs(writer) when is_binary(writer) do
    @control_stores
    |> Map.keys()
    |> Enum.map(&{:control, {&1, writer}})
    |> Enum.filter(&has_local_state?/1)
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
  def state_key({:dlq, {class, _procedure}}), do: class
  def state_key({:control, {store, _writer}}), do: store

  defp cspec_root(),
    do: Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")

  @doc "Local snapshot files of `ref`, oldest first. Only Entities have them."
  @spec snapshots(ref()) :: [{non_neg_integer(), binary()}]
  def snapshots({:entity, entity_id}), do: Paths.snapshot_files(entity_id)
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
  def markers({:entity, entity_id}), do: Paths.compensation_markers(entity_id)
  def markers(_ref), do: %{intent: false, done: false}

  @doc """
  Whether `ref` has anything durable on disk right now. For an Entity this is
  `Restore.has_persisted_state?/1` — the same predicate the bind-time restore
  filters on, so the tier and the restore path can never disagree about which
  Entities exist.
  """
  @spec has_local_state?(ref()) :: boolean()
  def has_local_state?({:entity, entity_id}), do: Restore.has_persisted_state?(entity_id)

  def has_local_state?(ref) do
    case payload_files(ref) do
      nil -> non_empty?(wal_path(ref))
      {_root, names} -> names != []
    end
  end

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
             {[RecordFrame.encode_record(seq, header, body) | acc], count + 1,
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
      path -> RecordFrame.fold_path(path, acc, fun)
    end
  end

  # ---------- hydration writes ----------

  @doc """
  Write fetched WAL bytes as `ref`'s local WAL, and fetched snapshot bytes as
  its snapshot files.

  Written through `Paths.atomic_install/2` — the same `.transferring` →
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
    do: atomic_write(Paths.snapshot_path(entity_id, seq), bytes)

  def write_snapshot(_ref, _seq, _bytes), do: {:error, :not_snapshotted}

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
    case payload_files(ref) do
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
    with {root, _names} <- payload_files(ref),
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
    do: Paths.atomic_install(path, &:file.write(&1, bytes))
end
