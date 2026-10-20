defmodule Flezha.State.DurabilityTier.Hydrator do
  @moduledoc """
  Rebuilds local durable files from the remote manifest, so a controller with an
  empty state directory can adopt a relay that lived on a node whose disk
  is gone — and, at boot, can come back as itself rather than as a blank
  cluster.

  Recovery reads **only** the manifest. Blobs it does not name are invisible
  here whatever they are — a torn upload, a superseded range, a half-finished
  correction — which is what makes manifest-last atomicity a recovery property.

  Nothing is hydrated over local state: a reference with local durable files is
  left exactly as it is, so a partially-restored node can never lose a record
  that a stale remote copy would have overwritten. A newer cut another
  controller shipped replaces a local copy only by `Flezha.State.Adoption`
  setting that copy aside first (`shipped/3` says how new it is).

  Once the files land, the ordinary restore path runs unchanged — there is no
  second restore implementation.
  """

  require Logger

  alias Flezha.Dlq
  alias Flezha.State.DurabilityTier.{Artifact, Manifest}
  alias Flezha.State.DurabilityTier.Backend.Config, as: BackendConfig
  alias Flezha.State.{DurabilityTier, InputLog, Lineage, Paths, Ref, TaskJournal, WalWriter}

  @doc """
  Hydrate every shipped reference of `relay_class` whose local files are
  absent, or with `:own` only those this controller shipped. Runs before the
  runner starts, so a fresh controller binding a remote-only relay finds its
  Cells, task-journal, input log, effect log and parked failures on disk by
  the time the bind enumerates them.
  """
  @spec hydrate_relay(module(), keyword(), binary(), :all | :own) :: :ok
  def hydrate_relay(module, opts, relay_class, scope) when is_binary(relay_class) do
    select = scope(scope)
    hydrate_prefix(module, opts, Manifest.prefix(:cell, [relay_class]), select)

    # The input log is class-level, so it sits outside the per-Cell prefix
    # above and has to be asked for by name. Without it a hydrated Cell
    # comes back with its consumed floor intact and no log to replay from —
    # the next input lands at offset 0, below the floor, and is silently
    # skipped as already-consumed.
    hydrate_ref(module, opts, {:input_log, relay_class}, select)

    # The effect results kept for the inputs that log still holds, so a
    # hydrated instance's re-run is served them.
    hydrate_ref(module, opts, {:effects_log, relay_class}, select)

    # Parked failures belong to the relay that produced them, so they come
    # back with it rather than at boot: a replacement controller's `flz dlq`
    # sees the inherited parks from the moment it binds the relay.
    hydrate_prefix(
      module,
      opts,
      Manifest.prefix(:dlq, [Dlq.Store.path_component(relay_class)]),
      select
    )

    :ok
  end

  defp scope(:all), do: & &1
  defp scope(:own), do: &Enum.filter(&1, fn m -> m.owner == DurabilityTier.writer_id() end)

  @doc """
  The shipped cuts of `relay_class`'s Cells, task-journal and input log: how
  new each is (`Manifest.freshness/1`), `%{}` when this controller shipped every
  one and still holds a copy of the class, and the lineage each names.
  """
  @spec shipped(module(), keyword(), binary()) :: {%{Paths.durable_ref() => term()}, map()}
  def shipped(module, opts, relay_class) do
    log_key = Manifest.key({:input_log, relay_class})

    manifests =
      manifests(module, opts, Manifest.prefix(:cell, [relay_class])) ++
        manifests(module, opts, log_key, &(&1 == log_key))

    freshness =
      if Enum.all?(manifests, &(&1.owner == DurabilityTier.writer_id())) and
           Paths.durable_refs(relay_class) != [],
         do: %{},
         else: Map.new(manifests, &{&1.ref, Manifest.freshness(&1)})

    {freshness, Map.new(manifests, &{&1.ref, &1.lineage})}
  end

  @doc """
  Hydrate only the named cell keys of one Tasklet.

  The manifest is per-Cell, so a narrowed request costs exactly the artifacts
  it names — no full-Tasklet fetch to answer a question about one key.
  """
  @spec hydrate_cells(module(), keyword(), binary(), binary(), [binary()]) :: :ok
  def hydrate_cells(module, opts, relay_class, tasklet_id, cell_keys) do
    Enum.each(cell_keys, fn cell_key ->
      ref = {:cell, {relay_class, tasklet_id, cell_key}}
      hydrate_ref(module, opts, ref)
    end)
  end

  @doc """
  Hydrate every shipped registry cspec whose local file is absent — the record
  of which relays exist and how they are configured.

  Must complete before any hub starts: ProcessHub opens `registry.dets` at hub
  start, so a cspec that lands afterwards is invisible to the orphan reconcile
  and the relays it names are never started.
  """
  @spec hydrate_cspecs() :: :ok
  def hydrate_cspecs(), do: hydrate_replicas(Manifest.prefix(:cspec))

  @doc """
  Hydrate every shipped control-plane store whose local files are absent — the
  authz registries and the schedule definitions.

  Same replica shape as the cspec: node-local stores with no cross-node
  replication, so each controller ships its own copy and recovery takes the
  newest.
  """
  @spec hydrate_control_stores() :: :ok
  def hydrate_control_stores(), do: hydrate_replicas(Manifest.prefix(:control))

  # Every controller ships its own replica of one node-local store, and they all
  # restore to the same local path — so the choice has to be made here rather
  # than left to listing order. The replicas are equivalent by design; newest is
  # the tie-break that behaves best when they are not yet converged.
  #
  # `shipped_at_ms` is the writing node's wall clock, so skew could pick a
  # slightly older replica. That is acceptable and not merely tolerated: the
  # orphan reconcile heals the difference, and there is no cluster-wide
  # monotonic source at this layer to do better.
  defp hydrate_replicas(prefix) do
    with {module, opts} <- BackendConfig.resolve() do
      hydrate_prefix(module, opts, prefix, &newest_replicas/1)
    end

    :ok
  end

  defp newest_replicas(manifests) do
    manifests
    |> Enum.group_by(&replica_identity(&1.ref))
    |> Enum.map(fn {_identity, replicas} ->
      Enum.max_by(replicas, &{&1.shipped_at_ms, writer_of(&1.ref)})
    end)
  end

  # What the replicas of one store are replicas OF: the writer is what separates
  # them, so it is the one part of the reference the grouping drops.
  defp replica_identity({kind, {id, _writer}}), do: {kind, id}
  defp writer_of({_kind, {_id, writer}}), do: writer

  @doc """
  One-shot supervision child that hydrates everything a controller needs before
  it starts serving: the control-plane stores, then the registry cspec.

  Placed ahead of `Flezha.Service.HubManager` in the application tree, where
  returning `:ignore` from `start_link` is what makes the ordering a guarantee
  rather than a race. Security state goes first: a resurrected controller must
  enforce from its first accepted connection, and the one-shot bootstrap claim
  must read already-claimed rather than being re-runnable.
  """
  @spec hydrate_on_boot() :: :ignore
  def hydrate_on_boot() do
    hydrate_control_stores()
    hydrate_cspecs()
    :ignore
  rescue
    error ->
      Logger.error("[durability] boot hydration failed: #{inspect(error)}")
      :ignore
  end

  # ---------- one reference ----------

  # One walk over a manifest-key prefix. `select` narrows the entries found to
  # the authoritative ones — identity for a prefix whose entries are all
  # distinct references, newest-per-store for replicas, this node's own (`scope/1`).
  defp hydrate_prefix(module, opts, prefix, select) do
    module
    |> manifests(opts, prefix)
    |> select.()
    |> Enum.each(&apply_manifest(module, opts, &1))
  end

  defp manifests(module, opts, prefix, keep? \\ fn _key -> true end) do
    case module.list(prefix, opts) do
      {:ok, entries} ->
        for {key, _age} <- entries,
            keep?.(key),
            {:ok, manifest} <- [read_manifest(module, opts, key)],
            do: manifest

      _unreachable ->
        []
    end
  end

  defp read_manifest(module, opts, manifest_key) do
    with {:ok, blob} <- module.get(manifest_key, opts),
         {:ok, manifest} <- Manifest.decode(blob) do
      {:ok, manifest}
    else
      _ ->
        Logger.warning("[durability] unreadable manifest #{manifest_key}; skipped")
        :error
    end
  end

  # `apply_manifest/3` is where local-state precedence is decided, so this only
  # names the manifest to fetch.
  defp hydrate_ref(module, opts, ref, select \\ & &1) do
    case read_manifest(module, opts, Manifest.key(ref)) do
      {:ok, manifest} -> Enum.each(select.([manifest]), &apply_manifest(module, opts, &1))
      :error -> :ok
    end
  end

  defp apply_manifest(module, opts, %Manifest{ref: ref} = manifest) do
    if Ref.has_local_state?(ref) do
      :ok
    else
      case land(module, opts, manifest) do
        :ok ->
          seed_floor(manifest)
          emit(:hydrated, ref, manifest)

        # A referenced artifact the store does not hold is a CORRUPT LINEAGE, not
        # a transport problem, and the two call for opposite user responses:
        # one waits for the backend, the other must be acted on while some node
        # still has a local copy to re-ship from. Degrading it into a silent cold
        # start is what made this class of damage discoverable only on recovery
        # day.
        {:error, {:missing_artifact, artifact}} ->
          Logger.error(
            "[durability] #{inspect(ref)} references #{artifact}, which the backend does not " <>
              "hold. Its remote lineage is incomplete and cannot be recovered from as it stands."
          )

          :telemetry.execute(
            [:flezha, :state, :artifact_missing],
            %{count: 1},
            %{ref: inspect(ref), artifact: artifact}
          )

        {:error, reason} ->
          Logger.error("[durability] hydration failed for #{inspect(ref)}: #{inspect(reason)}")

          :telemetry.execute(
            [:flezha, :state, :hydration_failed],
            %{count: 1},
            %{ref: inspect(ref), reason: inspect(reason)}
          )
      end
    end
  end

  defp fetch_and_write(module, opts, %Manifest{file: %{blob: blob}} = manifest) do
    with {:ok, bytes} <- fetch_artifact(module, opts, blob) do
      Artifact.unpack(manifest.ref, bytes)
    end
  end

  defp fetch_and_write(module, opts, %Manifest{} = manifest) do
    with :ok <- write_snapshots(module, opts, manifest) do
      write_ranges(module, opts, manifest)
    end
  end

  defp write_snapshots(module, opts, manifest) do
    Enum.reduce_while(manifest.snapshots, :ok, fn %{seq: seq, blob: blob}, :ok ->
      with {:ok, bytes} <- fetch_artifact(module, opts, blob),
           :ok <- Artifact.write_snapshot(manifest.ref, seq, bytes) do
        {:cont, :ok}
      else
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  # Ranges are disjoint and ordered by construction, so concatenating them in
  # `from` order reproduces the WAL framing byte for byte — the same bytes the
  # local writer would have left behind.
  defp write_ranges(module, opts, manifest) do
    manifest.ranges
    |> Enum.sort_by(& &1.from)
    |> Enum.reduce_while({:ok, []}, fn %{blob: blob}, {:ok, acc} ->
      case fetch_artifact(module, opts, blob) do
        {:ok, bytes} -> {:cont, {:ok, [bytes | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, chunks} ->
        Artifact.write_wal(manifest.ref, chunks |> Enum.reverse() |> IO.iodata_to_binary())

      {:error, _} = err ->
        err
    end
  end

  # The manifest is the only thing that names an artifact, so "the store does not
  # have it" means the entry is wrong — a distinct verdict from every other way a
  # fetch can fail.
  defp fetch_artifact(module, opts, blob) do
    case module.get(blob, opts) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, :not_found} -> {:error, {:missing_artifact, blob}}
      {:error, _} = err -> err
    end
  end

  # A Cell's small files no record implies land after its data (`Lineage.land/3`);
  # each overwrites a fixed path, so a retry after a partial landing converges.
  defp land(module, opts, %Manifest{ref: {:cell, cell_id}} = manifest),
    do: Lineage.land(cell_id, facts(manifest), fn -> fetch_and_write(module, opts, manifest) end)

  defp land(module, opts, manifest), do: fetch_and_write(module, opts, manifest)

  # A compacted Cell ships a snapshot and no effect record, so the shipped
  # consumed offset is its folded offset. A controller re-landing its own cut
  # takes no copy over and mints nothing.
  defp facts(%Manifest{} = manifest) do
    Map.merge(manifest.lineage || %{}, %{
      epoch: manifest.epoch,
      folded_offset: Map.get(manifest.source_offsets, :consumed_offset, -1),
      landed: manifest.owner != DurabilityTier.writer_id(),
      markers: manifest.markers
    })
  end

  # The in-memory floors a bind reads: the input log's for a Cell, and a
  # task-journal's consumed input, or the bind would re-deliver every retained
  # input the instance had consumed.
  defp seed_floor(%Manifest{ref: {:cell, {class, _, _} = cell_id}}) do
    with offset when is_integer(offset) and offset >= 0 <-
           WalWriter.consumed_offset_on_disk(cell_id),
         do: InputLog.record_consumed(class, cell_id, offset)

    if cell_id == Paths.journal_id(class), do: TaskJournal.release_input(class), else: :ok
  end

  defp seed_floor(_manifest), do: :ok

  defp emit(event, ref, manifest) do
    :telemetry.execute(
      [:flezha, :state, event],
      %{count: 1, ranges: length(manifest.ranges), snapshots: length(manifest.snapshots)},
      %{ref: inspect(ref)}
    )
  end
end
