defmodule Flezha.State.DurabilityTier.Hydrator do
  @moduledoc """
  Rebuilds local durable files from the remote manifest, so a controller with an
  empty state directory can adopt an operative that lived on a node whose disk
  is gone.

  Recovery reads **only** the manifest. Blobs the manifest does not name are
  invisible here whatever they are — a torn upload, a superseded range, a
  half-finished correction — which is what makes manifest-last atomicity a
  recovery property rather than a hope.

  Nothing is hydrated over local state: a reference with local durable files is
  left exactly as it is. Local is authoritative, always, so a partially-restored
  node can never lose a record that a stale remote copy would have overwritten.

  Once the files land, the ordinary restore path runs unchanged —
  `Flezha.Operative.Replay` enumerates the Entities it now finds on disk and
  ships each a `RestoreInitState`. There is no second restore implementation.
  """

  require Logger

  alias Flezha.State.DurabilityTier.{Artifact, Backend, Manifest}
  alias Flezha.State.{InputLog, Outbox, Registry}

  @doc """
  Hydrate every shipped reference of `operative_class` whose local files are
  absent. Called from the bind-time restore enumeration, so a fresh controller
  binding a remote-only operative finds its Entities, proc-journal, durable
  inbox and input log on disk by the time it enumerates them.
  """
  @spec hydrate_operative(binary()) :: :ok
  def hydrate_operative(operative_class) when is_binary(operative_class) do
    with {module, opts} <- Backend.resolve(),
         {:ok, entries} <- module.list(Manifest.class_prefix(operative_class), opts) do
      Enum.each(entries, fn {key, _age} -> hydrate_key(module, opts, key) end)

      # The input log is class-level, so it sits outside the per-Entity prefix
      # above and has to be asked for by name. Without it a hydrated Entity
      # comes back with its consumed floor intact and no log to replay from —
      # the next input lands at offset 0, below the floor, and is silently
      # skipped as already-consumed.
      hydrate_ref(module, opts, {:input_log, operative_class})
    end

    :ok
  end

  @doc """
  Hydrate only the named entity keys of one Procedure.

  The manifest is per-Entity, so a narrowed request costs exactly the artifacts
  it names — no full-Procedure fetch to answer a question about one key.
  """
  @spec hydrate_entities(binary(), binary(), [binary()]) :: :ok
  def hydrate_entities(operative_class, procedure_id, entity_keys) do
    case Backend.resolve() do
      :none ->
        :ok

      {module, opts} ->
        Enum.each(entity_keys, fn entity_key ->
          ref = {:entity, {operative_class, procedure_id, entity_key}}
          hydrate_ref(module, opts, ref)
        end)
    end
  end

  @doc """
  Hydrate every shipped registry cspec whose local file is absent — the record
  of which operatives exist and how they are configured.

  Must complete before any hub starts: ProcessHub opens `registry.dets` at hub
  start, so a cspec that lands afterwards is invisible to the orphan reconcile
  and the operatives it names are never started.
  """
  @spec hydrate_cspecs() :: :ok
  def hydrate_cspecs() do
    with {module, opts} <- Backend.resolve(),
         {:ok, entries} <- module.list(Manifest.cspec_prefix(), opts) do
      entries
      |> Enum.flat_map(fn {key, _age} -> read_manifest(module, opts, key) end)
      |> newest_per_hub()
      |> Enum.each(&apply_manifest(module, opts, &1))
    end

    :ok
  end

  # Every controller in a namespace ships its own replica of one gossiped
  # registry, and they all restore to the same local path — so the choice has to
  # be made here rather than left to listing order. The replicas are equivalent
  # by design; newest is the tie-break that behaves best when they are not yet
  # converged.
  #
  # `shipped_at_ms` is the writing node's wall clock, so skew could pick a
  # slightly older replica. That is acceptable and not merely tolerated: the
  # orphan reconcile heals the difference, and there is no cluster-wide
  # monotonic source at this layer to do better.
  defp newest_per_hub(manifests) do
    manifests
    |> Enum.group_by(fn %Manifest{ref: {:cspec, {hub_id, _writer}}} -> hub_id end)
    |> Enum.map(fn {_hub_id, replicas} ->
      Enum.max_by(replicas, fn %Manifest{ref: {:cspec, {_h, writer}}} = m ->
        {m.shipped_at_ms, writer}
      end)
    end)
  end

  @doc """
  One-shot supervision child that runs `hydrate_cspecs/0` and then gets out of
  the way. Placed ahead of `Flezha.Service.HubManager` in the application tree,
  where returning `:ignore` from `start_link` is what makes the ordering a
  guarantee rather than a race.
  """
  @spec hydrate_cspecs_on_boot() :: :ignore
  def hydrate_cspecs_on_boot() do
    hydrate_cspecs()
    :ignore
  rescue
    error ->
      Logger.error("[durability] cspec hydration failed: #{inspect(error)}")
      :ignore
  end

  # ---------- one reference ----------

  defp hydrate_key(module, opts, manifest_key) do
    case read_manifest(module, opts, manifest_key) do
      [manifest] -> apply_manifest(module, opts, manifest)
      [] -> :ok
    end
  end

  # A list rather than an option so callers can `flat_map` a whole prefix and
  # drop what they cannot read without a second unwrapping step.
  defp read_manifest(module, opts, manifest_key) do
    with {:ok, blob} <- module.get(manifest_key, opts),
         {:ok, manifest} <- Manifest.decode(blob) do
      [manifest]
    else
      _ ->
        Logger.warning("[durability] unreadable manifest #{manifest_key}; skipped")
        []
    end
  end

  # `apply_manifest/3` is where local-state precedence is decided, so this only
  # names the manifest to fetch.
  defp hydrate_ref(module, opts, ref), do: hydrate_key(module, opts, Manifest.key(ref))

  defp apply_manifest(module, opts, %Manifest{ref: ref} = manifest) do
    if Artifact.has_local_state?(ref) do
      :ok
    else
      case fetch_and_write(module, opts, manifest) do
        :ok ->
          seed_local_facts(manifest)
          emit(:hydrated, ref, manifest)

        # A referenced artifact the store does not hold is a CORRUPT LINEAGE, not
        # a transport problem, and the two call for opposite operator responses:
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
      Artifact.write_file(manifest.ref, bytes)
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

  # Fetching the data is only half a restore. An Entity is also described by
  # small local files that no record implies — its consumed offset, its WAL-owner
  # epoch, its saga markers — and an Entity that comes back without them looks
  # correct and behaves wrongly. Every write here is an overwrite of a fixed
  # path, so a retry after a partial hydration converges.
  defp seed_local_facts(%Manifest{ref: {:entity, {class, _, _} = entity_id}} = manifest) do
    seed_consumed_offset(class, entity_id, manifest)
    seed_epoch(entity_id, manifest)
    seed_markers(entity_id, manifest)
  end

  defp seed_local_facts(_manifest), do: :ok

  # Normally rebuilt from the effect records in the WAL tail — but a compacted
  # Entity ships a snapshot and no effect record, so the position its cut is
  # consistent with would be lost. Seeding it is what lets post-hydration replay
  # resume at the shipped cut instead of at the start of the retained input log.
  defp seed_consumed_offset(class, entity_id, manifest) do
    case Map.get(manifest.source_offsets, :consumed_offset) do
      offset when is_integer(offset) and offset >= 0 ->
        InputLog.record_consumed(class, entity_id, offset)

      _ ->
        :ok
    end
  end

  # The epoch's only home on disk is the dedup cache, which is not an artifact —
  # so without this the recovered Entity reads epoch 0, its next manifest write
  # is refused as stale against the epoch it shipped under, and it fences itself
  # off permanently. It survives one disk loss and is silently unprotected
  # against the next.
  #
  # The seq fields are deliberately neutral rather than restored.
  # `last_persisted_seq` is ignored at writer open (the WAL scan is
  # authoritative), and `outbox_flushed_seq` gates the boot re-drive with
  # `seq > cursor` — a cursor restored ahead of the restored WAL would silently
  # drop those effect records and never dispatch their outputs. `-1` re-drives
  # everything, which the sinks' dedup absorbs: the cursor may lag, never lead.
  defp seed_epoch(entity_id, %Manifest{epoch: epoch}) do
    _ =
      Outbox.write(entity_id, %{
        last_persisted_seq: 0,
        outbox_flushed_seq: -1,
        epoch: epoch
      })

    :ok
  end

  defp seed_markers(entity_id, %Manifest{markers: markers}) do
    for {kind, true} <- Map.to_list(markers || %{}) do
      Registry.write_compensation_marker(entity_id, kind)
    end

    :ok
  end

  defp emit(event, ref, manifest) do
    :telemetry.execute(
      [:flezha, :state, event],
      %{count: 1, ranges: length(manifest.ranges), snapshots: length(manifest.snapshots)},
      %{ref: inspect(ref)}
    )
  end
end
