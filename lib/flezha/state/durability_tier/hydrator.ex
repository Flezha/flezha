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
  alias Flezha.State.InputLog

  @doc """
  Hydrate every shipped reference of `operative_class` whose local files are
  absent. Called from the bind-time restore enumeration, so a fresh controller
  binding a remote-only operative finds its Entities, proc-journal, durable
  inbox and input log on disk by the time it enumerates them.
  """
  @spec hydrate_operative(binary()) :: :ok
  def hydrate_operative(operative_class) when is_binary(operative_class) do
    with {module, opts} <- Backend.resolve(),
         {:ok, keys} <- module.list(Manifest.class_prefix(operative_class), opts) do
      Enum.each(keys, &hydrate_key(module, opts, &1))

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
         {:ok, keys} <- module.list(Manifest.cspec_prefix(), opts) do
      Enum.each(keys, &hydrate_key(module, opts, &1))
    end

    :ok
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
    with {:ok, blob} <- module.get(manifest_key, opts),
         {:ok, manifest} <- Manifest.decode(blob) do
      apply_manifest(module, opts, manifest)
    else
      _ ->
        Logger.warning("[durability] unreadable manifest #{manifest_key}; skipped")
        :ok
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
          seed_source_offsets(manifest)
          emit(:hydrated, ref, manifest)

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
    with {:ok, bytes} <- module.get(blob, opts) do
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
      with {:ok, bytes} <- module.get(blob, opts),
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
      case module.get(blob, opts) do
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

  # The consumed offset is normally rebuilt from the effect records in the WAL
  # tail — but a compacted Entity ships a snapshot and no effect record, so the
  # position its cut is consistent with would be lost. Seeding it from the
  # manifest is what lets post-hydration replay resume at the shipped cut
  # instead of at the start of the retained input log.
  defp seed_source_offsets(%Manifest{ref: {:entity, {class, _, _} = entity_id}} = manifest) do
    case Map.get(manifest.source_offsets, :consumed_offset) do
      offset when is_integer(offset) and offset >= 0 ->
        InputLog.record_consumed(class, entity_id, offset)

      _ ->
        :ok
    end
  end

  defp seed_source_offsets(_manifest), do: :ok

  defp emit(event, ref, manifest) do
    :telemetry.execute(
      [:flezha, :state, event],
      %{count: 1, ranges: length(manifest.ranges), snapshots: length(manifest.snapshots)},
      %{ref: inspect(ref)}
    )
  end
end
