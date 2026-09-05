defmodule Flezha.Service.Cli.State do
  @moduledoc """
  `flz state get | list | at | rotate | delete | outbox | retention` and
  `flz operative replay` — the Entity-scoped state reads and writes.

  Every verb here is judged in the namespace that holds the Entity: the row a
  path or a class resolves to, read only for a bound caller whose role grants
  the action, so an unbound caller learns nothing about what exists on disk.
  """

  alias Flezha.Config
  alias Flezha.Operative.Lookup
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Operative.Replay
  alias Flezha.Partition
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Commands
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Metrics
  alias Flezha.State.Outbox
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Recovery
  alias Flezha.State.TimeTravel
  alias Flezha.State.WalWriter

  # ---------- The rows ----------

  @doc "The row a `state list` is judged by: the class's namespace."
  def class_row([operative_class | _]),
    do: {:ok, %{namespace: OperativeManager.namespace_of_class(operative_class)}}

  @doc """
  The row a `state at | rotate | outbox` is judged by: an Entity with persisted
  state, in its class's namespace. Each verb keeps its own prefix on the
  not-found it prints.
  """
  def at_row(args), do: entity_row(args, "state at")
  def rotate_row(args), do: entity_row(args, "state rotate")
  def outbox_row(args), do: entity_row(args, "state outbox")

  defp entity_row([path_string | _], verb) do
    case parse_state_path(path_string) do
      {:ok, {operative_class, _, _} = entity_id} ->
        {:ok,
         %{namespace: OperativeManager.namespace_of_class(operative_class), entity_id: entity_id}}

      {:error, reason} ->
        Context.fail!("#{verb}: #{reason}")
    end
  end

  @doc """
  The row a `state delete` is judged by: the path split, with no on-disk check,
  so deleting an Entity that left nothing behind still succeeds.
  """
  def split_row([path_string | _]) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        {:ok, %{namespace: OperativeManager.namespace_of_class(rc), entity_id: {rc, sh, pk}}}

      _ ->
        Context.fail!(
          "state delete: expected <operative>/<procedure>/<entity_key>; got #{path_string}"
        )
    end
  end

  @doc "The row a `state get` is judged by: the live Operative's namespace and the members that hold the key."
  def live_row([path_string | _]) do
    with {:ok, {operative_id, procedure, entity_key}} <- parse_live_path(path_string),
         {:ok, namespace, targets} <- resolve_state_targets(operative_id, entity_key) do
      {:ok,
       %{namespace: namespace, targets: targets, procedure: procedure, entity_key: entity_key}}
    else
      {:error, reason} ->
        Context.fail!("state get: #{render_state_get_error(reason)}")
    end
  end

  @doc "The row an `operative replay` is judged by: the Entity's class namespace."
  def replay_row([_namespace, path_string | _]) do
    case parse_state_path(path_string) do
      {:ok, {operative_class, _, _} = entity_id} ->
        {:ok,
         %{namespace: OperativeManager.namespace_of_class(operative_class), entity_id: entity_id}}

      {:error, reason} ->
        Context.fail!("replay: #{Context.format_reason(reason)}")
    end
  end

  # ---------- The verbs ----------

  @doc """
  `flz state get <operative/procedure[/entity_key]>` — the CURRENT in-memory
  state of one live Entity (or of one Procedure's Entities), as JSON, with the
  `change_seq` it was read at.

  The value is the freshest one there is: it includes changes applied but not
  yet flushed, so it is at or ahead of the durable state, and the seq is how a
  caller tells versions apart. An Entity that is not held in memory is refused
  naming why — never served from the durable store, which is what
  `flz state at` is for.
  """
  def state_get(path_string, output_format, %{row: row}) do
    %{namespace: namespace, targets: targets, procedure: procedure, entity_key: entity_key} = row

    with {:ok, json, change_seq} <- read_entity_state(namespace, targets, procedure, entity_key),
         :ok <- within_read_ceiling(json) do
      print_state_get(path_string, json, change_seq, output_format)
    else
      {:error, reason} ->
        Context.fail!("state get: #{render_state_get_error(reason)}")
    end
  end

  # The read resolves against the namespace that actually holds the Operative
  # rather than assuming `default`.
  #
  # A partition group id names no operative — it names N — so a keyed read
  # resolves the one member owning the key's group, and a procedure-level read
  # fans out over every member and merges: each holds only its own share.
  defp resolve_state_targets(operative_id, entity_key) do
    case OperativeManager.namespace_of(operative_id) do
      {:ok, namespace} ->
        {:ok, namespace, [operative_id]}

      :error ->
        with {:ok, namespace} <- group_namespace(operative_id),
             members = PartitionGroup.members(namespace, operative_id) do
          {:ok, namespace, owning_members(members, entity_key)}
        end
    end
  end

  defp group_namespace(operative_id) do
    case OperativeManager.namespace_of_group(operative_id) do
      {:ok, namespace} -> {:ok, namespace}
      :error -> {:error, {:no_operative, operative_id}}
    end
  end

  defp owning_members(members, nil), do: Enum.map(members, & &1.id)

  defp owning_members(members, entity_key) do
    case Enum.find(members, &Partition.owns?(&1.partition.range, entity_key)) do
      nil -> Enum.map(members, & &1.id)
      member -> [member.id]
    end
  end

  defp read_entity_state(namespace, [operative_id], procedure, entity_key) do
    OperativeManager.get_entity_state(namespace, operative_id, %{
      procedure: procedure,
      entity_key: entity_key
    })
  end

  defp read_entity_state(namespace, targets, procedure, entity_key) do
    targets
    |> Enum.map(&read_entity_state(namespace, [&1], procedure, entity_key))
    |> Enum.filter(&match?({:ok, _json, _seq}, &1))
    |> case do
      [] ->
        {:error, {:no_operative, hd(targets)}}

      reads ->
        merged =
          Enum.reduce(reads, %{}, fn {:ok, json, _seq}, acc ->
            case Jason.decode(json) do
              {:ok, decoded} when is_map(decoded) -> deep_merge(acc, decoded)
              _ -> acc
            end
          end)

        {:ok, Jason.encode!(merged), Enum.map(reads, fn {:ok, _, seq} -> seq end) |> Enum.max()}
    end
  end

  # Two members answer the same procedure with disjoint entity keys, so the
  # merge is per-procedure, per-key — never a whole-value overwrite.
  defp deep_merge(left, right) when is_map(left) and is_map(right),
    do: Map.merge(left, right, fn _key, l, r -> deep_merge(l, r) end)

  defp deep_merge(_left, right), do: right

  # `<operative>/<procedure>[/<entity_key>]` for a LIVE read. No on-disk check,
  # unlike `parse_state_path/1`: a resident Entity may have no WAL yet, and a
  # persistence-disabled one never will.
  defp parse_live_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [operative, procedure, entity_key] when operative != "" and procedure != "" ->
        {:ok, {operative, procedure, entity_key}}

      [operative, procedure] when operative != "" and procedure != "" ->
        {:ok, {operative, procedure, nil}}

      _ ->
        {:error,
         {:bad_path, "expected <operative>/<procedure>[/<entity_key>]; got #{path_string}"}}
    end
  end

  # Same ceiling the workflow handoff uses — a whole-Procedure read of a big
  # Operative is the way to blow it, and naming the narrowing selector is the
  # way out.
  defp within_read_ceiling(json) do
    limit = PersistenceResolver.wal_high_watermark_bytes()

    case byte_size(json) do
      bytes when bytes > limit -> {:error, {:oversized_state, bytes}}
      _ -> :ok
    end
  end

  defp print_state_get(path_string, json, change_seq, "json") do
    IO.puts(
      Jason.encode!(%{
        path: path_string,
        change_seq: change_seq,
        state: Jason.decode!(json)
      })
    )
  end

  defp print_state_get(path_string, json, change_seq, _) do
    IO.puts("state get #{path_string}:")
    IO.puts("  change_seq: #{change_seq || "-"}")
    IO.puts("  state:      #{json}")
  end

  defp render_state_get_error({:bad_path, message}), do: message

  defp render_state_get_error({:no_operative, operative_id}),
    do: "no running operative `#{operative_id}` in any namespace"

  defp render_state_get_error({:not_resident, :suspended}),
    do:
      "the instance is suspended and holds no live Entity — " <>
        "read its durable state with `flz state at`"

  defp render_state_get_error({:not_resident, :unbound}),
    do:
      "the Operative is not bound to a runner, so nothing holds its state — " <>
        "read its durable state with `flz state at`"

  defp render_state_get_error({:unsupported_for_kind, :persistent_state}),
    do: "this Operative kind holds no per-Entity state, so there is nothing to read"

  defp render_state_get_error({:oversized_state, bytes}),
    do:
      "the selected state is #{bytes} bytes, over the :wal_high_watermark_bytes " <>
        "ceiling — narrow the read with <operative>/<procedure>/<entity_key>"

  defp render_state_get_error(reason) when is_binary(reason) do
    # SDK refusals lead with a canonical token; the not-resident family points
    # at the durable-read path, which the SDK cannot know about.
    if String.starts_with?(reason, ["entity_evicted", "entity_not_found"]) do
      reason <> " — read its durable state with `flz state at`"
    else
      reason
    end
  end

  defp render_state_get_error(reason), do: Context.format_reason(reason)

  @doc """
  `flz state list <operative_class>` — print one row per Entity under
  `<operative_class>` with `wal_seq`, `snapshot_seq`, `wal_bytes`.
  """
  def state_list(operative_class, output_format, _call) do
    rows =
      for {{rc, sh, pk} = pid, counters} <- Metrics.snapshot_all(),
          rc == operative_class do
        wal_bytes =
          case :file.read_file_info(Paths.wal_path(pid)) do
            {:ok, info} -> elem(info, 1)
            _ -> 0
          end

        %{
          operative_class: rc,
          procedure_id: sh,
          entity_key: pk,
          wal_seq: counters.wal_seq,
          snapshot_seq: counters.snapshot_seq,
          wal_bytes: wal_bytes,
          flush_latency_ms_p99: counters.flush_latency_ms_p99,
          fsync_durability_lag_ms: counters.fsync_durability_lag_ms,
          dedup_drops_total: counters.dedup_drops_total
        }
      end

    case output_format do
      "json" ->
        IO.puts(Jason.encode!(%{operative_class: operative_class, entities: rows}))

      _ ->
        IO.puts("Entities under operative #{operative_class}:\n")
        Enum.each(rows, &print_state_row/1)
    end
  end

  defp print_state_row(row) do
    IO.puts("  #{row.procedure_id}/#{row.entity_key}")
    IO.puts("    wal_seq:                 #{row.wal_seq}")
    IO.puts("    snapshot_seq:            #{row.snapshot_seq}")
    IO.puts("    wal_bytes:               #{row.wal_bytes}")
    IO.puts("    flush_latency_ms_p99:    #{row.flush_latency_ms_p99}")
    IO.puts("    fsync_durability_lag_ms: #{row.fsync_durability_lag_ms}")
    IO.puts("    dedup_drops_total:       #{row.dedup_drops_total}")
    IO.puts("")
  end

  @doc """
  `flz state at <operative/procedure/entity> <seq | timestamp>` — point-in-time
  query. Resolves the path string into a `(runner_id, operative,
  procedure, entity_key)` by scanning the on-disk state tree (one match
  expected; ambiguous paths print all matches and exit non-zero).
  """
  def state_at(_path_string, target_string, output_format, %{row: %{entity_id: entity_id}}) do
    target = parse_state_target(target_string)

    case TimeTravel.query(entity_id, target) do
      {:ok, result} ->
        print_state_at(entity_id, target, result, output_format)

      {:error, %{kind: kind, message: msg, earliest_available: earliest}} ->
        Context.fail!("state at: #{kind}: #{msg} (earliest_available=#{inspect(earliest)})")
    end
  end

  defp print_state_at(_entity_id, _target, result, "json") do
    IO.puts(
      Jason.encode!(%{
        terminal_seq: result.terminal_seq,
        snapshot_size: byte_size(result.snapshot_bytes),
        change_log_size: byte_size(result.change_log_bytes)
      })
    )
  end

  defp print_state_at(entity_id, target, result, _) do
    {rc, sh, pk} = entity_id

    IO.puts("state at #{rc}/#{sh}/#{pk} target=#{inspect(target)}:")
    IO.puts("  terminal_seq:    #{result.terminal_seq}")
    IO.puts("  snapshot bytes:  #{byte_size(result.snapshot_bytes)}")
    IO.puts("  change_log bytes:#{byte_size(result.change_log_bytes)}")
  end

  @doc """
  `flz state rotate <operative/procedure/entity>` — force a compaction by
  dispatching a `CompactEntity` to the SDK.
  """
  def state_rotate(_path_string, %{row: %{entity_id: {rc, sh, pk}}}) do
    case Recovery.resolve_runner({rc, sh, pk}) do
      {:ok, runner_id} ->
        case Commands.dispatch_compact_entity(runner_id, {rc, sh, pk}) do
          {:ok, _ref} -> IO.puts("compaction queued for #{rc}/#{sh}/#{pk}")
          {:error, reason} -> Context.fail!("state rotate: #{Context.format_reason(reason)}")
        end

      {:error, reason} ->
        Context.fail!("state rotate: #{reason}")
    end
  end

  @doc """
  `flz state delete <operative/procedure/key>` — hard-delete one Entity's
  persisted state (WAL + snapshots + outbox-dedup). Idempotent: deleting
  an entity with no on-disk state still succeeds. This is the
  operator-driven, confirmation-returning counterpart to the SDK reaper's
  fire-and-forget `delete_entity_state` send (Decision 8).
  """
  def state_delete(_path_string, %{row: %{entity_id: {rc, sh, pk}}}) do
    {:ok, _} = Flezha.State.Supervisor.delete_entity({rc, sh, pk})
    IO.puts("deleted persisted state for #{rc}/#{sh}/#{pk}")
  end

  @doc """
  `flz state outbox <operative/procedure/key>` — effectively-once outbox
  status for one Entity: durable seq, flush cursor, epoch, undispatched
  lag, and the operative's input-log min-watermark.
  """
  def state_outbox(_path_string, %{row: %{entity_id: {rc, sh, pk} = entity_id}}) do
    status =
      case Flezha.State.WriterRegistry.lookup(entity_id) do
        {:ok, pid} ->
          WalWriter.outbox_status(pid)

        :error ->
          dedup = Outbox.read(entity_id)

          %{
            last_persisted_seq: dedup.last_persisted_seq,
            outbox_flushed_seq: dedup.outbox_flushed_seq,
            epoch: dedup.epoch,
            pending: nil
          }
      end

    watermark = InputLog.watermark(rc)

    IO.puts("entity            #{rc}/#{sh}/#{pk}")
    IO.puts("last_persisted_seq #{status.last_persisted_seq}")
    IO.puts("outbox_flushed_seq #{status.outbox_flushed_seq}")
    IO.puts("epoch              #{status.epoch}")
    IO.puts("outbox_lag         #{status.pending || "unknown (writer not running)"}")
    IO.puts("input_watermark    #{watermark || "none"}")
  end

  @doc """
  `flz operative replay <operative/procedure/key> --from <seq|ts>` — re-run a
  Entity's retained inputs from a point in the past through the current code.

  The same transaction a late input triggers automatically, with the operator
  choosing the baseline instead of a late event: rewind the Entity there, bump
  the epoch, and replay the input log forward. A conforming keyed sink overwrites
  the stale rows by natural key, which is what makes "deploy a fix, heal the
  recent past" a single verb rather than a migration.
  """
  def operative_replay(_namespace, path_string, from_string, %{row: %{entity_id: entity_id}}) do
    {operative_class, _, _} = entity_id

    with {:ok, overwatch} <- resolve_replay_overwatch(operative_class),
         {:ok, target_seq} <- resolve_replay_seq(entity_id, from_string) do
      Replay.replay_from(overwatch, entity_id, target_seq)

      IO.puts(
        "Replaying #{path_string} from seq #{target_seq}. Outputs republish under a " <>
          "bumped epoch; a keyed sink overwrites the stale rows by natural key."
      )
    else
      {:error, %{kind: :outside_retention, earliest_available: earliest}} ->
        Context.fail!(
          "replay: outside_retention — the earliest replayable point for " <>
            "#{path_string} is seq #{inspect(earliest)}"
        )

      {:error, reason} ->
        Context.fail!("replay: #{Context.format_reason(reason)}")
    end
  end

  defp resolve_replay_overwatch(operative_class) do
    case Lookup.overwatch(operative_class) do
      {:ok, pid} -> {:ok, pid}
      {:error, _reason} -> {:error, {:not_running, operative_class}}
    end
  end

  # A `--from` is a WAL seq or a point in time; the same `TimeTravel` resolution
  # `flz state at` uses, which is now exact rather than an mtime approximation.
  defp resolve_replay_seq(entity_id, from_string) do
    case TimeTravel.query(entity_id, parse_state_target(from_string)) do
      {:ok, %{terminal_seq: seq}} -> {:ok, seq}
      {:error, err} -> {:error, err}
    end
  end

  @doc """
  `flz state retention <operative_class> <hours>` — set runtime retention
  for `<operative_class>`. Persisted across the next Application boot only
  if the runner separately writes it to runtime.exs.
  """
  def state_retention(operative_class, hours_string, _call) do
    case Context.integer!(hours_string, "<hours>") do
      hours when hours >= 0 ->
        current = Config.get(:time_travel_retention_hours)
        updated = Map.put(current, operative_class, hours)
        Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)

        IO.puts(
          "retention[#{operative_class}] = #{hours} hours (in-memory; persist via runtime.exs)"
        )

      _ ->
        Context.fail!("state retention: hours must be a non-negative integer")
    end
  end

  defp parse_state_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        # A controller that adopted this operative may hold it only remotely.
        # The manifest is per-Entity, so answering about one key fetches one
        # key's artifacts — never the whole Procedure. Inert with no backend.
        if has_persisted_state?(rc, sh, pk) or hydrated_state?(rc, sh, pk) do
          {:ok, {rc, sh, pk}}
        else
          {:error, "no persisted state at #{path_string}"}
        end

      _ ->
        {:error, "expected <operative>/<procedure>/<entity_key>; got #{path_string}"}
    end
  end

  defp has_persisted_state?(rc, sh, pk) do
    Enum.any?(Paths.all_entities(), fn {r, s, p} ->
      r == rc and s == sh and p == pk
    end)
  end

  defp hydrated_state?(rc, sh, pk) do
    DurabilityTier.hydrate_entities(rc, sh, [pk])
    has_persisted_state?(rc, sh, pk)
  end

  defp parse_state_target(s) do
    case Integer.parse(s) do
      {n, ""} ->
        {:seq, n}

      _ ->
        # Try ISO 8601 timestamp.
        case DateTime.from_iso8601(s) do
          {:ok, dt, _} -> {:timestamp, DateTime.to_unix(dt)}
          _ -> {:seq, 0}
        end
    end
  end
end
