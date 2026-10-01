defmodule Flezha.Service.Cli.State do
  @moduledoc """
  `flz state get | list | at | rotate | delete | outbox | retention` and
  `flz operator replay` — the Cell-scoped state reads and writes.

  Every verb here is judged in the namespace that holds the Cell: the row a
  path or a class resolves to, read only for a bound caller whose role grants
  the action, so an unbound caller learns nothing about what exists on disk.
  """

  alias Flezha.Config
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.Lookup
  alias Flezha.Operator.Replay
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.OperatorManager
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
  def class_row([operator_class | _]),
    do: {:ok, %{namespace: OperatorManager.namespace_of_class(operator_class)}}

  @doc """
  The row a `state at | rotate | outbox` is judged by: a Cell with persisted
  state, in its class's namespace.
  """
  def at_row(args), do: cell_row(args)
  def rotate_row(args), do: cell_row(args)
  def outbox_row(args), do: cell_row(args)

  defp cell_row([path_string | _]) do
    with {:ok, {operator_class, _, _} = cell_id} <- parse_state_path(path_string) do
      {:ok, %{namespace: OperatorManager.namespace_of_class(operator_class), cell_id: cell_id}}
    end
  end

  @doc """
  The row a `state delete` is judged by: the path split, with no on-disk check,
  so deleting a Cell that left nothing behind still succeeds.
  """
  def split_row([path_string | _]) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        {:ok, %{namespace: OperatorManager.namespace_of_class(rc), cell_id: {rc, sh, pk}}}

      _ ->
        {:error, {:bad_cell_path, path_string}}
    end
  end

  @doc "The row a `state get` is judged by: the live Operator's namespace and the members that hold the key."
  def live_row([path_string | _]) do
    with {:ok, {operator_id, tasklet, cell_key}} <- parse_live_path(path_string),
         {:ok, namespace, targets} <- resolve_state_targets(operator_id, cell_key) do
      {:ok, %{namespace: namespace, targets: targets, tasklet: tasklet, cell_key: cell_key}}
    end
  end

  @doc "The row an `operator replay` is judged by: the Cell's class namespace."
  def replay_row([_namespace, path_string | _]) do
    with {:ok, {operator_class, _, _} = cell_id} <- parse_state_path(path_string) do
      {:ok, %{namespace: OperatorManager.namespace_of_class(operator_class), cell_id: cell_id}}
    end
  end

  # ---------- The verbs ----------

  @doc """
  `flz state get <operator/tasklet[/cell_key]>` — the CURRENT in-memory
  state of one live Cell (or of one Tasklet's Cells), as JSON, with the
  `change_seq` it was read at.

  The value is the freshest one there is: it includes changes applied but not
  yet flushed, so it is at or ahead of the durable state, and the seq is how a
  caller tells versions apart. A Cell that is not held in memory is refused
  naming why — never served from the durable store, which is what
  `flz state at` is for.
  """
  def state_get(path_string, %{row: row, opts: opts}) do
    %{namespace: namespace, targets: targets, tasklet: tasklet, cell_key: cell_key} = row

    with {:ok, json, change_seq} <- read_cell_state(namespace, targets, tasklet, cell_key),
         :ok <- within_read_ceiling(json) do
      print_state_get(path_string, json, change_seq, Context.format(opts))
    end
  end

  # The read resolves against the namespace that actually holds the Operator
  # rather than assuming `default`.
  #
  # A partition group id names no operator — it names N — so a keyed read
  # resolves the one member owning the key's group, and a tasklet-level read
  # fans out over every member and merges: each holds only its own share.
  defp resolve_state_targets(operator_id, cell_key) do
    case OperatorManager.namespace_of(operator_id) do
      {:ok, namespace} ->
        {:ok, namespace, [operator_id]}

      :error ->
        with {:ok, namespace} <- group_namespace(operator_id) do
          case OperatorManager.resolve_member(namespace, operator_id, cell_key) do
            {:ok, member} -> {:ok, namespace, [member]}
            {:error, {:unkeyed, _group, members}} -> {:ok, namespace, members}
            {:error, _gone} -> {:error, {:no_operator, operator_id}}
          end
        end
    end
  end

  defp group_namespace(operator_id) do
    case OperatorManager.namespace_of_group(operator_id) do
      {:ok, namespace} -> {:ok, namespace}
      :error -> {:error, {:no_operator, operator_id}}
    end
  end

  defp read_cell_state(namespace, [operator_id], tasklet, cell_key) do
    OperatorManager.get_cell_state(namespace, operator_id, %{
      tasklet: tasklet,
      cell_key: cell_key
    })
  end

  defp read_cell_state(namespace, targets, tasklet, cell_key) do
    targets
    |> Enum.map(&read_cell_state(namespace, [&1], tasklet, cell_key))
    |> Enum.filter(&match?({:ok, _json, _seq}, &1))
    |> case do
      [] ->
        {:error, {:no_operator, hd(targets)}}

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

  # Two members answer the same tasklet with disjoint cell keys, so the
  # merge is per-tasklet, per-key — never a whole-value overwrite.
  defp deep_merge(left, right) when is_map(left) and is_map(right),
    do: Map.merge(left, right, fn _key, l, r -> deep_merge(l, r) end)

  defp deep_merge(_left, right), do: right

  # `<operator>/<tasklet>[/<cell_key>]` for a LIVE read. No on-disk check,
  # unlike `parse_state_path/1`: a resident Cell may have no WAL yet, and a
  # persistence-disabled one never will.
  defp parse_live_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [operator, tasklet, cell_key] when operator != "" and tasklet != "" ->
        {:ok, {operator, tasklet, cell_key}}

      [operator, tasklet] when operator != "" and tasklet != "" ->
        {:ok, {operator, tasklet, nil}}

      _ ->
        {:error, {:bad_live_path, path_string}}
    end
  end

  # Same ceiling the steps operator handoff uses — a whole-Tasklet read of a big
  # Operator is the way to blow it, and naming the narrowing selector is the
  # way out.
  defp within_read_ceiling(json) do
    limit = PersistenceResolver.wal_high_watermark_bytes()

    case byte_size(json) do
      bytes when bytes > limit -> {:error, {:oversized_state, bytes, limit}}
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

  @doc """
  `flz state list <operator_class>` — print one row per Cell under
  `<operator_class>` with `wal_seq`, `snapshot_seq`, `wal_bytes`.
  """
  def state_list(operator_class, %{opts: opts}) do
    rows =
      for {{rc, sh, pk} = pid, counters} <- Metrics.snapshot_all(),
          rc == operator_class do
        wal_bytes =
          case :file.read_file_info(Paths.wal_path(pid)) do
            {:ok, info} -> elem(info, 1)
            _ -> 0
          end

        %{
          operator_class: rc,
          tasklet_id: sh,
          cell_key: pk,
          wal_seq: counters.wal_seq,
          snapshot_seq: counters.snapshot_seq,
          wal_bytes: wal_bytes,
          flush_latency_ms_p99: counters.flush_latency_ms_p99,
          fsync_durability_lag_ms: counters.fsync_durability_lag_ms,
          dedup_drops_total: counters.dedup_drops_total
        }
      end

    case Context.format(opts) do
      "json" ->
        IO.puts(Jason.encode!(%{operator_class: operator_class, cells: rows}))

      _ ->
        IO.puts("Cells under operator #{operator_class}:\n")
        Enum.each(rows, &print_state_row/1)
    end
  end

  defp print_state_row(row) do
    IO.puts("  #{row.tasklet_id}/#{row.cell_key}")
    IO.puts("    wal_seq:                 #{row.wal_seq}")
    IO.puts("    snapshot_seq:            #{row.snapshot_seq}")
    IO.puts("    wal_bytes:               #{row.wal_bytes}")
    IO.puts("    flush_latency_ms_p99:    #{row.flush_latency_ms_p99}")
    IO.puts("    fsync_durability_lag_ms: #{row.fsync_durability_lag_ms}")
    IO.puts("    dedup_drops_total:       #{row.dedup_drops_total}")
    IO.puts("")
  end

  @doc """
  `flz state at <operator/tasklet/cell> <seq | timestamp>` — point-in-time
  query. Resolves the path string into a `(runner_id, operator,
  tasklet, cell_key)` by scanning the on-disk state tree (one match
  expected; ambiguous paths print all matches and exit non-zero).
  """
  def state_at(_path_string, target_string, %{row: %{cell_id: cell_id}, opts: opts}) do
    target = parse_state_target(target_string)

    with {:ok, result} <- TimeTravel.query(cell_id, target),
         do: print_state_at(cell_id, target, result, Context.format(opts))
  end

  defp print_state_at(_cell_id, _target, result, "json") do
    IO.puts(
      Jason.encode!(%{
        terminal_seq: result.terminal_seq,
        snapshot_size: byte_size(result.snapshot_bytes),
        change_log_size: byte_size(result.change_log_bytes)
      })
    )
  end

  defp print_state_at(cell_id, target, result, _) do
    {rc, sh, pk} = cell_id

    IO.puts("state at #{rc}/#{sh}/#{pk} target=#{inspect(target)}:")
    IO.puts("  terminal_seq:    #{result.terminal_seq}")
    IO.puts("  snapshot bytes:  #{byte_size(result.snapshot_bytes)}")
    IO.puts("  change_log bytes:#{byte_size(result.change_log_bytes)}")
  end

  @doc """
  `flz state rotate <operator/tasklet/cell>` — force a compaction by
  dispatching a `CompactCell` to the SDK.
  """
  def state_rotate(_path_string, %{row: %{cell_id: {rc, sh, pk}}}) do
    with {:ok, runner_id} <- Recovery.resolve_runner({rc, sh, pk}),
         {:ok, _ref} <- Commands.dispatch_compact_cell(runner_id, {rc, sh, pk}) do
      IO.puts("compaction queued for #{rc}/#{sh}/#{pk}")
    end
  end

  @doc """
  `flz state delete <operator/tasklet/key>` — hard-delete one Cell's
  persisted state (WAL + snapshots + outbox-dedup). Idempotent: deleting
  a cell with no on-disk state still succeeds. This is the
  caller-driven, confirmation-returning counterpart to the SDK reaper's
  fire-and-forget `delete_cell_state` send (Decision 8).
  """
  def state_delete(_path_string, %{row: %{cell_id: {rc, sh, pk}}}) do
    {:ok, _} = Flezha.State.Supervisor.delete_cell({rc, sh, pk})
    IO.puts("deleted persisted state for #{rc}/#{sh}/#{pk}")
  end

  @doc """
  `flz state outbox <operator/tasklet/key>`: outbox status for one Cell
  (effectively-once tier or durable steps operator): durable seq, flush cursor,
  epoch, undispatched lag, the entries a steps operator Cell holds until its stage
  commits, and the operator's input-log min-watermark.
  """
  def state_outbox(_path_string, %{row: %{cell_id: {rc, sh, pk} = cell_id}}) do
    status =
      case Flezha.State.WriterRegistry.lookup(cell_id) do
        {:ok, pid} ->
          WalWriter.outbox_status(pid)

        :error ->
          # The same cursor record the writer answers with, read off the dedup
          # file instead of the live writer; only the outbox depth is unknown.
          Map.merge(Outbox.read(cell_id), %{pending: nil, held: nil})
      end

    watermark = InputLog.watermark(rc)

    IO.puts("cell              #{rc}/#{sh}/#{pk}")
    IO.puts("last_persisted_seq #{status.last_persisted_seq}")
    IO.puts("outbox_flushed_seq #{status.outbox_flushed_seq}")
    IO.puts("epoch              #{status.epoch}")
    IO.puts("outbox_lag         #{status.pending || "unknown (writer not running)"}")
    IO.puts("held_for_stage     #{status.held || "unknown (writer not running)"}")
    IO.puts("input_watermark    #{watermark || "none"}")
  end

  @doc """
  `flz operator replay <operator/tasklet/key> --from <seq|ts>` — re-run a
  Cell's retained inputs from a point in the past through the current code.

  The same transaction a late input triggers automatically, with the caller
  choosing the baseline instead of a late event: rewind the Cell there, bump
  the epoch, and replay the input log forward. A conforming keyed sink overwrites
  the stale rows by natural key, which is what makes "deploy a fix, heal the
  recent past" a single verb rather than a migration.

  A Cell of a steps operator is refused before anything is rewound.
  """
  def operator_replay(_namespace, path_string, from_string, %{row: %{cell_id: cell_id}}) do
    {operator_class, _, _} = cell_id

    with :ok <- replayable(operator_class, path_string),
         {:ok, overwatch} <- resolve_replay_overwatch(operator_class),
         {:ok, target_seq} <- resolve_replay_seq(cell_id, from_string) do
      Replay.replay_from(overwatch, cell_id, target_seq)

      IO.puts(
        "Replaying #{path_string} from seq #{target_seq}. Outputs republish under a " <>
          "bumped epoch; a keyed sink overwrites the stale rows by natural key."
      )
    end
  end

  defp replayable(operator_class, path_string) do
    if ClassRegistry.journals?(operator_class),
      do: {:error, {:steps_cell, path_string}},
      else: :ok
  end

  defp resolve_replay_overwatch(operator_class) do
    case Lookup.overwatch(operator_class) do
      {:ok, pid} -> {:ok, pid}
      {:error, _reason} -> {:error, {:operator_not_running, operator_class}}
    end
  end

  # A `--from` is a WAL seq or a point in time; the same `TimeTravel` resolution
  # `flz state at` uses, which is now exact rather than an mtime approximation.
  defp resolve_replay_seq(cell_id, from_string) do
    with {:ok, %{terminal_seq: seq}} <- TimeTravel.query(cell_id, parse_state_target(from_string)),
         do: {:ok, seq}
  end

  @doc """
  `flz state retention <operator_class> <hours>` — set runtime retention
  for `<operator_class>`. Persisted across the next Application boot only
  if the runner separately writes it to runtime.exs.
  """
  def state_retention(operator_class, hours_string, _call) do
    case Context.integer!(hours_string, "<hours>") do
      hours when hours >= 0 ->
        current = Config.get(:time_travel_retention_hours)
        updated = Map.put(current, operator_class, hours)
        Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)

        IO.puts(
          "retention[#{operator_class}] = #{hours} hours (in-memory; persist via runtime.exs)"
        )

      hours ->
        Context.fail!(Flezha.Error.new("usage.negative", option: "<hours>", value: hours))
    end
  end

  defp parse_state_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        # A controller that adopted this operator may hold it only remotely.
        # The manifest is per-Cell, so answering about one key fetches one
        # key's artifacts — never the whole Tasklet. Inert with no backend.
        if has_persisted_state?(rc, sh, pk) or hydrated_state?(rc, sh, pk) do
          {:ok, {rc, sh, pk}}
        else
          {:error, {:no_persisted_state, path_string}}
        end

      _ ->
        {:error, {:bad_cell_path, path_string}}
    end
  end

  defp has_persisted_state?(rc, sh, pk) do
    Enum.any?(Paths.all_cells(), fn {r, s, p} ->
      r == rc and s == sh and p == pk
    end)
  end

  defp hydrated_state?(rc, sh, pk) do
    DurabilityTier.hydrate_cells(rc, sh, [pk])
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
