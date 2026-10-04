defmodule Flezha.Service.Steps do
  @moduledoc """
  Published service-API surface for steps instances: the task journal, the
  task inbox, completion, retention, time travel and the two verbs that move an
  instance — rescale and rewind.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  The journal, the input log and the completion record are records in flight —
  the same kind of data the dead-letter queue holds — so they carry
  `state.records`, judged in the instance's own namespace, and a point-in-time
  read carries `state.at`. `rescale/5` and `rewind/4` move live work, so each
  carries an action of its own — `operator.scale` and `operator.rewind`,
  both the user's, both judged in the namespace the caller names.

  `rewind/4` takes an operator rather than a pid. The console resolves a pid
  and then rewinds it, which puts a process identifier on the wire and makes the
  caller do the lookup; a published surface names what is being rewound.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Config
  alias Flezha.Operator.Migration
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.Steps.Journal
  alias Flezha.State.Commands
  alias Flezha.State.Completion
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Metrics
  alias Flezha.State.Outbox
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Recovery
  alias Flezha.State.TaskJournal
  alias Flezha.State.TimeTravel
  alias Flezha.State.WalWriter
  alias Flezha.State.WriterRegistry

  @type caller_context :: CallerContext.t()

  @namespace [type: :string, doc: "The namespace."]
  @instance_id [type: :string, doc: "The steps instance."]
  @cell [type: :string, doc: "The Cell, as `<operator>/<tasklet>/<cell_key>`."]
  @operator_class [type: :string, doc: "The operator class."]

  # ---------- Reads ----------

  @doc "Every operator class this controller holds persisted state for."
  @spec operator_classes(caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb operator_classes(caller_context) do
    {:ok, Paths.operator_classes()}
  end

  @doc "One steps instance's completion record."
  @spec completion(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb completion(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    Completion.read(instance_id)
  end

  @doc """
  How long a completed instance of `operator_class` is retained, in hours.

  `metadata` is the instance's own, which takes precedence over the global
  default and is overridden in turn by a user setting.
  """
  @spec completed_retention_hours(String.t(), map(), caller_context()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  verb completed_retention_hours(operator_class, metadata, caller_context),
    verb: :retention_default,
    args: [
      operator_class: [type: :string, doc: "The operator class."],
      metadata: [type: :map, default: %{}, doc: "The instance's own metadata."]
    ] do
    {:ok, PersistenceResolver.completed_retention_hours(operator_class, metadata)}
  end

  @doc """
  One instance's task journal in append order, the leaf tasklets that define
  "done" for it, and whether the in-flight iteration has committed every leaf.
  """
  @spec journal(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb journal(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    Journal.report(instance_id)
  end

  @doc "The iterations the instance's journal has committed."
  @spec committed_iterations(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb committed_iterations(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    {:ok, TaskJournal.committed_iterations(instance_id)}
  end

  @doc "The input-log offset the instance's last iteration consumed, `nil` for none."
  @spec consumed_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb consumed_offset(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    {:ok, TaskJournal.consumed_offset(instance_id)}
  end

  @doc "The input-log records above the instance's floor: what it has not consumed yet."
  @spec undelivered(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb undelivered(instance_id, caller_context),
    verb: :state_inbox,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    with records when is_list(records) <- InputLog.undelivered(instance_id) do
      {:ok,
       Enum.map(records, fn {offset, message, metadata} ->
         %{offset: offset, message: message, metadata: metadata}
       end)}
    end
  end

  @doc "The last offset written to the instance's input log, `-1` when it has none."
  @spec last_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb last_offset(instance_id, caller_context),
    verb: :state_inbox,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    with {:ok, _floor, last} <- InputLog.tail(instance_id), do: {:ok, last}
  end

  @doc "Durable-state counters across every operator class."
  @spec state_metrics(caller_context()) :: {:ok, term()} | {:error, term()}
  verb state_metrics(caller_context) do
    {:ok, Metrics.snapshot_all()}
  end

  @doc """
  One Cell's state as of `target` (a WAL seq, or an ISO 8601 time), replayed
  from its write-ahead log: the seq it resolves to, and the snapshot and change
  log bytes behind it.
  """
  @spec time_travel(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb time_travel(cell, target, caller_context),
    verb: :state_at,
    row: &persisted_cell_row/1,
    as: located,
    args: [cell: @cell, target: [type: :string, doc: "A WAL seq, or an ISO 8601 time."]] do
    with {:ok, result} <- TimeTravel.query(located.cell_id, target(target)) do
      {:ok,
       %{
         cell: cell,
         target: target,
         terminal_seq: result.terminal_seq,
         snapshot_size: byte_size(result.snapshot_bytes),
         change_log_size: byte_size(result.change_log_bytes)
       }}
    end
  end

  @doc """
  The CURRENT in-memory state of one live Cell, or of one tasklet's Cells, as
  JSON, with the change sequence it was read at. It includes changes applied
  but not yet flushed, so it is at or ahead of the durable state. A Cell that
  is not held in memory is refused naming why, never served from the durable
  store, which is what `time_travel/3` is for.
  """
  @spec state_get(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb state_get(path, caller_context),
    verb: :state_get,
    row: &live_row/1,
    as: located,
    args: [path: [type: :string, doc: "`<operator>/<tasklet>[/<cell_key>]`."]] do
    %{namespace: namespace, targets: targets, tasklet: tasklet, cell_key: cell_key} = located

    with {:ok, json, change_seq} <- read_cell_state(namespace, targets, tasklet, cell_key),
         :ok <- within_read_ceiling(json) do
      {:ok, %{path: path, change_seq: change_seq, state: Jason.decode!(json)}}
    end
  end

  @doc """
  One row per Cell of `operator_class` with persisted state: its WAL and
  snapshot sequences, WAL size and the writer's counters.
  """
  @spec cells(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb cells(operator_class, caller_context),
    verb: :state_list,
    row: &class_row/1,
    args: [operator_class: @operator_class] do
    rows =
      for {{class, tasklet, key} = cell_id, counters} <- Metrics.snapshot_all(),
          class == operator_class do
        %{
          operator_class: class,
          tasklet_id: tasklet,
          cell_key: key,
          wal_seq: counters.wal_seq,
          snapshot_seq: counters.snapshot_seq,
          wal_bytes: wal_bytes(cell_id),
          flush_latency_ms_p99: counters.flush_latency_ms_p99,
          fsync_durability_lag_ms: counters.fsync_durability_lag_ms,
          dedup_drops_total: counters.dedup_drops_total
        }
      end

    {:ok, %{operator_class: operator_class, cells: rows}}
  end

  @doc """
  One Cell's outbox: durable seq, flush cursor, epoch, undispatched lag, the
  entries a steps Cell holds until its stage commits, and the operator's
  input-log min-watermark. The lag and the held count are `nil` while the
  Cell's writer is not running.
  """
  @spec outbox(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb outbox(cell, caller_context),
    verb: :state_outbox,
    row: &persisted_cell_row/1,
    as: located,
    args: [cell: @cell] do
    {operator_class, _tasklet, _key} = cell_id = located.cell_id

    status =
      case WriterRegistry.lookup(cell_id) do
        {:ok, pid} ->
          WalWriter.outbox_status(pid)

        :error ->
          # The same cursor record the writer answers with, read off the dedup
          # file instead of the live writer; only the outbox depth is unknown.
          Map.merge(Outbox.read(cell_id), %{pending: nil, held: nil})
      end

    {:ok, Map.merge(status, %{cell: cell, input_watermark: InputLog.watermark(operator_class)})}
  end

  @doc """
  An instance's durable input log, for a flow operator on the effectively-once
  tier and a durable steps operator alike: every retained record with its
  offset, whether it is at or below the instance's floor (consumed), its size
  and channel.
  """
  @spec inbox(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb inbox(instance_id, caller_context),
    verb: :state_inbox,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    case retained_input(instance_id) do
      {:ok, floor, records} ->
        {:ok,
         %{
           instance: instance_id,
           floor: floor,
           entries:
             Enum.map(records, fn {offset, message, metadata} ->
               %{
                 offset: offset,
                 consumed: offset <= floor,
                 bytes: byte_size(message),
                 channel: Map.get(metadata, "channel")
               }
             end)
         }}

      {:error, refusal} ->
        {:error, {:inbox_unreadable, instance_id, refusal}}
    end
  end

  # ---------- Mutations ----------

  @doc "Rescale the partition group `group_id` in `namespace` to `target`."
  @spec rescale(String.t(), String.t(), pos_integer(), keyword(), caller_context()) ::
          term() | {:error, term()}
  verb rescale(namespace, group_id, target, opts, caller_context),
    verb: :operator_scale,
    args: [
      namespace: @namespace,
      group_id: [type: :string, doc: "The partition group."],
      target: [type: :int, min: 1, doc: "The new member count."],
      opts: [type: :keyword, default: [], doc: "`quiesce_timeout_ms`."]
    ] do
    PartitionGroup.rescale(namespace, group_id, target, opts)
  end

  @doc """
  Rewind `operator_class` in `namespace` to the committed iteration `to_iter`.

  Answers `{:error, {:operator_not_running, operator_class}}` rather than resolving a pid for the
  caller to carry, and `{:error, {:uncommitted_iteration, to_iter, committed}}` when
  `to_iter` is not one of the iterations the instance has committed.
  """
  @spec rewind(String.t(), String.t(), non_neg_integer(), caller_context()) ::
          :ok | {:error, term()}
  verb rewind(namespace, operator_class, to_iter, caller_context),
    verb: :operator_rewind,
    args: [
      namespace: @namespace,
      operator_class: [type: :string, doc: "The steps operator."],
      to_iter: [type: :int, min: 0, doc: "The committed iteration to rewind to."]
    ] do
    case OperatorManager.get_operator_pid(
           Flezha.AuthZ.Namespace.or_default(namespace),
           operator_class
         ) do
      {:ok, pid} -> Migration.rewind(pid, to_iter)
      _other -> {:error, {:operator_not_running, operator_class}}
    end
  end

  @doc "Force a compaction of one Cell by dispatching a `CompactCell` to its runner."
  @spec rotate(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb rotate(cell, caller_context),
    verb: :state_rotate,
    row: &persisted_cell_row/1,
    as: located,
    args: [cell: @cell] do
    with {:ok, runner_id} <- Recovery.resolve_runner(located.cell_id),
         {:ok, _ref} <- Commands.dispatch_compact_cell(runner_id, located.cell_id),
         do: {:ok, %{cell: cell}}
  end

  @doc """
  Hard-delete one Cell's persisted state: WAL, snapshots and outbox dedup.
  Deleting a Cell with no state on disk still succeeds.
  """
  @spec delete(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb delete(cell, caller_context),
    verb: :state_delete,
    row: &cell_path_row/1,
    as: located,
    args: [cell: @cell] do
    {:ok, _} = Flezha.State.Supervisor.delete_cell(located.cell_id)
    {:ok, %{cell: cell}}
  end

  @doc """
  Retain `operator_class`'s write-ahead history for `hours`, in this
  controller's memory: it lasts until the next boot unless the runtime
  configuration says so too.
  """
  @spec set_retention(String.t(), non_neg_integer(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb set_retention(operator_class, hours, caller_context),
    verb: :state_retention,
    args: [
      operator_class: @operator_class,
      hours: [type: :int, min: 0, doc: "The hours to retain."]
    ] do
    updated = Map.put(Config.get(:time_travel_retention_hours), operator_class, hours)
    Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)
    {:ok, %{operator_class: operator_class, hours: hours}}
  end

  # ---------- The rows ----------

  @doc false
  # The row a Cell with persisted state is judged by: its class's namespace. A
  # controller that adopted the operator may hold it only remotely, so a Cell
  # missing locally is hydrated, one key's artifacts, never the whole tasklet.
  @spec persisted_cell(String.t()) :: {:ok, map()} | {:error, term()}
  def persisted_cell(path) do
    with {:ok, {class, tasklet, key} = cell_id} <- split_cell(path) do
      if persisted?(cell_id) or hydrated?(class, tasklet, key),
        do: {:ok, %{namespace: OperatorManager.namespace_of_class(class), cell_id: cell_id}},
        else: {:error, {:no_persisted_state, path}}
    end
  end

  @doc false
  # A `--from` or a time-travel target: a WAL seq, or an ISO 8601 time.
  @spec target(String.t()) :: {:seq, integer()} | {:timestamp, integer()}
  def target(written) do
    case Integer.parse(written) do
      {n, ""} ->
        {:seq, n}

      _ ->
        case DateTime.from_iso8601(written) do
          {:ok, dt, _} -> {:timestamp, DateTime.to_unix(dt)}
          _ -> {:seq, 0}
        end
    end
  end

  defp persisted_cell_row([path | _]), do: persisted_cell(path)

  # The path split, with no on-disk check, so deleting a Cell that left nothing
  # behind still succeeds.
  defp cell_path_row([path | _]) do
    with {:ok, {class, _, _} = cell_id} <- split_cell(path),
         do: {:ok, %{namespace: OperatorManager.namespace_of_class(class), cell_id: cell_id}}
  end

  defp class_row([operator_class | _]),
    do: {:ok, %{namespace: OperatorManager.namespace_of_class(operator_class)}}

  # The live Operator's namespace, and the members that hold the key: a
  # partition group id names no operator — it names N — so a keyed read
  # resolves the one member owning the key's group, and a tasklet-level read
  # fans out over every member.
  defp live_row([path | _]) do
    with {:ok, {operator_id, tasklet, cell_key}} <- split_live(path),
         {:ok, namespace, targets} <- state_targets(operator_id, cell_key) do
      {:ok, %{namespace: namespace, targets: targets, tasklet: tasklet, cell_key: cell_key}}
    end
  end

  defp split_cell(path) do
    case String.split(path, "/", parts: 3) do
      [class, tasklet, key] -> {:ok, {class, tasklet, key}}
      _ -> {:error, {:bad_cell_path, path}}
    end
  end

  # No on-disk check for a LIVE read: a resident Cell may have no WAL yet, and
  # a persistence-disabled one never will.
  defp split_live(path) do
    case String.split(path, "/", parts: 3) do
      [operator, tasklet, key] when operator != "" and tasklet != "" ->
        {:ok, {operator, tasklet, key}}

      [operator, tasklet] when operator != "" and tasklet != "" ->
        {:ok, {operator, tasklet, nil}}

      _ ->
        {:error, {:bad_live_path, path}}
    end
  end

  defp persisted?(cell_id), do: cell_id in Paths.all_cells()

  defp hydrated?(class, tasklet, key) do
    DurabilityTier.hydrate_cells(class, tasklet, [key])
    persisted?({class, tasklet, key})
  end

  defp state_targets(operator_id, cell_key) do
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

  defp read_cell_state(namespace, [operator_id], tasklet, cell_key),
    do:
      OperatorManager.get_cell_state(namespace, operator_id, %{
        tasklet: tasklet,
        cell_key: cell_key
      })

  # Two members answer the same tasklet with disjoint cell keys, so the merge is
  # per-tasklet, per-key, never a whole-value overwrite.
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

        {:ok, Jason.encode!(merged), reads |> Enum.map(fn {:ok, _, seq} -> seq end) |> Enum.max()}
    end
  end

  defp deep_merge(left, right) when is_map(left) and is_map(right),
    do: Map.merge(left, right, fn _key, l, r -> deep_merge(l, r) end)

  defp deep_merge(_left, right), do: right

  # The ceiling the steps handoff uses: a whole-tasklet read of a big operator
  # is the way to blow it, and naming the narrowing selector is the way out.
  defp within_read_ceiling(json) do
    limit = PersistenceResolver.wal_high_watermark_bytes()

    case byte_size(json) do
      bytes when bytes > limit -> {:error, {:oversized_state, bytes, limit}}
      _ -> :ok
    end
  end

  defp wal_bytes(cell_id) do
    case :file.read_file_info(Paths.wal_path(cell_id)) do
      {:ok, info} -> elem(info, 1)
      _ -> 0
    end
  end

  # The floor is `-1` while no reporter has consumed anything.
  defp retained_input(instance_id) do
    with {:ok, floor, last} when last >= 0 <- InputLog.tail(instance_id),
         records when is_list(records) <- InputLog.read_range(instance_id, 0, last) do
      {:ok, floor, records}
    else
      {:ok, floor, -1} -> {:ok, floor, []}
      {:error, _refusal} = refused -> refused
    end
  end

  # An instance's namespace is its class's: the registry's answer while it
  # runs, the state root's afterwards.
  defp instance_row([instance_id | _]) when is_binary(instance_id),
    do: {:ok, %{namespace: OperatorManager.namespace_of_class(instance_id)}}
end
