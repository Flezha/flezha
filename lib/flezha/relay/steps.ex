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
  carries an action of its own — `relay.scale` and `relay.rewind`,
  both the user's, both judged in the namespace the caller names.

  `rewind/4` takes a relay rather than a pid. The console resolves a pid
  and then rewinds it, which puts a process identifier on the wire and makes the
  caller do the lookup; a published surface names what is being rewound.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Logs.RelayHistory
  alias Flezha.Cluster.Page
  alias Flezha.Config
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Relay.Suspend
  alias Flezha.Service.CallerContext
  alias Flezha.Service.RelayManager
  alias Flezha.Service.Steps.Journal
  alias Flezha.State.Commands
  alias Flezha.State.Completion
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog
  alias Flezha.State.Metrics
  alias Flezha.State.Lineage
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
  @cell [type: :string, doc: "The Cell, as `<relay>/<tasklet>/<cell_key>`."]
  @relay_class [type: :string, doc: "The relay class."]

  # ---------- Reads ----------

  @doc "One steps instance's completion record."
  @spec completion(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb completion(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    RelayManager.on_state_node(instance_id, Completion, :read, [instance_id])
  end

  @doc """
  How long a completed instance of `relay_class` is retained, in hours.

  `metadata` is the instance's own, which takes precedence over the global
  default and is overridden in turn by a user setting.
  """
  @spec completed_retention_hours(String.t(), map(), caller_context()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  verb completed_retention_hours(relay_class, metadata, caller_context),
    verb: :retention_default,
    args: [
      relay_class: [type: :string, doc: "The relay class."],
      metadata: [type: :map, default: %{}, doc: "The instance's own metadata."]
    ] do
    {:ok, PersistenceResolver.completed_retention_hours(relay_class, metadata)}
  end

  @doc """
  One instance's task journal, in append order, and the run's outcome. The
  outcome is the status and error the engine completed the run with, or `nil`
  while it runs. With `opts` the entries are paged newest first by seq
  (`Flezha.Cluster.Page`), under `rows`.
  """
  @spec journal(String.t(), keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb journal(instance_id, opts \\ [], caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id, opts: Page.arg()] do
    with {:ok, page} <- Page.parse(opts, :journal, sorts: [seq: :desc]),
         do: RelayManager.on_state_node(instance_id, Journal, :report, [instance_id, page])
  end

  @doc "The iterations the instance's journal has committed; with `opts`, paged newest first."
  @spec committed_iterations(String.t(), keyword(), caller_context()) ::
          {:ok, [term()] | map()} | {:error, term()}
  verb committed_iterations(instance_id, opts \\ [], caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id, opts: Page.arg()] do
    with {:ok, page} <- Page.parse(opts, :committed_iterations, sorts: [iteration: :desc]),
         do: on_state(instance_id, :local_committed_iterations, [instance_id, page])
  end

  @doc "The input-log offset the instance's last iteration consumed. It is `nil` for none."
  @spec consumed_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb consumed_offset(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    on_state(instance_id, :local_consumed_offset, [instance_id])
  end

  @doc """
  The input-log records above the instance's floor. They are what it has not
  consumed yet. With `opts` they are paged by offset, `total` counted from the
  log's offsets and only the page's records read; `limit: 0` reads none.
  """
  @spec undelivered(String.t(), keyword(), caller_context()) ::
          {:ok, [term()] | map()} | {:error, term()}
  verb undelivered(instance_id, opts \\ [], caller_context),
    verb: :state_inbox,
    row: &instance_row/1,
    args: [instance_id: @instance_id, opts: Page.arg()] do
    with {:ok, page} <- Page.parse(opts, :undelivered, sorts: [offset: :asc]),
         do: on_state(instance_id, :local_undelivered, [instance_id, page])
  end

  @doc "The last offset written to the instance's input log. It is `-1` when it has none."
  @spec last_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb last_offset(instance_id, caller_context),
    verb: :state_inbox,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    on_state(instance_id, :local_last_offset, [instance_id])
  end

  @doc """
  One Cell's state at a past point, from its write-ahead log. `target` is a WAL
  seq, or an ISO 8601 time. The state is replayed from the log, answering the
  seq it resolves to, and the snapshot and change log bytes behind it.
  """
  @spec time_travel(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb time_travel(cell, target, caller_context),
    verb: :state_at,
    row: &cell_path_row/1,
    as: located,
    args: [cell: @cell, target: [type: :string, doc: "A WAL seq, or an ISO 8601 time."]] do
    {relay_class, _tasklet, _key} = located.cell_id
    on_state(relay_class, :local_time_travel, [cell, target])
  end

  @doc """
  The CURRENT in-memory state of live Cells, as JSON. It reads one live Cell, or
  one tasklet's Cells, with the change sequence it was read at. It includes
  changes applied but not yet flushed, so it is at or ahead of the durable
  state. A Cell that is not held in memory is refused naming why, never served
  from the durable store, which is what `time_travel/3` is for.
  """
  @spec state_get(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb state_get(path, caller_context),
    verb: :state_get,
    row: &live_row/1,
    as: located,
    args: [path: [type: :string, doc: "`<relay>/<tasklet>[/<cell_key>]`."]] do
    %{namespace: namespace, targets: targets, tasklet: tasklet, cell_key: cell_key} = located

    with {:ok, json, change_seq} <- read_cell_state(namespace, targets, tasklet, cell_key),
         :ok <- within_read_ceiling(json) do
      {:ok, %{path: path, change_seq: change_seq, state: Jason.decode!(json)}}
    end
  end

  @doc """
  One row per Cell of a relay class with persisted state. Each row holds its WAL
  and snapshot sequences, WAL size and the writer's counters.

  `history_retention_hours` is how long the class's write-ahead history is
  kept, the value `set_retention/3` changes. With `opts` the Cells are paged by
  tasklet and key under `rows`, `filter: [text: ...]` a substring of the key.
  """
  @spec cells(String.t(), keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb cells(relay_class, opts \\ [], caller_context),
    verb: :state_list,
    row: &class_row/1,
    args: [relay_class: @relay_class, opts: Page.arg()] do
    with {:ok, page} <- Page.parse(opts, :cells, sorts: [key: :asc], filters: [:text]),
         do: on_state(relay_class, :local_cells, [relay_class, page])
  end

  @doc """
  One Cell's outbox, with its cursors, epoch and lag. It answers the durable
  seq, flush cursor, epoch, undispatched lag, the entries a steps Cell holds
  until its stage commits, and the relay's input-log min-watermark. The lag and
  the held count are `nil` while the Cell's writer is not running.
  """
  @spec outbox(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb outbox(cell, caller_context),
    verb: :state_outbox,
    row: &cell_path_row/1,
    as: located,
    args: [cell: @cell] do
    {relay_class, _tasklet, _key} = located.cell_id
    on_state(relay_class, :local_outbox, [cell])
  end

  @doc """
  An instance's durable input log, record by record. It covers a flow relay on
  the effectively-once tier and a durable steps relay alike: every retained
  record with its offset, whether it is at or below the instance's floor
  (consumed), its size and channel.
  """
  @spec inbox(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb inbox(instance_id, caller_context),
    verb: :state_inbox,
    row: &instance_row/1,
    args: [instance_id: @instance_id] do
    on_state(instance_id, :local_inbox, [instance_id])
  end

  # ---------- Mutations ----------

  @doc "Rescale a partition group in a namespace to a new size."
  @spec rescale(String.t(), String.t(), pos_integer(), keyword(), caller_context()) ::
          term() | {:error, term()}
  verb rescale(namespace, group_id, target, opts, caller_context),
    verb: :relay_scale,
    args: [
      namespace: @namespace,
      group_id: [type: :string, doc: "The partition group."],
      target: [type: :int, min: 1, doc: "The new member count."],
      opts: [type: :keyword, default: [], doc: "`quiesce_timeout_ms`."]
    ] do
    origin = RelayHistory.origin("rescale", caller_context)
    PartitionGroup.rescale(namespace, group_id, target, Keyword.put(opts, :origin, origin))
  end

  @doc """
  Rewind a relay class in a namespace to an earlier iteration. `to_iter` must be
  an iteration the instance has committed. Answers
  `{:error, {:relay_not_running, relay_class}}` rather than resolving a pid for
  the caller to carry, and
  `{:error, {:uncommitted_iteration, to_iter, committed}}` when `to_iter` is not
  one of the iterations the instance has committed.
  """
  @spec rewind(String.t(), String.t(), non_neg_integer(), caller_context()) ::
          :ok | {:error, term()}
  verb rewind(namespace, relay_class, to_iter, caller_context),
    verb: :relay_rewind,
    args: [
      namespace: @namespace,
      relay_class: [type: :string, doc: "The steps relay."],
      to_iter: [type: :int, min: 0, doc: "The committed iteration to rewind to."]
    ] do
    case RelayManager.get_relay_pid(
           Flezha.AuthZ.Namespace.or_default(namespace),
           relay_class
         ) do
      {:ok, pid} -> Suspend.rewind(pid, to_iter)
      _other -> {:error, {:relay_not_running, relay_class}}
    end
  end

  @doc """
  Force a compaction of one Cell. It dispatches a `CompactCell` to the Cell's
  runner and awaits its answer: `compaction` is `:queued`; `:deferred` when the
  runner refused the snapshot as busy and the Watchdog asks again; `:timed_out`
  when no answer came within `compaction_timeout_ms`; or `:failed`, with the
  `reason`.
  """
  @spec rotate(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb rotate(cell, caller_context),
    verb: :state_rotate,
    row: &persisted_cell_row/1,
    as: located,
    args: [cell: @cell] do
    with {:ok, runner_id} <- Recovery.resolve_runner(located.cell_id),
         {:ok, verdict} <- Commands.dispatch_compact_cell(runner_id, located.cell_id, await: true),
         do: {:ok, Map.put(rotated(verdict), :cell, cell)}
  end

  # The verdict as the verb answers it; the bare dispatch without a control
  # unit answers its send reference, a compaction queued all the same.
  defp rotated(verdict) when verdict in [:deferred, :timed_out], do: %{compaction: verdict}
  defp rotated({:failed, %Flezha.Error{} = e}), do: rotated({:failed, "#{e.message} (#{e.code})"})

  defp rotated({:failed, reason}) when is_binary(reason),
    do: %{compaction: :failed, reason: reason}

  defp rotated({:failed, reason}), do: rotated({:failed, inspect(reason)})
  defp rotated(_queued), do: %{compaction: :queued}

  @doc """
  Hard-delete one Cell's persisted state. That is its WAL, snapshots and outbox
  dedup. Deleting a Cell with no state on disk still succeeds.
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
  Set how long a relay class keeps its write-ahead history. The setting is held
  in this controller's memory: it lasts until the next boot unless the runtime
  configuration says so too.
  """
  @spec set_retention(String.t(), non_neg_integer(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb set_retention(relay_class, hours, caller_context),
    verb: :state_retention,
    args: [
      relay_class: @relay_class,
      hours: [type: :int, min: 0, doc: "The hours to retain."]
    ] do
    updated = Map.put(Config.get(:time_travel_retention_hours), relay_class, hours)
    Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)
    {:ok, %{relay_class: relay_class, hours: hours}}
  end

  # ---------- Local halves ----------

  # Each read above answers from the controller holding the instance's state;
  # these are the halves that controller runs, called by name.

  defp on_state(instance_id, function, args),
    do: RelayManager.on_state_node(instance_id, __MODULE__, function, args)

  @doc false
  def local_committed_iterations(instance_id, page \\ nil),
    do: {:ok, paged(TaskJournal.committed_iterations(instance_id), page, & &1)}

  @doc false
  def local_consumed_offset(instance_id), do: {:ok, TaskJournal.consumed_offset(instance_id)}

  @doc false
  def local_undelivered(instance_id, page \\ nil)

  def local_undelivered(instance_id, nil) do
    with records when is_list(records) <- InputLog.undelivered(instance_id),
         do: {:ok, Enum.map(records, &undelivered_row/1)}
  end

  # The records above the floor are the contiguous offsets `floor + 1..last`,
  # so the total needs no record and the page reads only its own.
  def local_undelivered(instance_id, %Page{} = page) do
    with {:ok, floor, last} <- InputLog.tail(instance_id) do
      from = max(floor, page.after || -1) + 1
      to = min(from + page.limit - 1, last)
      records = if to >= from, do: InputLog.read_range(instance_id, from, to), else: []

      with records when is_list(records) <- records do
        rows = Enum.map(records, &undelivered_row/1)
        {:ok, Page.answer(rows, max(last - floor, 0), to < last, page, & &1.offset)}
      end
    end
  end

  defp undelivered_row({offset, message, metadata}),
    do: %{offset: offset, message: message, metadata: metadata}

  @doc false
  def local_last_offset(instance_id) do
    with {:ok, _floor, last} <- InputLog.tail(instance_id), do: {:ok, last}
  end

  @doc false
  def local_time_travel(cell, target) do
    with {:ok, %{cell_id: cell_id}} <- persisted_cell(cell),
         {:ok, at} <- target(target, "target"),
         {:ok, result} <- TimeTravel.query(cell_id, at) do
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

  @doc false
  def local_cells(relay_class, page \\ nil) do
    rows =
      for {{class, tasklet, key} = cell_id, counters} <- Metrics.snapshot_all(),
          class == relay_class do
        %{
          relay_class: class,
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

    {:ok,
     %{
       relay_class: relay_class,
       history_retention_hours: PersistenceResolver.retention_hours(relay_class)
     }
     |> Map.merge(paged_cells(rows, page))}
  end

  defp paged_cells(rows, nil), do: %{cells: rows}

  defp paged_cells(rows, page) do
    text = page.filter |> Map.get(:text, "") |> to_string()

    rows
    |> Enum.filter(&String.contains?(&1.cell_key, text))
    |> paged(page, &{&1.tasklet_id, &1.cell_key})
  end

  # The answer as a page when one is asked for, else as it was.
  defp paged(rows, nil, _key), do: rows
  defp paged(rows, page, key), do: Page.slice(rows, page, key)

  @doc false
  def local_outbox(cell) do
    with {:ok, %{cell_id: {relay_class, _tasklet, _key} = cell_id}} <- persisted_cell(cell) do
      status =
        case WriterRegistry.lookup(cell_id) do
          {:ok, pid} ->
            WalWriter.outbox_status(pid)

          :error ->
            # The same cursor record the writer answers with, read off the dedup
            # file instead of the live writer; only the outbox depth is unknown.
            Map.merge(Lineage.progress(cell_id), %{pending: nil, held: nil})
        end

      {:ok, Map.merge(status, %{cell: cell, input_watermark: InputLog.watermark(relay_class)})}
    end
  end

  @doc false
  def local_inbox(instance_id) do
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

  # ---------- The rows ----------

  @doc false
  # The row a Cell with persisted state is judged by: its class's namespace. A
  # controller that adopted the relay may hold it only remotely, so a Cell
  # missing locally is hydrated, one key's artifacts, never the whole tasklet.
  @spec persisted_cell(String.t()) :: {:ok, map()} | {:error, term()}
  def persisted_cell(path) do
    with {:ok, {class, tasklet, key} = cell_id} <- split_cell(path) do
      if persisted?(cell_id) or hydrated?(class, tasklet, key),
        do: {:ok, %{namespace: RelayManager.namespace_of_class(class), cell_id: cell_id}},
        else: {:error, {:no_persisted_state, path}}
    end
  end

  @doc false
  # A `--from` or a time-travel target: a WAL seq, or an ISO 8601 time.
  # `option` names the argument in a refusal.
  @spec target(String.t(), String.t()) ::
          {:ok, {:seq, non_neg_integer()} | {:timestamp, integer()}} | {:error, Flezha.Error.t()}
  def target(written, option) do
    case Integer.parse(written) do
      {n, ""} when n >= 0 ->
        {:ok, {:seq, n}}

      {n, ""} ->
        {:error, Flezha.Error.new("usage.negative", option: option, value: n)}

      _ ->
        case DateTime.from_iso8601(written) do
          {:ok, dt, _} -> {:ok, {:timestamp, DateTime.to_unix(dt)}}
          _ -> invalid_number(option, written)
        end
    end
  end

  @doc false
  @spec invalid_number(String.t(), term()) :: {:error, Flezha.Error.t()}
  def invalid_number(option, written),
    do:
      {:error, Flezha.Error.new("usage.invalid_number", option: option, value: inspect(written))}

  defp persisted_cell_row([path | _]), do: persisted_cell(path)

  # The path split, with no on-disk check, so deleting a Cell that left nothing
  # behind still succeeds.
  defp cell_path_row([path | _]) do
    with {:ok, {class, _, _} = cell_id} <- split_cell(path),
         do: {:ok, %{namespace: RelayManager.namespace_of_class(class), cell_id: cell_id}}
  end

  defp class_row([relay_class | _]),
    do: {:ok, %{namespace: RelayManager.namespace_of_class(relay_class)}}

  # The live Relay's namespace, and the members that hold the key: a
  # partition group id names no relay — it names N — so a keyed read
  # resolves the one member owning the key's group, and a tasklet-level read
  # fans out over every member.
  defp live_row([path | _]) do
    with {:ok, {relay_id, tasklet, cell_key}} <- split_live(path),
         {:ok, namespace, targets} <- state_targets(relay_id, cell_key) do
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
      [relay, tasklet, key] when relay != "" and tasklet != "" ->
        {:ok, {relay, tasklet, key}}

      [relay, tasklet] when relay != "" and tasklet != "" ->
        {:ok, {relay, tasklet, nil}}

      _ ->
        {:error, {:bad_live_path, path}}
    end
  end

  defp persisted?(cell_id), do: cell_id in Paths.all_cells()

  defp hydrated?(class, tasklet, key) do
    DurabilityTier.hydrate_cells(class, tasklet, [key])
    persisted?({class, tasklet, key})
  end

  defp state_targets(relay_id, cell_key) do
    case RelayManager.namespace_of(relay_id) do
      {:ok, namespace} ->
        {:ok, namespace, [relay_id]}

      :error ->
        with {:ok, namespace} <- group_namespace(relay_id) do
          case RelayManager.resolve_member(namespace, relay_id, cell_key) do
            {:ok, member} -> {:ok, namespace, [member]}
            {:error, {:unkeyed, _group, members}} -> {:ok, namespace, members}
            {:error, _gone} -> {:error, {:no_relay, relay_id}}
          end
        end
    end
  end

  defp group_namespace(relay_id) do
    case RelayManager.namespace_of_group(relay_id) do
      {:ok, namespace} -> {:ok, namespace}
      :error -> {:error, {:no_relay, relay_id}}
    end
  end

  defp read_cell_state(namespace, [relay_id], tasklet, cell_key),
    do:
      RelayManager.get_cell_state(namespace, relay_id, %{
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
        {:error, {:no_relay, hd(targets)}}

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

  # The ceiling the steps handoff uses: a whole-tasklet read of a big relay
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
    do: {:ok, %{namespace: RelayManager.namespace_of_class(instance_id)}}
end
