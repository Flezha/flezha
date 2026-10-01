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

  alias Flezha.Operator.Migration
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion
  alias Flezha.State.InputLog
  alias Flezha.State.Metrics
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.ScopePaths
  alias Flezha.State.TaskJournal
  alias Flezha.State.TimeTravel

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "Every operator class this controller holds persisted state for."
  @spec operator_classes(caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb operator_classes(caller_context) do
    {:ok, Paths.operator_classes()}
  end

  @doc "One steps instance's completion record."
  @spec completion(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb completion(instance_id, caller_context), verb: :state_journal, row: &instance_row/1 do
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
    verb: :retention_default do
    {:ok, PersistenceResolver.completed_retention_hours(operator_class, metadata)}
  end

  @doc "One instance's task-journal entries."
  @spec journal(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb journal(instance_id, caller_context), verb: :state_journal, row: &instance_row/1 do
    TaskJournal.read(instance_id)
  end

  @doc "The iterations the instance's journal has committed."
  @spec committed_iterations(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb committed_iterations(instance_id, caller_context),
    verb: :state_journal,
    row: &instance_row/1 do
    {:ok, TaskJournal.committed_iterations(instance_id)}
  end

  @doc "The input-log offset the instance's last iteration consumed, `nil` for none."
  @spec consumed_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb consumed_offset(instance_id, caller_context), verb: :state_journal, row: &instance_row/1 do
    {:ok, TaskJournal.consumed_offset(instance_id)}
  end

  @doc "The input-log records above the instance's floor: what it has not consumed yet."
  @spec undelivered(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb undelivered(instance_id, caller_context), verb: :state_inbox, row: &instance_row/1 do
    with records when is_list(records) <- InputLog.undelivered(instance_id) do
      {:ok,
       Enum.map(records, fn {offset, message, metadata} ->
         %{offset: offset, message: message, metadata: metadata}
       end)}
    end
  end

  @doc "The last offset written to the instance's input log, `-1` when it has none."
  @spec last_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb last_offset(instance_id, caller_context), verb: :state_inbox, row: &instance_row/1 do
    with {:ok, _floor, last} <- InputLog.tail(instance_id), do: {:ok, last}
  end

  @doc "Durable-state counters across every operator class."
  @spec state_metrics(caller_context()) :: {:ok, term()} | {:error, term()}
  verb state_metrics(caller_context) do
    {:ok, Metrics.snapshot_all()}
  end

  @doc "One Cell's state as of `target`, replayed from its write-ahead log."
  @spec time_travel(String.t(), term(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb time_travel(cell_id, target, caller_context), verb: :state_at, row: &cell_row/1 do
    TimeTravel.query(cell_id, target)
  end

  # ---------- Mutations ----------

  @doc "Rescale the partition group `group_id` in `namespace` to `target`."
  @spec rescale(String.t(), String.t(), pos_integer(), keyword(), caller_context()) ::
          term() | {:error, term()}
  verb rescale(namespace, group_id, target, opts, caller_context), verb: :operator_scale do
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
  verb rewind(namespace, operator_class, to_iter, caller_context), verb: :operator_rewind do
    rewind_instance(namespace, operator_class, to_iter)
  end

  @doc false
  # The effect of a rewind, shared with the terminal.
  @spec rewind_instance(String.t(), String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind_instance(namespace, operator_class, to_iter) do
    # A stepped scope `<outer>/<inner>/<key>` is rewound by its outer instance.
    {instance, to} =
      case ScopePaths.split(operator_class) do
        {outer, scope} -> {outer, {scope, to_iter}}
        :error -> {operator_class, to_iter}
      end

    case OperatorManager.get_operator_pid(namespace, instance) do
      {:ok, pid} -> Migration.rewind(pid, to)
      _other -> {:error, {:operator_not_running, operator_class}}
    end
  end

  # An instance's namespace is its class's: the registry's answer while it
  # runs, the state root's afterwards.
  defp instance_row([instance_id | _]) when is_binary(instance_id),
    do: {:ok, %{namespace: OperatorManager.namespace_of_class(instance_id)}}

  defp cell_row([{operator_class, _tasklet, _key} | _]),
    do: {:ok, %{namespace: OperatorManager.namespace_of_class(operator_class)}}
end
