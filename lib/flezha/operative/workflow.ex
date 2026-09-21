defmodule Flezha.Service.Workflow do
  @moduledoc """
  Published service-API surface for workflow instances: the task journal, the
  task inbox, completion, retention, time travel and the two verbs that move an
  instance — rescale and rewind.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  The journal, the inbox and the completion record are records in flight —
  the same kind of data the dead-letter queue holds — so they carry
  `state.records`, judged in the instance's own namespace, and a point-in-time
  read carries `state.at`. `rescale/5` and `rewind/4` move live work, so each
  carries an action of its own — `operative.scale` and `operative.rewind`,
  both the operator's, both judged in the namespace the caller names.

  `rewind/4` takes an operative rather than a pid. The console resolves a pid
  and then rewinds it, which puts a process identifier on the wire and makes the
  caller do the lookup; a published surface names what is being rewound.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Operative.Migration
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Metrics
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.TaskInbox
  alias Flezha.State.TaskJournal
  alias Flezha.State.TimeTravel

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "Every operative class this controller holds persisted state for."
  @spec operative_classes(caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb operative_classes(caller_context) do
    {:ok, Paths.operative_classes()}
  end

  @doc "One workflow instance's completion record."
  @spec completion(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb completion(instance_id, caller_context), verb: :state_journal, row: &instance_row/1 do
    Completion.read(instance_id)
  end

  @doc """
  How long a completed instance of `operative_class` is retained, in hours.

  `metadata` is the instance's own, which takes precedence over the global
  default and is overridden in turn by an operator setting.
  """
  @spec completed_retention_hours(String.t(), map(), caller_context()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  verb completed_retention_hours(operative_class, metadata, caller_context),
    verb: :retention_default do
    {:ok, PersistenceResolver.completed_retention_hours(operative_class, metadata)}
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

  @doc "The journal offset the instance has consumed up to."
  @spec consumed_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb consumed_offset(instance_id, caller_context), verb: :state_journal, row: &instance_row/1 do
    {:ok, TaskJournal.consumed_offset(instance_id)}
  end

  @doc "The inbox messages the instance has not taken yet."
  @spec undelivered(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb undelivered(instance_id, caller_context), verb: :state_inbox, row: &instance_row/1 do
    {:ok, TaskInbox.undelivered(instance_id)}
  end

  @doc "The last offset written to the instance's inbox."
  @spec last_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb last_offset(instance_id, caller_context), verb: :state_inbox, row: &instance_row/1 do
    {:ok, TaskInbox.last_offset(instance_id)}
  end

  @doc "Durable-state counters across every operative class."
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
  verb rescale(namespace, group_id, target, opts, caller_context), verb: :operative_scale do
    PartitionGroup.rescale(namespace, group_id, target, opts)
  end

  @doc """
  Rewind `operative_class` in `namespace` to the committed iteration `to_iter`.

  Answers `{:error, :operative_not_running}` rather than resolving a pid for the
  caller to carry, and `{:error, {:uncommitted_iteration, committed}}` when
  `to_iter` is not one of the iterations the instance has committed.
  """
  @spec rewind(String.t(), String.t(), non_neg_integer(), caller_context()) ::
          :ok | {:error, term()}
  verb rewind(namespace, operative_class, to_iter, caller_context), verb: :operative_rewind do
    rewind_instance(namespace, operative_class, to_iter)
  end

  @doc false
  # The effect of a rewind, shared with the terminal.
  @spec rewind_instance(String.t(), String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def rewind_instance(namespace, operative_class, to_iter) do
    case OperativeManager.get_operative_pid(namespace, operative_class) do
      {:ok, pid} -> Migration.rewind(pid, to_iter)
      _other -> {:error, :operative_not_running}
    end
  end

  # An instance's namespace is its class's: the registry's answer while it
  # runs, the state root's afterwards.
  defp instance_row([instance_id | _]) when is_binary(instance_id),
    do: {:ok, %{namespace: OperativeManager.namespace_of_class(instance_id)}}

  defp cell_row([{operative_class, _tasklet, _key} | _]),
    do: {:ok, %{namespace: OperativeManager.namespace_of_class(operative_class)}}
end
