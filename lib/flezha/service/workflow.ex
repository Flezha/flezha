defmodule Flezha.Service.Workflow do
  @moduledoc """
  Published service-API surface for workflow instances: the proc journal, the
  proc inbox, completion, retention, time travel and the two verbs that move an
  instance — rescale and rewind.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument and answers data.

  `rescale/5` and `rewind/4` move live work, so each carries an action of its
  own — `operative.scale` and `operative.rewind`, both the operator's, both
  judged in the namespace the caller names rather than one the context carries.
  They sit beside `operative.stop` in the grant table because they are the same
  kind of intervention on a running operative.

  `rewind/4` takes an operative rather than a pid. The console resolves a pid
  and then rewinds it, which puts a process identifier on the wire and makes the
  caller do the lookup; a published surface names what is being rewound.
  """

  alias Flezha.Operative.Migration
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Metrics
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias Flezha.State.TimeTravel

  @audit_source :workflow_service

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "Every operative class this controller holds persisted state for."
  @spec operative_classes(caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  def operative_classes(caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, Paths.operative_classes()} end)
  end

  @doc "One workflow instance's completion record."
  @spec completion(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  def completion(instance_id, caller_context) do
    CallerContext.validated(caller_context, fn -> Completion.read(instance_id) end)
  end

  @doc """
  How long a completed instance of `operative_class` is retained, in hours.

  `metadata` is the instance's own, which takes precedence over the global
  default and is overridden in turn by an operator setting.
  """
  @spec completed_retention_hours(String.t(), map(), caller_context()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def completed_retention_hours(operative_class, metadata, caller_context) do
    CallerContext.validated(caller_context, fn ->
      {:ok, PersistenceResolver.completed_retention_hours(operative_class, metadata)}
    end)
  end

  @doc "One instance's proc-journal entries."
  @spec journal(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  def journal(instance_id, caller_context) do
    CallerContext.validated(caller_context, fn -> ProcJournal.read(instance_id) end)
  end

  @doc "The iterations the instance's journal has committed."
  @spec committed_iterations(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  def committed_iterations(instance_id, caller_context) do
    CallerContext.validated(caller_context, fn ->
      {:ok, ProcJournal.committed_iterations(instance_id)}
    end)
  end

  @doc "The journal offset the instance has consumed up to."
  @spec consumed_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  def consumed_offset(instance_id, caller_context) do
    CallerContext.validated(caller_context, fn ->
      {:ok, ProcJournal.consumed_offset(instance_id)}
    end)
  end

  @doc "The inbox messages the instance has not taken yet."
  @spec undelivered(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  def undelivered(instance_id, caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, ProcInbox.undelivered(instance_id)} end)
  end

  @doc "The last offset written to the instance's inbox."
  @spec last_offset(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  def last_offset(instance_id, caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, ProcInbox.last_offset(instance_id)} end)
  end

  @doc "Durable-state counters across every operative class."
  @spec state_metrics(caller_context()) :: {:ok, term()} | {:error, term()}
  def state_metrics(caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, Metrics.snapshot_all()} end)
  end

  @doc "One Entity's state as of `target`, replayed from its write-ahead log."
  @spec time_travel(String.t(), term(), caller_context()) :: {:ok, term()} | {:error, term()}
  def time_travel(entity_id, target, caller_context) do
    CallerContext.validated(caller_context, fn -> TimeTravel.query(entity_id, target) end)
  end

  # ---------- Mutations ----------

  @doc "Rescale the partition group `group_id` in `namespace` to `target`."
  @spec rescale(String.t(), String.t(), pos_integer(), keyword(), caller_context()) ::
          term() | {:error, term()}
  def rescale(namespace, group_id, target, opts, caller_context) do
    CallerContext.gated(
      caller_context,
      :"operative.scale",
      namespace,
      @audit_source,
      %{group: group_id, partitions: target},
      fn -> PartitionGroup.rescale(namespace, group_id, target, opts) end
    )
  end

  @doc """
  Rewind `operative_class` in `namespace` to the committed iteration `to_iter`.

  Answers `{:error, :operative_not_running}` rather than resolving a pid for the
  caller to carry, and `{:error, {:uncommitted_iteration, committed}}` when
  `to_iter` is not one of the iterations the instance has committed.
  """
  @spec rewind(String.t(), String.t(), non_neg_integer(), caller_context()) ::
          :ok | {:error, term()}
  def rewind(namespace, operative_class, to_iter, caller_context) do
    CallerContext.gated(
      caller_context,
      :"operative.rewind",
      namespace,
      @audit_source,
      %{operative: operative_class, to_iteration: to_iter},
      fn ->
        case OperativeManager.get_operative_pid(namespace, operative_class) do
          {:ok, pid} -> Migration.rewind(pid, to_iter)
          _other -> {:error, :operative_not_running}
        end
      end
    )
  end
end
