defmodule Flezha.Service.Operator do
  @moduledoc """
  Published service-API surface for namespaces, operators and their
  partitions: every public function takes a `Flezha.Service.CallerContext` as
  its last argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  `Flezha.Service.OperatorManager` is the internal manager and stays internal;
  this is the half a consumer is allowed to see. They are two modules rather
  than one because the manager is within a hundred lines of the repository's
  800-line file ceiling, which the charter says wins over collocation.

  One question is asked one way here. `list_namespaces/1` is the only
  spelling a consumer has for "which namespaces are running": the reads below
  it take no caller and answer the whole cluster, which is what the
  controller's own machinery needs and what a tenant-bound caller must not be
  handed.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.Namespace
  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Partition
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc """
  The namespaces this cluster is running that the caller may be shown: its
  own, or all of them for an admin in `_system`.
  """
  @spec list_namespaces(caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb list_namespaces(caller_context) do
    # `unbound: :all` for the reason `Cli.Listing` gives: the row is
    # `:caller`-scoped, so a context with no namespace is an ungated
    # deployment's user rather than a tenant.
    {:ok, Namespace.visible(HubManager.get_running_namespaces(), caller_context, :all)}
  end

  @doc "The operator ids running in `namespace`."
  @spec list_operators(String.t(), caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb list_operators(namespace, caller_context) do
    OperatorManager.get_operator_ids(namespace)
  end

  @doc "One row per operator in `namespace`: node, health and partition."
  @spec operator_rows(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb operator_rows(namespace, caller_context), verb: :operator_info do
    {:ok, HubManager.operator_rows(namespace)}
  end

  @doc "The plugin bindings each operator in `namespace` declares."
  @spec operator_plugins(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb operator_plugins(namespace, caller_context), verb: :operator_info do
    {:ok, HubManager.operator_bindings(namespace)}
  end

  @doc "The namespace's hub registry, as the hub reports it."
  @spec registry(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb registry(namespace, caller_context), verb: :operator_info do
    HubManager.get_registry(namespace)
  end

  @doc "Partition coverage over `{id, range}` pairs: gaps, overlaps and total."
  @spec partition_coverage([{String.t(), term()}], caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb partition_coverage(members, caller_context) when is_list(members),
    verb: :partition_report do
    {:ok, Partition.coverage(members)}
  end

  @doc """
  The partition groups `rows` describe: group, declared width, members and
  key-group coverage.

  The report, not the row. `Flezha.Service.CliManager.group_partition_rows/1`
  collapses the same rows into one line each and formats their runners into a
  string for a terminal to print; a caller that is not a terminal wants the
  members.
  """
  @spec partition_groups([map()], caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb partition_groups(rows, caller_context) when is_list(rows), verb: :partition_report do
    {:ok, Partition.groups(rows)}
  end

  @doc "Whether `run` is a way an operator runs that this controller knows."
  @spec run_known?(term(), caller_context()) :: {:ok, boolean()} | {:error, term()}
  verb run_known?(run, caller_context), verb: :run_read do
    {:ok, ClassRegistry.known_run?(run)}
  end

  @doc """
  One Cell's live state and the change sequence it was read at.

  The sequence is part of the answer because a caller that renders state next
  to a position needs both, and reading them separately cannot pair them.
  """
  @spec cell_state(String.t(), String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb cell_state(namespace, operator_id, cell_key, caller_context) do
    OperatorManager.get_cell_state(namespace, operator_id, cell_key)
  end

  @doc "One operator's Overwatch state."
  @spec overwatch_state(String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb overwatch_state(namespace, operator_id, caller_context), verb: :operator_info do
    OperatorManager.get_overwatch_state(namespace, operator_id)
  end

  # ---------- Mutations ----------

  @doc """
  Start `operator` in `namespace`, answering the id it runs under and whether
  this call started it (`:started`) or found a live instance already holding
  the id it named (`:already_running`).

  `operator` is a `Flezha.Operator.Builder` or the named fields to build one
  from, so a consumer that reached the controller over a wire never has to put
  a struct on it. The same affordance `Flezha.Service.PluginManager.enable/3`
  gives for a plugin. A spec without an id is given a generated one.
  """
  @spec start(String.t(), Builder.t() | keyword(), caller_context()) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  verb start(namespace, operator, caller_context),
    verb: :operator_start,
    target: &OperatorManager.start_target/2 do
    with {:ok, spec} <- Builder.build(operator) do
      namespace
      |> OperatorManager.start_operator(spec)
      |> OperatorManager.start_answer(spec.id)
    end
  end

  @doc "Stop the operator `operator_id` in `namespace`."
  @spec stop(String.t(), String.t(), caller_context()) :: term() | {:error, term()}
  verb stop(namespace, operator_id, caller_context), verb: :operator_stop do
    OperatorManager.stop_operator(namespace, operator_id)
  end

  @doc """
  Inject `message` into `operator_id`'s input stream, blocking for the
  confirming verdict so a parked or absent target is reported rather than
  silently dropped.

  `channel` names the input channel; `""` is none. The verdict is the
  operator's own: `:ok`, `{:ok, :absorbed}` for a duplicate,
  `{:ok, :suspended}` for one journaled until the instance wakes.
  """
  @spec send(String.t(), String.t(), binary(), String.t(), caller_context()) ::
          term() | {:error, term()}
  verb send(namespace, operator_id, message, channel, caller_context), verb: :operator_send do
    OperatorManager.send_io_confirming(
      operator_id,
      namespace,
      message,
      OperatorManager.channel_metadata(channel)
    )
  end

  @doc """
  Call `operator_id` with `message` and wait for its answer: the one output
  a `response()` end produced from it, as `{:ok, payload, %{"channel" =>
  channel}}`, or a coded error saying why none came.

  `opts` carries `deadline_ms` (required, a positive integer) and `key`, the
  key whose member of a partition group takes the input. The call is judged
  as a send: its act is an input, and its answer is the sender's own.
  """
  @spec call(String.t(), String.t(), binary(), String.t(), keyword(), caller_context()) ::
          {:ok, binary(), map()} | {:error, term()}
  verb call(namespace, operator_id, message, channel, opts, caller_context),
    verb: :operator_call do
    OperatorManager.call_operator(
      namespace,
      operator_id,
      message,
      OperatorManager.channel_metadata(channel),
      Keyword.fetch!(opts, :deadline_ms),
      Keyword.get(opts, :key)
    )
  end
end
