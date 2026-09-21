defmodule Flezha.Service.Operative do
  @moduledoc """
  Published service-API surface for namespaces, operatives and their
  partitions: every public function takes a `Flezha.Service.CallerContext` as
  its last argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  `Flezha.Service.OperativeManager` is the internal manager and stays internal;
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
  alias Flezha.Operative.Builder
  alias Flezha.Operative.Kind
  alias Flezha.Partition
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager

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
    # deployment's operator rather than a tenant.
    {:ok, Namespace.visible(HubManager.get_running_namespaces(), caller_context, :all)}
  end

  @doc "The operative ids running in `namespace`."
  @spec list_operatives(String.t(), caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb list_operatives(namespace, caller_context) do
    OperativeManager.get_operative_ids(namespace)
  end

  @doc "One row per operative in `namespace`: node, health and partition."
  @spec operative_rows(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb operative_rows(namespace, caller_context), verb: :operative_info do
    {:ok, HubManager.operative_rows(namespace)}
  end

  @doc "The plugins declared by each operative in `namespace`."
  @spec operative_plugins(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb operative_plugins(namespace, caller_context), verb: :operative_info do
    {:ok, HubManager.operative_plugins(namespace)}
  end

  @doc "The namespace's hub registry, as the hub reports it."
  @spec registry(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb registry(namespace, caller_context), verb: :operative_info do
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

  @doc "Whether `kind` is an operative kind this controller knows."
  @spec kind_known?(term(), caller_context()) :: {:ok, boolean()} | {:error, term()}
  verb kind_known?(kind, caller_context), verb: :kind_read do
    {:ok, Kind.known?(kind)}
  end

  @doc "The capability flags an operative kind carries."
  @spec kind_capabilities(term(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb kind_capabilities(kind, caller_context), verb: :kind_read do
    {:ok, Kind.capabilities(kind)}
  end

  @doc """
  One Cell's live state and the change sequence it was read at.

  The sequence is part of the answer because a caller that renders state next
  to a position needs both, and reading them separately cannot pair them.
  """
  @spec cell_state(String.t(), String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb cell_state(namespace, operative_id, cell_key, caller_context) do
    OperativeManager.get_cell_state(namespace, operative_id, cell_key)
  end

  @doc "One operative's Overwatch state."
  @spec overwatch_state(String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb overwatch_state(namespace, operative_id, caller_context), verb: :operative_info do
    OperativeManager.get_overwatch_state(namespace, operative_id)
  end

  # ---------- Mutations ----------

  @doc """
  Start `operative` in `namespace`, answering the id it runs under and whether
  this call started it (`:started`) or found a live instance already holding
  the id it named (`:already_running`).

  `operative` is a `Flezha.Operative.Builder` or the named fields to build one
  from, so a consumer that reached the controller over a wire never has to put
  a struct on it. The same affordance `Flezha.Service.PluginManager.enable/3`
  gives for a plugin. A spec without an id is given a generated one.
  """
  @spec start(String.t(), Builder.t() | keyword(), caller_context()) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  verb start(namespace, operative, caller_context),
    verb: :operative_start,
    target: &OperativeManager.start_target/2 do
    with {:ok, spec} <- Builder.build(operative) do
      namespace
      |> OperativeManager.start_operative(spec)
      |> OperativeManager.start_answer(spec.id)
    end
  end

  @doc "Stop the operative `operative_id` in `namespace`."
  @spec stop(String.t(), String.t(), caller_context()) :: term() | {:error, term()}
  verb stop(namespace, operative_id, caller_context), verb: :operative_stop do
    OperativeManager.stop_operative(namespace, operative_id)
  end

  @doc """
  Inject `message` into `operative_id`'s input stream, blocking for the
  confirming verdict so a parked or absent target is reported rather than
  silently dropped.

  `channel` names the input channel; `""` is none. The verdict is the
  operative's own: `:ok`, `{:ok, :absorbed}` for a duplicate,
  `{:ok, :suspended}` for one journaled until the instance wakes.
  """
  @spec send(String.t(), String.t(), binary(), String.t(), caller_context()) ::
          term() | {:error, term()}
  verb send(namespace, operative_id, message, channel, caller_context), verb: :operative_send do
    OperativeManager.send_io_confirming(
      operative_id,
      namespace,
      message,
      channel_metadata(channel)
    )
  end

  defp channel_metadata(channel) when channel in [nil, ""], do: %{}
  defp channel_metadata(channel), do: %{"channel" => channel}
end
