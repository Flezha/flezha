defmodule Flezha.Service.Operative do
  @moduledoc """
  Published service-API surface for namespaces, operatives and their
  partitions: every public function takes a `Flezha.Service.CallerContext` as
  its last argument, authorizes through it, and answers data.

  `Flezha.Service.OperativeManager` is the internal manager and stays internal;
  this is the half a consumer is allowed to see. They are two modules rather
  than one because the manager is within a hundred lines of the repository's
  800-line file ceiling, which the charter says wins over collocation.

  One question is asked one way here. The console asks for the running
  namespaces through both `Flezha.list_namespaces/0` and
  `Flezha.Service.HubManager.get_running_namespaces/0`, which are the same
  function under two names; `list_namespaces/1` is the only spelling on this
  surface.
  """

  alias Flezha.Operative.Kind
  alias Flezha.Partition
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager

  @audit_source :operative_service

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc "The namespaces this cluster is running."
  @spec list_namespaces(caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  def list_namespaces(caller_context) do
    operative_read(caller_context, CallerContext.system_scope(), fn ->
      {:ok, HubManager.get_running_namespaces()}
    end)
  end

  @doc "The operative ids running in `namespace`."
  @spec list_operatives(String.t(), caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  def list_operatives(namespace, caller_context) do
    operative_read(caller_context, namespace, fn ->
      {:ok, OperativeManager.get_operative_ids(namespace)}
    end)
  end

  @doc "One row per operative in `namespace`: node, health and partition."
  @spec operative_rows(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  def operative_rows(namespace, caller_context) do
    operative_read(caller_context, namespace, fn ->
      {:ok, HubManager.operative_rows(namespace)}
    end)
  end

  @doc "The plugins declared by each operative in `namespace`."
  @spec operative_plugins(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  def operative_plugins(namespace, caller_context) do
    operative_read(caller_context, namespace, fn ->
      {:ok, HubManager.operative_plugins(namespace)}
    end)
  end

  @doc "The namespace's hub registry, as the hub reports it."
  @spec registry(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  def registry(namespace, caller_context) do
    operative_read(caller_context, namespace, fn ->
      HubManager.get_registry(namespace)
    end)
  end

  @doc "Partition coverage over `{id, range}` pairs: gaps, overlaps and total."
  @spec partition_coverage([{String.t(), term()}], caller_context()) ::
          {:ok, term()} | {:error, term()}
  def partition_coverage(members, caller_context) when is_list(members) do
    CallerContext.validated(caller_context, fn -> {:ok, Partition.coverage(members)} end)
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
  def partition_groups(rows, caller_context) when is_list(rows) do
    CallerContext.validated(caller_context, fn -> {:ok, Partition.groups(rows)} end)
  end

  @doc "Whether `kind` is an operative kind this controller knows."
  @spec kind_known?(term(), caller_context()) :: {:ok, boolean()} | {:error, term()}
  def kind_known?(kind, caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, Kind.known?(kind)} end)
  end

  @doc "The capability flags an operative kind carries."
  @spec kind_capabilities(term(), caller_context()) :: {:ok, map()} | {:error, term()}
  def kind_capabilities(kind, caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, Kind.capabilities(kind)} end)
  end

  @doc """
  One Entity's live state and the change sequence it was read at.

  The sequence is part of the answer because a caller that renders state next
  to a position needs both, and reading them separately cannot pair them.
  """
  @spec entity_state(String.t(), String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  def entity_state(namespace, operative_id, entity_key, caller_context) do
    CallerContext.scoped_read(caller_context, :"state.get", namespace, fn ->
      OperativeManager.get_entity_state(namespace, operative_id, entity_key)
    end)
  end

  @doc "One operative's Overwatch state."
  @spec overwatch_state(String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  def overwatch_state(namespace, operative_id, caller_context) do
    operative_read(caller_context, namespace, fn ->
      OperativeManager.get_overwatch_state(namespace, operative_id)
    end)
  end

  # ---------- Mutations ----------

  @doc "Start `operative` in `namespace`."
  @spec start(String.t(), term(), caller_context()) :: term() | {:error, term()}
  def start(namespace, operative, caller_context) do
    CallerContext.gated(
      caller_context,
      :"operative.start",
      namespace,
      @audit_source,
      %{operative: inspect(operative)},
      fn -> OperativeManager.start_operative(namespace, operative) end
    )
  end

  @doc "Stop the operative `operative_id` in `namespace`."
  @spec stop(String.t(), String.t(), caller_context()) :: term() | {:error, term()}
  def stop(namespace, operative_id, caller_context) do
    CallerContext.gated(
      caller_context,
      :"operative.stop",
      namespace,
      @audit_source,
      %{operative: operative_id},
      fn -> OperativeManager.stop_operative(namespace, operative_id) end
    )
  end

  defp operative_read(caller_context, namespace, fun) do
    CallerContext.scoped_read(caller_context, :"operative.list", namespace, fun)
  end
end
