defmodule Flezha.Service.Fleet do
  @moduledoc """
  Published service-API surface for the runner fleet and the cluster's
  directory: which runners a namespace has, which factories, which nodes hold
  its hub, and what is installed.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument and answers data.

  The two runner reads are named so they cannot be confused, because the console
  takes both today and the answers differ: `runners/2` is **cluster-wide** — it
  fans out across this node and every node the directory lists for the namespace
  — while `runners_on_this_node/2` is **node-local**. Two screens took the
  node-local answer while believing it was the fleet's.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Plugin.Installer
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @doc "Every runner serving `namespace`, anywhere in the cluster."
  @spec runners(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  def runners(namespace, caller_context) do
    fleet_read(caller_context, namespace, fn ->
      {:ok, FleetManager.global_runners(namespace)}
    end)
  end

  @doc """
  The runners serving `namespace` **on this node only**.

  For a node-scoped view. A caller asking "what runners does this namespace
  have" wants `runners/2`.
  """
  @spec runners_on_this_node(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  def runners_on_this_node(namespace, caller_context) do
    fleet_read(caller_context, namespace, fn ->
      {:ok, FleetManager.active_runners(namespace)}
    end)
  end

  @doc "The runners of one factory in `namespace`."
  @spec runners_for_factory(String.t(), String.t(), caller_context()) ::
          {:ok, [term()]} | {:error, term()}
  def runners_for_factory(namespace, factory_id, caller_context) do
    fleet_read(caller_context, namespace, fn ->
      {:ok, FleetManager.runners_for_factory(namespace, factory_id)}
    end)
  end

  @doc "The factories declared in `namespace`."
  @spec factories(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  def factories(namespace, caller_context) do
    fleet_read(caller_context, namespace, fn ->
      {:ok, FleetManager.list_factories(namespace)}
    end)
  end

  @doc "The caller context a runner in `namespace` connects with."
  @spec runner_caller_context(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  def runner_caller_context(namespace, caller_context) do
    fleet_read(caller_context, namespace, fn ->
      FleetManager.runner_caller_context(namespace)
    end)
  end

  @doc "The nodes holding a hub for `namespace`."
  @spec hub_nodes(String.t(), caller_context()) :: {:ok, [node()]} | {:error, term()}
  def hub_nodes(namespace, caller_context) do
    fleet_read(caller_context, namespace, fn ->
      {:ok, Directory.hub_nodes(namespace)}
    end)
  end

  @doc "Every plugin package installed on this controller."
  @spec installed_plugins(caller_context()) :: {:ok, [term()]} | {:error, term()}
  def installed_plugins(caller_context) do
    CallerContext.scoped_read(caller_context, :"plugin.list", CallerContext.system_scope(), fn ->
      {:ok, Installer.list()}
    end)
  end

  # A fleet read is an operative-topology read, scoped to the namespace asked
  # about rather than to one the caller declares.
  defp fleet_read(caller_context, namespace, fun) do
    CallerContext.scoped_read(caller_context, :"operative.list", namespace, fun)
  end
end
