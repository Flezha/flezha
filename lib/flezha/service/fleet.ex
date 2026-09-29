defmodule Flezha.Service.Fleet do
  @moduledoc """
  Published service-API surface for the runner fleet and the cluster's
  directory: which runners a namespace has, which factories, which nodes hold
  its hub, and what is installed.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data. A fleet read
  is an operator-topology read — the row `flz runner list` binds — scoped to
  the namespace asked about rather than to one the caller declares.

  The two runner reads are named so they cannot be confused, because the console
  takes both today and the answers differ: `runners/2` is **cluster-wide** — it
  fans out across this node and every node the directory lists for the namespace
  — while `runners_on_this_node/2` is **node-local**. Two screens took the
  node-local answer while believing it was the fleet's.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Cluster.Directory
  alias Flezha.Plugin.Installer
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @doc "Every runner serving `namespace`, anywhere in the cluster."
  @spec runners(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb runners(namespace, caller_context), verb: :list_runners do
    {:ok, FleetManager.global_runners(namespace)}
  end

  @doc """
  The runners serving `namespace` **on this node only**.

  For a node-scoped view. A caller asking "what runners does this namespace
  have" wants `runners/2`.
  """
  @spec runners_on_this_node(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb runners_on_this_node(namespace, caller_context), verb: :list_runners do
    {:ok, FleetManager.active_runners(namespace)}
  end

  @doc "The runners of one factory in `namespace`."
  @spec runners_for_factory(String.t(), String.t(), caller_context()) ::
          {:ok, [term()]} | {:error, term()}
  verb runners_for_factory(namespace, factory_id, caller_context), verb: :list_runners do
    {:ok, FleetManager.runners_for_factory(namespace, factory_id)}
  end

  @doc "The factories declared in `namespace`."
  @spec factories(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb factories(namespace, caller_context), verb: :list_runners do
    {:ok, FleetManager.list_factories(namespace)}
  end

  @doc "The caller context a runner in `namespace` connects with."
  @spec runner_caller_context(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb runner_caller_context(namespace, caller_context), verb: :list_runners do
    FleetManager.runner_caller_context(namespace)
  end

  @doc "The nodes holding a hub for `namespace`."
  @spec hub_nodes(String.t(), caller_context()) :: {:ok, [node()]} | {:error, term()}
  verb hub_nodes(namespace, caller_context), verb: :list_runners do
    {:ok, Directory.hub_nodes(namespace)}
  end

  @doc "Every plugin package installed on this controller."
  @spec installed_plugins(caller_context()) ::
          {:ok, [Flezha.Plugin.Installer.InstalledPlugin.t()]} | {:error, term()}
  verb installed_plugins(caller_context), verb: :list_installed_plugins do
    {:ok, Installer.list()}
  end
end
