defmodule Flezha.Service.Fleet do
  @moduledoc """
  Published service-API surface for the runner fleet and the cluster's
  directory: which runners a namespace has, which factories, which nodes hold
  its hub, and what is installed.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data. A fleet read
  is a relay-topology read — the row `flz runner list` binds — scoped to
  the namespace asked about rather than to one the caller declares.

  The two runner reads are named so they cannot be confused, because the console
  takes both today and the answers differ: `runners/2` is **cluster-wide** — it
  fans out across this node and every node the directory lists for the namespace
  — while `runners_on_this_node/2` is **node-local**. Two screens took the
  node-local answer while believing it was the fleet's.
  """

  use Flezha.AuthZ.Verb

  @namespace [type: :string, doc: "The namespace."]

  alias Flezha.Cluster.Directory
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Providers
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @doc """
  Every runner serving `namespace`, anywhere in the cluster, each with the
  process id of the runner on its socket where the transport can say, so a
  user can tell which process a runner is.
  """
  @spec runners(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb runners(namespace, caller_context), verb: :list_runners, args: [namespace: @namespace] do
    with {:ok, rows} <- FleetManager.global_runner_pids(namespace),
         do: {:ok, Enum.map(rows, fn {id, pid} -> %{id: id, pid: pid} end)}
  end

  @doc """
  The runners serving `namespace` **on this node only**.

  For a node-scoped view. A caller asking "what runners does this namespace
  have" wants `runners/2`.
  """
  @spec runners_on_this_node(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb runners_on_this_node(namespace, caller_context),
    verb: :list_runners,
    args: [namespace: @namespace] do
    {:ok, FleetManager.active_runners(namespace)}
  end

  @doc "The runners of one factory in `namespace`."
  @spec runners_for_factory(String.t(), String.t(), caller_context()) ::
          {:ok, [term()]} | {:error, term()}
  verb runners_for_factory(namespace, factory_id, caller_context),
    verb: :list_runners,
    args: [namespace: @namespace, factory_id: [type: :string, doc: "The factory."]] do
    {:ok, FleetManager.runners_for_factory(namespace, factory_id)}
  end

  @doc "The factories declared in `namespace`."
  @spec factories(String.t(), caller_context()) :: {:ok, [term()]} | {:error, term()}
  verb factories(namespace, caller_context), verb: :list_runners, args: [namespace: @namespace] do
    {:ok, FleetManager.list_factories(namespace)}
  end

  @doc "The caller context a runner in `namespace` connects with."
  @spec runner_caller_context(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb runner_caller_context(namespace, caller_context),
    verb: :list_runners,
    args: [namespace: @namespace] do
    FleetManager.runner_caller_context(namespace)
  end

  @doc "The nodes holding a hub for `namespace`."
  @spec hub_nodes(String.t(), caller_context()) :: {:ok, [node()]} | {:error, term()}
  verb hub_nodes(namespace, caller_context), verb: :list_runners, args: [namespace: @namespace] do
    {:ok, Directory.hub_nodes(namespace)}
  end

  @doc """
  Every plugin package this controller loaded or refused at boot, and what
  its plugin root holds since.
  """
  @spec installed_plugins(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb installed_plugins(caller_context), verb: :list_installed_plugins do
    {:ok, Enum.map(Installer.list(), &installed/1)}
  end

  @doc """
  Every plugin provider this controller can enable, with its module and type,
  and how many plugin packages it refused at boot.
  """
  @spec providers(caller_context()) :: {:ok, map()} | {:error, term()}
  verb providers(caller_context), verb: :plugin_list do
    {:ok,
     %{
       providers: Providers.describe_all(),
       refused: Enum.count(Installer.list(), &(&1.status == :refused))
     }}
  end

  @doc "One plugin provider's help text, its plugin type and the hooks it supports."
  @spec provider(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb provider(provider_id, caller_context),
    verb: :plugin_info,
    args: [provider_id: [type: :string, doc: "The provider's id."]] do
    Providers.help(provider_id)
  end

  # A refusal is carried as its fields, which every surface can render.
  defp installed(%{error: %Flezha.Error{} = error} = row),
    do: %{Map.from_struct(row) | error: Map.from_struct(error)}

  defp installed(row), do: Map.from_struct(row)
end
