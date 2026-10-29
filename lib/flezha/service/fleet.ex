defmodule Flezha.Service.Fleet do
  @moduledoc """
  Published service-API surface for the runner fleet and the cluster's
  directory: which runners a namespace has, which factories, and what is
  installed. It also drops a runner process and lifts the drop
  (`drop_runner/3`, `readmit_runner/3`).

  Every published function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data. A fleet read
  is a relay-topology read — the row `flz runner list` binds — scoped to
  the namespace asked about rather than to one the caller declares.
  """

  use Flezha.AuthZ.Verb

  @namespace [type: :string, doc: "The namespace."]
  @runner_id [type: :string, doc: "The runner's id, as `runners/2` lists it."]
  @factory_id [type: :string, doc: "The factory."]

  alias Flezha.Cluster.Reads
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Providers
  alias Flezha.Runner.DropMarks
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CallerContext
  alias Flezha.Service.RelayManager

  @type caller_context :: CallerContext.t()

  @doc """
  Every runner serving a namespace, anywhere in the cluster. One row each: `id`,
  the `node` holding its session, `connected_at` (unix seconds), the runner's OS
  `pid` where its transport says, `silent_ms` since the controller last heard
  from it, and `late?` once that silence shows a missed keepalive
  (`FleetManager.global_runner_rows/1`).
  """
  @spec runners(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb runners(namespace, caller_context), verb: :list_runners, args: [namespace: @namespace] do
    FleetManager.global_runner_rows(namespace)
  end

  @doc """
  Drop one runner serving a namespace. It is dropped wherever in the cluster it
  is connected. Places a drop mark on the runner's process and closes its
  session without telling it to stop: its relays are re-placed at once, and
  every controller refuses the same process (`runner.dropped`) until a different
  one of the same runner connects or the mark is lifted with `readmit_runner/3`.
  Answers the mark: the runner's stable id and incarnation, the process id where
  the transport can say, the node it was on, and when and by whom it was
  dropped. A runner that presents no stable identity or incarnation cannot be
  recognised again and is refused with `runner.untrackable`.
  """
  @spec drop_runner(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb drop_runner(namespace, runner_id, caller_context),
    verb: :runner_drop,
    call: call,
    args: [namespace: @namespace, runner_id: @runner_id] do
    case FleetManager.find_session(call.namespace, runner_id) do
      nil ->
        {:error, {:runner_not_found, call.namespace, runner_id}}

      %{stable_id: stable_id, incarnation: incarnation} = session
      when is_binary(stable_id) and is_binary(incarnation) ->
        DropMarks.put(%{
          namespace: call.namespace,
          stable_id: stable_id,
          incarnation: incarnation,
          pid: session.pid,
          node: session.node,
          dropped_at: System.system_time(:second),
          dropped_by: Map.get(caller_context, :principal)
        })

      _untrackable ->
        {:error, {:runner_untrackable, runner_id}}
    end
  end

  @doc """
  Lift the drop mark on a runner in a namespace. The mark is lifted on every
  controller, so the dropped process is admitted on its next retry. Answers the
  lifted mark.
  """
  @spec readmit_runner(String.t(), String.t(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb readmit_runner(namespace, stable_id, caller_context),
    verb: :runner_readmit,
    call: call,
    args: [
      namespace: @namespace,
      stable_id: [type: :string, doc: "The dropped runner's stable id."]
    ] do
    DropMarks.clear(call.namespace, stable_id)
  end

  @doc "The drop marks standing in a namespace, oldest first."
  @spec dropped_runners(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb dropped_runners(namespace, caller_context),
    verb: :list_runners,
    call: call,
    args: [namespace: @namespace] do
    {:ok, DropMarks.list(call.namespace)}
  end

  @doc """
  The runners of one factory in `namespace`, on every controller. Each
  controller advertising it answers its own; `unreachable` names those that
  did not answer.
  """
  @spec runners_for_factory(String.t(), String.t(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb runners_for_factory(namespace, factory_id, caller_context),
    verb: :list_runners,
    args: [namespace: @namespace, factory_id: @factory_id] do
    {:ok, FleetManager.Cluster.factory_runners(namespace, factory_id)}
  end

  @doc """
  The factories any controller's runners advertise in `namespace`. One row
  each, carrying the `nodes` advertising it and the `runners` that can place
  it, so a listing needs no `runners_for_factory/3` call per factory;
  `unreachable` names the controllers that did not answer.

  A row also carries how the factory runs (`run`, and a steps run's `each` and
  `on_failure`), its API and its `declarations`: `durable`, `sleep_after_ms`,
  `max_duration_ms`, `effectively_once`, `ack`,
  `correction_ms` and `keyed`, a value the class did not declare being `nil`,
  and `declarations: nil` when its runner announced none. `api_disagree` and
  `declarations_disagree` say its runners, on any controller, announce
  different ones.
  """
  @spec factories(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb factories(namespace, caller_context), verb: :list_runners, args: [namespace: @namespace] do
    {:ok, FleetManager.Cluster.global_factories(namespace)}
  end

  @doc """
  One factory of a namespace, described whether or not an instance of it runs.

  It answers its row as `factories/2` answers it, its announced `graph` (`nil`
  when its runners announced none, each tasklet saying whether it
  `compensates`), and
  `disagree`, true when its runners do not all announce one class: different
  graphs on the controller that read it, or different APIs or declarations on
  any controller. The graph is read on a controller whose runner announces the
  factory. A factory no connected runner announces is `relay.factory_unknown`.
  """
  @spec factory(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb factory(namespace, factory_id, caller_context),
    verb: :list_runners,
    args: [namespace: @namespace, factory_id: @factory_id] do
    %{rows: rows, unreachable: unreachable} = FleetManager.Cluster.global_factories(namespace)

    with %{} = row <-
           Enum.find(rows, &(&1.id == factory_id)) ||
             {:error, {:factory_unknown, namespace, factory_id}},
         %{graph: graph, disagree: disagree} <- RelayManager.factory_graph(namespace, factory_id) do
      {:ok,
       Map.merge(row, %{
         graph: graph,
         disagree: disagree or row.api_disagree or row.declarations_disagree,
         unreachable: unreachable
       })}
    end
  end

  @doc "The caller context a runner in `namespace` connects with."
  @spec runner_caller_context(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb runner_caller_context(namespace, caller_context),
    verb: :list_runners,
    args: [namespace: @namespace] do
    {:ok, FleetManager.runner_caller_context(namespace)}
  end

  @doc """
  Every plugin package each controller loaded or refused at boot, each row with
  its `node`. It also lists what each controller's plugin root holds since boot.
  """
  @spec installed_plugins(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb installed_plugins(caller_context), verb: :list_installed_plugins do
    {:ok, Reads.rows(Reads.controllers(), __MODULE__, :local_installed, [])}
  end

  @doc """
  Every plugin provider any controller can enable, one row per provider. Each
  comes with its module, its type, `feeds` (whether it injects records into a
  relay, as its declaration says) and the `nodes` that can enable it. The
  answer also counts, per controller, the plugin packages it refused at boot
  (`refused`).
  """
  @spec providers(caller_context()) :: {:ok, map()} | {:error, term()}
  verb providers(caller_context), verb: :plugin_list do
    %{answers: answers, unreachable: unreachable} =
      Reads.ask(Reads.controllers(), __MODULE__, :local_providers, [])

    rows =
      for {node, %{providers: providers}} <- answers,
          row <- providers,
          do: Map.put(row, :node, node)

    joined = Reads.grouped(%{rows: rows, unreachable: unreachable}, & &1.id, &provider_row/1)

    {:ok,
     Map.put(joined, :refused, Map.new(answers, fn {node, answer} -> {node, answer.refused} end))}
  end

  @doc """
  One plugin provider's help text, type, hooks and options. It answers the
  provider's help text, its plugin type, whether it feeds a relay (`feeds`), the
  hooks it supports and its options as typed rows, from a controller that can
  enable it.

  `options` holds one map per public option, in declaration order: `name`,
  `type`, `required` and `doc`, and `default`, `values`, `min`, `max`, `live` and
  `transient` when the option declares them. A transient option never answers
  its default.
  """
  @spec provider(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb provider(provider_id, caller_context),
    verb: :plugin_info,
    args: [provider_id: [type: :string, doc: "The provider's id."]] do
    # No controller holding it: this controller's own refusal says why.
    case Reads.find(Reads.controllers(), Providers, :help, [provider_id]) do
      {:ok, _node, help} -> help
      {:error, _unreachable} -> Providers.help(provider_id)
    end
  end

  @doc false
  # The local half of `installed_plugins/1`.
  @spec local_installed() :: [map()]
  def local_installed, do: Enum.map(Installer.list(), &installed/1)

  @doc false
  # The local half of `providers/1`.
  @spec local_providers() :: map()
  def local_providers do
    %{
      providers: Providers.describe_all(),
      refused: Enum.count(Installer.list(), &(&1.status == :refused))
    }
  end

  defp provider_row([first | _] = rows),
    do: first |> Map.delete(:node) |> Map.put(:nodes, Enum.map(rows, & &1.node))

  # A refusal is carried as its fields, which every surface can render.
  defp installed(%{error: %Flezha.Error{} = error} = row),
    do: %{Map.from_struct(row) | error: Map.from_struct(error)}

  defp installed(row), do: Map.from_struct(row)
end
