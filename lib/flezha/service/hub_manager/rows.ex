defmodule Flezha.Service.HubManager.Rows do
  @moduledoc """
  Reads over the hub's process registry: the live-hub guard, the registry dump,
  a row's cspec and its partition identity, and the plugin lists a row carries.

  **This module MUST NOT be added to `test/test_helper.exs`'s `Mimic.copy`
  list.** `RegistryUpdate` ships transform closures that reach
  `rewrite_cspec_plugins/2` on every peer's registry; a closure bound to a
  Mimic-copied module names `<Mod>.Mimic.Original.Module`, an in-memory test
  artifact that does not exist on peers, and crashes the remote registry with
  `UndefinedFunctionError`.
  """

  alias Flezha.Config

  alias Flezha.Convergence.Snapshot
  alias Flezha.Identificator
  alias Flezha.Operator.Lookup
  alias Flezha.Plugin
  alias Flezha.Process.Cspec
  alias Flezha.Process.Hub
  alias ProcessHub.Service.DeclaredChildren
  alias ProcessHub.Service.ProcessRegistry

  def hub_alive?(namespace) do
    Hub.alive?(Identificator.hub(namespace))
  end

  @doc """
  Run `fun` with `hub_id` when that hub is alive, else return `on_dead`.

  The one guarded seam for reaching ANY hub — the namespace hubs via
  `with_live_hub/3` below, the schedule hub via `Flezha.Schedule.Hub`. Every
  hub reader and every hub call goes through it so none can forget the
  liveness check: the class of bug where `get_pid/2` alone omitted it and
  raised on a torn-down hub.

  The check alone is not the guarantee. `ProcessHub.is_alive?/1` is
  `Process.whereis/1`, which answers `true` for a coordinator that is
  terminating, and stops being true the instant it returns. So a hub that dies
  DURING `fun` answers `on_dead` too — one namespace's teardown must never
  reach a node-wide caller as an exit. Both kinds are caught because a dying
  hub speaks both: its coordinator is a GenServer and exits, its registry is
  ETS and raises. A containment is loud, never silent.
  """
  @spec with_live_hub_id(atom(), term(), (atom() -> term())) :: term()
  defdelegate with_live_hub_id(hub_id, on_dead, fun), to: Hub, as: :with_live

  def with_live_hub(namespace, on_dead, fun) do
    with_live_hub_id(Identificator.hub(namespace), on_dead, fun)
  end

  def get_registry(namespace) do
    with_live_hub(namespace, {:error, {:hub_down, namespace}}, fn hub_id ->
      {:ok, live_registry_dump(hub_id)}
    end)
  end

  @doc """
  The same dump, for a caller that only folds it: a hub that is not up holds no
  running operator, and that is an empty dump rather than an error to unwrap.
  """
  @spec registry_dump(String.t()) :: map()
  def registry_dump(namespace), do: with_live_hub(namespace, %{}, &live_registry_dump/1)

  # Every caller here means "what is currently running with a live binding":
  # unbound rows are replay residue, not runtime. Declared intent is a
  # different question — `declared_operators/1` answers that from the hub's
  # declared set.
  defp live_registry_dump(hub_id) do
    ProcessRegistry.dump_all(hub_id, include_unbound: false, include_stopped: false)
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts = if include_local, do: [:include_local], else: []
    with_live_hub(namespace, [], fn hub_id -> ProcessHub.nodes(hub_id, opts) end)
  end

  # `Lookup` answers in the three atoms every resolver shares; this is the
  # boundary where a hub read becomes the reason a caller is answered with,
  # naming what was looked for.
  def get_pid(namespace, operator_id) do
    case Lookup.operator(namespace, operator_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, :hub_down} -> {:error, {:hub_down, namespace}}
      {:error, _reason} -> {:error, {:operator_not_found, namespace, operator_id}}
    end
  end

  @doc """
  Resolve an operator's runnable pid from a ProcessHub `node_pids` list
  (`[{node, pid}]`, with 0, 1, or many entries).

  Drops entries on nodes absent from the live cluster, prefers the local node's
  pid (verified alive), and otherwise returns a pid on another live node.
  Returns `nil` when no live pid remains.

  This centralises the multi-node-safe access that replaced the former
  single-element `[{_node, pid}]` matches, which raised `FunctionClauseError`
  the moment an operator was registered on more than one node.
  """
  @spec live_pid([{node(), pid()}]) :: pid() | nil
  def live_pid(node_pids) when is_list(node_pids) do
    local = node()
    live_nodes = [local | Node.list()]
    on_live_node = Enum.filter(node_pids, fn {n, _pid} -> n in live_nodes end)

    local_alive =
      Enum.find_value(on_live_node, fn
        {^local, pid} -> if Process.alive?(pid), do: pid, else: nil
        _ -> nil
      end)

    cond do
      is_pid(local_alive) ->
        local_alive

      true ->
        case Enum.find(on_live_node, fn {n, _pid} -> n != local end) do
          {_n, pid} -> pid
          nil -> nil
        end
    end
  end

  def live_pid(_), do: nil

  def get_all_operators(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      ProcessHub.process_list(hub_id, :global)
      |> Enum.flat_map(fn {operator_id, node_pids} ->
        case live_pid(node_pids) do
          nil -> []
          operator_pid -> [{operator_id, operator_pid}]
        end
      end)
    end)
  end

  @doc """
  The live node hosting an operator from its `node_pids` (`[{node, pid}]`): the
  local node when it holds the runnable pid, else another live cluster node,
  else `nil` (every entry is on a dead node — a FAILED placement). Sibling of
  `live_pid/1` for surfacing per-node placement in the CLI.
  """
  @spec live_node([{node(), pid()}]) :: node() | nil
  def live_node(node_pids) when is_list(node_pids) do
    local = node()
    live_nodes = [local | Node.list()]
    on_live_node = Enum.filter(node_pids, fn {n, _pid} -> n in live_nodes end)

    cond do
      Enum.any?(on_live_node, fn {n, pid} -> n == local and Process.alive?(pid) end) ->
        local

      true ->
        case Enum.find(on_live_node, fn {n, _pid} -> n != local end) do
          {n, _pid} -> n
          nil -> nil
        end
    end
  end

  def live_node(_), do: nil

  @doc """
  Per-operator view of a namespace's durable registry for user surfaces:
  one row per registered operator (including FAILED ones whose only placement is
  a dead node, which `get_all_operators/1` filters out), carrying the assigned
  runner, the declared factory (from the durable cspec start-args) and live node
  from the registry dump and the live health classified
  by the Convergence Loop (`Snapshot.operator_health/2`), so the CLI sees exactly
  what the loop sees, worded as every surface shows it: `:recovering` for a
  rebinding operator inside the unmet-intent grace. A registry row the loop has
  not yet observed reports `:unknown` until the next tick covers it.
  """
  @spec operator_rows(String.t()) :: [
          %{id: term(), health: atom(), runner: term(), factory: term(), node: term()}
        ]
  def operator_rows(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> live_registry_dump()
      |> Enum.map(fn {operator_id, entry} ->
        {cspec, node_pids, metadata} = normalize_registry_entry(entry)

        builder = cspec_operator(cspec)

        %{
          id: operator_id,
          health: Snapshot.operator_health(namespace, operator_id),
          runner: Map.get(metadata, :tag),
          factory: builder && Map.get(builder, :factory_id),
          partition: cspec_partition(cspec),
          node: live_node(node_pids)
        }
      end)
      |> Enum.sort_by(&inspect(&1.id))
    end)
  end

  @doc """
  Every operator's declared plugins, as `{operator_id, [%Flezha.Plugin{}]}`.

  Read from the same durable registry dump `operator_rows/1` walks, so asking
  "who depends on X" costs one registry read rather than an Overwatch
  round-trip per operator. Declared, not live: a plugin the cspec records but
  whose host has not re-subscribed it yet still appears, which is what a caller
  looking for dependents wants.
  """
  @spec operator_plugins(binary() | atom()) :: [{binary(), [Flezha.Plugin.t()]}]
  def operator_plugins(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> live_registry_dump()
      |> Enum.map(fn {operator_id, entry} ->
        {cspec, _node_pids, _metadata} = normalize_registry_entry(entry)
        {operator_id, Keyword.get(cspec_opts(cspec), :plugins, [])}
      end)
      |> Enum.reject(fn {_id, plugins} -> plugins == [] end)
    end)
  end

  @doc """
  Read the current plugin list embedded in an operator's child-spec
  start args. Returns `{:ok, [{plugin_id, %Flezha.Plugin{}}]}` (entries
  are normalised to tuple form regardless of how they are stored).
  """
  @spec operator_plugins(String.t(), String.t()) ::
          {:ok, [{binary(), Plugin.t()}]} | {:error, term()}
  def operator_plugins(namespace, operator_id) do
    with_live_hub(namespace, {:error, {:hub_down, namespace}}, fn hub_id ->
      case ProcessRegistry.lookup(hub_id, operator_id) do
        nil ->
          {:error, {:operator_not_found, namespace, operator_id}}

        entry ->
          case Cspec.start_arg(elem(entry, 0)) do
            {:ok, arg} -> {:ok, normalize_plugins(extract_plugins(arg))}
            :error -> {:error, {:operator_not_found, namespace, operator_id}}
          end
      end
    end)
  end

  # The declared operator from a registry entry's cspec start-args — the same
  # `{namespace, operator}` / `{namespace, runner, operator}` arg shapes
  # `alter_child_data/2` rewrites. The durable record of everything a start
  # declared, which is why partition identity lives there.
  defp cspec_operator(cspec) do
    case Cspec.start_arg(cspec) do
      {:ok, {_namespace, %{} = operator}} -> operator
      {:ok, {_namespace, _runner, %{} = operator}} -> operator
      _ -> nil
    end
  end

  @doc "The partition identity a registry entry's cspec carries, or `nil`."
  @spec cspec_partition(map()) :: map() | nil
  def cspec_partition(cspec), do: Flezha.Partition.identity(cspec_opts(cspec))

  # The builder opts a registry entry's cspec start-args carry, or `[]` — where
  # a partition member's identity is durable.
  defp cspec_opts(cspec) do
    case cspec_operator(cspec) do
      nil -> []
      operator -> Map.get(operator, :opts, [])
    end
  end

  defp normalize_registry_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_registry_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  @doc """
  Apply `update_fn` to the plugin list embedded in a child-spec's
  start-args and return the rewritten cspec. Used by
  `Flezha.Service.PluginManager` when it issues a registry-write-first
  cspec update through `ProcessHub.Service.ProcessRegistry.update/3`.
  """
  @spec rewrite_cspec_plugins(map(), ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])) ::
          map()
  def rewrite_cspec_plugins(%{start: {m, f, [arg]}} = cspec, update_fn) do
    new_arg =
      case arg do
        {namespace, operator} ->
          {namespace, rewrite_operator_plugins(operator, update_fn)}

        {namespace, runner_id, operator} ->
          {namespace, runner_id, rewrite_operator_plugins(operator, update_fn)}
      end

    Map.put(cspec, :start, {m, f, [new_arg]})
  end

  defp rewrite_operator_plugins(operator, update_fn) do
    current = normalize_plugins(extract_plugins({nil, operator}))
    new_plugins = update_fn.(current)
    new_opts = Keyword.put(operator.opts, :plugins, new_plugins)
    %{operator | opts: new_opts}
  end

  defp extract_plugins({_namespace, operator}),
    do: Keyword.get(operator.opts, :plugins, [])

  defp extract_plugins({_namespace, _runner_id, operator}),
    do: Keyword.get(operator.opts, :plugins, [])

  defp normalize_plugins(plugins) when is_list(plugins) do
    Enum.map(plugins, fn
      {id, %Plugin{} = plugin} -> {id, plugin}
      %Plugin{id: id} = plugin -> {id, plugin}
    end)
  end

  def durable_operator_count(hub_id) do
    with_live_hub_id(hub_id, 0, fn id -> map_size(live_registry_dump(id)) end)
  end

  @doc """
  Whether operator starts declare durable intent — `durable: true` through
  the hub's declared set. THE auto-restore kill-switch predicate: the start
  authority, the hub's `auto_recovery` config, the registry backend, and the
  pending-restore health read all flip on it together — a hub without
  auto-recovery refuses durable starts, so they cannot diverge.

  Only an explicit `false` disables. Reading it as a strict boolean would
  crash callers on a non-boolean value; `!= false` keeps the default-on
  behaviour robust.
  """
  @spec durable_starts?() :: boolean()
  def durable_starts?() do
    Config.get(:auto_restore_enabled) != false
  end

  @doc """
  The namespace's declared operators, read from the hub's declared set:
  intent, not runtime. An entry exists here for exactly as long as the
  operator should exist, whether or not anything currently runs it — a wiped
  runtime registry does not empty this list, and a deliberate stop removes its
  entry for good. Parsed through the same cspec start-args shapes the registry
  readers use; an entry whose args no current clause understands still counts
  as declared, with an unknown factory. A hub that cannot be read answers
  `unreadable`: none declared, unless the caller must tell the two apart.
  """
  @spec declared_operators(binary() | atom(), term()) ::
          [%{id: binary(), factory_id: binary() | nil}] | term()
  def declared_operators(namespace, unreadable \\ []) do
    with_live_hub(namespace, unreadable, fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      Enum.map(children, fn child_spec ->
        operator = cspec_operator(child_spec)
        %{id: child_spec.id, factory_id: operator && Map.get(operator, :factory_id)}
      end)
    end)
  end

  @doc """
  Every declared operator's plugins, as `{operator_id, [plugin]}`, read from the
  hub's declared set: the intent a move, a restart and a rescale all keep,
  while the registry row of a moving operator may be between its two homes.
  """
  @spec declared_plugins(binary() | atom()) :: [{binary(), list()}]
  def declared_plugins(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      for child_spec <- children,
          plugins = Keyword.get(cspec_opts(child_spec), :plugins, []),
          plugins != [],
          do: {child_spec.id, plugins}
    end)
  end

  def durable_operator_keys(hub_id) do
    with_live_hub_id(hub_id, [], fn id ->
      live_registry_dump(id) |> Map.keys() |> Enum.filter(&is_binary/1)
    end)
  end

  @doc """
  True when the namespace's hub still holds registered operators (running or
  durably suspended). Runner cleanup / migration consult this so a hub is never
  torn down while it holds parked operators that must stay addressable.
  """
  def registered_operators?(namespace) do
    Identificator.hub(namespace) |> durable_operator_keys() != []
  end

  @doc """
  True when THIS node still hosts operators of `namespace`'s hub — the
  question a node-local reclaim asks, as opposed to the cluster-wide
  `registered_operators?/1`. A hub that cannot be read answers `true`: a
  read that failed proves nothing, and nothing is torn down on it.
  """
  @spec local_operators?(binary()) :: boolean()
  def local_operators?(namespace) do
    with_live_hub(namespace, true, fn hub_id ->
      ProcessHub.process_list(hub_id, :local) != []
    end)
  catch
    _kind, _reason -> true
  end
end
