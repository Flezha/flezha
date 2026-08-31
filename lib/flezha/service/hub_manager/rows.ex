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

  alias Flezha.Convergence.Snapshot
  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.State.DurabilityTier
  alias ProcessHub.Service.DeclaredChildren
  alias ProcessHub.Service.ProcessRegistry

  def hub_alive?(namespace) do
    ProcessHub.is_alive?(Identificator.hub(namespace))
  end

  @doc """
  Run `fun` with `hub_id` when that hub is alive, else return `on_dead`.

  The one guarded seam for reading ANY hub — the namespace hubs via
  `with_live_hub/3` below, the schedule hub via `Flezha.Schedule.Hub`. Every
  hub reader goes through it so none can forget the liveness check: the class
  of bug where `get_pid/2` alone omitted it and raised on a torn-down hub.
  """
  @spec with_live_hub_id(atom(), term(), (atom() -> term())) :: term()
  def with_live_hub_id(hub_id, on_dead, fun) when is_atom(hub_id) do
    if ProcessHub.is_alive?(hub_id), do: fun.(hub_id), else: on_dead
  end

  def with_live_hub(namespace, on_dead, fun) do
    with_live_hub_id(Identificator.hub(namespace), on_dead, fun)
  end

  def get_registry(namespace) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      {:ok, live_registry_dump(hub_id)}
    end)
  end

  # Every caller here means "what is currently running with a live binding":
  # unbound rows are replay residue, not runtime. Declared intent is a
  # different question — `declared_operatives/1` answers that from the hub's
  # declared set.
  defp live_registry_dump(hub_id) do
    ProcessRegistry.dump_all(hub_id, include_unbound: false, include_stopped: false)
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts = if include_local, do: [:include_local], else: []
    with_live_hub(namespace, [], fn hub_id -> ProcessHub.nodes(hub_id, opts) end)
  end

  def get_pid(namespace, operative_id) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      case ProcessHub.get_pids(hub_id, operative_id) do
        [] ->
          {:error, "no pid found"}

        pids ->
          {:ok, List.first(pids)}
      end
    end)
  end

  @doc """
  Resolve a operative's runnable pid from a ProcessHub `node_pids` list
  (`[{node, pid}]`, with 0, 1, or many entries).

  Drops entries on nodes absent from the live cluster, prefers the local node's
  pid (verified alive), and otherwise returns a pid on another live node.
  Returns `nil` when no live pid remains.

  This centralises the multi-node-safe access that replaced the former
  single-element `[{_node, pid}]` matches in `get_all_operatives/1` and
  `Flezha.Runner.FleetManager.runner_reroute/4` — either of which raised
  `FunctionClauseError` the moment a operative was registered on more than
  one node.
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

  def get_all_operatives(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      ProcessHub.process_list(hub_id, :global)
      |> Enum.flat_map(fn {operative_id, node_pids} ->
        case live_pid(node_pids) do
          nil -> []
          operative_pid -> [{operative_id, operative_pid}]
        end
      end)
    end)
  end

  @doc """
  The live node hosting a operative from its `node_pids` (`[{node, pid}]`): the
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
  Per-operative view of a namespace's durable registry for operator surfaces:
  one row per registered operative (including FAILED ones whose only placement is
  a dead node, which `get_all_operatives/1` filters out), carrying the assigned
  runner, the declared factory (from the durable cspec start-args) and live node
  from the registry dump and the live health classified
  by the Convergence Loop (`Snapshot.operative_health/2`), so the CLI sees exactly
  what the loop sees. A registry row the loop has not yet observed reports
  `:unknown` until the next tick covers it.
  """
  @spec operative_rows(String.t()) :: [
          %{id: term(), health: atom(), runner: term(), factory: term(), node: term()}
        ]
  def operative_rows(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> live_registry_dump()
      |> Enum.map(fn {operative_id, entry} ->
        {cspec, node_pids, metadata} = normalize_registry_entry(entry)

        builder = cspec_operative(cspec)

        %{
          id: operative_id,
          health: Snapshot.operative_health(namespace, operative_id),
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
  Every operative's declared plugins, as `{operative_id, [%Flezha.Plugin{}]}`.

  Read from the same durable registry dump `operative_rows/1` walks, so asking
  "who depends on X" costs one registry read rather than an Overwatch
  round-trip per operative. Declared, not live: a plugin the cspec records but
  whose host has not re-subscribed it yet still appears, which is what a caller
  looking for dependents wants.
  """
  @spec operative_plugins(binary() | atom()) :: [{binary(), [Flezha.Plugin.t()]}]
  def operative_plugins(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> live_registry_dump()
      |> Enum.map(fn {operative_id, entry} ->
        {cspec, _node_pids, _metadata} = normalize_registry_entry(entry)
        {operative_id, Keyword.get(cspec_opts(cspec), :plugins, [])}
      end)
      |> Enum.reject(fn {_id, plugins} -> plugins == [] end)
    end)
  end

  @doc """
  Read the current plugin list embedded in a operative's child-spec
  start args. Returns `{:ok, [{plugin_id, %Flezha.Plugin{}}]}` (entries
  are normalised to tuple form regardless of how they are stored).
  """
  @spec operative_plugins(String.t(), String.t()) ::
          {:ok, [{binary(), Plugin.t()}]} | {:error, term()}
  def operative_plugins(namespace, operative_id) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      case ProcessRegistry.lookup(hub_id, operative_id) do
        nil ->
          {:error, "operative not found"}

        {%{start: {_m, _f, [arg]}}, _node_pids} ->
          {:ok, normalize_plugins(extract_plugins(arg))}

        {%{start: {_m, _f, [arg]}}, _node_pids, _metadata} ->
          {:ok, normalize_plugins(extract_plugins(arg))}
      end
    end)
  end

  # The declared operative from a registry entry's cspec start-args — the same
  # `{namespace, operative}` / `{namespace, runner, operative}` arg shapes
  # `alter_child_data/2` rewrites. The durable record of everything a start
  # declared, which is why partition identity lives there.
  defp cspec_operative(%{start: {_m, _f, [arg]}}) do
    case arg do
      {_namespace, %{} = operative} -> operative
      {_namespace, _runner, %{} = operative} -> operative
      _ -> nil
    end
  end

  defp cspec_operative(_), do: nil

  @doc "The partition identity a registry entry's cspec carries, or `nil`."
  @spec cspec_partition(map()) :: map() | nil
  def cspec_partition(cspec), do: Flezha.Partition.identity(cspec_opts(cspec))

  # The builder opts a registry entry's cspec start-args carry, or `[]` — where
  # a partition member's identity is durable.
  defp cspec_opts(cspec) do
    case cspec_operative(cspec) do
      nil -> []
      operative -> Map.get(operative, :opts, [])
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
        {namespace, operative} ->
          {namespace, rewrite_operative_plugins(operative, update_fn)}

        {namespace, runner_id, operative} ->
          {namespace, runner_id, rewrite_operative_plugins(operative, update_fn)}
      end

    Map.put(cspec, :start, {m, f, [new_arg]})
  end

  defp rewrite_operative_plugins(operative, update_fn) do
    current = normalize_plugins(extract_plugins({nil, operative}))
    new_plugins = update_fn.(current)
    new_opts = Keyword.put(operative.opts, :plugins, new_plugins)
    %{operative | opts: new_opts}
  end

  defp extract_plugins({_namespace, operative}),
    do: Keyword.get(operative.opts, :plugins, [])

  defp extract_plugins({_namespace, _runner_id, operative}),
    do: Keyword.get(operative.opts, :plugins, [])

  defp normalize_plugins(plugins) when is_list(plugins) do
    Enum.map(plugins, fn
      {id, %Plugin{} = plugin} -> {id, plugin}
      %Plugin{id: id} = plugin -> {id, plugin}
    end)
  end

  def durable_operative_count(hub_id) do
    map_size(live_registry_dump(hub_id))
  catch
    :exit, _ -> 0
  end

  @doc """
  Whether operative starts declare durable intent — `durable: true` through
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
    Application.get_env(:flezha, :auto_restore_enabled, true) != false
  end

  @doc """
  The namespace's declared operatives, read from the hub's declared set:
  intent, not runtime. An entry exists here for exactly as long as the
  operative should exist, whether or not anything currently runs it — a wiped
  runtime registry does not empty this list, and a deliberate stop removes its
  entry for good. Parsed through the same cspec start-args shapes the registry
  readers use; an entry whose args no current clause understands still counts
  as declared, with an unknown factory.
  """
  @spec declared_operatives(binary() | atom()) ::
          [%{id: binary(), factory_id: binary() | nil}]
  def declared_operatives(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      Enum.map(children, fn child_spec ->
        operative = cspec_operative(child_spec)
        %{id: child_spec.id, factory_id: operative && Map.get(operative, :factory_id)}
      end)
    end)
  end

  def durable_operative_keys(hub_id) do
    live_registry_dump(hub_id) |> Map.keys() |> Enum.filter(&is_binary/1)
  catch
    # A hub whose registry table is already gone holds no operatives — the
    # table vanishing IS the hub being down. `:ets` RAISES for that rather than
    # exiting, so the exit clause alone left `runner_cleanup/1` killing the
    # FleetManager, and the control plane with it, on any namespace whose hub
    # died before the tick reached it.
    :exit, _ -> []
    :error, _ -> []
  end

  @doc """
  True when the namespace's hub still holds registered operatives (running or
  durably suspended). Runner cleanup / migration consult this so a hub is never
  torn down while it holds parked operatives that must stay addressable.
  """
  def registered_operatives?(namespace) do
    Identificator.hub(namespace) |> durable_operative_keys() != []
  end

  @doc false
  def auto_recovery_config do
    # Orphan reconcile: each round converges the cluster toward the hub's
    # declared set — starting declared children observed running nowhere,
    # stopping running durable children whose entry was removed. The grace is
    # also what holds the hub in `:recovering`, so it gates `RecoveryGate` and
    # `Boot.Loader.await_namespace_hubs` — it is set long enough for runners to
    # finish their handshake, since an orphan start needs one to place against.
    if durable_starts?() do
      [
        reconcile_grace_ms: Application.get_env(:flezha, :process_hub_reconcile_grace_ms, 45_000),
        reconcile_interval_ms:
          Application.get_env(:flezha, :process_hub_reconcile_interval_ms, 30_000),
        remote_manifest: remote_manifest_config()
      ]
    else
      false
    end
  end

  # The declared set's off-cluster copy rides the durability tier the operator
  # already configured — one remote-storage path, every tier backend
  # (`:postgres`, `:s3`, `:local_path`, custom) works unchanged. No tier, no
  # remote copy: `nil` keeps the feature local-only, exactly as before the
  # tier existed.
  defp remote_manifest_config do
    if DurabilityTier.enabled?() do
      {Flezha.State.DurabilityTier.DeclaredManifest, []}
    else
      nil
    end
  end
end
