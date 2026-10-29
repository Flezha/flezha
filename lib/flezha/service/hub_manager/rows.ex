defmodule Flezha.Service.HubManager.Rows do
  @moduledoc """
  Reads over the hub's process registry: the live-hub guard, the registry dump,
  a row's cspec and its partition identity, and the plugin lists a row carries.

  **This module MUST NOT be added to `test/test_helper.exs`'s `Mimic.copy`
  list.** `RegistryUpdate` ships transform closures that reach
  its cspec rewrites on every peer's registry; a closure bound to a
  Mimic-copied module names `<Mod>.Mimic.Original.Module`, an in-memory test
  artifact that does not exist on peers, and crashes the remote registry with
  `UndefinedFunctionError`.
  """

  alias Flezha.Config

  alias Flezha.Identificator
  alias Flezha.Relay.Lookup
  alias Flezha.Plugin
  alias Flezha.Process.Cspec
  alias Flezha.Process.Hub
  alias ProcessHub.Service.Cluster
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
  running relay, and that is an empty dump rather than an error to unwrap.
  """
  @spec registry_dump(String.t()) :: map()
  def registry_dump(namespace), do: with_live_hub(namespace, %{}, &live_registry_dump/1)

  # Every caller here means "what is currently running with a live binding":
  # unbound rows are replay residue, not runtime. Declared intent is a
  # different question — `declared_relays/1` answers that from the hub's
  # declared set.
  defp live_registry_dump(hub_id) do
    ProcessRegistry.dump_all(hub_id, include_unbound: false, include_stopped: false)
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts = if include_local, do: [:include_local], else: []
    with_live_hub(namespace, [], &members(&1, opts))
  end

  @doc """
  Whether this node is the first of the namespace's hub members by name: the
  one that acts for the whole namespace where every member sees the same
  thing. `false` when the hub cannot be read.
  """
  @spec leads?(binary() | atom()) :: boolean()
  def leads?(namespace),
    do: with_live_hub(namespace, false, &(Enum.min(members(&1, [:include_local])) == node()))

  # The coordinator answers the same read; taken from the hub's storage, so
  # a busy coordinator never holds up the caller.
  defp members(hub_id, opts), do: Cluster.nodes(ProcessHub.Hub.get(hub_id).storage.misc, opts)

  # `Lookup` answers in the three atoms every resolver shares; this is the
  # boundary where a hub read becomes the reason a caller is answered with,
  # naming what was looked for.
  def get_pid(namespace, relay_id) do
    case Lookup.relay(namespace, relay_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, :hub_down} -> {:error, {:hub_down, namespace}}
      {:error, _reason} -> {:error, {:relay_not_found, namespace, relay_id}}
    end
  end

  # A plugin entry is a hub child too, keyed `{:plugin, name}`; every reader
  # here that means relays takes the binary keys only.
  def get_all_relays(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      ProcessHub.process_list(hub_id, :global)
      |> Enum.flat_map(fn {relay_id, node_pids} ->
        case is_binary(relay_id) && Hub.live_pid(node_pids) do
          pid when is_pid(pid) -> [{relay_id, pid}]
          _plugin_or_dead -> []
        end
      end)
    end)
  end

  @doc """
  The relay ids whose registry row is bound to a member of the namespace's hub,
  connected or held through a node-down grace, whether or not that node answers
  now; `unreadable` when the hub cannot be read.
  """
  @spec placed_relay_ids(binary() | atom(), term()) :: MapSet.t() | term()
  def placed_relay_ids(namespace, unreadable) do
    with_live_hub(namespace, unreadable, fn hub_id ->
      members = MapSet.new(members(hub_id, [:include_local]))

      for {relay_id, node_pids} <- ProcessHub.process_list(hub_id, :global),
          is_binary(relay_id),
          Enum.any?(node_pids, &MapSet.member?(members, elem(&1, 0))),
          into: MapSet.new(),
          do: relay_id
    end)
  end

  @doc """
  The node hosting the pid `Flezha.Process.Hub.live_pid/1` picks from
  `node_pids`, or `nil` when every entry is on a dead node (a FAILED
  placement); it surfaces per-node placement in the CLI.
  """
  @spec live_node([{node(), pid()}]) :: node() | nil
  def live_node(node_pids) do
    with pid when is_pid(pid) <- Hub.live_pid(node_pids),
         {host, ^pid} <- List.keyfind(node_pids, pid, 1),
         do: host
  end

  @doc """
  Per-relay view of a namespace's durable registry for user surfaces:
  one row per registered relay (including FAILED ones whose only placement is
  a dead node, which `get_all_relays/1` filters out), carrying the assigned
  runner, the declared factory (from the durable cspec start-args) and live node
  from the registry dump. Health is the hosting controller's verdict, which
  `Flezha.Service.Relay.Info.rows/1` adds.
  """
  @spec relay_rows(String.t()) :: [
          %{id: term(), runner: term(), factory: term(), node: term()}
        ]
  def relay_rows(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> relay_dump()
      |> Enum.map(fn {relay_id, entry} ->
        {cspec, node_pids, metadata} = normalize_registry_entry(entry)

        builder = cspec_relay(cspec)

        %{
          id: relay_id,
          factory: builder && Map.get(builder, :factory_id),
          partition: cspec_partition(cspec)
        }
        |> Map.merge(host(node_pids, metadata))
      end)
      |> Enum.sort_by(&inspect(&1.id))
    end)
  end

  @doc """
  Every relay's bindings, as `{relay_id, [{plugin_id, %Flezha.Plugin{}}]}`.

  Read from the same durable registry dump `relay_rows/1` walks, so asking
  "who is bound to X" costs one registry read rather than an Overwatch
  round-trip per relay. Declared, not live: a binding the cspec records but
  whose host has not brought it up yet still appears.
  """
  @spec relay_bindings(binary() | atom()) :: [{binary(), [{binary(), Plugin.t()}]}]
  def relay_bindings(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> relay_dump()
      |> Enum.map(fn {relay_id, entry} ->
        {cspec, _node_pids, _metadata} = normalize_registry_entry(entry)
        {relay_id, normalize_plugins(Keyword.get(cspec_opts(cspec), :bindings, []))}
      end)
      |> Enum.reject(fn {_id, bindings} -> bindings == [] end)
    end)
  end

  @doc """
  The bindings embedded in a relay's child-spec start args, as
  `{:ok, [{plugin_id, %Flezha.Plugin{}}]}`.
  """
  @spec relay_bindings(String.t(), String.t()) ::
          {:ok, [{binary(), Plugin.t()}]} | {:error, term()}
  def relay_bindings(namespace, relay_id) do
    with {:ok, relay, _host} <- relay_spec(namespace, relay_id),
         do: {:ok, normalize_plugins(Keyword.get(relay.opts, :bindings, []))}
  end

  @doc "The relay `relay_id`'s option `key` as its registry row carries it, or `nil`."
  @spec relay_opt(String.t(), String.t(), atom()) :: term()
  def relay_opt(namespace, relay_id, key) do
    case relay_spec(namespace, relay_id) do
      {:ok, relay, _host} -> relay |> Map.get(:opts, []) |> Keyword.get(key)
      {:error, _unreadable} -> nil
    end
  end

  @doc """
  The relay `relay_id` as its start declared it (its factory id, its
  options), and where it runs, as `relay_rows/1` names it. Read from the one
  registry entry it has.
  """
  @spec relay_spec(String.t(), String.t()) ::
          {:ok, map(), %{runner: term(), node: node() | nil}} | {:error, term()}
  def relay_spec(namespace, relay_id) do
    with_live_hub(namespace, {:error, {:hub_down, namespace}}, fn hub_id ->
      with {cspec, node_pids, metadata} <-
             ProcessRegistry.lookup(hub_id, relay_id, with_metadata: true),
           %{} = relay <- cspec_relay(cspec) do
        {:ok, relay, host(node_pids, metadata)}
      else
        _none -> {:error, {:relay_not_found, namespace, relay_id}}
      end
    end)
  end

  # Where a registry entry's relay runs: its runner (`nil` while it has none)
  # and its live node.
  defp host(node_pids, metadata),
    do: %{runner: Map.get(metadata, :tag), node: live_node(node_pids)}

  @doc """
  The plugins enabled in `namespace`, each the `%Flezha.Plugin{}` entry its
  `{:plugin, name}` child carries: the declared set, which a cold start keeps,
  and the registry rows, which hold an entry while durable starts are off.
  """
  @spec plugin_entries(binary() | atom()) :: [Plugin.t()]
  def plugin_entries(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      rows =
        for {{:plugin, _name}, entry} <- live_registry_dump(hub_id),
            do: elem(normalize_registry_entry(entry), 0)

      for spec <- children ++ rows, {:ok, plugin} <- [plugin_of(spec)], uniq: true, do: plugin
    end)
    |> Enum.uniq_by(& &1.id)
  end

  @doc """
  The plugin `name` enabled in `namespace`, or `:error`: its `{:plugin, name}`
  child's entry, read by that id from the declared set, else from its
  registry row, as `plugin_entries/1` ranks them.
  """
  @spec plugin_entry(binary() | atom(), binary()) :: {:ok, Plugin.t()} | :error
  def plugin_entry(namespace, name) do
    with_live_hub(namespace, :error, fn hub_id ->
      case DeclaredChildren.declared_child(hub_id, {:plugin, name}) do
        nil -> row_plugin(hub_id, name)
        spec -> plugin_of(spec)
      end
    end)
  end

  @doc "The plugin `name`'s entry as its registry row carries it, or `:error`."
  @spec plugin_row(binary() | atom(), binary()) :: {:ok, Plugin.t()} | :error
  def plugin_row(namespace, name), do: with_live_hub(namespace, :error, &row_plugin(&1, name))

  defp row_plugin(hub_id, name) do
    case ProcessRegistry.lookup(hub_id, {:plugin, name}) do
      {cspec, _node_pids} -> plugin_of(cspec)
      nil -> :error
    end
  end

  defp plugin_of(spec) do
    case Cspec.start_arg(spec) do
      {:ok, {_namespace, %Plugin{} = plugin}} -> {:ok, plugin}
      _other -> :error
    end
  end

  # The declared relay from a registry entry's cspec start-args — the same
  # `{namespace, relay}` / `{namespace, runner, relay}` arg shapes
  # `alter_child_data/2` rewrites. The durable record of everything a start
  # declared, which is why partition identity lives there. A plugin child's
  # entry is no relay.
  defp cspec_relay(cspec) do
    case Cspec.start_arg(cspec) do
      {:ok, {_namespace, %Plugin{}}} -> nil
      {:ok, {_namespace, %{} = relay}} -> relay
      {:ok, {_namespace, _runner, %{} = relay}} -> relay
      _ -> nil
    end
  end

  @doc "The partition identity a registry entry's cspec carries, or `nil`."
  @spec cspec_partition(map()) :: map() | nil
  def cspec_partition(cspec), do: Flezha.Partition.identity(cspec_opts(cspec))

  # The builder opts a registry entry's cspec start-args carry, or `[]` — where
  # a partition member's identity is durable.
  defp cspec_opts(cspec) do
    case cspec_relay(cspec) do
      nil -> []
      relay -> Map.get(relay, :opts, [])
    end
  end

  defp normalize_registry_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_registry_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  @doc """
  Apply `update_fn` to the plugin list a child spec's start args carry and
  return the rewritten spec: a relay's bindings, or a plugin child's own
  entry as a list of one. `Flezha.Service.PluginManager` writes both through
  `ProcessHub.Service.ProcessRegistry.update/3` with this one transform.
  """
  @spec rewrite_cspec_plugins(map(), ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])) ::
          map()
  def rewrite_cspec_plugins(
        %{start: {m, f, [{namespace, %Plugin{id: id} = plugin}]}} = cspec,
        update_fn
      ) do
    [{^id, rewritten}] = update_fn.([{id, plugin}])
    Map.put(cspec, :start, {m, f, [{namespace, rewritten}]})
  end

  def rewrite_cspec_plugins(cspec, update_fn) do
    rewrite_cspec_opts(cspec, fn opts ->
      current = normalize_plugins(Keyword.get(opts, :bindings, []))
      Keyword.put(opts, :bindings, update_fn.(current))
    end)
  end

  @doc """
  Apply `opts_fn` to the options a relay child spec's start args carry and
  return the rewritten spec.
  """
  @spec rewrite_cspec_opts(map(), (keyword() -> keyword())) :: map()
  def rewrite_cspec_opts(%{start: {m, f, [arg]}} = cspec, opts_fn) do
    new_arg =
      case arg do
        {namespace, relay} -> {namespace, %{relay | opts: opts_fn.(relay.opts)}}
        {namespace, runner, relay} -> {namespace, runner, %{relay | opts: opts_fn.(relay.opts)}}
      end

    Map.put(cspec, :start, {m, f, [new_arg]})
  end

  defp normalize_plugins(plugins) when is_list(plugins) do
    Enum.map(plugins, fn
      {id, %Plugin{} = plugin} -> {id, plugin}
      %Plugin{id: id} = plugin -> {id, plugin}
    end)
  end

  def durable_relay_count(hub_id) do
    with_live_hub_id(hub_id, 0, fn id -> map_size(relay_dump(id)) end)
  end

  defp relay_dump(hub_id),
    do: hub_id |> live_registry_dump() |> Map.filter(fn {id, _entry} -> is_binary(id) end)

  @doc """
  Whether relay starts declare durable intent — `durable: true` through
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
  The namespace's declared relays, read from the hub's declared set:
  intent, not runtime. An entry exists here for exactly as long as the
  relay should exist, whether or not anything currently runs it — a wiped
  runtime registry does not empty this list, and a deliberate stop removes its
  entry for good. Parsed through the same cspec start-args shapes the registry
  readers use; an entry whose args no current clause understands still counts
  as declared, with an unknown factory. A hub that cannot be read answers
  `unreadable`: none declared, unless the caller must tell the two apart.
  """
  @spec declared_relays(binary() | atom(), term()) ::
          [%{id: binary(), factory_id: binary() | nil}] | term()
  def declared_relays(namespace, unreadable \\ []) do
    with_live_hub(namespace, unreadable, fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      for %{id: id} = child_spec <- children, is_binary(id) do
        relay = cspec_relay(child_spec)
        %{id: id, factory_id: relay && Map.get(relay, :factory_id)}
      end
    end)
  end

  @doc """
  The relay builder `relay_id`'s declared entry starts, or `:error` when the
  entry is gone (a stop removed it), is not a relay, or the hub cannot be read.
  """
  @spec declared_relay(binary() | atom(), binary()) :: {:ok, map()} | :error
  def declared_relay(namespace, relay_id) do
    with_live_hub(namespace, :error, fn hub_id ->
      with %{} = spec <- DeclaredChildren.declared_child(hub_id, relay_id),
           %{} = relay <- cspec_relay(spec) do
        {:ok, relay}
      else
        _ -> :error
      end
    end)
  end

  @doc """
  Every declared relay's bindings, as `{relay_id, [binding]}`, read from
  the hub's declared set: the intent a move, a restart and a rescale all keep,
  while the registry row of a moving relay may be between its two homes.
  """
  @spec declared_bindings(binary() | atom()) :: [{binary(), list()}]
  def declared_bindings(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      for %{id: id} = child_spec <- children,
          is_binary(id),
          bindings = Keyword.get(cspec_opts(child_spec), :bindings, []),
          bindings != [],
          do: {id, normalize_plugins(bindings)}
    end)
  end

  @doc """
  Whether `namespace`'s declared set names any child, relay or plugin. The set
  is the cluster's, so it keeps a hub here only while no other member holds it
  (`HubManager.holds_anything?/1`).
  """
  @spec declares_anything?(binary() | atom()) :: boolean()
  def declares_anything?(namespace) do
    with_live_hub(namespace, false, fn hub_id ->
      DeclaredChildren.declared_children(hub_id).children != []
    end)
  end

  def durable_relay_keys(hub_id) do
    with_live_hub_id(hub_id, [], fn id ->
      live_registry_dump(id) |> Map.keys() |> Enum.filter(&is_binary/1)
    end)
  end

  @doc """
  True when the namespace's hub still holds registered relays (running or
  durably suspended). Runner cleanup / migration consult this so a hub is never
  torn down while it holds parked relays that must stay addressable.
  """
  def registered_relays?(namespace) do
    Identificator.hub(namespace) |> durable_relay_keys() != []
  end

  @doc """
  True when THIS node still hosts relays of `namespace`'s hub, as opposed to
  the cluster-wide `registered_relays?/1`. A plugin home placed here is not a
  relay: `HubManager.holds_anything?/1` weighs it with the declared set. A hub
  that cannot be read answers `true`: a read that failed proves nothing, and
  nothing is torn down on it.
  """
  @spec local_relays?(binary()) :: boolean()
  def local_relays?(namespace),
    do: with_live_hub(namespace, true, &(local_list(&1, :relays) != []))

  @doc """
  The `{child_id, pid}` pairs `namespace`'s hub runs on THIS node: its relays
  (binary ids) with `:relays`, every child, plugin homes included, with `:all`.
  A hub that is down or goes away mid-read runs nothing here.
  """
  @spec local_children(binary() | atom(), :relays | :all) :: [{term(), pid()}]
  def local_children(namespace, kind) when kind in [:relays, :all],
    do: with_live_hub(namespace, [], &local_list(&1, kind))

  @doc "The nodes that run any child of `namespace`'s hub, plugin homes included."
  @spec child_nodes(binary() | atom()) :: [node()]
  def child_nodes(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      for {_id, node_pids} <- ProcessHub.process_list(hub_id, :global),
          {host, _pid} <- node_pids,
          uniq: true,
          do: host
    end)
  end

  defp local_list(hub_id, kind) do
    for {id, pid} <- ProcessHub.process_list(hub_id, :local),
        is_pid(pid) and (kind == :all or is_binary(id)),
        do: {id, pid}
  end
end
