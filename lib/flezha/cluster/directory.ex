defmodule Flezha.Cluster.Directory do
  @moduledoc """
  Cluster-wide directory of runner capability and hub placement — one `:pg`
  scope, readable on every node without a network round.

  Writers advertise node-local truth: `Flezha.Runner.FleetManager` joins
  `{:factory, ns, id}` / `{:runners, ns}` groups as its fleet state changes,
  and `Flezha.Service.HubManager.start_hub/1` joins each namespace hub's own
  pid to `{:hub, ns}`. Membership is cleaned by `:pg`'s monitors, so a dead
  process or node needs no leave call and partitions heal on reconnect.

  Eventually consistent by design: every routed request is re-validated on
  the executing node, so a stale answer costs at most one failed forward.
  """

  alias Flezha.Process.Cspec

  @scope :flz_directory

  def child_spec(_opts) do
    Cspec.build(@scope, {:pg, :start_link, [@scope]}, :worker_permanent)
  end

  @doc "Group advertising runners for `factory_id` in `namespace` on the member's node."
  def factory_group(namespace, factory_id), do: {:factory, to_string(namespace), factory_id}

  @doc "Group advertising at least one active runner in `namespace` on the member's node."
  def runners_group(namespace), do: {:runners, to_string(namespace)}

  @doc """
  Diff `desired` against `joined` (both `MapSet`s of groups) for the calling
  process: join what is missing, leave what is extra. Returns the set now
  joined. A missing scope (isolated unit trees) makes this a no-op.
  """
  @spec sync_membership(MapSet.t(), MapSet.t()) :: MapSet.t()
  def sync_membership(desired, joined) do
    if scope_up?() do
      Enum.each(MapSet.difference(desired, joined), &:pg.join(@scope, &1, self()))
      Enum.each(MapSet.difference(joined, desired), &:pg.leave(@scope, &1, self()))
      desired
    else
      joined
    end
  end

  @doc """
  Advertise `pid` — the namespace hub's own supervisor — as `namespace`'s hub
  host. Idempotent; membership ends with the pid, so hub stop, crash, and
  nodedown all clean up without a leave call.
  """
  @spec join_hub(pid(), String.t() | atom()) :: :ok
  def join_hub(pid, namespace), do: join({:hub, to_string(namespace)}, pid)

  @doc """
  Advertise `pid` as a running copy of the plugin `name` of `namespace` on its
  node. Membership ends with the pid, like a hub's.
  """
  @spec join_plugin(pid(), String.t() | atom(), String.t()) :: :ok
  def join_plugin(pid, namespace, name), do: join(plugin_group(namespace, name), pid)

  @doc """
  Withdraw `pid` as a copy of the plugin `name` before it exits, so a stop that
  has returned finds it gone on this node: the exit alone leaves the group
  only when the scope reads it.
  """
  @spec leave_plugin(pid(), String.t() | atom(), String.t()) :: :ok
  def leave_plugin(pid, namespace, name) do
    if scope_up?(), do: :pg.leave(@scope, plugin_group(namespace, name), pid)
    :ok
  end

  defp join(group, pid) do
    if scope_up?() and pid not in :pg.get_local_members(@scope, group) do
      :pg.join(@scope, group, pid)
    end

    :ok
  end

  defp plugin_group(namespace, name), do: {:plugin, to_string(namespace), name}

  @doc "Every running copy of the plugin `name` of `namespace`, on every node."
  @spec plugin_hosts(String.t() | atom(), String.t()) :: [pid()]
  def plugin_hosts(namespace, name) do
    if scope_up?(), do: :pg.get_members(@scope, plugin_group(namespace, name)), else: []
  end

  @doc "Every running copy of any plugin of `namespace` on this node."
  @spec local_plugin_hosts(String.t() | atom()) :: [pid()]
  def local_plugin_hosts(namespace) do
    namespace = to_string(namespace)

    if scope_up?() do
      for {:plugin, ^namespace, _name} = group <- :pg.which_groups(@scope),
          copy <- :pg.get_local_members(@scope, group),
          do: copy
    else
      []
    end
  end

  @doc """
  Watch the copies of the plugin `name` of `namespace`: answers the reference
  `{ref, :join | :leave, group, pids}` messages carry and the copies running
  now, or `nil` when the directory is not running.
  """
  @spec monitor_plugin(String.t() | atom(), String.t()) :: {reference(), [pid()]} | nil
  def monitor_plugin(namespace, name), do: monitor(plugin_group(namespace, name))

  @doc "Watch the hubs of `namespace`, as `monitor_plugin/2` watches a plugin's copies."
  @spec monitor_hubs(String.t() | atom()) :: {reference(), [pid()]} | nil
  def monitor_hubs(namespace), do: monitor({:hub, to_string(namespace)})

  defp monitor(group), do: if(scope_up?(), do: :pg.monitor(@scope, group))

  @doc """
  The group a controller plugin's copy joins to hear `hook` in `namespace`:
  `:here` for a copy that hears its own controller, `:anywhere` for the one
  `single` copy, which hears every controller.
  """
  @spec hook_group(:here | :anywhere, String.t(), atom()) :: tuple()
  def hook_group(reach, namespace, hook), do: {:hooks, reach, to_string(namespace), hook}

  @doc "Whether `group` is one `hook_group/3` names."
  @spec hook_group?(term()) :: boolean()
  def hook_group?(group), do: match?({:hooks, _reach, _namespace, _hook}, group)

  @doc "The copies an event of `hook` in `namespace` that happened on this node goes to."
  @spec hook_targets(String.t(), atom()) :: [pid()]
  def hook_targets(namespace, hook) do
    if scope_up?(),
      do:
        :pg.get_local_members(@scope, hook_group(:here, namespace, hook)) ++
          :pg.get_members(@scope, hook_group(:anywhere, namespace, hook)),
      else: []
  end

  @doc "Whether any copy hears a hook event that happens on this node."
  @spec hooks_wanted?() :: boolean()
  def hooks_wanted?() do
    scope_up?() and
      Enum.any?(:pg.which_groups(@scope), fn
        {:hooks, :here, _, _} = group -> :pg.get_local_members(@scope, group) != []
        {:hooks, :anywhere, _, _} = group -> :pg.get_members(@scope, group) != []
        _other -> false
      end)
  end

  @doc """
  Watch every group, answering the reference `{ref, :join | :leave, group,
  pids}` messages carry and the groups now, or `nil` when the directory is not
  running.
  """
  @spec monitor_scope() :: {reference(), map()} | nil
  def monitor_scope(), do: if(scope_up?(), do: :pg.monitor_scope(@scope))

  @doc "Nodes running a copy of the plugin `name` of `namespace`."
  def plugin_nodes(namespace, name), do: nodes_of(plugin_group(namespace, name))

  @doc "Nodes whose runners advertise `factory_id` in `namespace`."
  def factory_nodes(namespace, factory_id), do: nodes_of(factory_group(namespace, factory_id))

  @doc "Nodes holding at least one active runner in `namespace`."
  def runner_nodes(namespace), do: nodes_of(runners_group(namespace))

  @doc "Nodes hosting `namespace`'s hub."
  def hub_nodes(namespace), do: nodes_of({:hub, to_string(namespace)})

  @doc "Every namespace any cluster node hosts a hub for."
  @spec namespaces() :: [String.t()]
  def namespaces() do
    if scope_up?() do
      for {:hub, ns} <- :pg.which_groups(@scope), uniq: true, do: ns
    else
      []
    end
  end

  defp nodes_of(group) do
    if scope_up?() do
      @scope |> :pg.get_members(group) |> Enum.map(&node/1) |> Enum.uniq()
    else
      []
    end
  end

  defp scope_up?(), do: Process.whereis(@scope) != nil
end
