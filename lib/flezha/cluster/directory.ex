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

  @scope :flz_directory

  def child_spec(_opts) do
    %{id: @scope, start: {:pg, :start_link, [@scope]}}
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
  def join_hub(pid, namespace) do
    group = {:hub, to_string(namespace)}

    if scope_up?() and pid not in :pg.get_local_members(@scope, group) do
      :pg.join(@scope, group, pid)
    end

    :ok
  end

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
