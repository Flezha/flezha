defmodule Flezha.Runner.FleetManager.Announce do
  @moduledoc """
  What this controller publishes about its own fleet, re-derived from the state
  each mutation just committed.

  Three channels, one entry point. `sync/2` runs at the end of every fleet
  mutation and answers each of them from the committed state rather than from
  an increment at the mutation site, so none of them can drift from the truth
  it claims to describe:

    * the per-namespace `:runners` quota counter,
    * the `:pg` capability groups of `Flezha.Cluster.Directory`, which say which
      nodes hold a runner for a factory,
    * the served groups of each namespace hub, which say which controllers the
      hub may place an operative of that factory on.

  Desired-state diff, not refcounts: a group stays published while ANY runner on
  this node still backs it. A crashed fleet manager's `:pg` memberships are
  cleaned by that scope's monitors, and a hub's group table dies with the hub,
  which is why `forget/2` exists.

  The two group channels answer deliberately different questions. The directory
  advertises what a runner here ADVERTISES, so a routed start reaches this node
  and is judged here; a served group is what this node can actually PLACE, which
  `Factories.placeable_runners/3` narrows to the factory's established language.
  """

  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager.Factories

  use Flezha.Process.Timer, keys: [:withdraw_group]

  @doc """
  Publish this node's fleet on every channel, from the state just committed.

  `immediate_withdrawal: true` says the sync follows a disconnect this
  controller asked for, whose factories are withdrawn without the grace.
  """
  @spec sync(map(), keyword()) :: map()
  def sync(state, opts \\ []) do
    sync_runner_counters(state)

    %{
      state
      | directory_groups:
          Directory.sync_membership(desired_directory_groups(state), state.directory_groups)
    }
    |> sync_served_groups(Keyword.get(opts, :immediate_withdrawal, false))
  end

  @doc """
  Stop declaring `factories` to `namespace`'s hub, which reassigns their
  operatives to the controllers still serving them. One call, so the whole
  withdrawal is one announcement and one redistribution pass.

  A hub that is gone took its group table with it, so the factories leave this
  node's record of what it declared either way.
  """
  @spec withdraw(map(), String.t(), [String.t()]) :: map()
  def withdraw(state, namespace, factories) do
    _ = GroupedHashing.unserve_groups(Identificator.hub(namespace), factories)

    Enum.reduce(factories, state, fn factory_id, state ->
      state
      |> cancel_timer({:withdraw_group, {namespace, factory_id}})
      |> put_announced(namespace, MapSet.delete(announced(state, namespace), factory_id))
    end)
  end

  @doc """
  Forget what this node declared to `namespace`'s hub, so the next sync declares
  it again. A hub's group table dies with the hub.
  """
  @spec forget(map(), String.t()) :: map()
  def forget(state, namespace), do: put_announced(state, namespace, MapSet.new())

  # Every fleet mutation ends here, so the per-namespace `:runners` quota
  # counter is DERIVED from the live session map rather than incremented and
  # decremented at each of the several removal seams — the counter cannot drift
  # from the truth it is quota-checked against. A briefly-down counter store
  # costs one sync, never the fleet manager.
  defp sync_runner_counters(state) do
    if Flezha.AuthZ.enabled?(), do: Enum.each(state.runner_namespaces, &set_runner_counter/1)

    :ok
  catch
    _, _ -> :ok
  end

  defp set_runner_counter({namespace, runners}) do
    QuotaCounters.set(namespace, :runners, Enum.count(runners, &active?/1))
  end

  @doc "The `:pg` capability groups this node's fleet backs right now."
  @spec desired_directory_groups(map()) :: MapSet.t()
  def desired_directory_groups(state) do
    runner_groups =
      for {namespace, runners} <- state.runner_namespaces,
          Enum.any?(runners, &active?/1),
          do: Directory.runners_group(namespace)

    factory_groups =
      for {namespace, factories} <- state.namespace_factories,
          {factory_id, _info} <- factories,
          do: Directory.factory_group(namespace, factory_id)

    MapSet.new(runner_groups ++ factory_groups)
  end

  defp active?({_id, {status, _ts}}), do: status == :active

  # A factory becoming placeable is declared at once. One that stops being
  # placeable is held for `group_withdraw_grace_ms`, because a runner back
  # inside the grace moves no operative at all; a disconnect this controller
  # asked for carries no such hope and withdraws immediately.
  defp sync_served_groups(state, immediate_withdrawal?) do
    Enum.reduce(served_namespaces(state), state, fn namespace, state ->
      desired = Factories.placeable_factories(state, namespace)
      announced = announced(state, namespace)

      state
      |> cancel_withdrawals(namespace, desired)
      |> declare(namespace, MapSet.difference(desired, announced))
      |> retire(namespace, MapSet.difference(announced, desired), immediate_withdrawal?)
    end)
  end

  defp served_namespaces(state) do
    Enum.uniq(Map.keys(state.namespace_factories) ++ Map.keys(state.announced_groups))
  end

  defp cancel_withdrawals(state, namespace, factories) do
    Enum.reduce(factories, state, &cancel_timer(&2, {:withdraw_group, {namespace, &1}}))
  end

  defp declare(state, namespace, additions) do
    if Enum.empty?(additions) do
      state
    else
      case GroupedHashing.serve_groups(Identificator.hub(namespace), MapSet.to_list(additions)) do
        :ok ->
          put_announced(state, namespace, MapSet.union(announced(state, namespace), additions))

        # No hub on this node yet. The one its start triggers declares the set.
        {:error, :not_grouped_hashing} ->
          state
      end
    end
  end

  defp retire(state, namespace, removals, immediate_withdrawal?) do
    cond do
      Enum.empty?(removals) -> state
      immediate_withdrawal? -> withdraw(state, namespace, MapSet.to_list(removals))
      true -> Enum.reduce(removals, state, &arm_withdrawal(&2, namespace, &1))
    end
  end

  defp arm_withdrawal(state, namespace, factory_id) do
    key = {:withdraw_group, {namespace, factory_id}}

    # Already counting down. Re-arming would push the deadline out again on
    # every fleet mutation, and the grace would never elapse on a busy node.
    if timer_armed?(state, key) do
      state
    else
      arm_timer(
        state,
        key,
        Durations.get(:group_withdraw_grace_ms),
        {:withdraw_group, namespace, factory_id}
      )
    end
  end

  defp announced(state, namespace), do: Map.get(state.announced_groups, namespace, MapSet.new())

  defp put_announced(state, namespace, groups) do
    if Enum.empty?(groups) do
      %{state | announced_groups: Map.delete(state.announced_groups, namespace)}
    else
      %{state | announced_groups: Map.put(state.announced_groups, namespace, groups)}
    end
  end
end
