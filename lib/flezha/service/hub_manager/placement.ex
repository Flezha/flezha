defmodule Flezha.Service.HubManager.Placement do
  @moduledoc """
  Where a child is placed and what it is told: the child spec and start args,
  what an unplaceable runner degrades to, and the runner-assignment wait.
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Process.Cspec
  alias Flezha.Service.HubManager.Rows
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.RegistryUpdate
  alias Flezha.State.DurabilityTier
  alias ProcessHub.Service.ProcessRegistry

  require Logger

  def runner_operatives(namespace, runner_id) do
    Rows.with_live_hub(namespace, [], fn hub_id -> ProcessHub.tag_query(hub_id, runner_id) end)
  end

  @doc """
  The live partition members of `group_id`, in partition order, each with its
  identity and node. A metadata match over the registry rows — the identity is
  stamped on the row at start and at bind — so the question every start and
  every plugin enable asks never decodes the whole registry.
  """
  @spec partition_members(String.t(), String.t()) :: [map()]
  def partition_members(namespace, group_id) do
    Rows.with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> ProcessHub.metadata_query(:partition)
      |> Enum.filter(fn {_id, node_pids, partition} ->
        node_pids != [] and is_map(partition) and partition.group == group_id
      end)
      |> Enum.map(fn {id, node_pids, partition} ->
        %{id: id, partition: partition, node: Rows.live_node(node_pids)}
      end)
      |> Enum.sort_by(& &1.partition.index)
    end)
  end

  def operative_set_runner(namespace, operative_id, runner_id) do
    Rows.with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      # propagate: true — `runner_operatives/2` is a local-table tag query, so a
      # peer handling this runner's disconnect misses the operative otherwise.
      # Returned, not discarded: a "No child found" is the whole signal that a
      # binding never reached the durable row.
      result =
        ProcessRegistry.update(
          hub_id,
          operative_id,
          RegistryUpdate.set_runner_tag(runner_id),
          propagate: true
        )

      # The cspec just changed: shipping state without it would give a
      # recovering controller perfect Cells for operatives it does not know
      # exist.
      DurabilityTier.ship_cspec(hub_id)
      result
    end)
  end

  @doc """
  Hub-wide `child_registered` handler: ships the cspec to the durability tier
  and declares each local operative's rewritten row to the convergence loop.

  A start request writes the row after its children are already running, so the
  `metadata.tag` it carries can name a runner an Overwatch has since rebound
  away from. The loop's next tick reads the row beside the binding and
  re-publishes the binding when they differ; nothing is decided here.
  """
  def hook_registry_written(hub_id, %{child_id: operative_id} = hook_data) do
    local = node()
    namespace = Identificator.namespace_from_hub(hub_id)
    DurabilityTier.ship_cspec(hub_id)

    Enum.each(Map.get(hook_data, :node_pids, []), fn
      {^local, pid} when is_pid(pid) ->
        Facts.declare({:registry_written, {namespace, operative_id}, %{}})

      _ ->
        :ok
    end)
  end

  @doc "The owning runner recorded on `operative_id`'s durable row, or `nil`."
  def operative_runner_tag(namespace, operative_id) do
    Rows.with_live_hub(namespace, nil, fn hub_id ->
      case ProcessRegistry.lookup(hub_id, operative_id, with_metadata: true) do
        {_cspec, _node_pids, metadata} when is_map(metadata) -> Map.get(metadata, :tag)
        _ -> nil
      end
    end)
  end

  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    case start_args(cs) do
      {:ok, parsed} -> place_child(child_data, cs, parsed)
      :error -> unparsable_child(child_data, cs)
    end
  end

  defp start_args(cspec) do
    case Cspec.start_arg(cspec) do
      {:ok, {namespace, %{} = operative}} -> {:ok, {namespace, nil, operative}}
      {:ok, {namespace, runner, %{} = operative}} -> {:ok, {namespace, runner, operative}}
      _ -> :error
    end
  end

  # Failing this one child in `start_child` beats raising, which would kill the
  # request before `update_registry` and strand every sibling row in the batch.
  defp unparsable_child(child_data, cs) do
    Logger.error(
      "[hub_manager] unrecognised start args for child #{inspect(Map.get(cs, :id))}; " <>
        "leaving it unaltered: #{inspect(Map.get(cs, :start))}"
    )

    child_data
  end

  defp place_child(child_data, %{start: {m, f, _args}}, {namespace, recorded_runner, operative}) do
    operative_id = Map.get(operative, :id)
    factory_id = Map.get(operative, :factory_id)

    # Recovery replay races the `FleetManager.runner_connect/3` that triggered
    # it: the connecting runner is not placeable until that handler returns, so
    # the first `nil` here is not final.
    assigned_runner =
      case OperativeManager.await_runner(
             namespace,
             factory_id,
             operative_id,
             batch_runner_wait_ms(),
             &OperativeManager.runner_registered?/1
           ) do
        {:ok, runner_id} ->
          runner_id

        {:error, reason} ->
          unplaceable_runner(recorded_runner, namespace, factory_id, operative_id, reason)
      end

    Logger.info(
      "[hub_manager] alter_child_data operative=#{inspect(operative_id)} " <>
        "factory=#{inspect(factory_id)} assigned_ce=#{inspect(assigned_runner)}"
    )

    new_args = [{namespace, assigned_runner, operative}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_runner)
    |> stamp_group(factory_id)
  end

  # The hub places an operative among the controllers serving its factory, and
  # reads that factory from the `:group` key of its row. A row written before
  # an operative carried one has none, so it is stamped here — `put_new`, so a
  # row that already carries a group keeps it and this handler never rewrites a
  # placement decision.
  defp stamp_group(child_data, nil), do: child_data

  defp stamp_group(child_data, factory_id) do
    update_in(child_data[:metadata], &Map.put_new(&1, :group, factory_id))
  end

  # Keep whatever the cspec recorded — a runner id, or nothing, which is what a
  # declaration holds, since the declared entry commits before this hook rewrites
  # anything. Either way the start proceeds.
  #
  # What that buys is small and worth stating plainly. The Overwatch starts, its
  # own bind re-resolves the runner (`Lifecycle.bind/3` ignores the recorded one
  # entirely), finds none, and holds the operative alive with no binding. The
  # convergence loop binds it when a runner advertising its factory connects. The
  # recorded-runner shape degrades exactly like this — the two shapes are not
  # distinguished here — and it is deliberately preferred to raising, which kills
  # the whole start request before `update_registry`, strands every row in the
  # batch, and, because the hook runs in the hub's shared `WorkerQueue` under a
  # linking `Task.async_stream`, takes that worker down with every start queued
  # behind it — without starting the operative either.
  #
  # The caller still learns, though not from here. A named factory with no runner
  # is refused by `Admission.validate_factory/2` before the hub is asked — its
  # catch-all clause means that covers a binary factory id only — and a runner
  # that goes away after that check reaches the caller through
  # `Init.bind_started/4`, whose bounded `:registry_runner_set` call meets the
  # stopping Overwatch and answers an error.
  defp unplaceable_runner(recorded_runner, namespace, _factory_id, operative_id, reason) do
    Logger.warning(
      "[hub_manager] no placeable runner for #{operative_id} in #{namespace} (#{reason}); " <>
        "starting with recorded_runner=#{inspect(recorded_runner)}, pending rebind"
    )

    recorded_runner
  end

  @batch_runner_wait_key :flezha_alter_child_data_deadline

  # One start request alters every child in sequence and runs `update_registry`
  # only after the last one, so the wait is shared across the batch: a per-child
  # budget would multiply past `cross_node_request_timeout` and the request
  # would be killed with its registry update undone.
  #
  # The process IS the request: `StartChildrenRequest.start_children/3` is the
  # sole dispatch site, under `WorkerQueue`'s `Task.async_stream`, which gives a
  # fresh unpooled process per request that dies with it. That is what scopes
  # the deadline — no wall-clock guard, which could only ever hand a second
  # request the first one's spent budget.
  defp batch_runner_wait_ms do
    now = System.monotonic_time(:millisecond)

    deadline =
      Process.get(@batch_runner_wait_key) ||
        now + Durations.get(:hub_runner_assign_wait_ms)

    Process.put(@batch_runner_wait_key, deadline)
    max(deadline - now, 0)
  end
end
