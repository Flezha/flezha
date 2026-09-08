defmodule Flezha.Service.HubManager.Placement do
  @moduledoc """
  Where a child is placed and what it is told: the child spec and start args,
  what an unplaceable runner degrades to, and the runner-assignment wait.
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Process.Cspec
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager.Rows
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.RegistryUpdate
  alias Flezha.State.DurabilityTier
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry

  require Logger

  # 20% of the request bound, leaving the rest for the operative starts the
  # bound was sized for. Overrunning it is not fatal — see `unplaceable_runner/5`.

  @doc """
  Register `runner_id`'s operative-insertion hook on the namespace's hub.

  Answers `{:error, :hub_unavailable}` when the hub is not there to take it —
  the connect is refused on that, because a runner whose hook never registered
  gets no operative pid seeded into its stream channels.
  """
  @spec register_handler(binary(), binary()) :: :ok | {:error, term()}
  def register_handler(namespace, runner_id) do
    Rows.with_live_hub(namespace, {:error, :hub_unavailable}, fn hub_id ->
      ProcessHub.register_hook_handlers(
        hub_id,
        Hook.child_registered(),
        [
          %HookManager{
            id: runner_id <> "_hub_pid_insertion",
            m: Flezha.Service.HubManager,
            f: :hook_insert_pid,
            a: [runner_id, :_]
          }
        ]
      )
    end)
  end

  @spec unregister_handler(binary(), binary()) :: :ok | {:error, term()}
  def unregister_handler(namespace, runner_id) do
    Rows.with_live_hub(namespace, {:error, :hub_unavailable}, fn hub_id ->
      ProcessHub.cancel_hook_handlers(
        hub_id,
        Hook.child_registered(),
        [runner_id <> "_hub_pid_insertion"]
      )
    end)
  end

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
      # recovering controller perfect Entities for operatives it does not know
      # exist.
      DurabilityTier.ship_cspec(hub_id)
      result
    end)
  end

  # The route is operative_id -> local Overwatch pid, so seeding it into a
  # non-owning runner's channels is inert: any runner's channel resolves the
  # same process, and only the owner ever emits frames for this operative.
  #
  # This and `hook_remove_pid/2` run inside the `child_registered` hook, where
  # an exit aborts the registry write that dispatched it — which is why the
  # fan-out is bounded and never raises.
  def hook_insert_pid(runner_id, %{child_id: operative_id} = hook_data) do
    curr_node = node()

    updated_pids =
      Map.get(hook_data, :node_pids, [])
      |> Enum.filter(fn {node, _pid} -> node === curr_node end)
      |> Enum.map(fn {_, pid} -> pid end)

    case updated_pids do
      [] ->
        nil

      _ ->
        StreamPool.call_channels(runner_id, {:operative_pid_update, operative_id, updated_pids})
    end
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

  @doc """
  Convenience over `hook_insert_pid/2` for the common case: seed one operative's
  local `pid` into `runner_id`'s pre-spawned StreamChannels so data-plane frames
  route to it. Used by the convergence rebind, the reconnect binding swap, and
  the Overwatch's own bind.
  """
  def hook_insert_local_pid(runner_id, operative_id, pid) when is_pid(pid) do
    hook_insert_pid(runner_id, %{child_id: operative_id, node_pids: [{node(), pid}]})
  end

  @doc """
  Clear a operative's pid mapping from every stream channel of `runner_id`.
  Invoked directly by `Flezha.Runner.FleetManager.evict_runner/1` so a
  dead runner's stale mappings never linger; idempotent on an
  already-gone runner.
  """
  def hook_remove_pid(runner_id, %{child_id: operative_id}) do
    StreamPool.call_channels(runner_id, {:operative_pid_remove, operative_id})

    :telemetry.execute(
      [:flezha, :hub, :pid_removed],
      %{count: 1},
      %{runner_id: runner_id, operative_id: operative_id}
    )

    :ok
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
