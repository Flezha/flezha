defmodule Flezha.Service.HubManager.Placement do
  @moduledoc """
  Where a child is placed and what it is told: the child spec and start args
  the hub's alter hook rewrites, and the runner tag on its row.
  """

  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Process.Cspec
  alias Flezha.Service.HubManager.Rows
  alias Flezha.Service.RelayManager.Init
  alias Flezha.Service.RegistryUpdate
  alias Flezha.State.DurabilityTier
  alias ProcessHub.Service.ProcessRegistry

  require Logger

  def runner_relays(namespace, runner_id) do
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

  def relay_set_runner(namespace, relay_id, runner_id) do
    Rows.with_live_hub(namespace, {:error, {:hub_down, namespace}}, fn hub_id ->
      # propagate: true — `runner_relays/2` is a local-table tag query, so a
      # peer handling this runner's disconnect misses the relay otherwise.
      # Returned, not discarded: a "No child found" is the whole signal that a
      # binding never reached the durable row.
      result =
        ProcessRegistry.update(
          hub_id,
          relay_id,
          RegistryUpdate.set_runner_tag(runner_id),
          propagate: true
        )

      # The cspec just changed: shipping state without it would give a
      # recovering controller perfect Cells for relays it does not know
      # exist.
      DurabilityTier.ship_cspec(hub_id)
      result
    end)
  end

  @doc """
  Hub-wide `child_registered` handler: ships the cspec to the durability tier
  and declares each local relay's rewritten row to the convergence loop.

  A start request writes the row after its children are already running, so the
  `metadata.tag` it carries can name a runner an Overwatch has since rebound
  away from. The loop's next tick reads the row beside the binding and
  re-publishes the binding when they differ; nothing is decided here.
  """
  def hook_registry_written(hub_id, %{child_id: relay_id} = hook_data)
      when is_binary(relay_id) do
    local = node()
    namespace = Identificator.namespace_from_hub(hub_id)
    DurabilityTier.ship_cspec(hub_id)

    Enum.each(Map.get(hook_data, :node_pids, []), fn
      {^local, pid} when is_pid(pid) ->
        Facts.declare({:registry_written, {namespace, relay_id}, %{}})

      _ ->
        :ok
    end)
  end

  # A plugin child has no binding for the loop to read.
  def hook_registry_written(hub_id, _plugin_child), do: DurabilityTier.ship_cspec(hub_id)

  @doc "The owning runner recorded on `relay_id`'s durable row, or `nil`."
  def relay_runner_tag(namespace, relay_id) do
    Rows.with_live_hub(namespace, nil, fn hub_id ->
      case ProcessRegistry.lookup(hub_id, relay_id, with_metadata: true) do
        {_cspec, _node_pids, metadata} when is_map(metadata) -> Map.get(metadata, :tag)
        _ -> nil
      end
    end)
  end

  # A plugin child is the plugin's home and has no runner to assign.
  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    case start_args(cs) do
      {:ok, {_namespace, _runner, %Flezha.Plugin{}}} -> child_data
      {:ok, parsed} -> place_child(child_data, cs, parsed)
      :error -> unparsable_child(child_data, cs)
    end
  end

  defp start_args(cspec) do
    case Cspec.start_arg(cspec) do
      {:ok, {namespace, %{} = relay}} -> {:ok, {namespace, nil, relay}}
      {:ok, {namespace, runner, %{} = relay}} -> {:ok, {namespace, runner, relay}}
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

  # The recorded runner is kept, whatever it is: the Overwatch's own bind
  # resolves the runner (`Lifecycle.bind/3` ignores this one), and the
  # convergence loop re-publishes a row whose tag differs from the binding.
  defp place_child(child_data, %{start: {m, f, _args}}, {namespace, recorded_runner, relay}) do
    child_data
    |> put_in([:child_spec, :start], {m, f, [{namespace, recorded_runner, relay}]})
    |> update_in([:child_spec], &Init.with_shutdown/1)
    |> put_in([:metadata, :tag], recorded_runner)
    |> stamp_group(Map.get(relay, :factory_id))
  end

  # The hub places a relay among the controllers serving its factory, and
  # reads that factory from the `:group` key of its row. A row written before
  # a relay carried one has none, so it is stamped here — `put_new`, so a
  # row that already carries a group keeps it and this handler never rewrites a
  # placement decision.
  defp stamp_group(child_data, nil), do: child_data

  defp stamp_group(child_data, factory_id) do
    update_in(child_data[:metadata], &Map.put_new(&1, :group, factory_id))
  end
end
