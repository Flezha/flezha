defmodule Flezha.Service.HubManager do
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias Flezha.Service.ComputationManager

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def stop_hub_manager() do
    Supervisor.stop(__MODULE__)
  end

  def start_hub(group_id) do
    Supervisor.start_child(__MODULE__, process_hub(group_id))
  end

  def stop_hub(group_id) do
    Supervisor.terminate_child(__MODULE__, Identificator.hub(group_id))
    Supervisor.delete_child(__MODULE__, Identificator.hub(group_id))
  end

  def hub_alive?(group_id) do
    ProcessHub.is_alive?(Identificator.hub(group_id))
  end

  def get_running_group_ids() do
    Supervisor.which_children(__MODULE__)
    |> Enum.map(fn {id, _pid, _type, _modules} -> id end)
    |> Enum.filter(fn id -> ProcessHub.is_alive?(id) end)
    |> Enum.map(&extract_group_id_from_hub/1)
    |> Enum.reject(&is_nil/1)
  end

  defp extract_group_id_from_hub(hub_id) when is_atom(hub_id) do
    hub_id
    |> Atom.to_string()
    |> String.replace_suffix("_hub", "")
  end

  defp extract_group_id_from_hub(_), do: nil

  def get_registry(group_id) do
    hub_id = Identificator.hub(group_id)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        {:ok, ProcessHub.registry_dump(hub_id)}

      false ->
        {:error, "hub not alive"}
    end
  end

  def get_hub_nodes(group_id, include_local \\ false) do
    opts =
      case include_local do
        true ->
          [:include_local]

        false ->
          []
      end

    hub_id = Identificator.hub(group_id)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.nodes(hub_id, opts)
    else
      []
    end
  end

  def get_pid(group_id, computation_id) do
    hub_id = Identificator.hub(group_id)

    case ProcessHub.get_pids(hub_id, computation_id) do
      [] ->
        {:error, "no pid found"}

      pids ->
        {:ok, List.first(pids)}
    end
  end

  def get_all_computations(group_id) do
    hub_id = Identificator.hub(group_id)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.process_list(hub_id, :global)
      |> Enum.map(fn {computation_id, [{_node, computation_pid}]} ->
        {computation_id, computation_pid}
      end)
    else
      []
    end
  end

  def register_handler(group_id, operator_id) do
    HookManager.register_handler(
      Identificator.hub(group_id),
      Hook.registry_pid_inserted(),
      %HookManager{
        id: operator_id <> "_hub_pid_insertion",
        m: Flezha.Service.HubManager,
        f: :hook_insert_pid,
        a: [operator_id, :_]
      }
    )
  end

  def unregister_handler(group_id, operator_id) do
    HookManager.cancel_handler(
      Identificator.hub(group_id),
      Hook.registry_pid_inserted(),
      operator_id <> "_hub_pid_insertion"
    )
  end

  def operator_computations(group_id, operator_id) do
    hub_id = Identificator.hub(group_id)

    ProcessHub.tag_query(hub_id, operator_id)
  end

  def operator_switch(group_id, old_operator_id, new_operator_id) do
    hub_id = Identificator.hub(group_id)

    # Updates the operator id in the registry.
    operator_computations(group_id, old_operator_id)
    |> Enum.each(fn {computation_id, [{_node, _computation_pid}]} ->
      computation_set_operator(group_id, computation_id, new_operator_id)
    end)

    # Propagate the change to all nodes in the group.
    ProcessHub.Service.Synchronizer.trigger_sync(hub_id)
  end

  def computation_set_operator(group_id, computation_id, operator_id) do
    hub_id = Identificator.hub(group_id)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        ProcessRegistry.update(hub_id, computation_id, fn cspec, node_pids, metadata ->
          {cspec, node_pids, Map.put(metadata, :tag, operator_id)}
        end)

        :ok

      false ->
        {:error, "hub not alive"}
    end
  end

  # TODO: we should also remove the pids incase the computation is removed.
  def hook_insert_pid(operator_id, {computation_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    case updated_pids do
      [] ->
        nil

      _ ->
        stream_channels =
          Supervisor.which_children(Identificator.stream_sup_primary(operator_id)) ++
            Supervisor.which_children(Identificator.stream_sup_secondary(operator_id))

        Enum.each(stream_channels, fn {_, pid, _, _} ->
          GenServer.call(pid, {:computation_pid_update, computation_id, updated_pids})
        end)
    end
  end

  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    %{start: {m, f, [arg]}} = cs

    {group_id, computation} =
      case arg do
        {group_id, computation} ->
          {group_id, computation}

        {group_id, _existing_operator, computation} ->
          {group_id, computation}
      end

    assigned_operator = ComputationManager.assign_operator(group_id)
    new_args = [{group_id, assigned_operator, computation}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_operator)
  end

  @impl true
  def init(_init_arg) do
    Supervisor.init([], strategy: :one_for_one)
  end

  defp process_hub(group_id) do
    hub_id = Identificator.hub(group_id)

    # TODO: The hub settings should be configurable.
    ProcessHub.child_spec(%ProcessHub{
      hub_id: hub_id,
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      # TODO: we may want to provide options for the partition tolerance strategy.
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
      distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
      migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{
        retention: 4000,
        handover: true,
        confirm_handover: true
      },
      hooks: %{
        Hook.child_data_alter() => [
          %ProcessHub.Service.HookManager{
            id: ProcessHub.Constant.Hook.child_data_alter(),
            m: Flezha.Service.HubManager,
            f: :alter_child_data,
            a: [hub_id, :_]
          }
        ]
      }
    })
  end
end
