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

  def start_hub(namespace) do
    Supervisor.start_child(__MODULE__, process_hub(namespace))
  end

  def stop_hub(namespace) do
    Supervisor.terminate_child(__MODULE__, Identificator.hub(namespace))
    Supervisor.delete_child(__MODULE__, Identificator.hub(namespace))
  end

  def hub_alive?(namespace) do
    ProcessHub.is_alive?(Identificator.hub(namespace))
  end

  def get_running_namespaces() do
    Supervisor.which_children(__MODULE__)
    |> Enum.map(fn {id, _pid, _type, _modules} -> id end)
    |> Enum.filter(fn id -> ProcessHub.is_alive?(id) end)
    |> Enum.map(&extract_namespace_from_hub/1)
    |> Enum.reject(&is_nil/1)
  end

  defp extract_namespace_from_hub(hub_id) when is_atom(hub_id) do
    hub_id
    |> Atom.to_string()
    |> String.replace_suffix("_hub", "")
  end

  defp extract_namespace_from_hub(_), do: nil

  def get_registry(namespace) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        {:ok, ProcessHub.registry_dump(hub_id)}

      false ->
        {:error, "hub not alive"}
    end
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts =
      case include_local do
        true ->
          [:include_local]

        false ->
          []
      end

    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.nodes(hub_id, opts)
    else
      []
    end
  end

  def get_pid(namespace, computation_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.get_pids(hub_id, computation_id) do
      [] ->
        {:error, "no pid found"}

      pids ->
        {:ok, List.first(pids)}
    end
  end

  def get_all_computations(namespace) do
    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.process_list(hub_id, :global)
      |> Enum.map(fn {computation_id, [{_node, computation_pid}]} ->
        {computation_id, computation_pid}
      end)
    else
      []
    end
  end

  def register_handler(namespace, operator_id) do
    HookManager.register_handler(
      Identificator.hub(namespace),
      Hook.registry_pid_inserted(),
      %HookManager{
        id: operator_id <> "_hub_pid_insertion",
        m: Flezha.Service.HubManager,
        f: :hook_insert_pid,
        a: [operator_id, :_]
      }
    )
  end

  def unregister_handler(namespace, operator_id) do
    HookManager.cancel_handler(
      Identificator.hub(namespace),
      Hook.registry_pid_inserted(),
      operator_id <> "_hub_pid_insertion"
    )
  end

  def operator_computations(namespace, operator_id) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.tag_query(hub_id, operator_id)
  end

  def operator_switch(namespace, old_operator_id, new_operator_id) do
    hub_id = Identificator.hub(namespace)

    # Updates the operator id in the registry.
    operator_computations(namespace, old_operator_id)
    |> Enum.each(fn {computation_id, [{_node, _computation_pid}]} ->
      computation_set_operator(namespace, computation_id, new_operator_id)
    end)

    # Propagate the change to all nodes in the namespace.
    ProcessHub.Service.Synchronizer.trigger_sync(hub_id)
  end

  def computation_set_operator(namespace, computation_id, operator_id) do
    hub_id = Identificator.hub(namespace)

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

    {namespace, computation} =
      case arg do
        {namespace, computation} ->
          {namespace, computation}

        {namespace, _existing_operator, computation} ->
          {namespace, computation}
      end

    assigned_operator = ComputationManager.assign_operator(namespace)
    new_args = [{namespace, assigned_operator, computation}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_operator)
  end

  @impl true
  def init(_init_arg) do
    Supervisor.init([], strategy: :one_for_one)
  end

  defp process_hub(namespace) do
    hub_id = Identificator.hub(namespace)

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
