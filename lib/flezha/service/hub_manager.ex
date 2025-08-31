defmodule Flezha.Service.HubManager do
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias Flezha.Service.ProcessorManager

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

  def get_pid(namespace, processor_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.get_pids(hub_id, processor_id) do
      [] ->
        {:error, "no pid found"}

      pids ->
        {:ok, List.first(pids)}
    end
  end

  def get_all_processors(namespace) do
    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.process_list(hub_id, :global)
      |> Enum.map(fn {processor_id, [{_node, processor_pid}]} ->
        {processor_id, processor_pid}
      end)
    else
      []
    end
  end

  def register_handler(namespace, compute_engine_id) do
    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.registry_pid_inserted(),
      [
        %HookManager{
          id: compute_engine_id <> "_hub_pid_insertion",
          m: Flezha.Service.HubManager,
          f: :hook_insert_pid,
          a: [compute_engine_id, :_]
        }
      ]
    )
  end

  def unregister_handler(namespace, compute_engine_id) do
    ProcessHub.cancel_hook_handlers(
      Identificator.hub(namespace),
      Hook.registry_pid_inserted(),
      [compute_engine_id <> "_hub_pid_insertion"]
    )
  end

  def compute_engine_processors(namespace, compute_engine_id) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.tag_query(hub_id, compute_engine_id)
  end

  def compute_engine_switch(namespace, old_compute_engine_id, new_compute_engine_id) do
    hub_id = Identificator.hub(namespace)

    # Updates the compute_engine id in the registry.
    compute_engine_processors(namespace, old_compute_engine_id)
    |> Enum.each(fn {processor_id, [{_node, _processor_pid}]} ->
      processor_set_compute_engine(namespace, processor_id, new_compute_engine_id)
    end)

    # Propagate the change to all nodes in the namespace.
    hub_id
    |> ProcessHub.Coordinator.get_hub()
    |> ProcessHub.Service.Synchronizer.trigger_sync()
  end

  def processor_set_compute_engine(namespace, processor_id, compute_engine_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        ProcessRegistry.update(hub_id, processor_id, fn cspec, node_pids, metadata ->
          {cspec, node_pids, Map.put(metadata, :tag, compute_engine_id)}
        end)

        :ok

      false ->
        {:error, "hub not alive"}
    end
  end

  # TODO: we should also remove the pids incase the processor is removed.
  def hook_insert_pid(compute_engine_id, {processor_id, node_pids}) do
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
          Supervisor.which_children(Identificator.stream_sup_primary(compute_engine_id)) ++
            Supervisor.which_children(Identificator.stream_sup_secondary(compute_engine_id))

        Enum.each(stream_channels, fn {_, pid, _, _} ->
          GenServer.call(pid, {:processor_pid_update, processor_id, updated_pids})
        end)
    end
  end

  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    %{start: {m, f, [arg]}} = cs

    {namespace, processor} =
      case arg do
        {namespace, processor} ->
          {namespace, processor}

        {namespace, _existing_compute_engine, processor} ->
          {namespace, processor}
      end

    assigned_compute_engine = ProcessorManager.assign_compute_engine(namespace)
    new_args = [{namespace, assigned_compute_engine, processor}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_compute_engine)
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
          %HookManager{
            id: ProcessHub.Constant.Hook.child_data_alter(),
            m: __MODULE__,
            f: :alter_child_data,
            a: [hub_id, :_]
          }
        ]
      }
    })
  end
end
