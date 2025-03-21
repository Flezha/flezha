defmodule Aflecha.HubManager do
  alias Aflecha.Service.Identificator

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def start_hub(group_id) do
    Supervisor.start_child(__MODULE__, process_hub(group_id))
  end

  def stop_hub(group_id) do
    Supervisor.terminate_child(__MODULE__, Identificator.hub(group_id))
  end

  def hub_alive?(group_id) do
    ProcessHub.is_alive?(Identificator.hub(group_id))
  end

  def get_pid(group_id, pipeline_id) do
    hub_id = Identificator.hub(group_id)

    case ProcessHub.get_pid(hub_id, pipeline_id) do
      nil -> {:error, "no pid found"}
      pid -> {:ok, pid}
    end
  end

  def register_handler(group_id, operator_id) do
    ProcessHub.Service.HookManager.register_handler(
      Identificator.hub(group_id),
      ProcessHub.Constant.Hook.registry_pid_inserted(),
      %ProcessHub.Service.HookManager{
        id: operator_id <> "_hub_pid_insertion",
        m: Aflecha.HubManager,
        f: :hook_insert_pid,
        a: [operator_id, :_]
      }
    )
  end

  def hook_insert_pid(operator_id, {pipeline_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(operator_id)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(operator_id))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.call(pid, {:worker_pid_update, pipeline_id, updated_pids})
    end)
  end

  @impl true
  def init(_init_arg) do
    Supervisor.init([], strategy: :one_for_one)
  end

  defp process_hub(group_id) do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: Identificator.hub(group_id),
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.DynamicQuorum{
        quorum_size: 60
      },
      distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
      migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{
        retention: 4000,
        handover: true
      }
    })
  end
end
