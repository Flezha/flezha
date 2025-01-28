defmodule Aflecha.HubManager do
  alias Aflecha.Service.Identificator

  use Supervisor

  def start_link() do
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

  def hook_insert_pid(group_id, {computation_id, node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(group_id)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(group_id))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.cast(pid, {:computation_pid_update, computation_id, updated_pids})
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
      },
      hooks: %{
        ProcessHub.Constant.Hook.registry_pid_inserted() => [
          %ProcessHub.Service.HookManager{
            id: :aflecha_hub_pid_insertion,
            m: Aflecha.HubManager,
            f: :hook_insert_pid,
            a: [group_id, :_]
          }
        ]
      }
    })
  end
end
