defmodule Aflecha.Application do
  @moduledoc false
  alias Aflecha.Socket.Support

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    Aflecha.Socket.Support.prepare_socket_dir()

    children = [
      Aflecha.Service.Warden,
      Aflecha.Service.Identificator,
      Aflecha.Service.EventManager,
      {Aflecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      {Aflecha.Socket.Dispatcher, &Aflecha.Engine.Toolbox.engine_dispatch_fn/1},
      Aflecha.Service.PortPilot,
      Aflecha.Controller,
      Aflecha.Cluster.Orchestrator,
      process_hub()
    ]

    opts = [strategy: :one_for_one, name: Aflecha.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp process_hub() do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: :aflecha_hub,
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
            m: Aflecha.Service.HubManager,
            f: :hook_insert_pid,
            a: [:_]
          }
        ]
      }
    })
  end
end
