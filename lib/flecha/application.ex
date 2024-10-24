defmodule Flecha.Application do
  @moduledoc false
  alias Flecha.Socket.Support

  require Logger

  use Application

  @impl true
  def start(_type, _args) do
    Flecha.Socket.Support.prepare_socket_dir()

    children = [
      Flecha.Service.Warden,
      Flecha.Service.Identificator,
      Flecha.Service.EventManager,
      {Flecha.Socket.Listener, {:control_listener, Support.control_socket_path()}},
      {Flecha.Socket.Dispatcher, &Flecha.Engine.Toolbox.engine_dispatch_fn/1},
      Flecha.Service.PortPilot,
      Flecha.Controller,
      Flecha.Cluster.DiscoverySupervisor,
      process_hub()
    ]

    opts = [strategy: :one_for_one, name: Flecha.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp process_hub() do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: :flecha_hub,
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
            id: :flecha_hub_pid_insertion,
            m: Flecha.Service.HubManager,
            f: :hook_insert_pid,
            a: [:_]
          }
        ]
      }
    })
  end
end
