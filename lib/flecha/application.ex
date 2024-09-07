defmodule Flecha.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    prepare_socket_dir()

    children = [
      Flecha.CommandChannel,
      Flecha.EngineManager,
      Flecha.ChannelSupervisor,
      Flecha.Controller,
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
      migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{},
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

  def prepare_socket_dir() do
    File.rm_rf("app_data/sockets")
    File.mkdir_p("app_data/sockets")
  end
end
