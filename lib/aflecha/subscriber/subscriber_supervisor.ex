defmodule Aflecha.Subscriber.SubscriberSupervisor do
  alias Aflecha.Subscriber.ControlUnit
  alias Aflecha.Subscriber.ControlChannel
  alias Aflecha.Service.Identificator
  alias Aflecha.Socket.Listener
  alias Aflecha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({group_id, sock}) do
    Supervisor.start_link(__MODULE__, {group_id, sock}, name: Identificator.engine_sup(group_id))
  end

  @impl true
  def init({group_id, sock}) do
    primary_stream_sock = SocketSupport.stream_socket_path(group_id, :primary)
    secondary_stream_sock = SocketSupport.stream_socket_path(group_id, :secondary)

    children = [
      process_hub(group_id),
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [group_id]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{group_id, sock}]}
      },
      %{
        id: :primary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(group_id, :primary), primary_stream_sock}]}
      },
      %{
        id: :secondary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(group_id, :secondary), secondary_stream_sock}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Aflecha.Subscriber.StreamSupervisor, :start_link, [{group_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Aflecha.Subscriber.StreamSupervisor, :start_link, [{group_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(group_id) do
    Supervisor.which_children(Identificator.engine_sup(group_id))
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
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
            m: Aflecha.Service.HubManager,
            f: :hook_insert_pid,
            a: [group_id, :_]
          }
        ]
      }
    })
  end
end
