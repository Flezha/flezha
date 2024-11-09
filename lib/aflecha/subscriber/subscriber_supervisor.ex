defmodule Aflecha.Subscriber.SubscriberSupervisor do
  alias Aflecha.Subscriber.Controller
  alias Aflecha.Subscriber.ControlUnit
  alias Aflecha.Subscriber.ControlChannel
  alias Aflecha.Service.Identificator
  alias Aflecha.Socket.Listener
  alias Aflecha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({subscriber_id, sock}) do
    Supervisor.start_link(__MODULE__, {subscriber_id, sock},
      name: Identificator.engine_sup(subscriber_id)
    )
  end

  @impl true
  def init({subscriber_id, sock}) do
    primary_stream_sock = SocketSupport.stream_socket_path(subscriber_id, :primary)
    secondary_stream_sock = SocketSupport.stream_socket_path(subscriber_id, :secondary)

    children = [
      process_hub(subscriber_id),
      %{
        id: Controller,
        start: {Controller, :start_link, [subscriber_id]}
      },
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [subscriber_id]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{subscriber_id, sock}]}
      },
      %{
        id: :primary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(subscriber_id, :primary), primary_stream_sock}]}
      },
      %{
        id: :secondary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(subscriber_id, :secondary), secondary_stream_sock}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Aflecha.Subscriber.StreamSupervisor, :start_link, [{subscriber_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Aflecha.Subscriber.StreamSupervisor, :start_link, [{subscriber_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(subscriber_id) do
    Supervisor.which_children(Identificator.engine_sup(subscriber_id))
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end

  defp process_hub(subscriber_id) do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: subscriber_id,
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
