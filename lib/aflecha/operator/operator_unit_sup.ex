defmodule Aflecha.Operator.OperatorUnitSup do
  alias Aflecha.Operator.ControlUnit
  alias Aflecha.Operator.ControlChannel
  alias Aflecha.Service.Identificator
  alias Aflecha.Socket.Listener
  alias Aflecha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({operator_id, sock}) do
    Supervisor.start_link(__MODULE__, {operator_id, sock},
      name: Identificator.engine_sup(operator_id)
    )
  end

  @impl true
  def init({operator_id, sock}) do
    primary_stream_sock = SocketSupport.stream_socket_path(operator_id, :primary)
    secondary_stream_sock = SocketSupport.stream_socket_path(operator_id, :secondary)

    children = [
      process_hub(operator_id),
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [operator_id]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{operator_id, sock}]}
      },
      %{
        id: :primary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(operator_id, :primary), primary_stream_sock}]}
      },
      %{
        id: :secondary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(operator_id, :secondary), secondary_stream_sock}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Aflecha.Operator.StreamSup, :start_link, [{operator_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Aflecha.Operator.StreamSup, :start_link, [{operator_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(operator_id) do
    Supervisor.which_children(Identificator.engine_sup(operator_id))
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end

  defp process_hub(operator_id) do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: Identificator.hub(operator_id),
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
            a: [operator_id, :_]
          }
        ]
      }
    })
  end
end
