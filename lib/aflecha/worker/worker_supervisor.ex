defmodule Aflecha.Worker.WorkerSupervisor do
  alias Aflecha.Worker.ControlUnit
  alias Aflecha.Worker.ControlChannel
  alias Aflecha.Service.Identificator
  alias Aflecha.Socket.Listener
  alias Aflecha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({group_id, worker_group_id, sock}) do
    Supervisor.start_link(__MODULE__, {group_id, worker_group_id, sock},
      name: Identificator.unit_sup(group_id)
    )
  end

  @impl true
  def init({group_id, worker_group_id, sock}) do
    primary_stream_sock = SocketSupport.stream_socket_path(group_id, :primary)
    secondary_stream_sock = SocketSupport.stream_socket_path(group_id, :secondary)

    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [{group_id, worker_group_id}]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{group_id, worker_group_id, sock}]}
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
        start: {Aflecha.Worker.StreamSup, :start_link, [{group_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Aflecha.Worker.StreamSup, :start_link, [{group_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(group_id) do
    Identificator.unit_sup(group_id)
    |> Supervisor.which_children()
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end
