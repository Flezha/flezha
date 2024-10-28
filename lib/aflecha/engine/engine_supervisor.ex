defmodule Aflecha.Engine.EngineSupervisor do
  alias Aflecha.Engine.ControlUnit
  alias Aflecha.Engine.ControlChannel
  alias Aflecha.Service.Identificator
  alias Aflecha.Socket.Listener
  alias Aflecha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({engine_id, sock}) do
    Supervisor.start_link(__MODULE__, {engine_id, sock},
      name: Identificator.engine_sup(engine_id)
    )
  end

  @impl true
  def init({engine_id, sock}) do
    primary_stream_sock = SocketSupport.stream_socket_path(engine_id, :primary)
    secondary_stream_sock = SocketSupport.stream_socket_path(engine_id, :secondary)

    children = [
      %{
        id: ControlUnit,
        start: {ControlUnit, :start_link, [engine_id]}
      },
      %{
        id: ControlChannel,
        start: {ControlChannel, :start_link, [{engine_id, sock}]}
      },
      %{
        id: :primary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(engine_id, :primary), primary_stream_sock}]}
      },
      %{
        id: :secondary_stream_listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(engine_id, :secondary), secondary_stream_sock}]}
      },
      %{
        id: :primary_stream_sup,
        start: {Aflecha.Engine.StreamSupervisor, :start_link, [{engine_id, :primary}]}
      },
      %{
        id: :secondary_stream_sup,
        start: {Aflecha.Engine.StreamSupervisor, :start_link, [{engine_id, :secondary}]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def find_control_channel(engine_id) do
    Supervisor.which_children(Identificator.engine_sup(engine_id))
    |> Enum.find(fn {id, _, _, _} -> id === ControlChannel end)
    |> elem(1)
  end
end
