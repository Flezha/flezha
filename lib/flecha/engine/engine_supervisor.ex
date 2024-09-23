defmodule Flecha.Engine.EngineSupervisor do
  alias Flecha.Engine.ControlUnit
  alias Flecha.Engine.ControlChannel
  alias Flecha.Service.Identificator
  alias Flecha.Socket.Listener
  alias Flecha.Socket.Support, as: SocketSupport

  use Supervisor

  def start_link({engine_id, sock}) do
    Supervisor.start_link(__MODULE__, {engine_id, sock},
      name: Identificator.engine_sup(engine_id)
    )
  end

  @impl true
  def init({engine_id, sock}) do
    stream_socket_path = SocketSupport.stream_socket_path(engine_id)

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
        id: Listener,
        start:
          {Listener, :start_link,
           [{Identificator.stream_listener(engine_id), stream_socket_path}]}
      },
      %{
        id: StreamSupervisor,
        start: {Flecha.Engine.StreamSupervisor, :start_link, [engine_id]}
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
